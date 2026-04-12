# domains/notifications/tasks.py
"""
Celery tasks for asynchronous email delivery.

IMPORTANT — registration:
  This module uses @celery_app.task (not @shared_task) so it is explicitly
  bound to the Calyphant Celery app instance. It is also added to the
  `include` list in worker/celery.py so the worker auto-discovers it on boot.

  Using @shared_task without being in `include` causes tasks to silently
  fail with "Received unregistered task" on the worker side.

Queue: "notifications"
Retry policy: max 3 retries, 60-second delay between attempts.
"""

from __future__ import annotations

import asyncio

from celery.utils.log import get_task_logger

# Import the app instance directly — do NOT use shared_task here.
# This guarantees the task is always bound to the correct Celery app
# regardless of import order.
from worker.celery import celery_app

logger = get_task_logger(__name__)


def _run(coro):
    """Run an async coroutine in a fresh event loop (safe inside Celery workers)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# send_async_email
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="notifications.send_async_email",
    max_retries=3,
    default_retry_delay=60,
    queue="notifications",
    # acks_late=True so we don't lose the task if the worker crashes mid-send
    acks_late=True,
)
def send_async_email(
    self,
    to_email: str,
    to_name: str,
    subject: str,
    html_content: str,
    sender_type: str = "system",
    notification_kind: str | None = None,
    correlation_id: str | None = None,
    idempotency_key: str | None = None,
) -> bool:
    """
    Celery task: deliver a transactional email via SendPulse.

    Called from:
      - domains/notifications/service.py  (dispatch_event fan-out)
      - domains/teams/service.py          (workspace invite)
      - core/auth.py                      (welcome, password reset)
      - domains/billing/flutterwave.py    (plan upgraded)
      - domains/backups/engine.py         (backup completed/failed — scheduled only)
      - domains/migrations/router.py      (migration failed)

    Returns True on successful delivery, False otherwise.
    On failure, retries up to 3 times with 60s delays before giving up.
    """
    from domains.notifications.emails import send_sendpulse_email

    try:
        if not to_email or "@" not in to_email:
            raise ValueError("send_async_email requires a valid recipient email")
        if not subject.strip():
            raise ValueError("send_async_email requires a non-empty subject")
        if not html_content.strip():
            raise ValueError("send_async_email requires non-empty html_content")

        success = _run(
            send_sendpulse_email(
                to_email=to_email,
                to_name=to_name,
                subject=subject,
                html_content=html_content,
                sender_type=sender_type,
                correlation_id=correlation_id,
            )
        )

        if not success:
            raise RuntimeError(
                f"SendPulse returned failure for {to_email}. Will retry."
            )

        logger.info(
            "send_async_email delivered "
            f"to={to_email} kind={notification_kind or 'n/a'} "
            f"corr={correlation_id or 'n/a'} idem={idempotency_key or 'n/a'}"
        )

        return True

    except Exception as exc:
        logger.error(
            f"send_async_email failed for {to_email} "
            f"(attempt {self.request.retries + 1}/{self.max_retries + 1}): {exc}"
        )
        # Retry with exponential-ish back-off: 60s, 120s, 180s
        raise self.retry(
            exc=exc,
            countdown=60 * (self.request.retries + 1),
        )

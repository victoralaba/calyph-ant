# domains/notifications/service.py
"""
Notifications Domain (State Manager & Event Dispatcher).

Acts as the "Engine of Truth" for the event matrix.
Evaluates preferences, saves state to Postgres, dispatches to WebSockets
(Toasts), and pushes email I/O to Celery via domains/notifications/tasks.py.

Key architectural decisions
----------------------------
1. EMAIL DISPATCH: All email sending goes through the Celery task
   `notifications.send_async_email`. Never call send_sendpulse_email directly
   from this service — it would block the async event loop.

2. N+1 FIX: dispatch_workspace_event fetches ALL workspace member user records
   in a single query before the fan-out loop, not one per member.

3. SINGLE SOURCE OF TRUTH: NotificationKind enum values are derived from
   EVENT_MATRIX keys at class definition time. Adding a new event type only
   requires adding it to EVENT_MATRIX — the enum updates automatically.

4. CONVENIENCE HELPERS: send_welcome_email() and send_password_reset_email()
   are defined here as top-level async functions so core/auth.py can import
   them without circular dependency issues. They dispatch directly to the
   Celery task (fire-and-forget) rather than going through dispatch_event,
   because they are pure email operations with no in-app inbox component.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import DateTime, String, Text, Boolean, select, update, and_
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.config import settings
from core.db import get_db, get_redis
from shared.types import Base


# ---------------------------------------------------------------------------
# The Event Matrix — single source of truth
# ---------------------------------------------------------------------------
# Defines the physical behavior of every system event.
# All keys must be valid Python identifiers (used to build NotificationKind).
#
# Fields:
#   in_app   — write a Notification row to the user's inbox
#   email    — dispatch send_async_email to Celery
#   toast    — broadcast a transient WebSocket toast (no persistence)
#   sender   — "system" | "ceo" (controls SendPulse from-address)
#   priority — "High" | "Medium" | "Low" (used in toast payloads)

EVENT_MATRIX: dict[str, dict[str, Any]] = {
    "user_signup":              {"in_app": False, "email": True,  "toast": False, "sender": "ceo",    "priority": "High"},
    "password_reset":           {"in_app": False, "email": True,  "toast": False, "sender": "system", "priority": "High"},
    "security_new_login":       {"in_app": True,  "email": True,  "toast": False, "sender": "system", "priority": "High"},
    "team_invite_received":     {"in_app": True,  "email": True,  "toast": False, "sender": "system", "priority": "High"},
    "team_invite_accepted":     {"in_app": True,  "email": False, "toast": False, "sender": "system", "priority": "Medium"},
    "team_role_changed":        {"in_app": True,  "email": False, "toast": False, "sender": "system", "priority": "Medium"},
    "db_connection_success":    {"in_app": False, "email": False, "toast": True,  "sender": "system", "priority": "Low"},
    "db_connection_failed":     {"in_app": True,  "email": False, "toast": True,  "sender": "system", "priority": "High"},
    "schema_migration_success": {"in_app": True,  "email": False, "toast": False, "sender": "system", "priority": "Medium"},
    "schema_migration_failed":  {"in_app": True,  "email": True,  "toast": False, "sender": "system", "priority": "High"},
    "backup_completed":         {"in_app": True,  "email": False, "toast": False, "sender": "system", "priority": "Low"},
    "backup_failed":            {"in_app": True,  "email": True,  "toast": False, "sender": "system", "priority": "High"},
    "billing_plan_upgraded":    {"in_app": False, "email": True,  "toast": False, "sender": "ceo",    "priority": "High"},
    "platform_update":          {"in_app": True,  "email": True,  "toast": False, "sender": "ceo",    "priority": "Low"},
}

NotificationSender = Literal["system", "ceo"]


def _normalize_sender(sender: str | None) -> NotificationSender:
    """
    Normalize sender identities to a safe, known set.

    We intentionally default unknown values to "system" so event dispatch
    remains resilient even when a caller passes malformed metadata.
    """
    if sender == "ceo":
        return "ceo"
    return "system"


async def enqueue_email_notification(
    *,
    to_email: str,
    to_name: str,
    subject: str,
    html_content: str,
    sender_type: str = "system",
    notification_kind: str | None = None,
    correlation_id: str | None = None,
    idempotency_key: str | None = None,
    bypass_rate_limit: bool = False,
) -> bool:
    """
    Single queueing boundary for outbound email jobs.

    Architectural intent:
      - Keep producer call-sites thin and consistent.
      - Guarantee all outbound emails go through the same Celery route.
      - Centralize sender normalization ("system" | "ceo").

    Returns:
      True if the task was enqueued, False if skipped by idempotency guard.
    """
    from domains.notifications.tasks import send_async_email

    # Per-recipient hourly cap to reduce blast radius from abuse loops.
    if not bypass_rate_limit:
        try:
            redis = await get_redis()
            recipient_key = f"notifications:email:hour:{to_email.lower()}"
            current = await redis.incr(recipient_key)
            if current == 1:
                await redis.expire(recipient_key, 3600)
            if current > settings.NOTIFICATIONS_EMAIL_LIMIT_PER_HOUR:
                await redis.incr(f"notifications:throttle:email:recipient:{to_email.lower()}")
                logger.warning(
                    "Email enqueue throttled "
                    f"recipient={to_email} limit={settings.NOTIFICATIONS_EMAIL_LIMIT_PER_HOUR}/hour"
                )
                return False
        except Exception as exc:
            logger.warning(f"Email rate-limit store unavailable; proceeding with enqueue: {exc}")

    if idempotency_key:
        try:
            redis = await get_redis()
            claimed = await redis.set(
                f"notifications:idempotency:{idempotency_key}",
                "1",
                ex=3600,
                nx=True,
            )
            if not claimed:
                logger.info(f"Notification email deduplicated for key={idempotency_key}")
                return False
        except Exception as exc:
            # Never block dispatch if Redis idempotency tracking is unavailable.
            logger.warning(f"Idempotency store unavailable; proceeding with enqueue: {exc}")

    send_async_email.apply_async(  # type: ignore[attr-defined]
        kwargs={
            "to_email": to_email,
            "to_name": to_name or to_email,
            "subject": subject,
            "html_content": html_content,
            "sender_type": _normalize_sender(sender_type),
            "notification_kind": notification_kind,
            "correlation_id": correlation_id,
            "idempotency_key": idempotency_key,
        },
        queue="notifications",
    )
    return True


# ---------------------------------------------------------------------------
# NotificationKind — auto-derived from EVENT_MATRIX keys
# ---------------------------------------------------------------------------
# Adding a new event to EVENT_MATRIX above automatically makes it valid here.
# You do NOT need to manually add it to this enum.

NotificationKind = Enum(  # type: ignore[misc]
    "NotificationKind",
    {k: k for k in EVENT_MATRIX},
    type=str,
)


# ---------------------------------------------------------------------------
# ORM model
# ---------------------------------------------------------------------------

class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    workspace_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)

    kind: Mapped[str] = mapped_column(String(60), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    meta: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    # Soft-delete / archive support
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Convenience email helpers (used by core/auth.py and other domains)
# ---------------------------------------------------------------------------
# These are pure fire-and-forget email dispatchers with NO in-app component.
# They exist so callers don't need to construct HTML or subjects themselves.
# They call the Celery task directly and never block the async event loop.

async def send_welcome_email(email: str, name: str) -> None:
    """
    Fire-and-forget: send the welcome email after registration.
    Called from core/auth.py inside asyncio.create_task().

    UI NOTE: The welcome email is sent immediately after account creation.
    If SendPulse is temporarily down, Celery will retry up to 3 times.
    """
    from domains.notifications.templates import build_welcome_email

    subject, html = build_welcome_email(name)
    await enqueue_email_notification(
        to_email=email,
        to_name=name or email,
        subject=subject,
        html_content=html,
        sender_type="ceo",
        notification_kind="user_signup",
    )


async def send_password_reset_email(email: str, token: str) -> None:
    """
    Fire-and-forget: send the password reset email.
    Called from core/auth.py inside asyncio.create_task().

    UI NOTE: The reset link expires in 1 hour. The email template states this.
    If the user doesn't receive it, they must request a new one.
    """
    from domains.notifications.templates import build_password_reset_email

    subject, html = build_password_reset_email(token)
    await enqueue_email_notification(
        to_email=email,
        to_name=email,
        subject=subject,
        html_content=html,
        sender_type="system",
        notification_kind="password_reset",
        idempotency_key=f"password_reset:{email}:{token}",
    )


# ---------------------------------------------------------------------------
# The Grand Dispatcher
# ---------------------------------------------------------------------------

async def dispatch_event(
    db: AsyncSession,
    user_id: UUID,
    kind: Any,  # NotificationKind member or its .value string
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    workspace_id: UUID | None = None,
    meta: dict | None = None,
    email_subject: str | None = None,
    email_html: str | None = None,
    # Pre-fetched user data to avoid redundant DB round-trips when called
    # in a loop from dispatch_workspace_event.
    _user_email: str | None = None,
    _user_name: str | None = None,
    _notif_prefs: dict | None = None,
    _autocommit: bool = True,
    event_id: str | None = None,
) -> None:
    """
    The Single Source of Truth for system messaging.

    Evaluates the EVENT_MATRIX, checks user preferences, and executes:
      1. Postgres Notification row (in-app inbox)
      2. WebSocket toast broadcast
      3. Celery email task (fire-and-forget)

    Private parameters _user_email, _user_name, _notif_prefs allow the caller
    to pass pre-fetched data, eliminating N+1 queries when this is called in a
    loop (e.g. dispatch_workspace_event). Do not use these from outside this module.
    """
    from domains.teams.service import ws_manager
    from shared.pubsub import SignalEvent

    # Normalise kind to its string value
    kind_str: str = kind.value if hasattr(kind, "value") else str(kind)

    rules = EVENT_MATRIX.get(kind_str)
    if not rules:
        logger.warning(f"Unmapped event kind dispatched: {kind_str}")
        return

    # Suppress repeated non-critical notifications with same fingerprint.
    # High-priority security/billing style alerts are never suppressed.
    if rules.get("priority") != "High":
        try:
            redis = await get_redis()
            raw = f"{user_id}|{workspace_id}|{kind_str}|{title}|{body or ''}|{event_id or ''}"
            fp = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            dedupe_key = f"notifications:repeat:{fp}"
            accepted = await redis.set(
                dedupe_key,
                "1",
                ex=settings.NOTIFICATIONS_REPEAT_SUPPRESS_SECONDS,
                nx=True,
            )
            if not accepted:
                await redis.incr(f"notifications:throttle:repeat:{kind_str}")
                logger.info(f"Notification repeat-suppressed kind={kind_str} user_id={user_id}")
                return
        except Exception as exc:
            logger.warning(f"Repeat suppression store unavailable; continuing: {exc}")

    # Fetch preferences & user data only if not pre-supplied (avoids N+1)
    notif_prefs: dict = _notif_prefs or {}
    user_email: str = _user_email or ""
    user_name: str = _user_name or ""

    if _notif_prefs is None or _user_email is None:
        from domains.users.service import get_preferences, get_user
        user = await get_user(db, user_id)
        if not user:
            return
        user_email = user.email
        user_name = user.full_name or user.email
        prefs = await get_preferences(db, user_id)
        notif_prefs = prefs.get("notifications", {})

    # 1. Database (In-App Inbox)
    if rules.get("in_app") and notif_prefs.get("in_app", True):
        if rules.get("priority") != "High":
            try:
                redis = await get_redis()
                in_app_key = f"notifications:in_app:hour:{user_id}"
                in_app_count = await redis.incr(in_app_key)
                if in_app_count == 1:
                    await redis.expire(in_app_key, 3600)
                if in_app_count > settings.NOTIFICATIONS_USER_IN_APP_LIMIT_PER_HOUR:
                    await redis.incr(f"notifications:throttle:in_app:user:{user_id}")
                    logger.warning(
                        "In-app notification throttled "
                        f"user_id={user_id} limit={settings.NOTIFICATIONS_USER_IN_APP_LIMIT_PER_HOUR}/hour"
                    )
                    return
            except Exception as exc:
                logger.warning(f"In-app rate-limit store unavailable; proceeding with write: {exc}")

        notif = Notification(
            user_id=user_id,
            workspace_id=workspace_id,
            kind=kind_str,
            title=title,
            body=body,
            action_url=action_url,
            meta=meta or {},
        )
        db.add(notif)
        if _autocommit:
            await db.commit()

    # 2. WebSocket Toast
    if rules.get("toast") and workspace_id:
        import asyncio
        event_payload = SignalEvent(
            event="ui_toast",
            workspace_id=workspace_id,
            entity_id=str(user_id),
            meta={  # type: ignore[call-arg]
                "title": title,
                "body": body,
                "kind": kind_str,
                "priority": rules.get("priority"),
            },
        )
        asyncio.create_task(ws_manager.broadcast(workspace_id, event_payload))

    # 3. Celery Email Fan-out
    if rules.get("email") and email_subject and email_html:
        if notif_prefs.get("email", True):
            await enqueue_email_notification(
                to_email=user_email,
                to_name=user_name,
                subject=email_subject,
                html_content=email_html,
                sender_type=rules.get("sender", "system"),
                notification_kind=kind_str,
                idempotency_key=(
                    event_id
                    or f"dispatch_event:{kind_str}:{user_id}:{workspace_id}:{email_subject}"
                ),
                bypass_rate_limit=(rules.get("priority") == "High"),
            )


async def dispatch_workspace_event(
    db: AsyncSession,
    workspace_id: UUID,
    kind: Any,  # NotificationKind member or string
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    exclude_user_id: UUID | None = None,
    meta: dict | None = None,
    email_subject: str | None = None,
    email_html: str | None = None,
    event_id: str | None = None,
) -> None:
    """
    Fan out an event to all active members of a workspace.

    N+1 FIX: Fetches all member user records and all preferences in two
    bulk queries before the fan-out loop. For a 25-member workspace this
    reduces the query count from ~50 to ~3 regardless of team size.
    """
    from domains.teams.service import WorkspaceMember
    from domains.users.service import User, DEFAULT_PREFERENCES

    # 1. Fetch all member user_ids
    result = await db.execute(
        select(WorkspaceMember.user_id).where(
            WorkspaceMember.workspace_id == workspace_id
        )
    )
    member_ids = [row[0] for row in result.all()]

    if exclude_user_id:
        member_ids = [uid for uid in member_ids if uid != exclude_user_id]

    if not member_ids:
        return

    # Workspace-wide fan-out throttle (per minute) to contain abuse blasts.
    try:
        redis = await get_redis()
        ws_key = f"notifications:workspace:minute:{workspace_id}:{str(kind)}"
        ws_count = await redis.incr(ws_key)
        if ws_count == 1:
            await redis.expire(ws_key, 60)
        if ws_count > settings.NOTIFICATIONS_WORKSPACE_EVENT_LIMIT_PER_MINUTE:
            logger.warning(
                "Workspace notification throttled "
                f"workspace_id={workspace_id} kind={kind} "
                f"limit={settings.NOTIFICATIONS_WORKSPACE_EVENT_LIMIT_PER_MINUTE}/minute"
            )
            return
    except Exception as exc:
        logger.warning(f"Workspace rate-limit store unavailable; proceeding with fan-out: {exc}")

    # 2. Bulk-fetch all user records in a single query (email, name, preferences)
    users_result = await db.execute(
        select(User.id, User.email, User.full_name, User.preferences).where(
            User.id.in_(member_ids),
            User.is_active == True,  # noqa: E712
        )
    )
    user_rows = users_result.all()

    # Build lookup dict: user_id -> (email, name, notif_prefs)
    user_data: dict[UUID, tuple[str, str, dict]] = {}
    for row in user_rows:
        merged_prefs = {**DEFAULT_PREFERENCES, **(row.preferences or {})}
        notif_prefs = merged_prefs.get("notifications", {})
        user_data[row.id] = (row.email, row.full_name or row.email, notif_prefs)

    # 3. Fan out — sequential dispatch avoids unsafe concurrent access
    # to a shared AsyncSession and allows one commit at the end.
    for uid in member_ids:
        if uid not in user_data:
            continue
        u_email, u_name, n_prefs = user_data[uid]
        await dispatch_event(
            db=db,
            user_id=uid,
            kind=kind,
            title=title,
            body=body,
            action_url=action_url,
            workspace_id=workspace_id,
            meta=meta,
            email_subject=email_subject,
            email_html=email_html,
            # Pass pre-fetched data to skip redundant queries
            _user_email=u_email,
            _user_name=u_name,
            _notif_prefs=n_prefs,
            _autocommit=False,
            event_id=event_id,
        )

    await db.commit()


# ---------------------------------------------------------------------------
# Inbox CRUD
# ---------------------------------------------------------------------------

async def list_notifications(
    db: AsyncSession,
    user_id: UUID,
    unread_only: bool = False,
    include_deleted: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> list[Notification]:
    q = select(Notification).where(Notification.user_id == user_id)

    if not include_deleted:
        q = q.where(Notification.deleted_at.is_(None))

    if unread_only:
        q = q.where(Notification.read == False)  # noqa: E712

    # Pinned items always float to the top, then sort by date
    q = q.order_by(
        Notification.is_pinned.desc(),
        Notification.created_at.desc(),
    ).offset(offset).limit(limit)

    result = await db.execute(q)
    return list(result.scalars().all())


async def unread_count(db: AsyncSession, user_id: UUID) -> int:
    from sqlalchemy import func
    result = await db.execute(
        select(func.count()).where(
            Notification.user_id == user_id,
            Notification.read == False,  # noqa: E712
            Notification.deleted_at.is_(None),
        )
    )
    return result.scalar() or 0


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/notifications", tags=["notifications"])


class NotificationResponse(BaseModel):
    id: UUID
    kind: str
    title: str
    body: str | None
    action_url: str | None
    read: bool
    is_pinned: bool
    created_at: str
    deleted_at: str | None


@router.get("", response_model=list[NotificationResponse])
async def list_user_notifications(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    unread_only: bool = False,
    include_deleted: bool = False,
    limit: int = 50,
    offset: int = 0,
):
    """
    UI CONSIDERATION:
    By default returns active notifications.
    Pass include_deleted=true to render the "Archive/Trash" view.
    Pinned items are automatically sorted to the top.
    """
    notifications = await list_notifications(
        db,
        user.id,
        unread_only=unread_only,
        include_deleted=include_deleted,
        limit=limit,
        offset=offset,
    )
    return [
        NotificationResponse(
            id=n.id,
            kind=n.kind,
            title=n.title,
            body=n.body,
            action_url=n.action_url,
            read=n.read,
            is_pinned=n.is_pinned,
            created_at=n.created_at.isoformat(),
            deleted_at=n.deleted_at.isoformat() if n.deleted_at else None,
        )
        for n in notifications
    ]


@router.get("/unread-count")
async def get_unread_count(user: CurrentUser, db: AsyncSession = Depends(get_db)):
    count = await unread_count(db, user.id)
    return {"unread": count}


@router.patch("/{notification_id}/read")
async def mark_notification_read(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user.id)
        .values(read=True)
    )
    await db.commit()
    if result.rowcount == 0:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"read": True}


@router.patch("/{notification_id}/pin")
async def toggle_notification_pin(
    notification_id: UUID,
    pin_state: bool,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user.id)
        .values(is_pinned=pin_state)
    )
    await db.commit()
    if result.rowcount == 0:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"is_pinned": pin_state}


@router.delete("/{notification_id}")
async def soft_delete_user_notification(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    UI CONSIDERATION:
    This moves the item to the Archive. Provide a toast with an "Undo" button
    that calls the /restore endpoint within ~5 seconds.
    """
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user.id)
        .values(deleted_at=datetime.now(timezone.utc))
    )
    await db.commit()
    if result.rowcount == 0:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"archived": True}


@router.patch("/{notification_id}/restore")
async def restore_user_notification(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user.id)
        .values(deleted_at=None)
    )
    await db.commit()
    if result.rowcount == 0:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"restored": True}


@router.delete("/{notification_id}/hard", status_code=status.HTTP_204_NO_CONTENT)
async def hard_delete_user_notification(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    UI CONSIDERATION:
    Only use this when emptying the trash/archive. Permanent, non-recoverable deletion.
    Show a confirmation dialog before calling this endpoint.
    """
    notif = await db.get(Notification, notification_id)
    if not notif or notif.user_id != user.id:
        raise HTTPException(status_code=404, detail="Notification not found.")
    await db.delete(notif)
    await db.commit()

# worker/celery.py
"""
Celery application, task definitions, and Beat schedule.

Changes from original
---------------------
GAP-10-4  Dynamic backup scheduling
    dispatch_scheduled_backups replaces the old static example entry.
    Runs every minute, queries BackupSchedule rows whose next_run has
    passed, updates next_run immediately (prevents double-dispatch across
    workers), then fires scheduled_backup per connection.

    scheduled_backup now accepts fmt, schema_only, schedule_id params
    and writes last_run / last_backup_id back to the BackupSchedule row
    on completion.

    task_routes and beat_schedule updated accordingly.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from kombu import Queue

from core.config import settings

logger = get_task_logger(__name__)

# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

celery_app = Celery(
    "calyphant",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["worker.celery"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_routes={
        "worker.celery.keep_alive_ping": {"queue": "keep_alive"},
        "worker.celery.run_all_keep_alive_pings": {"queue": "keep_alive"},
        "worker.celery.scheduled_backup": {"queue": "backups"},
        "worker.celery.dispatch_scheduled_backups": {"queue": "maintenance"},
        "worker.celery.drain_slow_queries": {"queue": "maintenance"},
        "worker.celery.expire_invites": {"queue": "maintenance"},
        "worker.celery.cleanup_old_backups": {"queue": "maintenance"},
        # Platform catalogue tasks
        "platform.catalogue.enrich_extension_async": {"queue": "maintenance"},
        "platform.catalogue.refresh_stale_entries": {"queue": "maintenance"},
    },
    task_queues=(
        Queue("keep_alive"),
        Queue("backups"),
        Queue("maintenance"),
        Queue("default"),
    ),
    task_default_queue="default",
    task_max_retries=3,
    task_default_retry_delay=60,
)

# ---------------------------------------------------------------------------
# Beat schedule
# ---------------------------------------------------------------------------

celery_app.conf.beat_schedule = {
    # Ping all keep-alive-enabled connections every 4 minutes
    "keep-alive-pings": {
        "task": "worker.celery.run_all_keep_alive_pings",
        "schedule": crontab(minute="*/4"),
    },
    # Check for due backup schedules every minute
    "dispatch-scheduled-backups": {
        "task": "worker.celery.dispatch_scheduled_backups",
        "schedule": crontab(minute="*"),
    },
    # Clean up expired invites daily at 02:00 UTC
    "expire-invites": {
        "task": "worker.celery.expire_invites",
        "schedule": crontab(hour=2, minute=0),
    },
    # Enforce backup retention limits daily at 03:00 UTC
    "cleanup-old-backups": {
        "task": "worker.celery.cleanup_old_backups",
        "schedule": crontab(hour=3, minute=0),
    },
    # Drain slow query stats every 15 minutes
    "drain-slow-queries": {
        "task": "worker.celery.drain_slow_queries",
        "schedule": crontab(minute="*/15"),
    },
    # Refresh stale extension catalogue entries nightly at 04:00 UTC
    "refresh-extension-catalogue": {
        "task": "platform.catalogue.refresh_stale_entries",
        "schedule": crontab(hour=4, minute=0),
    },
}

# ---------------------------------------------------------------------------
# Register platform catalogue tasks
# ---------------------------------------------------------------------------

from domains.platform.extension_catalogue import register_catalogue_tasks
register_catalogue_tasks(celery_app)


# ---------------------------------------------------------------------------
# Async runner helper
# ---------------------------------------------------------------------------

def _run(coro):
    """Run an async coroutine from a sync Celery task."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Keep-alive tasks  (unchanged)
# ---------------------------------------------------------------------------

@celery_app.task(
    name="worker.celery.keep_alive_ping",
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def keep_alive_ping(self, connection_id: str, encrypted_url: str):
    """Ping a single connection to prevent cloud databases from sleeping."""
    async def _ping():
        from core.db import init_db, decrypt_secret, _session_factory
        from domains.connections.service import ping_connection, mark_connection_status
        from domains.connections.models import ConnectionStatus

        url = decrypt_secret(encrypted_url)
        success = await ping_connection(url)

        if not _session_factory:
            return
        async with _session_factory() as db:
            status = ConnectionStatus.active if success else ConnectionStatus.unreachable
            await mark_connection_status(
                db,
                UUID(connection_id),
                status,
                error=None if success else "Keep-alive ping failed.",
            )
        logger.info(f"Keep-alive ping: connection={connection_id} success={success}")

    try:
        _run(_ping())
    except Exception as exc:
        logger.error(f"Keep-alive ping failed: {exc}")
        self.retry(exc=exc)


@celery_app.task(name="worker.celery.run_all_keep_alive_pings")
def run_all_keep_alive_pings():
    """Beat-triggered: find all keep-alive-enabled connections and dispatch pings."""
    async def _dispatch():
        from core.db import _session_factory
        from domains.connections.models import Connection
        from sqlalchemy import select

        if not _session_factory:
            return
        async with _session_factory() as db:
            result = await db.execute(
                select(Connection.id, Connection.encrypted_url).where(
                    Connection.keep_alive_enabled == True,  # noqa
                    Connection.is_active == True,           # noqa
                )
            )
            connections = result.all()

        for conn_id, enc_url in connections:
            keep_alive_ping.apply_async( #type: ignore
                args=[str(conn_id), enc_url],
                queue="keep_alive",
            )
        logger.info(f"Dispatched {len(connections)} keep-alive pings")

    _run(_dispatch())


# ---------------------------------------------------------------------------
# GAP-10-4  dispatch_scheduled_backups — runs every minute via beat
# ---------------------------------------------------------------------------

@celery_app.task(name="worker.celery.dispatch_scheduled_backups")
def dispatch_scheduled_backups():
    """
    Beat-triggered every minute.

    Queries BackupSchedule for rows whose next_run <= now and whose
    enabled=True.  For each due schedule:
      1. Advances next_run immediately (before dispatching) to prevent
         double-dispatch when multiple beat workers are running.
      2. Fires a scheduled_backup task.

    Any schedule with an invalid cron expression is disabled in place
    and logged as an error rather than crashing the whole task.
    """
    async def _dispatch():
        from core.db import _session_factory
        from domains.backups.engine import BackupSchedule, get_due_schedules, compute_next_run

        if not _session_factory:
            return

        async with _session_factory() as db:
            due = await get_due_schedules(db)
            if not due:
                return

            dispatched = 0
            for schedule in due:
                # Advance next_run before dispatching — prevents double-fire
                try:
                    schedule.next_run = compute_next_run(schedule.cron)
                except Exception as exc:
                    logger.error(
                        f"Invalid cron '{schedule.cron}' on schedule {schedule.id}: {exc}. "
                        "Disabling schedule to prevent repeated errors."
                    )
                    schedule.enabled = False
                    continue

                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                label = schedule.label_template.replace("{date}", date_str)

                scheduled_backup.apply_async( #type: ignore
                    args=[
                        str(schedule.connection_id),
                        str(schedule.workspace_id),
                        label,
                        schedule.format,
                        schedule.schema_only,
                        str(schedule.id),
                    ],
                    queue="backups",
                )
                dispatched += 1

            await db.commit()

            if dispatched:
                logger.info(f"dispatch_scheduled_backups: dispatched {dispatched} backup task(s)")

    _run(_dispatch())


# ---------------------------------------------------------------------------
# GAP-10-4  scheduled_backup — updated signature, writes back to schedule
# ---------------------------------------------------------------------------

@celery_app.task(
    name="worker.celery.scheduled_backup",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def scheduled_backup(
    self,
    connection_id: str,
    workspace_id: str,
    label: str,
    fmt: str = "calyph",
    schema_only: bool = False,
    schedule_id: str | None = None,
):
    """
    Run a backup for a connection.

    On completion, updates BackupSchedule.last_run and last_backup_id
    so the schedule history is always visible.  Failures update
    last_run too (so the schedule doesn't appear frozen) and let
    the retry mechanism handle re-attempts.
    """
    async def _backup():
        import asyncpg
        from core.db import _session_factory, get_connection_url
        from domains.backups.engine import create_backup, BackupSchedule
        from sqlalchemy import select

        if not _session_factory:
            logger.error("scheduled_backup: session factory not ready")
            return

        async with _session_factory() as db:
            url = await get_connection_url(db, UUID(connection_id), UUID(workspace_id))
            if not url:
                logger.warning(f"scheduled_backup: connection {connection_id} not found")
                return

            pg_conn = await asyncpg.connect(dsn=url, timeout=30)
            try:
                record = await create_backup(
                    db=db,
                    pg_conn=pg_conn,
                    connection_id=UUID(connection_id),
                    workspace_id=UUID(workspace_id),
                    label=label,
                    fmt=fmt,
                    schema_only=schema_only,
                )
            finally:
                await pg_conn.close()

            logger.info(
                f"Scheduled backup completed: "
                f"connection={connection_id} backup={record.id} status={record.status}"
            )

            # Write back to the schedule row regardless of success/failure
            if schedule_id:
                try:
                    result = await db.execute(
                        select(BackupSchedule).where(
                            BackupSchedule.id == UUID(schedule_id)
                        )
                    )
                    schedule = result.scalar_one_or_none()
                    if schedule:
                        schedule.last_run = datetime.now(timezone.utc)
                        if record.status == "completed":
                            schedule.last_backup_id = record.id
                        await db.commit()
                except Exception as exc:
                    logger.warning(
                        f"Could not update BackupSchedule {schedule_id} after backup: {exc}"
                    )

    try:
        _run(_backup())
    except Exception as exc:
        logger.error(f"Scheduled backup failed: connection={connection_id} error={exc}")
        self.retry(exc=exc)


# ---------------------------------------------------------------------------
# Maintenance tasks  (unchanged from original)
# ---------------------------------------------------------------------------

@celery_app.task(name="worker.celery.expire_invites")
def expire_invites():
    """Remove expired workspace invites."""
    async def _expire():
        from core.db import _session_factory
        from domains.teams.service import WorkspaceInvite
        from sqlalchemy import delete

        
        if not _session_factory:
            return
        async with _session_factory() as db:
            result = await db.execute(
                delete(WorkspaceInvite).where(
                    WorkspaceInvite.accepted == False,  # noqa
                    WorkspaceInvite.expires_at < datetime.now(timezone.utc),
                )
            )
            await db.commit()
            logger.info(f"Expired {result.rowcount} workspace invites") #type: ignore

    _run(_expire())


@celery_app.task(name="worker.celery.cleanup_old_backups")
def cleanup_old_backups():
    """Delete backups that exceed the user's tier retention limit."""
    async def _cleanup():
        from core.db import _session_factory
        from domains.backups.engine import BackupRecord, _r2_client
        from domains.billing.flutterwave import get_limits
        from domains.users.service import User
        from sqlalchemy import select
        import shared.storage as storage
        from datetime import timedelta

        if not _session_factory:
            return
        async with _session_factory() as db:
            users = (await db.execute(select(User.id, User.tier))).all()

            deleted = 0
            for user_id, tier in users:
                limits = get_limits(tier)
                retention_days = limits.get("backup_retention_days", 7)
                cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

                old_backups = (
                    await db.execute(
                        select(BackupRecord).where(
                            BackupRecord.workspace_id == user_id,
                            BackupRecord.created_at < cutoff,
                            BackupRecord.status == "completed",
                        )
                    )
                ).scalars().all()

                client = _r2_client()
                for backup in old_backups:
                    # Delete all parts
                    from domains.backups.engine import _r2_key
                    for part_idx in range(1, backup.part_count + 1):
                        part_num = part_idx if backup.part_count > 1 else None
                        key = _r2_key(
                            backup.workspace_id, backup.connection_id,
                            backup.id, backup.format, part_num,
                        )
                        try:
                            await asyncio.to_thread(
                                client.delete_object,
                                Bucket=settings.R2_BUCKET_NAME,
                                Key=key,
                            )
                        except Exception as exc:
                            logger.warning(f"Failed to delete R2 object {key}: {exc}")
                    await db.delete(backup)
                    deleted += 1

            await db.commit()
            logger.info(f"Cleanup: deleted {deleted} expired backups")

    _run(_cleanup())


@celery_app.task(name="worker.celery.drain_slow_queries")
def drain_slow_queries():
    """Snapshot pg_stat_statements for all active connections."""
    async def _drain():
        import asyncpg
        from core.db import _session_factory, get_connection_url
        from domains.connections.models import Connection
        from sqlalchemy import select

        if not _session_factory:
            return
        async with _session_factory() as db:
            result = await db.execute(
                select(Connection.id, Connection.workspace_id).where(
                    Connection.is_active == True  # noqa
                ).limit(100)
            )
            connections = result.all()

        for conn_id, workspace_id in connections:
            async with _session_factory() as db:
                url = await get_connection_url(db, conn_id, workspace_id)
                if not url:
                    continue
            try:
                pg_conn = await asyncpg.connect(dsn=url, timeout=10)
                try:
                    await pg_conn.fetchval(
                        "SELECT count(*) FROM pg_stat_statements LIMIT 1"
                    )
                except asyncpg.UndefinedTableError:
                    pass
                finally:
                    await pg_conn.close()
            except Exception:
                pass

    _run(_drain())

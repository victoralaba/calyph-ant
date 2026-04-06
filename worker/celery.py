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

from celery import Celery, shared_task
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from kombu import Queue

import socket
import subprocess
import time
import threading
from typing import Any, Dict

try:
    import docker #type: ignore
except ImportError:
    docker = None

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
        "worker.celery.execute_background_backup": {"queue": "backups"},
        "worker.celery.execute_background_restore": {"queue": "backups"},
        "worker.celery.scheduled_backup": {"queue": "backups"},
        "worker.celery.dispatch_scheduled_backups": {"queue": "maintenance"},
        "worker.celery.drain_slow_queries": {"queue": "maintenance"},
        "worker.celery.expire_invites": {"queue": "maintenance"},
        "worker.celery.cleanup_old_backups": {"queue": "maintenance"},
        "worker.celery.execute_ephemeral_semantic_diff": {"queue": "maintenance"},
        "worker.celery.sync_google_schema_cache": {"queue": "maintenance"},
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
    # Sweep orphaned scratch schemas every 5 minutes
    "garbage-collect-scratch-schemas": {
        "task": "worker.celery.garbage_collect_scratch_schemas",
        "schedule": crontab(minute="*/5"),
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
        from core.db import _session_factory, get_redis
        from domains.backups.engine import BackupRecord, _r2_client
        from domains.billing.flutterwave import get_limits
        from domains.users.service import User
        from sqlalchemy import select
        import shared.storage as storage
        from datetime import timedelta

        if not _session_factory:
            return
            
        redis = await get_redis()
        
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
                            
                    # --- THE MUSCLE: Background Storage Reclamation ---
                    if backup.size_bytes:
                        try:
                            current = await redis.decrby(f"workspace_storage_bytes:{user_id}", backup.size_bytes)
                            if current < 0:
                                await redis.set(f"workspace_storage_bytes:{user_id}", 0)
                        except Exception as redis_exc:
                            logger.error(f"Failed to decrement Redis on cleanup: {redis_exc}")
                    # --------------------------------------------------
                    
                    await db.delete(backup)
                    deleted += 1

            await db.commit()
            logger.info(f"Cleanup: deleted {deleted} expired backups")

    _run(_cleanup())


# ---------------------------------------------------------------------------
# AI Schema Caching (Google Gemini Context)
# ---------------------------------------------------------------------------

@celery_app.task(name="worker.celery.sync_google_schema_cache")
def sync_google_schema_cache(connection_id: str, workspace_id: str, schema_name: str = "public"):
    """
    Background task to extract target database schema and push it to Google's 
    Cached Content API for zero-latency, zero-DB-hit AI queries.
    
    TTL is set to 24 hours. The frontend should trigger this task whenever 
    a user successfully connects a database or clicks a "Sync Schema" button.
    """
    async def _sync():
        import httpx
        from uuid import UUID
        from core.db import _session_factory, get_connection_url, get_redis
        from domains.schema.introspection import introspect_database
        from core.config import settings

        if not getattr(settings, "AI_AVAILABLE", False) or not settings.GOOGLE_GENAI_API_KEY:
            logger.info("Skipping Google Schema Cache: AI disabled or missing API key.")
            return

        if not _session_factory:
            return

        async with _session_factory() as db:
            url = await get_connection_url(db, UUID(connection_id), UUID(workspace_id))
            if not url:
                return

        # 1. Introspect Schema (Heavy lifting done in background)
        try:
            snapshot = await introspect_database(url, schema_name)
            
            # Format minimal DDL for the LLM
            lines = [f"-- PostgreSQL Schema: {schema_name}"]
            for table in snapshot.tables:
                if table.kind not in ("table", "view"): continue
                cols = ", ".join(f"{c.name} {c.data_type}" for c in table.columns)
                lines.append(f"CREATE {table.kind.upper()} {table.name} ({cols});")
                
            schema_text = "\n".join(lines)
            if not schema_text:
                return
                
        except Exception as e:
            logger.error(f"Failed to introspect DB for AI cache ({connection_id}): {e}")
            return

        # 2. Push to Google Cached Content API
        try:
            api_url = f"https://generativelanguage.googleapis.com/v1beta/cachedContents?key={settings.GOOGLE_GENAI_API_KEY}"
            payload = {
                "model": f"models/{settings.GOOGLE_GENAI_MODEL}",
                "contents": [{"role": "user", "parts": [{"text": schema_text}]}],
                # 86400s = 24 hours. Gemini limits cache TTL.
                "ttl": "86400s" 
            }
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(api_url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                cache_name = data.get("name") # e.g., cachedContents/xxxxxxxx
                
                # 3. Store reference in Redis to be picked up by domains/ai/providers.py
                if cache_name:
                    redis = await get_redis()
                    # Keep in sync with Google's TTL
                    await redis.setex(f"schema_cache:google:{connection_id}", 86400, cache_name)
                    logger.info(f"AI Schema Cached Successfully: {connection_id} -> {cache_name}")
                    
        except httpx.HTTPStatusError as e:
             logger.error(f"Google Cache API Error: {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to push schema to Google cache: {e}")

    _run(_sync())


# In worker/celery.py (Add to task_routes and beat_schedule if necessary, but default queue works)
@celery_app.task(name="worker.celery.build_index_background")
def build_index_background(
    connection_id: str,
    workspace_id: str,
    sql: str,
    workspace_role: str,
    index_name: str,
    table_name: str,
):
    """
    Executes heavy CREATE INDEX CONCURRENTLY queries.
    By definition, this must run OUTSIDE a transaction block.
    """
    async def _build():
        import asyncpg
        from core.db import _session_factory, get_connection_url
        
        if not _session_factory:
            return

        async with _session_factory() as db:
            url = await get_connection_url(db, UUID(connection_id), UUID(workspace_id))
            if not url:
                return

        pg_conn = await asyncpg.connect(dsn=url)
        try:
            # 1. Identity Lock: Assume tenant privileges
            await pg_conn.execute(f'SET ROLE "{workspace_role}";')
            
            # 2. Heavy Compute Lock: Allow 1 hour for massive vector/GIN indexes
            await pg_conn.execute("SET statement_timeout = '3600000';")
            
            # 3. Execute directly (NO transaction wrapper)
            logger.info(f"Starting background index build for {index_name}...")
            await pg_conn.execute(sql)
            logger.info(f"Background index build {index_name} completed.")
            
            # 4. Telemetry: Ping the frontend that the heavy build is done
            from domains.schema.router.editor import _broadcast_schema_changed
            await _broadcast_schema_changed(
                UUID(workspace_id), UUID(connection_id),
                change_kind="index_created",
                object_name=index_name, table_name=table_name,
            )
        except Exception as exc:
            logger.error(f"Background index build failed: {exc}")
            # Optional: Broadcast a failure event so the UI stops showing a spinner
        finally:
            await pg_conn.close()

    _run(_build())


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


# ---------------------------------------------------------------------------
# Ephemeral Schema Diffing Engine
# ---------------------------------------------------------------------------

import logging
logger = logging.getLogger(__name__)

# Initialize Docker client lazily or handle absence gracefully
docker_client = None
if docker:
    try:
        docker_client = docker.from_env()
    except Exception as exc:
        logger.warning(f"Failed to bind to local Docker daemon. Ephemeral diffs will fail: {exc}")


def _wait_for_db(host: str, port: int, timeout: int = 15) -> bool:
    """Ping the port until Postgres is ready to accept connections."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


# --- THE MUSCLE: Hardware Concurrency Semaphore ---
# Limit the worker to a maximum of 3 concurrent Docker Postgres containers.
# If 10 requests come in, 3 run, 7 wait safely without triggering an OOM kill.
DOCKER_SEMAPHORE = threading.Semaphore(3)

@shared_task(bind=True, time_limit=120, soft_time_limit=90)
def execute_ephemeral_diff(self, source_sql: str, target_db_url: str, schema: str = "public") -> Dict[str, Any]:
    """
    Spawns an ephemeral Postgres container, applies the source SQL, 
    runs migra against the target DB, and parses the AST.
    Guarded by a thread semaphore to prevent node OOM crashes.
    """
    if not docker_client:
        return {"sql": "", "changes": [], "has_destructive_changes": False, "error": "Docker daemon is not available on the worker node."}

    container = None
    
    # Acquire hardware lock. Block until a slot opens up.
    logger.info("Waiting for available Docker capacity slot...")
    acquired = DOCKER_SEMAPHORE.acquire(timeout=60)
    
    if not acquired:
        return {"sql": "", "changes": [], "has_destructive_changes": False, "error": "System is currently at maximum capacity for schema computations. Try again in a few moments."}

    try:
        logger.info("Slot acquired. Spawning ephemeral Postgres container for diff...")
        
        # 1. Boot the ephemeral container with strict resource limits
        container = docker_client.containers.run(
            "postgres:15-alpine",
            detach=True,
            remove=True,  # The Guillotine: Auto-delete volume and container on stop
            environment={"POSTGRES_PASSWORD": "ephemeral_pass"},
            ports={'5432/tcp': None},  # Bind to a random available host port
            mem_limit="256m",          # Reduced from 512m. AST diffs don't need half a gig of RAM.
            nano_cpus=500000000        # 0.5 CPU limit
        )
        
        # 2. Extract the random port
        container.reload()
        host_port = container.attrs['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
        ephemeral_url = f"postgresql://postgres:ephemeral_pass@localhost:{host_port}/postgres"
        
        # 3. Wait for Postgres to boot (~1-2 seconds for Alpine)
        if not _wait_for_db("localhost", int(host_port)):
            raise RuntimeError("Ephemeral Postgres container failed to boot within timeout.")

        # 4. Inject the user's schema into the ephemeral container
        inject_cmd = [
            "psql", ephemeral_url, "-c", 
            f"CREATE SCHEMA IF NOT EXISTS \"{schema}\"; SET search_path TO \"{schema}\"; {source_sql}"
        ]
        subprocess.run(inject_cmd, check=True, capture_output=True, text=True)

        # 5. Run migra directly inside the Celery worker
        migra_cmd = ["migra", "--unsafe", "--schema", schema, ephemeral_url, target_db_url]
        proc = subprocess.run(migra_cmd, capture_output=True, text=True)

        if proc.returncode not in (0, 2):
            raise RuntimeError(f"Migra failed: {proc.stderr}")
            
        sql_output = proc.stdout.strip() if proc.returncode == 2 else ""

        # 6. Parse the AST safely off the main web thread
        from domains.schema.diff import _parse_sql_to_changes, _DESTRUCTIVE_KINDS
        
        changes = _parse_sql_to_changes(sql_output)
        
        serialized_changes = [
            {
                "kind": c.kind.value,
                "object_type": c.object_type,
                "object_name": c.object_name,
                "table_name": c.table_name,
                "detail": c.detail,
                "is_destructive": c.is_destructive,
                "sql": c.sql
            } for c in changes
        ]
        
        has_destructive = any(c.kind in _DESTRUCTIVE_KINDS for c in changes)

        return {
            "sql": sql_output,
            "changes": serialized_changes,
            "has_destructive_changes": has_destructive,
            "error": None
        }

    except subprocess.CalledProcessError as exc:
        logger.error(f"SQL Injection to ephemeral DB failed: {exc.stderr}")
        return {"sql": "", "changes": [], "has_destructive_changes": False, "error": "Provided schema SQL contains syntax errors."}
    except Exception as exc:
        logger.error(f"Ephemeral diff failed: {exc}")
        return {"sql": "", "changes": [], "has_destructive_changes": False, "error": str(exc)}
        
    finally:
        # 7. Guaranteed Destruction
        if container:
            try:
                container.kill()
            except Exception:
                pass
        
        # Release hardware lock regardless of success or failure
        DOCKER_SEMAPHORE.release()


@celery_app.task(name="worker.celery.execute_background_ephemeral_diff")
def execute_background_ephemeral_diff(
    job_id: str, 
    source_sql: str, 
    target_db_url: str, 
    schema: str, 
    live_label: str = "live", 
    sql_label: str = "provided schema"
):
    """
    Asynchronous state-machine wrapper for ephemeral schema diffing.
    Executes the AST parsing in the background and updates Redis state.
    """
    import json
    
    async def _process():
        from core.db import get_redis
        redis = await get_redis()
        state_key = f"diff_job:{job_id}"
        
        async def update_state(job_status: str, result=None, error=None):
            state = {
                "job_id": job_id,
                "status": job_status,
                "result": result,
                "error": error
            }
            await redis.setex(state_key, 3600, json.dumps(state))

        await update_state("PROCESSING")

        try:
            # Call the bound Celery task directly in-process. 
            # We pass None for 'self' since execute_ephemeral_diff doesn't use it.
            result_dict = execute_ephemeral_diff(
                None, 
                source_sql=source_sql, 
                target_db_url=target_db_url, 
                schema=schema
            )
            
            if result_dict.get("error"):
                await update_state("FAILED", error=result_dict["error"])
                return

            changes = result_dict.get("changes", [])
            has_destructive = result_dict.get("has_destructive_changes", False)
            
            # Reconstruct the summary matching DiffResult expectations
            if not changes:
                summary = "Schemas are identical."
            else:
                counts = {}
                for c in changes:
                    counts[c["kind"]] = counts.get(c["kind"], 0) + 1
                parts = [f"{count} {kind.replace('_', ' ')}" for kind, count in sorted(counts.items())]
                dest = " ⚠ Contains destructive changes." if has_destructive else ""
                summary = f"{len(changes)} changes: {', '.join(parts)}.{dest}"

            # Format the final HTTP payload
            final_result = {
                "source_label": live_label,
                "target_label": sql_label,
                "changes": changes,
                "sql": result_dict.get("sql", ""),
                "has_destructive_changes": has_destructive,
                "summary": summary,
                "diff_source": "schema_only"
            }
            
            await update_state("COMPLETED", result=final_result)
            
        except Exception as e:
            await update_state("FAILED", error=f"Ephemeral diff engine failure: {str(e)}")

    _run(_process())

# ---------------------------------------------------------------------------
# Background Diff Engine & Garbage Collector
# ---------------------------------------------------------------------------

@celery_app.task(name="worker.celery.execute_background_diff")
def execute_background_diff(job_id: str, source_url: str, target_url: str, schema: str, timeout_ms: int):
    """
    Executes the diffing engine against live databases. 
    Updates Redis state machine at every transition.
    """
    import json
    async def _process():
        from core.db import get_redis
        from domains.schema.diff import diff_schemas
        
        redis = await get_redis()
        state_key = f"diff_job:{job_id}"
        
        async def update_state(job_status: str, result=None, error=None):
            state = {
                "job_id": job_id,
                "status": job_status,
                "result": result,
                "error": error
            }
            await redis.setex(state_key, 3600, json.dumps(state))

        await update_state("PROCESSING")

        try:
            # Execute with physics lock enforced inside the diff module
            # We intercept diff_schemas directly since it wraps _run_migra
            diff_result = await diff_schemas(
                source_url=source_url, 
                target_url=target_url, 
                schema=schema
            )
            
            if diff_result.error:
                await update_state("FAILED", error=diff_result.error)
                return

            # Serialize dataclasses to dicts for HTTP delivery
            serialized = [
                {
                    "kind": c.kind.value,
                    "object_type": c.object_type,
                    "object_name": c.object_name,
                    "table_name": c.table_name,
                    "detail": c.detail,
                    "is_destructive": c.is_destructive,
                    "sql": c.sql
                } for c in diff_result.changes
            ]
            
            await update_state("COMPLETED", result=serialized)
            
        except Exception as e:
            if "timeout" in str(e).lower():
                await update_state("TIMEOUT", error="Execution exceeded tier time limits.")
            else:
                await update_state("FAILED", error=str(e))

    _run(_process())


@celery_app.task(name="worker.celery.execute_background_backup")
def execute_background_backup(record_id: str, connection_id: str, workspace_id: str, label: str, fmt: str, schema_only: bool):
    """
    Executes a user-initiated backup strictly outside the API lifecycle.
    Binds the generated physics data to the pending BackupRecord.
    """
    async def _run_backup():
        import asyncpg
        from core.db import _session_factory, get_connection_url
        from domains.backups.engine import create_backup
        from uuid import UUID

        if not _session_factory:
            return

        async with _session_factory() as db:
            url = await get_connection_url(db, UUID(connection_id), UUID(workspace_id))
            if not url:
                return

            pg_conn = await asyncpg.connect(dsn=url, timeout=15)
            try:
                await create_backup(
                    db=db,
                    pg_conn=pg_conn,
                    connection_id=UUID(connection_id),
                    workspace_id=UUID(workspace_id),
                    label=label,
                    fmt=fmt,
                    schema_only=schema_only,
                    existing_record_id=UUID(record_id)
                )
            except Exception as e:
                logger.error(f"Background Backup Task {record_id} failed: {e}")
            finally:
                await pg_conn.close()

    _run(_run_backup())


@celery_app.task(name="worker.celery.execute_background_restore")
def execute_background_restore(
    job_id: str, 
    connection_id: str, 
    backup_id: str, 
    workspace_id: str, 
    strategy: str | None, 
    per_row_overrides: dict | None, 
    restore_session_id: str | None
):
    """
    Executes heavy schema inserts, conflict detection, and restores 
    outside the API lifecycle. Updates the Redis state machine dynamically.
    """
    import json
    async def _run_restore():
        import asyncpg
        from core.db import _session_factory, get_connection_url, get_redis
        from domains.backups.engine import restore_backup
        from uuid import UUID

        redis = await get_redis()
        state_key = f"restore_job:{job_id}"

        async def update_state(status: str, result=None, error=None):
            state = {"job_id": job_id, "status": status, "result": result, "error": error}
            await redis.setex(state_key, 3600, json.dumps(state))

        await update_state("PROCESSING")

        if not _session_factory:
            await update_state("FAILED", error="Database factory unavailable.")
            return

        async with _session_factory() as db:
            url = await get_connection_url(db, UUID(connection_id), UUID(workspace_id))
            if not url:
                await update_state("FAILED", error="Target database connection not found.")
                return

            pg_conn = await asyncpg.connect(dsn=url, timeout=15)
            try:
                result = await restore_backup(
                    db=db,
                    pg_conn=pg_conn,
                    backup_id=UUID(backup_id),
                    workspace_id=UUID(workspace_id),
                    strategy=strategy,
                    per_row_overrides=per_row_overrides,
                    restore_session_id=restore_session_id
                )
                await update_state("COMPLETED", result=result)
            except Exception as e:
                logger.error(f"Background Restore Task {job_id} failed: {e}")
                await update_state("FAILED", error=str(e))
            finally:
                await pg_conn.close()

    _run(_run_restore())


@celery_app.task(name="worker.celery.garbage_collect_scratch_schemas")
def garbage_collect_scratch_schemas():
    """
    The Garbage Truck. Runs via Celery Beat every 5 minutes.
    Indiscriminately terminates any schema matching the signature older than 15 minutes.
    """
    async def _sweep():
        import time
        from sqlalchemy import text
        from core.db import _session_factory
        
        if not _session_factory:
            return

        now = int(time.time())
        
        async with _session_factory() as session:
            try:
                # 1. Single-sweep introspection
                result = await session.execute(
                    text("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '_calyphant_diff_%'")
                )
                schemas = [row[0] for row in result.fetchall()]
                
                # 2. Parse and execute drops
                for schema in schemas:
                    parts = schema.split('_')
                    try:
                        # Format: _calyphant_diff_{timestamp}_{uuid}
                        created_at = int(parts[3])
                        if now - created_at > 900:  # 15 minutes grace period
                            await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                    except (IndexError, ValueError):
                        # Ruthless fallback: If it matches prefix but violates timestamp, nuke it.
                        await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                
                await session.commit()
                logger.info(f"Garbage Collector swept {len(schemas)} scratch schemas.")
            except Exception as e:
                await session.rollback()
                logger.error(f"Garbage Collector failed: {e}")

    _run(_sweep())


@celery_app.task(name="worker.celery.execute_ephemeral_semantic_diff")
def execute_ephemeral_semantic_diff(
    scratch_url: str, 
    migrations_up_to_from: list[str], 
    migrations_up_to_to: list[str], 
    schema_name: str
) -> dict:
    """
    Executes a semantic schema diff inside an ephemeral schema, entirely 
    offloaded from the FastAPI event loop.
    """
    import asyncio
    
    # We must run this in a new local event loop because Celery tasks are sync wrappers
    def _run_sync():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_async_semantic_diff(
                scratch_url, migrations_up_to_from, migrations_up_to_to, schema_name
            ))
        finally:
            loop.close()
            
    return _run_sync()


async def _async_semantic_diff(scratch_url: str, migrations_from: list[str], migrations_to: list[str], schema_name: str) -> dict:
    import asyncpg
    from uuid import uuid4
    from domains.schema.diff import run_migra_on_schemas, _parse_sql_to_changes, _DESTRUCTIVE_KINDS
    from domains.schema.auto_migration import _rewrite_for_schema
    
    schema_a = f"_calyphant_diff_a_{uuid4().hex[:8]}"
    schema_b = f"_calyphant_diff_b_{uuid4().hex[:8]}"

    conn = None
    try:
        # Enforce physics: Timeout to prevent zombie connections
        conn = await asyncpg.connect(dsn=scratch_url, timeout=15)
        await conn.execute(f'CREATE SCHEMA "{schema_a}"')
        await conn.execute(f'CREATE SCHEMA "{schema_b}"')

        # Replay "from" state into schema_a
        for sql in migrations_from:
            rewritten_sql = _rewrite_for_schema(sql, schema_name, schema_a)
            await conn.execute(f'SET search_path TO "{schema_a}", public')
            await conn.execute(rewritten_sql)
            await conn.execute("RESET search_path")

        # Replay "to" state into schema_b
        for sql in migrations_to:
            rewritten_sql = _rewrite_for_schema(sql, schema_name, schema_b)
            await conn.execute(f'SET search_path TO "{schema_b}", public')
            await conn.execute(rewritten_sql)
            await conn.execute("RESET search_path")

        # Run migra via subprocess inside the worker
        diff_sql = await run_migra_on_schemas(scratch_url, schema_a, schema_b)

        if diff_sql is None:
            return {"sql": "", "changes": [], "has_destructive_changes": False}

        changes = _parse_sql_to_changes(diff_sql)
        has_destructive = any(c.kind in _DESTRUCTIVE_KINDS for c in changes)

        return {
            "sql": diff_sql,
            "has_destructive_changes": has_destructive,
            "changes": [
                {
                    "kind": c.kind.value,
                    "object_type": c.object_type,
                    "object_name": c.object_name,
                    "table_name": c.table_name,
                    "detail": c.detail,
                    "is_destructive": c.is_destructive,
                    "sql": c.sql
                } for c in changes
            ]
        }

    except Exception as exc:
        return {"error": str(exc)}
    finally:
        # Guaranteed cleanup of ephemeral resources
        if conn:
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_a}" CASCADE')
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE')
            except Exception:
                pass
            await conn.close()
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings


@dataclass
class ArchiveStats:
    notifications_archived: int = 0
    audit_logs_archived: int = 0
    notifications_pruned: int = 0
    audit_logs_pruned: int = 0


def _dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


async def _stream_to_jsonl_gzip(
    db: AsyncSession,
    *,
    window_start: datetime,
    window_end: datetime,
    out_path: Path,
    source: str,
) -> int:
    from domains.admin.service import AuditLog
    from domains.notifications.service import Notification
    from domains.users.service import User

    rows = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(out_path, "wt", encoding="utf-8") as gz:
        if source == "notifications":
            stmt = (
                select(
                    Notification.id,
                    Notification.user_id,
                    Notification.workspace_id,
                    Notification.kind,
                    Notification.title,
                    Notification.body,
                    Notification.action_url,
                    Notification.read,
                    Notification.is_pinned,
                    Notification.meta,
                    Notification.created_at,
                    Notification.deleted_at,
                    User.tier,
                )
                .join(User, User.id == Notification.user_id, isouter=True)
                .where(
                    Notification.created_at >= window_start,
                    Notification.created_at < window_end,
                )
                .order_by(Notification.created_at.asc())
                .execution_options(yield_per=1000)
            )
            async for row in await db.stream(stmt):
                payload = {
                    "id": str(row.id),
                    "user_id": str(row.user_id),
                    "workspace_id": str(row.workspace_id) if row.workspace_id else None,
                    "kind": row.kind,
                    "title": row.title,
                    "body": row.body,
                    "action_url": row.action_url,
                    "read": row.read,
                    "is_pinned": row.is_pinned,
                    "meta": row.meta or {},
                    "created_at": _dt(row.created_at),
                    "deleted_at": _dt(row.deleted_at),
                    "tier": row.tier or "free",
                }
                gz.write(json.dumps(payload, default=str) + "\n")
                rows += 1
        elif source == "audit_logs":
            stmt = (
                select(
                    AuditLog.id,
                    AuditLog.user_id,
                    AuditLog.workspace_id,
                    AuditLog.action,
                    AuditLog.resource_type,
                    AuditLog.resource_id,
                    AuditLog.ip_address,
                    AuditLog.user_agent,
                    AuditLog.diff,
                    AuditLog.meta,
                    AuditLog.created_at,
                    User.tier,
                )
                .join(User, User.id == AuditLog.user_id, isouter=True)
                .where(
                    AuditLog.created_at >= window_start,
                    AuditLog.created_at < window_end,
                )
                .order_by(AuditLog.created_at.asc())
                .execution_options(yield_per=1000)
            )
            async for row in await db.stream(stmt):
                payload = {
                    "id": str(row.id),
                    "user_id": str(row.user_id) if row.user_id else None,
                    "workspace_id": str(row.workspace_id) if row.workspace_id else None,
                    "action": row.action,
                    "resource_type": row.resource_type,
                    "resource_id": row.resource_id,
                    "ip_address": row.ip_address,
                    "user_agent": row.user_agent,
                    "diff": row.diff or {},
                    "meta": row.meta or {},
                    "created_at": _dt(row.created_at),
                    "tier": row.tier or "free",
                }
                gz.write(json.dumps(payload, default=str) + "\n")
                rows += 1
        else:
            raise ValueError(f"Unsupported archive source: {source}")

    return rows


async def _upload_and_verify_md5(path: Path, key: str) -> bool:
    from shared.storage import _get_client

    client = _get_client()
    payload = await asyncio.to_thread(path.read_bytes)
    md5_hex = hashlib.md5(payload).hexdigest()

    response = await asyncio.to_thread(
        client.put_object,
        Bucket=settings.R2_BUCKET_NAME,
        Key=key,
        Body=payload,
        ContentType="application/gzip",
    )
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        logger.error(f"Archive upload failed status={status} key={key}")
        return False

    head = await asyncio.to_thread(
        client.head_object,
        Bucket=settings.R2_BUCKET_NAME,
        Key=key,
    )
    etag = str(head.get("ETag", "")).strip('"')
    if etag != md5_hex:
        logger.error(f"Archive MD5 mismatch key={key} local={md5_hex} remote={etag}")
        return False
    return True


async def _prune_archived_window(
    db: AsyncSession,
    *,
    archive_until: datetime,
) -> tuple[int, int]:
    from domains.admin.service import AuditLog
    from domains.notifications.service import Notification
    from domains.users.service import User

    now = datetime.now(timezone.utc)
    free_cutoff = now - timedelta(days=30)
    pro_cutoff = now - timedelta(days=90)
    team_cutoff = now - timedelta(days=365)

    tiers_enterprise = ("team", "enterprise")

    notif_deleted = 0
    audit_deleted = 0

    notif_free = await db.execute(
        delete(Notification)
        .where(Notification.user_id == User.id)
        .where(User.tier == "free")
        .where(Notification.created_at < free_cutoff)
        .where(Notification.created_at <= archive_until)
    )
    notif_deleted += notif_free.rowcount or 0

    notif_pro = await db.execute(
        delete(Notification)
        .where(Notification.user_id == User.id)
        .where(User.tier == "pro")
        .where(Notification.created_at < pro_cutoff)
        .where(Notification.created_at <= archive_until)
    )
    notif_deleted += notif_pro.rowcount or 0

    notif_team = await db.execute(
        delete(Notification)
        .where(Notification.user_id == User.id)
        .where(User.tier.in_(tiers_enterprise))
        .where(Notification.created_at < team_cutoff)
        .where(Notification.created_at <= archive_until)
    )
    notif_deleted += notif_team.rowcount or 0

    audit_free = await db.execute(
        delete(AuditLog)
        .where(AuditLog.user_id == User.id)
        .where(User.tier == "free")
        .where(AuditLog.created_at < free_cutoff)
        .where(AuditLog.created_at <= archive_until)
    )
    audit_deleted += audit_free.rowcount or 0

    audit_pro = await db.execute(
        delete(AuditLog)
        .where(AuditLog.user_id == User.id)
        .where(User.tier == "pro")
        .where(AuditLog.created_at < pro_cutoff)
        .where(AuditLog.created_at <= archive_until)
    )
    audit_deleted += audit_pro.rowcount or 0

    audit_team = await db.execute(
        delete(AuditLog)
        .where(AuditLog.user_id == User.id)
        .where(User.tier.in_(tiers_enterprise))
        .where(AuditLog.created_at < team_cutoff)
        .where(AuditLog.created_at <= archive_until)
    )
    audit_deleted += audit_team.rowcount or 0

    audit_system = await db.execute(
        delete(AuditLog)
        .where(AuditLog.user_id.is_(None))
        .where(AuditLog.created_at < team_cutoff)
        .where(AuditLog.created_at <= archive_until)
    )
    audit_deleted += audit_system.rowcount or 0

    await db.commit()
    return notif_deleted, audit_deleted


async def run_monthly_archive_and_prune(db: AsyncSession) -> ArchiveStats:
    from core.db import get_redis

    now = datetime.now(timezone.utc)
    window_end = now - timedelta(hours=24)

    redis = None
    last_run_raw: bytes | None = None
    try:
        redis = await get_redis()
        last_run_raw = await redis.get("notifications:archive:last_run")
    except Exception as exc:
        logger.warning(f"Archive cursor store unavailable, using default overlap window: {exc}")

    if last_run_raw:
        last_run = datetime.fromisoformat(last_run_raw.decode("utf-8"))
        window_start = last_run - timedelta(hours=24)
    else:
        window_start = window_end - timedelta(days=31)

    if window_start >= window_end:
        logger.info("Archive window is empty; skipping notification lifecycle run")
        return ArchiveStats()

    year = window_end.strftime("%Y")
    month = window_end.strftime("%m")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    with tempfile.TemporaryDirectory(prefix="calyph-notif-archive-") as tmp:
        tmp_path = Path(tmp)
        notif_file = tmp_path / f"notifications_{ts}.jsonl.gz"
        audit_file = tmp_path / f"audit_logs_{ts}.jsonl.gz"

        notif_rows = await _stream_to_jsonl_gzip(
            db,
            window_start=window_start,
            window_end=window_end,
            out_path=notif_file,
            source="notifications",
        )
        audit_rows = await _stream_to_jsonl_gzip(
            db,
            window_start=window_start,
            window_end=window_end,
            out_path=audit_file,
            source="audit_logs",
        )

        notif_key = f"notifications/{year}/{month}/notifications_{ts}.jsonl.gz"
        audit_key = f"notifications/{year}/{month}/audit_logs_{ts}.jsonl.gz"

        notif_ok = await _upload_and_verify_md5(notif_file, notif_key)
        audit_ok = await _upload_and_verify_md5(audit_file, audit_key)

    stats = ArchiveStats(
        notifications_archived=notif_rows,
        audit_logs_archived=audit_rows,
    )

    if notif_ok and audit_ok:
        n_pruned, a_pruned = await _prune_archived_window(db, archive_until=window_end)
        stats.notifications_pruned = n_pruned
        stats.audit_logs_pruned = a_pruned

        if redis:
            try:
                await redis.set("notifications:archive:last_run", window_end.isoformat())
            except Exception as exc:
                logger.warning(f"Failed to persist archive cursor: {exc}")
    else:
        logger.error("Archive verification failed; live pruning skipped for safety")

    return stats

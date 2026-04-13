# domains/admin/service.py
"""
Admin domain.

Super admin panel — platform-wide management.
All routes require is_superadmin=True. Regular users cannot access any of this.

Includes:
- Platform-wide user management (list, search, tier override, ban)
- Platform usage statistics
- Audit log viewer
- Feature flag management (platform-wide overrides)
- Super admin creation helper (used by seed.py)

Router endpoints:
  GET    /admin/users                          — list all users
  GET    /admin/users/{id}                     — user detail
  PATCH  /admin/users/{id}/tier               — override tier
  PATCH  /admin/users/{id}/ban                — ban / unban
  PATCH  /admin/users/{id}/verify             — force-verify email
  GET    /admin/stats                          — platform usage stats
  GET    /admin/audit                          — audit log
  GET    /admin/flags                          — list all feature flags
  POST   /admin/flags                          — set flag override
  DELETE /admin/flags/{flag}/{user_id}         — remove flag override
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.db import get_db
from shared.types import Base


# ---------------------------------------------------------------------------
# Audit log ORM model
# ---------------------------------------------------------------------------

class AuditLog(Base):
    """
    Immutable audit trail. Never update or delete rows here.
    Written by the audit service (shared/observability.py).
    """
    __tablename__ = "audit_logs"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True, index=True)
    workspace_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True, index=True)

    action: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    # e.g. "connection.created" | "migration.applied" | "user.banned" | "backup.deleted"

    resource_type: Mapped[str | None] = mapped_column(String(60), nullable=True)
    resource_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)

    # What changed — before/after for mutations
    diff: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    meta: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )


# ---------------------------------------------------------------------------
# Audit log writer (called from other domains as a side effect)
# ---------------------------------------------------------------------------

async def write_audit(
    db: AsyncSession,  # kept for signature compatibility — no longer used internally
    action: str,
    user_id: UUID | None = None,
    workspace_id: UUID | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    diff: dict | None = None,
    meta: dict | None = None,
    ip_address: str | None = None,
) -> None:
    """
    Fire-and-forget audit log write.

    Uses its OWN isolated session so it never interferes with the caller's
    open transaction. The `db` parameter is kept for backward compatibility
    but is ignored — do not rely on it being committed here.

    Never raises. On failure, logs ERROR and returns silently.
    """
    # `db` parameter intentionally ignored — we open a fresh session below.
    try:
        from core.db import _session_factory
        if not _session_factory:
            return

        async with _session_factory() as audit_session:
            entry = AuditLog(
                user_id=user_id,
                workspace_id=workspace_id,
                action=action,
                resource_type=resource_type,
                resource_id=str(resource_id) if resource_id else None,
                diff=diff or {},
                meta=meta or {},
                ip_address=ip_address,
            )
            audit_session.add(entry)
            await audit_session.commit()
    except Exception as exc:
        logger.error(f"Audit log write failed for action '{action}': {exc}")


async def write_audit_batch(
    db: AsyncSession,  # kept for signature compatibility — no longer used internally
    entries: list[dict[str, Any]],
) -> None:
    """
    Batch write audit logs for high-volume operations (bulk update/delete).

    Uses its OWN isolated session. Prevents connection pool exhaustion by
    writing all events in a single transaction. The `db` parameter is kept
    for backward compatibility but is ignored.

    Never raises. On failure, logs ERROR and returns silently.
    """
    if not entries:
        return

    try:
        from core.db import _session_factory
        if not _session_factory:
            return

        async with _session_factory() as audit_session:
            logs = [
                AuditLog(
                    user_id=e.get("user_id"),
                    workspace_id=e.get("workspace_id"),
                    action=e["action"],
                    resource_type=e.get("resource_type"),
                    resource_id=str(e["resource_id"]) if e.get("resource_id") else None,
                    diff=e.get("diff", {}),
                    meta=e.get("meta", {}),
                )
                for e in entries
            ]
            audit_session.add_all(logs)
            await audit_session.commit()
    except Exception as exc:
        logger.error(f"Batch audit write failed ({len(entries)} entries): {exc}")


# ---------------------------------------------------------------------------
# Admin service functions
# ---------------------------------------------------------------------------

def _require_superadmin(user: CurrentUser) -> None:
    if not getattr(user, "is_superadmin", False):
        raise HTTPException(status_code=403, detail="Super admin access required.")


async def get_all_users(
    db: AsyncSession,
    search: str | None = None,
    tier: str | None = None,
    is_active: bool | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    from domains.users.service import User
    from sqlalchemy import func

    q = select(User)
    if search:
        q = q.where(
            User.email.ilike(f"%{search}%") |
            User.full_name.ilike(f"%{search}%")
        )
    if tier:
        q = q.where(User.tier == tier)
    if is_active is not None:
        q = q.where(User.is_active == is_active)

    count_q = select(func.count()).select_from(q.subquery())
    total = await db.scalar(count_q) or 0

    users = (
        await db.execute(
            q.order_by(User.created_at.desc()).offset(offset).limit(limit)
        )
    ).scalars().all()

    return {
        "users": [
            {
                "id": str(u.id),
                "email": u.email,
                "full_name": u.full_name,
                "tier": u.tier,
                "is_active": u.is_active,
                "is_verified": u.is_verified,
                "is_superadmin": u.is_superadmin,
                "created_at": u.created_at.isoformat(),
            }
            for u in users
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


async def override_user_tier(
    db: AsyncSession,
    target_user_id: UUID,
    new_tier: str,
    admin_user_id: UUID,
) -> bool:
    from domains.users.service import User
    user = await db.get(User, target_user_id)
    if not user:
        return False
    old_tier = user.tier
    user.tier = new_tier
    await db.commit()
    await write_audit(
        db, "admin.tier_override",
        user_id=admin_user_id,
        resource_type="user",
        resource_id=str(target_user_id),
        diff={"tier": {"before": old_tier, "after": new_tier}},
    )
    return True


async def ban_user(
    db: AsyncSession,
    target_user_id: UUID,
    ban: bool,
    admin_user_id: UUID,
) -> bool:
    from domains.users.service import User
    user = await db.get(User, target_user_id)
    if not user:
        return False
    if user.is_superadmin:
        raise ValueError("Cannot ban a super admin.")
    user.is_active = not ban
    await db.commit()
    await write_audit(
        db,
        "admin.user_banned" if ban else "admin.user_unbanned",
        user_id=admin_user_id,
        resource_type="user",
        resource_id=str(target_user_id),
    )
    return True


async def force_verify_user(
    db: AsyncSession,
    target_user_id: UUID,
    admin_user_id: UUID,
) -> bool:
    from domains.users.service import User
    user = await db.get(User, target_user_id)
    if not user:
        return False
    user.is_verified = True
    await db.commit()
    await write_audit(
        db, "admin.force_verify",
        user_id=admin_user_id,
        resource_type="user",
        resource_id=str(target_user_id),
    )
    return True


async def get_platform_stats(db: AsyncSession) -> dict[str, Any]:
    """Platform-wide usage summary for the admin dashboard."""
    from domains.users.service import User
    from domains.connections.models import Connection
    from domains.backups.engine import BackupRecord
    from domains.migrations.service import MigrationRecord

    user_count = await db.scalar(select(func.count()).select_from(User).where(User.is_active == True))  # noqa
    conn_count = await db.scalar(select(func.count()).select_from(Connection).where(Connection.is_active == True))  # noqa
    backup_count = await db.scalar(select(func.count()).select_from(BackupRecord))
    migration_count = await db.scalar(select(func.count()).select_from(MigrationRecord).where(MigrationRecord.applied == True))  # noqa

    # User breakdown by tier
    tier_counts = await db.execute(
        select(User.tier, func.count().label("count"))
        .where(User.is_active == True)  # noqa
        .group_by(User.tier)
    )
    tiers = {row.tier: row.count for row in tier_counts.all()}

    # New users last 30 days
    from sqlalchemy import text
    new_users = await db.scalar(
        text(
            "SELECT count(*) FROM users WHERE is_active = true "
            "AND created_at > now() - interval '30 days'"
        )
    )

    return {
        "users": {
            "total": user_count,
            "new_last_30_days": new_users,
            "by_tier": tiers,
        },
        "connections": conn_count,
        "backups": backup_count,
        "migrations_applied": migration_count,
    }


async def get_audit_log(
    db: AsyncSession,
    user_id: UUID | None = None,
    action: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    q = select(AuditLog)
    if user_id:
        q = q.where(AuditLog.user_id == user_id)
    if action:
        q = q.where(AuditLog.action.ilike(f"%{action}%"))
    q = q.order_by(AuditLog.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(q)
    logs = result.scalars().all()
    return [
        {
            "id": str(lg.id),
            "user_id": str(lg.user_id) if lg.user_id else None,
            "action": lg.action,
            "resource_type": lg.resource_type,
            "resource_id": lg.resource_id,
            "diff": lg.diff,
            "ip_address": lg.ip_address,
            "created_at": lg.created_at.isoformat(),
        }
        for lg in logs
    ]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/admin", tags=["admin"])


class TierOverrideRequest(BaseModel):
    tier: str = Field(..., description="explorer | builder | team | mega_team | enterprise")


class BanRequest(BaseModel):
    ban: bool


class FlagOverrideRequest(BaseModel):
    flag_name: str
    user_id: UUID
    enabled: bool
    ttl_days: int = 30


@router.get("/users")
async def list_users(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    search: str | None = Query(None),
    tier: str | None = Query(None),
    is_active: bool | None = Query(None),
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    _require_superadmin(user)
    return await get_all_users(db, search, tier, is_active, limit, offset)


@router.get("/users/{target_user_id}")
async def get_user_detail(
    target_user_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    _require_superadmin(user)
    from domains.users.service import get_user
    db_user = await get_user(db, target_user_id)
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found.")
    return {
        "id": str(db_user.id),
        "email": db_user.email,
        "full_name": db_user.full_name,
        "tier": db_user.tier,
        "is_active": db_user.is_active,
        "is_verified": db_user.is_verified,
        "is_superadmin": db_user.is_superadmin,
        "preferences": db_user.preferences,
        "created_at": db_user.created_at.isoformat(),
    }


@router.patch("/users/{target_user_id}/tier")
async def set_user_tier(
    target_user_id: UUID,
    body: TierOverrideRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    _require_superadmin(user)
    ok = await override_user_tier(db, target_user_id, body.tier, user.id)
    if not ok:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"tier": body.tier}


@router.patch("/users/{target_user_id}/ban")
async def ban_or_unban_user(
    target_user_id: UUID,
    body: BanRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    _require_superadmin(user)
    try:
        ok = await ban_user(db, target_user_id, body.ban, user.id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not ok:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"banned": body.ban}


@router.patch("/users/{target_user_id}/verify")
async def verify_user(
    target_user_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    _require_superadmin(user)
    ok = await force_verify_user(db, target_user_id, user.id)
    if not ok:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"verified": True}


@router.get("/stats")
async def platform_stats(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    _require_superadmin(user)
    return await get_platform_stats(db)


@router.get("/audit")
async def audit_log(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    filter_user_id: UUID | None = Query(None),
    action: str | None = Query(None),
    limit: int = Query(100, le=500),
    offset: int = 0,
):
    _require_superadmin(user)
    return {
        "logs": await get_audit_log(
            db, filter_user_id, action, limit, offset
        )
    }


@router.post("/flags")
async def set_feature_flag(
    body: FlagOverrideRequest,
    user: CurrentUser,
):
    """Set a user-level feature flag override via Redis."""
    _require_superadmin(user)
    from core.db import get_redis
    redis = await get_redis()
    from domains.billing.flutterwave import set_user_flag
    await set_user_flag(
        body.flag_name,
        body.user_id,
        body.enabled,
        redis,
        ttl_seconds=body.ttl_days * 86400,
    )
    return {"set": True, "flag": body.flag_name, "user_id": str(body.user_id), "enabled": body.enabled}


@router.delete("/flags/{flag_name}/{target_user_id}")
async def remove_feature_flag(
    flag_name: str,
    target_user_id: UUID,
    user: CurrentUser,
):
    _require_superadmin(user)
    from core.db import get_redis
    redis = await get_redis()
    await redis.delete(f"flag:{flag_name}:user:{target_user_id}")
    return {"removed": True}

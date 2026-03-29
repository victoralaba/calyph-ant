# domains/notifications/service.py
"""
Notifications domain.

In-app notification storage + fan-out, and Brevo email delivery.
Both live here — they share the same trigger points and are always
called together. No value in splitting them.

In-app notifications are stored in Calyphant's DB and served via API.
Email notifications go through Brevo's API.

Changes from original
---------------------
FIXED  notify_workspace() now respects per-user notification preferences.
       The original implementation created an in-app notification for every
       workspace member unconditionally. Now it checks each member's
       preferences.notifications.in_app flag before creating a notification.
       Members with in_app=False are silently skipped. Opt-out model —
       members who have no preferences set receive the notification.

       Similarly, email fan-out from notify_workspace_with_email() checks
       the preferences.notifications.email flag before sending.

Router endpoints:
  GET    /notifications                    — list user's notifications
  GET    /notifications/unread-count       — count of unread notifications
  PATCH  /notifications/{id}/read          — mark as read
  PATCH  /notifications/read-all           — mark all as read
  DELETE /notifications/{id}              — delete notification
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import DateTime, String, Text, Boolean
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.config import settings
from core.db import get_db
from shared.types import Base


# ---------------------------------------------------------------------------
# Notification types
# ---------------------------------------------------------------------------

class NotificationKind(str, Enum):
    migration_applied    = "migration_applied"
    migration_failed     = "migration_failed"
    backup_completed     = "backup_completed"
    backup_failed        = "backup_failed"
    member_joined        = "member_joined"
    member_left          = "member_left"
    invite_received      = "invite_received"
    connection_down      = "connection_down"
    connection_restored  = "connection_restored"
    billing_upgraded     = "billing_upgraded"
    billing_cancelled    = "billing_cancelled"
    storage_warning      = "storage_warning"


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
    meta: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# In-app notification service
# ---------------------------------------------------------------------------

async def create_notification(
    db: AsyncSession,
    user_id: UUID,
    kind: NotificationKind,
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    workspace_id: UUID | None = None,
    meta: dict | None = None,
) -> Notification:
    notif = Notification(
        user_id=user_id,
        workspace_id=workspace_id,
        kind=kind,
        title=title,
        body=body,
        action_url=action_url,
        meta=meta or {},
    )
    db.add(notif)
    await db.commit()
    await db.refresh(notif)
    return notif


async def notify_workspace(
    db: AsyncSession,
    workspace_id: UUID,
    kind: NotificationKind,
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    exclude_user_id: UUID | None = None,
    meta: dict | None = None,
) -> int:
    """
    Fan-out an in-app notification to all members of a workspace.

    Respects per-user notification preferences — members who have set
    preferences.notifications.in_app = False are silently skipped.
    Members with no preferences configured receive the notification
    (opt-out model, default = receive).

    Returns count of notifications actually created.
    """
    from domains.teams.service import WorkspaceMember
    from domains.users.service import get_preferences

    result = await db.execute(
        select(WorkspaceMember.user_id).where(
            WorkspaceMember.workspace_id == workspace_id
        )
    )
    member_ids = [row[0] for row in result.all()]
    if exclude_user_id:
        member_ids = [uid for uid in member_ids if uid != exclude_user_id]

    created = 0
    for user_id in member_ids:
        # Check the user's notification preferences
        prefs = await get_preferences(db, user_id)
        notif_prefs = prefs.get("notifications", {})

        # Global in-app toggle — defaults to True (opt-out model)
        if not notif_prefs.get("in_app", True):
            continue

        # Per-event-kind toggle if present (e.g. "migration_applied": True)
        if kind.value in notif_prefs and not notif_prefs[kind.value]:
            continue

        db.add(Notification(
            user_id=user_id,
            workspace_id=workspace_id,
            kind=kind,
            title=title,
            body=body,
            action_url=action_url,
            meta=meta or {},
        ))
        created += 1

    if created:
        await db.commit()

    return created


async def notify_workspace_with_email(
    db: AsyncSession,
    workspace_id: UUID,
    kind: NotificationKind,
    title: str,
    email_subject: str,
    email_html: str,
    body: str | None = None,
    action_url: str | None = None,
    exclude_user_id: UUID | None = None,
    meta: dict | None = None,
) -> dict[str, int]:
    """
    Fan-out both in-app notification AND email to all workspace members,
    each gated by the respective preference flag.

    Returns {"in_app": N, "emails": M} for observability.
    """
    from domains.teams.service import WorkspaceMember
    from domains.users.service import User, get_preferences

    result = await db.execute(
        select(WorkspaceMember.user_id).where(
            WorkspaceMember.workspace_id == workspace_id
        )
    )
    member_ids = [row[0] for row in result.all()]
    if exclude_user_id:
        member_ids = [uid for uid in member_ids if uid != exclude_user_id]

    in_app_count = 0
    email_count = 0

    for user_id in member_ids:
        prefs = await get_preferences(db, user_id)
        notif_prefs = prefs.get("notifications", {})

        # --- In-app ---
        send_in_app = notif_prefs.get("in_app", True)
        if kind.value in notif_prefs:
            send_in_app = send_in_app and notif_prefs[kind.value]

        if send_in_app:
            db.add(Notification(
                user_id=user_id,
                workspace_id=workspace_id,
                kind=kind,
                title=title,
                body=body,
                action_url=action_url,
                meta=meta or {},
            ))
            in_app_count += 1

        # --- Email ---
        send_email_pref = notif_prefs.get("email", True)
        if kind.value in notif_prefs:
            send_email_pref = send_email_pref and notif_prefs[kind.value]

        if send_email_pref:
            # Fetch email from users table
            user_result = await db.execute(
                select(User).where(User.id == user_id, User.is_active == True)  # noqa
            )
            user = user_result.scalar_one_or_none()
            if user:
                import asyncio
                asyncio.create_task(send_email(
                    to_email=user.email,
                    to_name=user.full_name or user.email,
                    subject=email_subject,
                    html_content=email_html,
                ))
                email_count += 1

    if in_app_count:
        await db.commit()

    return {"in_app": in_app_count, "emails": email_count}


async def list_notifications(
    db: AsyncSession,
    user_id: UUID,
    unread_only: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> list[Notification]:
    q = select(Notification).where(Notification.user_id == user_id)
    if unread_only:
        q = q.where(Notification.read == False)  # noqa
    q = q.order_by(Notification.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(q)
    return list(result.scalars().all())


async def mark_read(db: AsyncSession, notification_id: UUID, user_id: UUID) -> bool:
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user_id)
        .values(read=True)
    )
    await db.commit()
    # Tell Pylance this is a CursorResult
    cursor_result = cast(CursorResult, result)
    return cursor_result.rowcount > 0


async def mark_all_read(db: AsyncSession, user_id: UUID) -> int:
    result = await db.execute(
        update(Notification)
        .where(Notification.user_id == user_id, Notification.read == False)  # noqa
        .values(read=True)
    )
    await db.commit()
    # Tell Pylance this is a CursorResult
    cursor_result = cast(CursorResult, result)
    return cursor_result.rowcount


async def delete_notification(db: AsyncSession, notification_id: UUID, user_id: UUID) -> bool:
    notif = await db.get(Notification, notification_id)
    if not notif or notif.user_id != user_id:
        return False
    await db.delete(notif)
    await db.commit()
    return True


async def unread_count(db: AsyncSession, user_id: UUID) -> int:
    from sqlalchemy import func
    result = await db.execute(
        select(func.count()).where(
            Notification.user_id == user_id,
            Notification.read == False,  # noqa
        )
    )
    return result.scalar() or 0


# ---------------------------------------------------------------------------
# Brevo email service
# ---------------------------------------------------------------------------

BREVO_API = "https://api.brevo.com/v3"


async def send_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_content: str,
    template_id: int | None = None,
    template_params: dict | None = None,
) -> bool:
    """
    Send a transactional email via Brevo.
    Returns True on success, False on failure (never raises).
    """
    if not settings.BREVO_API_KEY:
        logger.warning("BREVO_API_KEY not set — email not sent.")
        return False

    payload: dict[str, Any] = {
        "to": [{"email": to_email, "name": to_name}],
        "sender": {
            "email": settings.EMAIL_FROM_ADDRESS,
            "name": settings.EMAIL_FROM_NAME,
        },
    }

    if template_id:
        payload["templateId"] = template_id
        payload["params"] = template_params or {}
    else:
        payload["subject"] = subject
        payload["htmlContent"] = html_content

    try:
        async with httpx.AsyncClient(
            base_url=BREVO_API,
            headers={
                "api-key": settings.BREVO_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        ) as client:
            response = await client.post("/smtp/email", json=payload)
            response.raise_for_status()
            return True
    except Exception as exc:
        logger.error(f"Brevo email failed to {to_email}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Email templates (inline HTML — replace with Brevo template IDs in production)
# ---------------------------------------------------------------------------

async def send_invite_email(
    to_email: str,
    inviter_name: str,
    workspace_name: str,
    invite_token: str,
) -> bool:
    accept_url = f"{settings.APP_BASE_URL}/invites/{invite_token}/accept"
    html = f"""
    <h2>You've been invited to join {workspace_name} on Calyphant</h2>
    <p>{inviter_name} has invited you to collaborate on their database workspace.</p>
    <p>
      <a href="{accept_url}"
         style="background:#4F46E5;color:white;padding:12px 24px;
                border-radius:6px;text-decoration:none;">
        Accept Invitation
      </a>
    </p>
    <p>This invite expires in 7 days.</p>
    <p style="color:#6b7280;font-size:12px;">
      If you weren't expecting this invitation, you can safely ignore this email.
    </p>
    """
    return await send_email(
        to_email=to_email,
        to_name=to_email,
        subject=f"{inviter_name} invited you to {workspace_name} on Calyphant",
        html_content=html,
    )


async def send_backup_complete_email(
    to_email: str,
    to_name: str,
    database_name: str,
    backup_size: str,
) -> bool:
    html = f"""
    <h2>Backup completed</h2>
    <p>Your backup of <strong>{database_name}</strong> completed successfully.</p>
    <p>Size: {backup_size}</p>
    <p><a href="{settings.APP_BASE_URL}/backups">View backups →</a></p>
    """
    return await send_email(
        to_email=to_email,
        to_name=to_name,
        subject=f"Backup complete — {database_name}",
        html_content=html,
    )


async def send_welcome_email(to_email: str, to_name: str) -> bool:
    html = f"""
    <h2>Welcome to Calyphant, {to_name or 'there'}!</h2>
    <p>Your PostgreSQL workspace is ready. Connect your first database to get started.</p>
    <p>
      <a href="{settings.APP_BASE_URL}/connections/new">Connect a database →</a>
    </p>
    """
    return await send_email(
        to_email=to_email,
        to_name=to_name or "there",
        subject="Welcome to Calyphant",
        html_content=html,
    )


async def send_password_reset_email(to_email: str, reset_token: str) -> bool:
    reset_url = f"{settings.APP_BASE_URL}/auth/reset-password?token={reset_token}"
    html = f"""
    <h2>Reset your password</h2>
    <p>
      Click the link below to reset your Calyphant password.
      This link expires in 1 hour.
    </p>
    <p><a href="{reset_url}">Reset password →</a></p>
    <p style="color:#6b7280;font-size:12px;">
      If you didn't request this, ignore this email — your password won't change.
    </p>
    """
    return await send_email(
        to_email=to_email,
        to_name=to_email,
        subject="Reset your Calyphant password",
        html_content=html,
    )


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
    created_at: str


@router.get("", response_model=list[NotificationResponse])
async def list_user_notifications(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    unread_only: bool = False,
    limit: int = 50,
    offset: int = 0,
):
    notifications = await list_notifications(
        db, user.id, unread_only=unread_only, limit=limit, offset=offset
    )
    return [
        NotificationResponse(
            id=n.id,
            kind=n.kind,
            title=n.title,
            body=n.body,
            action_url=n.action_url,
            read=n.read,
            created_at=n.created_at.isoformat(),
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
    updated = await mark_read(db, notification_id, user.id)
    if not updated:
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"read": True}


@router.patch("/read-all")
async def mark_all_notifications_read(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    count = await mark_all_read(db, user.id)
    return {"marked_read": count}


@router.delete("/{notification_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user_notification(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    deleted = await delete_notification(db, notification_id, user.id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Notification not found.")

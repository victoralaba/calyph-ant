# domains/notifications/service.py
"""
Notifications Domain (State Manager & Event Dispatcher).

Re-architected to act as the "Engine of Truth" for the event matrix.
Evaluates preferences, saves state to Postgres, dispatches to WebSockets (Toasts), 
and pushes email I/O to Celery.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import DateTime, String, Text, Boolean, select, update, and_
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.db import get_db
from shared.types import Base

# ---------------------------------------------------------------------------
# The Event Matrix Dictionary
# ---------------------------------------------------------------------------
# Defines the physical behavior of every system event.

EVENT_MATRIX = {
    "user_signup":              {"in_app": False, "email": True,  "toast": False, "sender": "ceo", "priority": "High"},
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
    "billing_plan_upgraded":    {"in_app": False, "email": True,  "toast": False, "sender": "ceo", "priority": "High"},
    "platform_update":          {"in_app": True,  "email": True,  "toast": False, "sender": "ceo", "priority": "Low"},
}


class NotificationKind(str, Enum):
    # Dynamically built from the matrix keys to maintain strict typing
    user_signup = "user_signup"
    password_reset = "password_reset"
    security_new_login = "security_new_login"
    team_invite_received = "team_invite_received"
    team_invite_accepted = "team_invite_accepted"
    team_role_changed = "team_role_changed"
    db_connection_success = "db_connection_success"
    db_connection_failed = "db_connection_failed"
    schema_migration_success = "schema_migration_success"
    schema_migration_failed = "schema_migration_failed"
    backup_completed = "backup_completed"
    backup_failed = "backup_failed"
    billing_plan_upgraded = "billing_plan_upgraded"
    platform_update = "platform_update"


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
    # Allows soft-delete, archiving, and restoring.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# The Grand Dispatcher
# ---------------------------------------------------------------------------

async def dispatch_event(
    db: AsyncSession,
    user_id: UUID,
    kind: NotificationKind,
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    workspace_id: UUID | None = None,
    meta: dict | None = None,
    email_subject: str | None = None,
    email_html: str | None = None,
):
    """
    The Single Source of Truth for system messaging.
    Evaluates the matrix, checks user preferences, and pushes out events.
    """
    from domains.users.service import get_preferences, get_user
    from domains.teams.service import ws_manager
    from shared.pubsub import SignalEvent
    from domains.notifications.tasks import send_async_email

    rules = EVENT_MATRIX.get(kind.value, {})
    if not rules:
        logger.warning(f"Unmapped event kind dispatched: {kind.value}")
        return

    # 1. Fetch Preferences & User Data
    prefs = await get_preferences(db, user_id)
    notif_prefs = prefs.get("notifications", {})
    user = await get_user(db, user_id)
    
    if not user:
        return

    # 2. Database (In-App Inbox)
    if rules.get("in_app"):
        if notif_prefs.get("in_app", True):
            notif = Notification(
                user_id=user_id,
                workspace_id=workspace_id,
                kind=kind.value,
                title=title,
                body=body,
                action_url=action_url,
                meta=meta or {},
            )
            db.add(notif)
            # We don't commit immediately; caller can wrap in a transaction if needed, 
            # or we commit at the end. We'll commit here for safety if called standalone.
            await db.commit()

    # 3. WebSockets (Live Toasts)
    if rules.get("toast") and workspace_id:
        # Pushes directly to the UI layer as a transient toast
        event_payload = SignalEvent(
            event="ui_toast",
            workspace_id=workspace_id,
            entity_id=str(user_id),
            meta={"title": title, "body": body, "kind": kind.value, "priority": rules.get("priority")}
        )
        import asyncio
        asyncio.create_task(ws_manager.broadcast(workspace_id, event_payload))

    # 4. Celery (Email Fan-out)
    if rules.get("email") and email_subject and email_html:
        if notif_prefs.get("email", True):
            # Fire and forget to Celery
            send_async_email.apply_async(
                kwargs={
                    "to_email": user.email,
                    "to_name": user.full_name or user.email,
                    "subject": email_subject,
                    "html_content": email_html,
                    "sender_type": rules.get("sender", "system")
                }
            )


async def dispatch_workspace_event(
    db: AsyncSession,
    workspace_id: UUID,
    kind: NotificationKind,
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    exclude_user_id: UUID | None = None,
    meta: dict | None = None,
    email_subject_template: str | None = None, # e.g. "{user} joined the workspace"
    email_html_template: str | None = None,
):
    """
    Fans out a matrix event to all active members of a workspace.
    """
    from domains.teams.service import WorkspaceMember
    
    # Get all workspace members
    result = await db.execute(
        select(WorkspaceMember.user_id).where(
            WorkspaceMember.workspace_id == workspace_id
        )
    )
    member_ids = [row[0] for row in result.all()]
    
    if exclude_user_id:
        member_ids = [uid for uid in member_ids if uid != exclude_user_id]

    import asyncio
    
    # Dispatch individual events concurrently
    tasks = []
    for uid in member_ids:
        tasks.append(
            dispatch_event(
                db=db,
                user_id=uid,
                kind=kind,
                title=title,
                body=body,
                action_url=action_url,
                workspace_id=workspace_id,
                meta=meta,
                email_subject=email_subject_template,
                email_html=email_html_template
            )
        )
    
    if tasks:
        await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Inbox Operations (CRUD)
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
        q = q.where(Notification.read == False)
        
    # Pinned items always float to the top, then sort by date
    q = q.order_by(Notification.is_pinned.desc(), Notification.created_at.desc()).offset(offset).limit(limit)
    
    result = await db.execute(q)
    return list(result.scalars().all())


async def unread_count(db: AsyncSession, user_id: UUID) -> int:
    from sqlalchemy import func
    result = await db.execute(
        select(func.count()).where(
            Notification.user_id == user_id,
            Notification.read == False,
            Notification.deleted_at.is_(None)
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
    include_deleted: bool = False, # Used to fetch the Archive view
    limit: int = 50,
    offset: int = 0,
):
    """
    [UI CONSIDERATION] 
    By default this returns active notifications. 
    Pass include_deleted=true to render the "Archive/Trash" view in Svelte.
    Pinned items are automatically sorted to the top.
    """
    notifications = await list_notifications(
        db, user.id, unread_only=unread_only, include_deleted=include_deleted, limit=limit, offset=offset
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
            deleted_at=n.deleted_at.isoformat() if n.deleted_at else None
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
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"read": True}


@router.patch("/{notification_id}/pin")
async def toggle_notification_pin(
    notification_id: UUID,
    pin_state: bool, # Send true to pin, false to unpin
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user.id)
        .values(is_pinned=pin_state)
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"is_pinned": pin_state}


@router.delete("/{notification_id}")
async def soft_delete_user_notification(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    [UI CONSIDERATION] 
    This moves the item to the Archive. Provide a UI toast offering an "Undo" button 
    that calls the /restore endpoint.
    """
    result = await db.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user.id)
        .values(deleted_at=datetime.now(timezone.utc))
    )
    await db.commit()
    if result.rowcount == 0:
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
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"restored": True}


@router.delete("/{notification_id}/hard", status_code=status.HTTP_204_NO_CONTENT)
async def hard_delete_user_notification(
    notification_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    [UI CONSIDERATION]
    Use this only when emptying the trash/archive. Permanent, non-recoverable deletion.
    """
    notif = await db.get(Notification, notification_id)
    if not notif or notif.user_id != user.id:
        raise HTTPException(status_code=404, detail="Notification not found.")
    await db.delete(notif)
    await db.commit()
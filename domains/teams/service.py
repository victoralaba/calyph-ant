# domains/teams/service.py
"""
Teams domain.

Workspace membership, invitations, role-based access,
real-time WebSocket change notifications, and read-only share links.

WebSocket architecture
----------------------
The workspace_websocket handler has been fundamentally redesigned to
avoid holding a database connection open for the lifetime of the
WebSocket session.

Old (broken) pattern:
    async def workspace_websocket(db: AsyncSession = Depends(get_db)):
        # db session (and its Postgres connection) held for hours
        while True:
            await websocket.receive_text()

New (correct) pattern:
    async def workspace_websocket():
        # Open a short-lived session ONLY for auth
        async with get_db_context() as db:
            role = await get_member_role(db, workspace_id, user_id)
        # Session is closed here. Zero DB connections held by this handler.
        while True:
            await websocket.receive_text()

WorkspaceConnectionManager
--------------------------
broadcast_local(workspace_id, event)
    Delivers directly to WebSocket connections on THIS worker process.
    Never touches Redis. Used by the pub/sub subscriber to avoid loops.

broadcast(workspace_id, event)
    High-level call used by all domain code. Routes through shared/pubsub.py
    which publishes to Redis AND calls broadcast_local for the local worker.
    This is the only method domain code should call.

Why the split:
    publish() in pubsub.py calls broadcast_local() to deliver locally,
    then publishes to Redis. The Redis subscriber calls broadcast_local()
    again when the message comes back. If broadcast() were used in the
    subscriber it would publish back to Redis → infinite loop.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from loguru import logger
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import DateTime, ForeignKey, Integer, String, Boolean, Text
from sqlalchemy import select, delete, update, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.config import settings
from core.auth import CurrentUser
from core.db import get_db, get_db_context
from shared.types import Base
from shared.pubsub import SignalEvent, ws_ingress_throttle
from domains.notifications.rate_limit import enforce_redis_limit
from domains.notifications.service import dispatch_workspace_event, NotificationKind


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class WorkspaceRole(str, Enum):
    owner = "owner"
    admin = "admin"
    editor = "editor"
    viewer = "viewer"


ROLE_HIERARCHY = {
    WorkspaceRole.owner: 4,
    WorkspaceRole.admin: 3,
    WorkspaceRole.editor: 2,
    WorkspaceRole.viewer: 1,
}


def can_manage(actor_role: WorkspaceRole, target_role: WorkspaceRole) -> bool:
    return ROLE_HIERARCHY[actor_role] > ROLE_HIERARCHY[target_role]


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class Workspace(Base):
    __tablename__ = "workspaces"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    slug: Mapped[str] = mapped_column(String(120), nullable=False, unique=True, index=True)
    subdomain: Mapped[str | None] = mapped_column(String(120), nullable=True, unique=True, index=True)
    billing_tier: Mapped[str] = mapped_column(String(30), nullable=False, default="explorer")
    created_by: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class WorkspaceMember(Base):
    __tablename__ = "workspace_members"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False, default=WorkspaceRole.viewer)
    joined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class WorkspaceInvite(Base):
    __tablename__ = "workspace_invites"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    invited_by: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(20), nullable=False, default=WorkspaceRole.viewer)
    token: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    accepted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class WorkspaceShareLink(Base):
    """
    A share link grants time-limited, read-only (viewer) access to a
    workspace to anyone who holds the token — no Calyphant account required.
    """
    __tablename__ = "workspace_share_links"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_by: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    token: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True, index=True
    )
    label: Mapped[str] = mapped_column(String(120), nullable=False, default="Share link")
    role: Mapped[str] = mapped_column(
        String(20), nullable=False, default=WorkspaceRole.viewer
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    max_uses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    use_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Service functions — workspaces
# ---------------------------------------------------------------------------

async def create_workspace(
    db: AsyncSession,
    name: str,
    user_id: UUID,
) -> Workspace:
    import re
    slug_base = re.sub(r"[^\w]", "-", name.lower()).strip("-")[:100]
    slug = f"{slug_base}-{secrets.token_hex(4)}"

    workspace = Workspace(name=name, slug=slug, created_by=user_id)
    db.add(workspace)
    await db.flush()

    member = WorkspaceMember(
        workspace_id=workspace.id,
        user_id=user_id,
        role=WorkspaceRole.owner,
    )
    db.add(member)
    await db.commit()
    await db.refresh(workspace)
    return workspace


def _sanitize_subdomain(name: str) -> str:
    import re
    label = re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")
    label = re.sub(r"-{2,}", "-", label)
    return (label or "workspace")[:63]


async def _allocate_subdomain(db: AsyncSession, workspace: Workspace) -> str:
    base = _sanitize_subdomain(workspace.name)
    candidate = base
    n = 1
    while True:
        existing = await db.execute(
            select(Workspace.id).where(
                Workspace.subdomain == candidate,
                Workspace.id != workspace.id,
            ).limit(1)
        )
        if existing.scalar_one_or_none() is None:
            workspace.subdomain = candidate
            return candidate
        n += 1
        candidate = f"{base}-{n}"


async def _enforce_workspace_member_limit(
    db: AsyncSession,
    workspace_id: UUID,
    incoming_delta: int = 1,
) -> None:
    from domains.billing.flutterwave import get_limits

    ws = await db.get(Workspace, workspace_id)
    if not ws:
        raise ValueError("Workspace not found.")

    limits = get_limits(ws.billing_tier)
    max_members = int(limits.get("workspace_members", 1))
    member_count = await db.scalar(
        select(func.count(WorkspaceMember.id)).where(WorkspaceMember.workspace_id == workspace_id)
    ) or 0
    if member_count + incoming_delta > max_members:
        raise ValueError(
            f"Workspace member limit reached ({member_count}/{max_members}). "
            "Upgrade workspace tier to add more members."
        )


async def soft_delete_workspace(
    db: AsyncSession,
    workspace_id: UUID,
    requesting_user_id: UUID,
) -> bool:
    workspace = await db.get(Workspace, workspace_id)
    if not workspace or not workspace.is_active:
        return False
    if workspace.created_by != requesting_user_id:
        raise ValueError("Only the workspace owner can delete this workspace.")
    workspace.is_active = False
    await db.commit()
    logger.info(f"Workspace soft-deleted: {workspace_id} by user {requesting_user_id}")
    return True


async def get_user_workspaces(db: AsyncSession, user_id: UUID) -> list[dict]:
    result = await db.execute(
        select(Workspace, WorkspaceMember.role)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(WorkspaceMember.user_id == user_id, Workspace.is_active == True)  # noqa
        .order_by(Workspace.created_at.desc())
    )
    return [
        {
            "id": str(ws.id),
            "name": ws.name,
            "slug": ws.slug,
            "subdomain": ws.subdomain,
            "billing_tier": ws.billing_tier,
            "role": role,
            "created_at": ws.created_at.isoformat(),
        }
        for ws, role in result.all()
    ]


async def get_member_role(
    db: AsyncSession, workspace_id: UUID, user_id: UUID
) -> WorkspaceRole | None:
    result = await db.execute(
        select(WorkspaceMember.role).where(
            WorkspaceMember.workspace_id == workspace_id,
            WorkspaceMember.user_id == user_id,
        )
    )
    row = result.scalar_one_or_none()
    return WorkspaceRole(row) if row else None


async def list_members(db: AsyncSession, workspace_id: UUID) -> list[dict]:
    result = await db.execute(
        select(WorkspaceMember).where(WorkspaceMember.workspace_id == workspace_id)
    )
    members = result.scalars().all()
    return [
        {
            "user_id": str(m.user_id),
            "role": m.role,
            "joined_at": m.joined_at.isoformat(),
        }
        for m in members
    ]


async def create_invite(
    db: AsyncSession,
    workspace_id: UUID,
    invited_by: UUID,
    email: str,
    role: WorkspaceRole,
) -> WorkspaceInvite:
    await _enforce_workspace_member_limit(db, workspace_id, incoming_delta=1)
    token = secrets.token_urlsafe(32)
    invite = WorkspaceInvite(
        workspace_id=workspace_id,
        invited_by=invited_by,
        email=email,
        role=role,
        token=token,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db.add(invite)
    await db.commit()
    await db.refresh(invite)
    return invite


async def accept_invite(
    db: AsyncSession,
    token: str,
    user_id: UUID,
    user_email: str,
) -> WorkspaceMember:
    result = await db.execute(
        select(WorkspaceInvite).where(
            WorkspaceInvite.token == token,
            WorkspaceInvite.accepted == False,  # noqa
        )
    )
    invite = result.scalar_one_or_none()

    if not invite:
        raise ValueError("Invite not found or already accepted.")
    if invite.expires_at < datetime.now(timezone.utc):
        raise ValueError("Invite has expired.")
    if invite.email.lower() != user_email.lower():
        raise ValueError("This invite was sent to a different email address.")

    existing = await get_member_role(db, invite.workspace_id, user_id)
    if existing:
        raise ValueError("You are already a member of this workspace.")
    await _enforce_workspace_member_limit(db, invite.workspace_id, incoming_delta=1)

    member = WorkspaceMember(
        workspace_id=invite.workspace_id,
        user_id=user_id,
        role=invite.role,
    )
    db.add(member)
    invite.accepted = True
    await db.commit()
    await db.refresh(member)
    return member


async def change_member_role(
    db: AsyncSession,
    workspace_id: UUID,
    target_user_id: UUID,
    new_role: WorkspaceRole,
) -> bool:
    result = await db.execute(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace_id,
            WorkspaceMember.user_id == target_user_id,
        )
    )
    member = result.scalar_one_or_none()
    if not member:
        return False
    member.role = new_role
    await db.commit()
    return True


async def remove_member(
    db: AsyncSession,
    workspace_id: UUID,
    target_user_id: UUID,
) -> bool:
    result = await db.execute(
        delete(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace_id,
            WorkspaceMember.user_id == target_user_id,
        ).returning(WorkspaceMember.id) # 1. Add the returning clause
    )
    await db.commit()
    
    # 2. Check if a row was actually returned (meaning it was deleted)
    deleted_member = result.first() 
    return deleted_member is not None


async def transfer_workspace_ownership(
    db: AsyncSession,
    workspace_id: UUID,
    current_owner_id: UUID,
    target_user_id: UUID,
) -> bool:
    current_owner = await db.execute(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace_id,
            WorkspaceMember.user_id == current_owner_id,
            WorkspaceMember.role == WorkspaceRole.owner,
        ).limit(1)
    )
    owner_row = current_owner.scalar_one_or_none()
    if not owner_row:
        raise ValueError("Only a current workspace owner can transfer ownership.")

    target = await db.execute(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace_id,
            WorkspaceMember.user_id == target_user_id,
        ).limit(1)
    )
    target_row = target.scalar_one_or_none()
    if not target_row:
        raise ValueError("Target user must already be a workspace member.")

    owner_row.role = WorkspaceRole.admin
    target_row.role = WorkspaceRole.owner
    await db.commit()
    return True


async def leave_workspace(
    db: AsyncSession,
    workspace_id: UUID,
    user_id: UUID,
) -> bool:
    role = await get_member_role(db, workspace_id, user_id)
    if not role:
        return False

    if role == WorkspaceRole.owner:
        result = await db.execute(
            select(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.role == WorkspaceRole.owner,
                WorkspaceMember.user_id != user_id,
            )
        )
        other_owners = result.scalars().all()
        if not other_owners:
            raise ValueError(
                "You are the only owner of this workspace and cannot leave. "
                "Transfer ownership to another member first, or delete the workspace."
            )

    return await remove_member(db, workspace_id, user_id)


# ---------------------------------------------------------------------------
# Service functions — share links
# ---------------------------------------------------------------------------

async def create_share_link(
    db: AsyncSession,
    workspace_id: UUID,
    created_by: UUID,
    label: str = "Share link",
    expires_in_days: int | None = None,
    max_uses: int | None = None,
) -> WorkspaceShareLink:
    token = secrets.token_urlsafe(36)
    expires_at: datetime | None = None
    if expires_in_days is not None and expires_in_days > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)

    link = WorkspaceShareLink(
        workspace_id=workspace_id,
        created_by=created_by,
        token=token,
        label=label,
        role=WorkspaceRole.viewer,
        is_active=True,
        expires_at=expires_at,
        max_uses=max_uses,
        use_count=0,
    )
    db.add(link)
    await db.commit()
    await db.refresh(link)
    logger.info(
        f"Share link created: workspace={workspace_id} "
        f"label='{label}' expires={expires_at or 'never'}"
    )
    return link


async def resolve_share_link(
    db: AsyncSession,
    token: str,
) -> WorkspaceShareLink | None:
    result = await db.execute(
        select(WorkspaceShareLink).where(
            WorkspaceShareLink.token == token,
            WorkspaceShareLink.is_active == True,  # noqa
        )
    )
    link = result.scalar_one_or_none()
    if link is None:
        return None
    if link.expires_at and link.expires_at < datetime.now(timezone.utc):
        return None
    if link.max_uses is not None and link.use_count >= link.max_uses:
        return None
    link.use_count += 1
    await db.commit()
    return link


async def list_share_links(
    db: AsyncSession,
    workspace_id: UUID,
) -> list[WorkspaceShareLink]:
    result = await db.execute(
        select(WorkspaceShareLink)
        .where(
            WorkspaceShareLink.workspace_id == workspace_id,
            WorkspaceShareLink.is_active == True,  # noqa
        )
        .order_by(WorkspaceShareLink.created_at.desc())
    )
    return list(result.scalars().all())


async def revoke_share_link(
    db: AsyncSession,
    workspace_id: UUID,
    token: str,
) -> bool:
    result = await db.execute(
        select(WorkspaceShareLink).where(
            WorkspaceShareLink.token == token,
            WorkspaceShareLink.workspace_id == workspace_id,
        )
    )
    link = result.scalar_one_or_none()
    if not link:
        return False
    link.is_active = False
    await db.commit()
    logger.info(f"Share link revoked: workspace={workspace_id} token={token[:8]}...")
    return True


# ---------------------------------------------------------------------------
# Notification fan-out
# ---------------------------------------------------------------------------

async def notify_workspace_members(
    db: AsyncSession,
    workspace_id: UUID,
    kind: str,
    title: str,
    body: str | None = None,
    action_url: str | None = None,
    exclude_user_id: UUID | None = None,
    meta: dict | None = None,
) -> int:
    from domains.notifications.service import Notification
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
        prefs = await get_preferences(db, user_id)
        notif_prefs = prefs.get("notifications", {})
        if not notif_prefs.get("in_app", True):
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


# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------

class WorkspaceConnectionManager:
    """
    Manages WebSocket connections for this worker process only.
    Acts as the Source of Truth for the Dynamic Subscription Matrix.
    """

    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = {}

    async def connect(self, workspace_id: UUID, ws: WebSocket) -> None:
        await ws.accept()
        key = str(workspace_id)

        is_first_connection = key not in self._connections or len(self._connections[key]) == 0
        
        self._connections.setdefault(key, set()).add(ws)
        
        if is_first_connection:
            from shared.pubsub import subscribe_workspace
            asyncio.create_task(subscribe_workspace(workspace_id))

        logger.debug(
            f"WS connected: workspace={workspace_id} "
            f"total={len(self._connections[key])}"
        )

    def disconnect(self, workspace_id: UUID, ws: WebSocket) -> None:
        key = str(workspace_id)
        if key in self._connections:
            self._connections[key].discard(ws)
            self._cleanup_if_empty(workspace_id)

    async def broadcast_local(self, workspace_id: UUID, event: SignalEvent) -> None:
        """
        Deliver signal to WebSocket connections on THIS worker process.
        Intercepts Control Signals to act as a Server-Side Kill Switch.
        """
        key = str(workspace_id)
        dead: set[WebSocket] = set()
        payload = event.model_dump_json()

        # 1. INTERCEPT: Execute Server-Side Kill Switch
        if event.event == "workspace_deleted":
            # Nuke the entire workspace
            for ws in list(self._connections.get(key, set())):
                try:
                    # UI CONSIDERATION: 4003 code indicates "Access Revoked".
                    # The Svelte UI MUST NOT attempt to reconnect if it receives a 4003 close frame.
                    await ws.close(code=4003, reason="Workspace deleted")
                except Exception:
                    pass
                dead.add(ws)

        elif event.event == "member_left" and event.entity_id:
            # Sniper rifle: Nuke only the specific kicked user
            for ws in list(self._connections.get(key, set())):
                if getattr(ws.state, "auth_type", None) == "user" and getattr(ws.state, "auth_id", None) == event.entity_id:
                    try:
                        await ws.close(code=4003, reason="Access revoked")
                    except Exception:
                        pass
                    dead.add(ws)

        elif event.event == "share_link_revoked" and event.entity_id:
            # Sniper rifle: Nuke only the specific revoked token
            for ws in list(self._connections.get(key, set())):
                if getattr(ws.state, "auth_type", None) == "link" and getattr(ws.state, "auth_token", None) == event.entity_id:
                    try:
                        await ws.close(code=4003, reason="Link revoked")
                    except Exception:
                        pass
                    dead.add(ws)

        # 2. DELIVER: Standard Broadcast to surviving connections
        for ws in list(self._connections.get(key, set())):
            if ws in dead:
                continue
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)

        # 3. GARBAGE COLLECTION
        if dead:
            for ws in dead:
                self._connections.get(key, set()).discard(ws)
            self._cleanup_if_empty(workspace_id)

    def _cleanup_if_empty(self, workspace_id: UUID) -> None:
        key = str(workspace_id)
        conns = self._connections.get(key)
        
        if conns is not None and len(conns) == 0:
            self._connections.pop(key, None)
            from shared.pubsub import unsubscribe_workspace
            asyncio.create_task(unsubscribe_workspace(workspace_id))
            logger.debug(f"WS disconnected: workspace={workspace_id} (0 remaining. Redis unsubscribed.)")

    async def broadcast(self, workspace_id: UUID, event: SignalEvent) -> None:
        from shared.pubsub import publish
        await publish(workspace_id, event)

    async def send_to(self, ws: WebSocket, event: dict[str, Any]) -> None:
        try:
            await ws.send_text(json.dumps(event, default=str))
        except Exception:
            pass

    def connection_count(self, workspace_id: UUID) -> int:
        return len(self._connections.get(str(workspace_id), set()))

ws_manager = WorkspaceConnectionManager()

# ---------------------------------------------------------------------------
# WebSocket authentication helper
# ---------------------------------------------------------------------------

async def _authenticate_ws_token(
    token: str,
    workspace_id: UUID,
) -> dict[str, str] | None:
    """
    Authenticate a WebSocket connection token and return its identity payload.

    Opens a short-lived DB session, verifies the token (JWT or share link),
    then closes the session immediately. Returns a dictionary identifying 
    the connection type and ID, or None if invalid.
    """
    from core.auth import decode_token

    # --- JWT path ---
    try:
        payload = decode_token(token)
        user_id = UUID(payload["sub"])

        async with get_db_context() as db:
            role = await get_member_role(db, workspace_id, user_id)

        if role is not None:
            return {"type": "user", "id": str(user_id)}

    except Exception:
        pass  # Not a valid JWT — try share link

    # --- Share link path ---
    try:
        async with get_db_context() as db:
            link = await resolve_share_link(db, token)

        if link is not None and link.workspace_id == workspace_id:
            return {"type": "link", "token": token}

    except Exception:
        pass
        
    return None


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/teams", tags=["teams"])


class CreateWorkspaceRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)

class InviteRequest(BaseModel):
    email: EmailStr = Field(..., max_length=255)
    role: WorkspaceRole = WorkspaceRole.viewer

class ChangeRoleRequest(BaseModel):
    role: WorkspaceRole

class CreateShareLinkRequest(BaseModel):
    label: str = Field(default="Share link", max_length=120)
    expires_in_days: int | None = Field(default=None, ge=1, le=365)
    max_uses: int | None = Field(default=None, ge=1, le=100000)


class TransferOwnershipRequest(BaseModel):
    target_user_id: UUID


class UpdateSubdomainRequest(BaseModel):
    subdomain: str = Field(..., min_length=3, max_length=63)


async def _require_role(
    db: AsyncSession,
    workspace_id: UUID,
    user_id: UUID,
    minimum: WorkspaceRole,
) -> WorkspaceRole:
    role = await get_member_role(db, workspace_id, user_id)
    if not role:
        raise HTTPException(status_code=403, detail="Not a member of this workspace.")
    if ROLE_HIERARCHY[role] < ROLE_HIERARCHY[minimum]:
        raise HTTPException(status_code=403, detail=f"Requires {minimum.value} role.")
    return role


# ---------------------------------------------------------------------------
# Workspace CRUD
# ---------------------------------------------------------------------------

@router.get("/workspaces")
async def list_workspaces(user: CurrentUser, db: AsyncSession = Depends(get_db)):
    return {"workspaces": await get_user_workspaces(db, user.id)}


@router.post("/workspaces", status_code=status.HTTP_201_CREATED)
async def create_workspace_endpoint(
    body: CreateWorkspaceRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    ws = await create_workspace(db, body.name, user.id)
    return {
        "id": str(ws.id),
        "name": ws.name,
        "slug": ws.slug,
        "subdomain": ws.subdomain,
        "billing_tier": ws.billing_tier,
    }


@router.get("/workspaces/{workspace_id}/resolve")
async def resolve_workspace_subdomain(
    workspace_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await _require_role(db, workspace_id, user.id, WorkspaceRole.viewer)
    ws = await db.get(Workspace, workspace_id)
    if not ws or not ws.is_active:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    if not ws.subdomain:
        await _allocate_subdomain(db, ws)
        await db.commit()
    base = settings.APP_BASE_URL.replace("https://", "").replace("http://", "")
    return {
        "workspace_id": str(ws.id),
        "subdomain": ws.subdomain,
        "redirect_url": f"https://{ws.subdomain}.{base}",
    }


@router.patch("/workspaces/{workspace_id}/subdomain")
async def update_workspace_subdomain(
    workspace_id: UUID,
    body: UpdateSubdomainRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await _require_role(db, workspace_id, user.id, WorkspaceRole.owner)
    ws = await db.get(Workspace, workspace_id)
    if not ws or not ws.is_active:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    if ws.billing_tier not in ("team", "mega_team", "enterprise"):
        raise HTTPException(status_code=403, detail="Subdomains are available on Team tier and above.")

    ws.subdomain = _sanitize_subdomain(body.subdomain)
    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail=f"Subdomain unavailable: {exc}")
    return {"updated": True, "subdomain": ws.subdomain}


@router.delete("/workspaces/{workspace_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workspace_endpoint(
    workspace_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    try:
        deleted = await soft_delete_workspace(db, workspace_id, user.id)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail="Workspace not found.")

    asyncio.create_task(ws_manager.broadcast(
        workspace_id,
        SignalEvent(event="workspace_deleted", workspace_id=workspace_id)
    ))


# ---------------------------------------------------------------------------
# Membership
# ---------------------------------------------------------------------------

@router.get("/workspaces/{workspace_id}/members")
async def list_workspace_members(
    workspace_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await _require_role(db, workspace_id, user.id, WorkspaceRole.viewer)
    return {"members": await list_members(db, workspace_id)}


@router.post("/workspaces/{workspace_id}/invite", status_code=status.HTTP_201_CREATED)
async def invite_member(
    workspace_id: UUID,
    body: InviteRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    actor_role = await _require_role(db, workspace_id, user.id, WorkspaceRole.admin)
    if not can_manage(actor_role, body.role):
        raise HTTPException(
            status_code=403,
            detail="You cannot invite someone with a role equal to or higher than yours.",
        )

    workspace = await db.get(Workspace, workspace_id)
    if not workspace or not workspace.is_active:
        raise HTTPException(status_code=404, detail="Workspace not found.")

    invite_limit = await enforce_redis_limit(
        key=f"notifications:rl:workspace_invite:hour:{workspace_id}:{user.id}",
        limit=settings.NOTIFICATIONS_WORKSPACE_INVITE_LIMIT_PER_HOUR,
        window_seconds=3600,
        fail_closed=True,
        action="workspace_invite",
    )
    if not invite_limit.allowed:
        status_code = 503 if invite_limit.reason == "redis_unavailable" else 429
        raise HTTPException(
            status_code=status_code,
            detail="Workspace invites are temporarily unavailable. Please try again shortly.",
        )

    invite = await create_invite(db, workspace_id, user.id, body.email, body.role)

    # Resolve inviter display name for the email
    inviter_name = user.email
    try:
        from domains.users.service import get_user
        inviter_user = await get_user(db, user.id)
        if inviter_user and inviter_user.full_name:
            inviter_name = inviter_user.full_name
    except Exception:
        pass

    # Build email from the template system (single source of HTML truth)
    from domains.notifications.service import enqueue_email_notification
    from domains.notifications.templates import build_invite_email

    email_subject, email_html = build_invite_email(
        inviter_name=inviter_name,
        workspace_name=workspace.name,
        invite_token=invite.token,
        role=body.role.value,
    )

    # Approved exception to centralized dispatch_event flow:
    # invite targets can be non-users (no user_id/preferences row yet), so this
    # path remains direct email dispatch by design.
    # Single Celery dispatch — fire-and-forget with automatic retry on failure.
    # UI NOTE: If this endpoint returns 201 but the invitee never receives an email,
    # it is a Celery/SendPulse delivery issue, not an invite creation failure.
    # The invite token is valid regardless — the invitee can accept via direct link.
    await enqueue_email_notification(
        to_email=body.email,
        to_name=body.email,
        subject=email_subject,
        html_content=email_html,
        sender_type="system",
        notification_kind="team_invite_received",
        idempotency_key=f"workspace_invite:{workspace_id}:{invite.token}",
        fail_closed_on_redis_error=True,
    )

    return {
        "token": invite.token,
        "email": invite.email,
        "expires_at": invite.expires_at.isoformat(),
    }


@router.post("/invites/{token}/accept")
async def accept_workspace_invite(
    token: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    try:
        member = await accept_invite(db, token, user.id, user.email)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    asyncio.create_task(ws_manager.broadcast(
        member.workspace_id,
        SignalEvent(
            event="member_joined", 
            workspace_id=member.workspace_id, 
            entity_id=str(user.id), 
            role=member.role
        )
    ))

    asyncio.create_task(dispatch_workspace_event(
        db=db,
        workspace_id=member.workspace_id,
        kind="team_invite_accepted",
        title=f"{user.email} joined the workspace",
        exclude_user_id=user.id,
        meta={"user_id": str(user.id), "role": member.role},
    ))

    return {"workspace_id": str(member.workspace_id), "role": member.role}


@router.patch("/workspaces/{workspace_id}/members/{target_user_id}")
async def update_member_role(
    workspace_id: UUID,
    target_user_id: UUID,
    body: ChangeRoleRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    actor_role = await _require_role(db, workspace_id, user.id, WorkspaceRole.admin)
    target_role = await get_member_role(db, workspace_id, target_user_id)
    if not target_role:
        raise HTTPException(status_code=404, detail="Member not found.")
    if not can_manage(actor_role, target_role):
        raise HTTPException(
            status_code=403,
            detail="Cannot manage a member with equal or higher role.",
        )
    await change_member_role(db, workspace_id, target_user_id, body.role)
    return {"updated": True}


@router.delete(
    "/workspaces/{workspace_id}/members/{target_user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_workspace_member(
    workspace_id: UUID,
    target_user_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    actor_role = await _require_role(db, workspace_id, user.id, WorkspaceRole.admin)
    target_role = await get_member_role(db, workspace_id, target_user_id)
    if target_role and not can_manage(actor_role, target_role):
        raise HTTPException(
            status_code=403,
            detail="Cannot remove a member with equal or higher role.",
        )
    removed = await remove_member(db, workspace_id, target_user_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Member not found.")

    asyncio.create_task(ws_manager.broadcast(
        workspace_id,
        SignalEvent(
            event="member_left", 
            workspace_id=workspace_id, 
            entity_id=str(target_user_id)
        )
    ))


@router.post("/workspaces/{workspace_id}/transfer-ownership")
async def transfer_workspace_owner(
    workspace_id: UUID,
    body: TransferOwnershipRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    try:
        await transfer_workspace_ownership(
            db=db,
            workspace_id=workspace_id,
            current_owner_id=user.id,
            target_user_id=body.target_user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"transferred": True, "workspace_id": str(workspace_id), "new_owner": str(body.target_user_id)}


@router.delete(
    "/workspaces/{workspace_id}/leave",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def leave_workspace_endpoint(
    workspace_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    try:
        left = await leave_workspace(db, workspace_id, user.id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not left:
        raise HTTPException(
            status_code=404,
            detail="You are not a member of this workspace.",
        )

    asyncio.create_task(ws_manager.broadcast(
        workspace_id,
        SignalEvent(
            event="member_left", 
            workspace_id=workspace_id, 
            entity_id=str(user.id)
        )
    ))


# ---------------------------------------------------------------------------
# Share link endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/workspaces/{workspace_id}/share-links",
    status_code=status.HTTP_201_CREATED,
)
async def create_workspace_share_link(
    workspace_id: UUID,
    body: CreateShareLinkRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await _require_role(db, workspace_id, user.id, WorkspaceRole.admin)
    link = await create_share_link(
        db=db,
        workspace_id=workspace_id,
        created_by=user.id,
        label=body.label,
        expires_in_days=body.expires_in_days,
        max_uses=body.max_uses,
    )
    from core.config import settings
    share_url = f"{settings.APP_BASE_URL}/share/{link.token}"
    return {
        "id": str(link.id),
        "token": link.token,
        "label": link.label,
        "share_url": share_url,
        "role": link.role,
        "expires_at": link.expires_at.isoformat() if link.expires_at else None,
        "max_uses": link.max_uses,
        "use_count": link.use_count,
        "created_at": link.created_at.isoformat(),
    }


@router.get("/workspaces/{workspace_id}/share-links")
async def list_workspace_share_links(
    workspace_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await _require_role(db, workspace_id, user.id, WorkspaceRole.admin)
    links = await list_share_links(db, workspace_id)
    from core.config import settings
    return {
        "share_links": [
            {
                "id": str(lnk.id),
                "token": lnk.token,
                "label": lnk.label,
                "share_url": f"{settings.APP_BASE_URL}/share/{lnk.token}",
                "role": lnk.role,
                "expires_at": lnk.expires_at.isoformat() if lnk.expires_at else None,
                "max_uses": lnk.max_uses,
                "use_count": lnk.use_count,
                "created_at": lnk.created_at.isoformat(),
            }
            for lnk in links
        ]
    }


@router.delete(
    "/workspaces/{workspace_id}/share-links/{token}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def revoke_workspace_share_link(
    workspace_id: UUID,
    token: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await _require_role(db, workspace_id, user.id, WorkspaceRole.admin)
    revoked = await revoke_share_link(db, workspace_id, token)
    if not revoked:
        raise HTTPException(status_code=404, detail="Share link not found.")
        
    # Send the Kill Signal to Redis. 
    # Any worker holding a socket for this token will sever it instantly.
    asyncio.create_task(ws_manager.broadcast(
        workspace_id,
        SignalEvent(
            event="share_link_revoked", 
            workspace_id=workspace_id, 
            entity_id=token  # We pass the token as the entity_id to target the exact sockets
        )
    ))


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@router.websocket("/workspaces/{workspace_id}/ws")
async def workspace_websocket(
    workspace_id: UUID,
    websocket: WebSocket,
):
    """
    Real-time workspace event stream.

    Authentication
    --------------
    Token is passed as the `token` query parameter. Both JWT access tokens
    and share link tokens are accepted.
    """
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=4001)
        return

    # --- Authentication: open session, verify, CLOSE before event loop ---
    auth_data = await _authenticate_ws_token(token, workspace_id)
    if not auth_data:
        # UI CONSIDERATION: The Svelte UI MUST NOT attempt to reconnect if 
        # the initial handshake fails with 4003. It means the token is dead.
        await websocket.close(code=4003)
        return
        
    # Tag the TCP socket with its identity so the Kill Switch can find it later
    websocket.state.auth_type = auth_data["type"]
    websocket.state.auth_id = auth_data.get("id")
    websocket.state.auth_token = auth_data.get("token")

    await ws_manager.connect(workspace_id, websocket)

    # Unique throttle key based on user ID or share link token
    throttle_key = f"ws_{auth_data.get('id') or auth_data.get('token')}"

    try:
        while True:
            try:
                msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=30.0,
                )

                # ==========================================================
                # INGRESS SHIELD: Execute Zero-I/O Throttle
                # Position: MUST be immediately after receive, before json.loads
                # ==========================================================
                is_allowed = await ws_ingress_throttle.consume(throttle_key)
                if not is_allowed:
                    logger.warning(f"Malicious WS Flood blocked for key: {throttle_key}")
                    # UI CONSIDERATION: 1008 is Policy Violation. 
                    # The Svelte UI MUST interpret this as a severe rate limit.
                    # Do NOT reconnect immediately. Implement an exponential backoff 
                    # starting at no less than 5 seconds.
                    await websocket.close(code=1008, reason="Rate limit exceeded")
                    return
                # ==========================================================

                try:
                    data = json.loads(msg)
                except (json.JSONDecodeError, TypeError):
                    continue

                if data.get("type") == "ping":
                    await ws_manager.send_to(websocket, {"type": "pong"})

            except asyncio.TimeoutError:
                await ws_manager.send_to(websocket, {"type": "ping"})

            except WebSocketDisconnect:
                break

    except Exception as exc:
        logger.warning(f"WS handler unexpected error: workspace={workspace_id} {exc}")
    finally:
        ws_manager.disconnect(workspace_id, websocket)

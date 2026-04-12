# core/auth.py
"""
Authentication.

JWT access + refresh tokens, argon2id password hashing,
FastAPI CurrentUser dependency, and password reset token management.

Token format:
  Access token:  {"sub": user_id, "email": email, "tier": tier,
                  "workspace_id": workspace_id, "is_superadmin": bool,
                  "type": "access"}
  Refresh token: {"sub": user_id, "type": "refresh"}

Share link tokens
-----------------
A share link token (created via POST /teams/workspaces/{id}/share-links)
grants read-only (viewer) access to a specific workspace without requiring
a Calyphant account. When _get_current_user encounters a Bearer token that
is neither a JWT nor an API key (no "caly_" prefix), it falls back to a
share link lookup. On success it constructs a synthetic AuthenticatedUser
with:
  - id: a deterministic UUID derived from the share link token
    (sha256 of the token, first 16 bytes → UUID)
  - email: "share-link@calyphant.internal" (never a real address)
  - tier: "free"
  - workspace_id: the workspace the link belongs to
  - is_superadmin: False

The synthetic user has no User row in the database. Any service that
calls get_user(db, user.id) for a share-link user will receive None and
must handle that gracefully (returning read-only data or 403 on writes).

Session management — FIXED
--------------------------
The old _get_db_session() helper returned a bare AsyncSession without a
context manager. This caused sessions to leak on every request to the
/auth/* endpoints because they were never explicitly closed.

All auth router endpoints now use Depends(get_db) directly, which wraps
the session in an async context manager that commits on success, rolls
back on exception, and always closes the session when the request ends.
The _get_db_session() helper has been removed entirely.

CurrentUser is an Annotated type used as a FastAPI dependency.
Every protected route declares: user: CurrentUser
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any
from uuid import UUID

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.db import get_db, get_redis
from domains.notifications.rate_limit import enforce_redis_limit


# ---------------------------------------------------------------------------
# Argon2id password hashing
# ---------------------------------------------------------------------------

_ph = PasswordHasher(
    time_cost=settings.ARGON2_TIME_COST,
    memory_cost=settings.ARGON2_MEMORY_COST,
    parallelism=settings.ARGON2_PARALLELISM,
)


def hash_password(plaintext: str) -> str:
    return _ph.hash(plaintext)


def verify_password(plaintext: str, hashed: str) -> bool:
    try:
        return _ph.verify(hashed, plaintext)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def password_needs_rehash(hashed: str) -> bool:
    return _ph.check_needs_rehash(hashed)


# ---------------------------------------------------------------------------
# JWT tokens
# ---------------------------------------------------------------------------

def create_access_token(
    user_id: UUID,
    email: str,
    tier: str,
    workspace_id: UUID | None,
    is_superadmin: bool = False,
) -> str:
    expires = datetime.now(timezone.utc) + timedelta(
        minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    )
    payload = {
        "sub": str(user_id),
        "email": email,
        "tier": tier,
        "workspace_id": str(workspace_id) if workspace_id else None,
        "is_superadmin": is_superadmin,
        "type": "access",
        "exp": expires,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(user_id: UUID) -> str:
    expires = datetime.now(timezone.utc) + timedelta(
        days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS
    )
    payload = {
        "sub": str(user_id),
        "type": "refresh",
        "exp": expires,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict[str, Any]:
    """
    Decode and validate a JWT.
    Raises HTTPException on invalid / expired tokens.
    """
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
        return payload
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ---------------------------------------------------------------------------
# Current user model
# ---------------------------------------------------------------------------

class AuthenticatedUser(BaseModel):
    """
    Parsed claims from a verified JWT access token, API key lookup, or
    share link validation.

    The `is_share_link_user` flag is True when authentication succeeded
    via a share link token. Such users have viewer access to one workspace
    and no real user account. Services should check this flag before
    performing writes — the router layer already guards most routes via
    the role check, but services can use it for additional safety.
    """
    id: UUID
    email: str
    tier: str
    workspace_id: UUID | None
    is_superadmin: bool = False
    is_share_link_user: bool = False

    model_config = {"frozen": True}


def _share_link_user_id(token: str) -> UUID:
    """
    Derive a deterministic, non-guessable UUID from a share link token.
    This UUID never corresponds to a real User row.
    """
    digest = hashlib.sha256(f"share_link:{token}".encode()).digest()
    return UUID(bytes=digest[:16])


# ---------------------------------------------------------------------------
# Authentication dependency
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


async def _get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> AuthenticatedUser:
    """
    FastAPI dependency — extracts and validates the credential from the
    Authorization header.

    Three authentication paths in priority order:

    1. API key (prefix "caly_") — user identified by API key lookup
    2. JWT access token — standard authenticated user
    3. Share link token — read-only guest access to a specific workspace
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # --- Path 1: API key ---
    if token.startswith("caly_"):
        return await _authenticate_api_key(token)

    # --- Path 2: JWT ---
    # Attempt to decode as JWT. If it succeeds, return the authenticated user.
    # ExpiredSignatureError is a hard 401 — don't fall through to share link.
    # Any other JWTError means this isn't a JWT — try share link instead.
    try:
        payload = decode_token(token)
        if payload.get("type") != "access":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type. Use an access token.",
            )
        workspace_id_str = payload.get("workspace_id")
        return AuthenticatedUser(
            id=UUID(payload["sub"]),
            email=payload["email"],
            tier=payload.get("tier", "free"),
            workspace_id=UUID(workspace_id_str) if workspace_id_str else None,
            is_superadmin=payload.get("is_superadmin", False),
            is_share_link_user=False,
        )
    except HTTPException as jwt_exc:
        if "expired" in (jwt_exc.detail or "").lower():
            raise  # Hard 401 for expired tokens — no fallback
        # Not a valid JWT — fall through to share link check

    # --- Path 3: Share link token ---
    return await _authenticate_share_link(token)


async def _authenticate_share_link(token: str) -> AuthenticatedUser:
    """
    Validate a share link token and return a synthetic read-only user.

    Returns a synthetic AuthenticatedUser with is_share_link_user=True.
    The id is deterministic but never maps to a real User DB row.
    """
    from core.db import _session_factory
    from domains.teams.service import resolve_share_link

    if not _session_factory:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not ready.",
        )

    async with _session_factory() as db:
        link = await resolve_share_link(db, token)

    if not link:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return AuthenticatedUser(
        id=_share_link_user_id(token),
        email="share-link@calyphant.internal",
        tier="free",
        workspace_id=link.workspace_id,
        is_superadmin=False,
        is_share_link_user=True,
    )


async def _authenticate_api_key(raw_key: str) -> AuthenticatedUser:
    """Validate an API key and return the associated user as AuthenticatedUser."""
    from core.db import _session_factory
    from domains.users.service import verify_api_key

    if not _session_factory:
        raise HTTPException(status_code=500, detail="Database not ready.")

    async with _session_factory() as db:
        user = await verify_api_key(db, raw_key)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key.",
        )

    from domains.teams.service import WorkspaceMember

    async with _session_factory() as db:
        result = await db.execute(
            select(WorkspaceMember.workspace_id)
            .where(WorkspaceMember.user_id == user.id)
            .order_by(WorkspaceMember.joined_at.asc())
            .limit(1)
        )
        ws_row = result.scalar_one_or_none()

    return AuthenticatedUser(
        id=user.id,
        email=user.email,
        tier=user.tier,
        workspace_id=ws_row,
        is_superadmin=user.is_superadmin,
        is_share_link_user=False,
    )


# Annotated shorthand used in every protected route: user: CurrentUser
CurrentUser = Annotated[AuthenticatedUser, Depends(_get_current_user)]


# ---------------------------------------------------------------------------
# Auth router
# ---------------------------------------------------------------------------

auth_router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    full_name: str | None = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str
    workspace_id: UUID | None = None


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8, max_length=128)


@auth_router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register(
    body: RegisterRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Register a new user account.

    Creates the user, a personal workspace, adds them as workspace owner,
    and fires a welcome email as a background Celery task. Returns access +
    refresh tokens immediately — no email verification gate on first login.
    """
    from domains.teams.service import create_workspace
    from domains.users.service import User, get_user_by_email

    client_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.headers.get("X-Real-IP")
        or (request.client.host if request.client else "unknown")
    )
    register_limit = await enforce_redis_limit(
        key=f"notifications:rl:register:hour:{client_ip}",
        limit=settings.NOTIFICATIONS_REGISTER_LIMIT_PER_HOUR,
        window_seconds=3600,
        fail_closed=True,
        action="register",
    )
    if not register_limit.allowed:
        status_code = 503 if register_limit.reason == "redis_unavailable" else 429
        raise HTTPException(
            status_code=status_code,
            detail="Registration temporarily unavailable. Please try again shortly.",
        )

    existing = await get_user_by_email(db, body.email)
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered.")

    user = User(
        email=body.email.lower(),
        full_name=body.full_name,
        password_hash=hash_password(body.password),
    )
    db.add(user)
    await db.flush()  # Populate user.id before create_workspace needs it

    workspace = await create_workspace(
        db, body.full_name or body.email.split("@")[0], user.id
    )
    await db.commit()

    # Fire-and-forget via Celery — never blocks the response.
    # The task handles retries internally if SendPulse is temporarily down.
    import asyncio
    from domains.notifications.service import send_welcome_email
    asyncio.create_task(send_welcome_email(user.email, user.full_name or ""))

    access = create_access_token(
        user.id, user.email, user.tier, workspace.id, user.is_superadmin
    )
    refresh = create_refresh_token(user.id)
    return TokenResponse(access_token=access, refresh_token=refresh)


@auth_router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    request: Request,                       # <-- ADD: needed to extract IP address
    db: AsyncSession = Depends(get_db),
):
    """
    Authenticate with email + password.

    Returns access + refresh tokens. If the stored password hash uses
    outdated argon2 parameters, it is transparently rehashed on login.

    After successful authentication, fires a fire-and-forget
    security_new_login notification so the user receives an in-app
    and email alert about the new sign-in. This never blocks the response.
    """
    from domains.teams.service import WorkspaceMember
    from domains.users.service import get_user_by_email

    user = await get_user_by_email(db, body.email)
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is inactive.")

    if password_needs_rehash(user.password_hash):
        user.password_hash = hash_password(body.password)
        await db.commit()

    result = await db.execute(
        select(WorkspaceMember.workspace_id)
        .where(WorkspaceMember.user_id == user.id)
        .order_by(WorkspaceMember.joined_at.asc())
        .limit(1)
    )
    workspace_id = result.scalar_one_or_none()

    access  = create_access_token(user.id, user.email, user.tier, workspace_id, user.is_superadmin)
    refresh = create_refresh_token(user.id)

    # -----------------------------------------------------------------------
    # FIRE-AND-FORGET: security_new_login notification
    #
    # Dispatches an in-app notification AND an email alert to the user
    # informing them of the new sign-in. Uses asyncio.create_task() so
    # it never adds latency to the login response.
    #
    # IP EXTRACTION: We extract the real client IP from X-Forwarded-For
    # when running behind a reverse proxy (nginx, Cloudflare), falling
    # back to the direct connection IP. This matches the ip_address field
    # on AuditLog for correlation.
    #
    # UI CONSIDERATION:
    #   The resulting in-app notification (kind="security_new_login") should
    #   be displayed with a lock/shield icon. It is High priority — do NOT
    #   hide it behind a collapsed "Other notifications" group. If the user
    #   did not initiate this login, the action_url should direct them to
    #   the password reset page.
    # -----------------------------------------------------------------------
    ip_address: str | None = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.headers.get("X-Real-IP")
        or (request.client.host if request.client else None)
    )

    import asyncio as _asyncio
    from domains.notifications.service import dispatch_event
    from domains.notifications.templates import build_new_login_email
    from core.config import settings as _settings

    async def _fire_login_notification():
        try:
            subject, html = build_new_login_email(
                name=user.full_name or user.email,
                ip_address=ip_address,
            )
            await dispatch_event(
                db=db,
                user_id=user.id,
                kind="security_new_login",
                title="New sign-in to your Calyphant account",
                body=f"A new sign-in was detected from IP {ip_address or 'unknown'}.",
                action_url=f"{_settings.APP_BASE_URL}/auth/forgot-password",
                workspace_id=workspace_id,
                meta={"ip_address": ip_address},
                email_subject=subject,
                email_html=html,
                # Pass pre-fetched user data to skip redundant DB queries
                # inside dispatch_event (avoids a second get_user() call)
                _user_email=user.email,
                _user_name=user.full_name or user.email,
            )
        except Exception as exc:
            # Notification failure must NEVER fail the login response
            from loguru import logger
            logger.warning(f"security_new_login notification failed (non-fatal): {exc}")

    _asyncio.create_task(_fire_login_notification())

    return TokenResponse(access_token=access, refresh_token=refresh)


@auth_router.post("/refresh", response_model=TokenResponse)
async def refresh_tokens(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Exchange a refresh token for a new access + refresh token pair.

    The refresh token is stateless — old tokens are not blocklisted.
    For rotation with revocation, add a Redis deny-list here.
    """
    from domains.teams.service import WorkspaceMember
    from domains.users.service import get_user

    payload = decode_token(body.refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=400, detail="Invalid token type.")

    user_id = UUID(payload["sub"])
    user = await get_user(db, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive.")

    workspace_id: UUID | None = None

    if body.workspace_id:
        # Verify the user is a member of the requested workspace
        result = await db.execute(
            select(WorkspaceMember.workspace_id)
            .where(
                WorkspaceMember.user_id == user.id,
                WorkspaceMember.workspace_id == body.workspace_id,
            )
            .limit(1)
        )
        workspace_id = result.scalar_one_or_none()

    if not workspace_id:
        # Fall back to the user's first/default workspace
        result = await db.execute(
            select(WorkspaceMember.workspace_id)
            .where(WorkspaceMember.user_id == user.id)
            .order_by(WorkspaceMember.joined_at.asc())
            .limit(1)
        )
        workspace_id = result.scalar_one_or_none()

    access = create_access_token(
        user.id, user.email, user.tier, workspace_id, user.is_superadmin
    )
    new_refresh = create_refresh_token(user.id)
    return TokenResponse(access_token=access, refresh_token=new_refresh)


@auth_router.post("/forgot-password", status_code=status.HTTP_204_NO_CONTENT)
async def forgot_password(
    body: PasswordResetRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Initiate a password reset.

    Stores a one-time token in Redis (1-hour TTL) and enqueues a Celery
    task to send the reset email. Always returns 204 regardless of whether
    the email is registered — prevents account enumeration.
    """
    from domains.users.service import get_user_by_email

    client_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.headers.get("X-Real-IP")
        or (request.client.host if request.client else "unknown")
    )
    reset_limit = await enforce_redis_limit(
        key=f"notifications:rl:pwd_reset:hour:{client_ip}:{body.email.lower()}",
        limit=settings.NOTIFICATIONS_PASSWORD_RESET_LIMIT_PER_HOUR,
        window_seconds=3600,
        fail_closed=True,
        action="password_reset",
    )
    if not reset_limit.allowed:
        status_code = 503 if reset_limit.reason == "redis_unavailable" else 429
        raise HTTPException(
            status_code=status_code,
            detail="Password reset temporarily unavailable. Please try again shortly.",
        )

    user = await get_user_by_email(db, body.email)
    if not user:
        return  # Silent — don't reveal whether the email is registered

    token = secrets.token_urlsafe(32)
    redis = await get_redis()
    await redis.setex(f"pwd_reset:{token}", 3600, str(user.id))

    # Fire-and-forget via Celery — never blocks the response.
    import asyncio
    from domains.notifications.service import send_password_reset_email
    asyncio.create_task(send_password_reset_email(user.email, token))


@auth_router.post("/reset-password", status_code=status.HTTP_204_NO_CONTENT)
async def reset_password(
    body: PasswordResetConfirm,
    db: AsyncSession = Depends(get_db),
):
    """
    Confirm a password reset using the single-use token from the reset email.

    The token is deleted from Redis after use — it cannot be replayed.
    """
    from domains.users.service import get_user

    redis = await get_redis()
    user_id_bytes = await redis.get(f"pwd_reset:{body.token}")
    if not user_id_bytes:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token.")

    user_id = UUID(user_id_bytes.decode())
    user = await get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    user.password_hash = hash_password(body.new_password)
    await db.commit()

    # Consume the token — single use only
    await redis.delete(f"pwd_reset:{body.token}")

# domains/users/service.py
"""
Users domain.

User profile management, preferences, and API key management.
Authentication (registration/login) lives in core/auth.py.
This domain handles everything after the user is authenticated.

Router endpoints:
  GET    /users/me                    — current user profile
  PATCH  /users/me                    — update profile
  DELETE /users/me                    — delete account (soft)
  GET    /users/me/preferences        — get preferences
  PATCH  /users/me/preferences        — update preferences
  GET    /users/me/api-keys           — list API keys
  POST   /users/me/api-keys           — create API key
  DELETE /users/me/api-keys/{id}      — revoke API key
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import DateTime, String, Text, Boolean
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.db import get_db
from shared.types import Base


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    full_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)

    # Account state
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_superadmin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Billing tier
    tier: Mapped[str] = mapped_column(String(30), default="free", nullable=False)

    # Preferences stored as JSON — avoids migrations for new preference keys
    preferences: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    # e.g. {"theme": "dark", "default_schema": "public", "query_limit": 1000,
    #        "notifications": {"email": true, "in_app": true}}

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    # Store only the hash — never the raw key
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    # Store first 8 chars for display ("sk_live_abcd1234...")
    key_preview: Mapped[str] = mapped_column(String(20), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Default preferences
# ---------------------------------------------------------------------------

DEFAULT_PREFERENCES: dict[str, Any] = {
    "theme": "system",           # "light" | "dark" | "system"
    "default_schema": "public",
    "query_limit": 1000,
    "auto_save_queries": True,
    "show_row_counts": True,
    "notifications": {
        # Global toggles
        "email": True,
        "in_app": True,
        # Matrix granular toggles (Opt-out model)
        "security_alerts": True,       # High priority, usually forced anyway
        "team_activity": True,         # Invites, role changes
        "schema_migrations": True,
        "backup_alerts": True,
        "billing_alerts": True,
        "platform_updates": True,
    },
}

# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------

async def get_user(db: AsyncSession, user_id: UUID) -> User | None:
    result = await db.execute(
        select(User).where(User.id == user_id, User.is_active == True)  # noqa
    )
    return result.scalar_one_or_none()


async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(
        select(User).where(User.email == email.lower(), User.is_active == True)  # noqa
    )
    return result.scalar_one_or_none()


async def update_profile(
    db: AsyncSession,
    user_id: UUID,
    full_name: str | None = None,
    avatar_url: str | None = None,
) -> User | None:
    user = await get_user(db, user_id)
    if not user:
        return None
    if full_name is not None:
        user.full_name = full_name
    if avatar_url is not None:
        user.avatar_url = avatar_url
    user.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(user)
    return user


async def get_preferences(db: AsyncSession, user_id: UUID) -> dict:
    user = await get_user(db, user_id)
    if not user:
        return DEFAULT_PREFERENCES.copy()
    # Merge with defaults so new keys always have a value
    merged = {**DEFAULT_PREFERENCES, **user.preferences}
    return merged


async def update_preferences(
    db: AsyncSession,
    user_id: UUID,
    updates: dict[str, Any],
) -> dict:
    user = await get_user(db, user_id)
    if not user:
        raise ValueError("User not found.")

    # Deep merge — don't replace nested dicts entirely
    current = {**DEFAULT_PREFERENCES, **user.preferences}
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(current.get(key), dict):
            current[key] = {**current[key], **value}
        else:
            current[key] = value

    user.preferences = current
    user.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return current


async def soft_delete_user(db: AsyncSession, user_id: UUID) -> bool:
    user = await get_user(db, user_id)
    if not user:
        return False
    user.is_active = False
    user.deleted_at = datetime.now(timezone.utc)
    # Anonymise PII
    user.email = f"deleted_{user_id}@deleted.calyphant"
    user.full_name = None
    user.avatar_url = None
    await db.commit()
    return True


# ---------------------------------------------------------------------------
# API key management
# ---------------------------------------------------------------------------

def _generate_api_key() -> tuple[str, str, str]:
    """
    Returns (raw_key, key_hash, key_preview).
    raw_key is shown once and never stored.
    """
    raw = f"caly_{secrets.token_urlsafe(32)}"
    hashed = hashlib.sha256(raw.encode()).hexdigest()
    preview = raw[:16] + "..."
    return raw, hashed, preview


async def create_api_key(
    db: AsyncSession,
    user_id: UUID,
    name: str,
    expires_at: datetime | None = None,
) -> tuple[ApiKey, str]:
    """Returns (ApiKey record, raw_key). raw_key is shown once."""
    raw, hashed, preview = _generate_api_key()
    key = ApiKey(
        user_id=user_id,
        name=name,
        key_hash=hashed,
        key_preview=preview,
        expires_at=expires_at,
    )
    db.add(key)
    await db.commit()
    await db.refresh(key)
    return key, raw


async def list_api_keys(db: AsyncSession, user_id: UUID) -> list[ApiKey]:
    result = await db.execute(
        select(ApiKey).where(
            ApiKey.user_id == user_id,
            ApiKey.is_active == True,  # noqa
        ).order_by(ApiKey.created_at.desc())
    )
    return list(result.scalars().all())


async def revoke_api_key(db: AsyncSession, key_id: UUID, user_id: UUID) -> bool:
    result = await db.execute(
        select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user_id)
    )
    key = result.scalar_one_or_none()
    if not key:
        return False
    key.is_active = False
    await db.commit()
    return True


async def verify_api_key(db: AsyncSession, raw_key: str) -> User | None:
    """Used by API key auth middleware. Returns user if key is valid."""
    hashed = hashlib.sha256(raw_key.encode()).hexdigest()
    result = await db.execute(
        select(ApiKey).where(
            ApiKey.key_hash == hashed,
            ApiKey.is_active == True,  # noqa
        )
    )
    key = result.scalar_one_or_none()
    if not key:
        return None
    if key.expires_at and key.expires_at < datetime.now(timezone.utc):
        return None
    # Update last used
    key.last_used_at = datetime.now(timezone.utc)
    await db.commit()
    return await get_user(db, key.user_id)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/users", tags=["users"])


class UpdateProfileRequest(BaseModel):
    full_name: str | None = Field(None, max_length=120)
    avatar_url: str | None = None


class UpdatePreferencesRequest(BaseModel):
    updates: dict[str, Any]


class CreateApiKeyRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    expires_at: datetime | None = None


class UserResponse(BaseModel):
    id: UUID
    email: str
    full_name: str | None
    avatar_url: str | None
    tier: str
    is_verified: bool
    created_at: str


@router.get("/me", response_model=UserResponse)
async def get_current_user_profile(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    db_user = await get_user(db, user.id)
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found.")
    return UserResponse(
        id=db_user.id,
        email=db_user.email,
        full_name=db_user.full_name,
        avatar_url=db_user.avatar_url,
        tier=db_user.tier,
        is_verified=db_user.is_verified,
        created_at=db_user.created_at.isoformat(),
    )


@router.patch("/me", response_model=UserResponse)
async def update_user_profile(
    body: UpdateProfileRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    updated = await update_profile(db, user.id, body.full_name, body.avatar_url)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found.")
    return UserResponse(
        id=updated.id,
        email=updated.email,
        full_name=updated.full_name,
        avatar_url=updated.avatar_url,
        tier=updated.tier,
        is_verified=updated.is_verified,
        created_at=updated.created_at.isoformat(),
    )


@router.delete("/me", status_code=status.HTTP_204_NO_CONTENT)
async def delete_account(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    await soft_delete_user(db, user.id)


@router.get("/me/preferences")
async def get_user_preferences(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    return await get_preferences(db, user.id)


@router.patch("/me/preferences")
async def update_user_preferences(
    body: UpdatePreferencesRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    return await update_preferences(db, user.id, body.updates)


@router.get("/me/api-keys")
async def list_user_api_keys(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    keys = await list_api_keys(db, user.id)
    return {
        "api_keys": [
            {
                "id": str(k.id),
                "name": k.name,
                "preview": k.key_preview,
                "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
                "expires_at": k.expires_at.isoformat() if k.expires_at else None,
                "created_at": k.created_at.isoformat(),
            }
            for k in keys
        ]
    }


@router.post("/me/api-keys", status_code=status.HTTP_201_CREATED)
async def create_user_api_key(
    body: CreateApiKeyRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    key_record, raw_key = await create_api_key(
        db, user.id, body.name, body.expires_at
    )
    return {
        "id": str(key_record.id),
        "name": key_record.name,
        "key": raw_key,   # Shown ONCE — not stored
        "preview": key_record.key_preview,
        "warning": "Save this key now. It will not be shown again.",
    }


@router.delete("/me/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_user_api_key(
    key_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    revoked = await revoke_api_key(db, key_id, user.id)
    if not revoked:
        raise HTTPException(status_code=404, detail="API key not found.")

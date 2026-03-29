# domains/connections/router.py

"""
Connections router.

Endpoints:
  POST   /connections/test              — test a URL without persisting
  POST   /connections                   — create a persisted connection
  GET    /connections                   — list workspace connections
  GET    /connections/{id}              — get single connection
  PATCH  /connections/{id}              — update name, keep-alive config
  DELETE /connections/{id}              — soft delete
  POST   /connections/{id}/ping         — manual keep-alive ping
  POST   /connections/{id}/refresh      — re-test and update metadata
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from pydantic.networks import PostgresDsn
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, encrypt_secret, decrypt_secret, get_db_context, get_redis
from domains.connections.models import CloudProvider, Connection, ConnectionStatus
from domains.connections import service
from shared.types import (
    ConnectionCreateRequest,
    ConnectionTestResult,
    ConnectionUpdateRequest,
    PaginatedResponse,
)

router = APIRouter(prefix="/connections", tags=["connections"])

# ---------------------------------------------------------------------------
# Request / Response schemas
# (Kept here — they are only used by this router)
# ---------------------------------------------------------------------------

class TestConnectionRequest(BaseModel):
    # Using Pydantic's native PostgresDsn ensures strict RFC-3986 compliance.
    # We cast it back to a string for seamless downstream usage.
    url: Annotated[PostgresDsn, Field(description="PostgreSQL connection URL")]

    @field_validator("url", mode="after")
    @classmethod
    def cast_url_to_string(cls, v: PostgresDsn) -> str:
        return str(v)


class TestConnectionResponse(BaseModel):
    success: bool
    host: str | None = None
    port: int | None = None
    database: str | None = None
    pg_version: str | None = None
    pg_version_num: int | None = None
    cloud_provider: CloudProvider | None = None
    capabilities: dict | None = None
    was_sleeping: bool = False
    error: str | None = None


class CreateConnectionRequest(BaseModel):
    url: Annotated[PostgresDsn, Field(description="PostgreSQL connection URL")]
    name: str = Field(..., min_length=1, max_length=120)
    keep_alive_enabled: bool = False
    keep_alive_interval_seconds: int = Field(300, ge=60, le=3600)

    @field_validator("url", mode="after")
    @classmethod
    def cast_url_to_string(cls, v: PostgresDsn) -> str:
        return str(v)


class UpdateConnectionRequest(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=120)
    keep_alive_enabled: bool | None = None
    keep_alive_interval_seconds: int | None = Field(None, ge=60, le=3600)


class ConnectionResponse(BaseModel):
    id: UUID
    workspace_id: UUID
    name: str
    slug: str
    host: str | None
    port: int | None
    database: str | None
    pg_version: str | None
    cloud_provider: CloudProvider
    status: ConnectionStatus
    last_connected_at: str | None
    keep_alive_enabled: bool
    keep_alive_interval_seconds: int
    capabilities: dict
    created_at: str

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_model(cls, c: Connection) -> "ConnectionResponse":
        return cls(
            id=c.id,
            workspace_id=c.workspace_id,
            name=c.name,
            slug=c.slug,
            host=c.host,
            port=c.port,
            database=c.database,
            pg_version=c.pg_version,
            cloud_provider=c.cloud_provider,
            status=c.status,
            last_connected_at=(
                c.last_connected_at.isoformat() if c.last_connected_at else None
            ),
            keep_alive_enabled=c.keep_alive_enabled,
            keep_alive_interval_seconds=c.keep_alive_interval_seconds,
            capabilities=c.capabilities,
            created_at=c.created_at.isoformat(),
        )


class PingResponse(BaseModel):
    success: bool
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id

def _encrypt_url(url: str) -> str:
    """Fernet-encrypt a connection URL before persisting it."""
    return encrypt_secret(url)


def _decrypt_url(encrypted: str) -> str:
    """Fernet-decrypt a stored connection URL."""
    return decrypt_secret(encrypted)


async def _get_connection_or_404(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> Connection:
    conn = await service.get_connection(db, connection_id, workspace_id)
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found.",
        )
    return conn


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/test", response_model=TestConnectionResponse)
async def test_connection(
    body: TestConnectionRequest,
    user: CurrentUser,
):
    """
    Test a PostgreSQL URL without persisting anything.
    Returns connectivity result, version info, and detected provider.
    """
    # FIX: Explicitly cast body.url to str to satisfy Pylance
    result = await service.test_connection(str(body.url))
    return TestConnectionResponse(**result.model_dump())


@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    body: CreateConnectionRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
):
    """
    Test a URL and persist it as a named connection in the workspace.
    Fails if the connection test fails.
    """
    # DEFENSE: Lock 24/7 background cron behind premium tier
    if body.keep_alive_enabled and user.tier == "free":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Background Keep-Alive (24/7) is a premium feature. Free tier databases are kept awake automatically while the Calyphant tab is open."
        )

    # 1. NETWORK IO: Execute long-running test before touching the DB
    result = await service.test_connection(str(body.url))
    if not result.success:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Connection test failed: {result.error}",
        )

    encrypted_url = _encrypt_url(str(body.url))

    create_data = ConnectionCreateRequest(
        name=body.name,
        keep_alive_enabled=body.keep_alive_enabled,
        keep_alive_interval_seconds=body.keep_alive_interval_seconds,
    )

    # 2. DB WRITE: Open short-lived session to save
    async with get_db_context() as db:
        conn = await service.create_connection(
            db=db,
            workspace_id=workspace_id,
            user_id=user.id,
            data=create_data,
            encrypted_url=encrypted_url,
            test_result=result,
        )
        return ConnectionResponse.from_orm_model(conn)


@router.get("", response_model=PaginatedResponse[ConnectionResponse])
async def list_connections(
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    limit: int = 50,
    offset: int = 0,
):
    page = await service.list_connections(
        db, workspace_id, limit=limit, offset=offset
    )
    return PaginatedResponse(
        items=[ConnectionResponse.from_orm_model(c) for c in page.items],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    conn = await _get_connection_or_404(connection_id, workspace_id, db)
    return ConnectionResponse.from_orm_model(conn)


@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: UUID,
    body: UpdateConnectionRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Update connection metadata or keep-alive settings.
    """
    # DEFENSE: Lock 24/7 background cron behind premium tier
    if body.keep_alive_enabled is True and user.tier == "free":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Background Keep-Alive (24/7) is a premium feature. Upgrade to enable."
        )

    update_data = ConnectionUpdateRequest(**body.model_dump(exclude_unset=True))
    
    conn = await service.update_connection(
        db, connection_id, workspace_id, update_data
    )
    
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found.")
        
    return ConnectionResponse.from_orm_model(conn)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    deleted = await service.delete_connection(db, connection_id, workspace_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Connection not found.")


@router.post("/{connection_id}/ping", response_model=PingResponse)
async def ping_connection(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Manual keep-alive ping. Decrypts the URL, fires SELECT 1, updates status.
    """
    conn = await _get_connection_or_404(connection_id, workspace_id, db)
    url = _decrypt_url(conn.encrypted_url)
    success = await service.ping_connection(url)

    new_status = ConnectionStatus.active if success else ConnectionStatus.unreachable
    await service.mark_connection_status(
        db,
        connection_id,
        new_status,
        error=None if success else "Ping failed.",
    )

    return PingResponse(
        success=success,
        message="Database is alive." if success else "Ping failed. Database may be sleeping.",
    )


@router.post("/{connection_id}/heartbeat", status_code=status.HTTP_202_ACCEPTED)
async def connection_presence_heartbeat(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
):
    """
    Ephemeral UI heartbeat.
    Reads from Redis cache to avoid heavy Fernet decryption on every pulse.
    Returns 202 Accepted instantly and fires the network pulse in the background.
    """
    redis = await get_redis()
    cache_key = f"calyphant:conn_url:{connection_id}"
    
    url = await redis.get(cache_key)
    
    if not url:
        # Cache miss: Fallback to short-lived DB fetch and Fernet decryption
        async with get_db_context() as db:
            conn = await _get_connection_or_404(connection_id, workspace_id, db)
            encrypted_url = conn.encrypted_url
            
        url = _decrypt_url(encrypted_url)
        
        # Cache the plaintext DSN for 5 minutes (300 seconds)
        # This perfectly covers the 4-minute frontend polling cycle.
        await redis.setex(cache_key, 300, url)
    else:
        # Redis returns bytes if decode_responses=False
        url = url.decode("utf-8") if isinstance(url, bytes) else url

    # Fire and forget
    import asyncio
    asyncio.create_task(service.execute_presence_heartbeat(url))
    
    return {"status": "acknowledged"}


@router.post("/{connection_id}/refresh", response_model=ConnectionResponse)
async def refresh_connection(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
):
    """
    Re-test the connection and update stored metadata (version, capabilities, provider).
    Useful after enabling extensions or upgrading the database.
    Split into discrete DB transactions to protect the connection pool.
    """
    # 1. DB READ: Fetch encrypted URL instantly
    async with get_db_context() as db:
        conn = await _get_connection_or_404(connection_id, workspace_id, db)
        encrypted_url = conn.encrypted_url

    # 2. NETWORK IO: Decrypt and test (Pool is NOT held here)
    url = _decrypt_url(encrypted_url)
    result = await service.test_connection(url)

    # 3. DB WRITE: Re-open session to apply updates instantly
    async with get_db_context() as db:
        conn = await _get_connection_or_404(connection_id, workspace_id, db)
        
        update_data = ConnectionUpdateRequest()
        updated = await service.update_connection(
            db, connection_id, workspace_id, update_data
        )

        # Manually patch metadata fields not covered by UpdateConnectionRequest
        if result.success:
            conn.pg_version = result.pg_version
            conn.pg_version_num = result.pg_version_num
            conn.capabilities = result.capabilities or {}
            conn.status = ConnectionStatus.active
            if result.cloud_provider:
                # If it's plain text, convert it to the official CloudProvider badge
                if isinstance(result.cloud_provider, str):
                    conn.cloud_provider = CloudProvider(result.cloud_provider)
                else:
                    conn.cloud_provider = result.cloud_provider
            await db.commit()
            await db.refresh(conn)
        else:
            await service.mark_connection_status(
                db, connection_id, ConnectionStatus.unreachable, error=result.error
            )

        return ConnectionResponse.from_orm_model(conn)
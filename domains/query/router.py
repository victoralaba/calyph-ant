# domains/query/router.py

"""
Query router.

Endpoints:
  POST   /query/{connection_id}/execute        — run SQL, return results
  POST   /query/{connection_id}/stream         — SSE stream for large results
  POST   /query/{connection_id}/cancel         — cancel a running query by PID
  POST   /query/{connection_id}/explain        — EXPLAIN / EXPLAIN ANALYZE
  POST   /query/{connection_id}/describe       — column type metadata (for charts)
  GET    /query/{connection_id}/history        — per-user query history
  DELETE /query/{connection_id}/history        — clear history
  GET    /query/{connection_id}/saved          — list saved queries
  POST   /query/{connection_id}/saved          — save a query
  DELETE /query/{connection_id}/saved/{id}     — delete saved query

Changes
-------
STREAM-LEAK-1
    The SSE stream endpoint previously used a raw asyncpg.connect() inside
    an async generator. If the HTTP client disconnected before the generator
    was exhausted, the finally block might never run, leaking the database
    connection indefinitely.

    Fix: the connection is now opened OUTSIDE the generator in the route
    handler using an explicit try/finally. The StreamingResponse is wrapped
    so that when the ASGI server closes the response (client disconnect or
    generator exhaustion), the connection is guaranteed to be closed.

    We use an anyio CancelScope approach: the generator is wrapped in a
    context manager that closes the pg_conn on any exit path including
    cancellation.

STREAM-LEAK-2
    Same fix applied to the table stream endpoint in tables/router.py.

STREAM-TIMEOUT
    StreamRequest now accepts execution_timeout_seconds (default 120, max
    600). This is passed through to stream_query() which sets
    statement_timeout on the PostgreSQL session so the server cancels the
    backend process rather than leaving it running after the client leaves.

CANCEL-ENDPOINT
    POST /query/{connection_id}/cancel accepts {"pid": int} and calls
    pg_cancel_backend(pid) on a fresh connection. Returns whether
    cancellation succeeded. The PID is obtained from the "meta" SSE event
    emitted as the first item from the stream endpoint.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator
from uuid import UUID

import asyncio
import asyncpg
from asyncpg.pool import PoolConnectionProxy  # <-- ADD THIS IMPORT
from fastapi import APIRouter, Depends, HTTPException, Query, status, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from dataclasses import dataclass
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from domains.billing.flutterwave import TIER_LIMITS
from domains.query import service
from domains.query.service import DEFAULT_STREAM_TIMEOUT, MAX_STREAM_TIMEOUT
from domains.users.service import User

router = APIRouter(prefix="/query", tags=["query"])


# ---------------------------------------------------------------------------
# Schemas (The Locked Contract)
# ---------------------------------------------------------------------------

class ExecuteRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    # UI NOTE: The client can only ask for a SMALLER limit for pagination. 
    # The server will clamp this value against the user's maximum entitlement.
    requested_limit: int = Field(100, ge=1, le=50_000)

class StreamRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    chunk_size: int = Field(500, ge=100, le=2000)
    # UI NOTE: 'execution_timeout_seconds' has been REMOVED. 
    # The server dictates stream lifespan based on the billing tier.


class CancelRequest(BaseModel):
    pid: int = Field(..., description="Backend PID from the 'meta' SSE event.")


class ExplainRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    analyze: bool = False
    buffers: bool = False


class DescribeRequest(BaseModel):
    sql: str = Field(
        ...,
        min_length=1,
        description=(
            "A SELECT query whose result columns should be described. "
            "The query is never executed — only its result type metadata "
            "is resolved."
        ),
    )


class SaveQueryRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    sql: str = Field(..., min_length=1)
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    is_public: bool = False


# ---------------------------------------------------------------------------
# Entitlements Engine (The Gateway Physics)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ComputeProfile:
    timeout_seconds: float
    max_rows: int
    is_flagged: bool

async def resolve_compute_profile(
    user: CurrentUser, 
    db: AsyncSession = Depends(get_db)
) -> ComputeProfile:
    """
    Evaluates the strict hierarchy of compute entitlements.
    Order of authority:
    1. Penalty Box (Flagged Users)
    2. Enterprise Custom Overrides
    3. Baseline Tier Physics
    """
    db_user = await db.get(User, user.id)
    if not db_user:
        raise HTTPException(status_code=403, detail="User validation failed.")

    # LEVEL 1: The Penalty Box (Survival limits only)
    if db_user.is_flagged:
        return ComputeProfile(timeout_seconds=5.0, max_rows=100, is_flagged=True)

    tier_physics = TIER_LIMITS.get(db_user.tier, TIER_LIMITS["free"])
    
    # LEVEL 2: Enterprise Overrides OR LEVEL 3: Tier Defaults
    timeout = float(
        db_user.custom_query_timeout 
        if db_user.custom_query_timeout is not None 
        else tier_physics.get("query_timeout_seconds", 30.0)
    )
    
    rows = int(
        db_user.custom_max_rows 
        if db_user.custom_max_rows is not None 
        else tier_physics.get("streaming_rows", 10_000)
    )
    
    # -1 implies unlimited in our billing config. Cap it at something sane for the DB's sake.
    if rows == -1:
        rows = 10_000_000
        
    return ComputeProfile(timeout_seconds=timeout, max_rows=rows, is_flagged=False)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

async def _get_url(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> str:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    return url

def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/execute")
async def execute_query(
    connection_id: UUID,
    body: ExecuteRequest,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    profile: ComputeProfile = Depends(resolve_compute_profile), # <-- Physics Injected
):
    """Execute a SQL query via persistent hot socket."""
    url = await _get_url(connection_id, workspace_id, db)
    
    # Clamp the requested rows to the user's actual entitlement
    actual_limit = min(body.requested_limit, profile.max_rows)
    
    pool = await service.pool_manager.get_pool(connection_id, url)
    
    try:
        async with pool.acquire() as pg_conn:
            return await service.execute_query(
                pg_conn=pg_conn,
                db=db,
                workspace_id=workspace_id,
                sql=body.sql,
                connection_id=connection_id,
                user_id=user.id,
                background_tasks=background_tasks,
                timeout=profile.timeout_seconds, # <-- Forced Server Timeout
                max_rows=actual_limit,           # <-- Forced Server Limit
            )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database execution failed: {exc}")


@router.post("/{connection_id}/stream")
async def stream_query(
    request: Request,
    connection_id: UUID,
    body: StreamRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    profile: ComputeProfile = Depends(resolve_compute_profile), # <-- Physics Injected
):
    """
    Stream large query results as Server-Sent Events.
    """
    url = await _get_url(connection_id, workspace_id, db)

    async def _generate_events(pg_conn: asyncpg.Connection | PoolConnectionProxy) -> AsyncGenerator[str, None]:
        yield f": {' ' * 4096}\n\n"
        
        raw_pid = await pg_conn.fetchval("SELECT pg_backend_pid()")
        pid: int = int(raw_pid) if raw_pid is not None else 0
        
        from loguru import logger
        
        try:
            stream = service.stream_query(
                pg_conn,
                body.sql,
                body.chunk_size,
                execution_timeout_seconds=profile.timeout_seconds, # <-- Forced Server Timeout
            )
            
            stream_iter = aiter(stream)
            while True:
                if await request.is_disconnected():
                    logger.warning(f"Client disconnected mid-stream. Assassinating PID: {pid}")
                    if pid > 0:
                        await service.cancel_query(url, pid)
                    break

                try:
                    event = await asyncio.wait_for(anext(stream_iter), timeout=10.0)
                    yield f"data: {json.dumps(event, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
                except StopAsyncIteration:
                    break
                    
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except asyncio.CancelledError:
            logger.warning(f"Stream task forcefully cancelled by ASGI. Assassinating PID: {pid}")
            if pid > 0:
                await service.cancel_query(url, pid)
            raise
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'error': str(exc)})}\n\n"

    async def _safe_stream() -> AsyncGenerator[str, None]:
        pool = await service.pool_manager.get_pool(connection_id, url)
        async with pool.acquire() as pg_conn:
            async for chunk in _generate_events(pg_conn):
                yield chunk

    return StreamingResponse(
        _safe_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Content-Encoding": "none",
            "Connection": "keep-alive",
            "Transfer-Encoding": "chunked",
        },
    )


@router.post("/{connection_id}/cancel")
async def cancel_query(
    connection_id: UUID,
    body: CancelRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Cancel a running query by its PostgreSQL backend PID.

    The PID is returned as the first SSE event from the stream endpoint:
        data: {"type": "meta", "pid": 12345}

    Uses pg_cancel_backend() which sends SIGINT to the backend process —
    this cancels the current query but leaves the connection alive.

    Returns:
        {"cancelled": bool, "pid": int, "message": str}
    """
    url = await _get_url(connection_id, workspace_id, db)
    result = await service.cancel_query(url, body.pid)
    return result


@router.post("/{connection_id}/explain")
async def explain_query(
    connection_id: UUID,
    body: ExplainRequest,
    user: CurrentUser,
    background_tasks: BackgroundTasks,                          # <-- NEW: Inject tasks
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    profile: ComputeProfile = Depends(resolve_compute_profile), # <-- NEW: Inject physics
):
    url = await _get_url(connection_id, workspace_id, db)
    pool = await service.pool_manager.get_pool(connection_id, url)
    
    try:
        async with pool.acquire() as pg_conn:
            return await service.explain_query(
                pg_conn, 
                body.sql, 
                user_id=user.id,
                background_tasks=background_tasks,
                analyze=body.analyze, 
                buffers=body.buffers,
                timeout=profile.timeout_seconds,
            )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Explain execution failed: {exc}")
    

@router.post("/{connection_id}/describe")
async def describe_result_columns(
    connection_id: UUID,
    body: DescribeRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),    
    db: AsyncSession = Depends(get_db),
):
    url = await _get_url(connection_id, workspace_id, db)
    pool = await service.pool_manager.get_pool(connection_id, url)
    
    try:
        async with pool.acquire() as pg_conn:
            return await service.describe_result_columns(pg_conn, body.sql)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Column description failed: {exc}")


@router.get("/{connection_id}/history")
async def get_history(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    records = await service.get_query_history(
        db, user.id, workspace_id, connection_id, limit=limit, offset=offset
    )
    return {
        "history": [
            {
                "id": str(r.id),
                "sql": r.sql,
                "duration_ms": r.duration_ms,
                "row_count": r.row_count,
                "error": r.error,
                "workspace_id": str(r.workspace_id),
                "executed_at": r.executed_at.isoformat(),
            }
            for r in records
        ]
    }


@router.delete("/{connection_id}/history", status_code=status.HTTP_204_NO_CONTENT)
async def clear_history(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    await service.clear_query_history(db, user.id, workspace_id, connection_id)


@router.get("/{connection_id}/saved")
async def list_saved(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")

    queries = await service.list_saved_queries(db, workspace_id, connection_id)
    return {
        "saved": [
            {
                "id": str(q.id),
                "name": q.name,
                "description": q.description,
                "sql": q.sql,
                "tags": q.tags,
                "is_public": q.is_public,
                "workspace_id": str(q.workspace_id),
                "created_at": q.created_at.isoformat(),
            }
            for q in queries
        ]
    }


@router.post("/{connection_id}/saved", status_code=status.HTTP_201_CREATED)
async def save_query(
    connection_id: UUID,
    body: SaveQueryRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")

    record = await service.save_query(
        db=db,
        workspace_id=workspace_id,
        user_id=user.id,
        connection_id=connection_id,
        name=body.name,
        sql=body.sql,
        description=body.description,
        tags=body.tags,
        is_public=body.is_public,
    )
    return {
        "id": str(record.id),
        "name": record.name,
        "workspace_id": str(record.workspace_id),
    }


@router.delete(
    "/{connection_id}/saved/{query_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_saved_query(
    connection_id: UUID,
    query_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")

    deleted = await service.delete_saved_query(db, query_id, workspace_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Saved query not found.")
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

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from domains.query import service
from domains.query.service import DEFAULT_STREAM_TIMEOUT, MAX_STREAM_TIMEOUT

router = APIRouter(prefix="/query", tags=["query"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ExecuteRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    timeout: float = Field(30.0, ge=1.0, le=300.0)
    max_rows: int = Field(10_000, ge=1, le=50_000)


class StreamRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    chunk_size: int = Field(500, ge=100, le=2000)
    execution_timeout_seconds: float = Field(
        DEFAULT_STREAM_TIMEOUT,
        ge=5.0,
        le=MAX_STREAM_TIMEOUT,
        description=(
            "How long PostgreSQL will allow this query to run before cancelling it. "
            f"Default {DEFAULT_STREAM_TIMEOUT}s, max {MAX_STREAM_TIMEOUT}s."
        ),
    )


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
# Connection helper
# ---------------------------------------------------------------------------

async def _pg(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> asyncpg.Connection:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=15)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")


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
# Stream connection lifecycle manager  (STREAM-LEAK-1 fix)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _managed_pg_conn(url: str) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Async context manager that opens an asyncpg connection and guarantees
    it is closed on exit regardless of how the context is left — normal
    return, exception, or cancellation (client disconnect).
    """
    # Acquire the resource first. If this fails, it raises an exception 
    # immediately, and there is nothing to clean up.
    conn = await asyncpg.connect(dsn=url, timeout=15)
    
    try:
        yield conn
    finally:
        # If we reach here, we are guaranteed 'conn' is a valid Connection.
        try:
            await conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/execute")
async def execute_query(
    connection_id: UUID,
    body: ExecuteRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """Execute a SQL query and return paginated results."""
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        return await service.execute_query(
            pg_conn=pg_conn,
            db=db,
            workspace_id=workspace_id,
            sql=body.sql,
            connection_id=connection_id,
            user_id=user.id,
            timeout=body.timeout,
            max_rows=body.max_rows,
        )
    finally:
        await pg_conn.close()


@router.post("/{connection_id}/stream")
async def stream_query(
    connection_id: UUID,
    body: StreamRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Stream large query results as Server-Sent Events.

    The first SSE event is always a metadata frame:
        data: {"type": "meta", "pid": <backend_pid>}

    Use the pid to cancel via POST /query/{connection_id}/cancel if needed.

    Subsequent events are chunk frames:
        data: {"type": "chunk", "chunk": N, "columns": [...], "rows": [...], "count": N}

    On query timeout or error:
        data: {"type": "error", "error": "..."}

    Final event:
        data: {"type": "done"}

    STREAM-LEAK-1 fix: the database connection is managed outside the
    generator so it is guaranteed to be closed on client disconnect,
    generator exhaustion, or any exception path.
    """
    url = await _get_url(connection_id, workspace_id, db)

    async def _generate_events(pg_conn: asyncpg.Connection) -> AsyncGenerator[str, None]:
        """
        Inner generator that assumes pg_conn is already open and managed
        by the caller. Never opens or closes the connection itself.
        """
        try:
            async for event in service.stream_query(
                pg_conn,
                body.sql,
                body.chunk_size,
                execution_timeout_seconds=body.execution_timeout_seconds,
            ):
                yield f"data: {json.dumps(event, default=str)}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'error': str(exc)})}\n\n"

    async def _safe_stream() -> AsyncGenerator[str, None]:
        """
        Outer generator that owns the connection lifetime.
        The async with block ensures pg_conn.close() is always called,
        including when the ASGI server cancels the task on client disconnect.
        """
        async with _managed_pg_conn(url) as pg_conn:
            async for chunk in _generate_events(pg_conn):
                yield chunk

    return StreamingResponse(
        _safe_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
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
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Run EXPLAIN or EXPLAIN ANALYZE on a query.
    Returns both the JSON plan (for tree rendering) and the text plan.
    """
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        return await service.explain_query(
            pg_conn, body.sql, analyze=body.analyze, buffers=body.buffers
        )
    finally:
        await pg_conn.close()


@router.post("/{connection_id}/describe")
async def describe_result_columns(
    connection_id: UUID,
    body: DescribeRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),    
    db: AsyncSession = Depends(get_db),
):
    """
    Return column-level type metadata for a SELECT query's result set
    without executing the query or fetching any rows.

    Each entry in the "columns" array contains:
      name    — column name as it appears in the SELECT list
      pg_type — PostgreSQL base type name
      kind    — "numeric" | "temporal" | "categorical"
      nullable — always true (asyncpg prepare does not expose nullability)
    """
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        return await service.describe_result_columns(pg_conn, body.sql)
    finally:
        await pg_conn.close()


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
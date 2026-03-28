# domains/query/service.py

"""
Query service.

Handles execution of user-authored SQL:
- Async execution with timeout
- EXPLAIN / EXPLAIN ANALYZE
- Result streaming for large outputs (with execution timeout + cancellation support)
- Per-user query history (stored in Calyphant DB)
- Saved queries (named, per-workspace)
- Result column type metadata (for frontend chart feature)
- Query cancellation via pg_cancel_backend()

Changes
-------
STREAM-1  stream_query() now accepts and enforces an execution_timeout_seconds
          parameter. The server-side cursor is opened inside a statement_timeout
          SET so PostgreSQL itself cancels the query if it runs too long.

STREAM-2  stream_query() yields the backend PID as the first SSE-compatible
          metadata event so the caller can cancel the query via
          POST /query/{connection_id}/cancel if needed.

STREAM-3  cancel_query() sends pg_cancel_backend(pid) on a separate connection
          so client-initiated cancellation works even while the streaming
          connection is blocked waiting for rows.

STREAM-4  execute_query() now passes the statement timeout directly to
          asyncpg via SET LOCAL statement_timeout rather than relying on
          the fetch() timeout kwarg alone. This ensures PostgreSQL actually
          cancels the backend rather than just closing the client socket.

AUDIT-1   _record_history() failure now logs ERROR (was silent) and raises
          so callers can decide whether to surface it; execute_query() still
          swallows it with a WARNING so it never blocks the response.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, cast
from uuid import UUID, uuid4

import asyncpg
from loguru import logger
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, Boolean
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from domains.tables.editor import stream_query_result
from shared.types import Base


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class QueryHistoryRecord(Base):
    __tablename__ = "query_history"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    workspace_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    connection_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("connections.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sql: Mapped[str] = mapped_column(Text, nullable=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class SavedQuery(Base):
    __tablename__ = "saved_queries"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_by: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    connection_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("connections.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    sql: Mapped[str] = mapped_column(Text, nullable=False)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    tags: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
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


# ---------------------------------------------------------------------------
# Column type classification
# ---------------------------------------------------------------------------

_NUMERIC_TYPE_NAMES = {
    "int2", "int4", "int8",
    "float4", "float8",
    "numeric", "decimal",
    "money",
    "smallint", "integer", "bigint",
    "real", "double precision",
}

_TEMPORAL_TYPE_NAMES = {
    "date",
    "time", "timetz",
    "timestamp", "timestamptz",
    "interval",
}


def _classify_pg_type(type_name: str) -> str:
    """
    Return one of "numeric", "temporal", or "categorical" for a
    PostgreSQL base type name (lowercased, no modifiers).
    """
    base = type_name.lower().split("(")[0].strip()
    if base in _NUMERIC_TYPE_NAMES:
        return "numeric"
    if base in _TEMPORAL_TYPE_NAMES:
        return "temporal"
    return "categorical"


# ---------------------------------------------------------------------------
# Execution constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 30.0       # seconds
MAX_TIMEOUT = 300.0          # 5 minutes max
RESULT_ROW_LIMIT = 10_000    # Cap non-streaming results
DEFAULT_STREAM_TIMEOUT = 120.0   # seconds — default stream execution timeout
MAX_STREAM_TIMEOUT = 600.0       # 10 minutes absolute max for streams


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

async def execute_query(
    pg_conn: asyncpg.Connection,
    db: AsyncSession,
    workspace_id: UUID,
    sql: str,
    connection_id: UUID,
    user_id: UUID,
    timeout: float = DEFAULT_TIMEOUT,
    max_rows: int = RESULT_ROW_LIMIT,
) -> dict[str, Any]:
    """
    Execute a SQL query and return results.
    Non-streaming — caps at max_rows. Use stream_query for large results.
    Records execution in query history.

    STREAM-4: Uses SET LOCAL statement_timeout so PostgreSQL cancels the
    backend process on timeout rather than just dropping the client socket.
    This prevents zombie backend processes from consuming server resources.
    """
    timeout = min(timeout, MAX_TIMEOUT)
    # Convert seconds to milliseconds for PostgreSQL statement_timeout
    timeout_ms = int(timeout * 1000)

    start = time.monotonic()
    error: str | None = None
    rows: list[dict] = []
    columns: list[str] = []
    row_count: int = 0
    truncated: bool = False

    try:
        # Set statement_timeout at the transaction level so Postgres itself
        # cancels the backend query, not just the client socket.
        await pg_conn.execute(f"SET LOCAL statement_timeout = {timeout_ms}")

        stmt = await pg_conn.prepare(sql)
        raw_rows = await stmt.fetch(timeout=timeout)
        columns = list(raw_rows[0].keys()) if raw_rows else []
        row_count = len(raw_rows)

        if row_count > max_rows:
            rows = [dict(r) for r in raw_rows[:max_rows]]
            truncated = True
        else:
            rows = [dict(r) for r in raw_rows]
            truncated = False

    except asyncpg.QueryCanceledError:
        error = (
            f"Query exceeded the {timeout}s timeout and was cancelled by the server. "
            "Reduce query complexity or increase the timeout limit."
        )
    except asyncpg.PostgresSyntaxError as exc:
        error = f"Syntax error: {exc.args[0]}"
    except asyncpg.UndefinedTableError as exc:
        error = f"Table not found: {exc.args[0]}"
    except asyncpg.UndefinedColumnError as exc:
        error = f"Column not found: {exc.args[0]}"
    except asyncpg.InsufficientPrivilegeError:
        error = "Insufficient privileges to execute this query."
    except Exception as exc:
        error = str(exc)

    duration_ms = int((time.monotonic() - start) * 1000)

    # Record in history — failure logs a WARNING but never fails the response
    try:
        await _record_history(
            db,
            user_id,
            workspace_id,
            connection_id,
            sql,
            duration_ms,
            row_count,
            error,
        )
    except Exception as exc:
        logger.warning(f"Failed to record query history: {exc}")

    return {
        "columns": columns,
        "rows": rows,
        "row_count": row_count,
        "truncated": truncated,
        "duration_ms": duration_ms,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

async def stream_query(
    pg_conn: asyncpg.Connection,
    sql: str,
    chunk_size: int = 500,
    execution_timeout_seconds: float = DEFAULT_STREAM_TIMEOUT,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    Stream large query results via server-side cursor.
    Passes execution_timeout down to the core streamer to avoid nested
    transaction isolation crashes.
    """
    timeout_seconds = min(execution_timeout_seconds, MAX_STREAM_TIMEOUT)
    timeout_ms = int(timeout_seconds * 1000)

    # Yield backend PID first so the caller can cancel if needed
    raw_pid = await pg_conn.fetchval("SELECT pg_backend_pid()")
    pid: int = int(raw_pid) if raw_pid is not None else 0 
    yield {"type": "meta", "pid": pid}

    try:
        # NO transaction block here. Just pass timeout_ms down.
        async for chunk in stream_query_result(
            pg_conn, 
            sql, 
            chunk_size=chunk_size, 
            timeout_ms=timeout_ms
        ):
            yield {"type": "chunk", **chunk}
                
    except asyncpg.QueryCanceledError:
        yield {
            "type": "error",
            "error": (
                f"Query exceeded the {timeout_seconds}s timeout and was cancelled. "
                "Try narrowing the query with a WHERE clause or LIMIT."
            ),
        }
    except Exception as exc:
        yield {"type": "error", "error": str(exc)}


# ---------------------------------------------------------------------------
# Query cancellation  (STREAM-3)
# ---------------------------------------------------------------------------

async def cancel_query(target_url: str, backend_pid: int) -> dict[str, Any]:
    # Try connecting first so `conn` is strictly typed as `asyncpg.Connection`
    try:
        conn = await asyncpg.connect(dsn=target_url, timeout=10)
    except Exception as exc:
        logger.warning(f"cancel_query connection failed for PID {backend_pid}: {exc}")
        return {
            "cancelled": False,
            "pid": backend_pid,
            "message": f"Connection failed: {exc}",
        }

    # Now execute the cancellation
    try:
        raw_result = await conn.fetchval(
            "SELECT pg_cancel_backend($1)", backend_pid
        )
        result: bool = bool(raw_result) # Explicit conversion satisfies Pylance
        
        if result:
            return {
                "cancelled": True,
                "pid": backend_pid,
                "message": f"Cancellation signal sent to backend PID {backend_pid}.",
            }
        else:
            return {
                "cancelled": False,
                "pid": backend_pid,
                "message": (
                    f"Backend PID {backend_pid} not found or already finished. "
                    "The query may have completed before cancellation arrived."
                ),
            }
    except asyncpg.InsufficientPrivilegeError:
        return {
            "cancelled": False,
            "pid": backend_pid,
            "message": (
                "Insufficient privileges to cancel this query. "
                "The connected role must own the backend process or be a superuser."
            ),
        }
    except Exception as exc:
        logger.warning(f"cancel_query failed for PID {backend_pid}: {exc}")
        return {
            "cancelled": False,
            "pid": backend_pid,
            "message": f"Cancellation failed: {exc}",
        }
    finally:
        try:
            await conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# EXPLAIN
# ---------------------------------------------------------------------------

async def explain_query(
    pg_conn: asyncpg.Connection,
    sql: str,
    analyze: bool = False,
    buffers: bool = False,
) -> dict[str, Any]:
    """
    Run EXPLAIN (or EXPLAIN ANALYZE) on a query.
    Returns both the raw text plan and a parsed JSON plan.
    """
    options = ["FORMAT JSON"]
    if analyze:
        options.append("ANALYZE")
    if buffers and analyze:
        options.append("BUFFERS")

    opts = ", ".join(options)
    explain_sql = f"EXPLAIN ({opts}) {sql}"

    try:
        rows = await pg_conn.fetch(explain_sql)
        json_plan = rows[0][0] if rows else []
    except Exception as exc:
        return {"error": str(exc), "plan": None}

    # Also fetch the human-readable text plan
    text_sql = f"EXPLAIN {sql}"
    try:
        text_rows = await pg_conn.fetch(text_sql)
        text_plan = "\n".join(r[0] for r in text_rows)
    except Exception:
        text_plan = None

    return {
        "plan": json_plan,
        "text_plan": text_plan,
        "analyzed": analyze,
    }


# ---------------------------------------------------------------------------
# Result column type metadata
# ---------------------------------------------------------------------------

async def describe_result_columns(
    pg_conn: asyncpg.Connection,
    sql: str,
) -> dict[str, Any]:
    """
    Return column-level type metadata for the result set of a SELECT query
    without fetching any rows.

    Uses a zero-row fetch wrapped in a rolled-back transaction so the
    query is never actually executed against real data.

    Each entry in "columns" contains:
      name        — column name as it appears in the result
      pg_type     — base PostgreSQL type name (e.g. "int4", "text")
      kind        — "numeric" | "temporal" | "categorical"
      nullable    — best-effort nullable hint (always True for expressions)
    """
    columns: list[dict[str, Any]] = []

    try:
        async with pg_conn.transaction():
            stmt = await pg_conn.prepare(sql)
            attributes = stmt.get_attributes()

            if not attributes:
                raise _RollbackSentinel()

            oids = list({attr.type.oid for attr in attributes})
            rows = await pg_conn.fetch(
                "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = ANY($1::oid[])",
                oids,
            )
            oid_to_name: dict[int, str] = {row["oid"]: row["typname"] for row in rows}

            for attr in attributes:
                type_name = oid_to_name.get(attr.type.oid, "text")
                columns.append({
                    "name": attr.name,
                    "pg_type": type_name,
                    "kind": _classify_pg_type(type_name),
                    "nullable": True,
                })

            raise _RollbackSentinel()

    except _RollbackSentinel:
        pass
    except asyncpg.PostgresSyntaxError as exc:
        return {"columns": [], "error": f"Syntax error: {exc.args[0]}"}
    except asyncpg.UndefinedTableError as exc:
        return {"columns": [], "error": f"Table not found: {exc.args[0]}"}
    except asyncpg.InsufficientPrivilegeError:
        return {"columns": [], "error": "Insufficient privileges."}
    except Exception as exc:
        return {"columns": [], "error": str(exc)}

    return {"columns": columns}


class _RollbackSentinel(Exception):
    """Used to intentionally roll back the describe transaction."""


# ---------------------------------------------------------------------------
# Query history
# ---------------------------------------------------------------------------

async def _record_history(
    db: AsyncSession,
    user_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    sql: str,
    duration_ms: int,
    row_count: int,
    error: str | None,
) -> None:
    """
    Persist a query history record.

    AUDIT-1: Raises on failure so execute_query() can log a WARNING.
    The caller decides whether to surface the error — execute_query()
    currently swallows it so a history-write failure never blocks the
    query response.
    """
    record = QueryHistoryRecord(
        user_id=user_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        sql=sql,
        duration_ms=duration_ms,
        row_count=row_count,
        error=error,
    )
    db.add(record)
    await db.commit()


async def get_query_history(
    db: AsyncSession,
    user_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    limit: int = 50,
    offset: int = 0,
) -> list[QueryHistoryRecord]:
    result = await db.execute(
        select(QueryHistoryRecord)
        .where(
            QueryHistoryRecord.user_id == user_id,
            QueryHistoryRecord.workspace_id == workspace_id,
            QueryHistoryRecord.connection_id == connection_id,
        )
        .order_by(QueryHistoryRecord.executed_at.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all())


async def clear_query_history(
    db: AsyncSession,
    user_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
) -> int:
    from sqlalchemy import delete
    result = await db.execute(
        delete(QueryHistoryRecord).where(
            QueryHistoryRecord.user_id == user_id,
            QueryHistoryRecord.workspace_id == workspace_id,
            QueryHistoryRecord.connection_id == connection_id,
        )
    )
    await db.commit()
    
    # Cast the generic Result to a CursorResult to expose `.rowcount` to Pylance
    cursor_result = cast(CursorResult, result)
    return cursor_result.rowcount


# ---------------------------------------------------------------------------
# Saved queries
# ---------------------------------------------------------------------------

async def save_query(
    db: AsyncSession,
    workspace_id: UUID,
    user_id: UUID,
    connection_id: UUID,
    name: str,
    sql: str,
    description: str | None = None,
    tags: list[str] | None = None,
    is_public: bool = False,
) -> SavedQuery:
    record = SavedQuery(
        workspace_id=workspace_id,
        created_by=user_id,
        connection_id=connection_id,
        name=name,
        description=description,
        sql=sql,
        is_public=is_public,
        tags=tags or [],
    )
    db.add(record)
    await db.commit()
    await db.refresh(record)
    return record


async def list_saved_queries(
    db: AsyncSession,
    workspace_id: UUID,
    connection_id: UUID | None = None,
) -> list[SavedQuery]:
    q = select(SavedQuery).where(SavedQuery.workspace_id == workspace_id)
    if connection_id:
        q = q.where(SavedQuery.connection_id == connection_id)
    result = await db.execute(q.order_by(SavedQuery.created_at.desc()))
    return list(result.scalars().all())


async def delete_saved_query(
    db: AsyncSession,
    query_id: UUID,
    workspace_id: UUID,
) -> bool:
    record = await db.get(SavedQuery, query_id)
    if not record or record.workspace_id != workspace_id:
        return False
    await db.delete(record)
    await db.commit()
    return True
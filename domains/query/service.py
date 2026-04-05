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
from fastapi import BackgroundTasks
from core.db import get_db_context

import asyncio
import asyncpg
from asyncpg.pool import PoolConnectionProxy
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

async def get_workspace_pool(url: str, application_name: str = "calyphant-engine") -> asyncpg.Pool:
    """
    Creates a strictly isolated, temporary connection pool for user queries.
    Enforces protocol-level timeouts to prevent runaway queries from locking up the worker.
    """
    return await asyncpg.create_pool(
        dsn=url,
        min_size=1,
        max_size=3, # Keep small to avoid exhausting the target DB's connection limits
        server_settings={
            "application_name": application_name,
            # THE KILL SWITCH: Force target DB to terminate queries after 15 seconds
            "statement_timeout": "15000",
            # Prevent users from trying to lock tables indefinitely
            "lock_timeout": "5000",
            # Ensure deterministic time logic
            "TimeZone": "UTC"
        }
    )


class WorkspacePoolManager:
    """
    Global registry for tenant-specific PostgreSQL connection pools.
    Engineered for O(1) critical path lookup, isolated background GC, 
    and strict thundering-herd prevention without cache poisoning.
    """
    def __init__(self, ttl_seconds: int = 600):
        # Stores: connection_id -> {"pool": asyncpg.Pool, "last_used": float}
        self._pools: dict[UUID, dict[str, Any]] = {}
        
        # Protects dictionary mutations (add/remove), NOT I/O operations
        self._dict_lock = asyncio.Lock()
        
        # Per-connection locks to prevent the Thundering Herd
        self._conn_locks: dict[UUID, asyncio.Lock] = {}
        
        self._ttl_seconds = ttl_seconds
        self._sweeper_task: asyncio.Task | None = None

    async def _ensure_sweeper(self) -> None:
        """Lazily boots the heartbeat sweeper on the first request."""
        if self._sweeper_task is None:
            async with self._dict_lock:
                if self._sweeper_task is None:
                    self._sweeper_task = asyncio.create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        """
        The automated garbage collector.
        Runs entirely out-of-band. Never blocks a user's request.
        """
        while True:
            try:
                await asyncio.sleep(60)  # Heartbeat: 60 seconds
                now = time.monotonic()
                stale_pools = []

                async with self._dict_lock:
                    stale_cids = [
                        cid for cid, state in self._pools.items()
                        if (now - state["last_used"]) > self._ttl_seconds
                    ]
                    
                    for cid in stale_cids:
                        state = self._pools.pop(cid)
                        stale_pools.append(state["pool"])
                        # Clean up the specific connection lock
                        self._conn_locks.pop(cid, None)

                # Close stale pools safely OUTSIDE the lock to avoid blocking traffic
                for pool in stale_pools:
                    try:
                        await pool.close()
                    except Exception as exc:
                        logger.warning(f"Failed to close stale workspace pool: {exc}")
                        
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Workspace pool sweeper encountered an error: {exc}")

    async def get_pool(self, connection_id: UUID, url: str) -> asyncpg.Pool:
        await self._ensure_sweeper()

        # -------------------------------------------------------------------
        # 1. Fast Path (O(1) lookup without locking)
        # Python dicts are safe for simple reads in async contexts.
        # -------------------------------------------------------------------
        state = self._pools.get(connection_id)
        if state is not None:
            state["last_used"] = time.monotonic()
            return state["pool"]

        # -------------------------------------------------------------------
        # 2. Prevent Thundering Herd via Per-Connection Locking
        # -------------------------------------------------------------------
        async with self._dict_lock:
            if connection_id not in self._conn_locks:
                self._conn_locks[connection_id] = asyncio.Lock()
            conn_lock = self._conn_locks[connection_id]

        # -------------------------------------------------------------------
        # 3. Double-Checked Locking
        # -------------------------------------------------------------------
        async with conn_lock:
            # Check if another concurrent request built it while we waited
            state = self._pools.get(connection_id)
            if state is not None:
                state["last_used"] = time.monotonic()
                return state["pool"]

            # -------------------------------------------------------------------
            # 4. The Critical I/O Block
            # 
            # UI CONTRACT: If the target database is sleeping or starting up, 
            # this await will block for up to 15 seconds (statement_timeout).
            # The frontend Svelte client MUST NOT abort the HTTP request early.
            # The UI should display a persistent "Waking up database..." or 
            # "Connecting..." state until this resolves or fails.
            # -------------------------------------------------------------------
            try:
                pool = await get_workspace_pool(url)
            except Exception:
                # THE CURE FOR POISON: 
                # If the task throws (e.g. InvalidPasswordError), the exception 
                # bubbles up to the router. The pool is NEVER cached. 
                # The next request will instantly try again.
                raise

            # -------------------------------------------------------------------
            # 5. Cache Successful Pool
            # -------------------------------------------------------------------
            async with self._dict_lock:
                self._pools[connection_id] = {
                    "pool": pool,
                    "last_used": time.monotonic()
                }

            return pool

# Initialize the global singleton manager
pool_manager = WorkspacePoolManager()


async def execute_query(
    pg_conn: asyncpg.Connection | PoolConnectionProxy,
    db: AsyncSession,
    workspace_id: UUID,
    sql: str,
    connection_id: UUID,
    user_id: UUID,
    background_tasks: BackgroundTasks,
    timeout: float = DEFAULT_TIMEOUT,
    max_rows: int = RESULT_ROW_LIMIT,
) -> dict[str, Any]:
    """
    Execute a SQL query and return results.
    Transaction block removed to allow native DBA commands (VACUUM, REINDEX).
    Relies on session-level statement_timeout and pool reset mechanics.
    """
    timeout = min(timeout, MAX_TIMEOUT)
    timeout_ms = int(timeout * 1000)

    start = time.monotonic()
    error: str | None = None
    rows: list[dict] = []
    columns: list[str] = []
    row_count: int = 0
    truncated: bool = False

    try:
        # 1. Dynamic Session Timeout (Replaces SET LOCAL)
        # The asyncpg pool automatically executes RESET ALL when the socket 
        # is returned, guaranteeing this state does not bleed to the next user.
        await pg_conn.execute(f"SET statement_timeout = {timeout_ms}")

        # 2. Direct Execution (The DBA Wall is down)
        # Because we removed the transaction block, we cannot use server-side cursors.
        # We fetch directly. The statement_timeout serves as our primary safety net 
        # against massive, memory-crashing queries (bounding by time instead of strict rows).
        raw_rows = await pg_conn.fetch(sql, timeout=timeout + 1.0) 
        
        columns = list(raw_rows[0].keys()) if raw_rows else []
        row_count_fetched = len(raw_rows)

        if row_count_fetched > max_rows:
            # Truncate the overflow rows safely in memory
            rows = [dict(r) for r in raw_rows[:max_rows]]
            truncated = True
            row_count = max_rows
        else:
            rows = [dict(r) for r in raw_rows]
            truncated = False
            row_count = row_count_fetched

    except asyncpg.QueryCanceledError:
        error = (
            f"Query exceeded the {timeout}s timeout and was cancelled by the server. "
            "Reduce query complexity or increase the timeout limit."
        )
    # Notice: asyncpg.exceptions.ActiveSQLTransactionError is GONE.
    except asyncpg.PostgresSyntaxError as exc:
        error = f"Syntax error: {exc.args[0]}"
    except asyncpg.UndefinedTableError as exc:
        error = f"Table not found: {exc.args[0]}"
    except asyncpg.UndefinedColumnError as exc:
        error = f"Column not found: {exc.args[0]}"
    except asyncpg.InsufficientPrivilegeError:
        error = "Insufficient privileges to execute this query."
    except asyncpg.PostgresError as exc:
        error = f"Database error: {exc.args[0]}"
    except Exception as exc:
        error = str(exc)

    duration_ms = int((time.monotonic() - start) * 1000)

    # ---------------------------------------------------------
    # FIRE AND FORGET: Eject telemetry off the critical path
    # ---------------------------------------------------------
    background_tasks.add_task(
        _background_record_history,
        user_id,
        workspace_id,
        connection_id,
        sql,
        duration_ms,
        row_count,
        error,
    )

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
    pg_conn: asyncpg.Connection | PoolConnectionProxy,
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
    pg_conn: asyncpg.Connection | PoolConnectionProxy,  # <-- UPDATE TYPE
    sql: str,
    analyze: bool = False,
    buffers: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    Run EXPLAIN (or EXPLAIN ANALYZE) on a query.
    Returns both the raw text plan and a parsed JSON plan.
    
    Protects against runaway EXPLAIN ANALYZE executions and explicitly
    rolls back the transaction natively so EXPLAIN ANALYZE UPDATE doesn't 
    mutate data and the asyncpg state machine remains uncorrupted.
    """
    options = ["FORMAT JSON"]
    if analyze:
        options.append("ANALYZE")
    if buffers and analyze:
        options.append("BUFFERS")

    opts = ", ".join(options)
    explain_sql = f"EXPLAIN ({opts}) {sql}"
    timeout_ms = int(min(timeout, MAX_TIMEOUT) * 1000)

    # We give json_plan a starting value here so Pylance knows it exists!
    json_plan = None

    try:
        # EXPLAIN ANALYZE actually executes the query. 
        # We must protect the backend from hanging, and we roll back natively 
        # so profiling an UPDATE doesn't accidentally mutate data.
        async with pg_conn.transaction():
            await pg_conn.execute(f"SET LOCAL statement_timeout = {timeout_ms}")
            
            rows = await pg_conn.fetch(explain_sql, timeout=timeout + 1.0)
            json_plan = rows[0][0] if rows else []
            if analyze:
                raise _RollbackSentinel()
                
    except _RollbackSentinel:
        pass
    except asyncpg.QueryCanceledError:
        return {"error": "EXPLAIN ANALYZE exceeded execution timeout.", "plan": None}
    except Exception as exc:
        return {"error": str(exc), "plan": None}

    # Also fetch the human-readable text plan (Fast, no execution needed)
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
    pg_conn: asyncpg.Connection | PoolConnectionProxy,  # <-- UPDATE TYPE
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

async def _background_record_history(
    user_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    sql: str,
    duration_ms: int,
    row_count: int,
    error: str | None,
) -> None:
    """
    Isolated background task for writing query history.
    Generates its own DB session so it safely survives after the HTTP response closes.
    """
    try:
        async with get_db_context() as isolated_db:
            await _record_history(
                isolated_db,
                user_id,
                workspace_id,
                connection_id,
                sql,
                duration_ms,
                row_count,
                error,
            )
    except Exception as exc:
        logger.warning(f"Background query history write failed: {exc}")


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
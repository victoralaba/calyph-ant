# domains/tables/router.py

"""
Tables router.

Endpoints — Row CRUD:
  GET    /tables/{connection_id}                              — list tables
  POST   /tables/{connection_id}/{table}/rows/query          — fetch rows (paginated)
  GET    /tables/{connection_id}/{table}/stream              — SSE stream
  POST   /tables/{connection_id}/{table}/rows                — insert row
  PATCH  /tables/{connection_id}/{table}/rows/{pk}           — update row
  DELETE /tables/{connection_id}/{table}/rows/{pk}           — delete row (two-phase)
  POST   /tables/{connection_id}/{table}/rows/bulk-delete    — bulk delete (two-phase)
  POST   /tables/{connection_id}/{table}/rows/bulk-update    — bulk update (two-phase)
  POST   /tables/{connection_id}/{table}/rows/{pk}/undo      — undo last update
  GET    /tables/{connection_id}/{table}/pk                  — detect primary key

Endpoints — Row editing presence (SAFETY-10):
  POST   /tables/{connection_id}/{table}/rows/{pk}/lock      — claim edit intent
  DELETE /tables/{connection_id}/{table}/rows/{pk}/lock      — release edit intent
  PUT    /tables/{connection_id}/{table}/rows/{pk}/lock      — renew (heartbeat)
  GET    /tables/{connection_id}/{table}/locks               — list active locks

Endpoints — Data Import:
  POST   /tables/{connection_id}/import/parse                — parse file, return preview
  POST   /tables/{connection_id}/import/execute              — execute confirmed import
  GET    /tables/{connection_id}/{table}/export/csv          — export table as CSV
  GET    /tables/{connection_id}/{table}/export/json         — export table as JSON
  POST   /tables/{connection_id}/import/sheets               — list Excel sheets

HTTP status mapping for concurrency errors
------------------------------------------
RowLockConflict  → 409  {"detail": {"message": str, "holder": dict|None}}
StaleRowError    → 409  {"detail": {"message": str, "conflicts": [...]}}
DuplicateRowError → 409 {"detail": {"message": str, "constraint": str, "db_detail": str}}

Changes
-------
STREAM-LEAK-2
    The table SSE stream endpoint previously opened a raw asyncpg connection
    inside an async generator. On client disconnect the finally block might
    never run, leaking the connection.

    Fix: mirrors the approach used in query/router.py — the connection is
    managed by a _managed_pg_conn() context manager that is the outermost
    wrapper of the generator chain. asynccontextmanager + an outer
    generator guarantees the connection is closed on any exit path.

SAFETY-11 (bulk_update dry-run)
    BulkUpdateRequest now includes confirmed: bool = False.

    When confirmed=False (new default):
        Returns a preview of what would change without writing anything:
          {
            "confirmed": False,
            "preview": [{
              "pk": ...,
              "status": "would_update" | "not_found" | "locked" | "stale",
              "current": {...},
              "proposed": {...},
              "changes": {"col": {"from": old, "to": new}},
              "lock_holder": {...} | None,
              "stale_conflicts": [...]
            }],
            "summary": {"would_update": N, "not_found": N, "locked": N, "stale": N}
          }

    When confirmed=True:
        Executes the update (original behaviour). The 409 responses for
        RowLockConflict and StaleRowError remain unchanged.

    Important: this is a BREAKING CHANGE for existing callers that call
    bulk-update without setting confirmed=true. Existing callers must add
    confirmed=true to retain the old execute-immediately behaviour.

SAFETY-12 (undo)
    New endpoint: POST /tables/{connection_id}/{table}/rows/{pk}/undo
    Reads the most recent update audit log entry for this row and
    re-applies its "before" snapshot. Returns the restored row or 404 if
    no undoable state exists.

PK type coercion
----------------
pk_value arrives as a plain string from the URL path (e.g. /rows/3).
asyncpg is strict about types — passing "3" for an INTEGER column raises:
  invalid input for query argument $1: '3' ('str' object cannot be
  interpreted as an integer)

_coerce_pk() queries information_schema.columns for the actual data type
of the pk_column and casts the string accordingly before it reaches any
asyncpg call. Supported casts: integer/bigint/smallint → int,
numeric/decimal/real/double → float, uuid → uuid.UUID, everything else
stays as str. Called at the top of every endpoint that takes a pk_value
path parameter.
"""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator
from uuid import UUID

import asyncpg
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import uuid4

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from domains.tables.editor import (
    ColumnFilter,
    DuplicateRowError,
    FKViolationError,
    RowLockConflict,
    SortOrder,
    StaleRowError,
    TableQuery,
    bulk_delete,
    bulk_update,
    delete_row,
    fetch_rows,
    get_column_names,
    get_primary_key_column,
    insert_row,
    stream_table,
    undo_row_update,
    update_row,
)
from domains.tables.importer import (
    ConflictStrategy,
    ColumnMapping,
    build_column_mappings,
    execute_import,
    parse_file,
    preview_import,
    rows_to_csv,
    rows_to_json,
    MAX_FILE_SIZE_BYTES,
)
from domains.tables.presence import (
    claim_row_lock,
    release_row_lock,
    renew_row_lock,
    list_table_locks,
)

router = APIRouter(prefix="/tables", tags=["tables"])


# ---------------------------------------------------------------------------
# Schemas — Row CRUD
# ---------------------------------------------------------------------------

class FetchRowsRequest(BaseModel):
    filters: list[dict] = Field(default_factory=list)
    sort: list[dict] = Field(default_factory=list)
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    columns: list[str] | None = None


class InsertRowRequest(BaseModel):
    data: dict[str, Any]


class UpdateRowRequest(BaseModel):
    data: dict[str, Any]
    row_version: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Snapshot of column values the client last read for this row. "
            "When supplied, the server verifies no column changed since "
            "the snapshot was taken before writing. "
            "Omit only for programmatic / internal callers."
        ),
    )


class BulkDeleteRequest(BaseModel):
    pk_column: str
    pk_values: list[Any] = Field(..., min_length=1, max_length=1000)
    confirmed: bool = Field(
        default=False,
        description=(
            "Submit with confirmed=false (default) to receive a dry-run "
            "preview. Re-submit with confirmed=true to execute."
        ),
    )
    cascade: bool = Field(
        default=False,
        description=(
            "When true, allow the database to cascade deletes to dependent "
            "rows via FK ON DELETE CASCADE constraints. When false, the "
            "delete will fail with 409 if any FK constraint would be violated."
        ),
    )


class BulkUpdateRequest(BaseModel):
    pk_column: str
    rows: list[dict[str, Any]] = Field(..., min_length=1, max_length=500)
    confirmed: bool = Field(
        default=False,
        description=(
            "Submit with confirmed=false (default) to receive a dry-run "
            "preview of what would change, including any lock or stale-row "
            "conflicts. Re-submit with confirmed=true to execute.\n\n"
            "Each row dict must include the pk_column value. "
            "Optionally include a '__version__' key with the column "
            "snapshot the client last read for that row to enable "
            "per-row optimistic concurrency checking."
        ),
    )


# ---------------------------------------------------------------------------
# Schemas — Import
# ---------------------------------------------------------------------------

class ColumnMappingRequest(BaseModel):
    source_name: str
    target_name: str
    inferred_type: str
    skip: bool = False


class ImportExecuteRequest(BaseModel):
    table_name: str
    schema_name: str = "public"
    column_mappings: list[ColumnMappingRequest]
    conflict_strategy: ConflictStrategy = ConflictStrategy.skip
    pk_column: str | None = None
    create_table_if_missing: bool = True
    parse_session_id: str


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


async def _pg(
    connection_id: UUID,
    workspace_id: UUID | None,  # <-- Update signature
    db: AsyncSession,
) -> asyncpg.Connection:

    """
    Open a managed asyncpg connection to a PostgreSQL connection.

    Raises 400 if workspace_id is None, and 404 if the connection is not found.
    Raises 503 if an exception occurs while connecting to the database.

    :param connection_id: The UUID of the connection to open.
    :param workspace_id: The UUID of the workspace the connection belongs to.
    :param db: The asyncpg database session.
    :return: An asyncpg connection to the database.
    """
    if not workspace_id:
        raise HTTPException(status_code=400, detail="Workspace ID is required.")
        
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=15)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")

async def _get_url(
    connection_id: UUID,
    workspace_id: UUID | None,  # <-- Update signature
    db: AsyncSession,
) -> str:
    if not workspace_id:
        raise HTTPException(status_code=400, detail="Workspace ID is required.")
        
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    return url


# ---------------------------------------------------------------------------
# PK type coercion
# ---------------------------------------------------------------------------

async def _coerce_pk(
    conn: asyncpg.Connection,
    table: str,
    schema: str,
    pk_column: str,
    pk_value: str,
) -> Any:
    """
    Cast a URL path string pk_value to the correct Python type for asyncpg.

    asyncpg is strict: passing "3" for an INTEGER column raises
    "invalid input for query argument $1: '3' ('str' object cannot be
    interpreted as an integer)".

    Queries information_schema.columns for the actual data_type of the
    pk column and casts accordingly:
      integer / bigint / smallint / serial / bigserial  → int
      numeric / decimal / real / double precision        → float
      uuid                                               → uuid.UUID
      everything else                                    → str (no change)

    Raises HTTPException 400 if the cast fails (e.g. "abc" for an int PK).
    """
    row = await conn.fetchrow(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name   = $2
          AND column_name  = $3
        """,
        schema, table, pk_column,
    )

    if row is None:
        # Column not found — let downstream handle it
        return pk_value

    data_type: str = row["data_type"].lower()

    try:
        if data_type in (
            "integer", "int", "int4",
            "bigint", "int8",
            "smallint", "int2",
            "serial", "bigserial", "smallserial",
        ):
            return int(pk_value)

        if data_type in (
            "numeric", "decimal",
            "real", "double precision",
            "float4", "float8",
        ):
            return float(pk_value)

        if data_type == "uuid":
            return uuid.UUID(pk_value)

    except (ValueError, AttributeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid value {pk_value!r} for primary key column "
                   f"'{pk_column}' (type {data_type}): {exc}",
        )

    return pk_value


# ---------------------------------------------------------------------------
# Stream connection lifecycle manager  (STREAM-LEAK-2 fix)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _managed_pg_conn(url: str):
    """
    Async context manager that opens an asyncpg connection and guarantees
    it is closed on any exit.
    """
    conn = await asyncpg.connect(dsn=url, timeout=15)
    try:
        yield conn
    finally:
        # Using a broad exception catch during close just in case the connection is already dead
        try:
            await conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Parse session store (Redis-backed, 30-minute TTL)
# ---------------------------------------------------------------------------

_PARSE_SESSION_TTL = 1800
_PARSE_KEY_PREFIX = "calyphant:import_session:"


async def _store_parse_session(session_id: str, data: dict) -> None:
    try:
        from core.db import get_redis, _fernet
        redis = await get_redis()
        raw = json.dumps(data, default=str).encode()
        if _fernet:
            raw = _fernet.encrypt(raw)
        await redis.setex(f"{_PARSE_KEY_PREFIX}{session_id}", _PARSE_SESSION_TTL, raw)
    except Exception as exc:
        logger.warning(f"Could not store parse session: {exc}")
        raise HTTPException(
            status_code=503,
            detail=(
                "Redis is required for the import workflow. "
                "Ensure Redis is running and reachable."
            ),
        )


async def _load_parse_session(session_id: str) -> dict | None:
    try:
        from core.db import get_redis, _fernet
        redis = await get_redis()
        raw = await redis.get(f"{_PARSE_KEY_PREFIX}{session_id}")
        if raw is None:
            return None
        if _fernet:
            raw = _fernet.decrypt(raw)
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:
        return None


async def _delete_parse_session(session_id: str) -> None:
    try:
        from core.db import get_redis
        redis = await get_redis()
        await redis.delete(f"{_PARSE_KEY_PREFIX}{session_id}")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Row CRUD routes
# ---------------------------------------------------------------------------

@router.get("/{connection_id}")
async def list_tables(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """List all tables in the schema with column and row estimates."""
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                t.table_name,
                t.table_type,
                pg_size_pretty(pg_total_relation_size(
                    quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)
                )) AS size,
                c.reltuples::bigint AS row_estimate,
                (SELECT count(*) FROM information_schema.columns col
                 WHERE col.table_schema = t.table_schema
                   AND col.table_name = t.table_name) AS column_count
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace
              AND n.nspname = t.table_schema
            WHERE t.table_schema = $1
            ORDER BY t.table_name
            """,
            schema_name,
        )
    finally:
        await pg_conn.close()

    return {"tables": [dict(r) for r in rows]}


@router.post("/{connection_id}/{table_name}/rows/query")
async def fetch_rows_endpoint(
    connection_id: UUID,
    table_name: str,
    body: FetchRowsRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    filters = [ColumnFilter(**f) for f in body.filters]
    sort = [SortOrder(**s) for s in body.sort]
    query = TableQuery(
        table=table_name,
        schema=schema_name,
        filters=filters,
        sort=sort,
        limit=body.limit,
        offset=body.offset,
        columns=body.columns,
    )
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        return await fetch_rows(pg_conn, query)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/{table_name}/stream")
async def stream_table_endpoint(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Stream all rows in a table as Server-Sent Events.

    STREAM-LEAK-2 fix: the database connection is managed by
    _managed_pg_conn so it is guaranteed to close on client disconnect,
    generator exhaustion, or any exception.
    """
    url = await _get_url(connection_id, user.workspace_id, db)
    query = TableQuery(table=table_name, schema=schema_name, limit=10_000_000)

    async def _safe_stream() -> AsyncGenerator[str, None]:
        async with _managed_pg_conn(url) as pg_conn:
            try:
                async for chunk in stream_table(pg_conn, query):
                    yield f"data: {json.dumps(chunk, default=str)}\n\n"
                yield f"data: {json.dumps({'done': True})}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"

    return StreamingResponse(
        _safe_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/{connection_id}/{table_name}/rows", status_code=status.HTTP_201_CREATED)
async def insert_row_endpoint(
    connection_id: UUID,
    table_name: str,
    body: InsertRowRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Insert a single row.

    Returns 409 when the insert violates a unique or primary-key constraint:
        {
            "detail": {
                "message": "Insert into '...' violates unique constraint '...'",
                "constraint": "uq_users_email",
                "db_detail": "Key (email)=(...) already exists."
            }
        }
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        return await insert_row(pg_conn, table_name, body.data, schema_name)
    except DuplicateRowError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": str(exc),
                "constraint": exc.constraint_name,
                "db_detail": exc.detail,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()


@router.patch("/{connection_id}/{table_name}/rows/{pk_value}")
async def update_row_endpoint(
    connection_id: UUID,
    table_name: str,
    pk_value: str,
    body: UpdateRowRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    pk_column: str = Query("id"),
):
    """
    Update a single row by primary key.

    Supply row_version with the column values your client last read to
    enable optimistic concurrency checking.

    409 — row locked:
        {"detail": {"message": str, "holder": dict|None}}

    409 — stale row:
        {"detail": {"message": str, "conflicts": [...]}}
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        coerced_pk = await _coerce_pk(pg_conn, table_name, schema_name, pk_column, pk_value)
        result = await update_row(
            pg_conn,
            table_name,
            pk_column,
            coerced_pk,
            body.data,
            schema_name,
            row_version=body.row_version,
            connection_id=connection_id,
        )
    except HTTPException:
        raise
    except RowLockConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": str(exc), "holder": exc.holder},
        )
    except StaleRowError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": str(exc), "conflicts": exc.conflicts},
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="Row not found.")
    return result


@router.delete("/{connection_id}/{table_name}/rows/{pk_value}")
async def delete_row_endpoint(
    connection_id: UUID,
    table_name: str,
    pk_value: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    pk_column: str = Query("id"),
    confirmed: bool = Query(
        default=False,
        description=(
            "confirmed=false (default): returns a preview of the row that "
            "would be deleted without executing. "
            "confirmed=true: executes the delete."
        ),
    ),
    cascade: bool = Query(
        default=False,
        description=(
            "When true, allow the database to cascade deletes to dependent "
            "rows via FK ON DELETE CASCADE constraints. When false, the "
            "delete will fail with 409 if any FK constraint would be violated."
        ),
    ),
):
    """
    Delete a single row by primary key.

    Two-phase confirmation — submit with confirmed=true to execute.
    Pass cascade=true to allow FK-cascaded deletes to dependent rows.
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        coerced_pk = await _coerce_pk(pg_conn, table_name, schema_name, pk_column, pk_value)
        result = await delete_row(
            pg_conn,
            table_name,
            pk_column,
            coerced_pk,
            schema_name,
            confirmed=confirmed,
            cascade=cascade,
        )
    except HTTPException:
        raise
    except FKViolationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": str(exc),
                "constraint": exc.constraint_name,
                "db_detail": exc.detail,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()

    return result


@router.post("/{connection_id}/{table_name}/rows/bulk-delete")
async def bulk_delete_endpoint(
    connection_id: UUID,
    table_name: str,
    body: BulkDeleteRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Bulk delete rows by primary key values.

    Phase 1 — confirmed=false (default):
        Returns a preview: how many rows would be deleted and a sample
        of their PKs. Nothing is written.

    Phase 2 — confirmed=true:
        Executes the delete. Returns {"confirmed": true, "deleted": N}.
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        result = await bulk_delete(
            pg_conn,
            table_name,
            body.pk_column,
            body.pk_values,
            schema_name,
            confirmed=body.confirmed,
            cascade=body.cascade,
        )
    except FKViolationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": str(exc),
                "constraint": exc.constraint_name,
                "db_detail": exc.detail,
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()
    return result


@router.post("/{connection_id}/{table_name}/rows/bulk-update")
async def bulk_update_endpoint(
    connection_id: UUID,
    table_name: str,
    body: BulkUpdateRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Update multiple rows. Two-phase: preview first, execute on confirm.

    Phase 1 — confirmed=false (default):
        Returns a per-row preview including what columns would change,
        any lock conflicts (holder identity), and any stale-row conflicts.
        Nothing is written. No 409s are raised — conflicts appear in the
        preview response under status="locked" or status="stale".

        Response shape:
          {
            "confirmed": false,
            "preview": [
              {
                "pk": <value>,
                "status": "would_update" | "not_found" | "locked" | "stale",
                "current": {...} | null,
                "proposed": {...},
                "changes": {"col": {"from": old, "to": new}},
                "lock_holder": {...} | null,
                "stale_conflicts": [...]
              }
            ],
            "summary": {
              "would_update": N, "not_found": N, "locked": N, "stale": N
            }
          }

    Phase 2 — confirmed=true:
        Executes all updates in a single transaction.

        409 responses (same shapes as single-row update):
          RowLockConflict → {"detail": {"message": str, "holder": dict|None}}
          StaleRowError   → {"detail": {"message": str, "conflicts": [...]}}

        Either error rolls back the entire batch.
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        result = await bulk_update(
            pg_conn,
            table_name,
            body.pk_column,
            body.rows,
            schema_name,
            connection_id=connection_id,
            confirmed=body.confirmed,
        )
    except RowLockConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": str(exc), "holder": exc.holder},
        )
    except StaleRowError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": str(exc), "conflicts": exc.conflicts},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()
    return result


@router.post("/{connection_id}/{table_name}/rows/{pk_value}/undo")
async def undo_row_update_endpoint(
    connection_id: UUID,
    table_name: str,
    pk_value: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    pk_column: str = Query("id"),
):
    """
    Undo the most recent UPDATE for a row.

    Reads the most recent "table_row.update" audit log entry for this row
    and re-applies its "before" snapshot as a blind UPDATE (no row_version
    check, since undo deliberately overwrites the current state).

    Limitations:
    - Single-level undo only (most recent update).
    - Requires the audit log to have a non-empty "before" snapshot for this
      row. Rows updated without row_version may have an empty "before".
    - Does not undo inserts or deletes — only updates.
    - The undo itself is recorded as a new audit entry (operation="undo").

    Returns:
        The restored row on success.

    Raises:
        404 — no undoable audit record exists for this row.
        400 — the row no longer exists (was deleted after the update).
        400 — the "before" snapshot is empty or contains only the primary key.
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        coerced_pk = await _coerce_pk(pg_conn, table_name, schema_name, pk_column, pk_value)
        restored = await undo_row_update(
            pg_conn,
            table_name,
            pk_column,
            coerced_pk,
            schema_name,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Undo failed: {exc}")
    finally:
        await pg_conn.close()

    if restored is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No undoable update found for row {pk_column}={pk_value!r} "
                f"in table '{table_name}'. "
                "Either this row has never been updated through Calyphant, "
                "or the audit log does not have a 'before' snapshot for it."
            ),
        )

    return {
        "undone": True,
        "pk_column": pk_column,
        "pk_value": pk_value,
        "table": table_name,
        "schema": schema_name,
        "restored_row": restored,
    }


@router.get("/{connection_id}/{table_name}/pk")
async def get_primary_key(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        pk = await get_primary_key_column(pg_conn, table_name, schema_name)
        cols = await get_column_names(pg_conn, table_name, schema_name)
    finally:
        await pg_conn.close()
    return {"pk_column": pk, "columns": cols}


# ---------------------------------------------------------------------------
# Row editing presence endpoints  (SAFETY-10)
# ---------------------------------------------------------------------------

@router.post(
    "/{connection_id}/{table_name}/rows/{pk_value}/lock",
    status_code=status.HTTP_200_OK,
)
async def claim_row_lock_endpoint(
    connection_id: UUID,
    table_name: str,
    pk_value: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Claim editing intent for a row. Returns conflict info if another
    user has already claimed it.
    """
    if not user.workspace_id:
        raise HTTPException(status_code=400, detail="Workspace ID is required.")
    
    result = await claim_row_lock(
        connection_id=connection_id,
        schema=schema_name,
        table=table_name,
        pk_value=pk_value,
        user_id=user.id,
        user_email=user.email,
        workspace_id=user.workspace_id,
    )

    if result.get("conflict"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": (
                    f"Row is currently being edited by "
                    f"{result['holder'].get('user_email', 'another user')}."
                ),
                "holder": result["holder"],
                "ttl": result.get("ttl"),
            },
        )

    return result


@router.delete(
    "/{connection_id}/{table_name}/rows/{pk_value}/lock",
    status_code=status.HTTP_200_OK,
)
async def release_row_lock_endpoint(
    connection_id: UUID,
    table_name: str,
    pk_value: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    if not user.workspace_id:
        raise HTTPException(status_code=400, detail="Workspace ID is required.")
    
    """Release editing intent for a row."""
    released = await release_row_lock(
        connection_id=connection_id,
        schema=schema_name,
        table=table_name,
        pk_value=pk_value,
        user_id=user.id,
        workspace_id=user.workspace_id,
    )
    return {"released": released}


@router.put(
    "/{connection_id}/{table_name}/rows/{pk_value}/lock",
    status_code=status.HTTP_200_OK,
)
async def renew_row_lock_endpoint(
    connection_id: UUID,
    table_name: str,
    pk_value: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """Heartbeat — renew an existing row lock claim."""
    renewed = await renew_row_lock(
        connection_id=connection_id,
        schema=schema_name,
        table=table_name,
        pk_value=pk_value,
        user_id=user.id,
    )
    return {"renewed": renewed}


@router.get("/{connection_id}/{table_name}/locks")
async def list_row_locks_endpoint(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """List all active row editing claims for a table."""
    locks = await list_table_locks(
        connection_id=connection_id,
        schema=schema_name,
        table=table_name,
    )
    return {"locks": locks}


# ---------------------------------------------------------------------------
# Import routes
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/import/parse")
async def parse_import_file(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    file: UploadFile = File(..., description="CSV, JSON, or Excel file to import"),
    table_name: str = Form(..., description="Target table name"),
    schema_name: str = Form(default="public"),
    sheet_name: str | None = Form(default=None, description="Excel sheet name (optional)"),
):
    """Phase 1 of import: parse the uploaded file and return a preview."""
    data = await file.read()
    if len(data) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum is {MAX_FILE_SIZE_BYTES // 1024 // 1024} MB.",
        )

    filename = file.filename or "upload"

    try:
        parsed = parse_file(data, filename, sheet_name=sheet_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {exc}")

    if not parsed.rows:
        raise HTTPException(status_code=400, detail="File is empty or contains no data rows.")

    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        preview = await preview_import(pg_conn, parsed, table_name, schema_name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()

    session_id = str(uuid4())
    session_data = {
        "rows": parsed.rows,
        "column_names": parsed.column_names,
        "total_rows": parsed.total_rows,
        "format": parsed.format,
        "connection_id": str(connection_id),
        "workspace_id": str(user.workspace_id),
        "table_name": table_name,
        "schema_name": schema_name,
    }

    await _store_parse_session(session_id, session_data)

    return {
        "parse_session_id": session_id,
        "filename": filename,
        "format": parsed.format,
        "total_rows": parsed.total_rows,
        "table_name": table_name,
        "schema_name": schema_name,
        "table_exists": preview.table_exists,
        "column_mappings": [
            {
                "source_name": m.source_name,
                "target_name": m.target_name,
                "inferred_type": m.inferred_type,
                "skip": m.skip,
                "sample_values": [str(v) for v in m.sample_values],
                "null_count": m.null_count,
                "non_null_count": m.non_null_count,
                "null_pct": round(
                    m.null_count / (m.null_count + m.non_null_count) * 100, 1
                ) if (m.null_count + m.non_null_count) > 0 else 0,
            }
            for m in preview.column_mappings
        ],
        "sample_rows": preview.sample_rows,
        "table_columns": preview.table_columns,
        "warnings": preview.warnings,
        "session_expires_in_seconds": _PARSE_SESSION_TTL,
    }


@router.post("/{connection_id}/import/execute")
async def execute_import_endpoint(
    connection_id: UUID,
    body: ImportExecuteRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """Phase 2 of import: execute the confirmed import."""
    session = await _load_parse_session(body.parse_session_id)
    if not session:
        raise HTTPException(
            status_code=400,
            detail=(
                "Import session not found or expired (30-minute TTL). "
                "Please re-upload your file and start the import again."
            ),
        )

    if (
        session.get("connection_id") != str(connection_id)
        or session.get("workspace_id") != str(user.workspace_id)
    ):
        raise HTTPException(
            status_code=403,
            detail="Import session does not belong to this connection.",
        )

    from domains.tables.importer import ParsedFile
    parsed = ParsedFile(
        rows=session["rows"],
        column_names=session["column_names"],
        total_rows=session["total_rows"],
        format=session["format"],
    )

    mappings = [
        ColumnMapping(
            source_name=m.source_name,
            target_name=m.target_name,
            inferred_type=m.inferred_type,
            skip=m.skip,
        )
        for m in body.column_mappings
    ]

    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        result = await execute_import(
            pg_conn=pg_conn,
            parsed=parsed,
            table_name=body.table_name,
            schema=body.schema_name,
            column_mappings=mappings,
            conflict_strategy=body.conflict_strategy,
            pk_column=body.pk_column,
            create_table_if_missing=body.create_table_if_missing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Import failed: {exc}")
    finally:
        await pg_conn.close()

    await _delete_parse_session(body.parse_session_id)

    return {
        "success": True,
        "table_name": body.table_name,
        "schema_name": body.schema_name,
        "table_created": result.table_created,
        "total": result.total,
        "inserted": result.inserted,
        "updated": result.updated,
        "skipped": result.skipped,
        "error_count": len(result.errors),
        "errors": result.errors[:100],
        "duration_ms": result.duration_ms,
        "rows_per_second": (
            round(result.total / (result.duration_ms / 1000))
            if result.duration_ms > 0 else 0
        ),
    }


@router.post("/{connection_id}/import/sheets")
async def list_excel_sheets(
    connection_id: UUID,
    user: CurrentUser,
    file: UploadFile = File(..., description="Excel file (.xlsx or .xls)"),
):
    """List all sheet names in an Excel file before import."""
    filename = file.filename or ""
    if not filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="File must be an Excel file (.xlsx or .xls).",
        )

    data = await file.read()
    try:
        import io
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        sheets = wb.sheetnames
        wb.close()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read Excel file: {exc}")

    return {
        "sheets": sheets,
        "default_sheet": sheets[0] if sheets else None,
    }


# ---------------------------------------------------------------------------
# Export routes
# ---------------------------------------------------------------------------

@router.get("/{connection_id}/{table_name}/export/csv")
async def export_table_csv(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    limit: int = Query(100_000, le=500_000),
):
    """Export table data as CSV (up to 500,000 rows)."""
    import io
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        query = TableQuery(table=table_name, schema=schema_name, limit=limit, offset=0)
        result = await fetch_rows(pg_conn, query)
        rows = result["rows"]
        columns = result["columns"]
    finally:
        await pg_conn.close()

    csv_bytes = rows_to_csv(rows, columns)
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table_name}.csv"'},
    )


@router.get("/{connection_id}/{table_name}/export/json")
async def export_table_json(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    limit: int = Query(100_000, le=500_000),
):
    """Export table data as JSON array."""
    import io
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        query = TableQuery(table=table_name, schema=schema_name, limit=limit, offset=0)
        result = await fetch_rows(pg_conn, query)
        rows = result["rows"]
    finally:
        await pg_conn.close()

    json_bytes = rows_to_json(rows)
    return StreamingResponse(
        io.BytesIO(json_bytes),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{table_name}.json"'},
    )
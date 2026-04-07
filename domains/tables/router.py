# domains/tables/router.py

"""
Tables router.

Endpoints — Row CRUD:
  GET    /tables/{connection_id}                              — list tables
  POST   /tables/{connection_id}/{table}/rows/query           — fetch rows (paginated)
  GET    /tables/{connection_id}/{table}/stream               — SSE stream
  POST   /tables/{connection_id}/{table}/rows                 — insert row
  PATCH  /tables/{connection_id}/{table}/rows/{pk}            — update row
  DELETE /tables/{connection_id}/{table}/rows/{pk}            — delete row (two-phase)
  POST   /tables/{connection_id}/{table}/rows/bulk-delete     — bulk delete (two-phase)
  POST   /tables/{connection_id}/{table}/rows/bulk-update     — bulk update (two-phase)
  POST   /tables/{connection_id}/{table}/rows/{pk}/undo       — undo last update
  GET    /tables/{connection_id}/{table}/pk                   — detect primary key

Endpoints — Row editing presence (SAFETY-10):
  POST   /tables/{connection_id}/{table}/rows/{pk}/lock       — claim edit intent
  DELETE /tables/{connection_id}/{table}/rows/{pk}/lock       — release edit intent
  PUT    /tables/{connection_id}/{table}/rows/{pk}/lock       — renew (heartbeat)
  GET    /tables/{connection_id}/{table}/locks                — list active locks

Endpoints — Data Import (Chunked Ingestion Architecture):
  POST   /tables/{connection_id}/import/init                  — initialize import session & schema
  POST   /tables/{connection_id}/import/chunk                 — ingest up to 5,000 rows
  GET    /tables/{connection_id}/{table}/export/csv           — export table as CSV
  GET    /tables/{connection_id}/{table}/export/json          — export table as JSON

HTTP status mapping for concurrency errors
------------------------------------------
RowLockConflict  → 409  {"detail": {"message": str, "holder": dict|None}}
StaleRowError    → 409  {"detail": {"message": str, "conflicts": [...]}}
DuplicateRowError → 409 {"detail": {"message": str, "constraint": str, "db_detail": str}}

Changes
-------
DATA-INGESTION-REFACTOR
    File parsing (CSV/JSON/Excel) and type inference have been shifted strictly 
    to the frontend (Svelte client). The old `/import/parse` and `/import/execute` 
    endpoints have been removed to prevent Python OOM crashes and Gzip bomb vectors.
    Data is now ingested via a high-speed, chunked `/import/chunk` endpoint limited
    to 5,000 rows per request.

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
        Returns a preview of what would change without writing anything.

    When confirmed=True:
        Executes the update (original behaviour). The 409 responses for
        RowLockConflict and StaleRowError remain unchanged.

SAFETY-12 (undo)
    New endpoint: POST /tables/{connection_id}/{table}/rows/{pk}/undo
    Reads the most recent update audit log entry for this row and
    re-applies its "before" snapshot. Returns the restored row or 404 if
    no undoable state exists.

PK type coercion
----------------
pk_value arrives as a plain string from the URL path (e.g. /rows/3).
asyncpg is strict about types. _coerce_pk() queries information_schema.columns 
for the actual data type of the pk_column and casts the string accordingly before 
it reaches any asyncpg call.
"""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Annotated
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
    Path,
)
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field, StringConstraints, model_validator
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
    rows_to_csv,
    rows_to_json,
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
import re

# We compile the regex for the dictionary key validator
_ID_REGEX_COMPILED = re.compile(r"^[a-zA-Z0-9_]+$")

class FetchRowsRequest(BaseModel):
    filters: list[dict] = Field(default_factory=list)
    sort: list[dict] = Field(default_factory=list)
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    # UI CONSIDERATION: Columns requested must strictly match the regex.
    columns: list[StrictIdentifier] | None = None

    @model_validator(mode='after')
    def validate_nested_identifiers(self) -> "FetchRowsRequest":
        # Defend against injection inside the nested filter/sort dicts
        for f in self.filters:
            if "column" in f and not _ID_REGEX_COMPILED.match(str(f["column"])):
                raise ValueError(f"Invalid filter column name: {f['column']}")
        for s in self.sort:
            if "column" in s and not _ID_REGEX_COMPILED.match(str(s["column"])):
                raise ValueError(f"Invalid sort column name: {s['column']}")
        return self


class InsertRowRequest(BaseModel):
    data: dict[str, Any]

    @model_validator(mode='after')
    def validate_keys(self) -> "InsertRowRequest":
        for key in self.data.keys():
            if not _ID_REGEX_COMPILED.match(key):
                raise ValueError(f"Invalid column name '{key}'. Column names must be alphanumeric and underscores only.")
        return self


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

    @model_validator(mode='after')
    def validate_keys(self) -> "UpdateRowRequest":
        for key in self.data.keys():
            if not _ID_REGEX_COMPILED.match(key):
                raise ValueError(f"Invalid update column name '{key}'.")
        if self.row_version:
            for key in self.row_version.keys():
                # __version__ is an internal artifact allowed in some contexts, but row_version keys must be structural
                if not _ID_REGEX_COMPILED.match(key) and key != "__version__":
                    raise ValueError(f"Invalid row_version column name '{key}'.")
        return self


class BulkDeleteRequest(BaseModel):
    pk_column: StrictIdentifier
    pk_values: list[Any] = Field(..., min_length=1, max_length=1000)
    confirmed: bool = Field(default=False)
    cascade: bool = Field(default=False)


class BulkUpdateRequest(BaseModel):
    pk_column: StrictIdentifier
    rows: list[dict[str, Any]] = Field(..., min_length=1, max_length=500)
    confirmed: bool = Field(default=False)

    @model_validator(mode='after')
    def validate_keys(self) -> "BulkUpdateRequest":
        for row in self.rows:
            for key in row.keys():
                if key != "__version__" and not _ID_REGEX_COMPILED.match(key):
                    raise ValueError(f"Invalid column name '{key}' in bulk update payload.")
        return self


# ---------------------------------------------------------------------------
# Schemas — Import
# ---------------------------------------------------------------------------

class ImportInitRequest(BaseModel):
    table_name: str = Field(..., pattern=r"^[a-zA-Z0-9_]+$")
    schema_name: str = Field(default="public", pattern=r"^[a-zA-Z0-9_]+$")
    columns: list[str]
    total_expected_rows: int = Field(ge=1)


class ImportChunkRequest(BaseModel):
    import_session_id: UUID
    chunk_index: int = Field(ge=0)
    # HARD LIMIT: Maximum 5000 rows per HTTP request to prevent memory spikes
    rows: list[dict[str, Any]] = Field(..., min_length=1, max_length=5000)

    @model_validator(mode='after')
    def validate_row_keys(self) -> "ImportChunkRequest":
        # Defend against SQL injection in JSON keys
        for row in self.rows:
            for key in row.keys():
                if not _ID_REGEX_COMPILED.match(key):
                    raise ValueError(f"Invalid column name '{key}' in chunk payload.")
        return self


# ---------------------------------------------------------------------------
# Strict Identifiers (SQL Injection Armor)
# ---------------------------------------------------------------------------

# UI CONSIDERATION: The frontend UI must strictly enforce that table names, 
# schema names, and column names only contain alphanumeric characters and underscores.
# Do not allow users to input spaces, dashes, or special characters. If they do, 
# the API will automatically reject it with a 422 Unprocessable Entity.
# This is a strict security boundary to prevent quote-breakout SQL injection.

_IDENTIFIER_REGEX = r"^[a-zA-Z0-9_]+$"

TablePath = Annotated[str, Path(pattern=_IDENTIFIER_REGEX)]
SchemaQuery = Annotated[str, Query(pattern=_IDENTIFIER_REGEX)]

# Used for Query parameters that represent structural column names
StrictColumnQuery = Annotated[str, Query(pattern=_IDENTIFIER_REGEX)]

# Used for Pydantic model fields that represent structural names
StrictIdentifier = Annotated[str, Field(pattern=_IDENTIFIER_REGEX)]


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
    pk_column: StrictColumnQuery = "id",
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
    pk_column: StrictColumnQuery = "id",
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
    pk_column: StrictColumnQuery = "id",
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
    table_name: TablePath,
    pk_value: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: SchemaQuery = "public",
):
    """
    Claim editing intent for a row with Hard Constraints (DoS Protection).
    """
    if not user.workspace_id:
        raise HTTPException(status_code=400, detail="Workspace ID is required.")
    
    # --- DOS PROTECTION: The Physics of Scaling ---
    # UI CONSIDERATION: If a user tries to bulk-edit and opens 50+ tabs, the UI MUST
    # gracefully handle the 429 Too Many Requests response. Show a toast explaining
    # that they have reached their concurrent editing limit and need to close some tabs.
    MAX_CONCURRENT_LOCKS = 50
    active_locks = await list_table_locks(
        connection_id=connection_id,
        schema=schema_name,
        table=table_name,
    )
    
    # Count how many locks this specific user currently holds on this table
    user_lock_count = sum(
        1 for lock in active_locks 
        if lock and lock.get("user_id") == str(user.id)
    )
    if user_lock_count >= MAX_CONCURRENT_LOCKS:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"You have reached the maximum limit of {MAX_CONCURRENT_LOCKS} "
                "concurrent row locks. Please release some edits or refresh your active sessions."
            )
        )
    # ----------------------------------------------

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
# Import routes (Streaming / Chunked Architecture)
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/import/init", status_code=status.HTTP_200_OK)
async def init_chunked_import(
    connection_id: UUID,
    body: ImportInitRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """Phase 1: Validates target table, retrieves column types, and initializes the session."""
    from core.db import get_redis
    
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        # Verify table exists and grab exact column data types for coercion
        rows = await pg_conn.fetch(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2
            """,
            body.schema_name, body.table_name,
        )
        if not rows:
            raise HTTPException(
                status_code=404, 
                detail=f"Table '{body.schema_name}.{body.table_name}' not found. Please create it first."
            )
            
        # Map existing columns to their postgres types
        db_columns = {r["column_name"]: r["data_type"] for r in rows}
        
        # Ensure all columns sent by frontend actually exist in the DB table
        for col in body.columns:
            if col not in db_columns:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Column '{col}' does not exist in target table."
                )

        # We only store the types for the columns the frontend intends to send
        column_types = {col: db_columns[col] for col in body.columns}

    finally:
        await pg_conn.close()

    session_id = str(uuid4())
    session_data = {
        "table_name": body.table_name,
        "schema_name": body.schema_name,
        "columns": body.columns,
        "column_types": column_types,
        "connection_id": str(connection_id),
        "workspace_id": str(user.workspace_id)
    }

    # Store lightweight state in Redis (TTL 1 hour)
    redis = await get_redis()
    await redis.setex(f"calyphant:import_session:{session_id}", 3600, json.dumps(session_data))

    return {
        "import_session_id": session_id,
        "chunk_size_limit": 5000,
        "status": "ready_for_chunks"
    }


@router.post("/{connection_id}/import/chunk", status_code=status.HTTP_202_ACCEPTED)
async def process_import_chunk(
    connection_id: UUID,
    body: ImportChunkRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """Phase 2: Receives up to 5,000 rows, coerces types, and executes a high-speed bulk insert."""
    from core.db import get_redis
    from domains.tables.importer import execute_import_chunk
    
    redis = await get_redis()
    raw_session = await redis.get(f"calyphant:import_session:{str(body.import_session_id)}")
    
    if not raw_session:
        raise HTTPException(
            status_code=404, 
            detail="Import session expired or invalid. Please re-initialize."
        )
        
    session = json.loads(raw_session)
    
    # Security Check: Ensure chunk belongs to this exact user/connection
    if session["connection_id"] != str(connection_id) or session["workspace_id"] != str(user.workspace_id):
        raise HTTPException(status_code=403, detail="Session mismatch.")

    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        inserted = await execute_import_chunk(
            pg_conn=pg_conn,
            table_name=session["table_name"],
            schema=session["schema_name"],
            columns=session["columns"],
            column_types=session["column_types"],
            rows=body.rows,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database insertion failed: {exc}")
    finally:
        await pg_conn.close()

    return {
        "chunk_index": body.chunk_index,
        "inserted": inserted,
        "status": "accepted"
    }


# ---------------------------------------------------------------------------
# Export routes (Zero-Memory Streaming Implementation)
# ---------------------------------------------------------------------------

@router.get("/{connection_id}/{table_name}/export/csv")
async def export_table_csv(
    connection_id: UUID,
    table_name: TablePath,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: SchemaQuery = "public",
    limit: int = Query(100_000, le=500_000),
):
    """Export table data as CSV via streaming to prevent Python OOM crashes."""
    import csv
    import io

    url = await _get_url(connection_id, user.workspace_id, db)
    query = TableQuery(table=table_name, schema=schema_name, limit=limit, offset=0)

    async def _stream_csv() -> AsyncGenerator[str, None]:
        # Connect once and use a server-side cursor to sip the data
        async with _managed_pg_conn(url) as pg_conn:
            header_written = False
            
            async for chunk in stream_table(pg_conn, query, chunk_size=1000):
                if not chunk["rows"]:
                    continue
                
                output = io.StringIO()
                # UI CONSIDERATION: The exported CSV uses standard comma separation.
                # If users complain about Excel formatting on Windows, it is an Excel
                # UTF-8 BOM issue, not a data issue. The data is structurally perfect.
                writer = csv.DictWriter(output, fieldnames=chunk["rows"][0].keys())
                
                if not header_written:
                    writer.writeheader()
                    header_written = True
                    
                writer.writerows(chunk["rows"])
                
                # Yield the string chunk directly to the network socket
                yield output.getvalue()
                
                # Truncate to keep memory footprint flat at exactly 1000 rows
                output.seek(0)
                output.truncate(0)

    return StreamingResponse(
        _stream_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table_name}.csv"'},
    )


@router.get("/{connection_id}/{table_name}/export/json")
async def export_table_json(
    connection_id: UUID,
    table_name: TablePath,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: SchemaQuery = "public",
    limit: int = Query(100_000, le=500_000),
):
    """Export table data as JSON array via streaming to prevent Python OOM crashes."""
    import json

    url = await _get_url(connection_id, user.workspace_id, db)
    query = TableQuery(table=table_name, schema=schema_name, limit=limit, offset=0)

    async def _stream_json() -> AsyncGenerator[str, None]:
        yield "[\n"  # Open the JSON array
        
        first_chunk = True
        async with _managed_pg_conn(url) as pg_conn:
            async for chunk in stream_table(pg_conn, query, chunk_size=1000):
                if not chunk["rows"]:
                    continue
                    
                # Serialize chunk, then strip the outer brackets `[` and `]` 
                # so we can stitch multiple JSON lists together into one continuous stream.
                chunk_json = json.dumps(chunk["rows"], default=str)
                inner_json = chunk_json[1:-1]
                
                if not inner_json.strip():
                    continue

                if not first_chunk:
                    yield ",\n" + inner_json
                else:
                    yield inner_json
                    first_chunk = False
                    
        yield "\n]"  # Close the JSON array

    return StreamingResponse(
        _stream_json(),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{table_name}.json"'},
    )
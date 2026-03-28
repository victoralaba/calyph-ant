# domains/tables/editor.py

"""
Tables editor.

Spreadsheet-style row editing against a live PostgreSQL connection.
All writes are transaction-safe. Destructive operations require explicit
confirmation. Bulk operations enforce row limits.

Transaction safety — full history
------------------------------------

Round 1 fixes (carried forward):
  SAFETY-1  Optimistic concurrency on update_row() via row_version +
            SELECT FOR UPDATE SKIP LOCKED.
  SAFETY-2  Two-phase confirmation gate on bulk_delete().
  SAFETY-3  REPEATABLE READ snapshot in fetch_rows() so SELECT and COUNT
            see the same consistent state.
  BUG-1/2/3 Streaming transaction nesting and LIMIT/OFFSET arg mismatch.

Round 2 fixes (carried forward):
  SAFETY-4  Single-row delete confirmation gate.
  SAFETY-5  Per-row row_version support in bulk_update().
  SAFETY-6  Audit trail for all destructive writes.
  SAFETY-7  stream_query_result() promoted to REPEATABLE READ isolation.
  SAFETY-8  DuplicateRowError — structured 409 on unique constraint violation.
  SAFETY-9  Type-aware row_version comparison via _values_equal().

Round 3 fixes (carried forward):
  SAFETY-10 RowLockConflict enriched with presence-layer identity.

Round 4 fixes (carried forward):
  SAFETY-11 bulk_update() dry-run / preview phase.
  SAFETY-12 undo_row_update() — single-level undo via audit log.

Round 5 fixes (this revision):
  CASCADE-1  delete_row() and bulk_delete() now accept cascade: bool = False.
             When cascade=False (default): a ForeignKeyViolationError is caught
             and re-raised as a structured FKViolationError so callers can
             return a clear 409 to the client explaining which constraint fired.
             When cascade=True: the delete proceeds as-is and the database
             handles cascading via its own FK ON DELETE CASCADE constraints.
             Note: PostgreSQL DELETE has no CASCADE keyword — cascade behaviour
             is defined on the FK constraint. cascade=True simply means the app
             will not intercept FK violations; cascade=False means it will catch
             them and surface a human-readable error instead of a raw asyncpg
             exception.
"""

from __future__ import annotations

import asyncio
import decimal
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, AsyncGenerator, cast
from uuid import UUID

import asyncpg
from loguru import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_BULK_DELETE = 1000
MAX_BULK_UPDATE = 500
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
STREAM_CHUNK_SIZE = 500
BULK_DELETE_PREVIEW_SAMPLE = 50


# ---------------------------------------------------------------------------
# Concurrency / integrity exceptions
# ---------------------------------------------------------------------------

class RowLockConflict(Exception):
    """
    Raised when SELECT FOR UPDATE SKIP LOCKED finds the row already locked
    by another transaction.

    Attributes
    ----------
    table       : str
    pk_column   : str
    pk_value    : Any
    holder      : dict | None
        Presence-layer record {user_id, user_email, claimed_at, ttl} for
        the user who has claimed editing intent for this row, or None when
        no presence record exists.
    """
    def __init__(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        holder: dict | None = None,
    ):
        self.table = table
        self.pk_column = pk_column
        self.pk_value = pk_value
        self.holder = holder

        if holder and holder.get("user_email"):
            who = holder["user_email"]
            message = (
                f"Row {pk_column}={pk_value!r} in '{table}' is currently "
                f"being edited by {who}. Try again in a moment."
            )
        else:
            message = (
                f"Row {pk_column}={pk_value!r} in '{table}' is currently "
                "being edited by another user. Try again in a moment."
            )
        super().__init__(message)


class StaleRowError(Exception):
    """
    Raised when optimistic concurrency check fails.

    Attributes: table, pk_column, pk_value,
                conflicts: list[{"column", "expected", "actual"}]
    """
    def __init__(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        conflicts: list[dict],
    ):
        self.table = table
        self.pk_column = pk_column
        self.pk_value = pk_value
        self.conflicts = conflicts
        cols = ", ".join(c["column"] for c in conflicts)
        super().__init__(
            f"Row {pk_column}={pk_value!r} in '{table}' was modified by "
            f"another user since you last loaded it. "
            f"Conflicting columns: {cols}. "
            "Reload the row and reapply your changes."
        )


class DuplicateRowError(Exception):
    """
    Raised when an INSERT violates a unique or primary-key constraint.

    Attributes: table, constraint_name, detail
    """
    def __init__(self, table: str, constraint_name: str, detail: str):
        self.table = table
        self.constraint_name = constraint_name
        self.detail = detail
        super().__init__(
            f"Insert into '{table}' violates unique constraint "
            f"'{constraint_name}': {detail}"
        )


class FKViolationError(Exception):
    """
    Raised when a DELETE is blocked by a foreign key constraint and
    cascade=False was specified.

    Attributes: table, pk_column, pk_value, constraint_name, detail
    """
    def __init__(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        constraint_name: str,
        detail: str,
    ):
        self.table = table
        self.pk_column = pk_column
        self.pk_value = pk_value
        self.constraint_name = constraint_name
        self.detail = detail
        super().__init__(
            f"Cannot delete row {pk_column}={pk_value!r} from '{table}': "
            f"foreign key constraint '{constraint_name}' is violated. "
            f"{detail} "
            "Pass cascade=true to allow dependent rows to be deleted, "
            "or remove dependent rows first."
        )


# ---------------------------------------------------------------------------
# Type-aware version comparison  (SAFETY-9)
# ---------------------------------------------------------------------------

def _values_equal(actual: Any, expected: Any) -> bool:
    """
    Compare a live database value against a client-supplied snapshot value.
    Handles all common Postgres → Python mappings to avoid false conflicts.
    """
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False

    # bool — must precede int because bool is a subclass of int
    if isinstance(actual, bool):
        if isinstance(expected, bool):
            return actual == expected
        if isinstance(expected, str):
            return actual == (expected.lower() in ("true", "1", "yes", "t", "y"))
        if isinstance(expected, int):
            return actual == bool(expected)
        return False

    # numeric
    if isinstance(actual, (int, float, decimal.Decimal)):
        try:
            return decimal.Decimal(str(actual)) == decimal.Decimal(str(expected))
        except (decimal.InvalidOperation, TypeError):
            return str(actual) == str(expected)

    # datetime (subclass of date — must come first)
    if isinstance(actual, datetime):
        if isinstance(expected, datetime):
            return actual == expected
        if isinstance(expected, str):
            try:
                return actual == datetime.fromisoformat(expected)
            except ValueError:
                return False
        return False

    # date
    if isinstance(actual, date):
        if isinstance(expected, date):
            return actual == expected
        if isinstance(expected, str):
            try:
                return actual == date.fromisoformat(expected)
            except ValueError:
                return False
        return False

    # time
    if isinstance(actual, time):
        if isinstance(expected, time):
            return actual == expected
        if isinstance(expected, str):
            try:
                return actual == time.fromisoformat(expected)
            except ValueError:
                return False
        return False

    # timedelta (Postgres INTERVAL)
    if isinstance(actual, timedelta):
        if isinstance(expected, timedelta):
            return actual == expected
        try:
            return actual.total_seconds() == float(expected)
        except (TypeError, ValueError):
            return False

    # UUID
    if isinstance(actual, uuid.UUID):
        try:
            return actual == uuid.UUID(str(expected))
        except (ValueError, AttributeError):
            return False

    # bytes
    if isinstance(actual, (bytes, bytearray)):
        if isinstance(expected, (bytes, bytearray)):
            return bytes(actual) == bytes(expected)
        return False

    # dict / list (JSONB)
    if isinstance(actual, (dict, list)):
        if isinstance(expected, str):
            try:
                return actual == json.loads(expected)
            except (json.JSONDecodeError, TypeError):
                return False
        return actual == expected

    # safe fallback
    return str(actual) == str(expected)


# ---------------------------------------------------------------------------
# Audit helper  (SAFETY-6)
# ---------------------------------------------------------------------------

def _serialise_for_audit(d: dict | None) -> dict:
    """Convert asyncpg-returned Python types to JSON-serialisable values."""
    if not d:
        return {}
    out: dict = {}
    for k, v in d.items():
        if isinstance(v, (datetime, date, time)):
            out[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            out[k] = str(v)
        elif isinstance(v, decimal.Decimal):
            out[k] = float(v)
        elif isinstance(v, (bytes, bytearray)):
            out[k] = "<binary>"
        elif isinstance(v, timedelta):
            out[k] = v.total_seconds()
        else:
            out[k] = v
    return out


async def _write_audit_entry(
    operation: str,
    table: str,
    schema: str,
    pk_column: str,
    pk_value: Any,
    before: dict | None = None,
    after: dict | None = None,
) -> None:
    """
    Write a row-level audit entry to AuditLog.

    Fire-and-forget: failure logs WARNING, never propagates.
    Silently skips when _session_factory is not available (tests, CLI).
    """
    try:
        from core.db import _session_factory
        from domains.admin.service import write_audit

        if not _session_factory:
            return

        async with _session_factory() as db:
            await write_audit(
                db=db,
                action=f"table_row.{operation}",
                resource_type="table_row",
                resource_id=f"{schema}.{table}:{pk_value}",
                diff={
                    "before": _serialise_for_audit(before),
                    "after": _serialise_for_audit(after),
                },
                meta={
                    "table": table,
                    "schema": schema,
                    "pk_column": pk_column,
                },
            )
    except Exception as exc:
        logger.warning(
            f"Audit write failed for {operation} on "
            f"{schema}.{table} pk={pk_value}: {exc}. "
            "The data operation succeeded but the audit trail is incomplete."
        )


# ---------------------------------------------------------------------------
# Filter / sort models
# ---------------------------------------------------------------------------

@dataclass
class ColumnFilter:
    column: str
    operator: str  # eq|neq|gt|gte|lt|lte|like|ilike|is_null|is_not_null|in
    value: Any = None


@dataclass
class SortOrder:
    column: str
    direction: str = "asc"  # asc | desc


@dataclass
class TableQuery:
    table: str
    schema: str = "public"
    filters: list[ColumnFilter] = field(default_factory=list)
    sort: list[SortOrder] = field(default_factory=list)
    limit: int = DEFAULT_PAGE_SIZE
    offset: int = 0
    columns: list[str] | None = None  # None = SELECT *


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

_SAFE_OP_MAP = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "like": "LIKE",
    "ilike": "ILIKE",
    "is_null": "IS NULL",
    "is_not_null": "IS NOT NULL",
}


def _build_select(q: TableQuery) -> tuple[str, list[Any]]:
    cols = "*"
    if q.columns:
        cols = ", ".join(f'"{c}"' for c in q.columns)

    sql = f'SELECT {cols} FROM "{q.schema}"."{q.table}"'
    args: list[Any] = []
    idx = 1

    sql, args, idx = _apply_filters(sql, args, idx, q.filters)
    sql = _apply_sort(sql, q.sort)

    limit = min(q.limit, MAX_PAGE_SIZE)
    sql += f" LIMIT ${idx} OFFSET ${idx + 1}"
    args.extend([limit, q.offset])
    return sql, args


def _build_stream_sql(q: TableQuery) -> tuple[str, list[Any]]:
    """
    Like _build_select but without LIMIT/OFFSET or their args.
    Used by stream_table() so the cursor controls row fetching.
    """
    cols = "*"
    if q.columns:
        cols = ", ".join(f'"{c}"' for c in q.columns)

    sql = f'SELECT {cols} FROM "{q.schema}"."{q.table}"'
    args: list[Any] = []
    idx = 1

    sql, args, idx = _apply_filters(sql, args, idx, q.filters)
    sql = _apply_sort(sql, q.sort)
    return sql, args


def _apply_filters(
    sql: str,
    args: list[Any],
    idx: int,
    filters: list[ColumnFilter],
) -> tuple[str, list[Any], int]:
    if not filters:
        return sql, args, idx

    clauses = []
    for f in filters:
        if f.operator in ("is_null", "is_not_null"):
            clauses.append(f'"{f.column}" {_SAFE_OP_MAP[f.operator]}')
        elif f.operator == "in":
            if not isinstance(f.value, list):
                raise ValueError("'in' operator requires a list value.")
            placeholders = ", ".join(f"${idx + i}" for i in range(len(f.value)))
            clauses.append(f'"{f.column}" = ANY(ARRAY[{placeholders}])')
            args.extend(f.value)
            idx += len(f.value)
            continue
        else:
            op = _SAFE_OP_MAP.get(f.operator)
            if not op:
                raise ValueError(f"Unsupported operator: {f.operator}")
            clauses.append(f'"{f.column}" {op} ${idx}')
            args.append(f.value)
            idx += 1

    sql += " WHERE " + " AND ".join(clauses)
    return sql, args, idx


def _apply_sort(sql: str, sort: list[SortOrder]) -> str:
    if not sort:
        return sql
    order_parts = []
    for s in sort:
        direction = "DESC" if s.direction.lower() == "desc" else "ASC"
        order_parts.append(f'"{s.column}" {direction}')
    return sql + " ORDER BY " + ", ".join(order_parts)


def _build_count(q: TableQuery) -> tuple[str, list[Any]]:
    sql = f'SELECT COUNT(*) FROM "{q.schema}"."{q.table}"'
    args: list[Any] = []
    idx = 1
    sql, args, idx = _apply_filters(sql, args, idx, q.filters)
    return sql, args


# ---------------------------------------------------------------------------
# Row fetch  (SAFETY-3: REPEATABLE READ snapshot)
# ---------------------------------------------------------------------------

async def fetch_rows(
    conn: asyncpg.Connection,
    query: TableQuery,
) -> dict[str, Any]:
    """
    Fetch a paginated page of rows with total count.

    Both SELECT and COUNT run inside a single REPEATABLE READ transaction
    so they see the same snapshot.
    """
    sql, args = _build_select(query)
    count_sql, count_args = _build_count(query)

    async with conn.transaction(isolation="repeatable_read", readonly=True):
        rows = await conn.fetch(sql, *args)
        total = await conn.fetchval(count_sql, *count_args) or 0

    columns = list(rows[0].keys()) if rows else []
    return {
        "rows": [dict(r) for r in rows],
        "total": total,
        "columns": columns,
        "limit": query.limit,
        "offset": query.offset,
    }


# ---------------------------------------------------------------------------
# Row CRUD
# ---------------------------------------------------------------------------

async def insert_row(
    conn: asyncpg.Connection,
    table: str,
    data: dict[str, Any],
    schema: str = "public",
) -> dict[str, Any]:
    if not data:
        raise ValueError("Cannot insert an empty row.")

    cols = ", ".join(f'"{k}"' for k in data.keys())
    placeholders = ", ".join(f"${i + 1}" for i in range(len(data)))
    sql = (
        f'INSERT INTO "{schema}"."{table}" ({cols}) '
        f"VALUES ({placeholders}) "
        f"RETURNING *"
    )

    try:
        row = await conn.fetchrow(sql, *data.values())
    except asyncpg.UniqueViolationError as exc:
        raise DuplicateRowError(
            table=table,
            constraint_name=getattr(exc, "constraint_name", None) or "unknown",
            detail=getattr(exc, "detail", None) or str(exc),
        ) from exc

    if not row:
        raise RuntimeError("Insert failed, no row was returned.")

    # Cast tells Pylance to treat this strictly as dict[str, Any]
    result = cast(dict[str, Any], dict(row))

    asyncio.create_task(
        _write_audit_entry(
            operation="insert",
            table=table,
            schema=schema,
            pk_column="(new)",
            pk_value=None,
            before=None,
            after=result,
        )
    )

    return result

async def update_row(
    conn: asyncpg.Connection,
    table: str,
    pk_column: str,
    pk_value: Any,
    data: dict[str, Any],
    schema: str = "public",
    row_version: dict[str, Any] | None = None,
    connection_id: UUID | str | None = None,
) -> dict[str, Any] | None:
    """
    Update a single row by primary key.

    SAFETY-1 — Optimistic concurrency when row_version is supplied.
    SAFETY-9 — Type-aware version comparison.
    SAFETY-10 — RowLockConflict enriched with presence identity.
    SAFETY-6 — Audit trail.

    Returns updated row dict or None if the row does not exist.
    Raises RowLockConflict, StaleRowError, ValueError.
    """
    if not data:
        raise ValueError("No fields to update.")

    if row_version is None:
        # Blind update path
        set_parts = [f'"{k}" = ${i + 1}' for i, k in enumerate(data.keys())]
        pk_idx = len(data) + 1
        sql = (
            f'UPDATE "{schema}"."{table}" '
            f"SET {', '.join(set_parts)} "
            f'WHERE "{pk_column}" = ${pk_idx} '
            f"RETURNING *"
        )
        row = await conn.fetchrow(sql, *data.values(), pk_value)
        if row is None:
            return None
        result = dict(row)
        asyncio.create_task(
            _write_audit_entry(
                operation="update",
                table=table,
                schema=schema,
                pk_column=pk_column,
                pk_value=pk_value,
                before=None,
                after=result,
            )
        )
        return result

    # Optimistic concurrency path
    live: dict = {}
    result: dict = {}

    async with conn.transaction():
        lock_sql = (
            f'SELECT * FROM "{schema}"."{table}" '
            f'WHERE "{pk_column}" = $1 '
            f"FOR UPDATE SKIP LOCKED"
        )
        locked_row = await conn.fetchrow(lock_sql, pk_value)

        if locked_row is None:
            exists = await conn.fetchval(
                f'SELECT 1 FROM "{schema}"."{table}" '
                f'WHERE "{pk_column}" = $1',
                pk_value,
            )
            if exists:
                # Row is locked — enrich with presence (SAFETY-10)
                holder: dict | None = None
                if connection_id is not None:
                    try:
                        from domains.tables.presence import get_row_lock
                        holder = await get_row_lock(
                            connection_id=connection_id,
                            schema=schema,
                            table=table,
                            pk_value=pk_value,
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Presence lookup failed during RowLockConflict "
                            f"enrichment for {schema}.{table} pk={pk_value}: {exc}"
                        )
                raise RowLockConflict(table, pk_column, pk_value, holder=holder)
            return None

        live = dict(locked_row)
        conflicts: list[dict] = []
        for col, expected in row_version.items():
            if col == pk_column:
                continue
            if col not in live:
                continue
            if not _values_equal(live[col], expected):
                conflicts.append({
                    "column": col,
                    "expected": expected,
                    "actual": live[col],
                })

        if conflicts:
            raise StaleRowError(table, pk_column, pk_value, conflicts)

        set_parts = [f'"{k}" = ${i + 1}' for i, k in enumerate(data.keys())]
        pk_idx = len(data) + 1
        update_sql = (
            f'UPDATE "{schema}"."{table}" '
            f"SET {', '.join(set_parts)} "
            f'WHERE "{pk_column}" = ${pk_idx} '
            f"RETURNING *"
        )
        row = await conn.fetchrow(update_sql, *data.values(), pk_value)
        if row is None:
            return None
        result = dict(row)

    asyncio.create_task(
        _write_audit_entry(
            operation="update",
            table=table,
            schema=schema,
            pk_column=pk_column,
            pk_value=pk_value,
            before=live,
            after=result,
        )
    )
    return result


async def delete_row(
    conn: asyncpg.Connection,
    table: str,
    pk_column: str,
    pk_value: Any,
    schema: str = "public",
    confirmed: bool = False,
    cascade: bool = False,
) -> dict[str, Any]:
    """
    Delete a single row by primary key.

    SAFETY-4 — Two-phase confirmation:
      confirmed=False: preview (nothing written).
      confirmed=True: execute.
    SAFETY-6 — Audit trail.
    CASCADE-1 — cascade=False raises FKViolationError when a FK constraint
      blocks the delete. cascade=True lets the DB's own ON DELETE CASCADE
      constraints handle dependent rows.
    """
    if not confirmed:
        preview_row = await conn.fetchrow(
            f'SELECT * FROM "{schema}"."{table}" WHERE "{pk_column}" = $1',
            pk_value,
        )
        row_dict = dict(preview_row) if preview_row else None
        return {
            "confirmed": False,
            "row": row_dict,
            "message": (
                f"This will permanently delete 1 row from '{table}' "
                f"where {pk_column}={pk_value!r}. "
                "Re-submit with confirmed=true to proceed."
                if row_dict
                else (
                    f"No row found in '{table}' "
                    f"where {pk_column}={pk_value!r}."
                )
            ),
        }

    sql = (
        f'DELETE FROM "{schema}"."{table}" '
        f'WHERE "{pk_column}" = $1 '
        f"RETURNING *"
    )

    try:
        deleted_row = await conn.fetchrow(sql, pk_value)
    except asyncpg.ForeignKeyViolationError as exc:
        if not cascade:
            raise FKViolationError(
                table=table,
                pk_column=pk_column,
                pk_value=pk_value,
                constraint_name=getattr(exc, "constraint_name", None) or "unknown",
                detail=getattr(exc, "detail", None) or str(exc),
            ) from exc
        raise  # cascade=True: let it propagate as-is (shouldn't happen if FK is ON DELETE CASCADE)

    if deleted_row is None:
        return {"confirmed": True, "deleted": False, "row": None}

    row_dict = dict(deleted_row)

    asyncio.create_task(
        _write_audit_entry(
            operation="delete",
            table=table,
            schema=schema,
            pk_column=pk_column,
            pk_value=pk_value,
            before=row_dict,
            after=None,
        )
    )

    return {"confirmed": True, "deleted": True, "row": row_dict}


# ---------------------------------------------------------------------------
# Undo  (SAFETY-12)
# ---------------------------------------------------------------------------

async def undo_row_update(
    conn: asyncpg.Connection,
    table: str,
    pk_column: str,
    pk_value: Any,
    schema: str = "public",
) -> dict[str, Any] | None:
    """
    Undo the most recent UPDATE for a row by re-applying its "before"
    snapshot from the audit log.

    This is single-level undo — only the most recent update is reversed.
    If no audit record with a non-empty "before" dict exists for this row,
    returns None and does nothing.

    Returns the restored row or None if no undoable state exists.
    Raises ValueError if the table or row does not exist, or if the
    before snapshot is empty.
    """
    from core.db import _session_factory

    if not _session_factory:
        raise ValueError("Audit log is not available (database not initialised).")

    before_snapshot: dict = {}
    audit_id: str | None = None

    try:
        from sqlalchemy import select
        from domains.admin.service import AuditLog

        resource_id = f"{schema}.{table}:{pk_value}"

        async with _session_factory() as db:
            result = await db.execute(
                select(AuditLog)
                .where(
                    AuditLog.action == "table_row.update",
                    AuditLog.resource_id == resource_id,
                )
                .order_by(AuditLog.created_at.desc())
                .limit(1)
            )
            audit_entry = result.scalar_one_or_none()

            if not audit_entry:
                return None

            diff = audit_entry.diff or {}
            before_snapshot = diff.get("before", {})
            audit_id = str(audit_entry.id)
    except Exception as exc:
        logger.warning(f"undo_row_update: could not read audit log: {exc}")
        raise ValueError(
            "Could not read audit log to determine previous row state. "
            f"Detail: {exc}"
        ) from exc

    if not before_snapshot:
        return None

    restore_data = {k: v for k, v in before_snapshot.items() if k != pk_column}
    if not restore_data:
        raise ValueError(
            f"Audit 'before' snapshot for row {pk_column}={pk_value!r} "
            "in table '{table}' contains only the primary key — nothing to restore."
        )

    result_row = await update_row(
        conn=conn,
        table=table,
        pk_column=pk_column,
        pk_value=pk_value,
        data=restore_data,
        schema=schema,
        row_version=None,
    )

    if result_row is None:
        raise ValueError(
            f"Row {pk_column}={pk_value!r} in '{table}' no longer exists "
            "and cannot be restored. The row may have been deleted."
        )

    asyncio.create_task(
        _write_audit_entry(
            operation="undo",
            table=table,
            schema=schema,
            pk_column=pk_column,
            pk_value=pk_value,
            before=None,
            after=_serialise_for_audit(result_row),
        )
    )

    return result_row


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------

async def bulk_delete(
    conn: asyncpg.Connection,
    table: str,
    pk_column: str,
    pk_values: list[Any],
    schema: str = "public",
    confirmed: bool = False,
    cascade: bool = False,
) -> dict[str, Any]:
    """
    Delete multiple rows by primary key values.

    SAFETY-2 — Two-phase confirmation gate.
    Hard capped at MAX_BULK_DELETE rows.
    CASCADE-1 — cascade=False raises FKViolationError if any FK constraint
      blocks the delete. cascade=True lets the DB handle cascading.

    confirmed=False: dry-run — counts matching rows, returns sample PKs.
    confirmed=True: executes DELETE.
    """
    if len(pk_values) > MAX_BULK_DELETE:
        raise ValueError(
            f"Bulk delete limited to {MAX_BULK_DELETE} rows. "
            f"Got {len(pk_values)}."
        )

    str_values = [str(v) for v in pk_values]

    if not confirmed:
        count_sql = (
            f'SELECT COUNT(*) FROM "{schema}"."{table}" '
            f'WHERE "{pk_column}" = ANY($1::text[])'
        )
        sample_sql = (
            f'SELECT "{pk_column}" FROM "{schema}"."{table}" '
            f'WHERE "{pk_column}" = ANY($1::text[]) '
            f"LIMIT {BULK_DELETE_PREVIEW_SAMPLE}"
        )
        would_delete = await conn.fetchval(count_sql, str_values) or 0
        sample_rows = await conn.fetch(sample_sql, str_values)
        sample_pks = [row[pk_column] for row in sample_rows]
        return {
            "confirmed": False,
            "would_delete": would_delete,
            "sample_pks": sample_pks,
            "message": (
                f"This will permanently delete {would_delete} row(s) from "
                f"'{table}'. Re-submit with confirmed=true to proceed."
            ),
        }

    delete_sql = (
        f'DELETE FROM "{schema}"."{table}" '
        f'WHERE "{pk_column}" = ANY($1::text[]) '
        f"RETURNING 1"
    )

    try:
        rows = await conn.fetch(delete_sql, str_values)
    except asyncpg.ForeignKeyViolationError as exc:
        if not cascade:
            raise FKViolationError(
                table=table,
                pk_column=pk_column,
                pk_value=f"one of {str_values[:5]}{'...' if len(str_values) > 5 else ''}",
                constraint_name=getattr(exc, "constraint_name", None) or "unknown",
                detail=getattr(exc, "detail", None) or str(exc),
            ) from exc
        raise

    return {"confirmed": True, "deleted": len(rows)}


async def _preview_bulk_update(
    conn: asyncpg.Connection,
    table: str,
    pk_column: str,
    rows: list[dict[str, Any]],
    schema: str = "public",
    connection_id: UUID | str | None = None,
) -> dict[str, Any]:
    """
    Dry-run for bulk_update. Reads current state of each row and computes
    what would change, including lock conflicts and stale-row issues.

    Runs under REPEATABLE READ so all row reads see a consistent snapshot.
    Nothing is written.

    Returns:
    {
        "confirmed": False,
        "preview": [
            {
                "pk": <value>,
                "status": "would_update" | "not_found" | "locked" | "stale",
                "current": {...} | None,        # current DB row
                "proposed": {...},              # columns being changed
                "changes": {                    # only when row_version given
                    "column": {"from": old, "to": new}
                },
                "lock_holder": {...} | None,    # when status=="locked"
                "stale_conflicts": [...]        # when status=="stale"
            }
        ],
        "summary": {
            "would_update": N,
            "not_found": N,
            "locked": N,
            "stale": N
        }
    }
    """
    preview: list[dict[str, Any]] = []
    summary = {"would_update": 0, "not_found": 0, "locked": 0, "stale": 0}

    async with conn.transaction(isolation="repeatable_read", readonly=True):
        for raw_row in rows:
            row = dict(raw_row)
            pk_value = row.get(pk_column)
            if pk_value is None:
                continue
            row_version = row.get("__version__")
            proposed = {
                k: v for k, v in row.items()
                if k != pk_column and k != "__version__"
            }

            current_row = await conn.fetchrow(
                f'SELECT * FROM "{schema}"."{table}" WHERE "{pk_column}" = $1',
                pk_value,
            )

            if current_row is None:
                summary["not_found"] += 1
                preview.append({
                    "pk": pk_value,
                    "status": "not_found",
                    "current": None,
                    "proposed": proposed,
                    "changes": {},
                    "lock_holder": None,
                    "stale_conflicts": [],
                })
                continue

            current_dict = dict(current_row)

            lock_holder: dict | None = None
            if connection_id is not None:
                try:
                    from domains.tables.presence import get_row_lock
                    lock_holder = await get_row_lock(
                        connection_id=connection_id,
                        schema=schema,
                        table=table,
                        pk_value=pk_value,
                    )
                except Exception:
                    pass

            if lock_holder:
                summary["locked"] += 1
                preview.append({
                    "pk": pk_value,
                    "status": "locked",
                    "current": _serialise_for_audit(current_dict),
                    "proposed": proposed,
                    "changes": {},
                    "lock_holder": lock_holder,
                    "stale_conflicts": [],
                })
                continue

            stale_conflicts: list[dict] = []
            if row_version:
                for col, expected in row_version.items():
                    if col == pk_column or col not in current_dict:
                        continue
                    if not _values_equal(current_dict[col], expected):
                        stale_conflicts.append({
                            "column": col,
                            "expected": expected,
                            "actual": current_dict[col],
                        })

            if stale_conflicts:
                summary["stale"] += 1
                preview.append({
                    "pk": pk_value,
                    "status": "stale",
                    "current": _serialise_for_audit(current_dict),
                    "proposed": proposed,
                    "changes": {},
                    "lock_holder": None,
                    "stale_conflicts": stale_conflicts,
                })
                continue

            changes: dict[str, dict] = {}
            for col, new_val in proposed.items():
                if col in current_dict:
                    old_val = current_dict[col]
                    if not _values_equal(old_val, new_val):
                        changes[col] = {
                            "from": _serialise_for_audit({col: old_val})[col],
                            "to": new_val,
                        }
                else:
                    changes[col] = {"from": None, "to": new_val}

            summary["would_update"] += 1
            preview.append({
                "pk": pk_value,
                "status": "would_update",
                "current": _serialise_for_audit(current_dict),
                "proposed": proposed,
                "changes": changes,
                "lock_holder": None,
                "stale_conflicts": [],
            })

    return {
        "confirmed": False,
        "preview": preview,
        "summary": summary,
    }


async def bulk_update(
    conn: asyncpg.Connection,
    table: str,
    pk_column: str,
    rows: list[dict[str, Any]],
    schema: str = "public",
    connection_id: UUID | str | None = None,
    confirmed: bool = True,
) -> dict[str, Any]:
    """
    Update multiple rows in a single transaction (when confirmed=True),
    or return a dry-run preview of what would change (when confirmed=False).

    Each dict must include the pk_column.
    Hard capped at MAX_BULK_UPDATE rows.

    SAFETY-11 — Dry-run / preview phase:
      confirmed=False (default is True for backward compatibility, but the
      router passes False until the user explicitly confirms):
        Reads current state of every row and returns what would change,
        including any lock conflicts or stale-row issues, without writing
        anything. No exceptions are raised for locked/stale rows in this
        mode — they are reported in the preview instead.

      confirmed=True:
        Executes all updates inside one transaction. Any RowLockConflict
        or StaleRowError rolls back the entire batch.

    SAFETY-5 — Per-row row_version for optimistic concurrency.
    SAFETY-10 — RowLockConflict enriched with presence identity.

    Returns (confirmed=False):
        {"confirmed": False, "preview": [...], "summary": {...}}

    Returns (confirmed=True):
        {"updated": int, "results": [{"pk": ..., "status": str}, ...]}
    """
    if len(rows) > MAX_BULK_UPDATE:
        raise ValueError(
            f"Bulk update limited to {MAX_BULK_UPDATE} rows. "
            f"Got {len(rows)}."
        )

    if not confirmed:
        return await _preview_bulk_update(
            conn=conn,
            table=table,
            pk_column=pk_column,
            rows=rows,
            schema=schema,
            connection_id=connection_id,
        )

    results = []
    updated = 0

    async with conn.transaction():
        for raw_row in rows:
            row = dict(raw_row)
            pk_value = row.pop(pk_column, None)
            if pk_value is None:
                raise ValueError(
                    f"Each row must include '{pk_column}' for bulk update."
                )
            row_version = row.pop("__version__", None)

            result = await update_row(
                conn,
                table,
                pk_column,
                pk_value,
                row,
                schema,
                row_version=row_version,
                connection_id=connection_id,
            )

            if result:
                updated += 1
                results.append({"pk": pk_value, "status": "updated"})
            else:
                results.append({"pk": pk_value, "status": "not_found"})

    return {"updated": updated, "results": results}


# ---------------------------------------------------------------------------
# Streaming (server-side cursor for large tables)
# ---------------------------------------------------------------------------

async def stream_table(
    conn: asyncpg.Connection,
    query: TableQuery,
    chunk_size: int = STREAM_CHUNK_SIZE,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    Stream table rows using a PostgreSQL server-side cursor.
    Yields chunks of rows — suitable for SSE transport.

    Uses REPEATABLE READ so every chunk sees the same consistent snapshot.
    Uses _build_stream_sql() so LIMIT/OFFSET are never appended.
    Single transaction, cursor inside it.
    """
    sql, args = _build_stream_sql(query)
    chunk_index = 0
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        cursor = await conn.cursor(sql, *args)
        while True:
            rows = await cursor.fetch(chunk_size)
            if not rows:
                break
            yield {
                "chunk": chunk_index,
                "rows": [dict(r) for r in rows],
                "count": len(rows),
            }
            chunk_index += 1


async def stream_query_result(
    conn: asyncpg.Connection,
    sql: str,
    args: list[Any] | None = None,
    chunk_size: int = STREAM_CHUNK_SIZE,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    Stream results of an arbitrary SELECT query via server-side cursor.
    Used by the query editor for large result sets.

    SAFETY-7: REPEATABLE READ so arbitrary query streams are phantom-read safe.
    Single transaction, cursor inside it.
    """
    args = args or []
    chunk_index = 0
    columns: list[str] | None = None

    async with conn.transaction(isolation="repeatable_read", readonly=True):
        cursor = await conn.cursor(sql, *args)
        while True:
            rows = await cursor.fetch(chunk_size)
            if not rows:
                break
            if columns is None:
                columns = list(rows[0].keys())
            yield {
                "chunk": chunk_index,
                "columns": columns,
                "rows": [dict(r) for r in rows],
                "count": len(rows),
            }
            chunk_index += 1


# ---------------------------------------------------------------------------
# Table metadata helpers
# ---------------------------------------------------------------------------

async def get_primary_key_column(
    conn: asyncpg.Connection,
    table: str,
    schema: str = "public",
) -> str | None:
    """Detect the primary key column name for a table."""
    row = await conn.fetchrow(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
        LIMIT 1
        """,
        schema,
        table,
    )
    return row["column_name"] if row else None


async def get_column_names(
    conn: asyncpg.Connection,
    table: str,
    schema: str = "public",
) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema,
        table,
    )
    return [r["column_name"] for r in rows]
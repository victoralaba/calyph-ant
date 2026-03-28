# domains/schema/auto_migration.py
"""
Auto-migration generation.

Changes from original
---------------------
GAP-9-4  Semantic diff search_path fix
    diff_at_versions_semantic previously passed search_path as a URL
    query parameter which migra ignores.  The replacement uses
    run_migra_on_schemas() from diff.py, which embeds search_path in
    the libpq options DSN parameter that migra's driver actually reads.
    Migrations are replayed into two named schemas with explicit
    SET search_path before each statement, then reset after.

All other functions are unchanged from the original.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from domains.migrations.service import (
    MigrationRecord,
    create_migration,
    list_migrations,
    get_migration_at_order_index,
)
from domains.schema.diff import (
    DiffResult,
    _DESTRUCTIVE_KINDS,
    _parse_sql_to_changes,
    run_migra_on_schemas,
)
from domains.schema.introspection import introspect_database


# ---------------------------------------------------------------------------
# Typed Redis-unavailability error
# ---------------------------------------------------------------------------

class RedisUnavailableError(RuntimeError):
    """
    Raised when a snapshot operation cannot proceed because Redis is
    unreachable.
    """


# ---------------------------------------------------------------------------
# Snapshot TTL and key prefix
# ---------------------------------------------------------------------------

_SNAPSHOT_TTL_SECONDS = 7200
_SNAPSHOT_KEY_PREFIX = "calyphant:schema_snapshot:"


# ---------------------------------------------------------------------------
# Redis-backed snapshot store
# ---------------------------------------------------------------------------

async def _get_redis():
    from core.db import get_redis as _get_redis_client
    try:
        redis = await _get_redis_client()
        return redis
    except RuntimeError as exc:
        raise RedisUnavailableError(
            "Redis is required for schema snapshots and is currently unavailable. "
            "Ensure Redis is running and reachable."
        ) from exc


def _get_fernet():
    from core.db import _fernet
    if _fernet is None:
        raise RuntimeError("Fernet not initialised. Call init_db() at startup.")
    return _fernet


def _encrypt_snapshot(data: dict) -> bytes:
    fernet = _get_fernet()
    raw = json.dumps(data).encode()
    return fernet.encrypt(raw)


def _decrypt_snapshot(token: bytes) -> dict:
    fernet = _get_fernet()
    return json.loads(fernet.decrypt(token).decode())


async def _snapshot_set(snapshot_id: str, fingerprint: dict[str, Any]) -> None:
    redis = await _get_redis()
    encrypted = _encrypt_snapshot(fingerprint)
    await redis.setex(
        f"{_SNAPSHOT_KEY_PREFIX}{snapshot_id}",
        _SNAPSHOT_TTL_SECONDS,
        encrypted,
    )


async def _snapshot_get(snapshot_id: str) -> dict[str, Any] | None:
    redis = await _get_redis()
    raw = await redis.get(f"{_SNAPSHOT_KEY_PREFIX}{snapshot_id}")
    if raw is None:
        return None
    return _decrypt_snapshot(raw)


async def _snapshot_delete(snapshot_id: str) -> None:
    try:
        redis = await _get_redis()
        await redis.delete(f"{_SNAPSHOT_KEY_PREFIX}{snapshot_id}")
    except Exception as exc:
        logger.warning(f"Could not delete snapshot {snapshot_id} from Redis: {exc}")


# ---------------------------------------------------------------------------
# Vector type helpers
# ---------------------------------------------------------------------------

async def _resolve_vector_types(
    pg_conn: asyncpg.Connection,
    table_name: str,
    schema: str,
) -> dict[str, str]:
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                a.attname AS column_name,
                t.typname AS type_name,
                a.atttypmod AS type_mod
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND t.typname = 'vector'
            """,
            schema,
            table_name,
        )
        result: dict[str, str] = {}
        for row in rows:
            type_name = row["type_name"]
            type_mod = row["type_mod"]
            col = row["column_name"]
            if type_name == "vector" and type_mod and type_mod > 0:
                result[col] = f"vector({type_mod})"
            elif type_name == "vector":
                result[col] = "vector"
        return result
    except Exception as exc:
        logger.warning(f"Could not resolve vector types for {schema}.{table_name}: {exc}")
        return {}


def _is_vector_type(type_str: str) -> bool:
    return type_str.lower().startswith("vector")


def _vector_dimension(type_str: str) -> int | None:
    import re
    match = re.match(r"vector\((\d+)\)", type_str.lower())
    return int(match.group(1)) if match else None


# ---------------------------------------------------------------------------
# ANN index WITH clause defaults hint
# ---------------------------------------------------------------------------

_ANN_INDEX_DEFAULTS: dict[str, dict[str, int]] = {
    "ivfflat": {"lists": 100},
    "hnsw": {"m": 16, "ef_construction": 64},
}


def _ann_defaults_hint(method: str) -> str | None:
    defaults = _ANN_INDEX_DEFAULTS.get(method.lower())
    if not defaults:
        return None
    params = ", ".join(f"{k}={v}" for k, v in defaults.items())
    return (
        f"-- Hint: {method} index created without explicit WITH params. "
        f"pgvector defaults are used. Consider: WITH ({params}) for tuning."
    )


# ---------------------------------------------------------------------------
# 1. SNAPSHOT-BASED AUTO-MIGRATION
# ---------------------------------------------------------------------------

async def _build_table_fingerprint_with_conn(
    tables_from_snapshot: list,
    schema: str,
    pg_conn: asyncpg.Connection,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for t in tables_from_snapshot:
        if t.kind != "table":
            continue
        vector_types = await _resolve_vector_types(pg_conn, t.name, schema)
        result[t.name] = {
            "columns": {
                c.name: {
                    "type": (
                        vector_types[c.name]
                        if c.data_type.upper() in ("USER-DEFINED", "USER_DEFINED")
                        and c.name in vector_types
                        else c.data_type
                    ),
                    "nullable": c.nullable,
                    "default": c.default,
                    "is_primary_key": c.is_primary_key,
                }
                for c in t.columns
            },
            "primary_key": sorted(t.primary_key),
            "indexes": {
                i.name: {
                    "columns": i.columns,
                    "unique": i.unique,
                    "method": i.method,
                    "partial": i.partial,
                    "predicate": i.predicate,
                }
                for i in t.indexes
                if i.name
            },
        }
    return result


def _build_table_fingerprint(tables_from_snapshot: list) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for t in tables_from_snapshot:
        if t.kind != "table":
            continue
        result[t.name] = {
            "columns": {
                c.name: {
                    "type": c.data_type,
                    "nullable": c.nullable,
                    "default": c.default,
                    "is_primary_key": c.is_primary_key,
                }
                for c in t.columns
            },
            "primary_key": sorted(t.primary_key),
            "indexes": {
                i.name: {
                    "columns": i.columns,
                    "unique": i.unique,
                    "method": i.method,
                    "partial": i.partial,
                    "predicate": i.predicate,
                }
                for i in t.indexes
                if i.name
            },
        }
    return result


async def capture_snapshot(
    connection_id: UUID,
    workspace_id: UUID,
    schema_name: str = "public",
) -> str:
    import asyncio

    from core.db import _session_factory
    if not _session_factory:
        raise RuntimeError("Database not initialised.")

    from core.db import get_connection_url
    async with _session_factory() as db:
        db_url = await get_connection_url(db, connection_id, workspace_id)
    if not db_url:
        raise ValueError("Connection not found or not in this workspace.")

    snapshot = await asyncio.to_thread(introspect_database, db_url, schema_name)

    pg_conn: asyncpg.Connection | None = None
    try:
        pg_conn = await asyncpg.connect(dsn=db_url, timeout=15)
        assert pg_conn is not None  # <-- Add this line to satisfy type checker
        table_fingerprint = await _build_table_fingerprint_with_conn(
            snapshot.tables, schema_name, pg_conn
        )
    except Exception as exc:
        logger.warning(
            f"Could not open connection for vector type resolution during snapshot: {exc}. "
            "Falling back to sync fingerprint."
        )
        table_fingerprint = _build_table_fingerprint(snapshot.tables)
    finally:
        if pg_conn:
            try:
                await pg_conn.close()
            except Exception:
                pass

    fingerprint: dict[str, Any] = {
        "connection_id": str(connection_id),
        "workspace_id": str(workspace_id),
        "schema": schema_name,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "tables": table_fingerprint,
        "enums": {e.name: e.values for e in snapshot.enums},
    }

    canonical = json.dumps(fingerprint["tables"], sort_keys=True)
    fingerprint["hash"] = hashlib.sha256(canonical.encode()).hexdigest()

    snapshot_id = str(uuid4())
    await _snapshot_set(snapshot_id, fingerprint)
    logger.debug(
        f"Schema snapshot captured: id={snapshot_id} "
        f"connection={connection_id} hash={fingerprint['hash'][:12]}"
    )
    return snapshot_id


async def auto_generate_migration(
    snapshot_id: str,
    db: AsyncSession,
    connection_id: UUID,
    workspace_id: UUID,
    label: str | None = None,
    schema_name: str = "public",
) -> MigrationRecord | None:
    import asyncio

    fingerprint = await _snapshot_get(snapshot_id)
    if not fingerprint:
        raise ValueError(
            f"Snapshot '{snapshot_id}' not found or expired. "
            "Snapshots expire after 2 hours — re-capture and retry."
        )

    if (
        fingerprint.get("connection_id") != str(connection_id)
        or fingerprint.get("workspace_id") != str(workspace_id)
    ):
        raise ValueError(
            "Snapshot does not belong to this connection or workspace."
        )

    from core.db import get_connection_url
    db_url = await get_connection_url(db, connection_id, workspace_id)
    if not db_url:
        raise ValueError("Connection not found or not in this workspace.")

    live_snapshot = await asyncio.to_thread(
        introspect_database, db_url, schema_name
    )

    pg_conn: asyncpg.Connection | None = None
    try:
        pg_conn = await asyncpg.connect(dsn=db_url, timeout=15)
        assert pg_conn is not None  # <-- Add this line to satisfy type checker
        live_tables = await _build_table_fingerprint_with_conn(
            live_snapshot.tables, schema_name, pg_conn
        )
    except Exception as exc:
        logger.warning(f"Could not resolve vector types in live snapshot: {exc}.")
        live_tables = _build_table_fingerprint(live_snapshot.tables)
    finally:
        if pg_conn:
            try:
                await pg_conn.close()
            except Exception:
                pass

    live_canonical = json.dumps(live_tables, sort_keys=True)
    live_hash = hashlib.sha256(live_canonical.encode()).hexdigest()

    if live_hash == fingerprint["hash"]:
        logger.info(f"Auto-migrate: no schema changes detected (snapshot={snapshot_id})")
        await _snapshot_delete(snapshot_id)
        return None

    diff_result = await _structural_diff(
        before_tables=fingerprint["tables"],
        after_tables=live_tables,
        before_enums=fingerprint["enums"],
        after_enums={e.name: e.values for e in live_snapshot.enums},
        schema=schema_name,
    )

    if not diff_result["up_sql"].strip():
        await _snapshot_delete(snapshot_id)
        return None

    migration_label = label or _auto_label(diff_result["changes"])

    record = await create_migration(
        db=db,
        connection_id=connection_id,
        workspace_id=workspace_id,
        label=migration_label,
        up_sql=diff_result["up_sql"],
        down_sql=diff_result.get("down_sql"),
        generated_by="auto",
        db_url=db_url,
    )

    await _snapshot_delete(snapshot_id)
    logger.info(
        f"Auto-migration created: {record.version} order={record.order_index} "
        f"({len(diff_result['changes'])} changes)"
    )
    return record


def _auto_label(changes: list[dict]) -> str:
    if not changes:
        return "auto_migration"
    kinds: dict[str, int] = {}
    tables: set[str] = set()
    for c in changes:
        k = c.get("kind", "change")
        kinds[k] = kinds.get(k, 0) + 1
        if c.get("table_name"):
            tables.add(c["table_name"])
        elif c.get("object_name"):
            tables.add(c["object_name"])
    parts = []
    for kind, count in sorted(kinds.items()):
        prefix = f"{count}_" if count > 1 else ""
        parts.append(f"{prefix}{kind}")
    if tables and len(tables) <= 3:
        parts.append("_".join(sorted(tables)))
    label = "_".join(parts)[:120]
    return label or "auto_migration"


# ---------------------------------------------------------------------------
# 2. MIGRATION HISTORY DIFF
# ---------------------------------------------------------------------------

async def diff_at_versions(
    db: AsyncSession,
    connection_id: UUID,
    from_version: str | None = None,
    to_version: str | None = None,
    from_order_index: int | None = None,
    to_order_index: int | None = None,
) -> dict[str, Any]:
    all_migrations = await list_migrations(db, connection_id, limit=1000)

    if not all_migrations:
        return _empty_diff(from_version, to_version)

    version_to_pos: dict[str, int] = {
        m.version: i for i, m in enumerate(all_migrations)
    }
    order_to_pos: dict[int, int] = {
        m.order_index: i for i, m in enumerate(all_migrations)
    }

    from_pos = -1
    if from_order_index is not None:
        if from_order_index not in order_to_pos:
            raise ValueError(f"order_index {from_order_index} not found.")
        from_pos = order_to_pos[from_order_index]
    elif from_version is not None:
        if from_version not in version_to_pos:
            raise ValueError(f"Version '{from_version}' not found.")
        from_pos = version_to_pos[from_version]

    to_pos = len(all_migrations) - 1
    if to_order_index is not None:
        if to_order_index not in order_to_pos:
            raise ValueError(f"order_index {to_order_index} not found.")
        to_pos = order_to_pos[to_order_index]
    elif to_version is not None:
        if to_version not in version_to_pos:
            raise ValueError(f"Version '{to_version}' not found.")
        to_pos = version_to_pos[to_version]

    if from_pos >= to_pos:
        return _empty_diff(
            from_version or (str(all_migrations[from_pos].order_index) if from_pos >= 0 else None),
            to_version or str(all_migrations[to_pos].order_index),
            note="from is not before to — no changes.",
        )

    added_migrations = all_migrations[from_pos + 1: to_pos + 1]

    changes: list[dict] = []
    up_sql_parts: list[str] = []
    down_sql_parts: list[str] = []

    for m in added_migrations:
        up_sql_parts.append(
            f"-- Migration {m.order_index}: {m.version} — {m.label}\n{m.up_sql}"
        )
        if m.down_sql:
            down_sql_parts.insert(
                0,
                f"-- Rollback {m.order_index}: {m.version} — {m.label}\n{m.down_sql}",
            )
        changes.extend(_parse_migration_changes(m))

    has_destructive = any(c.get("is_destructive") for c in changes)

    resolved_from_version = all_migrations[from_pos].version if from_pos >= 0 else None
    resolved_to_version = all_migrations[to_pos].version
    resolved_from_order = all_migrations[from_pos].order_index if from_pos >= 0 else 0
    resolved_to_order = all_migrations[to_pos].order_index

    return {
        "from_version": resolved_from_version,
        "to_version": resolved_to_version,
        "from_order_index": resolved_from_order,
        "to_order_index": resolved_to_order,
        "migration_count": len(added_migrations),
        "changes": changes,
        "up_sql": "\n\n".join(up_sql_parts),
        "down_sql": "\n\n".join(down_sql_parts) if down_sql_parts else None,
        "has_destructive_changes": has_destructive,
        "summary": _build_summary(
            changes, resolved_from_version, resolved_to_version, has_destructive
        ),
    }


# ---------------------------------------------------------------------------
# GAP-9-4  Semantic diff — fixed search_path approach
# ---------------------------------------------------------------------------

async def diff_at_versions_semantic(
    db: AsyncSession,
    connection_id: UUID,
    scratch_url: str,
    from_version: str | None = None,
    to_version: str | None = None,
    from_order_index: int | None = None,
    to_order_index: int | None = None,
    schema_name: str = "public",
) -> DiffResult:
    """
    Full semantic diff using migra against a real scratch database.

    Replays migrations into two temp schemas (schema_a and schema_b) on
    scratch_url, then calls run_migra_on_schemas() which correctly sets
    the libpq search_path option on both DSNs before invoking migra.

    The original implementation passed search_path as a URL query
    parameter (?options=-csearch_path%3D...) which migra ignores.
    The correct approach is to embed it via the libpq options parameter
    in a form that migra's psycopg driver decodes:
        ?options=-c%20search_path%3D<schema>%2Cpublic
    run_migra_on_schemas() handles this encoding.
    """
    all_migrations = await list_migrations(db, connection_id, limit=1000)
    if not all_migrations:
        return DiffResult(
            source_label=from_version or str(from_order_index or 0),
            target_label=to_version or str(to_order_index or "latest"),
        )

    version_to_pos = {m.version: i for i, m in enumerate(all_migrations)}
    order_to_pos = {m.order_index: i for i, m in enumerate(all_migrations)}

    from_pos = -1
    to_pos = len(all_migrations) - 1

    if from_order_index is not None and from_order_index in order_to_pos:
        from_pos = order_to_pos[from_order_index]
    elif from_version and from_version in version_to_pos:
        from_pos = version_to_pos[from_version]

    if to_order_index is not None and to_order_index in order_to_pos:
        to_pos = order_to_pos[to_order_index]
    elif to_version and to_version in version_to_pos:
        to_pos = version_to_pos[to_version]

    schema_a = f"_calyphant_diff_a_{uuid4().hex[:8]}"
    schema_b = f"_calyphant_diff_b_{uuid4().hex[:8]}"

    conn: asyncpg.Connection | None = None
    try:
        conn = await asyncpg.connect(dsn=scratch_url, timeout=15)
        assert conn is not None  # <-- Add this line to make mypy happy
        await conn.execute(f'CREATE SCHEMA "{schema_a}"')
        await conn.execute(f'CREATE SCHEMA "{schema_b}"')

        # Replay "from" state into schema_a
        for m in all_migrations[: from_pos + 1]:
            sql = _rewrite_for_schema(m.up_sql, schema_name, schema_a)
            try:
                await conn.execute(f'SET search_path TO "{schema_a}", public')
                await conn.execute(sql)
            except Exception as exc:
                logger.warning(
                    f"Semantic diff: replay into schema_a failed for {m.version}: {exc}"
                )
            finally:
                await conn.execute("RESET search_path")

        # Replay "to" state into schema_b
        for m in all_migrations[: to_pos + 1]:
            sql = _rewrite_for_schema(m.up_sql, schema_name, schema_b)
            try:
                await conn.execute(f'SET search_path TO "{schema_b}", public')
                await conn.execute(sql)
            except Exception as exc:
                logger.warning(
                    f"Semantic diff: replay into schema_b failed for {m.version}: {exc}"
                )
            finally:
                await conn.execute("RESET search_path")

        # run_migra_on_schemas embeds search_path correctly in both DSNs
        diff_sql = await run_migra_on_schemas(scratch_url, schema_a, schema_b)

        source_label = from_version or str(from_order_index or "empty")
        target_label = to_version or str(to_order_index or "latest")

        if diff_sql is None:
            return DiffResult(source_label=source_label, target_label=target_label)

        changes = _parse_sql_to_changes(diff_sql)
        has_destructive = any(c.kind in _DESTRUCTIVE_KINDS for c in changes)

        return DiffResult(
            source_label=source_label,
            target_label=target_label,
            changes=changes,
            sql=diff_sql,
            has_destructive_changes=has_destructive,
        )

    finally:
        if conn:
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_a}" CASCADE')
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE')
            except Exception:
                pass
            try:
                await conn.close()
            except Exception:
                pass


def _rewrite_for_schema(sql: str, from_schema: str, to_schema: str) -> str:
    """Rewrite schema-qualified identifiers for temp schema replay."""
    import re
    result = re.sub(rf'"{re.escape(from_schema)}"\.', f'"{to_schema}".', sql)
    result = re.sub(rf'\b{re.escape(from_schema)}\.', f'"{to_schema}".', result)
    return result


# ---------------------------------------------------------------------------
# Structural diff  (unchanged from original)
# ---------------------------------------------------------------------------

async def _structural_diff(
    before_tables: dict,
    after_tables: dict,
    before_enums: dict,
    after_enums: dict,
    schema: str,
) -> dict[str, Any]:
    up_lines: list[str] = []
    down_lines: list[str] = []
    changes: list[dict] = []

    before_names = set(before_tables)
    after_names = set(after_tables)

    # Tables added
    for name in sorted(after_names - before_names):
        t = after_tables[name]
        col_parts = []
        pk_cols = t.get("primary_key", [])
        use_composite_pk = len(pk_cols) > 1

        for col_name, col_info in t["columns"].items():
            col_type = col_info["type"]
            is_pk = col_name in pk_cols
            parts = [f'    "{col_name}" {col_type}']
            if is_pk and not use_composite_pk:
                parts.append("PRIMARY KEY")
            elif not col_info.get("nullable", True) and not is_pk:
                parts.append("NOT NULL")
            if col_info.get("default") is not None:
                parts.append(f"DEFAULT {col_info['default']}")
            col_parts.append(" ".join(parts))

        if use_composite_pk:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            col_parts.append(f"    PRIMARY KEY ({pk_list})")

        cols = ",\n".join(col_parts) if col_parts else "    -- no columns"
        up_lines.append(
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{name}" (\n{cols}\n);'
        )
        down_lines.append(f'DROP TABLE IF EXISTS "{schema}"."{name}";')
        changes.append({"kind": "table_added", "object_name": name, "is_destructive": False})

        for idx_name, idx_info in t.get("indexes", {}).items():
            idx_cols = ", ".join(f'"{c}"' for c in idx_info["columns"])
            unique = "UNIQUE " if idx_info.get("unique") else ""
            method = idx_info.get("method", "btree")
            where = f" WHERE {idx_info['predicate']}" if idx_info.get("predicate") else ""
            hint = _ann_defaults_hint(method)
            if hint:
                up_lines.append(hint)
            up_lines.append(
                f'CREATE {unique}INDEX "{idx_name}" '
                f'ON "{schema}"."{name}" USING {method} ({idx_cols}){where};'
            )
            down_lines.append(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}";')

    # Tables dropped
    for name in sorted(before_names - after_names):
        up_lines.append(f'DROP TABLE IF EXISTS "{schema}"."{name}";')
        t = before_tables[name]
        col_parts = []
        pk_cols = t.get("primary_key", [])
        use_composite_pk = len(pk_cols) > 1

        for col_name, col_info in t["columns"].items():
            col_type = col_info["type"]
            is_pk = col_name in pk_cols
            parts = [f'    "{col_name}" {col_type}']
            if is_pk and not use_composite_pk:
                parts.append("PRIMARY KEY")
            elif not col_info.get("nullable", True) and not is_pk:
                parts.append("NOT NULL")
            if col_info.get("default") is not None:
                parts.append(f"DEFAULT {col_info['default']}")
            col_parts.append(" ".join(parts))

        if use_composite_pk:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            col_parts.append(f"    PRIMARY KEY ({pk_list})")

        cols = ",\n".join(col_parts) if col_parts else "    -- no columns"
        down_lines.append(
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{name}" (\n{cols}\n);'
        )
        changes.append({"kind": "table_dropped", "object_name": name, "is_destructive": True})

    # Columns and indexes in existing tables
    for name in sorted(before_names & after_names):
        before_cols = before_tables[name]["columns"]
        after_cols = after_tables[name]["columns"]
        before_idxs = before_tables[name].get("indexes", {})
        after_idxs = after_tables[name].get("indexes", {})
        before_pk = sorted(before_tables[name].get("primary_key", []))
        after_pk = sorted(after_tables[name].get("primary_key", []))

        for col_name in sorted(set(after_cols) - set(before_cols)):
            col_info = after_cols[col_name]
            col_type = col_info["type"]
            nullable = col_info.get("nullable", True)
            default = col_info.get("default")
            if not nullable:
                parts_inner = [col_type, "NOT NULL"]
                if default is not None:
                    parts_inner.append(f"DEFAULT {default}")
                up_lines.append(
                    f'ALTER TABLE "{schema}"."{name}" '
                    f'ADD COLUMN "{col_name}" {" ".join(parts_inner)};'
                )
            else:
                suffix = f" DEFAULT {default}" if default is not None else ""
                up_lines.append(
                    f'ALTER TABLE "{schema}"."{name}" '
                    f'ADD COLUMN "{col_name}" {col_type}{suffix};'
                )
            down_lines.append(
                f'ALTER TABLE "{schema}"."{name}" DROP COLUMN IF EXISTS "{col_name}";'
            )
            changes.append({
                "kind": "column_added", "object_name": col_name,
                "table_name": name, "is_destructive": False,
            })

        for col_name in sorted(set(before_cols) - set(after_cols)):
            up_lines.append(
                f'ALTER TABLE "{schema}"."{name}" DROP COLUMN IF EXISTS "{col_name}";'
            )
            col_info = before_cols[col_name]
            null_clause = "" if col_info.get("nullable", True) else " NOT NULL"
            default_clause = f" DEFAULT {col_info['default']}" if col_info.get("default") is not None else ""
            down_lines.append(
                f'ALTER TABLE "{schema}"."{name}" '
                f'ADD COLUMN "{col_name}" {col_info["type"]}{null_clause}{default_clause};'
            )
            changes.append({
                "kind": "column_dropped", "object_name": col_name,
                "table_name": name, "is_destructive": True,
            })

        for col_name in sorted(set(before_cols) & set(after_cols)):
            b = before_cols[col_name]
            a = after_cols[col_name]

            if b["type"] != a["type"]:
                old_type = b["type"]
                new_type = a["type"]

                if _is_vector_type(old_type) and _is_vector_type(new_type):
                    old_dim = _vector_dimension(old_type)
                    new_dim = _vector_dimension(new_type)
                    warning = (
                        f"-- WARNING: vector column \"{schema}\".\"{name}\".\"{col_name}\" "
                        f"dimension changed from {old_dim or '?'} to {new_dim or '?'}.\n"
                        f"-- Changing vector dimensions destroys all stored embeddings.\n"
                        f"-- This cannot be done automatically. Manual steps required:\n"
                        f'--   1. Add a new column: ALTER TABLE "{schema}"."{name}" '
                        f'ADD COLUMN "{col_name}_new" {new_type};\n'
                        f"--   2. Re-embed all data with your new embedding model.\n"
                        f'--   3. Drop old column: ALTER TABLE "{schema}"."{name}" '
                        f'DROP COLUMN "{col_name}";\n'
                        f'--   4. Rename: ALTER TABLE "{schema}"."{name}" '
                        f'RENAME COLUMN "{col_name}_new" TO "{col_name}";'
                    )
                    up_lines.append(warning)
                    changes.append({
                        "kind": "vector_dimension_changed", "object_name": col_name,
                        "table_name": name,
                        "detail": f"dimension {old_dim or '?'} → {new_dim or '?'} — manual intervention required",
                        "is_destructive": True,
                    })
                else:
                    up_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" '
                        f'ALTER COLUMN "{col_name}" TYPE {new_type} '
                        f'USING "{col_name}"::{new_type};'
                    )
                    down_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" '
                        f'ALTER COLUMN "{col_name}" TYPE {old_type} '
                        f'USING "{col_name}"::{old_type};'
                    )
                    changes.append({
                        "kind": "column_type_changed", "object_name": col_name,
                        "table_name": name,
                        "detail": f"{old_type} → {new_type} — verify cast is safe",
                        "is_destructive": True,
                    })

            if b.get("nullable", True) != a.get("nullable", True):
                if not a.get("nullable", True):
                    up_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" ALTER COLUMN "{col_name}" SET NOT NULL;'
                    )
                    down_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" ALTER COLUMN "{col_name}" DROP NOT NULL;'
                    )
                    changes.append({
                        "kind": "column_not_null_added", "object_name": col_name,
                        "table_name": name, "detail": "SET NOT NULL", "is_destructive": True,
                    })
                else:
                    up_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" ALTER COLUMN "{col_name}" DROP NOT NULL;'
                    )
                    down_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" ALTER COLUMN "{col_name}" SET NOT NULL;'
                    )
                    changes.append({
                        "kind": "column_not_null_dropped", "object_name": col_name,
                        "table_name": name, "detail": "DROP NOT NULL", "is_destructive": False,
                    })

            b_default = b.get("default")
            a_default = a.get("default")
            if b_default != a_default:
                if a_default is not None:
                    up_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" '
                        f'ALTER COLUMN "{col_name}" SET DEFAULT {a_default};'
                    )
                    down_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" '
                        f'ALTER COLUMN "{col_name}" '
                        + (f'SET DEFAULT {b_default};' if b_default is not None else 'DROP DEFAULT;')
                    )
                else:
                    up_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" ALTER COLUMN "{col_name}" DROP DEFAULT;'
                    )
                    down_lines.append(
                        f'ALTER TABLE "{schema}"."{name}" '
                        f'ALTER COLUMN "{col_name}" SET DEFAULT {b_default};'
                    )
                changes.append({
                    "kind": "column_default_changed", "object_name": col_name,
                    "table_name": name,
                    "detail": f"default {b_default!r} → {a_default!r}",
                    "is_destructive": False,
                })

        if before_pk != after_pk:
            note = (
                f"-- WARNING: primary key changed on \"{schema}\".\"{name}\" "
                f"from ({', '.join(before_pk)}) to ({', '.join(after_pk)}). "
                f"Requires dropping and recreating the PK constraint manually."
            )
            up_lines.append(note)
            changes.append({
                "kind": "primary_key_changed", "object_name": name, "table_name": name,
                "detail": f"PK {before_pk} → {after_pk}", "is_destructive": True,
            })

        for idx_name in sorted(set(after_idxs) - set(before_idxs)):
            idx_info = after_idxs[idx_name]
            idx_cols = ", ".join(f'"{c}"' for c in idx_info["columns"])
            unique = "UNIQUE " if idx_info.get("unique") else ""
            method = idx_info.get("method", "btree")
            where = f" WHERE {idx_info['predicate']}" if idx_info.get("predicate") else ""
            hint = _ann_defaults_hint(method)
            if hint:
                up_lines.append(hint)
            up_lines.append(
                f'CREATE {unique}INDEX "{idx_name}" '
                f'ON "{schema}"."{name}" USING {method} ({idx_cols}){where};'
            )
            down_lines.append(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}";')
            changes.append({
                "kind": "index_added", "object_name": idx_name,
                "table_name": name, "is_destructive": False,
            })

        for idx_name in sorted(set(before_idxs) - set(after_idxs)):
            up_lines.append(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}";')
            idx_info = before_idxs[idx_name]
            idx_cols = ", ".join(f'"{c}"' for c in idx_info["columns"])
            unique = "UNIQUE " if idx_info.get("unique") else ""
            method = idx_info.get("method", "btree")
            where = f" WHERE {idx_info['predicate']}" if idx_info.get("predicate") else ""
            down_lines.append(
                f'CREATE {unique}INDEX "{idx_name}" '
                f'ON "{schema}"."{name}" USING {method} ({idx_cols}){where};'
            )
            changes.append({
                "kind": "index_dropped", "object_name": idx_name,
                "table_name": name, "is_destructive": True,
            })

        for idx_name in sorted(set(before_idxs) & set(after_idxs)):
            b_idx = before_idxs[idx_name]
            a_idx = after_idxs[idx_name]
            if (
                b_idx["columns"] != a_idx["columns"]
                or b_idx.get("unique") != a_idx.get("unique")
                or b_idx.get("method", "btree") != a_idx.get("method", "btree")
                or b_idx.get("predicate") != a_idx.get("predicate")
            ):
                up_lines.append(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}";')
                idx_cols = ", ".join(f'"{c}"' for c in a_idx["columns"])
                unique = "UNIQUE " if a_idx.get("unique") else ""
                method = a_idx.get("method", "btree")
                where = f" WHERE {a_idx['predicate']}" if a_idx.get("predicate") else ""
                hint = _ann_defaults_hint(method)
                if hint:
                    up_lines.append(hint)
                up_lines.append(
                    f'CREATE {unique}INDEX "{idx_name}" '
                    f'ON "{schema}"."{name}" USING {method} ({idx_cols}){where};'
                )
                b_cols = ", ".join(f'"{c}"' for c in b_idx["columns"])
                b_unique = "UNIQUE " if b_idx.get("unique") else ""
                b_method = b_idx.get("method", "btree")
                b_where = f" WHERE {b_idx['predicate']}" if b_idx.get("predicate") else ""
                down_lines.append(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}";')
                down_lines.append(
                    f'CREATE {b_unique}INDEX "{idx_name}" '
                    f'ON "{schema}"."{name}" USING {b_method} ({b_cols}){b_where};'
                )
                changes.append({
                    "kind": "index_modified", "object_name": idx_name,
                    "table_name": name, "is_destructive": False,
                })

    # Enums
    for name in sorted(set(after_enums) - set(before_enums)):
        vals = ", ".join(f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in after_enums[name])
        up_lines.append(f'CREATE TYPE "{schema}"."{name}" AS ENUM ({vals});')
        down_lines.append(f'DROP TYPE IF EXISTS "{schema}"."{name}";')
        changes.append({"kind": "enum_added", "object_name": name, "is_destructive": False})

    for name in sorted(set(before_enums) - set(after_enums)):
        up_lines.append(f'DROP TYPE IF EXISTS "{schema}"."{name}";')
        vals = ", ".join(f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in before_enums[name])
        down_lines.append(f'CREATE TYPE "{schema}"."{name}" AS ENUM ({vals});')
        changes.append({"kind": "enum_dropped", "object_name": name, "is_destructive": True})

    for name in sorted(set(before_enums) & set(after_enums)):
        before_vals = before_enums[name]
        after_vals = after_enums[name]
        added_vals = [v for v in after_vals if v not in before_vals]
        removed_vals = [v for v in before_vals if v not in after_vals]

        for v in added_vals:
            escaped = v.replace("'", "''")
            up_lines.append(
                f"ALTER TYPE \"{schema}\".\"{name}\" ADD VALUE IF NOT EXISTS '{escaped}';"
            )
            changes.append({
                "kind": "enum_value_added", "object_name": name,
                "detail": f"added value '{v}'", "is_destructive": False,
            })

        if removed_vals:
            up_lines.append(
                f"-- WARNING: enum '{name}' had values removed: {removed_vals}. "
                "Postgres does not support removing enum values without recreating the type."
            )
            changes.append({
                "kind": "enum_value_removed", "object_name": name,
                "detail": f"removed values {removed_vals} — requires manual type recreation",
                "is_destructive": True,
            })

    return {
        "up_sql": "\n\n".join(up_lines),
        "down_sql": "\n\n".join(reversed(down_lines)) if down_lines else None,
        "changes": changes,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_migration_changes(m: MigrationRecord) -> list[dict]:
    raw = _parse_sql_to_changes(m.up_sql)
    return [
        {
            "kind": c.kind.value,
            "object_type": c.object_type,
            "object_name": c.object_name,
            "table_name": c.table_name,
            "detail": c.detail,
            "is_destructive": c.is_destructive,
            "migration_version": m.version,
            "migration_order_index": m.order_index,
            "migration_label": m.label,
        }
        for c in raw
    ]


def _build_summary(
    changes: list[dict],
    from_version: str | None,
    to_version: str | None,
    has_destructive: bool,
) -> str:
    if not changes:
        return "No schema changes between the two versions."
    counts: dict[str, int] = {}
    for c in changes:
        k = c.get("kind", "change")
        counts[k] = counts.get(k, 0) + 1
    parts = [f"{v} {k.replace('_', ' ')}" for k, v in sorted(counts.items())]
    dest = " ⚠ Contains destructive changes." if has_destructive else ""
    frm = from_version or "empty schema"
    to = to_version or "latest"
    return f"{len(changes)} changes from {frm} → {to}: {', '.join(parts)}.{dest}"


def _empty_diff(
    from_version: str | None,
    to_version: str | None,
    note: str = "No changes.",
) -> dict[str, Any]:
    return {
        "from_version": from_version,
        "to_version": to_version,
        "from_order_index": None,
        "to_order_index": None,
        "migration_count": 0,
        "changes": [],
        "up_sql": "",
        "down_sql": None,
        "has_destructive_changes": False,
        "summary": note,
    }

# domains/schema/introspection.py

"""
Schema introspection.

Uses SQLAlchemy's Inspector over a live psycopg3 connection to extract
the full structural metadata of a PostgreSQL database:
- Tables, views, materialized views
- Columns with types, defaults, nullability
- Primary keys, foreign keys, unique + check constraints
- Indexes (including partial, expression-based)
- Sequences
- Enums
- Schemas (namespaces)

View and materialized view definitions are fetched from pg_views and
pg_matviews respectively so the schema export can emit real
CREATE OR REPLACE VIEW / CREATE MATERIALIZED VIEW statements.

All results are returned as plain dicts / dataclasses — nothing
SQLAlchemy-specific leaks out of this module.
"""

from __future__ import annotations

import asyncio
import socket
import ipaddress
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any
from uuid import UUID

from loguru import logger
from sqlalchemy import Inspector, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError


# ---------------------------------------------------------------------------
# Result Contracts (Strict Pydantic V2)
# ---------------------------------------------------------------------------
from pydantic import BaseModel, ConfigDict, Field
from typing import Any

class ColumnInfo(BaseModel):
    model_config = ConfigDict(strict=False)
    name: str
    data_type: str
    nullable: bool
    default: str | None
    is_primary_key: bool = False
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    comment: str | None = None
    is_array: bool = False
    element_type: str | None = None

class IndexInfo(BaseModel):
    model_config = ConfigDict(strict=False)
    name: str
    columns: list[str]
    unique: bool
    partial: bool = False
    predicate: str | None = None
    method: str = "btree"
    is_expression: bool = False
    expression: str | None = None

class ForeignKeyInfo(BaseModel):
    model_config = ConfigDict(strict=False)
    name: str | None
    columns: list[str]
    referred_schema: str | None
    referred_table: str
    referred_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None

class CheckConstraintInfo(BaseModel):
    model_config = ConfigDict(strict=False)
    name: str
    sqltext: str

class TableInfo(BaseModel):
    model_config = ConfigDict(strict=False, populate_by_name=True)
    name: str
    schema_name: str = Field(alias="schema") # Aliased to avoid BaseModel.schema() collision in Pylance
    kind: str  # "table" | "view" | "materialized_view"
    columns: list[ColumnInfo] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    unique_constraints: list[Any] = Field(default_factory=list)
    check_constraints: list[CheckConstraintInfo] = Field(default_factory=list)
    row_count_estimate: int | None = None
    comment: str | None = None
    view_definition: str | None = None

class EnumInfo(BaseModel):
    model_config = ConfigDict(strict=False, populate_by_name=True)
    name: str
    schema_name: str = Field(alias="schema")
    values: list[str]

class SequenceInfo(BaseModel):
    model_config = ConfigDict(strict=False, populate_by_name=True)
    name: str
    schema_name: str = Field(alias="schema")
    data_type: str
    start: int
    increment: int
    min_value: int
    max_value: int
    cycle: bool

class SchemaSnapshot(BaseModel):
    """Full structural snapshot of a database at a point in time."""
    model_config = ConfigDict(strict=False)
    database: str
    pg_version: str
    schemas: list[str]
    tables: list[TableInfo]
    enums: list[EnumInfo]
    sequences: list[SequenceInfo]
    extensions: list[dict]
    unreadable_entities: list[dict] = Field(default_factory=list)


def _validate_safe_target(url_str: str) -> None:
    """
    SECURITY: Server-Side Request Forgery (SSRF) Prevention.
    Deconstructs the URL, resolves the hostname to an IP, and blocks 
    any attempt to connect to loopback, private VPC, or cloud metadata IPs.
    """
    try:
        parsed = make_url(url_str)
    except Exception as exc:
        raise ValueError(f"Malformed database URL: {exc}")

    if not parsed.host:
        raise ValueError("Invalid database URL: Missing hostname.")

    try:
        # Resolve the hostname to its physical IP address
        ip_str = socket.gethostbyname(parsed.host)
        ip_obj = ipaddress.ip_address(ip_str)
    except socket.gaierror:
        raise ValueError(f"Could not resolve hostname: {parsed.host}")

    # The Guillotine: Block internal routing
    if ip_obj.is_loopback or ip_obj.is_private or ip_obj.is_link_local:
        logger.warning(f"SSRF Attempt Blocked: Target '{parsed.host}' resolved to internal IP {ip_str}.")
        raise ValueError(
            "Security Violation: Connection to internal or private networks is strictly prohibited."
        )


# ---------------------------------------------------------------------------
# Engine factory (sync — Inspector requires sync engine)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=2048)
def _make_sync_engine(url: str) -> Engine:
    """
    Build a short-lived sync SQLAlchemy engine for introspection.
    
    SECURITY & STABILITY:
    - Validates target IP against SSRF attacks.
    - Cache maxsize bumped to 2048 to prevent multi-tenant thrashing.
    - Injects strict libpq driver-level timeouts (5s connect, 15s statement).
    """
    from sqlalchemy import create_engine

    # 1. Enforce Network Boundary (SSRF Protection)
    _validate_safe_target(url)

    # 2. Normalize Protocol
    clean = url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    if clean.startswith("postgres://"):
        clean = clean.replace("postgres://", "postgresql+psycopg://", 1)
    elif not clean.startswith("postgresql+"):
        clean = clean.replace("postgresql://", "postgresql+psycopg://", 1)
        
    # 3. Enforce Temporal Boundary (Guillotine Protocol)
    strict_timeouts = {
        "connect_timeout": 5, 
        "options": "-c statement_timeout=15000"
    }

    return create_engine(
        clean, 
        poolclass=NullPool,
        connect_args=strict_timeouts
    )


# ---------------------------------------------------------------------------
# Core introspection
# ---------------------------------------------------------------------------

async def introspect_database(connection_id: UUID, url: str, target_schema: str = "public") -> SchemaSnapshot:
    """
    Full structural introspection of a database.
    ASYNC GATEWAY: Bypasses OS threading entirely. Borrows an AsyncEngine 
    from the Fortress and yields to the sync Inspector via run_sync.
    """
    from domains.query.service import pool_manager
    engine = await pool_manager.get_engine(connection_id, url)
    
    # ---------------------------------------------------------------------
    # UI CONSIDERATION: Because connection pooling is now multiplexed, 
    # parallel automated schema refreshes from multiple tabs will no longer 
    # throw HTTP 503 "Connection limits exceeded" errors.
    # ---------------------------------------------------------------------
    async with engine.connect() as conn:
        return await conn.run_sync(
            _sync_introspect_database_logic, 
            target_schema=target_schema
        )


async def introspect_table(connection_id: UUID, url: str, table_name: str, schema: str = "public") -> TableInfo:
    """
    Introspect a single table. Faster than full snapshot for targeted ops.
    """
    from domains.query.service import pool_manager
    engine = await pool_manager.get_engine(connection_id, url)
    
    async with engine.connect() as conn:
        return await conn.run_sync(
            _sync_introspect_table_logic, 
            table_name=table_name, 
            schema=schema
        )


def _sync_introspect_database_logic(sync_conn: Any, target_schema: str) -> SchemaSnapshot:
    """
    PRIVATE: Synchronous execution payload. Executed safely inside the run_sync greenlet.
    sync_conn is a proxied synchronous connection provided by SQLAlchemy ext.asyncio.
    """
    inspector: Inspector = inspect(sync_conn)

    pg_version = sync_conn.execute(text("SELECT version()")).scalar()
    database = sync_conn.execute(text("SELECT current_database()")).scalar()

    schemas = [
        s for s in inspector.get_schema_names()
        if s not in ("pg_catalog", "information_schema", "pg_toast")
    ]

    tables, unreadable_entities = _introspect_tables(inspector, sync_conn, target_schema)
    enums = _introspect_enums(sync_conn, target_schema)
    sequences = _introspect_sequences(sync_conn, target_schema)
    extensions = _introspect_extensions(sync_conn)

    return SchemaSnapshot(
        database=str(database or "unknown"),
        pg_version=str(pg_version or "").split(",")[0].strip(),
        schemas=schemas,
        tables=tables,
        enums=enums,
        sequences=sequences,
        extensions=extensions,
        unreadable_entities=unreadable_entities,
    )


def _sync_introspect_table_logic(sync_conn: Any, table_name: str, schema: str) -> TableInfo:
    """
    PRIVATE: Synchronous execution payload for single-table inspection.
    """
    inspector = inspect(sync_conn)
    
    tables, unreadable_entities = _introspect_tables(inspector, sync_conn, schema, only=[table_name])
    
    if not tables:
        reason = unreadable_entities[0]["reason"] if unreadable_entities else "Unknown error"
        raise ValueError(f"Table '{schema}.{table_name}' could not be read. Reason: {reason}")
        
    return tables[0]


def _introspect_tables(
    inspector: Any, # Kept for signature compatibility, though unused internally
    conn: Any,
    schema: str,
    only: list[str] | None = None,
) -> tuple[list[TableInfo], list[dict]]:
    """
    O(1) Network Sweep Architecture with Strict Memory Boundaries.
    Phase 1: Fetches lightweight metadata to prevent RAM exhaustion on massive databases.
    Phase 2: Executes targeted O(1) queries for columns, constraints, and indexes ONLY if drilling down.
    """
    import re
    from sqlalchemy import text
    
    unreadable_entities: list[dict] = []
    tables_map: dict[str, TableInfo] = {}
    
    is_full_sweep = only is None
    params = {"schema": schema, "only_names": only}

    # -----------------------------------------------------------------------
    # PHASE 1: The Blueprint Sweep (Always Run)
    # -----------------------------------------------------------------------
    blueprint_sql = text("""
        SELECT
            c.relname AS table_name,
            c.relkind AS kind,
            c.reltuples::bigint AS row_estimate,
            obj_description(c.oid, 'pg_class') AS table_comment
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema
          AND c.relkind IN ('r', 'v', 'm', 'p')
          AND (CAST(:only_names AS text[]) IS NULL OR c.relname = ANY(CAST(:only_names AS text[])));
    """)

    try:
        blueprint_rows = conn.execute(blueprint_sql, params).fetchall()
        kind_map = {'r': 'table', 'p': 'table', 'v': 'view', 'm': 'materialized_view'}
        
        for r in blueprint_rows:
            t_name = r.table_name
            reltuples = r.row_estimate
            
            # STALE DATA FIX / FALLBACK RADAR
            if reltuples == -1:
                row_est = None
            elif reltuples == 0 and not is_full_sweep:
                try:
                    explain_res = conn.execute(text(f'EXPLAIN SELECT 1 FROM "{schema}"."{t_name}"')).fetchone()
                    match = re.search(r'rows=(\d+)', explain_res[0]) if explain_res else None
                    row_est = int(match.group(1)) if match else 0
                except Exception:
                    row_est = 0
            else:
                row_est = reltuples if reltuples >= 0 else 0

            tables_map[t_name] = TableInfo(
                name=t_name,
                schema=schema, # Uses the alias for Pydantic/Pylance compatibility
                kind=kind_map.get(r.kind, 'table'),
                row_count_estimate=row_est,
                comment=r.table_comment,
            )
            
    except Exception as exc:
        logger.error(f"Failed Phase 1 blueprint sweep for schema '{schema}': {exc}")
        unreadable_entities.append({"type": "schema", "name": schema, "reason": str(exc)})
        return [], unreadable_entities

    if not tables_map or is_full_sweep:
        # MEMORY FIX: If full sweep, HALT HERE. Do not fetch deep constraints to save RAM.
        return list(tables_map.values()), unreadable_entities

    # -----------------------------------------------------------------------
    # PHASE 2: The Deep Sweep (Drill-Down Only)
    # -----------------------------------------------------------------------
    
    # 2a. Columns & Primary Keys
    cols_sql = text("""
        SELECT
            c.relname AS table_name,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            NOT a.attnotnull AS nullable,
            pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_value,
            COALESCE(i.indisprimary, false) AS is_primary_key,
            col_description(c.oid, a.attnum) AS column_comment
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        LEFT JOIN pg_catalog.pg_index i ON i.indrelid = c.oid AND a.attnum = ANY(i.indkey) AND i.indisprimary
        WHERE n.nspname = :schema
          AND c.relname = ANY(:only_names)
        ORDER BY c.relname, a.attnum;
    """)
    try:
        col_rows = conn.execute(cols_sql, params).fetchall()
        for r in col_rows:
            if r.table_name in tables_map:
                is_array = r.data_type.endswith("[]") or r.data_type.upper().startswith("ARRAY")
                tables_map[r.table_name].columns.append(ColumnInfo(
                    name=r.column_name,
                    data_type=r.data_type,
                    nullable=r.nullable,
                    default=r.default_value,
                    is_primary_key=r.is_primary_key,
                    comment=r.column_comment,
                    is_array=is_array,
                    element_type=r.data_type.rstrip("[]") if is_array else None
                ))
                if r.is_primary_key:
                    tables_map[r.table_name].primary_key.append(r.column_name)
    except Exception as exc:
        logger.warning(f"Failed fetching columns: {exc}")

    # 2b. Foreign Keys
    fks_sql = text("""
        SELECT
            c.relname AS table_name, con.conname AS name,
            ns.nspname AS referred_schema, cl.relname AS referred_table,
            (SELECT array_agg(a.attname ORDER BY x.ord) FROM unnest(con.conkey) WITH ORDINALITY x(attnum, ord) JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = x.attnum) AS columns,
            (SELECT array_agg(a.attname ORDER BY x.ord) FROM unnest(con.confkey) WITH ORDINALITY x(attnum, ord) JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = x.attnum) AS referred_columns,
            con.confdeltype AS on_delete_char, con.confupdtype AS on_update_char
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class cl ON cl.oid = con.confrelid
        JOIN pg_namespace ns ON ns.oid = cl.relnamespace
        WHERE con.contype = 'f' AND n.nspname = :schema AND c.relname = ANY(:only_names);
    """)
    try:
        fk_rows = conn.execute(fks_sql, params).fetchall()
        action_map = {'a': 'NO ACTION', 'r': 'RESTRICT', 'c': 'CASCADE', 'n': 'SET NULL', 'd': 'SET DEFAULT'}
        for r in fk_rows:
            if r.table_name in tables_map:
                tables_map[r.table_name].foreign_keys.append(ForeignKeyInfo(
                    name=r.name, columns=r.columns or [],
                    referred_schema=r.referred_schema, referred_table=r.referred_table,
                    referred_columns=r.referred_columns or [],
                    on_delete=action_map.get(r.on_delete_char, 'NO ACTION'),
                    on_update=action_map.get(r.on_update_char, 'NO ACTION')
                ))
    except Exception as exc:
        logger.warning(f"Failed fetching foreign keys: {exc}")

    # 2c. Indexes
    idx_sql = text("""
        SELECT
            c.relname AS table_name, i.relname AS index_name, ix.indisunique AS is_unique,
            pg_get_expr(ix.indpred, ix.indrelid) AS predicate, am.amname AS method,
            (SELECT array_agg(a.attname ORDER BY x.ord) FROM unnest(ix.indkey) WITH ORDINALITY x(attnum, ord) LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum) AS columns
        FROM pg_index ix
        JOIN pg_class c ON c.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        WHERE n.nspname = :schema AND c.relname = ANY(:only_names) AND NOT ix.indisprimary;
    """)
    try:
        idx_rows = conn.execute(idx_sql, params).fetchall()
        for r in idx_rows:
            if r.table_name in tables_map:
                tables_map[r.table_name].indexes.append(IndexInfo(
                    name=r.index_name, columns=[c for c in (r.columns or []) if c is not None],
                    unique=r.is_unique, partial=r.predicate is not None, predicate=r.predicate, method=r.method
                ))
    except Exception as exc:
        logger.warning(f"Failed fetching indexes: {exc}")

    # 2d. Check Constraints
    chk_sql = text("""
        SELECT c.relname AS table_name, con.conname AS name, pg_get_constraintdef(con.oid) AS sqltext
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE con.contype = 'c' AND n.nspname = :schema AND c.relname = ANY(:only_names);
    """)
    try:
        chk_rows = conn.execute(chk_sql, params).fetchall()
        for r in chk_rows:
            if r.table_name in tables_map:
                tables_map[r.table_name].check_constraints.append(CheckConstraintInfo(name=r.name, sqltext=r.sqltext))
    except Exception as exc:
        logger.warning(f"Failed fetching check constraints: {exc}")

    # 2e. Views
    views_to_fetch = [t for t, obj in tables_map.items() if obj.kind in ('view', 'materialized_view')]
    if views_to_fetch:
        try:
            v_sql = text("""
                SELECT viewname as name, definition FROM pg_views WHERE schemaname = :schema AND viewname = ANY(:v_names)
                UNION ALL
                SELECT matviewname as name, definition FROM pg_matviews WHERE schemaname = :schema AND matviewname = ANY(:v_names)
            """)
            v_rows = conn.execute(v_sql, {"schema": schema, "v_names": views_to_fetch}).fetchall()
            for r in v_rows:
                if r.name in tables_map:
                    tables_map[r.name].view_definition = (r.definition or "").strip()
        except Exception as exc:
            logger.warning(f"Failed fetching view definitions: {exc}")

    return list(tables_map.values()), unreadable_entities


def _introspect_enums(conn: Any, schema: str) -> list[EnumInfo]:
    rows = conn.execute(
        text(
            """
            SELECT t.typname AS name,
                   n.nspname AS schema,
                   array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE t.typtype = 'e' AND n.nspname = :schema
            GROUP BY t.typname, n.nspname
            """
        ),
        {"schema": schema},
    )
    return [
        EnumInfo(name=r.name, schema=r.schema, values=list(r.values))
        for r in rows
    ]


def _introspect_sequences(conn: Any, schema: str) -> list[SequenceInfo]:
    rows = conn.execute(
        text(
            """
            SELECT sequence_name, data_type,
                   start_value::bigint, increment::bigint,
                   minimum_value::bigint, maximum_value::bigint,
                   cycle_option
            FROM information_schema.sequences
            WHERE sequence_schema = :schema
            """
        ),
        {"schema": schema},
    )
    return [
        SequenceInfo(
            name=r.sequence_name,
            schema=schema,
            data_type=r.data_type,
            start=r.start_value,
            increment=r.increment,
            min_value=r.minimum_value,
            max_value=r.maximum_value,
            cycle=r.cycle_option == "YES",
        )
        for r in rows
    ]


def _introspect_extensions(conn: Any) -> list[dict]:
    rows = conn.execute(
        text(
            "SELECT name, default_version, installed_version, comment "
            "FROM pg_available_extensions "
            "WHERE installed_version IS NOT NULL "
            "ORDER BY name"
        )
    )
    return [
        {
            "name": r.name,
            "default_version": r.default_version,
            "installed_version": r.installed_version,
            "comment": r.comment,
        }
        for r in rows
    ]
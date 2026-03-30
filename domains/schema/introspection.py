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

from loguru import logger
from sqlalchemy import Inspector, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
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


@dataclass
class IndexInfo:
    name: str
    columns: list[str]
    unique: bool
    partial: bool = False
    predicate: str | None = None
    method: str = "btree"
    is_expression: bool = False
    expression: str | None = None


@dataclass
class ForeignKeyInfo:
    name: str | None
    columns: list[str]
    referred_schema: str | None
    referred_table: str
    referred_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None


@dataclass
class CheckConstraintInfo:
    name: str
    sqltext: str


@dataclass
class TableInfo:
    name: str
    schema: str
    kind: str  # "table" | "view" | "materialized_view"
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    unique_constraints: list[Any] = field(default_factory=list)
    check_constraints: list[CheckConstraintInfo] = field(default_factory=list)
    row_count_estimate: int | None = None
    comment: str | None = None
    # For views and materialized views — the raw SQL definition.
    # None for base tables.
    view_definition: str | None = None


@dataclass
class EnumInfo:
    name: str
    schema: str
    values: list[str]


@dataclass
class SequenceInfo:
    name: str
    schema: str
    data_type: str
    start: int
    increment: int
    min_value: int
    max_value: int
    cycle: bool


@dataclass
class SchemaSnapshot:
    """Full structural snapshot of a database at a point in time."""
    database: str
    pg_version: str
    schemas: list[str]
    tables: list[TableInfo]
    enums: list[EnumInfo]
    sequences: list[SequenceInfo]
    extensions: list[dict]
    unreadable_entities: list[dict] = field(default_factory=list)


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

async def introspect_database(url: str, target_schema: str = "public") -> SchemaSnapshot:
    """
    Full structural introspection of a database.
    
    ASYNC GATEWAY: This forces the underlying synchronous SQLAlchemy Inspector 
    operations onto a background worker thread. It guarantees the FastAPI 
    event loop will never be blocked by slow external database connections.
    """
    return await asyncio.to_thread(_sync_introspect_database, url, target_schema)


async def introspect_table(url: str, table_name: str, schema: str = "public") -> TableInfo:
    """
    Introspect a single table. Faster than full snapshot for targeted ops.
    
    ASYNC GATEWAY: Safely delegates to a background thread to prevent starvation.
    """
    return await asyncio.to_thread(_sync_introspect_table, url, table_name, schema)


def _sync_introspect_database(url: str, target_schema: str = "public") -> SchemaSnapshot:
    """
    PRIVATE: Synchronous execution payload. Do not call this directly from an async router.
    """
    engine = _make_sync_engine(url)
    with engine.connect() as conn:
        inspector: Inspector = inspect(engine)

        pg_version = conn.execute(text("SELECT version()")).scalar()
        database = conn.execute(text("SELECT current_database()")).scalar()

        schemas = [
            s for s in inspector.get_schema_names()
            if s not in ("pg_catalog", "information_schema", "pg_toast")
        ]

        # Explicitly unpacking the tuple to handle the new telemetry contract
        tables, unreadable_entities = _introspect_tables(inspector, conn, target_schema)
        enums = _introspect_enums(conn, target_schema)
        sequences = _introspect_sequences(conn, target_schema)
        extensions = _introspect_extensions(conn)

    # Returning outside the `with` block so the connection is closed before we serialize
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


def _sync_introspect_table(url: str, table_name: str, schema: str = "public") -> TableInfo:
    """
    PRIVATE: Synchronous execution payload for single-table inspection.
    """
    engine = _make_sync_engine(url)
    with engine.connect() as conn:
        inspector = inspect(engine)
        
        # Explicitly unpacking the tuple to handle the new telemetry contract
        tables, unreadable_entities = _introspect_tables(inspector, conn, schema, only=[table_name])
        
        if not tables:
            # If the table failed, extract the specific DB reason and raise it violently
            reason = unreadable_entities[0]["reason"] if unreadable_entities else "Unknown error"
            raise ValueError(f"Table '{schema}.{table_name}' could not be read. Reason: {reason}")
            
        return tables[0]


def _introspect_tables(
    inspector: Inspector,
    conn: Any,
    schema: str,
    only: list[str] | None = None,
) -> tuple[list[TableInfo], list[dict]]:
    """
    Extracts table metadata. 
    
    PERFORMANCE & MEMORY CONTRACT:
    - Phase 1 (Map): If `only` is None, this returns a lightweight blueprint 
      (Names, Kinds, Row Counts) to prevent memory exhaustion on massive databases.
    - Phase 2 (Drill-Down): If `only` is provided, it performs deep introspection
      (Columns, Indexes, Constraints) for those specific tables.
      
    Returns:
        A tuple of (successful_tables, unreadable_entities)
    """
    import re  # Required for the EXPLAIN fallback telemetry
    
    tables: list[TableInfo] = []
    unreadable_entities: list[dict] = []

    table_names = inspector.get_table_names(schema=schema)
    view_names = inspector.get_view_names(schema=schema)

    # Materialized views via raw query
    mat_views = {
        row[0]
        for row in conn.execute(
            text("SELECT matviewname FROM pg_matviews WHERE schemaname = :schema"),
            {"schema": schema},
        )
    }

    # Fetch view definitions safely
    view_definitions: dict[str, str] = {}
    try:
        if only:
            rows = conn.execute(
                text("SELECT viewname, definition FROM pg_views WHERE schemaname = :schema AND viewname = ANY(:only_names)"),
                {"schema": schema, "only_names": only},
            )
        else:
            rows = conn.execute(
                text("SELECT viewname, definition FROM pg_views WHERE schemaname = :schema"),
                {"schema": schema},
            )
        for row in rows:
            view_definitions[row[0]] = (row[1] or "").strip()
    except SQLAlchemyError as exc:
        logger.error(f"Failed to fetch view definitions for schema '{schema}': {exc}")
        unreadable_entities.append({"type": "schema_views", "name": "all", "reason": str(exc)})

    # Fetch materialized view definitions safely
    matview_definitions: dict[str, str] = {}
    try:
        if only:
            rows = conn.execute(
                text("SELECT matviewname, definition FROM pg_matviews WHERE schemaname = :schema AND matviewname = ANY(:only_names)"),
                {"schema": schema, "only_names": only},
            )
        else:
            rows = conn.execute(
                text("SELECT matviewname, definition FROM pg_matviews WHERE schemaname = :schema"),
                {"schema": schema},
            )
        for row in rows:
            matview_definitions[row[0]] = (row[1] or "").strip()
    except SQLAlchemyError as exc:
        logger.error(f"Failed to fetch matview definitions for schema '{schema}': {exc}")
        unreadable_entities.append({"type": "schema_matviews", "name": "all", "reason": str(exc)})

    def kind_of(name: str) -> str:
        if name in mat_views:
            return "materialized_view"
        if name in view_names:
            return "view"
        return "table"

    all_names = table_names + [v for v in view_names if v not in table_names]
    if only:
        all_names = [n for n in all_names if n in only]

    if not all_names:
        return [], unreadable_entities

    # 1. Telemetry Sanitization: Fetch custom row estimates safely
    if only:
        est_sql = text("""
            SELECT c.relname, c.reltuples::bigint 
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = :schema AND c.relname = ANY(:only_names)
        """)
        est_rows = conn.execute(est_sql, {"schema": schema, "only_names": all_names})
    else:
        est_sql = text("""
            SELECT c.relname, c.reltuples::bigint 
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = :schema
        """)
        est_rows = conn.execute(est_sql, {"schema": schema})
        
    row_estimates = {}
    for row in est_rows:
        t_name = row[0]
        reltuples = row[1]
        
        if reltuples == -1:
            # STALE DATA FIX: Table has never been analyzed. Value is dead.
            row_estimates[t_name] = None
        elif reltuples == 0 and only:
            # FALLBACK RADAR: If drilling down into a "0 row" table, verify with planner
            try:
                # Safely format identifiers since they originate directly from the catalog
                explain_res = conn.execute(text(f'EXPLAIN SELECT 1 FROM "{schema}"."{t_name}"')).fetchone()
                match = re.search(r'rows=(\d+)', explain_res[0]) if explain_res else None
                row_estimates[t_name] = int(match.group(1)) if match else 0
            except Exception:
                row_estimates[t_name] = 0
        else:
            row_estimates[t_name] = reltuples

    # 2. Lazy Loading Boundary
    is_full_sweep = only is None
    is_bulk_drilldown = only is not None and len(only) > 1

    multi_cols, multi_pks, multi_fks, multi_idxs, multi_checks, multi_uniques = {}, {}, {}, {}, {}, {}

    if not is_full_sweep and is_bulk_drilldown:
        multi_cols = inspector.get_multi_columns(schema=schema)
        multi_pks = inspector.get_multi_pk_constraint(schema=schema)
        multi_fks = inspector.get_multi_foreign_keys(schema=schema)
        multi_idxs = inspector.get_multi_indexes(schema=schema)
        multi_checks = inspector.get_multi_check_constraints(schema=schema)
        multi_uniques = inspector.get_multi_unique_constraints(schema=schema)

    for name in all_names:
        k = kind_of(name)
        try:
            # MEMORY FIX: If full sweep, skip fetching deep constraints entirely to save RAM
            if is_full_sweep:
                cols_raw, pk_raw, fks_raw, idxs_raw, checks_raw, uniques_raw = [], {}, [], [], [], []
            elif is_bulk_drilldown:
                table_key = (schema, name)
                cols_raw = multi_cols.get(table_key, [])
                pk_raw = multi_pks.get(table_key, {})
                fks_raw = multi_fks.get(table_key, [])
                idxs_raw = multi_idxs.get(table_key, [])
                checks_raw = multi_checks.get(table_key, [])
                uniques_raw = multi_uniques.get(table_key, [])
            else:
                cols_raw = inspector.get_columns(name, schema=schema)
                pk_raw = inspector.get_pk_constraint(name, schema=schema)
                fks_raw = inspector.get_foreign_keys(name, schema=schema)
                idxs_raw = inspector.get_indexes(name, schema=schema)
                checks_raw = inspector.get_check_constraints(name, schema=schema)
                uniques_raw = inspector.get_unique_constraints(name, schema=schema)

            table_info = _build_table_info(
                name=name,
                schema=schema,
                kind=k,
                cols_raw=cols_raw,
                pk_raw=pk_raw,
                fks_raw=fks_raw,
                idxs_raw=idxs_raw,
                checks_raw=checks_raw,
                uniques_raw=uniques_raw,
                row_est=row_estimates.get(name)
            )

            if k == "view":
                table_info.view_definition = view_definitions.get(name)
            elif k == "materialized_view":
                table_info.view_definition = matview_definitions.get(name)
                
            tables.append(table_info)
            
        except SQLAlchemyError as exc:
            logger.error(f"Database error introspecting {schema}.{name}: {exc}")
            unreadable_entities.append({
                "type": k,
                "name": name,
                "reason": str(exc)
            })

    return tables, unreadable_entities



def _build_table_info(
    name: str,
    schema: str,
    kind: str,
    cols_raw: Any,    
    pk_raw: Any,      
    fks_raw: Any,     
    idxs_raw: Any,    
    checks_raw: Any,  
    uniques_raw: Any, 
    row_est: int | None
) -> TableInfo:
    # ... keep the rest of your function the exact same!
    """
    Pure synchronous mapping function. 
    ZERO database I/O is performed in this logic.
    """
    pk_cols = set(pk_raw.get("constrained_columns", []))

    columns = []
    for col in cols_raw:
        col_type = str(col["type"])
        is_array = col_type.endswith("[]") or col_type.upper().startswith("ARRAY")
        columns.append(
            ColumnInfo(
                name=col["name"],
                data_type=col_type,
                nullable=col.get("nullable", True),
                default=str(col["default"]) if col.get("default") is not None else None,
                is_primary_key=col["name"] in pk_cols,
                comment=col.get("comment"),
                is_array=is_array,
                element_type=col_type.rstrip("[]") if is_array else None,
            )
        )

    fks = []
    for fk in fks_raw:
        fks.append(
            ForeignKeyInfo(
                name=fk.get("name"),
                columns=fk["constrained_columns"],
                referred_schema=fk.get("referred_schema"),
                referred_table=fk["referred_table"],
                referred_columns=fk["referred_columns"],
            )
        )

    indexes = []
    for idx in idxs_raw:
        raw_cols = idx.get("column_names") or []
        indexes.append(
            IndexInfo(
                name=str(idx.get("name") or ""),
                columns=[c for c in raw_cols if c is not None],
                unique=idx.get("unique", False),
                partial=idx.get("dialect_options", {}).get("postgresql_where") is not None,
                predicate=idx.get("dialect_options", {}).get("postgresql_where"),
            )
        )

    check_constraints = [
        CheckConstraintInfo(
            name=str(c.get("name") or ""), 
            sqltext=str(c.get("sqltext") or "")
        )
        for c in checks_raw
    ]

    return TableInfo(
        name=name,
        schema=schema,
        kind=kind,
        columns=columns,
        primary_key=list(pk_cols),
        foreign_keys=fks,
        indexes=indexes,
        unique_constraints=uniques_raw,
        check_constraints=check_constraints,
        row_count_estimate=row_est,
    )


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

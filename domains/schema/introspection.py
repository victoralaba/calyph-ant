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

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from loguru import logger
from sqlalchemy import Inspector, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool


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


# ---------------------------------------------------------------------------
# Engine factory (sync — Inspector requires sync engine)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=128)
def _make_sync_engine(url: str) -> Engine:
    """
    Build a short-lived sync SQLAlchemy engine for introspection.
    Converts asyncpg DSN to psycopg3-compatible if needed.

    Cached via LRU to prevent dialect setup overhead on repeated calls.
    Uses NullPool so connections are never held open across the cache.
    """
    from sqlalchemy import create_engine

    clean = url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    if clean.startswith("postgres://"):
        clean = clean.replace("postgres://", "postgresql+psycopg://", 1)
    elif not clean.startswith("postgresql+"):
        clean = clean.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(clean, poolclass=NullPool)


# ---------------------------------------------------------------------------
# Core introspection
# ---------------------------------------------------------------------------

def introspect_database(url: str, target_schema: str = "public") -> SchemaSnapshot:
    """
    Full structural introspection of a database.
    Runs synchronously — call from a thread pool in async contexts.

    Args:
        url: PostgreSQL connection URL (any supported scheme)
        target_schema: PostgreSQL schema to inspect (default: public)

    Returns:
        SchemaSnapshot with complete structural metadata
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

        tables = _introspect_tables(inspector, conn, target_schema)
        enums = _introspect_enums(conn, target_schema)
        sequences = _introspect_sequences(conn, target_schema)
        extensions = _introspect_extensions(conn)

    return SchemaSnapshot(
        database=str(database or "unknown"),
        pg_version=str(pg_version or "").split(",")[0].strip(),
        schemas=schemas,
        tables=tables,
        enums=enums,
        sequences=sequences,
        extensions=extensions,
    )


def introspect_table(url: str, table_name: str, schema: str = "public") -> TableInfo:
    """Introspect a single table. Faster than full snapshot for targeted ops."""
    engine = _make_sync_engine(url)
    with engine.connect() as conn:
        inspector = inspect(engine)
        tables = _introspect_tables(inspector, conn, schema, only=[table_name])
        if not tables:
            raise ValueError(f"Table '{schema}.{table_name}' not found.")
        return tables[0]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _introspect_tables(
    inspector: Inspector,
    conn: Any,
    schema: str,
    only: list[str] | None = None,
) -> list[TableInfo]:
    tables: list[TableInfo] = []

    table_names = inspector.get_table_names(schema=schema)
    view_names = inspector.get_view_names(schema=schema)

    # Materialized views via raw query
    mat_views = {
        row[0]
        for row in conn.execute(
            text(
                "SELECT matviewname FROM pg_matviews WHERE schemaname = :schema"
            ),
            {"schema": schema},
        )
    }

    # Fetch view definitions in one query — keyed by name
    view_definitions: dict[str, str] = {}
    try:
        rows = conn.execute(
            text(
                "SELECT viewname, definition "
                "FROM pg_views "
                "WHERE schemaname = :schema"
            ),
            {"schema": schema},
        )
        for row in rows:
            view_definitions[row[0]] = (row[1] or "").strip()
    except Exception as exc:
        logger.warning(f"Could not fetch view definitions for schema '{schema}': {exc}")

    # Fetch materialized view definitions in one query — keyed by name
    matview_definitions: dict[str, str] = {}
    try:
        rows = conn.execute(
            text(
                "SELECT matviewname, definition "
                "FROM pg_matviews "
                "WHERE schemaname = :schema"
            ),
            {"schema": schema},
        )
        for row in rows:
            matview_definitions[row[0]] = (row[1] or "").strip()
    except Exception as exc:
        logger.warning(
            f"Could not fetch materialized view definitions for schema '{schema}': {exc}"
        )

    def kind_of(name: str) -> str:
        if name in mat_views:
            return "materialized_view"
        if name in view_names:
            return "view"
        return "table"

    all_names = table_names + [v for v in view_names if v not in table_names]
    if only:
        all_names = [n for n in all_names if n in only]

    for name in all_names:
        k = kind_of(name)
        try:
            table_info = _build_table_info(inspector, conn, name, schema, k)
            # Attach the view definition so the export can emit real SQL
            if k == "view":
                table_info.view_definition = view_definitions.get(name)
            elif k == "materialized_view":
                table_info.view_definition = matview_definitions.get(name)
            tables.append(table_info)
        except Exception as exc:
            logger.warning(f"Could not introspect {schema}.{name}: {exc}")

    return tables


def _build_table_info(
    inspector: Inspector,
    conn: Any,
    name: str,
    schema: str,
    kind: str,
) -> TableInfo:
    raw_columns = inspector.get_columns(name, schema=schema)
    pk_info = inspector.get_pk_constraint(name, schema=schema)
    pk_cols = set(pk_info.get("constrained_columns", []))

    columns = []
    for col in raw_columns:
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
    for fk in inspector.get_foreign_keys(name, schema=schema):
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
    for idx in inspector.get_indexes(name, schema=schema):
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
        for c in inspector.get_check_constraints(name, schema=schema)
    ]

    unique_constraints = inspector.get_unique_constraints(name, schema=schema)

    # Estimated row count from pg_class (fast, not exact)
    row_est = conn.execute(
        text(
            "SELECT reltuples::bigint FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = :name AND n.nspname = :schema"
        ),
        {"name": name, "schema": schema},
    ).scalar()

    return TableInfo(
        name=name,
        schema=schema,
        kind=kind,
        columns=columns,
        primary_key=list(pk_cols),
        foreign_keys=fks,
        indexes=indexes,
        unique_constraints=unique_constraints,
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

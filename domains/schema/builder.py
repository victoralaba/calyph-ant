# domains/schema/builder.py

"""
Schema builder.

Translates visual schema editor operations into safe, executable SQL.
All operations return generated SQL for preview before execution.
Execution is always explicit — nothing auto-runs without confirmation.

Supported operations:
- Create table with columns, PK (simple and composite), constraints
- Add / alter / drop columns
- Add / drop indexes
- Add / drop foreign keys
- Add / drop check constraints
- Add / drop unique constraints
- Rename table / column
- Drop table (with safeguards)
- Create / drop / alter enum types
- Extension capability check before emitting extension-dependent DDL
- Set / clear comments on tables and columns (COMMENT ON)

Index method registry (INDEX_METHOD_REGISTRY)
---------------------------------------------
Every known index access method is described by an IndexMethodDescriptor
that owns ALL knowledge about that method in one place:
  - extension: which extension is required (None = built-in, no check)
  - description: human-readable summary for the UI
  - with_options: ordered tuple of WITH parameter names this method accepts

This means:
  - assert_extensions_for_index() reads .extension from the descriptor.
  - sql_create_index() iterates .with_options against the caller-supplied
    IndexDefinition fields — no method-specific if/elif branches needed.
  - get_index_method_catalogue() serialises the whole registry for the API.

To add a new extension-backed index method (e.g. lantern, diskann):
  1. Add one IndexMethodDescriptor entry to INDEX_METHOD_REGISTRY below.
  2. If it has WITH parameters, add them to .with_options AND add the
     corresponding field to IndexDefinition.
  That is the complete change. No other code needs to be touched.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import asyncpg
from loguru import logger


# ---------------------------------------------------------------------------
# Column type catalogue
# ---------------------------------------------------------------------------

class PgColumnType(str, Enum):
    # Numeric
    smallint = "SMALLINT"
    integer = "INTEGER"
    bigint = "BIGINT"
    decimal = "DECIMAL"
    numeric = "NUMERIC"
    real = "REAL"
    double_precision = "DOUBLE PRECISION"
    serial = "SERIAL"
    bigserial = "BIGSERIAL"
    smallserial = "SMALLSERIAL"
    # Text
    text = "TEXT"
    varchar = "VARCHAR"
    char = "CHAR"
    citext = "CITEXT"
    # Boolean
    boolean = "BOOLEAN"
    # Date/time
    date = "DATE"
    time = "TIME"
    timestamp = "TIMESTAMP"
    timestamptz = "TIMESTAMPTZ"
    interval = "INTERVAL"
    # UUID
    uuid = "UUID"
    # JSON
    json = "JSON"
    jsonb = "JSONB"
    # Binary
    bytea = "BYTEA"
    # Network
    inet = "INET"
    cidr = "CIDR"
    macaddr = "MACADDR"
    macaddr8 = "MACADDR8"
    # Range types
    int4range = "INT4RANGE"
    int8range = "INT8RANGE"
    numrange = "NUMRANGE"
    tsrange = "TSRANGE"
    tstzrange = "TSTZRANGE"
    daterange = "DATERANGE"
    # Full-text search
    tsvector = "TSVECTOR"
    tsquery = "TSQUERY"
    # Money
    money = "MONEY"
    # Geometric
    point = "POINT"
    line = "LINE"
    lseg = "LSEG"
    box = "BOX"
    path = "PATH"
    polygon = "POLYGON"
    circle = "CIRCLE"
    # Extensions
    vector = "VECTOR"        # pgvector
    geometry = "GEOMETRY"    # PostGIS
    geography = "GEOGRAPHY"  # PostGIS
    hstore = "HSTORE"        # hstore extension
    ltree = "LTREE"          # ltree extension


# Maps column data type → required extension name (pg_available_extensions.name)
EXTENSION_REQUIRED: dict[str, str] = {
    "VECTOR": "vector",
    "GEOMETRY": "postgis",
    "GEOGRAPHY": "postgis",
    "HSTORE": "hstore",
    "LTREE": "ltree",
    "CITEXT": "citext",
}

# Valid vector dimensions range (pgvector supports 1–16000)
VECTOR_DIMENSIONS_MIN = 1
VECTOR_DIMENSIONS_MAX = 16000

# Common vector dimension presets for UI hints
VECTOR_DIMENSION_PRESETS: dict[str, int] = {
    "openai_small":  1536,   # text-embedding-3-small
    "openai_large":  3072,   # text-embedding-3-large
    "openai_ada":    1536,   # text-embedding-ada-002
    "cohere_v3":     1024,   # Cohere embed-v3
    "gemini":        768,    # Gemini embedding
    "nomic":         768,    # nomic-embed-text
    "clip":          512,    # OpenAI CLIP
    "minilm":        384,    # sentence-transformers/all-MiniLM
}


# ---------------------------------------------------------------------------
# Index method registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IndexMethodDescriptor:
    """
    Complete description of a single PostgreSQL index access method.

    All knowledge about a method lives here — extension requirement,
    human-readable description, and the WITH parameter names it accepts.
    No other part of the codebase needs to change when a new method is
    added to INDEX_METHOD_REGISTRY.

    Fields
    ------
    extension : str | None
        pg_available_extensions.name that must be installed before this
        method can be used. None for built-in methods (btree, hash, etc.).

    description : str
        One-line summary shown in the index-method picker UI.

    with_options : tuple[str, ...]
        Ordered sequence of WITH parameter names this method accepts.
        Each name must match a field on IndexDefinition so that
        sql_create_index() can look them up generically without any
        method-specific branching.

        Convention: use the IndexDefinition field name (e.g. "vector_lists",
        "vector_m") rather than the raw Postgres parameter name. The mapping
        from field name to Postgres parameter name lives in
        _WITH_OPTION_SQL_NAME below. This keeps IndexDefinition field names
        collision-free while keeping the SQL correct.
    """
    extension: str | None
    description: str
    with_options: tuple[str, ...] = ()


INDEX_METHOD_REGISTRY: dict[str, IndexMethodDescriptor] = {
    # ---- Built-in methods (extension=None) ---------------------------------
    "btree": IndexMethodDescriptor(
        extension=None,
        description="Default B-tree index. Best for equality and range queries.",
    ),
    "hash": IndexMethodDescriptor(
        extension=None,
        description="Hash index. Equality comparisons only.",
    ),
    "gist": IndexMethodDescriptor(
        extension=None,
        description="Generalised Search Tree. Geometric, range, and full-text types.",
    ),
    "gin": IndexMethodDescriptor(
        extension=None,
        description="Generalised Inverted Index. Best for JSONB, arrays, full-text.",
    ),
    "spgist": IndexMethodDescriptor(
        extension=None,
        description="Space-partitioned GiST. IP ranges, geometric, text.",
    ),
    "brin": IndexMethodDescriptor(
        extension=None,
        description="Block Range Index. Very large, naturally-ordered tables.",
    ),
    # ---- Extension-backed methods ------------------------------------------
    "ivfflat": IndexMethodDescriptor(
        extension="vector",
        description=(
            "Approximate nearest-neighbour via IVFFlat. "
            "Requires pgvector. Faster build, lower recall than HNSW."
        ),
        with_options=("vector_lists",),
    ),
    "hnsw": IndexMethodDescriptor(
        extension="vector",
        description=(
            "Approximate nearest-neighbour via HNSW. "
            "Requires pgvector. Higher recall than IVFFlat, slower build."
        ),
        with_options=("vector_m", "vector_ef_construction"),
    ),
    "rum": IndexMethodDescriptor(
        extension="rum",
        description=(
            "RUM index. Faster full-text search than GIN for phrase "
            "search and ranking queries."
        ),
    ),
    "bloom": IndexMethodDescriptor(
        extension="bloom",
        description=(
            "Bloom filter index. Low-storage equality index across "
            "many columns. Ships with PostgreSQL as an extension."
        ),
    ),
}

# Maps IndexDefinition field name → Postgres WITH parameter name.
_WITH_OPTION_SQL_NAME: dict[str, str] = {
    "vector_lists":           "lists",
    "vector_m":               "m",
    "vector_ef_construction": "ef_construction",
}


# ---------------------------------------------------------------------------
# Smart default resolution
# ---------------------------------------------------------------------------

SMART_DEFAULTS_STATIC: dict[str, str] = {
    "now":               "NOW()",
    "current_timestamp": "CURRENT_TIMESTAMP",
    "current_date":      "CURRENT_DATE",
    "current_time":      "CURRENT_TIME",
    "current_user":      "CURRENT_USER",
    "true":              "TRUE",
    "false":             "FALSE",
    "empty_array":       "'{}'",
    "empty_object":      "'{}'::jsonb",
    "zero":              "0",
    "one":               "1",
}

SMART_DEFAULTS_ASYNC = {"auto_uuid", "auto_uuidv7"}


async def resolve_smart_default(
    conn: asyncpg.Connection,
    symbolic_default: str,
    data_type: str,
) -> str:
    key = symbolic_default.strip().lower()

    if key in SMART_DEFAULTS_STATIC:
        return SMART_DEFAULTS_STATIC[key]

    if key == "auto_uuid":
        pg_version_num = await conn.fetchval("SHOW server_version_num")
        try:
            if pg_version_num is not None and int(pg_version_num) >= 130000:
                return "gen_random_uuid()"
        except (ValueError, TypeError):
            pass

        installed = await check_extension_installed(conn, "uuid-ossp")
        if installed:
            return "uuid_generate_v4()"

        raise ValueError(
            "UUID auto-generation requires PostgreSQL 13+ (built-in gen_random_uuid()) "
            "or the 'uuid-ossp' extension. "
            "Enable uuid-ossp via the Extensions manager or upgrade to PostgreSQL 13+."
        )

    if key == "auto_uuidv7":
        installed = await check_extension_installed(conn, "pg_uuidv7")
        if not installed:
            raise ValueError(
                "UUIDv7 auto-generation requires the 'pg_uuidv7' extension. "
                "Enable it via the Extensions manager."
            )
        return "uuid_generate_v7()"

    return symbolic_default


async def resolve_column_defaults(
    conn: asyncpg.Connection,
    columns: list["ColumnDefinition"],
) -> list["ColumnDefinition"]:
    resolved = []
    for col in columns:
        if col.default is not None:
            raw_key = col.default.strip().lower()
            if raw_key in SMART_DEFAULTS_STATIC or raw_key in SMART_DEFAULTS_ASYNC:
                resolved_default = await resolve_smart_default(
                    conn, col.default, col.data_type
                )
                col = ColumnDefinition(
                    name=col.name,
                    data_type=col.data_type,
                    nullable=col.nullable,
                    default=resolved_default,
                    primary_key=col.primary_key,
                    unique=col.unique,
                    check=col.check,
                    references=col.references,
                    comment=col.comment,
                    vector_dimensions=col.vector_dimensions,
                )
        resolved.append(col)
    return resolved


# ---------------------------------------------------------------------------
# Extension capability checks
# ---------------------------------------------------------------------------

async def check_extension_installed(
    conn: asyncpg.Connection,
    extension_name: str,
) -> bool:
    row = await conn.fetchrow(
        "SELECT installed_version FROM pg_available_extensions WHERE name = $1",
        extension_name,
    )
    return row is not None and row["installed_version"] is not None


async def assert_extensions_for_columns(
    conn: asyncpg.Connection,
    columns: list["ColumnDefinition"],
) -> list[str]:
    checked: set[str] = set()
    extensions_required: list[str] = []

    for col in columns:
        data_type = col.data_type.upper().split("(")[0].strip()

        if data_type == "VECTOR":
            dims = col.vector_dimensions
            if dims is None:
                raise ValueError(
                    f"Column '{col.name}' has type VECTOR but no vector_dimensions "
                    f"specified. Set vector_dimensions to the embedding size "
                    f"(e.g. 1536 for OpenAI text-embedding-3-small, "
                    f"768 for Gemini, 384 for MiniLM). "
                    f"pgvector supports {VECTOR_DIMENSIONS_MIN}–{VECTOR_DIMENSIONS_MAX}."
                )
            if not (VECTOR_DIMENSIONS_MIN <= dims <= VECTOR_DIMENSIONS_MAX):
                raise ValueError(
                    f"Column '{col.name}': vector_dimensions must be between "
                    f"{VECTOR_DIMENSIONS_MIN} and {VECTOR_DIMENSIONS_MAX}, got {dims}. "
                    f"Common values: "
                    f"{', '.join(f'{k}={v}' for k, v in VECTOR_DIMENSION_PRESETS.items())}."
                )

        required_ext = EXTENSION_REQUIRED.get(data_type)
        if required_ext and required_ext not in checked:
            installed = await check_extension_installed(conn, required_ext)
            if not installed:
                raise ValueError(
                    f"Column '{col.name}' uses type '{col.data_type}' which requires "
                    f"the '{required_ext}' extension. "
                    f"Enable it first via the Extensions manager or "
                    f"POST /extensions/{{connection_id}}/enable."
                )
            checked.add(required_ext)
            extensions_required.append(required_ext)

    return extensions_required


async def assert_extensions_for_index(
    conn: asyncpg.Connection,
    index: "IndexDefinition",
) -> list[str]:
    method = index.method.lower()
    descriptor = INDEX_METHOD_REGISTRY.get(method)

    if descriptor is None:
        raise ValueError(
            f"Unknown index method '{index.method}'. "
            f"Supported methods: {', '.join(sorted(INDEX_METHOD_REGISTRY))}."
        )

    if descriptor.extension is None:
        return []

    installed = await check_extension_installed(conn, descriptor.extension)
    if not installed:
        raise ValueError(
            f"Index method '{index.method}' requires the '{descriptor.extension}' "
            f"extension which is not installed on this database. "
            f"Enable it via the Extensions manager or "
            f"POST /extensions/{{connection_id}}/enable, then retry."
        )

    return [descriptor.extension]


async def validate_default_expression(
    conn: asyncpg.Connection,
    expression: str,
    data_type: str,
) -> str | None:
    if not expression or not expression.strip():
        return None

    stripped = expression.strip().upper()

    safe_literals = {
        "NULL", "TRUE", "FALSE", "CURRENT_TIMESTAMP", "NOW()",
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_USER",
        "GEN_RANDOM_UUID()", "UUID_GENERATE_V4()", "UUID_GENERATE_V7()",
    }
    if stripped in safe_literals:
        return None

    try:
        float(expression.strip())
        return None
    except ValueError:
        pass

    s = expression.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return None

    try:
        safe_type = re.sub(r"[^\w\s\(\),]", "", data_type)
        await conn.fetchval(f"SELECT ({expression})::{safe_type}")
        return None
    except asyncpg.PostgresSyntaxError as exc:
        return f"Syntax error in DEFAULT expression: {exc.args[0]}"
    except asyncpg.InvalidTextRepresentationError:
        return f"DEFAULT value is not compatible with type {data_type}"
    except asyncpg.UndefinedFunctionError as exc:
        return f"Unknown function in DEFAULT: {exc.args[0]}"
    except Exception as exc:
        logger.warning(f"Default expression validation non-fatal error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Catalogue helpers (static — no DB connection needed)
# ---------------------------------------------------------------------------

def get_column_type_catalogue() -> list[dict[str, Any]]:
    types = []
    for pg_type in PgColumnType:
        raw = pg_type.value.upper().split("(")[0]
        required_ext = EXTENSION_REQUIRED.get(raw)

        entry: dict[str, Any] = {
            "type": pg_type.value,
            "name": pg_type.name,
            "requires_extension": required_ext,
            "smart_defaults": _type_smart_defaults(pg_type.value),
        }

        if raw == "VECTOR":
            entry["vector_dimension_presets"] = VECTOR_DIMENSION_PRESETS
            entry["vector_dimensions_min"] = VECTOR_DIMENSIONS_MIN
            entry["vector_dimensions_max"] = VECTOR_DIMENSIONS_MAX
            entry["note"] = (
                "Requires pgvector extension and a vector_dimensions value. "
                "Use the dimension preset that matches your embedding model."
            )

        types.append(entry)

    return types


def get_index_method_catalogue() -> list[dict[str, Any]]:
    return [
        {
            "method": method,
            "requires_extension": descriptor.extension,
            "description": descriptor.description,
            "with_options": list(descriptor.with_options),
        }
        for method, descriptor in INDEX_METHOD_REGISTRY.items()
    ]


def _type_smart_defaults(data_type: str) -> list[dict[str, str]]:
    upper = data_type.upper().split("(")[0]

    if upper == "UUID":
        return [
            {"label": "Auto Generate (UUID4)", "value": "auto_uuid"},
            {"label": "Auto Generate (UUID7 — time-ordered)", "value": "auto_uuidv7"},
        ]
    if upper in ("TIMESTAMP", "TIMESTAMPTZ"):
        return [
            {"label": "Current Timestamp", "value": "current_timestamp"},
            {"label": "Now (with timezone)", "value": "now"},
        ]
    if upper == "DATE":
        return [{"label": "Today", "value": "current_date"}]
    if upper == "TIME":
        return [{"label": "Current Time", "value": "current_time"}]
    if upper == "BOOLEAN":
        return [
            {"label": "True", "value": "true"},
            {"label": "False", "value": "false"},
        ]
    if upper == "JSONB":
        return [
            {"label": "Empty Object", "value": "empty_object"},
            {"label": "Empty Array", "value": "empty_array"},
        ]
    if upper in ("INTEGER", "BIGINT", "SMALLINT", "NUMERIC", "DECIMAL"):
        return [
            {"label": "Zero", "value": "zero"},
            {"label": "One", "value": "one"},
        ]
    return []


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------

@dataclass
class ColumnDefinition:
    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None
    primary_key: bool = False
    unique: bool = False
    check: str | None = None
    references: str | None = None
    comment: str | None = None
    vector_dimensions: int | None = None


@dataclass
class IndexDefinition:
    name: str
    columns: list[str]
    unique: bool = False
    method: str = "btree"
    predicate: str | None = None
    vector_lists: int | None = None
    vector_m: int | None = None
    vector_ef_construction: int | None = None


@dataclass
class CreateTableRequest:
    table_name: str
    schema_name: str = "public"
    columns: list[ColumnDefinition] = field(default_factory=list)
    if_not_exists: bool = False
    composite_pk: list[str] = field(default_factory=list)


@dataclass
class AlterColumnRequest:
    table_name: str
    column_name: str
    schema_name: str = "public"
    new_name: str | None = None
    new_type: str | None = None
    set_nullable: bool | None = None
    set_default: str | None = None
    drop_default: bool = False


# ---------------------------------------------------------------------------
# SQL generators — tables
# ---------------------------------------------------------------------------

def sql_create_table(req: CreateTableRequest) -> str:
    _validate_identifier(req.table_name)
    _validate_identifier(req.schema_name)

    use_composite_pk = bool(req.composite_pk)
    if use_composite_pk:
        for col_name in req.composite_pk:
            _validate_identifier(col_name)

    col_parts = []
    for col in req.columns:
        _validate_identifier(col.name)
        effective_col = col
        if use_composite_pk and col.primary_key:
            effective_col = ColumnDefinition(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                default=col.default,
                primary_key=False,
                unique=col.unique,
                check=col.check,
                references=col.references,
                comment=col.comment,
                vector_dimensions=col.vector_dimensions,
            )
        col_parts.append(_column_sql(effective_col))

    if use_composite_pk:
        pk_cols = ", ".join(f'"{c}"' for c in req.composite_pk)
        col_parts.append(f"PRIMARY KEY ({pk_cols})")

    exists_clause = "IF NOT EXISTS " if req.if_not_exists else ""
    cols_joined = ",\n    ".join(col_parts)

    return (
        f'CREATE TABLE {exists_clause}"{req.schema_name}"."{req.table_name}" (\n'
        f"    {cols_joined}\n"
        f");"
    )


def sql_rename_table(
    old_name: str,
    new_name: str,
    schema_name: str = "public",
) -> str:
    _validate_identifier(old_name)
    _validate_identifier(new_name)
    _validate_identifier(schema_name)
    return f'ALTER TABLE "{schema_name}"."{old_name}" RENAME TO "{new_name}";'


def sql_drop_table(
    table: str,
    schema_name: str = "public",
    cascade: bool = False,
) -> str:
    _validate_identifier(table)
    _validate_identifier(schema_name)
    cascade_clause = " CASCADE" if cascade else ""
    return f'DROP TABLE IF EXISTS "{schema_name}"."{table}"{cascade_clause};'


# ---------------------------------------------------------------------------
# SQL generators — columns
# ---------------------------------------------------------------------------

def sql_add_column(
    table: str,
    column: ColumnDefinition,
    schema_name: str = "public",
) -> str:
    _validate_identifier(table)
    _validate_identifier(schema_name)
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f"ADD COLUMN {_column_sql(column)};"
    )


def sql_drop_column(
    table: str,
    column: str,
    schema_name: str = "public",
    cascade: bool = False,
) -> str:
    _validate_identifier(table)
    _validate_identifier(column)
    _validate_identifier(schema_name)
    cascade_clause = " CASCADE" if cascade else " RESTRICT"
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f'DROP COLUMN IF EXISTS "{column}"{cascade_clause};'
    )


def sql_rename_column(
    table: str,
    old_name: str,
    new_name: str,
    schema_name: str = "public",
) -> str:
    _validate_identifier(table)
    _validate_identifier(old_name)
    _validate_identifier(new_name)
    _validate_identifier(schema_name)
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f'RENAME COLUMN "{old_name}" TO "{new_name}";'
    )


def sql_alter_column(req: AlterColumnRequest) -> list[str]:
    statements = []
    t = f'"{req.schema_name}"."{req.table_name}"'
    c = f'"{req.column_name}"'

    if req.new_name:
        _validate_identifier(req.new_name)
        statements.append(
            f'ALTER TABLE {t} RENAME COLUMN {c} TO "{req.new_name}";'
        )
        c = f'"{req.new_name}"'

    if req.new_type:
        statements.append(
            f"ALTER TABLE {t} ALTER COLUMN {c} TYPE {req.new_type} "
            f"USING {c}::{req.new_type};"
        )

    if req.set_nullable is True:
        statements.append(f"ALTER TABLE {t} ALTER COLUMN {c} DROP NOT NULL;")
    elif req.set_nullable is False:
        statements.append(f"ALTER TABLE {t} ALTER COLUMN {c} SET NOT NULL;")

    if req.drop_default:
        statements.append(f"ALTER TABLE {t} ALTER COLUMN {c} DROP DEFAULT;")
    elif req.set_default is not None:
        statements.append(
            f"ALTER TABLE {t} ALTER COLUMN {c} SET DEFAULT {req.set_default};"
        )

    return statements


# ---------------------------------------------------------------------------
# SQL generators — indexes
# ---------------------------------------------------------------------------

def sql_create_index(
    table: str,
    idx: IndexDefinition,
    schema_name: str = "public",
) -> str:
    _validate_identifier(table)
    _validate_identifier(idx.name)
    _validate_identifier(schema_name)

    unique_clause = "UNIQUE " if idx.unique else ""
    cols = ", ".join(f'"{c}"' for c in idx.columns)

    descriptor = INDEX_METHOD_REGISTRY.get(idx.method.lower())
    with_clause = ""
    if descriptor and descriptor.with_options:
        opts = []
        for field_name in descriptor.with_options:
            value = getattr(idx, field_name, None)
            if value is not None:
                sql_param = _WITH_OPTION_SQL_NAME.get(field_name, field_name)
                opts.append(f"{sql_param} = {value}")
        if opts:
            with_clause = f" WITH ({', '.join(opts)})"

    where_clause = f" WHERE {idx.predicate}" if idx.predicate else ""

    return (
        f'CREATE {unique_clause}INDEX "{idx.name}" '
        f'ON "{schema_name}"."{table}" '
        f"USING {idx.method} ({cols})"
        f"{with_clause}"
        f"{where_clause};"
    )


def sql_drop_index(name: str, schema_name: str = "public") -> str:
    _validate_identifier(name)
    _validate_identifier(schema_name)
    return f'DROP INDEX IF EXISTS "{schema_name}"."{name}";'


# ---------------------------------------------------------------------------
# SQL generators — foreign keys
# ---------------------------------------------------------------------------

def sql_add_foreign_key(
    table: str,
    name: str,
    columns: list[str],
    ref_table: str,
    ref_columns: list[str],
    on_delete: str = "NO ACTION",
    on_update: str = "NO ACTION",
    schema_name: str = "public",
    ref_schema: str | None = None,
) -> str:
    _validate_identifier(table)
    _validate_identifier(name)
    _validate_identifier(ref_table)
    _validate_identifier(schema_name)
    _allowed_fk_action(on_delete)
    _allowed_fk_action(on_update)

    cols = ", ".join(f'"{c}"' for c in columns)
    refs = ", ".join(f'"{c}"' for c in ref_columns)
    ref_schema_str = ref_schema or schema_name
    _validate_identifier(ref_schema_str)
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f'ADD CONSTRAINT "{name}" '
        f'FOREIGN KEY ({cols}) '
        f'REFERENCES "{ref_schema_str}"."{ref_table}" ({refs}) '
        f"ON DELETE {on_delete} "
        f"ON UPDATE {on_update};"
    )


# ---------------------------------------------------------------------------
# SQL generators — check constraints
# ---------------------------------------------------------------------------

def sql_add_check_constraint(
    table: str,
    name: str,
    expression: str,
    schema_name: str = "public",
) -> str:
    _validate_identifier(table)
    _validate_identifier(name)
    _validate_identifier(schema_name)
    if not expression.strip():
        raise ValueError("Check constraint expression cannot be empty.")
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f'ADD CONSTRAINT "{name}" CHECK ({expression});'
    )


# ---------------------------------------------------------------------------
# SQL generators — unique constraints
# ---------------------------------------------------------------------------

def sql_add_unique_constraint(
    table: str,
    name: str,
    columns: list[str],
    schema_name: str = "public",
) -> str:
    if not columns:
        raise ValueError("Unique constraint must reference at least one column.")
    _validate_identifier(table)
    _validate_identifier(name)
    _validate_identifier(schema_name)
    cols = ", ".join(f'"{c}"' for c in columns)
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f'ADD CONSTRAINT "{name}" UNIQUE ({cols});'
    )


# ---------------------------------------------------------------------------
# SQL generators — drop constraint
# ---------------------------------------------------------------------------

def sql_drop_constraint(
    table: str,
    constraint_name: str,
    schema_name: str = "public",
    cascade: bool = False,
) -> str:
    _validate_identifier(table)
    _validate_identifier(constraint_name)
    _validate_identifier(schema_name)
    cascade_clause = " CASCADE" if cascade else ""
    return (
        f'ALTER TABLE "{schema_name}"."{table}" '
        f'DROP CONSTRAINT IF EXISTS "{constraint_name}"{cascade_clause};'
    )


# ---------------------------------------------------------------------------
# SQL generators — enum types
# ---------------------------------------------------------------------------

def sql_create_enum(
    name: str,
    values: list[str],
    schema_name: str = "public",
) -> str:
    _validate_identifier(name)
    _validate_identifier(schema_name)
    if not values:
        raise ValueError("Enum must have at least one value.")
    quoted_vals = ", ".join(
        f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in values
    )
    return f'CREATE TYPE "{schema_name}"."{name}" AS ENUM ({quoted_vals});'


def sql_add_enum_value(
    enum_name: str,
    new_value: str,
    after: str | None = None,
    before: str | None = None,
    schema_name: str = "public",
) -> str:
    _validate_identifier(enum_name)
    _validate_identifier(schema_name)
    if after and before:
        raise ValueError("Specify either 'after' or 'before', not both.")
    escaped = new_value.replace("'", "''")
    position_clause = ""
    if after:
        after_escaped = after.replace("'", "''")
        position_clause = f" AFTER '{after_escaped}'"
    elif before:
        before_escaped = before.replace("'", "''")
        position_clause = f" BEFORE '{before_escaped}'"
    return (
        f'ALTER TYPE "{schema_name}"."{enum_name}" '
        f"ADD VALUE IF NOT EXISTS '{escaped}'{position_clause};"
    )


def sql_rename_enum_value(
    enum_name: str,
    old_value: str,
    new_value: str,
    schema_name: str = "public",
) -> str:
    """Requires Postgres 10+."""
    _validate_identifier(enum_name)
    _validate_identifier(schema_name)
    old_escaped = old_value.replace("'", "''")
    new_escaped = new_value.replace("'", "''")
    return (
        f'ALTER TYPE "{schema_name}"."{enum_name}" '
        f"RENAME VALUE '{old_escaped}' TO '{new_escaped}';"
    )


def sql_drop_enum(
    name: str,
    schema_name: str = "public",
    cascade: bool = False,
) -> str:
    _validate_identifier(name)
    _validate_identifier(schema_name)
    cascade_clause = " CASCADE" if cascade else ""
    return f'DROP TYPE IF EXISTS "{schema_name}"."{name}"{cascade_clause};'


# ---------------------------------------------------------------------------
# SQL generators — comments (COMMENT ON)
# ---------------------------------------------------------------------------

def sql_comment_on_table(
    table: str,
    comment: str | None,
    schema_name: str = "public",
) -> str:
    """
    Generate a COMMENT ON TABLE statement.

    Pass comment=None to clear an existing comment (emits NULL).
    Pass comment='' to also clear — treated identically to None.

    Example:
        sql_comment_on_table("users", "Core user accounts table")
        → COMMENT ON TABLE "public"."users" IS 'Core user accounts table';

        sql_comment_on_table("users", None)
        → COMMENT ON TABLE "public"."users" IS NULL;
    """
    _validate_identifier(table)
    _validate_identifier(schema_name)

    if comment is None or comment == "":
        value_clause = "NULL"
    else:
        escaped = comment.replace("'", "''")
        value_clause = f"'{escaped}'"

    return f'COMMENT ON TABLE "{schema_name}"."{table}" IS {value_clause};'


def sql_comment_on_column(
    table: str,
    column: str,
    comment: str | None,
    schema_name: str = "public",
) -> str:
    """
    Generate a COMMENT ON COLUMN statement.

    Pass comment=None or comment='' to clear an existing comment.

    Example:
        sql_comment_on_column("users", "email", "User's primary email address")
        → COMMENT ON COLUMN "public"."users"."email" IS 'User''s primary email address';

        sql_comment_on_column("users", "email", None)
        → COMMENT ON COLUMN "public"."users"."email" IS NULL;
    """
    _validate_identifier(table)
    _validate_identifier(column)
    _validate_identifier(schema_name)

    if comment is None or comment == "":
        value_clause = "NULL"
    else:
        escaped = comment.replace("'", "''")
        value_clause = f"'{escaped}'"

    return (
        f'COMMENT ON COLUMN "{schema_name}"."{table}"."{column}" IS {value_clause};'
    )


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

async def execute_sql(
    conn: asyncpg.Connection,
    statements: list[str],
    dry_run: bool = False,
) -> dict[str, Any]:
    if dry_run:
        try:
            async with conn.transaction():
                for stmt in statements:
                    await conn.execute(stmt)
                raise _DryRunRollback()
        except _DryRunRollback:
            return {
                "dry_run": True,
                "statements": statements,
                "error": None,
                "message": "Dry run succeeded — no changes were committed.",
            }
        except Exception as exc:
            return {
                "dry_run": True,
                "statements": statements,
                "error": str(exc),
                "message": "Dry run failed — the SQL contains errors.",
            }

    executed = []
    try:
        async with conn.transaction():
            for stmt in statements:
                await conn.execute(stmt)
                executed.append(stmt)
        return {"dry_run": False, "statements": executed, "error": None}
    except Exception as exc:
        logger.error(f"Schema execution failed: {exc}\nStatements: {statements}")
        return {"dry_run": False, "statements": executed, "error": str(exc)}


class _DryRunRollback(Exception):
    pass


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

_SAFE_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_$]*$')

_ALLOWED_FK_ACTIONS = {
    "NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT",
}


def _validate_identifier(name: str) -> None:
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe identifier '{name}'. "
            "Identifiers must start with a letter or underscore and contain "
            "only letters, digits, underscores, or dollar signs."
        )


def _allowed_fk_action(action: str) -> None:
    if action.upper() not in _ALLOWED_FK_ACTIONS:
        raise ValueError(
            f"Invalid FK action '{action}'. "
            f"Must be one of: {', '.join(sorted(_ALLOWED_FK_ACTIONS))}"
        )


def _column_sql(col: ColumnDefinition) -> str:
    """Build the SQL fragment for a single column definition."""
    data_type = col.data_type.upper()

    if data_type == "VECTOR" and col.vector_dimensions:
        data_type = f"VECTOR({col.vector_dimensions})"

    parts = [f'"{col.name}" {data_type}']

    if col.primary_key:
        parts.append("PRIMARY KEY")
    if not col.nullable and not col.primary_key:
        parts.append("NOT NULL")
    if col.unique and not col.primary_key:
        parts.append("UNIQUE")
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    if col.check:
        parts.append(f"CHECK ({col.check})")
    if col.references:
        parts.append(f"REFERENCES {col.references}")

    return " ".join(parts)

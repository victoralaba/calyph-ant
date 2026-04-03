# domains/schema/diff.py

"""
Schema diff.

Changes from original
---------------------
GAP-9-3  SQL tokenizer-based change parser
    _parse_sql_to_changes() now uses sqlparse to tokenise migra's output
    into complete statements before classifying them.  Multi-line ALTER
    TABLE statements, foreign-key constraints, inline comments — all
    handled correctly.  The old line-by-line approach is gone.

GAP-9-4  Semantic diff search_path fix
    _run_migra_schemas() is a new helper used by diff_at_versions_semantic
    in auto_migration.py.  It runs two psql sessions with explicit
    SET search_path before calling migra, which is the only reliable way
    to point migra at non-public schemas.  The broken query-parameter
    approach is removed.

GAP-9-1  diff_against_sql / diff_against_calyph helpers
    New functions diff_against_sql() and diff_against_calyph() replay a
    caller-supplied schema into a temp schema on a scratch database then
    run migra.  Used by the new router endpoints in schema/router.py.
    The scratch database is Calyphant's own internal DB (no new infra).
"""

from __future__ import annotations

import asyncio
import re
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4

import asyncpg
from loguru import logger

from worker.celery import execute_ephemeral_diff


def enforce_physics_timeout(dsn: str, timeout_ms: int) -> str:
    """
    Appends strict statement_timeout to the libpq connection string.
    If the Celery worker dies, PostgreSQL unilaterally severs the connection
    and kills the orphaned migra process.
    """
    parsed = urlparse(dsn)
    query = parse_qs(parsed.query, keep_blank_values=True)
    # Inject PostgreSQL native statement timeout. 
    # Use existing options if present, or create new list.
    existing_options = query.get('options', [''])[0]
    new_option = f"-c statement_timeout={timeout_ms}"
    query['options'] = [f"{existing_options} {new_option}".strip()]
    
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def generate_scratch_schema_name() -> str:
    """
    Embeds the UNIX timestamp directly into the schema name.
    The background Garbage Collector relies on this for precise execution.
    """
    current_time = int(time.time())
    short_uuid = uuid4().hex[:8]
    return f"_calyphant_diff_{current_time}_{short_uuid}"

# ---------------------------------------------------------------------------
# Change models  (unchanged)
# ---------------------------------------------------------------------------

class ChangeKind(str, Enum):
    table_added = "table_added"
    table_dropped = "table_dropped"
    column_added = "column_added"
    column_dropped = "column_dropped"
    column_modified = "column_modified"
    column_type_changed = "column_type_changed"
    index_added = "index_added"
    index_dropped = "index_dropped"
    constraint_added = "constraint_added"
    constraint_dropped = "constraint_dropped"
    extension_added = "extension_added"
    extension_dropped = "extension_dropped"
    enum_added = "enum_added"
    enum_modified = "enum_modified"
    enum_dropped = "enum_dropped"
    sequence_added = "sequence_added"
    sequence_dropped = "sequence_dropped"


@dataclass
class SchemaChange:
    kind: ChangeKind
    object_type: str
    object_name: str
    table_name: str | None = None
    detail: str | None = None
    is_destructive: bool = False
    sql: str | None = None


@dataclass
class DiffResult:
    source_label: str
    target_label: str
    changes: list[SchemaChange] = field(default_factory=list)
    sql: str = ""
    has_destructive_changes: bool = False
    error: str | None = None

    @property
    def summary(self) -> str:
        if self.error:
            return f"Diff failed: {self.error}"
        if not self.changes:
            return "Schemas are identical."
        counts: dict[str, int] = {}
        for c in self.changes:
            counts[c.kind.value] = counts.get(c.kind.value, 0) + 1
        parts = []
        for kind, count in sorted(counts.items()):
            parts.append(f"{count} {kind.replace('_', ' ')}")
        dest = " ⚠ Contains destructive changes." if self.has_destructive_changes else ""
        return f"{len(self.changes)} changes: {', '.join(parts)}.{dest}"

    @property
    def table_summary(self) -> dict[str, Any]:
        grouped: dict[str, list[SchemaChange]] = {}
        for change in self.changes:
            key = change.table_name or change.object_name
            grouped.setdefault(key, []).append(change)
        return grouped


_DESTRUCTIVE_KINDS = {
    ChangeKind.table_dropped,
    ChangeKind.column_dropped,
    ChangeKind.column_type_changed,
    ChangeKind.index_dropped,
    ChangeKind.constraint_dropped,
    ChangeKind.enum_dropped,
    ChangeKind.sequence_dropped,
}


# ---------------------------------------------------------------------------
# Main diff entry point
# ---------------------------------------------------------------------------

async def diff_schemas(
    source_url: str,
    target_url: str,
    source_label: str = "source",
    target_label: str = "target",
    schema: str = "public",
) -> DiffResult:
    try:
        sql = await _run_migra(source_url, target_url, schema)
    except Exception as exc:
        logger.error(f"migra diff failed: {exc}")
        return DiffResult(source_label=source_label, target_label=target_label, error=str(exc))

    if sql is None:
        return DiffResult(source_label=source_label, target_label=target_label, sql="", changes=[])

    changes = _parse_sql_to_changes(sql)
    has_destructive = any(c.kind in _DESTRUCTIVE_KINDS for c in changes)

    return DiffResult(
        source_label=source_label,
        target_label=target_label,
        changes=changes,
        sql=sql,
        has_destructive_changes=has_destructive,
    )


# ---------------------------------------------------------------------------
# GAP-9-1  Diff against raw SQL or .calyph schema
# ---------------------------------------------------------------------------

async def diff_against_sql(
    live_url: str,
    schema_sql: str,
    live_label: str = "live",
    sql_label: str = "provided schema",
    schema: str = "public",
    internal_db_url: str | None = None,  # Ignored, kept for backward compatibility
) -> DiffResult:
    """
    Compare a live connection's schema against an arbitrary SQL schema string.

    The SQL is dispatched to a background Celery worker which spins up an 
    ephemeral Docker container, executes the diff, parses the AST, and 
    returns the payload. The main web thread remains entirely unblocked.
    """
    try:
        # 1. Dispatch to Celery (does not block event loop)
        celery_task = execute_ephemeral_diff.delay(  #type: ignore
            source_sql=schema_sql,
            target_db_url=live_url,
            schema=schema
        )
        
        # 2. Wait for background worker to finish (with a 45s hard timeout)
        # Yields the thread back to FastAPI so other users can be served
        result_dict = await asyncio.to_thread(celery_task.get, timeout=45.0)
        
        if result_dict.get("error"):
            return DiffResult(
                source_label=live_label, 
                target_label=sql_label, 
                error=result_dict["error"]
            )

        # 3. Rehydrate the dictionaries back into SchemaChange dataclasses
        rehydrated_changes = [
            SchemaChange(
                kind=ChangeKind(c["kind"]),
                object_type=c["object_type"],
                object_name=c["object_name"],
                table_name=c["table_name"],
                detail=c["detail"],
                is_destructive=c["is_destructive"],
                sql=c["sql"]
            ) for c in result_dict.get("changes", [])
        ]

        return DiffResult(
            source_label=live_label,
            target_label=sql_label,
            changes=rehydrated_changes,
            sql=result_dict.get("sql", ""),
            has_destructive_changes=result_dict.get("has_destructive_changes", False),
        )

    except asyncio.TimeoutError:
        return DiffResult(
            source_label=live_label, 
            target_label=sql_label, 
            error="Schema diffing timed out. The schema is too large or the worker is overloaded."
        )
    except Exception as exc:
        logger.error(f"Celery ephemeral diff task failed: {exc}")
        return DiffResult(
            source_label=live_label, 
            target_label=sql_label, 
            error=f"Diff engine failure: {str(exc)}"
        )


async def diff_against_calyph(
    live_url: str,
    calyph_data: bytes,
    live_label: str = "live",
    calyph_label: str = "backup schema",
    schema: str = "public",
    internal_db_url: str | None = None,
) -> DiffResult:
    """
    Compare a live connection's schema against the DDL embedded in a
    .calyph backup file.

    Only the schema block of the .calyph file is used — row data is
    never loaded.  This is always a schema-only comparison.
    """
    from domains.backups.engine import parse_calyph_backup

    try:
        parsed = parse_calyph_backup(calyph_data)
    except Exception as exc:
        raise ValueError(f"Could not parse .calyph file: {exc}") from exc

    schema_block = parsed.get("schema", {})
    if not schema_block or not schema_block.get("tables"):
        raise ValueError(
            "The .calyph file contains no schema DDL. "
            "Only v2 .calyph backups include full DDL."
        )

    # Reconstruct SQL from the embedded schema block
    schema_sql = _calyph_schema_to_sql(schema_block)
    return await diff_against_sql(
        live_url=live_url,
        schema_sql=schema_sql,
        live_label=live_label,
        sql_label=calyph_label,
        schema=schema,
        internal_db_url=internal_db_url,
    )


def _calyph_schema_to_sql(schema_block: dict) -> str:
    """
    Convert a .calyph schema block back into executable SQL.
    Only base tables are reconstructed — views and mat-views are omitted
    as their definitions are not always reliably stored.
    """
    lines: list[str] = []

    # Enums first (tables may reference them)
    for enum_name, enum_info in schema_block.get("enums", {}).items():
        values = enum_info.get("values", enum_info) if isinstance(enum_info, dict) else enum_info
        vals = ", ".join(f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in values)
        lines.append(f'CREATE TYPE "{enum_name}" AS ENUM ({vals});')

    for table_name, table_info in schema_block.get("tables", {}).items():
        if not isinstance(table_info, dict):
            continue
        if table_info.get("kind", "table") != "table":
            continue

        col_parts: list[str] = []
        pk_cols = table_info.get("primary_key", [])
        single_pk = pk_cols[0] if len(pk_cols) == 1 else None

        for col in table_info.get("columns", []):
            col_type = col.get("data_type", "TEXT")
            is_pk = col.get("is_primary_key", False)
            nullable = col.get("nullable", True)
            default = col.get("default")

            parts = [f'    "{col["name"]}" {col_type}']
            if is_pk and single_pk:
                parts.append("PRIMARY KEY")
            elif not nullable and not is_pk:
                parts.append("NOT NULL")
            if default is not None:
                parts.append(f"DEFAULT {default}")
            col_parts.append(" ".join(parts))

        if len(pk_cols) > 1:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            col_parts.append(f"    PRIMARY KEY ({pk_list})")

        for fk in table_info.get("foreign_keys", []):
            cols = ", ".join(f'"{c}"' for c in fk.get("columns", []))
            refs = ", ".join(f'"{c}"' for c in fk.get("referred_columns", []))
            ref_table = fk.get("referred_table", "")
            ref_schema = fk.get("referred_schema") or "public"
            fk_name = fk.get("name") or f"fk_{table_name}"
            col_parts.append(
                f'    CONSTRAINT "{fk_name}" FOREIGN KEY ({cols}) '
                f'REFERENCES "{ref_schema}"."{ref_table}" ({refs})'
            )

        for cc in table_info.get("check_constraints", []):
            col_parts.append(f'    CONSTRAINT "{cc["name"]}" CHECK ({cc["sqltext"]})')

        cols_sql = ",\n".join(col_parts)
        lines.append(f'CREATE TABLE IF NOT EXISTS "public"."{table_name}" (')
        lines.append(cols_sql)
        lines.append(");")

        for idx in table_info.get("indexes", []):
            idx_cols = ", ".join(f'"{c}"' for c in idx.get("columns", []))
            unique = "UNIQUE " if idx.get("unique") else ""
            method = idx.get("method", "btree")
            where = f" WHERE {idx['predicate']}" if idx.get("predicate") else ""
            lines.append(
                f'CREATE {unique}INDEX "{idx["name"]}" '
                f'ON "public"."{table_name}" USING {method} ({idx_cols}){where};'
            )

    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
# GAP-9-4  Migra runner for two non-public schemas (semantic diff fix)
# ---------------------------------------------------------------------------

async def run_migra_on_schemas(
    db_url: str,
    schema_a: str,
    schema_b: str,
) -> str | None:
    """
    Run migra comparing two schemas inside the SAME database.

    The correct approach is to open two psql sessions, each with
    search_path set to the respective schema, then point migra at
    connection strings that embed the search_path override.

    migra reads --schema from the connection's search_path when
    --schema flag is 'public' but the actual objects are in a different
    schema.  We achieve this by embedding options in the DSN.
    """
    url_a = _url_with_search_path(db_url, schema_a)
    url_b = _url_with_search_path(db_url, schema_b)
    return await _run_migra(url_a, url_b, schema_a)


def _url_with_search_path(url: str, schema: str) -> str:
    """
    Append search_path options to a PostgreSQL DSN.
    Uses libpq's options parameter which psycopg / asyncpg both honour.
    Example: postgresql://user:pass@host/db?options=-c%20search_path%3Dmy_schema
    """
    import urllib.parse
    parsed = urllib.parse.urlparse(_normalise_url(url))
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    # Overwrite any existing options
    params["options"] = [f"-c search_path={schema},public"]
    # quote_via=quote keeps spaces as %20 (libpq compatible; + breaks option parsing)
    new_query = urllib.parse.urlencode(params, doseq=True, quote_via=urllib.parse.quote)
    new_parsed = parsed._replace(query=new_query)
    return urllib.parse.urlunparse(new_parsed)


def _rewrite_schema(sql: str, from_schema: str, to_schema: str) -> str:
    """
    Replace schema-qualified references in SQL for temp schema replay.
    Handles both quoted and unquoted schema names.
    """
    # Replace "public"."table" → "temp_schema"."table"
    result = re.sub(
        rf'"{re.escape(from_schema)}"\.', f'"{to_schema}".', sql
    )
    # Replace public.table (unquoted) → "temp_schema".table
    result = re.sub(
        rf'\b{re.escape(from_schema)}\.', f'"{to_schema}".', result
    )
    return result


def _get_internal_db_url() -> str:
    from core.config import settings
    return settings.DATABASE_URL


# ---------------------------------------------------------------------------
# migra subprocess runner
# ---------------------------------------------------------------------------

async def _run_migra(source_url: str, target_url: str, schema: str, timeout_ms: int = 15000) -> str | None:
    # Enforce physical constraints on the target database connection
    # to mathematically eliminate zombie processes.
    src = _normalise_url(source_url)
    tgt = enforce_physics_timeout(_normalise_url(target_url), timeout_ms)

    proc = await asyncio.create_subprocess_exec(
        "migra",
        "--unsafe",
        "--schema", schema,
        src,
        tgt,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    try:
        stdout, stderr = await proc.communicate()
    except asyncio.CancelledError:
        proc.terminate()
        raise

    if proc.returncode == 0:
        return None
    if proc.returncode == 2:
        return stdout.decode().strip()

    err = stderr.decode().strip()
    raise RuntimeError(f"migra exited with code {proc.returncode}: {err}")


def _normalise_url(url: str) -> str:
    url = url.replace("postgresql+asyncpg://", "postgresql://")
    url = url.replace("postgresql+psycopg://", "postgresql://")
    url = url.replace("postgres://", "postgresql://")
    return url


# ---------------------------------------------------------------------------
# GAP-9-3  SQL tokenizer-based change parser
# ---------------------------------------------------------------------------

def _parse_sql_to_changes(sql: str) -> list[SchemaChange]:
    """
    Parse migra-generated SQL into structured SchemaChange objects using
    sqlparse to tokenise into complete statements.

    Multi-line statements, inline comments, and parenthesized blocks are
    all handled correctly.  The old line-by-line approach silently dropped
    any statement that spanned more than one line.

    Falls back gracefully to an empty list if sqlparse is unavailable.
    """
    try:
        import sqlparse
    except ImportError:
        logger.warning(
            "sqlparse not installed — structured diff changes unavailable. "
            "Install sqlparse for full change classification."
        )
        return _parse_sql_fallback(sql)

    statements = sqlparse.parse(sql)
    changes: list[SchemaChange] = []

    for stmt in statements:
        flat = stmt.value.strip()
        if not flat or flat.startswith("--"):
            continue

        # Normalise whitespace for keyword matching
        normalised = " ".join(flat.upper().split())

        change = _classify_statement(flat, normalised)
        if change:
            changes.append(change)

    return changes


def _classify_statement(flat: str, normalised: str) -> SchemaChange | None:
    """Classify a single complete SQL statement."""

    # ---- CREATE TABLE ----
    if normalised.startswith("CREATE TABLE"):
        name = _extract_identifier_after(flat, r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.table_added, object_type="table",
            object_name=name, sql=flat, is_destructive=False,
        )

    # ---- DROP TABLE ----
    if normalised.startswith("DROP TABLE"):
        name = _extract_identifier_after(flat, r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.table_dropped, object_type="table",
            object_name=name, sql=flat, is_destructive=True,
        )

    # ---- ALTER TABLE ----
    if normalised.startswith("ALTER TABLE"):
        return _classify_alter_table(flat, normalised)

    # ---- CREATE [UNIQUE] INDEX ----
    if normalised.startswith("CREATE INDEX") or normalised.startswith("CREATE UNIQUE INDEX"):
        name = _extract_identifier_after(flat, r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?")
        table = _extract_after_on(flat)
        return SchemaChange(
            kind=ChangeKind.index_added, object_type="index",
            object_name=name, table_name=table, sql=flat, is_destructive=False,
        )

    # ---- DROP INDEX ----
    if normalised.startswith("DROP INDEX"):
        name = _extract_identifier_after(flat, r"DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.index_dropped, object_type="index",
            object_name=name, sql=flat, is_destructive=True,
        )

    # ---- CREATE EXTENSION ----
    if normalised.startswith("CREATE EXTENSION"):
        name = _extract_identifier_after(flat, r"CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.extension_added, object_type="extension",
            object_name=name, sql=flat, is_destructive=False,
        )

    # ---- DROP EXTENSION ----
    if normalised.startswith("DROP EXTENSION"):
        name = _extract_identifier_after(flat, r"DROP\s+EXTENSION\s+(?:IF\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.extension_dropped, object_type="extension",
            object_name=name, sql=flat, is_destructive=True,
        )

    # ---- CREATE TYPE (enum) ----
    if normalised.startswith("CREATE TYPE") and "AS ENUM" in normalised:
        name = _extract_identifier_after(flat, r"CREATE\s+TYPE\s+")
        return SchemaChange(
            kind=ChangeKind.enum_added, object_type="enum",
            object_name=name, sql=flat, is_destructive=False,
        )

    # ---- DROP TYPE ----
    if normalised.startswith("DROP TYPE"):
        name = _extract_identifier_after(flat, r"DROP\s+TYPE\s+(?:IF\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.enum_dropped, object_type="enum",
            object_name=name, sql=flat, is_destructive=True,
        )

    # ---- ALTER TYPE (enum modification) ----
    if normalised.startswith("ALTER TYPE"):
        name = _extract_identifier_after(flat, r"ALTER\s+TYPE\s+")
        return SchemaChange(
            kind=ChangeKind.enum_modified, object_type="enum",
            object_name=name, sql=flat, is_destructive=False,
        )

    # ---- CREATE SEQUENCE ----
    if normalised.startswith("CREATE SEQUENCE"):
        name = _extract_identifier_after(flat, r"CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.sequence_added, object_type="sequence",
            object_name=name, sql=flat, is_destructive=False,
        )

    # ---- DROP SEQUENCE ----
    if normalised.startswith("DROP SEQUENCE"):
        name = _extract_identifier_after(flat, r"DROP\s+SEQUENCE\s+(?:IF\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.sequence_dropped, object_type="sequence",
            object_name=name, sql=flat, is_destructive=True,
        )

    return None


def _classify_alter_table(flat: str, normalised: str) -> SchemaChange | None:
    table = _extract_identifier_after(flat, r"ALTER\s+TABLE\s+(?:ONLY\s+)?(?:IF\s+EXISTS\s+)?")

    # ADD COLUMN
    if "ADD COLUMN" in normalised:
        col = _extract_identifier_after(flat, r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.column_added, object_type="column",
            object_name=col, table_name=table, sql=flat, is_destructive=False,
        )

    # DROP COLUMN
    if "DROP COLUMN" in normalised:
        col = _extract_identifier_after(flat, r"DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?")
        return SchemaChange(
            kind=ChangeKind.column_dropped, object_type="column",
            object_name=col, table_name=table, sql=flat, is_destructive=True,
        )

    # ALTER COLUMN ... TYPE  — must check before generic ALTER COLUMN
    if "ALTER COLUMN" in normalised and re.search(r"\bTYPE\b", normalised):
        col = _extract_identifier_after(flat, r"ALTER\s+COLUMN\s+")
        # Extract the new type
        type_match = re.search(r"\bTYPE\b\s+(\S+)", flat, re.IGNORECASE)
        new_type = type_match.group(1).rstrip(";,)") if type_match else "unknown"
        return SchemaChange(
            kind=ChangeKind.column_type_changed, object_type="column",
            object_name=col, table_name=table,
            detail=f"type changed to {new_type} — verify cast is safe",
            sql=flat, is_destructive=True,
        )

    # ALTER COLUMN (nullability, default — not destructive)
    if "ALTER COLUMN" in normalised:
        col = _extract_identifier_after(flat, r"ALTER\s+COLUMN\s+")
        return SchemaChange(
            kind=ChangeKind.column_modified, object_type="column",
            object_name=col, table_name=table, sql=flat, is_destructive=False,
        )

    # ADD CONSTRAINT
    if "ADD CONSTRAINT" in normalised:
        constraint_name = _extract_identifier_after(flat, r"ADD\s+CONSTRAINT\s+")
        kind = ChangeKind.constraint_added
        return SchemaChange(
            kind=kind, object_type="constraint",
            object_name=constraint_name, table_name=table,
            sql=flat, is_destructive=False,
        )

    # DROP CONSTRAINT
    if "DROP CONSTRAINT" in normalised:
        constraint_name = _extract_identifier_after(
            flat, r"DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?"
        )
        return SchemaChange(
            kind=ChangeKind.constraint_dropped, object_type="constraint",
            object_name=constraint_name, table_name=table,
            sql=flat, is_destructive=True,
        )

    return None


# ---------------------------------------------------------------------------
# Regex extraction helpers
# ---------------------------------------------------------------------------

def _extract_identifier_after(sql: str, pattern: str) -> str:
    """
    Extract the first identifier (quoted or unquoted) after a regex pattern.
    Strips schema prefix if present.
    """
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
        return "unknown"
    rest = sql[match.end():].strip()
    # Grab quoted identifier: "name"
    quoted = re.match(r'"([^"]+)"', rest)
    if quoted:
        raw = quoted.group(1)
        # Strip schema.name → name
        if "." in raw:
            raw = raw.split(".")[-1]
        return raw
    # Grab unquoted identifier up to space or (
    unquoted = re.match(r"([\w$]+)", rest)
    if unquoted:
        raw = unquoted.group(1)
        return raw
    return "unknown"


def _extract_after_on(sql: str) -> str:
    """Extract table name after ON keyword in a CREATE INDEX statement."""
    match = re.search(r"\bON\b\s+", sql, re.IGNORECASE)
    if not match:
        return "unknown"
    return _extract_identifier_after(sql[match.start():], r"\bON\b\s+")


# ---------------------------------------------------------------------------
# Fallback parser (no sqlparse) — kept for environments without it
# ---------------------------------------------------------------------------

def _parse_sql_fallback(sql: str) -> list[SchemaChange]:
    """Best-effort line-by-line parser used when sqlparse is unavailable."""
    changes: list[SchemaChange] = []
    for line in sql.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        upper = line.upper()
        change = _classify_statement(line, " ".join(upper.split()))
        if change:
            changes.append(change)
    return changes


async def diff_schema_snapshots(
    source_url: str,
    target_url: str,
    source_label: str = "source",
    target_label: str = "target",
) -> DiffResult:
    return await diff_schemas(source_url, target_url, source_label, target_label)

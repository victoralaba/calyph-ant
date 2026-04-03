# domains/schema/router/intelligence.py
"""
Schema intelligence router.

Handles everything that observes, reasons about, or exports schema
state — without mutating it.  No DDL is executed through this router.

The distinction from editor_router.py:
  editor_router    — "change something in the database"
  intelligence_router — "tell me something about the database's schema"

Static catalogues live in editor_router because they describe the
building blocks the editor uses.

Endpoints:

  Diff (live vs live):
    POST /schema/{connection_id}/diff               — diff two live connections
    POST /schema/{connection_id}/diff-against-sql   — diff live vs SQL text (Gap 9-1)
    POST /schema/{connection_id}/diff-against-calyph — diff live vs .calyph file (Gap 9-1)

  Export:
    GET  /schema/{connection_id}/export                        — export full schema as SQL
    GET  /schema/{connection_id}/tables/{table_name}/export    — export single table structure

  Auto-migration (snapshot-driven):
    POST /schema/{connection_id}/snapshot           — capture pre-edit snapshot
    POST /schema/{connection_id}/auto-migrate       — generate migration from snapshot diff

  History diff:
    POST /schema/{connection_id}/history-diff       — diff two migration history points

All diff responses share the same shape so the frontend can handle
them with a single renderer regardless of the diff source.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url, get_redis
from domains.schema import introspection
from domains.schema.diff import (
    DiffResult,
    diff_schemas,
)
from domains.schema.auto_migration import (
    RedisUnavailableError,
    capture_snapshot,
    auto_generate_migration,
    diff_at_versions,
    diff_at_versions_semantic,
)
from domains.connections.service import get_validated_workspace_url
from shared.types import DiffJobResponse, DiffJobStatus

router = APIRouter(prefix="/schema", tags=["schema-intelligence"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class DiffResponse(BaseModel):
    source_label: str
    target_label: str
    changes: list[dict]
    sql: str
    has_destructive_changes: bool
    summary: str
    diff_source: str = "live"
    error: str | None = None


class DiffRequest(BaseModel):
    target_connection_id: UUID
    source_label: str = "source"
    target_label: str = "target"
    schema_name: str = "public"


class DiffAgainstSqlRequest(BaseModel):
    schema_sql: str = Field(..., description="Raw CREATE TABLE / DDL SQL to compare against")
    live_label: str = "live"
    sql_label: str = "provided schema"
    schema_name: str = "public"


class SnapshotResponse(BaseModel):
    snapshot_id: str
    captured_at: str
    table_count: int
    redis_available: bool


class AutoMigrateRequest(BaseModel):
    snapshot_id: str
    label: str | None = None
    schema_name: str = "public"


class AutoMigrateResponse(BaseModel):
    created: bool
    migration_id: str | None = None
    version: str | None = None
    label: str | None = None
    up_sql: str | None = None
    down_sql: str | None = None
    changes: list[dict] = []
    message: str = ""


class HistoryDiffRequest(BaseModel):
    from_version: str | None = None
    to_version: str | None = None
    from_order_index: int | None = None
    to_order_index: int | None = None
    semantic: bool = False
    scratch_url: str | None = None


class HistoryDiffResponse(BaseModel):
    from_version: str | None
    to_version: str | None
    migration_count: int
    changes: list[dict]
    up_sql: str
    down_sql: str | None
    has_destructive_changes: bool
    summary: str


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def _get_url(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> str:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    return url

def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id


def _serialise_diff(result: DiffResult, diff_source: str = "live") -> dict:
    return {
        "source_label": result.source_label,
        "target_label": result.target_label,
        "changes": [
            {
                "kind": c.kind.value,
                "object_type": c.object_type,
                "object_name": c.object_name,
                "table_name": c.table_name,
                "detail": c.detail,
                "is_destructive": c.is_destructive,
                "sql": c.sql,
            }
            for c in result.changes
        ],
        "sql": result.sql,
        "has_destructive_changes": result.has_destructive_changes,
        "summary": result.summary,
        "diff_source": diff_source,
        "error": result.error,
    }


# ---------------------------------------------------------------------------
# Diff — live vs live
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/diff")
async def diff_live_schemas(
    connection_id: UUID,
    body: DiffRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Compare this connection's schema against another live connection.
    Returns SQL needed to bring the source in line with the target.
    """
    source_url = await _get_url(connection_id, workspace_id, db)
    target_url = await _get_url(body.target_connection_id, workspace_id, db)

    result = await diff_schemas(
        source_url=source_url,
        target_url=target_url,
        source_label=body.source_label,
        target_label=body.target_label,
        schema=body.schema_name,
    )
    return _serialise_diff(result, diff_source="live")


# ---------------------------------------------------------------------------
# Diff — live vs SQL text (Gap 9-1)
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/diff-against-sql", status_code=status.HTTP_202_ACCEPTED, response_model=DiffJobResponse)
async def diff_live_against_sql(
    connection_id: UUID,
    body: DiffAgainstSqlRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Compare the live database schema against a raw SQL schema string.
    Returns a job_id for background processing to prevent thread starvation.
    
    UI EXPECTATION: The Svelte frontend MUST poll `/schema/diff/{job_id}/status` 
    (or equivalent status endpoint) until status is 'COMPLETED' or 'FAILED'. 
    Do not expect an immediate DiffResult payload.
    """
    # 1. SSRF Boundary: Enforce connection ownership natively before touching DSNs
    live_url = await get_validated_workspace_url(db, connection_id, workspace_id)

    # 2. Establish State Machine
    job_id = str(uuid.uuid4())
    state = {
        "job_id": job_id,
        "status": DiffJobStatus.PENDING.value,
        "result": None,
        "error": None
    }
    
    redis = await get_redis()
    await redis.setex(f"diff_job:{job_id}", 3600, json.dumps(state))

    # 3. Dispatch to Background Worker
    from worker.celery import execute_background_ephemeral_diff
    execute_background_ephemeral_diff.apply_async( # type: ignore
        args=[
            job_id, 
            body.schema_sql, 
            live_url, 
            body.schema_name,
            body.live_label,
            body.sql_label
        ],
        queue="default"
    )

    return DiffJobResponse(**state)


# ---------------------------------------------------------------------------
# Diff — live vs .calyph backup (Gap 9-1)
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/diff-against-calyph", status_code=status.HTTP_202_ACCEPTED, response_model=DiffJobResponse)
async def diff_live_against_calyph(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    backup_file: UploadFile = File(..., description=".calyph.gz backup file to compare against"),
    live_label: str = Form(default="live"),
    calyph_label: str = Form(default="backup schema"),
    schema_name: str = Form(default="public"),
):
    """
    Compare the live database schema against the DDL embedded in a .calyph backup file.
    Extracts the schema block synchronously, then dispatches AST parsing to a background worker.
    
    UI EXPECTATION: The Svelte frontend MUST poll `/schema/diff/{job_id}/status` 
    until status is 'COMPLETED' or 'FAILED'.
    """
    if not backup_file.filename or not backup_file.filename.endswith(".gz"):
        raise HTTPException(
            status_code=400,
            detail="File must be a .calyph.gz backup file.",
        )

    calyph_data = await backup_file.read()
    if not calyph_data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # 1. Parse .calyph file to raw SQL on the fast web thread (JSON extraction, no heavy AST parsing)
    from domains.backups.engine import parse_calyph_backup
    from domains.schema.diff import _calyph_schema_to_sql
    
    try:
        parsed = parse_calyph_backup(calyph_data)
        schema_block = parsed.get("schema", {})
        if not schema_block or not schema_block.get("tables"):
            raise ValueError("The .calyph file contains no schema DDL. Only v2 backups include full DDL.")
        
        schema_sql = _calyph_schema_to_sql(schema_block)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse backup file: {exc}")

    # 2. SSRF Boundary: Enforce connection ownership
    live_url = await get_validated_workspace_url(db, connection_id, workspace_id)

    # 3. Setup Async Job
    job_id = str(uuid.uuid4())
    state = {
        "job_id": job_id,
        "status": DiffJobStatus.PENDING.value,
        "result": None,
        "error": None
    }
    
    redis = await get_redis()
    await redis.setex(f"diff_job:{job_id}", 3600, json.dumps(state))

    # 4. Dispatch to Background Worker
    from worker.celery import execute_background_ephemeral_diff
    execute_background_ephemeral_diff.apply_async( # type: ignore
        args=[
            job_id, 
            schema_sql, 
            live_url, 
            schema_name,
            live_label,
            calyph_label
        ],
        queue="default"
    )

    return DiffJobResponse(**state)


# ---------------------------------------------------------------------------
# Export — full schema
# ---------------------------------------------------------------------------

@router.get("/{connection_id}/export")
async def export_schema_sql(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Export the full schema as SQL statements.

    Notes:
    - Postgres does NOT support CREATE TYPE IF NOT EXISTS.
    - Functions/triggers are listed as comment stubs only.
      Use pg_dump --schema-only for complete procedural code export.
    """
    url = await _get_url(connection_id, workspace_id, db)
    snapshot = await asyncio.to_thread(
        introspection.introspect_database, url, schema_name
    )

    lines = [
        f"-- Calyphant schema export",
        f"-- Database: {snapshot.database}",
        f"-- PostgreSQL: {snapshot.pg_version}",
        f"-- Schema: {schema_name}",
        f"-- Note: run inside a transaction and ROLLBACK to preview safely.",
        "",
    ]

    # Extensions
    if snapshot.extensions:
        lines.append("-- Extensions")
        for ext in snapshot.extensions:
            lines.append(
                f"CREATE EXTENSION IF NOT EXISTS \"{ext['name']}\" "
                f"WITH SCHEMA {schema_name};"
            )
        lines.append("")

    # Enums — no IF NOT EXISTS support in Postgres
    if snapshot.enums:
        lines.append("-- Enum types")
        lines.append(
            "-- Note: CREATE TYPE does not support IF NOT EXISTS. "
            "Drop types first or wrap in exception handling for idempotent runs."
        )
        for enum in snapshot.enums:
            vals = ", ".join(
                f"'{v.replace(chr(39), chr(39)+chr(39))}'"
                for v in enum.values
            )
            lines.append(
                f'CREATE TYPE "{enum.schema}"."{enum.name}" AS ENUM ({vals});'
            )
        lines.append("")

    # Standalone sequences
    standalone_seqs = [
        s for s in snapshot.sequences
        if not any(
            any(
                col.default and "nextval" in (col.default or "").lower()
                and s.name in (col.default or "")
                for col in t.columns
            )
            for t in snapshot.tables
            if t.kind == "table"
        )
    ]
    if standalone_seqs:
        lines.append("-- Sequences")
        for seq in standalone_seqs:
            lines.append(
                f'CREATE SEQUENCE IF NOT EXISTS "{seq.schema}"."{seq.name}" '
                f"AS {seq.data_type} "
                f"START {seq.start} INCREMENT {seq.increment};"
            )
        lines.append("")

    # Base tables
    base_tables = [t for t in snapshot.tables if t.kind == "table"]
    if base_tables:
        lines.append("-- Tables")
    for table in base_tables:
        col_parts = []
        pk_cols = table.primary_key or []
        single_pk_col = pk_cols[0] if len(pk_cols) == 1 else None

        for col in table.columns:
            default = f" DEFAULT {col.default}" if col.default else ""
            null = "" if col.nullable else " NOT NULL"
            pk = " PRIMARY KEY" if (col.is_primary_key and single_pk_col) else ""
            col_parts.append(f'    "{col.name}" {col.data_type}{pk}{null}{default}')

        if len(pk_cols) > 1:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            col_parts.append(f"    PRIMARY KEY ({pk_list})")

        for fk in table.foreign_keys:
            cols = ", ".join(f'"{c}"' for c in fk.columns)
            refs = ", ".join(f'"{c}"' for c in fk.referred_columns)
            ref_schema = fk.referred_schema or schema_name
            fk_name = fk.name or f"fk_{table.name}_{'_'.join(fk.columns)}"
            col_parts.append(
                f'    CONSTRAINT "{fk_name}" FOREIGN KEY ({cols}) '
                f'REFERENCES "{ref_schema}"."{fk.referred_table}" ({refs})'
            )

        for cc in table.check_constraints:
            col_parts.append(
                f'    CONSTRAINT "{cc.name}" CHECK ({cc.sqltext})'
            )

        cols_sql = ",\n".join(col_parts)
        lines.append(f'CREATE TABLE IF NOT EXISTS "{table.schema}"."{table.name}" (')
        lines.append(cols_sql)
        lines.append(");")
        lines.append("")

        for uc in table.unique_constraints:
            uc_cols = ", ".join(f'"{c}"' for c in uc.get("column_names", []))
            uc_name = (
                uc.get("name")
                or f"uq_{table.name}_{'_'.join(uc.get('column_names', []))}"
            )
            if uc_cols:
                lines.append(
                    f'ALTER TABLE "{table.schema}"."{table.name}" '
                    f'ADD CONSTRAINT "{uc_name}" UNIQUE ({uc_cols});'
                )

        for idx in table.indexes:
            cols = ", ".join(f'"{c}"' for c in idx.columns)
            unique = "UNIQUE " if idx.unique else ""
            where = f" WHERE {idx.predicate}" if idx.predicate else ""
            lines.append(
                f'CREATE {unique}INDEX IF NOT EXISTS "{idx.name}" '
                f'ON "{table.schema}"."{table.name}" '
                f"USING {idx.method} ({cols}){where};"
            )
        lines.append("")

    # Views
    views = [t for t in snapshot.tables if t.kind == "view"]
    if views:
        lines.append("-- Views")
        for view in views:
            if view.view_definition:
                defn = view.view_definition.rstrip(";").strip()
                lines.append(
                    f'CREATE OR REPLACE VIEW "{view.schema}"."{view.name}" AS\n{defn};'
                )
            else:
                lines.append(
                    f"-- VIEW: \"{view.schema}\".\"{view.name}\" "
                    f"({len(view.columns)} columns) — definition unavailable"
                )
            lines.append("")

    # Materialized views
    mat_views = [t for t in snapshot.tables if t.kind == "materialized_view"]
    if mat_views:
        lines.append("-- Materialized views")
        for mv in mat_views:
            if mv.view_definition:
                defn = mv.view_definition.rstrip(";").strip()
                lines.append(
                    f'CREATE MATERIALIZED VIEW IF NOT EXISTS "{mv.schema}"."{mv.name}" AS\n{defn};'
                )
                for idx in mv.indexes:
                    cols = ", ".join(f'"{c}"' for c in idx.columns)
                    unique = "UNIQUE " if idx.unique else ""
                    where = f" WHERE {idx.predicate}" if idx.predicate else ""
                    lines.append(
                        f'CREATE {unique}INDEX IF NOT EXISTS "{idx.name}" '
                        f'ON "{mv.schema}"."{mv.name}" '
                        f"USING {idx.method} ({cols}){where};"
                    )
            else:
                lines.append(
                    f"-- MATERIALIZED VIEW: \"{mv.schema}\".\"{mv.name}\" "
                    f"({len(mv.columns)} columns) — definition unavailable"
                )
            lines.append("")

    lines.append("-- Functions and triggers")
    lines.append(
        "-- Full function/trigger body export is not supported here. "
        "Use pg_dump --schema-only for a complete export including procedural code."
    )
    lines.append("")

    return {
        "sql": "\n".join(lines),
        "database": snapshot.database,
        "stats": {
            "tables": len(base_tables),
            "views": len(views),
            "materialized_views": len(mat_views),
            "enums": len(snapshot.enums),
            "extensions": len(snapshot.extensions),
        },
    }


# ---------------------------------------------------------------------------
# Export — single table structure
# ---------------------------------------------------------------------------

@router.get("/{connection_id}/tables/{table_name}/export")
async def export_table_structure(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    include_indexes: bool = Query(True),
    include_constraints: bool = Query(True),
    include_drop: bool = Query(False, description="Prepend DROP TABLE IF EXISTS"),
):
    """
    Export the full DDL for a single table.

    Returns:
      - CREATE TABLE statement with all columns, nullability, defaults, PK
      - Foreign key constraints (ALTER TABLE ADD CONSTRAINT)
      - Check constraints
      - Unique constraints
      - Indexes (CREATE INDEX)
      - A structured column manifest for programmatic use

    Query params:
      schema_name        — PostgreSQL schema (default: public)
      include_indexes    — include CREATE INDEX statements (default: true)
      include_constraints — include FK, check, unique constraints (default: true)
      include_drop       — prepend DROP TABLE IF EXISTS (default: false)
    """
    url = await _get_url(connection_id, workspace_id, db)

    try:
        table = await asyncio.to_thread(
            introspection.introspect_table, url, table_name, schema_name
        )
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{schema_name}.{table_name}' not found.",
        )

    lines: list[str] = [
        f"-- Calyphant table export",
        f"-- Table: {schema_name}.{table_name}",
        f"-- Kind: {table.kind}",
        f"-- Rows (estimate): {table.row_count_estimate or 'unknown'}",
        "",
    ]

    # Optional DROP
    if include_drop:
        lines.append(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}";')
        lines.append("")

    # Build CREATE TABLE
    pk_cols = table.primary_key or []
    single_pk = pk_cols[0] if len(pk_cols) == 1 else None
    col_parts: list[str] = []

    for col in table.columns:
        parts = [f'    "{col.name}" {col.data_type}']
        if col.is_primary_key and single_pk:
            parts.append("PRIMARY KEY")
        elif not col.nullable and not col.is_primary_key:
            parts.append("NOT NULL")
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
        col_parts.append(" ".join(parts))

    if len(pk_cols) > 1:
        pk_list = ", ".join(f'"{c}"' for c in pk_cols)
        col_parts.append(f"    PRIMARY KEY ({pk_list})")

    if include_constraints:
        for fk in table.foreign_keys:
            cols = ", ".join(f'"{c}"' for c in fk.columns)
            refs = ", ".join(f'"{c}"' for c in fk.referred_columns)
            ref_schema = fk.referred_schema or schema_name
            fk_name = fk.name or f"fk_{table_name}_{'_'.join(fk.columns)}"
            col_parts.append(
                f'    CONSTRAINT "{fk_name}" FOREIGN KEY ({cols}) '
                f'REFERENCES "{ref_schema}"."{fk.referred_table}" ({refs})'
            )

        for cc in table.check_constraints:
            col_parts.append(
                f'    CONSTRAINT "{cc.name}" CHECK ({cc.sqltext})'
            )

    cols_joined = ",\n".join(col_parts)
    lines.append(f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (')
    lines.append(cols_joined)
    lines.append(");")
    lines.append("")

    # Unique constraints (ALTER TABLE form)
    if include_constraints and table.unique_constraints:
        lines.append("-- Unique constraints")
        for uc in table.unique_constraints:
            uc_cols = ", ".join(f'"{c}"' for c in uc.get("column_names", []))
            uc_name = (
                uc.get("name")
                or f"uq_{table_name}_{'_'.join(uc.get('column_names', []))}"
            )
            if uc_cols:
                lines.append(
                    f'ALTER TABLE "{schema_name}"."{table_name}" '
                    f'ADD CONSTRAINT "{uc_name}" UNIQUE ({uc_cols});'
                )
        lines.append("")

    # Indexes
    if include_indexes and table.indexes:
        lines.append("-- Indexes")
        for idx in table.indexes:
            idx_cols = ", ".join(f'"{c}"' for c in idx.columns)
            unique = "UNIQUE " if idx.unique else ""
            method = idx.method or "btree"
            where = f" WHERE {idx.predicate}" if idx.predicate else ""
            lines.append(
                f'CREATE {unique}INDEX IF NOT EXISTS "{idx.name}" '
                f'ON "{schema_name}"."{table_name}" '
                f"USING {method} ({idx_cols}){where};"
            )
        lines.append("")

    # Structured column manifest for programmatic consumers
    columns_manifest = [
        {
            "name": col.name,
            "data_type": col.data_type,
            "nullable": col.nullable,
            "default": col.default,
            "is_primary_key": col.is_primary_key,
            "is_array": col.is_array,
            "element_type": col.element_type,
            "comment": col.comment,
        }
        for col in table.columns
    ]

    indexes_manifest = [
        {
            "name": idx.name,
            "columns": idx.columns,
            "unique": idx.unique,
            "method": idx.method,
            "partial": idx.partial,
            "predicate": idx.predicate,
        }
        for idx in table.indexes
    ]

    foreign_keys_manifest = [
        {
            "name": fk.name,
            "columns": fk.columns,
            "referred_schema": fk.referred_schema,
            "referred_table": fk.referred_table,
            "referred_columns": fk.referred_columns,
            "on_delete": fk.on_delete,
            "on_update": fk.on_update,
        }
        for fk in table.foreign_keys
    ]

    return {
        "table": table_name,
        "schema": schema_name,
        "kind": table.kind,
        "row_count_estimate": table.row_count_estimate,
        "sql": "\n".join(lines),
        "columns": columns_manifest,
        "primary_key": pk_cols,
        "indexes": indexes_manifest,
        "foreign_keys": foreign_keys_manifest,
        "check_constraints": [
            {"name": cc.name, "sqltext": cc.sqltext}
            for cc in table.check_constraints
        ],
        "unique_constraints": table.unique_constraints,
    }


# ---------------------------------------------------------------------------
# Auto-migration — snapshot capture
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/snapshot", response_model=SnapshotResponse)
async def capture_pre_edit_snapshot(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    """
    Capture a schema snapshot before making visual edits.

    Call BEFORE any create/alter operations via the editor router.
    Hold the snapshot_id, then call /auto-migrate after edits to
    generate a migration automatically.

    Returns redis_available=False (with empty snapshot_id) when Redis
    is unavailable — never returns 503 so editors can always continue.
    """
    url = await _get_url(connection_id, workspace_id, db)
    snapshot_meta = await asyncio.to_thread(
        introspection.introspect_database, url, schema_name
    )
    table_count = len([t for t in snapshot_meta.tables if t.kind == "table"])

    snapshot_id: str = ""
    redis_available = False
    captured_at: str = ""

    try:
        snapshot_id = await capture_snapshot(
            connection_id=connection_id,
            workspace_id=workspace_id,
            schema_name=schema_name,
        )
        redis_available = True
        from datetime import datetime, timezone
        captured_at = datetime.now(timezone.utc).isoformat()
    except RedisUnavailableError:
        pass

    return SnapshotResponse(
        snapshot_id=snapshot_id,
        captured_at=captured_at,
        table_count=table_count,
        redis_available=redis_available,
    )


# ---------------------------------------------------------------------------
# Auto-migration — generate from snapshot diff
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/auto-migrate", response_model=AutoMigrateResponse)
async def auto_migrate_from_snapshot(
    connection_id: UUID,
    body: AutoMigrateRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Diff the current schema against a pre-captured snapshot and
    automatically create a MigrationRecord for the changes.

    Returns created=False if no changes detected.
    Returns 400 when snapshot_id is missing or expired (2-hour TTL).
    Returns 503 when Redis is unavailable at diff time.
    """
    if not body.snapshot_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "snapshot_id is required. If Redis was unavailable when the "
                "snapshot was captured (redis_available=False), use the manual "
                "migration workflow."
            ),
        )

    try:
        record = await auto_generate_migration(
            snapshot_id=body.snapshot_id,
            db=db,
            connection_id=connection_id,
            workspace_id=workspace_id,
            label=body.label,
            schema_name=body.schema_name,
        )
    except RedisUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if record is None:
        return AutoMigrateResponse(
            created=False,
            message="No schema changes detected since snapshot.",
        )

    from domains.schema.diff import _parse_sql_to_changes
    raw_changes = _parse_sql_to_changes(record.up_sql)

    return AutoMigrateResponse(
        created=True,
        migration_id=str(record.id),
        version=record.version,
        label=record.label,
        up_sql=record.up_sql,
        down_sql=record.down_sql,
        changes=[
            {
                "kind": c.kind.value,
                "object_type": c.object_type,
                "object_name": c.object_name,
                "table_name": c.table_name,
                "is_destructive": c.is_destructive,
            }
            for c in raw_changes
        ],
        message=f"Migration {record.version} created with {len(raw_changes)} change(s).",
    )


# ---------------------------------------------------------------------------
# History diff
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/history-diff", response_model=HistoryDiffResponse)
async def migration_history_diff(
    connection_id: UUID,
    body: HistoryDiffRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Diff the schema at two points in this connection's migration history.

    Prefer from_order_index / to_order_index over version strings —
    order_index values are stable integers that never change.

    Set semantic=true and provide a scratch_url for a full migra-powered
    diff that catches index, constraint, and default changes that the
    structural diff may miss.
    """
    if body.semantic:
        if not body.scratch_url:
            raise HTTPException(
                status_code=400,
                detail="scratch_url is required for semantic diff.",
            )
        result = await diff_at_versions_semantic(
            db=db,
            connection_id=connection_id,
            scratch_url=body.scratch_url,
            from_version=body.from_version,
            to_version=body.to_version,
            from_order_index=body.from_order_index,
            to_order_index=body.to_order_index,
        )
        return HistoryDiffResponse(
            from_version=body.from_version,
            to_version=body.to_version,
            migration_count=len(result.changes),
            changes=[
                {
                    "kind": c.kind.value,
                    "object_type": c.object_type,
                    "object_name": c.object_name,
                    "table_name": c.table_name,
                    "is_destructive": c.is_destructive,
                    "sql": c.sql,
                }
                for c in result.changes
            ],
            up_sql=result.sql,
            down_sql=None,
            has_destructive_changes=result.has_destructive_changes,
            summary=result.summary,
        )

    try:
        result = await diff_at_versions(
            db=db,
            connection_id=connection_id,
            from_version=body.from_version,
            to_version=body.to_version,
            from_order_index=body.from_order_index,
            to_order_index=body.to_order_index,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return HistoryDiffResponse(
        from_version=result["from_version"],
        to_version=result["to_version"],
        migration_count=result["migration_count"],
        changes=result["changes"],
        up_sql=result["up_sql"],
        down_sql=result["down_sql"],
        has_destructive_changes=result["has_destructive_changes"],
        summary=result["summary"],
    )
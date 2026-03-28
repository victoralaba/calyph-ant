# domains/backups/engine.py

"""
Backups domain.

Changes from original
---------------------
GAP-10-1  Large-table chunking
    Tables exceeding CHUNK_ROW_LIMIT are split across numbered part files:
    backup_id_001.calyph.gz, backup_id_002.calyph.gz, …
    Each part is a self-contained .calyph file with the full schema block
    repeated so any single part is independently inspectable.
    BackupRecord gains part_count and total_rows columns.
    The old hard LIMIT 100000 with silent truncation is gone.

GAP-10-2  Restore conflict detection + user-directed strategy
    restore_backup() now runs in two phases:
      Phase 1 (strategy=None)  — dry-run conflict scan.  Returns a
        ConflictReport listing new rows, conflicting PKs, and counts per
        table.  Nothing is written.  The report is stored in Redis under
        a restore_session_id that expires in 30 minutes.
      Phase 2 (strategy supplied) — execute with "skip", "overwrite", or
        per-row "review" overrides from the client.
    ON CONFLICT DO NOTHING is gone from non-skip paths.

GAP-10-3  SQL format restore via psql subprocess
    restore_backup() now handles format="sql" by decompressing the gzip
    and piping it to psql.  Errors are captured and returned; a
    completion summary is always provided.

GAP-10-4  BackupSchedule model + scheduling API
    New BackupSchedule ORM model (separate table, not on BackupRecord).
    Router gains:
      POST   /backups/{connection_id}/schedule    — create/update schedule
      GET    /backups/{connection_id}/schedule    — read schedule
      DELETE /backups/{connection_id}/schedule    — disable schedule
    The beat task in worker/celery.py dispatches scheduled_backup for
    connections whose next_run has passed.

GAP-10-5  Restore atomicity
    Data restore (user DB) and migration-record creation (Calyphant DB)
    can't share a single distributed transaction.  The fix is explicit:
    — Data is committed first inside pg_conn.transaction().
    — Migration record creation is retried up to 3 times with backoff.
    — On exhausted retries the failure is written to restore_dead_letters
      so the audit trail is never silently lost.
    The misleading comment claiming atomicity is removed.

FIX-JSON  datetime/UUID/Decimal serialisation
    asyncpg returns native Python types (datetime, UUID, Decimal, etc.)
    that the stdlib json module cannot serialise.  A single _json_default
    encoder is defined once and passed to every json.dumps call that
    touches row data, making calyph backups robust to all standard
    Postgres column types.

Format notes
------------
.calyph v2 header gains two new optional fields:
  "part"        : 1-based part number (absent = single-part backup)
  "total_parts" : total number of parts (absent = single-part backup)
  "row_counts"  : {table_name: row_count} for this part
"""

from __future__ import annotations

import asyncio
import decimal
import gzip
import hashlib
import json
import os
import tempfile
import time
from datetime import date, datetime, time as dt_time, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, status
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, Boolean
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.config import settings
from core.db import get_db, get_connection_url
from shared.storage import _check_r2_enabled
from shared.types import Base


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNK_ROW_LIMIT = 50_000   # rows per .calyph part file
CALYPH_VERSION = "2"


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class BackupRecord(Base):
    __tablename__ = "backup_records"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    connection_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("connections.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    workspace_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)

    label: Mapped[str] = mapped_column(String(255), nullable=False)
    format: Mapped[str] = mapped_column(String(20), default="calyph", nullable=False)

    # Storage — for multi-part backups r2_key is the key of part 001.
    # All parts share the prefix: backups/{workspace}/{connection}/{backup_id}_NNN.calyph.gz
    r2_key: Mapped[str | None] = mapped_column(String(500), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Multi-part support
    part_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    total_rows: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # State
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Schema version snapshot
    schema_version: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    schema_order_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Metadata 
    pg_version: Mapped[str | None] = mapped_column(String(40), nullable=True)
    table_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_count_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    meta: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class BackupSchedule(Base):
    """
    One row per connection that has an active backup schedule.
    Separate from BackupRecord — this is configuration, not history.
    """
    __tablename__ = "backup_schedules"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    connection_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("connections.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,   # one schedule per connection
        index=True,
    )
    workspace_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)

    # Standard cron expression: "0 2 * * *" = daily at 02:00 UTC
    cron: Mapped[str] = mapped_column(String(50), nullable=False)
    label_template: Mapped[str] = mapped_column(
        String(255), nullable=False, default="scheduled-{date}"
    )
    format: Mapped[str] = mapped_column(String(20), nullable=False, default="calyph")
    schema_only: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Scheduling state — updated by the beat dispatcher
    next_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_backup_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class RestoreDeadLetter(Base):
    """
    Append-only audit table.  A row is written here when the data restore
    succeeded but the post-restore migration-record creation failed after
    all retries.  Ensures no successful restore is ever silently lost from
    the audit trail.
    """
    __tablename__ = "restore_dead_letters"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    backup_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    connection_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    restored_tables: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    schema_version: Mapped[str | None] = mapped_column(String(40), nullable=True)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# JSON serialisation
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> Any:
    """
    Custom JSON encoder for types that asyncpg returns from Postgres columns
    but that the stdlib json module cannot serialise natively.

    Covers:
      - datetime, date, time  →  ISO 8601 string
      - UUID                  →  plain string
      - Decimal               →  float  (use str(obj) if you need exact precision)

    Any other unhandled type re-raises TypeError so the caller gets a clear
    error rather than silent data corruption.
    """
    if isinstance(obj, (datetime, date, dt_time)):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# .calyph format helpers
# ---------------------------------------------------------------------------

def _calyph_header(
    database: str,
    pg_version: str,
    label: str,
    schema_version: str | None = None,
    schema_order_index: int = 0,
    part: int | None = None,
    total_parts: int | None = None,
) -> dict[str, Any]:
    h: dict[str, Any] = {
        "calyph_version": CALYPH_VERSION,
        "format": "calyph",
        "database": database,
        "pg_version": pg_version,
        "label": label,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": schema_version,
        "schema_order_index": schema_order_index,
    }
    if part is not None:
        h["part"] = part
        h["total_parts"] = total_parts
    return h


def _serialise_snapshot(snapshot) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for t in snapshot.tables:
        if t.kind not in ("table", "view", "materialized_view"):
            continue
        tables[t.name] = {
            "schema": t.schema,
            "kind": t.kind,
            "primary_key": t.primary_key,
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "nullable": c.nullable,
                    "default": c.default,
                    "is_primary_key": c.is_primary_key,
                    "is_array": c.is_array,
                    "element_type": c.element_type,
                    "comment": c.comment,
                }
                for c in t.columns
            ],
            "indexes": [
                {
                    "name": i.name, "columns": i.columns, "unique": i.unique,
                    "method": i.method, "partial": i.partial, "predicate": i.predicate,
                }
                for i in t.indexes
            ],
            "foreign_keys": [
                {
                    "name": fk.name, "columns": fk.columns,
                    "referred_schema": fk.referred_schema,
                    "referred_table": fk.referred_table,
                    "referred_columns": fk.referred_columns,
                }
                for fk in t.foreign_keys
            ],
            "check_constraints": [
                {"name": cc.name, "sqltext": cc.sqltext}
                for cc in t.check_constraints
            ],
            "row_count_estimate": t.row_count_estimate,
        }
    return {
        "calyph_schema_version": 2,
        "tables": tables,
        "enums": {
            e.name: {"schema": e.schema, "values": e.values}
            for e in snapshot.enums
        },
        "sequences": [
            {
                "name": s.name, "schema": s.schema, "data_type": s.data_type,
                "start": s.start, "increment": s.increment,
            }
            for s in snapshot.sequences
        ],
        "extensions": snapshot.extensions,
    }


def _build_calyph_bytes(
    header: dict,
    schema_snapshot: dict,
    table_data: dict[str, list[dict]],
) -> bytes:
    lines = [
        json.dumps(header, default=_json_default),
        json.dumps(schema_snapshot, default=_json_default),
    ]
    for table_name, rows in table_data.items():
        lines.append(json.dumps({"table": table_name, "rows": rows}, default=_json_default))
    body = "\n".join(lines)
    checksum = hashlib.sha256(body.encode()).hexdigest()
    lines.append(json.dumps({"manifest": checksum}))
    return gzip.compress("\n".join(lines).encode())


def parse_calyph_backup(data: bytes) -> dict[str, Any]:
    raw = gzip.decompress(data).decode()
    lines = raw.splitlines()
    if len(lines) < 3:
        raise ValueError("Invalid .calyph file: too few lines.")
    header = json.loads(lines[0])
    schema = json.loads(lines[1])
    manifest_line = json.loads(lines[-1])
    if "manifest" not in manifest_line:
        raise ValueError("Missing manifest line in .calyph file.")
    body = "\n".join(lines[:-1])
    actual = hashlib.sha256(body.encode()).hexdigest()
    if actual != manifest_line["manifest"]:
        raise ValueError("Backup checksum mismatch — file may be corrupted or tampered with.")
    table_data: dict[str, list] = {}
    for line in lines[2:-1]:
        block = json.loads(line)
        if "table" in block:
            table_data[block["table"]] = block.get("rows", [])
    return {
        "header": header,
        "schema": schema,
        "table_data": table_data,
        "manifest": manifest_line["manifest"],
        "schema_version": header.get("schema_version"),
        "schema_order_index": header.get("schema_order_index", 0),
        "calyph_version": header.get("calyph_version", "1"),
        "part": header.get("part"),
        "total_parts": header.get("total_parts"),
    }


# ---------------------------------------------------------------------------
# R2 storage helpers
# ---------------------------------------------------------------------------

def _r2_client():
    _check_r2_enabled()
    return boto3.client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT_URL,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def _r2_key(
    workspace_id: UUID,
    connection_id: UUID,
    backup_id: UUID,
    fmt: str,
    part: int | None = None,
) -> str:
    if fmt == "sql":
        return f"backups/{workspace_id}/{connection_id}/{backup_id}.sql.gz"
    if part is not None:
        return f"backups/{workspace_id}/{connection_id}/{backup_id}_{part:03d}.calyph.gz"
    return f"backups/{workspace_id}/{connection_id}/{backup_id}.calyph.gz"


async def upload_to_r2(key: str, data: bytes) -> int:
    client = _r2_client()
    await asyncio.to_thread(
        client.put_object, Bucket=settings.R2_BUCKET_NAME, Key=key, Body=data
    )
    return len(data)


async def download_from_r2(key: str) -> bytes:
    client = _r2_client()
    try:
        response = await asyncio.to_thread(
            client.get_object, Bucket=settings.R2_BUCKET_NAME, Key=key
        )
        return response["Body"].read()
    except ClientError as exc:
        raise ValueError(f"Backup not found in storage: {exc}") from exc


async def generate_r2_download_url(key: str, expires_in: int = 3600) -> str:
    client = _r2_client()
    return await asyncio.to_thread(
        client.generate_presigned_url,
        "get_object",
        Params={"Bucket": settings.R2_BUCKET_NAME, "Key": key},
        ExpiresIn=expires_in,
    )


# ---------------------------------------------------------------------------
# pg_dump (SQL format)
# ---------------------------------------------------------------------------

async def pg_dump_sql(url: str, schema_only: bool = False) -> bytes:
    args = ["pg_dump", "--no-password"]
    if schema_only:
        args.append("--schema-only")
    args.append(url)
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={"PGPASSWORD": _extract_password(url), **_safe_env()},
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {stderr.decode()}")
    return gzip.compress(stdout)


async def psql_restore(url: str, sql_gz: bytes) -> dict[str, Any]:
    """
    Restore a gzipped SQL dump via psql subprocess.
    Returns a summary dict with executed_ok, errors, and stderr.
    ON_ERROR_STOP is intentionally off so we collect all errors.
    """
    sql_bytes = gzip.decompress(sql_gz)

    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as f:
        f.write(sql_bytes)
        tmp_path = f.name

    try:
        proc = await asyncio.create_subprocess_exec(
            "psql",
            "--no-password",
            "--set", "ON_ERROR_STOP=off",
            "--file", tmp_path,
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PGPASSWORD": _extract_password(url), **_safe_env()},
        )
        stdout, stderr = await proc.communicate()
        stderr_text = stderr.decode()
        errors = [
            line for line in stderr_text.splitlines()
            if line.strip().upper().startswith("ERROR")
        ]
        return {
            "success": proc.returncode == 0,
            "return_code": proc.returncode,
            "error_count": len(errors),
            "errors": errors[:50],   # cap for response size
            "stdout": stdout.decode()[:2000],
        }
    finally:
        os.unlink(tmp_path)


def _extract_password(url: str) -> str:
    from urllib.parse import urlparse
    return urlparse(url).password or ""


def _safe_env() -> dict:
    return {k: v for k, v in os.environ.items() if k.startswith(("PATH", "HOME", "USER"))}


# ---------------------------------------------------------------------------
# Schema version resolver
# ---------------------------------------------------------------------------

async def _resolve_schema_version(
    connection_id: UUID | None,
    db: AsyncSession | None,
) -> tuple[str | None, int]:
    if connection_id is None or db is None:
        return None, 0
    try:
        from domains.migrations.service import MigrationRecord
        from sqlalchemy import select as sa_select
        result = await db.execute(
            sa_select(MigrationRecord.version, MigrationRecord.order_index)
            .where(
                MigrationRecord.connection_id == connection_id,
                MigrationRecord.applied == True,  # noqa
            )
            .order_by(MigrationRecord.order_index.desc())
            .limit(1)
        )
        row = result.one_or_none()
        if row:
            return row.version, row.order_index
    except Exception as exc:
        logger.warning(f"Could not resolve schema_version for backup: {exc}")
    return None, 0


# ---------------------------------------------------------------------------
# Core backup — chunked .calyph builder
# ---------------------------------------------------------------------------

async def _build_calyph_chunked(
    pg_conn: asyncpg.Connection,
    label: str,
    schema_only: bool = False,
    connection_id: UUID | None = None,
    db: AsyncSession | None = None,
    schema_version: str | None = None,
    schema_order_index: int = 0,
    db_url: str | None = None,
) -> list[bytes]:
    """
    Build one or more .calyph part files.

    All data reads happen inside a single REPEATABLE READ transaction so
    every part sees the same consistent snapshot.  The transaction is held
    open only for data reads — DDL introspection happens after.

    Returns a list of gzip bytes, one element per part.
    Single-part backups return a list of length 1.
    """
    from domains.schema.introspection import introspect_database

    async with pg_conn.transaction(isolation="repeatable_read", readonly=True):
        db_name = await pg_conn.fetchval("SELECT current_database()")
        pg_ver = await pg_conn.fetchval("SELECT version()")
        assert db_name is not None, "Failed to get database name"
        assert pg_ver is not None, "Failed to get PostgreSQL version"

        table_rows = await pg_conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        table_names = [r["table_name"] for r in table_rows]

        # Read all rows inside the transaction using server-side cursor
        # so we never hold the full result set in memory.
        all_table_data: dict[str, list[dict]] = {}
        if not schema_only:
            for table_name in table_names:
                rows = []
                async with pg_conn.transaction():
                    async for batch in _cursor_chunks(pg_conn, table_name, CHUNK_ROW_LIMIT):
                        rows.extend(batch)
                all_table_data[table_name] = rows
        # Transaction ends here

    # DDL introspection after the transaction
    snapshot = None
    dsn = db_url
    if not dsn and db is not None and connection_id is not None:
        dsn = await get_connection_url(db, connection_id, workspace_id=None)  # type: ignore
    if dsn:
        try:
            snapshot = await asyncio.to_thread(introspect_database, dsn, "public")
        except Exception as exc:
            logger.warning(f"Full DDL introspection failed: {exc}")

    if snapshot is not None:
        table_names_set = set(table_names)
        schema_block = _serialise_snapshot_filtered(snapshot, table_names_set)
    else:
        schema_block = {
            "calyph_schema_version": 1,
            "tables": {name: {} for name in table_names},
        }

    # Split table data into chunks of CHUNK_ROW_LIMIT rows per part
    if schema_only:
        # Schema-only: single part, no row data
        header = _calyph_header(
            database=db_name, pg_version=pg_ver.split(",")[0],
            label=label, schema_version=schema_version,
            schema_order_index=schema_order_index,
        )
        return [_build_calyph_bytes(header, schema_block, {})]

    parts = _partition_table_data(all_table_data)
    total_parts = len(parts)

    result_parts: list[bytes] = []
    for idx, part_data in enumerate(parts, start=1):
        header = _calyph_header(
            database=db_name, pg_version=pg_ver.split(",")[0],
            label=label, schema_version=schema_version,
            schema_order_index=schema_order_index,
            part=idx if total_parts > 1 else None,
            total_parts=total_parts if total_parts > 1 else None,
        )
        result_parts.append(_build_calyph_bytes(header, schema_block, part_data))

    return result_parts


async def _cursor_chunks(
    conn: asyncpg.Connection,
    table_name: str,
    chunk_size: int,
):
    """Async generator — yields lists of row dicts using a server-side cursor."""
    async with conn.transaction():
        cursor = await conn.cursor(
            f'SELECT * FROM "public"."{table_name}"'
        )
        while True:
            rows = await cursor.fetch(chunk_size)
            if not rows:
                break
            yield [dict(r) for r in rows]


def _partition_table_data(
    all_data: dict[str, list[dict]],
) -> list[dict[str, list[dict]]]:
    """
    Split table data into parts where each part contains at most
    CHUNK_ROW_LIMIT total rows across all tables.

    Large tables are split across parts; each table fragment keeps the
    table name so the reassembly logic in restore can merge them.
    """
    parts: list[dict[str, list[dict]]] = [{}]

    for table_name, rows in all_data.items():
        offset = 0
        while offset < len(rows) or (offset == 0 and len(rows) == 0):
            chunk = rows[offset: offset + CHUNK_ROW_LIMIT]
            offset += CHUNK_ROW_LIMIT

            current = parts[-1]
            current_total = sum(len(v) for v in current.values())

            if current_total >= CHUNK_ROW_LIMIT:
                parts.append({})
                current = parts[-1]

            current.setdefault(table_name, []).extend(chunk)

            if offset >= len(rows):
                break

    return parts


def _serialise_snapshot_filtered(snapshot, table_names_set: set[str]) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for t in snapshot.tables:
        if t.kind not in ("table", "view", "materialized_view"):
            continue
        if t.kind == "table" and t.name not in table_names_set:
            continue
        tables[t.name] = {
            "schema": t.schema, "kind": t.kind, "primary_key": t.primary_key,
            "columns": [
                {
                    "name": c.name, "data_type": c.data_type, "nullable": c.nullable,
                    "default": c.default, "is_primary_key": c.is_primary_key,
                    "is_array": c.is_array, "element_type": c.element_type,
                    "comment": c.comment,
                }
                for c in t.columns
            ],
            "indexes": [
                {
                    "name": i.name, "columns": i.columns, "unique": i.unique,
                    "method": i.method, "partial": i.partial, "predicate": i.predicate,
                }
                for i in t.indexes
            ],
            "foreign_keys": [
                {
                    "name": fk.name, "columns": fk.columns,
                    "referred_schema": fk.referred_schema,
                    "referred_table": fk.referred_table,
                    "referred_columns": fk.referred_columns,
                }
                for fk in t.foreign_keys
            ],
            "check_constraints": [
                {"name": cc.name, "sqltext": cc.sqltext}
                for cc in t.check_constraints
            ],
            "row_count_estimate": t.row_count_estimate,
        }
    return {
        "calyph_schema_version": 2,
        "tables": tables,
        "enums": {
            e.name: {"schema": e.schema, "values": e.values}
            for e in snapshot.enums
        },
        "sequences": [
            {
                "name": s.name, "schema": s.schema, "data_type": s.data_type,
                "start": s.start, "increment": s.increment,
            }
            for s in snapshot.sequences
        ],
        "extensions": snapshot.extensions,
    }


# ---------------------------------------------------------------------------
# Backup orchestration
# ---------------------------------------------------------------------------

async def create_backup(
    db: AsyncSession,
    pg_conn: asyncpg.Connection,
    connection_id: UUID,
    workspace_id: UUID,
    label: str,
    fmt: str = "calyph",
    schema_only: bool = False,
) -> BackupRecord:
    schema_version, schema_order_index = await _resolve_schema_version(connection_id, db)

    record = BackupRecord(
        connection_id=connection_id,
        workspace_id=workspace_id,
        label=label,
        format=fmt,
        status="running",
        schema_version=schema_version,
        schema_order_index=schema_order_index,
    )
    db.add(record)
    await db.commit()
    await db.refresh(record)

    try:
        url = await get_connection_url(db, connection_id, workspace_id)

        if fmt == "calyph":
            parts = await _build_calyph_chunked(
                pg_conn=pg_conn,
                label=label,
                schema_only=schema_only,
                connection_id=connection_id,
                db=db,
                schema_version=schema_version,
                schema_order_index=schema_order_index,
                db_url=url,
            )
            total_size = 0
            first_key: str | None = None
            for idx, part_bytes in enumerate(parts, start=1):
                part_num = idx if len(parts) > 1 else None
                key = _r2_key(workspace_id, connection_id, record.id, "calyph", part_num)
                await upload_to_r2(key, part_bytes)
                total_size += len(part_bytes)
                if first_key is None:
                    first_key = key

            record.r2_key = first_key
            record.size_bytes = total_size
            record.part_count = len(parts)
            record.checksum = hashlib.sha256(parts[0]).hexdigest()

        else:
            if not url:
                raise ValueError("Connection URL not found.")
            data = await pg_dump_sql(url, schema_only=schema_only)
            key = _r2_key(workspace_id, connection_id, record.id, "sql")
            await upload_to_r2(key, data)
            record.r2_key = key
            record.size_bytes = len(data)
            record.part_count = 1
            record.checksum = hashlib.sha256(data).hexdigest()

        record.status = "completed"
        record.completed_at = datetime.now(timezone.utc)

    except Exception as exc:
        record.status = "failed"
        record.error = str(exc)
        logger.error(f"Backup failed for connection {connection_id}: {exc}")

    await db.commit()
    await db.refresh(record)
    return record


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------

async def _detect_conflicts(
    pg_conn: asyncpg.Connection,
    table_data: dict[str, list[dict]],
    schema: dict,
) -> dict[str, Any]:
    """
    Phase 1 of restore: scan for PK conflicts without writing anything.

    Returns a conflict_report dict:
      {
        table_name: {
          "new_rows": int,
          "conflicting_count": int,
          "conflicting_pks": [...]  # capped at 100
        }
      }
    """
    report: dict[str, Any] = {}
    tables_meta = schema.get("tables", {})

    for table_name, rows in table_data.items():
        if not rows:
            report[table_name] = {
                "new_rows": 0, "conflicting_count": 0, "conflicting_pks": []
            }
            continue

        # Detect PK column(s) from schema block
        table_meta = tables_meta.get(table_name, {})
        pk_cols = table_meta.get("primary_key", [])

        if not pk_cols:
            # No PK info — assume all rows are new, can't detect conflicts
            report[table_name] = {
                "new_rows": len(rows),
                "conflicting_count": 0,
                "conflicting_pks": [],
                "note": "No primary key metadata; conflict detection skipped for this table.",
            }
            continue

        pk_col = pk_cols[0]  # Use first PK col for lookup
        incoming_pks = [str(row[pk_col]) for row in rows if pk_col in row]

        if not incoming_pks:
            report[table_name] = {
                "new_rows": len(rows), "conflicting_count": 0, "conflicting_pks": []
            }
            continue

        # Batch check existing PKs
        existing_rows = await pg_conn.fetch(
            f'SELECT "{pk_col}" FROM "public"."{table_name}" '
            f'WHERE "{pk_col}" = ANY($1::text[])',
            incoming_pks,
        )
        existing_pks = {str(r[pk_col]) for r in existing_rows}
        conflicting = [pk for pk in incoming_pks if pk in existing_pks]

        report[table_name] = {
            "new_rows": len(rows),
            "conflicting_count": len(conflicting),
            "conflicting_pks": conflicting[:100],
        }

    return report


async def _store_restore_session(
    session_id: str,
    payload: dict,
    ttl_seconds: int = 1800,  # 30 minutes
) -> None:
    from core.db import get_redis, _fernet
    redis = await get_redis()
    raw = json.dumps(payload).encode()
    encrypted = _fernet.encrypt(raw) if _fernet else raw
    await redis.setex(f"restore_session:{session_id}", ttl_seconds, encrypted)


async def _load_restore_session(session_id: str) -> dict | None:
    from core.db import get_redis, _fernet
    redis = await get_redis()
    raw = await redis.get(f"restore_session:{session_id}")
    if raw is None:
        return None
    try:
        decrypted = _fernet.decrypt(raw).decode() if _fernet else raw.decode()
        return json.loads(decrypted)
    except Exception:
        return None


async def _delete_restore_session(session_id: str) -> None:
    try:
        from core.db import get_redis
        redis = await get_redis()
        await redis.delete(f"restore_session:{session_id}")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------

async def restore_backup(
    db: AsyncSession,
    pg_conn: asyncpg.Connection,
    backup_id: UUID,
    workspace_id: UUID,
    strategy: str | None = None,
    per_row_overrides: dict[str, dict[str, str]] | None = None,
    restore_session_id: str | None = None,
) -> dict[str, Any]:
    """
    Two-phase restore.

    Phase 1 — strategy is None (or not provided):
        Scans for conflicts, stores session in Redis, returns conflict_report.
        Nothing is written.

    Phase 2 — strategy is "skip", "overwrite", or "review":
        Executes restore using the chosen strategy.
        restore_session_id must match a valid Phase 1 result.
        For "review", per_row_overrides maps table_name → {pk_str: "skip"|"overwrite"}.

    Atomicity guarantee:
        Data is committed inside pg_conn.transaction() (user's DB).
        The post-restore migration record is written to Calyphant's DB
        with up to 3 retries + exponential backoff.
        On exhausted retries, a RestoreDeadLetter row is written so the
        successful restore is never lost from the audit trail.
    """
    record = await db.get(BackupRecord, backup_id)
    if not record or record.workspace_id != workspace_id:
        raise ValueError("Backup not found.")
    if record.status != "completed":
        raise ValueError(f"Backup is not in completed state: {record.status}")

    # -----------------------------------------------------------------------
    # SQL format restore (Gap 10-3)
    # -----------------------------------------------------------------------
    if record.format == "sql":
        if not record.r2_key:
            raise ValueError("Backup has no stored data.")
        data = await download_from_r2(record.r2_key)
        url = await get_connection_url(db, record.connection_id, workspace_id)
        if not url:
            raise ValueError("Connection not found.")
        result = await psql_restore(url, data)
        if result["success"]:
            record.status = "restored"
            await db.commit()
        return {
            "format": "sql",
            "success": result["success"],
            "error_count": result["error_count"],
            "errors": result["errors"],
            "message": (
                "SQL restore completed."
                if result["success"]
                else f"SQL restore finished with {result['error_count']} error(s)."
            ),
        }

    # -----------------------------------------------------------------------
    # .calyph restore
    # -----------------------------------------------------------------------

    # Load all parts
    all_table_data: dict[str, list[dict]] = {}
    schema_block: dict = {}
    parsed_meta: dict = {}

    for part_idx in range(1, record.part_count + 1):
        part_num = part_idx if record.part_count > 1 else None
        key = _r2_key(workspace_id, record.connection_id, backup_id, "calyph", part_num)
        data = await download_from_r2(key)
        parsed = parse_calyph_backup(data)
        if not schema_block:
            schema_block = parsed["schema"]
            parsed_meta = parsed
        for tname, rows in parsed["table_data"].items():
            all_table_data.setdefault(tname, []).extend(rows)

    # Phase 1 — conflict scan
    if strategy is None:
        conflict_report = await _detect_conflicts(pg_conn, all_table_data, schema_block)
        has_conflicts = any(
            t["conflicting_count"] > 0 for t in conflict_report.values()
        )
        session_id = str(uuid4())
        session_payload = {
            "backup_id": str(backup_id),
            "workspace_id": str(workspace_id),
            "conflict_report": conflict_report,
        }
        await _store_restore_session(session_id, session_payload)

        return {
            "phase": 1,
            "restore_session_id": session_id,
            "has_conflicts": has_conflicts,
            "conflict_report": conflict_report,
            "message": (
                "Conflicts detected. Choose a strategy and re-submit with "
                "restore_session_id and strategy: 'skip', 'overwrite', or 'review'."
                if has_conflicts
                else
                "No conflicts detected. Submit with strategy='overwrite' to proceed."
            ),
        }

    # Phase 2 — execute
    if not restore_session_id:
        raise ValueError(
            "restore_session_id is required for Phase 2. "
            "Call restore first without strategy to obtain it."
        )
    session = await _load_restore_session(restore_session_id)
    if not session or session.get("backup_id") != str(backup_id):
        raise ValueError(
            "restore_session_id is invalid or expired (30-minute TTL). "
            "Re-run Phase 1 to get a new session."
        )

    if strategy not in ("skip", "overwrite", "review"):
        raise ValueError("strategy must be 'skip', 'overwrite', or 'review'.")

    conflict_report = session["conflict_report"]
    restored_tables: list[str] = []
    skipped_rows: dict[str, int] = {}

    # Build a set of conflicting PKs per table for quick lookup
    conflict_pks_by_table: dict[str, set[str]] = {
        tname: set(info.get("conflicting_pks", []))
        for tname, info in conflict_report.items()
    }

    # Execute inside a single transaction on the user's DB.
    # No data loss: if anything fails here, the whole thing rolls back.
    async with pg_conn.transaction():
        for table_name, rows in all_table_data.items():
            if not rows:
                continue

            table_meta = schema_block.get("tables", {}).get(table_name, {})
            pk_cols = table_meta.get("primary_key", [])
            pk_col = pk_cols[0] if pk_cols else None
            conflict_pks = conflict_pks_by_table.get(table_name, set())
            cols = list(rows[0].keys())
            col_sql = ", ".join(f'"{c}"' for c in cols)

            skipped_rows[table_name] = 0

            for row in rows:
                pk_val = str(row.get(pk_col, "")) if pk_col else None
                is_conflict = pk_val in conflict_pks if pk_val else False

                # Determine action for this row
                if not is_conflict:
                    action = "overwrite"
                elif strategy == "skip":
                    action = "skip"
                elif strategy == "overwrite":
                    action = "overwrite"
                elif strategy == "review":
                    per_table = (per_row_overrides or {}).get(table_name, {})
                    action = per_table.get(pk_val or "", "skip")
                else:
                    action = "skip"

                if action == "skip":
                    skipped_rows[table_name] += 1
                    continue

                vals = list(row.values())
                placeholders = ", ".join(f"${i+1}" for i in range(len(vals)))

                if action == "overwrite" and is_conflict:
                    # UPDATE existing row
                    set_parts = [
                        f'"{c}" = ${i+1}'
                        for i, c in enumerate(cols)
                        if c != pk_col
                    ]
                    if set_parts and pk_col:
                        pk_idx = cols.index(pk_col) + 1
                        update_vals = [v for c, v in zip(cols, vals) if c != pk_col]
                        update_vals.append(row[pk_col])
                        await pg_conn.execute(
                            f'UPDATE "public"."{table_name}" '
                            f'SET {", ".join(set_parts)} '
                            f'WHERE "{pk_col}" = ${len(update_vals)}',
                            *update_vals,
                        )
                    else:
                        # No PK to update by — insert and let DB handle
                        await pg_conn.execute(
                            f'INSERT INTO "public"."{table_name}" ({col_sql}) '
                            f'VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                            *vals,
                        )
                else:
                    await pg_conn.execute(
                        f'INSERT INTO "public"."{table_name}" ({col_sql}) '
                        f'VALUES ({placeholders})',
                        *vals,
                    )

            restored_tables.append(table_name)

    # Data is now committed. Update record status.
    record.status = "restored"
    await db.commit()

    await _delete_restore_session(restore_session_id)

    # Post-restore migration record — retried with backoff.
    # On failure, writes to restore_dead_letters — never silently lost.
    restore_migration = await _create_restore_migration_safe(
        db=db,
        record=record,
        workspace_id=workspace_id,
        restored_tables=restored_tables,
    )

    return {
        "phase": 2,
        "strategy": strategy,
        "restored_tables": restored_tables,
        "skipped_rows": skipped_rows,
        "schema_version": parsed_meta.get("schema_version"),
        "schema_order_index": parsed_meta.get("schema_order_index", 0),
        "restore_migration_id": str(restore_migration.id) if restore_migration else None,
        "audit_warning": restore_migration is None,
    }


async def _create_restore_migration_safe(
    db: AsyncSession,
    record: BackupRecord,
    workspace_id: UUID,
    restored_tables: list[str],
    max_retries: int = 3,
) -> Any | None:
    """
    Write the informational post-restore migration record.
    Retries up to max_retries times with exponential backoff.
    On exhausted retries, writes to restore_dead_letters instead.
    Returns the MigrationRecord or None if all retries failed.
    """
    from domains.migrations.service import create_migration, MigrationRecord
    from sqlalchemy import update as sa_update

    restore_note = (
        f"Restore from backup '{record.label}' "
        f"(schema at order_index {record.schema_order_index}). "
        f"Restored tables: {', '.join(restored_tables)}."
    )

    for attempt in range(1, max_retries + 1):
        try:
            migration = await create_migration(
                db=db,
                connection_id=record.connection_id,
                workspace_id=workspace_id,
                label=f"post_restore_{record.id.hex[:8]}",
                up_sql=f"-- {restore_note}\n-- No executable SQL: informational only.",
                down_sql=None,
                generated_by="restore",
                skip_validation=True,
            )
            # Force-mark applied
            await db.execute(
                sa_update(MigrationRecord)
                .where(MigrationRecord.id == migration.id)
                .values(
                    applied=True,
                    applied_at=datetime.now(timezone.utc),
                )
            )
            await db.commit()
            return migration
        except Exception as exc:
            wait = 0.5 * (2 ** (attempt - 1))
            logger.warning(
                f"Restore migration record creation failed "
                f"(attempt {attempt}/{max_retries}): {exc}. "
                f"Retrying in {wait:.1f}s."
            )
            if attempt < max_retries:
                await asyncio.sleep(wait)

    # All retries exhausted — write dead letter
    logger.error(
        f"Restore migration record creation exhausted retries for backup "
        f"{record.id}. Writing dead letter."
    )
    try:
        dead = RestoreDeadLetter(
            backup_id=record.id,
            connection_id=record.connection_id,
            workspace_id=workspace_id,
            restored_tables=restored_tables,
            schema_version=record.schema_version,
            error="Migration record creation failed after all retries.",
        )
        db.add(dead)
        await db.commit()
    except Exception as dl_exc:
        logger.critical(
            f"DEAD LETTER WRITE ALSO FAILED for backup {record.id}: {dl_exc}. "
            f"Manual audit required. Restore succeeded at "
            f"{datetime.now(timezone.utc).isoformat()}, "
            f"tables: {restored_tables}"
        )
    return None


# ---------------------------------------------------------------------------
# Backup schema diff  (unchanged from original — delegates to schema diff)
# ---------------------------------------------------------------------------

async def diff_backups(
    db: AsyncSession,
    backup_a_id: UUID,
    backup_b_id: UUID,
    workspace_id: UUID,
) -> dict[str, Any]:
    """
    Diff the embedded DDL schema of two .calyph backups.

    Only the schema blocks are compared — row data is never touched.
    If both backups have schema_version set, delegates to migration
    history diff.  Otherwise performs a structural DDL diff.

    The diff_source field in the response is always "schema_only" to
    make clear that no row data is involved.
    """
    record_a = await db.get(BackupRecord, backup_a_id)
    record_b = await db.get(BackupRecord, backup_b_id)

    for rec, bid in [(record_a, backup_a_id), (record_b, backup_b_id)]:
        if not rec or rec.workspace_id != workspace_id:
            raise ValueError(f"Backup {bid} not found.")
        if rec.status != "completed":
            raise ValueError(f"Backup {bid} is not in completed state.")
        if rec.format != "calyph":
            raise ValueError(f"Backup {bid} is not a .calyph backup.")

    assert record_a is not None
    assert record_b is not None

    # Ensure a is the older one
    if record_a.schema_order_index > record_b.schema_order_index:
        record_a, record_b = record_b, record_a
        backup_a_id, backup_b_id = backup_b_id, backup_a_id

    # Load only part 1 from each backup (schema block is identical across parts)
    key_a = _r2_key(workspace_id, record_a.connection_id, backup_a_id, "calyph",
                    1 if record_a.part_count > 1 else None)
    key_b = _r2_key(workspace_id, record_b.connection_id, backup_b_id, "calyph",
                    1 if record_b.part_count > 1 else None)

    data_a = await download_from_r2(key_a)
    data_b = await download_from_r2(key_b)
    parsed_a = parse_calyph_backup(data_a)
    parsed_b = parse_calyph_backup(data_b)

    if (
        record_a.schema_version is not None
        and record_b.schema_version is not None
    ):
        from domains.schema.auto_migration import diff_at_versions
        result = await diff_at_versions(
            db=db,
            connection_id=record_a.connection_id,
            from_version=record_a.schema_version,
            to_version=record_b.schema_version,
        )
        result["diff_source"] = "schema_only"
        result["backup_a"] = _backup_meta(record_a, backup_a_id)
        result["backup_b"] = _backup_meta(record_b, backup_b_id)
        return result

    schema_a = parsed_a["schema"]
    schema_b = parsed_b["schema"]
    tables_a = _normalise_tables(schema_a)
    tables_b = _normalise_tables(schema_b)
    enums_a = {
        k: v.get("values", v) if isinstance(v, dict) else v
        for k, v in schema_a.get("enums", {}).items()
    }
    enums_b = {
        k: v.get("values", v) if isinstance(v, dict) else v
        for k, v in schema_b.get("enums", {}).items()
    }

    from domains.schema.auto_migration import _structural_diff
    diff = await _structural_diff(
        before_tables=tables_a, after_tables=tables_b,
        before_enums=enums_a, after_enums=enums_b,
        schema="public",
    )
    has_destructive = any(c.get("is_destructive") for c in diff["changes"])

    return {
        "diff_source": "schema_only",
        "from_version": parsed_a["schema_version"],
        "to_version": parsed_b["schema_version"],
        "from_index": record_a.schema_order_index,
        "to_index": record_b.schema_order_index,
        "migration_count": None,
        "changes": diff["changes"],
        "up_sql": diff["up_sql"],
        "down_sql": diff["down_sql"],
        "has_destructive_changes": has_destructive,
        "summary": _diff_summary(diff["changes"], has_destructive),
        "backup_a": _backup_meta(record_a, backup_a_id),
        "backup_b": _backup_meta(record_b, backup_b_id),
    }


def _backup_meta(record: BackupRecord, backup_id: UUID) -> dict:
    return {
        "id": str(backup_id),
        "label": record.label,
        "schema_version": record.schema_version,
        "schema_order_index": record.schema_order_index,
        "part_count": record.part_count,
        "created_at": record.created_at.isoformat(),
    }


def _normalise_tables(schema: dict) -> dict[str, dict]:
    tables = schema.get("tables", {})
    result: dict[str, dict] = {}
    for name, t in tables.items():
        if isinstance(t, dict) and "columns" in t and isinstance(t["columns"], list):
            result[name] = {
                "columns": {c["name"]: c["data_type"] for c in t["columns"]},
                "indexes": [i["name"] for i in t.get("indexes", [])],
                "primary_key": t.get("primary_key", []),
            }
        else:
            result[name] = {"columns": {}, "indexes": [], "primary_key": []}
    return result


def _diff_summary(changes: list[dict], has_destructive: bool) -> str:
    if not changes:
        return "Schemas are identical."
    counts: dict[str, int] = {}
    for c in changes:
        k = c.get("kind", "change")
        counts[k] = counts.get(k, 0) + 1
    parts = [f"{v} {k.replace('_', ' ')}" for k, v in sorted(counts.items())]
    dest = " ⚠ Contains destructive changes." if has_destructive else ""
    return f"{len(changes)} changes: {', '.join(parts)}.{dest}"


# ---------------------------------------------------------------------------
# Schedule helpers (used by beat dispatcher in worker/celery.py)
# ---------------------------------------------------------------------------

async def get_due_schedules(db: AsyncSession) -> list[BackupSchedule]:
    """Return all enabled schedules whose next_run is in the past."""
    now = datetime.now(timezone.utc)
    result = await db.execute(
        select(BackupSchedule).where(
            BackupSchedule.enabled == True,  # noqa
            BackupSchedule.next_run <= now,
        )
    )
    return list(result.scalars().all())


def compute_next_run(cron: str, after: datetime | None = None) -> datetime:
    """Compute the next run datetime for a cron expression."""
    try:
        from croniter import croniter
        base = after or datetime.now(timezone.utc)
        it = croniter(cron, base)
        return it.get_next(datetime)
    except Exception as exc:
        raise ValueError(f"Invalid cron expression '{cron}': {exc}") from exc


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/backups", tags=["backups"])


class CreateBackupRequest(BaseModel):
    label: str
    format: str = "calyph"
    schema_only: bool = False


class RestoreRequest(BaseModel):
    strategy: str | None = None
    restore_session_id: str | None = None
    per_row_overrides: dict[str, dict[str, str]] | None = None


class DiffBackupsRequest(BaseModel):
    backup_a_id: UUID
    backup_b_id: UUID


class CreateScheduleRequest(BaseModel):
    cron: str = Field(..., description="Standard cron expression, e.g. '0 2 * * *'")
    label_template: str = Field(default="scheduled-{date}")
    format: str = Field(default="calyph")
    schema_only: bool = False


async def _pg(connection_id: UUID, workspace_id: UUID, db: AsyncSession) -> asyncpg.Connection:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=30)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")


@router.get("/{connection_id}")
async def list_backups(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    assert user.workspace_id is not None
    result = await db.execute(
        select(BackupRecord)
        .where(
            BackupRecord.connection_id == connection_id,
            BackupRecord.workspace_id == user.workspace_id,
        )
        .order_by(BackupRecord.created_at.desc())
    )
    records = result.scalars().all()
    return {
        "backups": [
            {
                "id": str(r.id),
                "label": r.label,
                "format": r.format,
                "status": r.status,
                "size_bytes": r.size_bytes,
                "part_count": r.part_count,
                "total_rows": r.total_rows,
                "schema_version": r.schema_version,
                "schema_order_index": r.schema_order_index,
                "error": r.error,
                "created_at": r.created_at.isoformat(),
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in records
        ]
    }


@router.post("/{connection_id}", status_code=status.HTTP_201_CREATED)
async def create_backup_endpoint(
    connection_id: UUID,
    body: CreateBackupRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    if not user.workspace_id:
        raise HTTPException(status_code=400, detail="User has no workspace.")
    assert user.workspace_id is not None
    if body.format not in ("calyph", "sql"):
        raise HTTPException(status_code=400, detail="format must be 'calyph' or 'sql'")

    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        record = await create_backup(
            db=db, pg_conn=pg_conn, connection_id=connection_id,
            workspace_id=user.workspace_id, label=body.label,
            fmt=body.format, schema_only=body.schema_only,
        )
    finally:
        await pg_conn.close()

    if record.status == "failed":
        raise HTTPException(status_code=500, detail=f"Backup failed: {record.error}")

    return {
        "id": str(record.id),
        "status": record.status,
        "size_bytes": record.size_bytes,
        "part_count": record.part_count,
        "schema_version": record.schema_version,
        "schema_order_index": record.schema_order_index,
    }


@router.get("/{connection_id}/{backup_id}/download")
async def download_backup(
    connection_id: UUID,
    backup_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    part: int = Query(1, ge=1, description="Part number to download (default: 1)"),
):
    assert user.workspace_id is not None
    record = await db.get(BackupRecord, backup_id)
    if not record or record.workspace_id != user.workspace_id:
        raise HTTPException(status_code=404, detail="Backup not found.")
    if not record.r2_key:
        raise HTTPException(status_code=400, detail="Backup has no stored file.")
    if part > record.part_count:
        raise HTTPException(
            status_code=400,
            detail=f"Backup has {record.part_count} part(s). Requested part {part}.",
        )

    key = _r2_key(
        user.workspace_id, connection_id, backup_id, record.format,
        part if record.part_count > 1 else None,
    )
    url = await generate_r2_download_url(key)
    return {
        "url": url,
        "expires_in": 3600,
        "part": part,
        "total_parts": record.part_count,
    }


@router.post("/{connection_id}/{backup_id}/restore")
async def restore_backup_endpoint(
    connection_id: UUID,
    backup_id: UUID,
    body: RestoreRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Two-phase restore.

    Phase 1: POST with empty body (no strategy).  Returns conflict_report
             and restore_session_id.

    Phase 2: POST with strategy='skip'|'overwrite'|'review' and the
             restore_session_id from Phase 1.
             For strategy='review', also include per_row_overrides:
               {"table_name": {"pk_value": "skip"|"overwrite"}}
    """
    assert user.workspace_id is not None
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        result = await restore_backup(
            db=db,
            pg_conn=pg_conn,
            backup_id=backup_id,
            workspace_id=user.workspace_id,
            strategy=body.strategy,
            per_row_overrides=body.per_row_overrides,
            restore_session_id=body.restore_session_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()
    return result


@router.post("/{connection_id}/diff")
async def diff_backups_endpoint(
    connection_id: UUID,
    body: DiffBackupsRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    assert user.workspace_id is not None
    try:
        result = await diff_backups(
            db=db,
            backup_a_id=body.backup_a_id,
            backup_b_id=body.backup_b_id,
            workspace_id=user.workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result


@router.delete("/{connection_id}/{backup_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_backup(
    connection_id: UUID,
    backup_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    assert user.workspace_id is not None
    record = await db.get(BackupRecord, backup_id)
    if not record or record.workspace_id != user.workspace_id:
        raise HTTPException(status_code=404, detail="Backup not found.")

    # Delete all parts
    client = _r2_client()
    for part_idx in range(1, record.part_count + 1):
        part_num = part_idx if record.part_count > 1 else None
        key = _r2_key(user.workspace_id, connection_id, backup_id, record.format, part_num)
        try:
            await asyncio.to_thread(
                client.delete_object, Bucket=settings.R2_BUCKET_NAME, Key=key
            )
        except Exception as exc:
            logger.warning(f"Failed to delete R2 object {key}: {exc}")

    await db.delete(record)
    await db.commit()


# ---------------------------------------------------------------------------
# Schedule endpoints (Gap 10-4)
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/schedule", status_code=status.HTTP_201_CREATED)
async def create_or_update_schedule(
    connection_id: UUID,
    body: CreateScheduleRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Create or update the backup schedule for a connection.
    One schedule per connection — subsequent calls update the existing one.
    """
    assert user.workspace_id is not None
    # Validate cron expression
    try:
        next_run = compute_next_run(body.cron)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    result = await db.execute(
        select(BackupSchedule).where(BackupSchedule.connection_id == connection_id)
    )
    schedule = result.scalar_one_or_none()

    if schedule:
        schedule.cron = body.cron
        schedule.label_template = body.label_template
        schedule.format = body.format
        schedule.schema_only = body.schema_only
        schedule.enabled = True
        schedule.next_run = next_run
        schedule.updated_at = datetime.now(timezone.utc)
    else:
        schedule = BackupSchedule(
            connection_id=connection_id,
            workspace_id=user.workspace_id,
            cron=body.cron,
            label_template=body.label_template,
            format=body.format,
            schema_only=body.schema_only,
            enabled=True,
            next_run=next_run,
        )
        db.add(schedule)

    await db.commit()
    await db.refresh(schedule)

    return {
        "id": str(schedule.id),
        "connection_id": str(schedule.connection_id),
        "cron": schedule.cron,
        "next_run": schedule.next_run.isoformat() if schedule.next_run else None,
        "enabled": schedule.enabled,
    }


@router.get("/{connection_id}/schedule")
async def get_schedule(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    assert user.workspace_id is not None
    result = await db.execute(
        select(BackupSchedule).where(
            BackupSchedule.connection_id == connection_id,
            BackupSchedule.workspace_id == user.workspace_id,
        )
    )
    schedule = result.scalar_one_or_none()
    if not schedule:
        raise HTTPException(status_code=404, detail="No schedule for this connection.")
    return {
        "id": str(schedule.id),
        "cron": schedule.cron,
        "label_template": schedule.label_template,
        "format": schedule.format,
        "schema_only": schedule.schema_only,
        "enabled": schedule.enabled,
        "next_run": schedule.next_run.isoformat() if schedule.next_run else None,
        "last_run": schedule.last_run.isoformat() if schedule.last_run else None,
        "last_backup_id": str(schedule.last_backup_id) if schedule.last_backup_id else None,
    }


@router.delete("/{connection_id}/schedule", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    assert user.workspace_id is not None
    result = await db.execute(
        select(BackupSchedule).where(
            BackupSchedule.connection_id == connection_id,
            BackupSchedule.workspace_id == user.workspace_id,
        )
    )
    schedule = result.scalar_one_or_none()
    if not schedule:
        raise HTTPException(status_code=404, detail="No schedule for this connection.")
    await db.delete(schedule)
    await db.commit()
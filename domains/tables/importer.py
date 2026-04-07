# domains/tables/importer.py

"""
Data Import Ingestion Engine.

ARCHITECTURAL BOUNDARY:
  File parsing (CSV, JSON, Excel), decompression, and type inference are 
  STRICTLY frontend responsibilities. 
  
  The backend does NOT read files. It receives structured, validated, 
  chunked JSON arrays from the client (Svelte) via the /import/chunk endpoint.
  This protects the API layer from OOM crashes, malicious gzip bombs, and 
  long-running synchronous request blocks.

Design principles:
  - Streaming Ingestion: The client chunks data (e.g., 5,000 rows max).
  - Transaction Safety: Each chunk is executed inside a single asyncpg transaction.
  - Zero-State Backend: Session metadata is held in Redis; the database only 
    touches data when it is ready to be written.

Flow:
  1. Frontend parses CSV/Excel locally via Web Workers.
  2. Frontend calls POST /import/init to define schema and get a session ID.
  3. Frontend streams data via POST /import/chunk.
  4. execute_import_chunk() -> executes asyncpg.executemany().
"""

from __future__ import annotations
import csv
import io
import json
import asyncpg
from typing import Any
from loguru import logger
from uuid import UUID
from fastapi import HTTPException, status

from core.db import get_redis
from redis.asyncio import Redis as AsyncRedis

# ---------------------------------------------------------------------------
# Value coercion (Crucial for translating JSON strings to PG native types)
# ---------------------------------------------------------------------------

def _coerce_value(raw: Any, pg_type: str) -> Any:
    """
    Coerce a raw string/value to the appropriate Python type for asyncpg.
    Returns None for empty / null-like values.
    Raises ValueError if coercion fails.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s in ("", "null", "NULL", "None", "NA", "N/A", "nan", "NaN"):
        return None

    t = pg_type.upper()

    if t in ("TEXT", "VARCHAR", "CHAR", "CITEXT"):
        return s

    if t in ("INTEGER", "INT", "INT4", "SMALLINT", "INT2", "BIGINT", "INT8"):
        return int(s.replace(",", ""))

    if t in ("NUMERIC", "DECIMAL", "REAL", "DOUBLE PRECISION", "FLOAT", "FLOAT4", "FLOAT8"):
        return float(s.replace(",", ""))

    if t == "BOOLEAN":
        if s.lower() in ("true", "yes", "1", "t", "y", "on"):
            return True
        if s.lower() in ("false", "no", "0", "f", "n", "off"):
            return False
        raise ValueError(f"Cannot coerce '{s}' to BOOLEAN")

    if t in ("UUID",):
        return s  # asyncpg handles UUID strings automatically

    if t in ("DATE", "TIMESTAMPTZ", "TIMESTAMP"):
        return s  # asyncpg parses standard ISO date/timestamp strings

    if t in ("JSONB", "JSON"):
        if isinstance(raw, (dict, list)):
            return json.dumps(raw)
        try:
            json.loads(s)
            return s
        except json.JSONDecodeError:
            raise ValueError(f"Cannot coerce '{s[:50]}...' to JSON")

    # Fallback — return as string
    return s


# ---------------------------------------------------------------------------
# Core Ingestion Engine
# ---------------------------------------------------------------------------


async def init_import_session(
    workspace_id: UUID, 
    connection_id: UUID, 
    session_id: str,
    tier_limits: dict[str, Any]
) -> None:
    """
    Initializes metering. Must be called by the POST /import/init endpoint before chunks begin.
    """
    redis: AsyncRedis = await get_redis()
    active_imports_key = f"active_imports:{workspace_id}:{connection_id}"
    
    # 1. Enforce Concurrency Lock
    active_count = await redis.scard(active_imports_key)  # type: ignore
    max_concurrent = tier_limits.get("max_concurrent_imports", 1)
    
    if active_count >= max_concurrent:
        """
        [UI CONSIDERATION]
        If 429 is returned, UI should alert the user that another import is currently 
        running on this database and they must wait.
        """
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Limit reached: You can only run {max_concurrent} concurrent import(s) on this database."
        )

    # 2. Register session & initialize row counter
    await redis.sadd(active_imports_key, session_id)  # type: ignore
    await redis.expire(active_imports_key, 3600) 
    await redis.setex(f"import_rows_counted:{session_id}", 3600, 0)


async def complete_import_session(workspace_id: UUID, connection_id: UUID, session_id: str) -> None:
    """ 
    Cleans up locks. Must be called by the frontend when all chunks are sent, or if user cancels. 
    """
    redis: AsyncRedis = await get_redis()
    await redis.srem(f"active_imports:{workspace_id}:{connection_id}", session_id)  # type: ignore
    await redis.delete(f"import_rows_counted:{session_id}")


async def execute_import_chunk(
    pg_conn: asyncpg.Connection,
    table_name: str,
    schema: str,
    columns: list[str],
    column_types: dict[str, str],
    rows: list[dict[str, Any]],
    session_id: str,
    tier_limits: dict[str, Any]
) -> int:
    """
    Executes a high-speed bulk insert and meters the rows against tier limits.
    """
    redis: AsyncRedis = await get_redis()
    chunk_size = len(rows)
    
    # 1. Enforce Row Limits
    counter_key = f"import_rows_counted:{session_id}"
    current_rows_raw = await redis.get(counter_key)
    
    if current_rows_raw is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Import session invalid or expired. Please restart the import."
        )
        
    current_rows = int(current_rows_raw)
    max_rows = tier_limits.get("max_import_rows_per_job", 10000) 
    
    if current_rows + chunk_size > max_rows:
        """
        [UI CONSIDERATION]
        If the frontend receives a 402 here, it MUST immediately halt sending further chunks,
        show the user a success message for the rows already imported, and prompt an upgrade 
        to continue importing unlimited rows.
        """
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=f"Tier limit reached: Your plan allows a maximum of {max_rows} rows per import job."
        )

    # 2. Database Execution
    cols_sql = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    insert_sql = f'INSERT INTO "{schema}"."{table_name}" ({cols_sql}) VALUES ({placeholders})'

    value_tuples = []
    for row_idx, row in enumerate(rows):
        try:
            values = tuple(
                _coerce_value(row.get(col), column_types[col])
                for col in columns
            )
            value_tuples.append(values)
        except Exception as exc:
            raise ValueError(f"Data type error on row {row_idx + 1}: {exc}")

    async with pg_conn.transaction():
        await pg_conn.executemany(insert_sql, value_tuples)
        
    # 3. Update Metering
    await redis.incrby(counter_key, chunk_size)
    return len(value_tuples)

# ---------------------------------------------------------------------------
# Export helpers (for round-trip testing and download)
# ---------------------------------------------------------------------------

def rows_to_csv(rows: list[dict[str, Any]], column_names: list[str]) -> bytes:
    """Convert rows to CSV bytes for download."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=column_names, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")

def rows_to_json(rows: list[dict[str, Any]]) -> bytes:
    """Convert rows to JSON bytes for download."""
    return json.dumps(rows, default=str, indent=2).encode("utf-8")
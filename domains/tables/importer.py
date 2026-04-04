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

async def execute_import_chunk(
    pg_conn: asyncpg.Connection,
    table_name: str,
    schema: str,
    columns: list[str],
    column_types: dict[str, str],
    rows: list[dict[str, Any]],
) -> int:
    """
    Executes a high-speed bulk insert for a single chunk of data.
    All rows succeed, or the entire chunk rolls back.
    """
    cols_sql = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    insert_sql = f'INSERT INTO "{schema}"."{table_name}" ({cols_sql}) VALUES ({placeholders})'

    value_tuples = []
    
    # Map dictionaries to flat tuples, coercing types based on DB schema
    for row_idx, row in enumerate(rows):
        try:
            values = tuple(
                _coerce_value(row.get(col), column_types[col])
                for col in columns
            )
            value_tuples.append(values)
        except Exception as exc:
            raise ValueError(f"Data type error on row {row_idx + 1}: {exc}")

    # Bulk execute inside a single transaction
    async with pg_conn.transaction():
        await pg_conn.executemany(insert_sql, value_tuples)
        
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
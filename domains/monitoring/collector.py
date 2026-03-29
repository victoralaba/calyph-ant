# domains/monitoring/collector.py
"""
Monitoring domain.

Collects and exposes database health and performance metrics:
- Table and index sizes
- Row count estimates
- Active connections and locks
- Slow query log via pg_stat_statements
- Index usage stats (unused index detection)
- Cache hit ratios
- Bloat estimates

Permission-aware degradation
-----------------------------
All collectors now catch asyncpg.InsufficientPrivilegeError and return
a structured "restricted" response rather than raising a 500. This is
critical because cloud-managed databases (Neon, Supabase, Railway) often
use restricted default roles that lack access to pg_locks, pg_stat_activity,
and pg_stat_statements.

Each endpoint that may require elevated privileges returns a consistent
shape:
  {
    "data": [...] | null,
    "restricted": bool,
    "restriction_message": str | null,
    "required_role": str | null,
  }

The overview endpoint aggregates availability and sets
"monitoring_available": false when most stats are restricted.

Feature 22: AI-powered index recommendations + one-click apply.
(unchanged from original)
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from loguru import logger


# ---------------------------------------------------------------------------
# Restricted-result helper
# ---------------------------------------------------------------------------

def _restricted(message: str, required_role: str | None = None) -> dict[str, Any]:
    """Standard shape returned when a monitoring query fails due to privileges."""
    return {
        "data": None,
        "restricted": True,
        "restriction_message": message,
        "required_role": required_role,
    }


def _ok(data: Any) -> dict[str, Any]:
    return {
        "data": data,
        "restricted": False,
        "restriction_message": None,
        "required_role": None,
    }

def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------

async def get_overview(pg_conn: asyncpg.Connection) -> dict[str, Any]:
    """High-level dashboard summary — single round trip via CTEs."""
    try:
        row = await pg_conn.fetchrow(
            """
            WITH
            db_size AS (
                SELECT pg_size_pretty(pg_database_size(current_database())) AS total_size,
                       pg_database_size(current_database()) AS total_bytes
            ),
            conn_count AS (
                SELECT count(*) AS active,
                       max_conn.setting::int AS max_connections
                FROM pg_stat_activity, (SELECT setting FROM pg_settings WHERE name = 'max_connections') max_conn
                WHERE state = 'active'
                GROUP BY max_conn.setting
            ),
            table_count AS (
                SELECT count(*) AS tables
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ),
            index_count AS (
                SELECT count(*) AS indexes
                FROM pg_indexes
                WHERE schemaname = 'public'
            ),
            cache AS (
                SELECT round(
                    sum(heap_blks_hit) * 100.0 /
                    NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2
                ) AS cache_hit_ratio
                FROM pg_statio_user_tables
            )
            SELECT
                db_size.total_size,
                db_size.total_bytes,
                COALESCE(conn_count.active, 0) AS active_connections,
                COALESCE(conn_count.max_connections, 100) AS max_connections,
                table_count.tables,
                index_count.indexes,
                COALESCE(cache.cache_hit_ratio, 0) AS cache_hit_ratio,
                current_database() AS database_name,
                version() AS pg_version
            FROM db_size, table_count, index_count, cache
            LEFT JOIN conn_count ON true
            """
        )
        result = dict(row) if row else {}
        result["monitoring_available"] = True
        result["restricted_features"] = []
        return result

    except asyncpg.InsufficientPrivilegeError as exc:
        # Fallback: collect what we can without privileged views
        try:
            minimal = await pg_conn.fetchrow(
                """
                SELECT
                    pg_size_pretty(pg_database_size(current_database())) AS total_size,
                    pg_database_size(current_database()) AS total_bytes,
                    current_database() AS database_name,
                    version() AS pg_version,
                    (SELECT count(*) FROM information_schema.tables
                     WHERE table_schema = 'public' AND table_type = 'BASE TABLE') AS tables,
                    (SELECT count(*) FROM pg_indexes WHERE schemaname = 'public') AS indexes
                """
            )
            result = dict(minimal) if minimal else {}
            result["monitoring_available"] = False
            result["restricted_features"] = [
                "active_connections",
                "cache_hit_ratio",
                "pg_stat_activity",
            ]
            result["restriction_message"] = (
                f"Some monitoring stats are unavailable: {exc.args[0]}. "
                "Grant the pg_monitor role for full monitoring access."
            )
            return result
        except Exception as fallback_exc:
            logger.warning(f"Overview fallback also failed: {fallback_exc}")
            return {
                "monitoring_available": False,
                "restricted_features": ["all"],
                "restriction_message": str(exc),
            }


async def get_table_sizes(pg_conn: asyncpg.Connection, schema: str = "public") -> dict[str, Any]:
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                t.relname AS table_name,
                pg_size_pretty(pg_total_relation_size(t.oid)) AS total_size,
                pg_size_pretty(pg_relation_size(t.oid)) AS table_size,
                pg_size_pretty(pg_total_relation_size(t.oid) - pg_relation_size(t.oid)) AS index_size,
                pg_total_relation_size(t.oid) AS total_bytes,
                t.reltuples::bigint AS row_estimate,
                t.relhasindex AS has_indexes,
                age(t.relfrozenxid) AS xid_age
            FROM pg_class t
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1
              AND t.relkind = 'r'
            ORDER BY pg_total_relation_size(t.oid) DESC
            """,
            schema,
        )
        return _ok([dict(r) for r in rows])
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot read table sizes: {exc.args[0]}. "
            "Requires SELECT on pg_class and pg_namespace.",
            required_role="pg_monitor or superuser",
        )


async def get_index_stats(pg_conn: asyncpg.Connection, schema: str = "public") -> dict[str, Any]:
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                i.indexrelname AS index_name,
                i.relname AS table_name,
                pg_size_pretty(pg_relation_size(i.indexrelid)) AS size,
                pg_relation_size(i.indexrelid) AS size_bytes,
                i.idx_scan AS scans,
                i.idx_tup_read AS tuples_read,
                i.idx_tup_fetch AS tuples_fetched,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                CASE WHEN i.idx_scan = 0 THEN true ELSE false END AS is_unused
            FROM pg_stat_user_indexes i
            JOIN pg_index ix ON ix.indexrelid = i.indexrelid
            JOIN pg_namespace n ON n.oid = (
                SELECT relnamespace FROM pg_class WHERE oid = i.relid
            )
            WHERE n.nspname = $1
            ORDER BY i.idx_scan ASC, pg_relation_size(i.indexrelid) DESC
            """,
            schema,
        )
        return _ok([dict(r) for r in rows])
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot read index statistics: {exc.args[0]}. "
            "Grant the pg_monitor role for index usage stats.",
            required_role="pg_monitor or superuser",
        )


async def get_slow_queries(
    pg_conn: asyncpg.Connection,
    min_duration_ms: float = 100.0,
    limit: int = 20,
) -> dict[str, Any]:
    """
    Fetch slow queries from pg_stat_statements.

    Returns a structured restricted response (not a dict with "error" key)
    when pg_stat_statements is not installed or the role lacks access.
    """
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                query,
                calls,
                round((total_exec_time / calls)::numeric, 2) AS avg_ms,
                round(total_exec_time::numeric, 2) AS total_ms,
                round(min_exec_time::numeric, 2) AS min_ms,
                round(max_exec_time::numeric, 2) AS max_ms,
                rows,
                shared_blks_hit,
                shared_blks_read,
                round(
                    shared_blks_hit * 100.0 /
                    NULLIF(shared_blks_hit + shared_blks_read, 0), 2
                ) AS cache_hit_pct
            FROM pg_stat_statements
            WHERE total_exec_time / calls >= $1
              AND query NOT LIKE '%pg_stat_statements%'
            ORDER BY avg_ms DESC
            LIMIT $2
            """,
            min_duration_ms,
            limit,
        )
        return _ok([dict(r) for r in rows])

    except asyncpg.UndefinedTableError:
        return _restricted(
            "pg_stat_statements extension is not installed. "
            "Enable it via the Extensions manager to track slow queries.",
            required_role=None,
        )
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot read pg_stat_statements: {exc.args[0]}. "
            "Grant pg_monitor role or enable pg_stat_statements with "
            "pg_stat_statements.track = 'all' and appropriate permissions.",
            required_role="pg_monitor or superuser",
        )
    except Exception as exc:
        # Catches the case where pg_stat_statements exists but the view
        # is in a different schema or has a different structure
        logger.warning(f"get_slow_queries unexpected error: {exc}")
        return _restricted(
            f"Could not read slow query data: {exc}",
            required_role="pg_monitor or superuser",
        )


async def get_active_locks(pg_conn: asyncpg.Connection) -> dict[str, Any]:
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                pg_locks.pid,
                pg_locks.mode,
                pg_locks.granted,
                pg_locks.locktype,
                pg_class.relname AS table_name,
                pg_stat_activity.query,
                pg_stat_activity.state,
                age(now(), pg_stat_activity.query_start) AS duration,
                pg_stat_activity.wait_event_type,
                pg_stat_activity.wait_event
            FROM pg_locks
            LEFT JOIN pg_class ON pg_class.oid = pg_locks.relation
            LEFT JOIN pg_stat_activity ON pg_stat_activity.pid = pg_locks.pid
            WHERE pg_class.relnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = 'public'
            )
            ORDER BY pg_locks.granted ASC, pg_stat_activity.query_start ASC
            LIMIT 50
            """
        )
        return _ok([dict(r) for r in rows])
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot read lock information: {exc.args[0]}. "
            "Grant the pg_monitor role to view active locks.",
            required_role="pg_monitor or superuser",
        )


async def get_connection_stats(pg_conn: asyncpg.Connection) -> dict[str, Any]:
    try:
        row = await pg_conn.fetchrow(
            """
            SELECT
                count(*) FILTER (WHERE state = 'active') AS active,
                count(*) FILTER (WHERE state = 'idle') AS idle,
                count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
                count(*) FILTER (WHERE wait_event_type = 'Lock') AS waiting_on_lock,
                count(*) AS total,
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections
            FROM pg_stat_activity
            WHERE datname = current_database()
            """
        )
        return _ok(dict(row) if row else {})
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot read connection statistics: {exc.args[0]}. "
            "Grant the pg_monitor role to view pg_stat_activity.",
            required_role="pg_monitor or superuser",
        )


async def get_cache_stats(pg_conn: asyncpg.Connection) -> dict[str, Any]:
    try:
        row = await pg_conn.fetchrow(
            """
            SELECT
                sum(heap_blks_read) AS heap_read,
                sum(heap_blks_hit) AS heap_hit,
                round(
                    sum(heap_blks_hit) * 100.0 /
                    NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 4
                ) AS table_cache_hit_ratio,
                sum(idx_blks_read) AS idx_read,
                sum(idx_blks_hit) AS idx_hit,
                round(
                    sum(idx_blks_hit) * 100.0 /
                    NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0), 4
                ) AS index_cache_hit_ratio
            FROM pg_statio_user_tables
            """
        )
        return _ok(dict(row) if row else {})
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot read buffer cache statistics: {exc.args[0]}. "
            "Grant the pg_monitor role for cache hit ratios.",
            required_role="pg_monitor or superuser",
        )


async def get_bloat_estimates(pg_conn: asyncpg.Connection, schema: str = "public") -> dict[str, Any]:
    try:
        rows = await pg_conn.fetch(
            """
            SELECT
                schemaname,
                tablename,
                pg_size_pretty(real_size::bigint) AS real_size,
                pg_size_pretty(bloat_size::bigint) AS bloat_size,
                round(bloat_ratio::numeric, 2) AS bloat_ratio_pct
            FROM (
                SELECT
                    s.schemaname,
                    s.relname AS tablename,
                    pg_total_relation_size(s.relid) AS real_size,
                    GREATEST(
                        pg_total_relation_size(s.relid)
                        - (c.reltuples * (8192 / NULLIF(c.reltuples, 0) + 1)),
                        0
                    ) AS bloat_size,
                    GREATEST(
                        (pg_total_relation_size(s.relid)
                        - c.reltuples * 8192) * 100.0
                        / NULLIF(pg_total_relation_size(s.relid), 0),
                        0
                    ) AS bloat_ratio
                FROM pg_stat_user_tables s
                JOIN pg_class c ON c.oid = s.relid
                WHERE s.schemaname = $1
            ) sub
            ORDER BY bloat_ratio DESC
            LIMIT 20
            """,
            schema,
        )
        return _ok([dict(r) for r in rows])
    except asyncpg.InsufficientPrivilegeError as exc:
        return _restricted(
            f"Cannot estimate bloat: {exc.args[0]}. "
            "Grant the pg_monitor role for bloat estimates.",
            required_role="pg_monitor or superuser",
        )


# ---------------------------------------------------------------------------
# Feature 22: AI-powered index recommendations
# ---------------------------------------------------------------------------

async def _get_schema_context(pg_conn: asyncpg.Connection, schema: str) -> str:
    rows = await pg_conn.fetch(
        """
        SELECT
            t.table_name,
            string_agg(
                c.column_name || ' ' || c.data_type,
                ', ' ORDER BY c.ordinal_position
            ) AS columns
        FROM information_schema.tables t
        JOIN information_schema.columns c
          ON c.table_name = t.table_name AND c.table_schema = t.table_schema
        WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE'
        GROUP BY t.table_name
        ORDER BY t.table_name
        LIMIT 30
        """,
        schema,
    )
    lines = [f"  {r['table_name']}({r['columns']})" for r in rows]
    return "\n".join(lines) or "No tables found."


async def _get_existing_indexes(pg_conn: asyncpg.Connection, schema: str) -> str:
    rows = await pg_conn.fetch(
        """
        SELECT indexname, tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = $1
        ORDER BY tablename, indexname
        LIMIT 50
        """,
        schema,
    )
    lines = [f"  {r['tablename']}: {r['indexname']}" for r in rows]
    return "\n".join(lines) or "No indexes."


async def generate_index_recommendations(
    pg_conn: asyncpg.Connection,
    schema: str = "public",
    slow_query_threshold_ms: float = 100.0,
) -> list[dict[str, Any]]:
    from core.config import settings
    from domains.ai.providers import get_provider

    slow_query_result = await get_slow_queries(pg_conn, slow_query_threshold_ms, limit=15)
    slow_queries: list[dict] = []
    if not slow_query_result.get("restricted") and slow_query_result.get("data"):
        slow_queries = slow_query_result["data"]

    schema_context = await _get_schema_context(pg_conn, schema)
    existing_indexes = await _get_existing_indexes(pg_conn, schema)

    index_result = await get_index_stats(pg_conn, schema)
    unused_indexes: list[dict] = []
    if not index_result.get("restricted") and index_result.get("data"):
        unused_indexes = [
            i for i in index_result["data"]
            if i.get("is_unused") and not i.get("is_primary")
        ]

    slow_q_text = ""
    if slow_queries:
        for i, q in enumerate(slow_queries[:10], 1):
            slow_q_text += (
                f"\n{i}. avg={q['avg_ms']}ms calls={q['calls']} "
                f"cache_hit={q['cache_hit_pct']}%\n   {str(q['query'])[:200]}\n"
            )
    else:
        slow_q_text = (
            "No slow query data available "
            "(pg_stat_statements not installed or role lacks pg_monitor)."
        )

    unused_text = ""
    if unused_indexes:
        unused_text = "\n".join(
            f"  {u['table_name']}.{u['index_name']} ({u['size']})"
            for u in unused_indexes[:10]
        )
    else:
        unused_text = "None detected or stats unavailable."

    system_prompt = """\
You are a PostgreSQL performance expert embedded in Calyphant, a database workspace tool.
Analyse the provided schema, slow queries, and index usage to generate specific, actionable
index recommendations.

Output ONLY a valid JSON array. No explanation text, no markdown fences.
Each element must have exactly these fields:
{
  "title": "Short action title (max 60 chars)",
  "reason": "Why this index helps — reference specific query patterns (max 200 chars)",
  "impact": "high" | "medium" | "low",
  "sql": "Complete CREATE INDEX statement — must be immediately executable",
  "table": "table_name",
  "affected_queries": ["brief query snippet that benefits"]
}

Rules:
- Only recommend indexes that don't already exist.
- Prefer composite indexes over multiple single-column indexes.
- For foreign keys without indexes, always recommend — high impact.
- For columns in WHERE clauses of slow queries, recommend — medium/high impact.
- Unused indexes: flag for dropping with sql = "DROP INDEX CONCURRENTLY ...".
- Maximum 8 recommendations total.
- If no recommendations, return empty array [].
"""

    user_prompt = f"""Schema (public):
{schema_context}

Existing indexes:
{existing_indexes}

Slow queries (avg_ms, calls, cache_hit%):
{slow_q_text}

Unused indexes (candidates for removal):
{unused_text}

Generate index recommendations:"""

    try:
        provider = get_provider()
        raw = await provider.complete(
            system=system_prompt,
            prompt=user_prompt,
            max_tokens=2000,
            temperature=0.1,
        )
        clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        recommendations = json.loads(clean)
        if not isinstance(recommendations, list):
            recommendations = []
    except json.JSONDecodeError as exc:
        logger.warning(f"AI recommendations returned invalid JSON: {exc}")
        recommendations = []
    except Exception as exc:
        logger.error(f"AI recommendation generation failed: {exc}")
        raise ValueError(f"Could not generate recommendations: {exc}")

    return recommendations


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


class ApplyRecommendationRequest(BaseModel):
    sql: str = Field(..., description="CREATE INDEX or DROP INDEX SQL from recommendation")
    label: str = Field(..., description="Human-readable label for the migration record")
    apply_immediately: bool = Field(default=False)


async def _pg(connection_id: UUID, workspace_id: UUID, db: AsyncSession) -> asyncpg.Connection:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=15)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")


@router.get("/{connection_id}/overview")
async def overview(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        return await get_overview(pg_conn)
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/tables")
async def table_sizes(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_table_sizes(pg_conn, schema_name)
        # Backwards-compatible: unwrap for clients that expect {"tables": [...]}
        if not result.get("restricted"):
            return {"tables": result["data"], "restricted": False}
        return {"tables": [], **result}
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/indexes")
async def index_stats(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_index_stats(pg_conn, schema_name)
        if not result.get("restricted"):
            return {"indexes": result["data"], "restricted": False}
        return {"indexes": [], **result}
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/queries")
async def slow_queries(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    min_ms: float = Query(100.0),
    limit: int = Query(20, le=100),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_slow_queries(pg_conn, min_ms, limit)
        if not result.get("restricted"):
            return {"queries": result["data"], "restricted": False}
        return {"queries": [], **result}
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/locks")
async def active_locks(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_active_locks(pg_conn)
        if not result.get("restricted"):
            return {"locks": result["data"], "restricted": False}
        return {"locks": [], **result}
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/connections")
async def connection_stats(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),

    db: AsyncSession = Depends(get_db),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_connection_stats(pg_conn)
        if not result.get("restricted"):
            return result["data"]
        return result
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/cache")
async def cache_stats(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_cache_stats(pg_conn)
        if not result.get("restricted"):
            return result["data"]
        return result
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/bloat")
async def bloat_estimates(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        result = await get_bloat_estimates(pg_conn, schema_name)
        if not result.get("restricted"):
            return {"bloat": result["data"], "restricted": False}
        return {"bloat": [], **result}
    finally:
        await pg_conn.close()


@router.post("/{connection_id}/recommendations")
async def get_index_recommendations(
    connection_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    min_ms: float = Query(100.0),
):
    pg_conn = await _pg(connection_id, workspace_id, db)
    try:
        recommendations = await generate_index_recommendations(
            pg_conn, schema=schema_name, slow_query_threshold_ms=min_ms
        )
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Recommendation generation failed: {exc}")
    finally:
        await pg_conn.close()

    return {
        "connection_id": str(connection_id),
        "schema": schema_name,
        "recommendation_count": len(recommendations),
        "recommendations": recommendations,
        "note": (
            "Use POST /monitoring/{connection_id}/recommendations/apply "
            "to create a migration from any recommendation."
        ),
    }


@router.post("/{connection_id}/recommendations/apply")
async def apply_recommendation(
    connection_id: UUID,
    body: ApplyRecommendationRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    from domains.migrations.service import create_migration, apply_migration

    db_url = await get_connection_url(db, connection_id, workspace_id)
    if not db_url:
        raise HTTPException(status_code=404, detail="Connection not found.")

    try:
        record = await create_migration(
            db=db,
            connection_id=connection_id,
            workspace_id=workspace_id,
            label=body.label,
            up_sql=body.sql,
            down_sql=None,
            generated_by="ai",
            db_url=db_url,
            skip_validation=False,
        )
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": str(exc),
                "code": "INSUFFICIENT_PRIVILEGES",
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to create migration: {exc}")

    if record.syntax_valid is False:
        validation_error = record.meta.get("validation_error", "unknown")
        return {
            "migration_id": str(record.id),
            "applied": False,
            "syntax_valid": False,
            "error": f"SQL validation failed: {validation_error}.",
            "version": record.version,
        }

    applied = False
    apply_error: str | None = None

    if body.apply_immediately:
        pg_conn = await _pg(connection_id, workspace_id, db)
        try:
            await apply_migration(pg_conn, db, record.id)
            applied = True
        except asyncpg.InsufficientPrivilegeError as exc:
            apply_error = (
                f"Insufficient privileges: {exc.args[0]}. "
                "The connected role needs DDL privileges to apply this index."
            )
        except Exception as exc:
            apply_error = str(exc)
        finally:
            await pg_conn.close()

    return {
        "migration_id": str(record.id),
        "version": record.version,
        "label": record.label,
        "sql": body.sql,
        "syntax_valid": record.syntax_valid,
        "applied": applied,
        "apply_error": apply_error,
        "message": (
            f"Migration {record.version} applied successfully."
            if applied
            else (
                f"Migration {record.version} created and pending review."
                if not apply_error
                else f"Migration created but apply failed: {apply_error}"
            )
        ),
    }
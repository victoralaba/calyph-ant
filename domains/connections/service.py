# domains/connections/service.py

"""
Connection service.

Handles the full lifecycle of a database connection:
- URL parsing and validation
- Live connectivity check
- PostgreSQL version and capability detection
- Cloud provider detection from hostname patterns
- Sleeping database detection and wake attempts
- Keep-alive ping execution (called by Celery tasks)
- CRUD for persisted connections

Privilege introspection
-----------------------
test_connection() now also calls introspect_privileges() which runs a
battery of lightweight SQL checks against the connected role to produce
a PrivilegeReport. This report is stored in capabilities and used by:

  - Schema editor   → 403 before attempting DDL on read-only connections
  - Migration apply → 403 with a clear message
  - Backup engine   → warning about incomplete dump risk
  - Monitoring      → graceful degradation on restricted roles
  - Extension mgr   → already handled per-operation; now also pre-checked

PrivilegeReport fields
----------------------
  is_read_only          bool  — user has no write privilege on public schema
  can_create_schema     bool  — CREATE on the database
  can_manage_extensions bool  — superuser or pg_extension_owner_member
  can_read_stats        bool  — pg_monitor role or superuser
  current_role          str   — the role name used for this connection
  missing_roles         list  — roles that would unlock additional features
  privilege_warnings    list  — human-readable warnings for the UI

Fail-open: if introspect_privileges() raises for any reason the
connection test still succeeds — privilege data is just absent from
capabilities. This prevents a privilege-check bug from blocking all
connections.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import ipaddress
import socket
import asyncpg
from loguru import logger
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.config import settings
from domains.connections.models import (
    CloudProvider,
    Connection,
    ConnectionStatus,
)
from shared.types import (
    ConnectionCreateRequest,
    ConnectionTestResult,
    ConnectionUpdateRequest,
    PaginatedResponse,
)


# ---------------------------------------------------------------------------
# Provider detection — hostname pattern matching
# ---------------------------------------------------------------------------

_PROVIDER_PATTERNS: list[tuple[re.Pattern, CloudProvider]] = [
    (re.compile(r"\.neon\.tech$"), CloudProvider.neon),
    (re.compile(r"\.supabase\.co$"), CloudProvider.supabase),
    (re.compile(r"\.railway\.app$"), CloudProvider.railway),
    (re.compile(r"\.render\.com$"), CloudProvider.render),
    (re.compile(r"\.rds\.amazonaws\.com$"), CloudProvider.aws_rds),
    (re.compile(r"\.cloudsql\.google\.com$"), CloudProvider.google_cloud_sql),
    (re.compile(r"\.database\.windows\.net$"), CloudProvider.azure),
    (re.compile(r"^(localhost|127\.0\.0\.1|0\.0\.0\.0|::1)$"), CloudProvider.local),
]

# Providers known to suspend connections after inactivity
_SLEEPING_PROVIDERS = {
    CloudProvider.neon,
    CloudProvider.supabase,
    CloudProvider.railway,
    CloudProvider.render,
}


def detect_provider(host: str) -> CloudProvider:
    for pattern, provider in _PROVIDER_PATTERNS:
        if pattern.search(host):
            return provider
    return CloudProvider.unknown


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------

async def parse_and_validate_url(url: str) -> dict[str, Any]:
    """
    Parse a PostgreSQL connection URL and resolve its DNS asynchronously.
    Enforces SSRF protection by blocking internal/private IPs in production.
    """
    try:
        parsed = urlparse(url)
    except Exception as exc:
        raise ValueError(f"Could not parse URL: {exc}") from exc

    host = parsed.hostname
    if not host:
        raise ValueError("No host found in connection URL.")

    # ---------------------------------------------------------
    # DEFENSE: Async DNS Resolution & SSRF Protection
    # ---------------------------------------------------------
    loop = asyncio.get_running_loop()
    try:
        # Resolve host asynchronously to prevent event-loop blocking
        # Returns a list of (family, type, proto, canonname, sockaddr)
        addr_info = await loop.getaddrinfo(host, parsed.port or 5432, family=socket.AF_UNSPEC)
        
        # Extract the first resolved IP address
        resolved_ip = addr_info[0][4][0]
        ip_obj = ipaddress.ip_address(resolved_ip)

        # Block private, loopback, and link-local routing
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
            if not settings.is_development:
                raise ValueError(
                    f"Security Exception: Host '{host}' resolves to an internal IP "
                    f"({resolved_ip}). Targeting internal networks is forbidden."
                )
    except socket.gaierror:
        raise ValueError(f"Could not resolve hostname: {host}")
    # ---------------------------------------------------------

    return {
        "host": host,
        "port": parsed.port or 5432,
        "database": (parsed.path or "").lstrip("/") or "postgres",
        "username": parsed.username,
        "password": parsed.password or "",
        "provider": detect_provider(host),
    }


# ---------------------------------------------------------------------------
# Privilege introspection
# ---------------------------------------------------------------------------

@dataclass
class PrivilegeReport:
    """
    Lightweight privilege snapshot for a connected role.

    Built by introspect_privileges() and stored in capabilities so the
    rest of the application can gate write operations without re-querying
    the database on every request.
    """
    is_read_only: bool = True
    can_create_schema: bool = False
    can_manage_extensions: bool = False
    can_read_stats: bool = False
    current_role: str = "unknown"
    missing_roles: list[str] = field(default_factory=list)
    privilege_warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_read_only": self.is_read_only,
            "can_create_schema": self.can_create_schema,
            "can_manage_extensions": self.can_manage_extensions,
            "can_read_stats": self.can_read_stats,
            "current_role": self.current_role,
            "missing_roles": self.missing_roles,
            "privilege_warnings": self.privilege_warnings,
        }


async def introspect_privileges(conn: asyncpg.Connection) -> PrivilegeReport:
    """
    Run a battery of lightweight privilege checks against the connected role.

    All queries are read-only, O(1), and complete in < 5 ms on any Postgres version.
    Never raises — on any error returns a conservative report (is_read_only=True)
    so the caller can still surface the connection as restricted rather than
    crashing.
    """
    report = PrivilegeReport()

    try:
        # Current role
        report.current_role = await conn.fetchval("SELECT current_user") or "unknown"

        # Superuser shortcut — superuser can do everything
        is_superuser: bool = await conn.fetchval(
            "SELECT usesuper FROM pg_user WHERE usename = current_user"
        ) or False

        if is_superuser:
            report.is_read_only = False
            report.can_create_schema = True
            report.can_manage_extensions = True
            report.can_read_stats = True
            return report

        # Check CREATE on the current database (needed for schema creation)
        report.can_create_schema = await conn.fetchval(
            "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
        ) or False

        # Check INSERT/UPDATE/DELETE on the public schema — O(1) proxy for write access.
        can_write_schema: bool = await conn.fetchval(
            "SELECT has_schema_privilege(current_user, 'public', 'CREATE')"
        ) or False

        # Architectural Decision: We DO NOT scan pg_class with has_table_privilege here.
        # It causes O(N) timeouts on databases with massive table counts. 
        # We assume read-only if they cannot create in the public schema. 
        # Granular table-level write failures will be safely caught at execution time.
        report.is_read_only = not can_write_schema

        # pg_monitor role grants access to monitoring views without superuser
        has_pg_monitor: bool = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_monitor', 'MEMBER')"
        ) or False
        report.can_read_stats = has_pg_monitor

        # Extension management requires superuser (already handled above)
        # or pg_extension_owner_member on Postgres 15+
        try:
            has_ext_role: bool = await conn.fetchval(
                "SELECT pg_has_role(current_user, 'pg_extension_owner', 'MEMBER')"
            ) or False
            report.can_manage_extensions = has_ext_role
        except asyncpg.UndefinedObjectError:
            # pg_extension_owner doesn't exist on Postgres < 15
            report.can_manage_extensions = False

        # Build missing_roles list for UI hints
        if report.is_read_only:
            report.missing_roles.append("write access on public schema")
            report.privilege_warnings.append(
                f"Role '{report.current_role}' lacks schema creation privileges. "
                "Assuming read-only access. Schema changes, migrations, and "
                "data imports are disabled."
            )

        if not report.can_create_schema:
            report.missing_roles.append("CREATE on database")

        if not report.can_manage_extensions:
            report.missing_roles.append(
                "superuser or pg_extension_owner (for extension management)"
            )
            report.privilege_warnings.append(
                f"Role '{report.current_role}' cannot install or remove extensions. "
                "Connect with a superuser role to manage extensions."
            )

        if not report.can_read_stats:
            report.missing_roles.append("pg_monitor (for monitoring stats)")
            report.privilege_warnings.append(
                f"Role '{report.current_role}' cannot read pg_stat_statements "
                "or pg_locks. Grant pg_monitor for full monitoring access."
            )

    except Exception as exc:
        logger.warning(
            f"Privilege introspection failed (failing open): {exc}"
        )
        # Conservative: assume read-only when introspection fails
        report.is_read_only = True
        report.privilege_warnings.append(
            "Could not determine connection privileges. "
            "Write operations may be restricted."
        )

    return report


# ---------------------------------------------------------------------------
# Live connectivity and introspection
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type((OSError, asyncpg.PostgresConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
async def _connect_raw(url: str, timeout: int = 10) -> asyncpg.Connection:
    """Attempt a raw asyncpg connection with retries for transient failures."""
    return await asyncpg.connect(dsn=url, timeout=timeout)


async def test_connection(url: str) -> ConnectionTestResult:
    """
    Attempt to connect to the database and collect metadata.
    Returns a ConnectionTestResult regardless of success/failure.
    Does not persist anything.

    Privilege introspection is run after a successful connect and stored
    in capabilities under the "privileges" key. Introspection failure
    never fails the overall connection test — it just leaves privileges
    absent from capabilities.
    """
    components = await parse_and_validate_url(url)
    provider = components["provider"]
    is_sleeping_provider = provider in _SLEEPING_PROVIDERS

    conn: asyncpg.Connection | None = None
    try:
        # Allow a longer timeout for providers that may need to wake
        timeout = 20 if is_sleeping_provider else 10
        conn = await _connect_raw(url, timeout=timeout)

        # Collect version info
        version_str: str = str(await conn.fetchval("SELECT version()"))
        version_num: int = int(await conn.fetchval("SHOW server_version_num") or 0)

        # Collect installed extensions
        ext_rows = await conn.fetch(
            "SELECT name, default_version, installed_version "
            "FROM pg_available_extensions "
            "WHERE installed_version IS NOT NULL "
            "ORDER BY name"
        )
        extensions = [dict(r) for r in ext_rows]

        # Quick capability flags
        ext_names = {r["name"] for r in ext_rows}
        capabilities: dict[str, Any] = {
            "has_pgvector": "vector" in ext_names,
            "has_postgis": "postgis" in ext_names,
            "has_pg_trgm": "pg_trgm" in ext_names,
            "has_uuid_ossp": "uuid-ossp" in ext_names,
            "has_pg_stat_statements": "pg_stat_statements" in ext_names,
            "extensions": extensions,
        }

        # Privilege introspection — fail-open
        try:
            priv_report = await introspect_privileges(conn)
            capabilities["privileges"] = priv_report.to_dict()
        except Exception as exc:
            logger.warning(f"Privilege introspection skipped: {exc}")

        return ConnectionTestResult(
            success=True,
            host=components["host"],
            port=components["port"],
            database=components["database"],
            pg_version=version_str.split(",")[0].strip(),
            pg_version_num=int(version_num),
            cloud_provider=provider,
            capabilities=capabilities,
            was_sleeping=is_sleeping_provider,
            error=None,
        )

    except asyncpg.InvalidPasswordError:
        return ConnectionTestResult(
            success=False,
            error="Authentication failed. Check your username and password.",
            cloud_provider=provider,
        )
    except asyncpg.InvalidCatalogNameError:
        return ConnectionTestResult(
            success=False,
            error=f"Database not found: {components['database']}",
            cloud_provider=provider,
        )
    except (OSError, asyncpg.PostgresConnectionError, TimeoutError) as exc:
        error_msg = str(exc)
        # Pass the raw exception object down, not the stringified message
        was_sleeping = is_sleeping_provider and _looks_like_sleep_error(exc)
        return ConnectionTestResult(
            success=False,
            error=f"Could not reach database: {error_msg}",
            cloud_provider=provider,
            was_sleeping=was_sleeping,
        )
    except Exception as exc:
        logger.exception("Unexpected error testing connection")
        return ConnectionTestResult(
            success=False,
            error=f"Unexpected error: {exc}",
            cloud_provider=provider,
        )
    finally:
        if conn:
            await conn.close()


def _looks_like_sleep_error(exc: Exception) -> bool:
    """
    Precision check: does this exception indicate the DB compute is asleep/waking?
    Evaluates native Postgres SQLStates and OS-level socket errors rather than
    relying solely on fragile string matching.
    """
    # 1. OS-level network timeouts and refusals (Compute node offline)
    if isinstance(exc, (TimeoutError, ConnectionRefusedError)):
        return True

    # 2. Native Postgres SQLStates (Proxy is up, but compute is starting/routing)
    if isinstance(exc, asyncpg.PostgresError):
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate in (
            "57P03",  # cannot_connect_now (Server is starting up/waking)
            "08006",  # connection_failure
            "08001",  # sqlclient_unable_to_establish_sqlconnection
            "08004",  # sqlserver_rejected_establishment_of_sqlconnection
        ):
            return True

    # 3. Fallback heuristic for generic asyncpg/proxy text errors
    error_str = str(exc).lower()
    sleep_hints = [
        "connection refused",
        "connection timed out",
        "could not connect",
        "eof detected",
        "connection reset",
        "no route to host",
        "the database system is starting up",
    ]
    return any(hint in error_str for hint in sleep_hints)


# ---------------------------------------------------------------------------
# Keep-alive ping (called by Celery task)
# ---------------------------------------------------------------------------

async def ping_connection(url: str) -> bool:
    """
    Fire a cheap query to keep a cloud database awake.
    Returns True if successful. Does not raise.
    """
    conn = None
    try:
        conn = await asyncpg.connect(dsn=url, timeout=15)
        await conn.fetchval("SELECT 1")
        return True
    except Exception as exc:
        logger.warning(f"Keep-alive ping failed: {exc}")
        return False
    finally:
        if conn:
            await conn.close()


async def execute_presence_heartbeat(url: str) -> None:
    """
    A brutally fast, silent network pulse triggered by the frontend visibility API.
    Fails silently. Strict 3-second timeout. No database writes.
    Designed exclusively to reset the idle-timer on serverless DB proxies.
    """
    conn = None
    try:
        # 3-second timeout: If it takes longer than 3 seconds to connect, 
        # it is either already asleep or unreachable. Don't block the worker.
        conn = await asyncpg.connect(
            dsn=url, 
            timeout=3,
            server_settings={"application_name": "calyphant-ui-heartbeat"}
        )
        await conn.fetchval("SELECT 1")
    except Exception as exc:
        # We explicitly swallow exceptions here. Heartbeats are ephemeral.
        # If the DB is down, the actual query/schema routers will catch it.
        logger.debug(f"Presence heartbeat dropped: {exc}")
    finally:
        if conn:
            await conn.close()


# ---------------------------------------------------------------------------
# Privilege helpers — used by other domains
# ---------------------------------------------------------------------------

def get_privileges(capabilities: dict) -> dict[str, Any]:
    """
    Extract the privilege report from a connection's capabilities dict.
    Returns a safe default (read-only) when privileges are absent.
    """
    return capabilities.get("privileges", {
        "is_read_only": True,
        "can_create_schema": False,
        "can_manage_extensions": False,
        "can_read_stats": False,
        "current_role": "unknown",
        "missing_roles": [],
        "privilege_warnings": ["Privilege information unavailable."],
    })


def assert_writable(capabilities: dict, operation: str = "This operation") -> None:
    """
    Raise ValueError with a clear message if the connection is read-only.

    Called by schema editor, migration apply, and backup write paths
    before attempting any DDL or DML.
    """
    privs = get_privileges(capabilities)
    if privs.get("is_read_only", True):
        role = privs.get("current_role", "unknown")
        warnings = privs.get("privilege_warnings", [])
        detail = warnings[0] if warnings else ""
        raise ValueError(
            f"{operation} requires write access, but role '{role}' "
            f"has read-only privileges on this database. {detail}"
        )


# ---------------------------------------------------------------------------
# CRUD — persisted connections
# ---------------------------------------------------------------------------

async def create_connection(
    db: AsyncSession,
    workspace_id: UUID,
    user_id: UUID,
    data: ConnectionCreateRequest,
    encrypted_url: str,
    test_result: ConnectionTestResult,
) -> Connection:
    """
    Persist a new connection after a successful test.
    The caller is responsible for encrypting the URL before passing it.
    """
    conn = Connection(
        workspace_id=workspace_id,
        created_by=user_id,
        name=data.name,
        slug=_slugify(data.name),
        encrypted_url=encrypted_url,
        host=test_result.host,
        port=test_result.port,
        database=test_result.database,
        pg_version=test_result.pg_version,
        pg_version_num=test_result.pg_version_num,
        cloud_provider=test_result.cloud_provider or CloudProvider.unknown,
        status=ConnectionStatus.active,
        last_connected_at=datetime.now(timezone.utc),
        capabilities=test_result.capabilities or {},
        keep_alive_enabled=data.keep_alive_enabled,
        keep_alive_interval_seconds=data.keep_alive_interval_seconds,
    )
    db.add(conn)
    await db.commit()
    await db.refresh(conn)
    logger.info(
        f"Connection created: {conn.id} ({conn.name}) "
        f"provider={conn.cloud_provider} workspace={workspace_id}"
    )
    return conn


async def get_connection(
    db: AsyncSession,
    connection_id: UUID,
    workspace_id: UUID,
) -> Connection | None:
    result = await db.execute(
        select(Connection).where(
            Connection.id == connection_id,
            Connection.workspace_id == workspace_id,
            Connection.is_active == True,  # noqa: E712
        )
    )
    return result.scalar_one_or_none()


async def list_connections(
    db: AsyncSession,
    workspace_id: UUID,
    limit: int = 50,
    offset: int = 0,
) -> PaginatedResponse[Connection]:
    from sqlalchemy import func as sa_func

    q = select(Connection).where(
        Connection.workspace_id == workspace_id,
        Connection.is_active == True,  # noqa: E712
    )
    count_q = select(sa_func.count()).select_from(q.subquery())

    total = await db.scalar(count_q) or 0
    rows = (await db.execute(q.offset(offset).limit(limit))).scalars().all()

    return PaginatedResponse(items=list(rows), total=total, limit=limit, offset=offset)


async def update_connection(
    db: AsyncSession,
    connection_id: UUID,
    workspace_id: UUID,
    data: ConnectionUpdateRequest,
) -> Connection | None:
    conn = await get_connection(db, connection_id, workspace_id)
    if not conn:
        return None

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(conn, field, value)

    await db.commit()
    await db.refresh(conn)
    return conn


async def delete_connection(
    db: AsyncSession,
    connection_id: UUID,
    workspace_id: UUID,
) -> bool:
    """Soft delete, but strictly scrub the cryptographic payload to prevent credential hoarding."""
    from core.db import encrypt_secret
    
    # Overwrite the actual credentials with a dead payload
    scrubbed_payload = encrypt_secret("SCRUBBED_ON_DELETE")
    
    result = await db.execute(
        update(Connection)
        .where(
            Connection.id == connection_id,
            Connection.workspace_id == workspace_id,
        )
        .values(
            is_active=False,
            encrypted_url=scrubbed_payload
        )
    )
    await db.commit()
    return result.rowcount > 0 #type: ignore


async def mark_connection_status(
    db: AsyncSession,
    connection_id: UUID,
    status: ConnectionStatus,
    error: str | None = None,
) -> None:
    """Called by keep-alive tasks and health checks to update status."""
    values: dict[str, Any] = {
        "status": status,
        "last_error": error,
    }
    if status == ConnectionStatus.active:
        values["last_connected_at"] = datetime.now(timezone.utc)
        values["last_error"] = None

    await db.execute(
        update(Connection)
        .where(Connection.id == connection_id)
        .values(**values)
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_-]+", "-", slug)
    return slug[:120]


async def rotate_workspace_keys(db: AsyncSession) -> dict[str, int]:
    """
    Cryptographic sweep: Decrypts all active connections and re-encrypts 
    them using the current Primary Key (the first key in the ENCRYPTION_KEYS array).
    
    Returns a telemetry dict of scanned, rotated, and failed counts.
    """
    from core.db import encrypt_secret, decrypt_secret
    
    # Fetch all active connections
    result = await db.execute(
        select(Connection).where(Connection.is_active == True)
    )
    connections = result.scalars().all()

    rotated_count = 0
    failed_count = 0

    for conn in connections:
        try:
            # MultiFernet seamlessly decrypts using whichever legacy key matches
            plaintext_url = decrypt_secret(conn.encrypted_url)
            
            # Encrypts using the NEW Primary Key
            new_ciphertext = encrypt_secret(plaintext_url)
            
            # Only flag as modified if the ciphertext actually changed
            if new_ciphertext != conn.encrypted_url:
                conn.encrypted_url = new_ciphertext
                rotated_count += 1
                
        except Exception as exc:
            logger.error(f"Key rotation failed for connection {conn.id}: {exc}")
            failed_count += 1

    if rotated_count > 0:
        await db.commit()
        
    return {
        "scanned": len(connections),
        "rotated": rotated_count,
        "failed": failed_count
    }
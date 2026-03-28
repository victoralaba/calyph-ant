# domains/backups/privileges.py
"""
Backup privilege pre-checks.

Separated here to avoid a circular import between backups/engine.py and
connections/service.py. Imported only by backups/engine.py.
"""

from __future__ import annotations

from typing import Any

import asyncpg
from loguru import logger


async def check_backup_privileges(pg_conn: asyncpg.Connection) -> dict[str, Any]:
    """
    Check whether the connected role can produce a complete backup.

    Returns a dict with:
      can_read_all_tables  bool  — SELECT on all tables in public
      is_superuser         bool  — full access, no restrictions
      restricted_tables    list  — table names this role cannot SELECT
      warnings             list  — human-readable warnings for the UI

    Never raises — on any error returns a conservative dict with
    warnings so the backup can proceed with a user-visible caveat.
    """
    result: dict[str, Any] = {
        "can_read_all_tables": True,
        "is_superuser": False,
        "restricted_tables": [],
        "warnings": [],
    }

    try:
        is_superuser: bool = await pg_conn.fetchval(
            "SELECT usesuper FROM pg_user WHERE usename = current_user"
        ) or False
        result["is_superuser"] = is_superuser

        if is_superuser:
            return result

        # Check which tables in public the current role cannot SELECT
        rows = await pg_conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND NOT has_table_privilege(current_user, table_schema || '.' || table_name, 'SELECT')
            ORDER BY table_name
            """
        )
        restricted = [r["table_name"] for r in rows]
        result["restricted_tables"] = restricted

        if restricted:
            result["can_read_all_tables"] = False
            current_role: str = await pg_conn.fetchval("SELECT current_user") or "unknown"
            result["warnings"].append(
                f"Role '{current_role}' cannot SELECT {len(restricted)} table(s): "
                f"{', '.join(restricted[:5])}"
                + (f" ... and {len(restricted) - 5} more" if len(restricted) > 5 else "")
                + ". These tables will be EMPTY in the backup. "
                "Connect with a superuser role for a complete backup."
            )

    except asyncpg.InsufficientPrivilegeError as exc:
        result["can_read_all_tables"] = False
        result["warnings"].append(
            f"Cannot determine backup completeness (privilege check failed): {exc.args[0]}. "
            "The backup may be incomplete if this role lacks SELECT on some tables."
        )
    except Exception as exc:
        logger.warning(f"Backup privilege check failed (non-fatal): {exc}")
        result["warnings"].append(
            "Could not verify backup completeness. "
            "Ensure the connected role has SELECT on all tables."
        )

    return result
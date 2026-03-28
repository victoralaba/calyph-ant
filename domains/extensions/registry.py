# domains/extensions/registry.py

"""
Extensions domain.

Manages PostgreSQL extension discovery, installation, and removal for a
specific user database connection.

Two-layer architecture:
  1. Platform catalogue (domains/platform/extension_catalogue.py)
     Global knowledge base — display names, descriptions, categories,
     docs URLs, capability metadata. Persisted in Calyphant's own DB.
     Enriched from PGXN / Trunk via background Celery tasks when an
     unknown extension is encountered.

  2. This module — live, connection-specific layer
     Queries the user's actual database via pg_available_extensions to
     get real installed/available status, then merges in platform
     catalogue metadata for rich UI display.

The extension domain IMPORTS from the platform catalogue.
The platform catalogue knows nothing about connections or asyncpg.

Router endpoints:
  GET    /extensions/{connection_id}              — list available + installed
  GET    /extensions/{connection_id}/installed    — installed only
  POST   /extensions/{connection_id}/enable       — CREATE EXTENSION
  DELETE /extensions/{connection_id}/{name}       — DROP EXTENSION
  GET    /extensions/{connection_id}/{name}       — extension detail + status
"""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from domains.platform.extension_catalogue import (
    ExtensionCatalogEntry,
    enrich_extension,
    get_entries_by_slugs,
    get_entry,
    increment_install_count,
    is_stale,
)


# ---------------------------------------------------------------------------
# Helpers — merge live Postgres data with platform catalogue metadata
# ---------------------------------------------------------------------------

def _build_extension_dict(
    pg_row: dict,
    catalogue_entry: ExtensionCatalogEntry | None,
) -> dict[str, Any]:
    """
    Merge a pg_available_extensions row with a catalogue entry.
    When no catalogue entry exists, the extension is still returned —
    just with minimal metadata. The background enrichment task will
    populate it before the next request.
    """
    name = pg_row["name"]
    installed = pg_row.get("installed_version") is not None

    if catalogue_entry:
        return {
            "name": name,
            "display_name": catalogue_entry.display_name,
            "description": catalogue_entry.description or pg_row.get("comment") or "",
            "category": catalogue_entry.category,
            "default_version": pg_row.get("default_version"),
            "installed_version": pg_row.get("installed_version"),
            "installed": installed,
            "adds_types": catalogue_entry.adds_types,
            "adds_functions": catalogue_entry.adds_functions,
            "adds_index_methods": catalogue_entry.adds_index_methods,
            "requires_superuser": catalogue_entry.requires_superuser,
            "docs_url": catalogue_entry.docs_url,
            "install_count": catalogue_entry.install_count,
            "is_curated": catalogue_entry.is_curated,
            "metadata_source": catalogue_entry.source,
        }

    # No catalogue entry yet — return minimal info
    return {
        "name": name,
        "display_name": name,
        "description": pg_row.get("comment") or "",
        "category": "other",
        "default_version": pg_row.get("default_version"),
        "installed_version": pg_row.get("installed_version"),
        "installed": installed,
        "adds_types": [],
        "adds_functions": [],
        "adds_index_methods": [],
        "requires_superuser": False,
        "docs_url": None,
        "install_count": 0,
        "is_curated": False,
        "metadata_source": None,
    }


def _dispatch_enrichment(slug: str) -> None:
    """
    Fire a background Celery task to enrich an unknown extension.
    Best-effort — never raises.
    """
    try:
        from celery import current_app as celery_app
        celery_app.send_task(
            "platform.catalogue.enrich_extension_async",
            args=[slug],
            queue="maintenance",
        )
    except Exception as exc:
        # Celery may not be available in all environments (tests, CLI)
        import logging
        logging.getLogger(__name__).debug(
            f"Could not dispatch enrichment task for '{slug}': {exc}"
        )


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------

async def list_extensions(
    pg_conn: asyncpg.Connection,
    db: AsyncSession,
) -> dict[str, Any]:
    """
    Return all available extensions with installed status merged in,
    enriched with platform catalogue metadata.

    For each extension not found in (or stale in) the catalogue, a
    background enrichment task is dispatched. The current request
    returns whatever metadata is available right now.
    """
    rows = await pg_conn.fetch(
        """
        SELECT name, default_version, installed_version, comment
        FROM pg_available_extensions
        ORDER BY name
        """
    )

    slugs = [r["name"] for r in rows]
    catalogue_map = await get_entries_by_slugs(db, slugs)

    installed_names = {
        r["name"] for r in rows if r["installed_version"] is not None
    }

    result = []
    unknown_slugs: list[str] = []
    stale_slugs: list[str] = []

    for row in rows:
        name = row["name"]
        pg_row = dict(row)
        entry = catalogue_map.get(name)

        if entry is None:
            unknown_slugs.append(name)
        elif is_stale(entry):
            stale_slugs.append(name)

        # Increment install count for installed extensions — fire and forget
        if row["installed_version"] is not None and entry is not None:
            # Use a background task to avoid blocking the response
            try:
                from core.db import _session_factory
                if _session_factory:
                    import asyncio
                    asyncio.create_task(increment_install_count(name))
            except Exception:
                pass

        result.append(_build_extension_dict(pg_row, entry))

    # Dispatch background enrichment for unknown / stale entries
    for slug in unknown_slugs + stale_slugs:
        _dispatch_enrichment(slug)

    return {
        "extensions": result,
        "installed_count": len(installed_names),
        "total_available": len(result),
        "unknown_count": len(unknown_slugs),   # How many are being enriched
    }


async def get_extension_detail(
    pg_conn: asyncpg.Connection,
    db: AsyncSession,
    name: str,
) -> dict[str, Any]:
    """
    Get detailed metadata for a single extension.

    Unlike list_extensions, this performs an inline (synchronous) enrichment
    fetch if the catalogue entry is missing or stale — because the user is
    explicitly asking about this specific extension and can tolerate a short
    wait for accurate metadata.
    """
    row = await pg_conn.fetchrow(
        "SELECT name, default_version, installed_version, comment "
        "FROM pg_available_extensions WHERE name = $1",
        name,
    )
    if not row:
        raise ValueError(f"Extension '{name}' not found on this database server.")

    pg_row = dict(row)
    entry = await get_entry(db, name)

    # Inline enrichment for single-extension detail view
    if entry is None or is_stale(entry):
        try:
            entry = await enrich_extension(db, name)
        except Exception as exc:
            # Enrichment failure must never break the detail endpoint
            import logging
            logging.getLogger(__name__).warning(
                f"Inline enrichment failed for '{name}': {exc}"
            )

    return _build_extension_dict(pg_row, entry)


async def enable_extension(
    pg_conn: asyncpg.Connection,
    db: AsyncSession,
    name: str,
    schema: str = "public",
) -> dict[str, Any]:
    """
    CREATE EXTENSION IF NOT EXISTS.
    Returns the updated detail with installed version.
    """
    if not re.match(r'^[\w\-]+$', name):
        raise ValueError(f"Invalid extension name: {name}")

    try:
        await pg_conn.execute(
            f'CREATE EXTENSION IF NOT EXISTS "{name}" WITH SCHEMA "{schema}"'
        )
    except asyncpg.InsufficientPrivilegeError:
        raise ValueError(
            f"Insufficient privileges to install '{name}'. "
            "This extension may require superuser access."
        )
    except asyncpg.UndefinedObjectError:
        raise ValueError(
            f"Extension '{name}' is not available on this PostgreSQL server."
        )

    # Bump install count after successful installation
    try:
        import asyncio
        asyncio.create_task(increment_install_count(name))
    except Exception:
        pass

    return await get_extension_detail(pg_conn, db, name)


async def disable_extension(
    pg_conn: asyncpg.Connection,
    name: str,
    cascade: bool = False,
) -> bool:
    """
    DROP EXTENSION IF EXISTS.
    cascade=True drops dependent objects — requires explicit opt-in.
    """
    if not re.match(r'^[\w\-]+$', name):
        raise ValueError(f"Invalid extension name: {name}")

    cascade_clause = " CASCADE" if cascade else " RESTRICT"
    try:
        await pg_conn.execute(
            f'DROP EXTENSION IF EXISTS "{name}"{cascade_clause}'
        )
        return True
    except asyncpg.DependentObjectsStillExistError as exc:
        raise ValueError(
            f"Cannot drop '{name}' — dependent objects exist. "
            "Use cascade=true to drop them too."
        ) from exc
    except asyncpg.InsufficientPrivilegeError:
        raise ValueError(f"Insufficient privileges to drop '{name}'.")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/extensions", tags=["extensions"])


class EnableRequest(BaseModel):
    name: str
    schema_name: str = "public"


class DisableRequest(BaseModel):
    cascade: bool = False


async def _pg(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> asyncpg.Connection:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=15.0)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")


@router.get("/{connection_id}")
async def list_all_extensions(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    List all extensions available on this connection's PostgreSQL server,
    merged with platform catalogue metadata.

    Extensions not yet in our catalogue are returned with minimal metadata
    and enriched in the background for subsequent requests.
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        return await list_extensions(pg_conn, db)
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/installed")
async def list_installed(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """List only the extensions currently installed on this connection."""
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        data = await list_extensions(pg_conn, db)
        installed = [e for e in data["extensions"] if e["installed"]]
        return {"extensions": installed, "count": len(installed)}
    finally:
        await pg_conn.close()


@router.get("/{connection_id}/{extension_name}")
async def get_extension(
    connection_id: UUID,
    extension_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed metadata for a single extension.

    Performs inline enrichment if the catalogue entry is missing or stale,
    so the response always has the freshest available metadata.
    """
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        return await get_extension_detail(pg_conn, db, extension_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    finally:
        await pg_conn.close()


@router.post("/{connection_id}/enable", status_code=status.HTTP_201_CREATED)
async def enable(
    connection_id: UUID,
    body: EnableRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """Install an extension on this connection's database."""
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        return await enable_extension(pg_conn, db, body.name, body.schema_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()


@router.delete("/{connection_id}/{extension_name}", status_code=status.HTTP_200_OK)
async def disable(
    connection_id: UUID,
    extension_name: str,
    body: DisableRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """Uninstall an extension from this connection's database."""
    pg_conn = await _pg(connection_id, user.workspace_id, db)
    try:
        await disable_extension(pg_conn, extension_name, cascade=body.cascade)
        return {"message": f"Extension '{extension_name}' removed."}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await pg_conn.close()

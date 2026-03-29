# domains/tables/presence.py
"""
Row editing presence layer.

Solves the UX gap between "a row is DB-locked" and "a user can see that
before they try to edit."

PostgreSQL's SELECT FOR UPDATE holds a lock only for the duration of the
update transaction — milliseconds. That window is too short to broadcast
meaningfully. The real collision window is the *editing intent* period:
the seconds or minutes between when a user clicks into a cell and when
they submit the write.

This module manages that intent window using Redis:

  CLAIM  — user signals "I am editing this row"
           Redis key set with 30s TTL, WebSocket broadcast to workspace
  RENEW  — heartbeat every ~15s while the edit dialog is open
           Resets the 30s TTL; no broadcast (reduces noise)
  RELEASE — user submits or cancels
           Redis key deleted, WebSocket broadcast to workspace

Key schema
----------
  calyphant:row_lock:{connection_id}:{schema}:{table}:{pk_value}
  Value: JSON {"user_id": str, "user_email": str, "claimed_at": ISO-8601}
  TTL: ROW_LOCK_TTL_SECONDS (30s) — auto-expires if client dies

Integration points
------------------
  1. editor.update_row() calls get_row_lock() to enrich RowLockConflict
     with the holder's identity rather than a generic message.
  2. tables/router.py exposes three REST endpoints:
       POST   /tables/{connection_id}/{table}/rows/{pk}/lock   — claim
       DELETE /tables/{connection_id}/{table}/rows/{pk}/lock   — release
       GET    /tables/{connection_id}/{table}/locks            — list all active

  3. Both claim and release broadcast a "row_lock_changed" event to the
     workspace WebSocket channel so connected clients can update their UI
     in real time without polling.

Fail-open design
----------------
Every public function catches Redis exceptions and logs a warning rather
than raising. A Redis outage must never block a row edit — the DB-level
RowLockConflict remains the authoritative safety net.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from loguru import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROW_LOCK_TTL_SECONDS = 30          # Key auto-expires after this many seconds
ROW_LOCK_RENEW_THRESHOLD = 15      # Client should heartbeat before this
_KEY_PREFIX = "calyphant:row_lock"


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------

def _lock_key(
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
) -> str:
    # Normalise pk_value to a safe string — strip whitespace, truncate
    pk_str = str(pk_value).strip()[:200]
    return f"{_KEY_PREFIX}:{connection_id}:{schema}:{table}:{pk_str}"


def _table_pattern(
    connection_id: UUID | str,
    schema: str,
    table: str,
) -> str:
    """Redis SCAN pattern for all locks on a single table."""
    return f"{_KEY_PREFIX}:{connection_id}:{schema}:{table}:*"


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

async def _get_redis():
    """Return the module-level Redis client. Returns None on failure."""
    try:
        from core.db import get_redis
        return await get_redis()
    except Exception as exc:
        logger.warning(f"Row presence: Redis unavailable — {exc}")
        return None


# ---------------------------------------------------------------------------
# Broadcast helper
# ---------------------------------------------------------------------------

async def _broadcast_lock_event(
    workspace_id: UUID,
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
    event: str,               # "row_lock_claimed" | "row_lock_released"
    holder: dict | None,      # None on release
) -> None:
    """
    Broadcast a row lock change to all WebSocket clients in the workspace.
    Fire-and-forget — failure is logged but never propagated.
    """
    try:
        from domains.teams.service import ws_manager
        await ws_manager.broadcast(
            workspace_id,
            {
                "event": event,
                "connection_id": str(connection_id),
                "schema": schema,
                "table": table,
                "pk_value": str(pk_value),
                "holder": holder,
            },
        )
    except Exception as exc:
        logger.warning(f"Row presence: broadcast failed for {event} — {exc}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def claim_row_lock(
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
    user_id: UUID | str,
    user_email: str,
    workspace_id: UUID,
) -> dict[str, Any]:
    """
    Register editing intent for a row.

    If the row is already claimed by a different user, returns a dict
    with "conflict": True and the current holder's info. The caller
    (router) can return this as a 409 so the UI can display who is
    editing before the user even tries to save.

    If the row is claimed by the *same* user (e.g. re-opening the edit
    dialog), the TTL is refreshed and the claim is returned as-is.

    On Redis failure: returns {"conflict": False, "redis_available": False}
    so the UI proceeds without presence tracking.

    Returns
    -------
    {
        "conflict": bool,
        "holder": {user_id, user_email, claimed_at} | None,
        "lock_key": str,
        "ttl": int,
        "redis_available": bool,
    }
    """
    redis = await _get_redis()
    if redis is None:
        return {
            "conflict": False,
            "holder": None,
            "lock_key": "",
            "ttl": 0,
            "redis_available": False,
        }

    key = _lock_key(connection_id, schema, table, pk_value)

    try:
        # Check for an existing claim
        raw = await redis.get(key)
        if raw:
            existing: dict = json.loads(raw)
            # Same user — refresh TTL silently
            if str(existing.get("user_id")) == str(user_id):
                await redis.expire(key, ROW_LOCK_TTL_SECONDS)
                return {
                    "conflict": False,
                    "holder": existing,
                    "lock_key": key,
                    "ttl": ROW_LOCK_TTL_SECONDS,
                    "redis_available": True,
                }
            # Different user — return conflict without overwriting
            return {
                "conflict": True,
                "holder": existing,
                "lock_key": key,
                "ttl": await redis.ttl(key),
                "redis_available": True,
            }

        # No existing claim — set it
        holder = {
            "user_id": str(user_id),
            "user_email": user_email,
            "claimed_at": datetime.now(timezone.utc).isoformat(),
        }
        await redis.setex(key, ROW_LOCK_TTL_SECONDS, json.dumps(holder))

        # Broadcast to workspace
        await _broadcast_lock_event(
            workspace_id=workspace_id,
            connection_id=connection_id,
            schema=schema,
            table=table,
            pk_value=pk_value,
            event="row_lock_claimed",
            holder=holder,
        )

        return {
            "conflict": False,
            "holder": holder,
            "lock_key": key,
            "ttl": ROW_LOCK_TTL_SECONDS,
            "redis_available": True,
        }

    except Exception as exc:
        logger.warning(f"Row presence: claim_row_lock failed — {exc}")
        return {
            "conflict": False,
            "holder": None,
            "lock_key": key,
            "ttl": 0,
            "redis_available": False,
        }


async def renew_row_lock(
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
    user_id: UUID | str,
) -> bool:
    """
    Heartbeat — reset the TTL for an existing claim.

    Only renews if the key exists AND belongs to user_id.
    Returns True if renewed, False if key is gone or belongs to someone else.
    No broadcast — renewal is silent to avoid flooding WebSocket clients.
    """
    redis = await _get_redis()
    if redis is None:
        return False

    key = _lock_key(connection_id, schema, table, pk_value)
    try:
        raw = await redis.get(key)
        if not raw:
            return False
        existing: dict = json.loads(raw)
        if str(existing.get("user_id")) != str(user_id):
            return False
        await redis.expire(key, ROW_LOCK_TTL_SECONDS)
        return True
    except Exception as exc:
        logger.warning(f"Row presence: renew_row_lock failed — {exc}")
        return False


async def release_row_lock(
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
    user_id: UUID | str,
    workspace_id: UUID,
) -> bool:
    """
    Release editing intent for a row.

    Only deletes the key if it belongs to user_id — prevents a user from
    forcibly clearing another user's claim.
    Returns True if released, False if key was absent or owned by someone else.
    Broadcasts "row_lock_released" to the workspace on success.
    """
    redis = await _get_redis()
    if redis is None:
        return False

    key = _lock_key(connection_id, schema, table, pk_value)
    try:
        raw = await redis.get(key)
        if not raw:
            return False
        existing: dict = json.loads(raw)
        if str(existing.get("user_id")) != str(user_id):
            return False

        await redis.delete(key)

        await _broadcast_lock_event(
            workspace_id=workspace_id,
            connection_id=connection_id,
            schema=schema,
            table=table,
            pk_value=pk_value,
            event="row_lock_released",
            holder=None,
        )
        return True

    except Exception as exc:
        logger.warning(f"Row presence: release_row_lock failed — {exc}")
        return False


async def get_row_lock(
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
) -> dict | None:
    """
    Return the current holder for a row, or None if unclaimed.

    Called by editor.update_row() to enrich RowLockConflict with the
    holder's identity. Also used by the GET /locks endpoint.

    Returns None on Redis failure (fail-open).
    """
    redis = await _get_redis()
    if redis is None:
        return None

    key = _lock_key(connection_id, schema, table, pk_value)
    try:
        raw = await redis.get(key)
        if not raw:
            return None
        holder: dict = json.loads(raw)
        holder["ttl"] = await redis.ttl(key)
        return holder
    except Exception as exc:
        logger.warning(f"Row presence: get_row_lock failed — {exc}")
        return None


async def get_row_locks_batch(
    connection_id: str | UUID,
    schema: str,
    table: str,
    pk_values: list[Any],
) -> dict[str, dict]:
    """
    Batch fetches presence locks for multiple rows using a single Redis MGET.
    Eliminates N+1 Redis connection storms during bulk preview operations.
    """
    if not pk_values:
        return {}

    # 1. Grab the Redis tool first!
    redis = await _get_redis()
    
    # If the tool is broken or missing, just return an empty dictionary
    if redis is None:
        return {}

    str_pks = [str(pk) for pk in pk_values]
    
    # 2. Make the keys using your handy helper function
    keys = [_lock_key(connection_id, schema, table, pk) for pk in str_pks]

    # 3. Use 'redis' to get all the keys at once
    try:
        raw_results = await redis.mget(keys)
    except Exception as exc:
        logger.warning(f"Batch MGET failed for presence locks: {exc}")
        return {}

    # 4. Put the results together
    import json
    lock_holders: dict[str, dict] = {}
    
    for pk, raw_data in zip(str_pks, raw_results):
        if raw_data is not None:
            try:
                lock_holders[pk] = json.loads(raw_data)
            except (json.JSONDecodeError, TypeError):
                pass

    return lock_holders


async def list_table_locks(
    connection_id: UUID | str,
    schema: str,
    table: str,
) -> list[dict]:
    """
    Return all active row locks for a table.

    Uses Redis SCAN with a key pattern — never KEYS — so it is safe on
    production Redis instances with large key spaces.

    Returns an empty list on Redis failure (fail-open).
    """
    redis = await _get_redis()
    if redis is None:
        return []

    pattern = _table_pattern(connection_id, schema, table)
    locks: list[dict] = []

    try:
        async for key in redis.scan_iter(pattern):
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                holder: dict = json.loads(raw)
                # Extract pk_value from key suffix
                prefix = f"{_KEY_PREFIX}:{connection_id}:{schema}:{table}:"
                pk_value = key.decode() if isinstance(key, bytes) else key
                pk_value = pk_value.replace(prefix, "", 1)
                holder["pk_value"] = pk_value
                holder["ttl"] = await redis.ttl(key)
                locks.append(holder)
            except Exception:
                continue
        return locks

    except Exception as exc:
        logger.warning(f"Row presence: list_table_locks failed — {exc}")
        return []


async def force_release_row_lock(
    connection_id: UUID | str,
    schema: str,
    table: str,
    pk_value: Any,
    workspace_id: UUID,
) -> bool:
    """
    Admin-only: release a row lock regardless of who holds it.

    Used by superadmin endpoints or when a connection is deleted.
    Broadcasts release event to workspace.
    """
    redis = await _get_redis()
    if redis is None:
        return False

    key = _lock_key(connection_id, schema, table, pk_value)
    try:
        deleted = await redis.delete(key)
        if deleted:
            await _broadcast_lock_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                schema=schema,
                table=table,
                pk_value=pk_value,
                event="row_lock_released",
                holder=None,
            )
        return bool(deleted)
    except Exception as exc:
        logger.warning(f"Row presence: force_release_row_lock failed — {exc}")
        return False
# shared/pubsub.py
"""
Redis-backed pub/sub for WebSocket broadcast coordination.

Dynamic Subscription Matrix
---------------------------
Unlike legacy wildcard fanouts (psubscribe), this module maintains a strict,
in-memory registry of active workspaces relevant ONLY to this specific Uvicorn 
worker. 

When the first user for Workspace A connects to Worker 1, Worker 1 issues a 
precise SUBSCRIBE to Redis. When the last user disconnects, it issues an UNSUBSCRIBE.
This prevents O(N) event parsing at scale.

Resilience
----------
If Redis restarts, `_run_subscriber` reads `_local_subscriptions` and instantly
re-subscribes to all active channels upon recovery. No signals are permanently lost.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID
from pydantic import BaseModel, Field

from loguru import logger

_CHANNEL_PREFIX = "calyphant:ws:"
_CONTROL_CHANNEL = "calyphant:ws_control"
_RECONNECT_DELAY = 2.0       # seconds between reconnect attempts
_MAX_RECONNECT_DELAY = 30.0  # cap on exponential backoff

# ---------------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------------

# The Source of Truth for what this specific worker process cares about.
# Required for surviving Redis connection drops.
_local_subscriptions: set[str] = set()

# Global reference to the active pubsub instance, enabling dynamic 
# subscribe/unsubscribe while the listen() loop is running.
_active_pubsub: Any = None 


def _channel(workspace_id: UUID) -> str:
    return f"{_CHANNEL_PREFIX}{workspace_id}"


class SignalEvent(BaseModel):
    """
    The only payload structure permitted on the Pub/Sub bus.
    State must not be transferred over Redis or WebSockets.
    
    UI BEHAVIOR:
    When the UI receives this payload over a WebSocket frame, it MUST trigger 
    a rate-limited HTTP GET request to the appropriate endpoint using the 
    entity_id to hydrate the updated state.
    """
    event: str = Field(..., max_length=64, description="Signal identifier e.g., 'schema_updated'")
    workspace_id: UUID
    entity_id: str | None = Field(default=None, max_length=64, description="Target entity to fetch via HTTP")
    role: str | None = Field(default=None, max_length=32, description="Target context if needed")


# ---------------------------------------------------------------------------
# Dynamic Subscription Controls
# ---------------------------------------------------------------------------

async def subscribe_workspace(workspace_id: UUID) -> None:
    """Invoked by ws_manager when the FIRST client for a workspace connects."""
    channel = _channel(workspace_id)
    _local_subscriptions.add(channel)
    
    if _active_pubsub:
        try:
            await _active_pubsub.subscribe(channel)
            logger.debug(f"PubSub: Dynamically subscribed to {channel}")
        except Exception as exc:
            logger.warning(f"PubSub: Failed to subscribe dynamically: {exc}")


async def unsubscribe_workspace(workspace_id: UUID) -> None:
    """Invoked by ws_manager when the LAST client for a workspace disconnects."""
    channel = _channel(workspace_id)
    _local_subscriptions.discard(channel)
    
    if _active_pubsub:
        try:
            await _active_pubsub.unsubscribe(channel)
            logger.debug(f"PubSub: Dynamically unsubscribed from {channel}")
        except Exception as exc:
            logger.warning(f"PubSub: Failed to unsubscribe dynamically: {exc}")


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

async def publish(workspace_id: UUID, event: SignalEvent) -> None:
    """
    Publish a workspace signal. Local delivery bypasses Redis entirely.
    Redis handles multi-worker routing.
    """
    from domains.teams.service import ws_manager

    await ws_manager.broadcast_local(workspace_id, event)

    try:
        from core.db import get_pubsub_redis
        redis = await get_pubsub_redis()
        payload = event.model_dump_json() 
        await redis.publish(_channel(workspace_id), payload)
    except Exception as exc:
        logger.debug(f"PubSub: Redis publish failed (local broadcast only): {exc}")


# ---------------------------------------------------------------------------
# Subscriber loop
# ---------------------------------------------------------------------------

async def start_pubsub_listener() -> None:
    delay = _RECONNECT_DELAY

    while True:
        try:
            await _run_subscriber()
            break
        except asyncio.CancelledError:
            logger.info("PubSub: listener task cancelled — shutting down")
            break
        except Exception as exc:
            logger.warning(
                f"PubSub: subscriber error ({exc}). "
                f"Reconnecting in {delay:.0f}s..."
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_DELAY)
        else:
            delay = _RECONNECT_DELAY


async def _run_subscriber() -> None:
    global _active_pubsub
    from core.db import get_pubsub_redis
    from domains.teams.service import ws_manager

    try:
        redis = await get_pubsub_redis()
    except RuntimeError:
        logger.warning("PubSub: Redis unavailable — retrying multi-worker broadcast layer.")
        await asyncio.sleep(_RECONNECT_DELAY)
        return

    pubsub = redis.pubsub()
    _active_pubsub = pubsub

    # 1. Boot up: Combine explicit workspace channels with a persistent control channel.
    # The control channel ensures pubsub.listen() does not immediately exit if _local_subscriptions is empty.
    channels_to_watch = list(_local_subscriptions) + [_CONTROL_CHANNEL]
    await pubsub.subscribe(*channels_to_watch)
    logger.info(f"PubSub: Subscribed to {len(channels_to_watch)} channels (including control)")

    try:
        async for message in pubsub.listen():
            if message is None:
                continue

            msg_type = message.get("type")
            # 2. Shift from pattern matching (pmessage) to strict channel targeting (message)
            if msg_type != "message":
                continue

            channel: bytes | str = message.get("channel", b"")
            if isinstance(channel, bytes):
                channel = channel.decode()

            if channel == _CONTROL_CHANNEL:
                continue

            if not channel.startswith(_CHANNEL_PREFIX):
                continue

            workspace_id_str = channel[len(_CHANNEL_PREFIX):]
            try:
                workspace_id = UUID(workspace_id_str)
            except ValueError:
                continue

            raw = message.get("data", b"")
            if isinstance(raw, bytes):
                raw = raw.decode()

            try:
                raw_dict = json.loads(raw)
                event = SignalEvent(**raw_dict)
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                logger.warning(f"PubSub: dropped invalid signal on {channel}: {exc}")
                continue

            await ws_manager.broadcast_local(workspace_id, event)

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise RuntimeError(f"PubSub subscriber loop failed: {exc}") from exc
    finally:
        _active_pubsub = None
        try:
            await pubsub.aclose()
        except Exception:
            pass
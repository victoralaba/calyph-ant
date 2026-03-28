# shared/pubsub.py
"""
Redis-backed pub/sub for WebSocket broadcast coordination.

Problem
-------
`ws_manager` in domains/teams/service.py is an in-memory dict. With
multiple uvicorn workers (or multiple replicas), each process has its
own dict. A schema change event published by worker-1 never reaches
WebSocket clients connected to worker-2.

Solution
--------
Every event goes through Redis pub/sub:

  Publisher  (any worker): publish(workspace_id, event)
             → PUBLISH calyphant:ws:{workspace_id} <json>

  Subscriber (every worker): a background task runs listen_and_broadcast()
             → SUBSCRIBE calyphant:ws:*
             → on message: fan out to local ws_manager connections

This means every worker receives every event and delivers it to whichever
clients happen to be connected to that worker. No event is ever lost as
long as at least one subscriber is alive when the publish happens (Redis
pub/sub is fire-and-forget — for durability under restarts, use Redis
Streams, but pub/sub is sufficient for ephemeral UI notifications).

Graceful degradation
--------------------
If Redis is unavailable, publish() falls back to direct local broadcast
via ws_manager. This means single-process deployments work without Redis,
and multi-process deployments degrade to per-process delivery rather than
crashing.

Usage
-----
In main.py lifespan:

    from shared.pubsub import start_pubsub_listener
    listener_task = asyncio.create_task(start_pubsub_listener())

In domain code, replace direct ws_manager.broadcast() with:

    from shared.pubsub import publish
    await publish(workspace_id, event_dict)

The publish() function handles both Redis fanout (multi-worker) and
direct local broadcast fallback (single-worker / Redis down).
"""

from __future__ import annotations

import asyncio
import json
from uuid import UUID

from loguru import logger


_CHANNEL_PREFIX = "calyphant:ws:"
_RECONNECT_DELAY = 2.0       # seconds between reconnect attempts
_MAX_RECONNECT_DELAY = 30.0  # cap on exponential backoff


def _channel(workspace_id: UUID) -> str:
    return f"{_CHANNEL_PREFIX}{workspace_id}"


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

async def publish(workspace_id: UUID, event: dict) -> None:
    """
    Publish a workspace event.

    1. Publishes to Redis so all workers receive it via their subscriber.
    2. Also broadcasts directly to local ws_manager connections so the
       originating worker doesn't wait for the round-trip.

    Falls back to local-only broadcast if Redis is unavailable.
    """
    from domains.teams.service import ws_manager

    # Always deliver locally — the originating worker should not wait for
    # its own Redis message to arrive via the subscriber loop.
    await ws_manager.broadcast_local(workspace_id, event)

    # Publish to Redis for other workers
    try:
        from core.db import get_pubsub_redis
        redis = await get_pubsub_redis()
        payload = json.dumps(event, default=str)
        await redis.publish(_channel(workspace_id), payload)
    except Exception as exc:
        # Redis unavailable — local broadcast above already handled it.
        logger.debug(f"PubSub: Redis publish failed (local broadcast only): {exc}")


# ---------------------------------------------------------------------------
# Subscriber loop
# ---------------------------------------------------------------------------

async def start_pubsub_listener() -> None:
    """
    Long-running background task that subscribes to all workspace channels
    and fans out received messages to local WebSocket connections.

    Reconnects automatically with exponential backoff on failure.
    Should be started once per worker process during app lifespan.
    """
    delay = _RECONNECT_DELAY

    while True:
        try:
            await _run_subscriber()
            # _run_subscriber only returns on graceful shutdown
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
    """
    Inner subscriber loop. Subscribes to the wildcard pattern and
    dispatches events to ws_manager.broadcast_local().

    Uses PSUBSCRIBE (pattern subscribe) so a single subscription covers
    all workspace channels without needing to know workspace IDs upfront.
    """
    from core.db import get_pubsub_redis
    from domains.teams.service import ws_manager

    try:
        redis = await get_pubsub_redis()
    except RuntimeError:
        # Redis not available — log once and return so the caller retries
        logger.warning(
            "PubSub: Redis unavailable — subscriber will retry. "
            "Multi-worker WS broadcast is disabled until Redis reconnects."
        )
        await asyncio.sleep(_RECONNECT_DELAY)
        return

    # Create a dedicated pubsub object on this client
    pubsub = redis.pubsub()
    pattern = f"{_CHANNEL_PREFIX}*"
    await pubsub.psubscribe(pattern)
    logger.info(f"PubSub: subscribed to pattern '{pattern}'")

    try:
        async for message in pubsub.listen():
            if message is None:
                continue

            msg_type = message.get("type")

            # psubscribe confirmation — not a real message
            if msg_type == "psubscribe":
                continue

            if msg_type != "pmessage":
                continue

            channel: bytes | str = message.get("channel", b"")
            if isinstance(channel, bytes):
                channel = channel.decode()

            # Extract workspace_id from channel name
            if not channel.startswith(_CHANNEL_PREFIX):
                continue

            workspace_id_str = channel[len(_CHANNEL_PREFIX):]
            try:
                workspace_id = UUID(workspace_id_str)
            except ValueError:
                logger.warning(f"PubSub: invalid workspace_id in channel: {channel}")
                continue

            raw = message.get("data", b"")
            if isinstance(raw, bytes):
                raw = raw.decode()

            try:
                event = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"PubSub: could not decode message on {channel}")
                continue

            # Fan out to WebSocket clients connected to this worker
            # broadcast_local skips Redis to avoid infinite loop
            await ws_manager.broadcast_local(workspace_id, event)

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise RuntimeError(f"PubSub subscriber loop failed: {exc}") from exc
    finally:
        try:
            await pubsub.punsubscribe(pattern)
            await pubsub.aclose()
        except Exception:
            pass

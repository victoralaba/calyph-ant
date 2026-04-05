# shared/pubsub.py
"""
Redis-backed pub/sub for WebSocket broadcast coordination with local 
conveyor-belt batching and zero-I/O ingress throttling.

Architecture Defense
--------------------
1. Ingress: `WSMemoryThrottle` protects the event loop from malicious WS floods.
2. Egress: `_publish_buffer` deduplicates rapid DB mutations (e.g., 500 schema 
   updates in 1s become 4 batched signals/sec).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from uuid import UUID
from pydantic import BaseModel, Field

from loguru import logger

_CHANNEL_PREFIX = "calyphant:ws:"
_CONTROL_CHANNEL = "calyphant:ws_control"
_RECONNECT_DELAY = 2.0
_MAX_RECONNECT_DELAY = 30.0

# ---------------------------------------------------------------------------
# State Management & Buffers
# ---------------------------------------------------------------------------

_local_subscriptions: set[str] = set()
_active_pubsub: Any = None 

# The Egress Buffer: Prevents network stampedes by deduplicating signals in RAM
_publish_buffer: dict[str, SignalEvent] = {}
_buffer_lock = asyncio.Lock()
_FLUSH_INTERVAL = 0.250  # 250ms batching window


def _channel(workspace_id: UUID) -> str:
    return f"{_CHANNEL_PREFIX}{workspace_id}"


# ---------------------------------------------------------------------------
# Zero-I/O Ingress Throttle
# ---------------------------------------------------------------------------

class WSMemoryThrottle:
    """
    Lock-protected Token Bucket algorithm running entirely in Uvicorn RAM.
    Bypasses Redis to ensure malicious actors cannot exhaust network I/O.
    Includes amortized garbage collection to prevent memory leaks.
    """
    def __init__(self, capacity: int, fill_rate: float):
        self.capacity = capacity
        self.fill_rate = fill_rate
        # key -> (current_tokens, last_update_monotonic_time)
        self.buckets: dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()
        self._sweep_counter = 0
        self._SWEEP_THRESHOLD = 1000  # Run GC every 1,000 calls
        self._STALE_SECONDS = 60.0    # Drop keys inactive for 60s

    async def consume(self, key: str, tokens: int = 1) -> bool:
        now = time.monotonic()
        async with self._lock:
            # 1. Amortized Garbage Collection
            self._sweep_counter += 1
            if self._sweep_counter > self._SWEEP_THRESHOLD:
                self._sweep(now)
                self._sweep_counter = 0

            # 2. Token Math
            current_tokens, last_update = self.buckets.get(key, (self.capacity, now))
            elapsed = now - last_update
            current_tokens = min(self.capacity, current_tokens + (elapsed * self.fill_rate))
            
            # 3. Consumption Decision
            if current_tokens >= tokens:
                self.buckets[key] = (current_tokens - tokens, now)
                return True
            
            self.buckets[key] = (current_tokens, now)
            return False

    def _sweep(self, now: float) -> None:
        """
        Executes synchronously under the async lock. 
        Deletes buckets that are completely full and inactive.
        """
        stale_keys = []
        for k, (tokens, last_update) in self.buckets.items():
            # If the bucket has been untouched long enough to fully replenish, 
            # and it has been at least 60 seconds since the last touch, drop it.
            if (now - last_update > self._STALE_SECONDS) and (tokens >= self.capacity):
                stale_keys.append(k)
                
        for k in stale_keys:
            del self.buckets[k]

# Global Throttle Instance: 50 burst frames, replenishing at 5 frames/sec.
ws_ingress_throttle = WSMemoryThrottle(capacity=50, fill_rate=5.0)


# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------

class SignalEvent(BaseModel):
    """
    The only payload structure permitted on the Pub/Sub bus.
    
    FRONTEND UI BEHAVIOR MANDATE:
    -----------------------------
    The UI MUST NOT fire an HTTP request immediately upon receiving this frame.
    The Svelte client MUST wrap its hydration HTTP GET requests in a trailing 
    debounce function of at least 250ms to prevent Postgres pool exhaustion.
    If the UI fails to do this, the backend will return HTTP 429 Too Many Requests.
    """
    event: str = Field(..., max_length=64)
    workspace_id: UUID
    entity_id: str | None = Field(default=None, max_length=64)
    role: str | None = Field(default=None, max_length=32)


# ---------------------------------------------------------------------------
# Dynamic Subscription Controls
# ---------------------------------------------------------------------------

async def subscribe_workspace(workspace_id: UUID) -> None:
    channel = _channel(workspace_id)
    _local_subscriptions.add(channel)
    if _active_pubsub:
        try:
            await _active_pubsub.subscribe(channel)
        except Exception as exc:
            logger.warning(f"PubSub: Failed to subscribe dynamically: {exc}")


async def unsubscribe_workspace(workspace_id: UUID) -> None:
    channel = _channel(workspace_id)
    _local_subscriptions.discard(channel)
    if _active_pubsub:
        try:
            await _active_pubsub.unsubscribe(channel)
        except Exception as exc:
            logger.warning(f"PubSub: Failed to unsubscribe dynamically: {exc}")


# ---------------------------------------------------------------------------
# Publish (Buffered)
# ---------------------------------------------------------------------------

async def publish(workspace_id: UUID, event: SignalEvent) -> None:
    """
    Dumps the event into the local memory buffer instead of hitting the network.
    Rapid identical events overwrite previous ones (deduplication).
    """
    # Deduplication key ensures 500 identical updates become exactly 1
    dedupe_key = f"{workspace_id}:{event.event}:{event.entity_id or 'none'}"
    async with _buffer_lock:
        _publish_buffer[dedupe_key] = event


async def _flush_buffer_loop() -> None:
    """Background task that sweeps the buffer and broadcasts every 250ms."""
    from domains.teams.service import ws_manager
    from core.db import get_pubsub_redis

    while True:
        try:
            await asyncio.sleep(_FLUSH_INTERVAL)
            
            async with _buffer_lock:
                if not _publish_buffer:
                    continue
                events_to_flush = list(_publish_buffer.values())
                _publish_buffer.clear()

            # Attempt Redis connection once per batch, not per event
            try:
                redis = await get_pubsub_redis()
            except Exception as exc:
                logger.warning(f"PubSub: Redis unavailable during flush: {exc}")
                redis = None

            for event in events_to_flush:
                # 1. Zero-latency local broadcast
                await ws_manager.broadcast_local(event.workspace_id, event)
                
                # 2. Redis broadcast to other workers
                if redis:
                    try:
                        payload = event.model_dump_json() 
                        await redis.publish(_channel(event.workspace_id), payload)
                    except Exception as exc:
                        logger.debug(f"PubSub: Redis batch publish failed: {exc}")
                        
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"PubSub: Buffer flusher crashed: {exc}")


# ---------------------------------------------------------------------------
# Subscriber loop
# ---------------------------------------------------------------------------

async def start_pubsub_listener() -> None:
    # Boot the flusher alongside the listener
    asyncio.create_task(_flush_buffer_loop())
    
    delay = _RECONNECT_DELAY
    while True:
        try:
            await _run_subscriber()
            break
        except asyncio.CancelledError:
            logger.info("PubSub: listener task cancelled")
            break
        except Exception as exc:
            logger.warning(f"PubSub: subscriber error ({exc}). Reconnecting...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_DELAY)
        else:
            delay = _RECONNECT_DELAY


async def _run_subscriber() -> None:
    global _active_pubsub
    from core.db import get_pubsub_redis
    from domains.teams.service import ws_manager

    redis = await get_pubsub_redis()
    pubsub = redis.pubsub()
    _active_pubsub = pubsub

    channels_to_watch = list(_local_subscriptions) + [_CONTROL_CHANNEL]
    await pubsub.subscribe(*channels_to_watch)

    try:
        async for message in pubsub.listen():
            if message is None or message.get("type") != "message":
                continue

            channel = message.get("channel", b"").decode() if isinstance(message.get("channel"), bytes) else message.get("channel", "")
            if channel == _CONTROL_CHANNEL or not channel.startswith(_CHANNEL_PREFIX):
                continue

            try:
                workspace_id = UUID(channel[len(_CHANNEL_PREFIX):])
                raw = message.get("data", b"").decode() if isinstance(message.get("data"), bytes) else message.get("data", "")
                
                raw_dict = json.loads(raw)
                event = SignalEvent(**raw_dict)
                await ws_manager.broadcast_local(workspace_id, event)
            except (ValueError, json.JSONDecodeError, TypeError) as exc:
                logger.warning(f"PubSub: dropped invalid inbound signal: {exc}")
                continue

    finally:
        _active_pubsub = None
        await pubsub.aclose()
# core/db.py
"""
Database layer.

Connection strategies controlled by POOLING_ENABLED:

  POOLING_ENABLED=False (development):
    - SQLAlchemy NullPool — a new connection per session, closed immediately
      after the session context exits. Safe for single-process dev.

  POOLING_ENABLED=True (production):
    - SQLAlchemy AsyncAdaptedQueuePool backed by asyncpg.
    - Pool sized by POOL_MIN_SIZE / POOL_MAX_SIZE.
    - Compatible with PgBouncer in transaction mode (no prepared statements
      across pool boundaries — use use_native_enum=False and
      execution_options(no_parameters=True) where needed).

Redis pub/sub
-------------
A dedicated Redis client (_redis_pubsub) is kept separate from the
request-serving client (_redis) so that long-running SUBSCRIBE calls
never block the main connection pool. The pub/sub client is used
exclusively by the WebSocket broadcast layer in shared/pubsub.py.

Raw asyncpg connections
-----------------------
get_raw_conn() yields a raw asyncpg connection from the asyncpg-level
pool when pooling is enabled, or opens a direct connection otherwise.
Domain routers use this for COPY, streaming cursors, and target-DB
operations that bypass SQLAlchemy.

Session factory
---------------
_session_factory is set once during init_db() and is safe to import
at module level in other domains. Always prefer using it as an async
context manager:

    async with _session_factory() as db:
        ...

Never hold a session open across I/O that is not DB-related (e.g. do
not hold a session while waiting on a WebSocket message).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any
from uuid import UUID

import asyncpg
from asyncpg.pool import PoolConnectionProxy
import redis.asyncio as aioredis
from cryptography.fernet import Fernet, MultiFernet
from loguru import logger
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import settings


# ---------------------------------------------------------------------------
# Module-level singletons — set during lifespan startup
# ---------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None
_async_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_sync_engine: Any | None = None

# Two Redis clients: one for general use, one exclusively for pub/sub
_redis: aioredis.Redis | None = None
_redis_pubsub: aioredis.Redis | None = None

_fernet: MultiFernet | None = None


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """
    Initialise database connections.
    Called once at application startup via lifespan handler in main.py.
    """
    global _pool, _async_engine, _session_factory, _fernet

    # Initialize MultiFernet with the list of keys. 
    # Index 0 is the primary key for encryption. All keys try to decrypt.
    fernet_instances = [Fernet(k.encode()) for k in settings.ENCRYPTION_KEYS]
    _fernet = MultiFernet(fernet_instances)

    db_url = (
        settings.DATABASE_URL
        .replace("postgres://", "postgresql+asyncpg://")
        .replace("postgresql://", "postgresql+asyncpg://")
    )

    if settings.POOLING_ENABLED:
        logger.info(
            f"DB: pooling enabled "
            f"(min={settings.POOL_MIN_SIZE} max={settings.POOL_MAX_SIZE})"
        )

        _pool = await asyncpg.create_pool(
            dsn=settings.DATABASE_URL,
            min_size=settings.POOL_MIN_SIZE,
            max_size=settings.POOL_MAX_SIZE,
            max_inactive_connection_lifetime=settings.POOL_MAX_INACTIVE_CONNECTION_LIFETIME,
            command_timeout=60,
        )

        engine_kwargs: dict = {
            "pool_size": settings.POOL_MIN_SIZE,
            "max_overflow": settings.POOL_MAX_SIZE - settings.POOL_MIN_SIZE,
            "pool_pre_ping": True,
            "pool_recycle": 1800,
        }
    else:
        logger.info("DB: pooling disabled (NullPool — dev mode)")
        _pool = None
        from sqlalchemy.pool import NullPool
        engine_kwargs = {"poolclass": NullPool}

    _async_engine = create_async_engine(
        db_url,
        echo=settings.DEBUG,
        **engine_kwargs,
    )

    _session_factory = async_sessionmaker(
        bind=_async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    logger.info("DB: session factory ready")


async def init_redis() -> None:
    """
    Initialise two Redis clients:
      _redis        — general purpose (rate limiting, caching, queues)
      _redis_pubsub — exclusive to pub/sub subscriptions

    Non-fatal if Redis is unavailable — the app degrades gracefully.
    """
    global _redis, _redis_pubsub

    async def _connect(db_index: int, label: str) -> aioredis.Redis | None:
        try:
            client = aioredis.from_url(
                settings.REDIS_URL,
                db=db_index,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                decode_responses=False,
            )
            await client.ping()
            logger.info(f"Redis: {label} client connected (db={db_index})")
            return client
        except Exception as exc:
            logger.warning(
                f"Redis: {label} client unavailable ({exc}). "
                "Features backed by Redis will degrade gracefully."
            )
            return None

    _redis = await _connect(0, "general")
    _redis_pubsub = await _connect(1, "pubsub")


async def close_db() -> None:
    """Graceful shutdown — close pool, engine, and Redis clients."""
    global _pool, _async_engine, _sync_engine, _redis, _redis_pubsub

    if _pool:
        await _pool.close()
        logger.info("DB: asyncpg pool closed")

    if _async_engine:
        await _async_engine.dispose()
        logger.info("DB: SQLAlchemy engine disposed")

    if _sync_engine:
        _sync_engine.dispose()
        logger.info("DB: Sync SQLAlchemy engine disposed")

    if _redis:
        await _redis.aclose()
        logger.info("Redis: general client closed")

    if _redis_pubsub:
        await _redis_pubsub.aclose()
        logger.info("Redis: pubsub client closed")


# ---------------------------------------------------------------------------
# FastAPI / general dependencies
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency — yields an AsyncSession scoped to one HTTP request.

    Commits on success, rolls back on any exception, always closes.

    IMPORTANT: Do NOT use this as a dependency on WebSocket endpoints.
    WebSocket handlers must manage their own short-lived sessions via
    _session_factory() so the session is never held open during the
    long-lived event loop.

    Usage:
        async def my_route(db: AsyncSession = Depends(get_db)):
    """
    if not _session_factory:
        raise RuntimeError("Database not initialised. Call init_db() at startup.")

    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager version of get_db for use outside FastAPI
    dependency injection (e.g. background tasks, WebSocket auth blocks,
    Celery tasks).

    Usage:
        async with get_db_context() as db:
            result = await db.execute(...)
    """
    if not _session_factory:
        raise RuntimeError("Database not initialised. Call init_db() at startup.")

    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_raw_conn() -> AsyncGenerator[asyncpg.Connection | PoolConnectionProxy, None]:
    """
    FastAPI dependency — yields a raw asyncpg connection.

    Use for: streaming cursors, COPY protocol, target-DB operations.

    If pooling is enabled: acquires from pool, releases on exit.
    If pooling is disabled: opens a direct connection, closes on exit.
    """
    if _pool:
        async with _pool.acquire() as conn:
            yield conn
    else:
        conn = await asyncpg.connect(dsn=settings.DATABASE_URL)
        try:
            yield conn
        finally:
            await conn.close()


async def get_redis() -> aioredis.Redis:
    """
    Returns the general-purpose Redis client.
    Raises RuntimeError if Redis is not available.
    """
    if not _redis:
        raise RuntimeError(
            "Redis not initialised or unavailable. "
            "Check REDIS_URL and ensure Redis is running."
        )
    return _redis


async def get_pubsub_redis() -> aioredis.Redis:
    """
    Returns the Redis client dedicated to pub/sub.
    Raises RuntimeError if Redis is not available.
    """
    if not _redis_pubsub:
        raise RuntimeError(
            "Redis pub/sub client not available. "
            "Check REDIS_URL and ensure Redis is running."
        )
    return _redis_pubsub


# ---------------------------------------------------------------------------
# Connection URL helper
# ---------------------------------------------------------------------------

async def get_connection_url(
    db: AsyncSession,
    connection_id: UUID,
    workspace_id: UUID,
) -> str | None:
    """
    Fetch and decrypt a stored connection URL.
    Returns None if not found or not owned by the given workspace.
    """
    from sqlalchemy import select
    from domains.connections.models import Connection

    result = await db.execute(
        select(Connection.encrypted_url).where(
            Connection.id == connection_id,
            Connection.workspace_id == workspace_id,
            Connection.is_active == True,  # noqa: E712
        )
    )
    encrypted = result.scalar_one_or_none()
    if not encrypted:
        return None
    return decrypt_secret(encrypted)


# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------


def encrypt_secret(plaintext: str) -> str:
    """Encrypt a string using the Primary (first) Fernet key via MultiFernet."""
    if not _fernet:
        raise RuntimeError("MultiFernet not initialised. Call init_db() at startup.")
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    """Decrypt a Fernet-encrypted string. MultiFernet tries all configured keys automatically."""
    if not _fernet:
        raise RuntimeError("MultiFernet not initialised. Call init_db() at startup.")
    return _fernet.decrypt(ciphertext.encode()).decode()


# ---------------------------------------------------------------------------
# Sync engine (Alembic + introspection only — never in async paths)
# ---------------------------------------------------------------------------

def get_sync_engine():
    """
    Returns a synchronous SQLAlchemy engine.
    Used ONLY by Alembic migrations and schema introspection threads.
    Never call this in async request handlers.

    Uses NullPool so each call gets a fresh connection that is closed
    immediately. The engine instance itself is cached to eliminate 
    dialect initialization CPU overhead.
    """
    global _sync_engine
    if _sync_engine is not None:
        return _sync_engine

    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    _sync_engine = create_engine(
        settings.get_sync_url(),
        poolclass=NullPool,
    )
    return _sync_engine
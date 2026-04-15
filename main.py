# main.py
"""
FastAPI application factory.

Startup sequence (lifespan):
  1. Init telemetry (logging, Sentry, PostHog)
  2. Init database (pool or NullPool per POOLING_ENABLED)
  3. Init Redis (general + pubsub clients)
  4. Start Redis pub/sub listener (cross-worker WS broadcast)
  5. Register middleware
  6. Register exception handlers
  7. Mount all domain routers

Shutdown sequence:
  1. Cancel pub/sub listener task
  2. Close DB pool / engine
  3. Close Redis connections
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from loguru import logger

from core.config import settings
from core.db import init_db, init_redis, close_db
from core.exceptions import register_exception_handlers
from core.middleware import init_sentry, register_middleware
from shared.telemetry import init_telemetry
from domains.query.service import pool_manager


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- Startup ----
    init_telemetry(is_production=settings.is_production)
    init_sentry()

    await init_db()
    await init_redis()

    # Seed the platform extension catalogue (idempotent — skips existing entries)
    try:
        import core.db as _core_db
        from domains.platform.extension_catalogue import seed_catalogue
        if _core_db._session_factory:
            async with _core_db._session_factory() as db:
                n = await seed_catalogue(db)
                if n:
                    logger.info(f"Extension catalogue: seeded {n} new entries on startup")
    except Exception as _seed_exc:
        logger.warning(f"Extension catalogue seed failed (non-fatal): {_seed_exc}")

    # Start the Redis pub/sub listener for cross-worker WebSocket broadcast.
    # The task runs for the lifetime of the process. If Redis is unavailable
    # it retries automatically — see shared/pubsub.py for backoff logic.
    from shared.pubsub import start_pubsub_listener
    pubsub_task = asyncio.create_task(
        start_pubsub_listener(),
        name="redis-pubsub-listener",
    )

    logger.info(
        f"{settings.APP_NAME} started | "
        f"env={settings.APP_ENV} | "
        f"pooling={settings.POOLING_ENABLED} | "
        f"debug={settings.DEBUG}"
    )

    yield

    # ---- Shutdown ----
    pubsub_task.cancel()
    try:
        await pubsub_task
    except asyncio.CancelledError:
        pass

    logger.info("SIGTERM received. Initiating graceful shutdown of all PostgreSQL workspace pools...")
    await pool_manager.close_all()
    logger.info("All workspace pools successfully closed. Network boundary secured.")

    await close_db()
    logger.info(f"{settings.APP_NAME} shutdown complete")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        description="Universal PostgreSQL workspace API",
        version="0.1.0",
        docs_url="/docs" if not settings.is_production else None,
        redoc_url="/redoc" if not settings.is_production else None,
        openapi_url="/openapi.json" if not settings.is_production else None,
        lifespan=lifespan,
    )

    register_middleware(app)
    register_exception_handlers(app)

    from core.middleware import limiter
    app.state.limiter = limiter

    # ---------------------------------------------------------------------------
    # Routers
    # ---------------------------------------------------------------------------

    from core.auth import auth_router
    from domains.connections.router import router as connections_router
    from domains.schema.router.editor import router as schema_editor_router
    from domains.schema.router.intelligence import router as schema_intel_router
    from domains.migrations.router import router as migrations_router
    from domains.tables.router import router as tables_router
    from domains.query.router import router as query_router
    from domains.extensions.registry import router as extensions_router
    from domains.backups.engine import router as backups_router
    from domains.monitoring.collector import router as monitoring_router
    from domains.ai.providers import router as ai_router
    from domains.teams.service import router as teams_router
    from domains.users.service import router as users_router
    from domains.billing.flutterwave import router as billing_router
    from domains.notifications.service import router as notifications_router
    from domains.admin.service import router as admin_router

    prefix = "/api/v1"

    # Apply the prefix consistently to all core domain routes
    app.include_router(auth_router, prefix=prefix)
    app.include_router(connections_router, prefix=prefix)
    app.include_router(schema_editor_router, prefix=prefix)
    app.include_router(schema_intel_router, prefix=prefix)
    app.include_router(migrations_router, prefix=prefix)
    app.include_router(tables_router, prefix=prefix)
    app.include_router(query_router, prefix=prefix)
    app.include_router(extensions_router, prefix=prefix)
    app.include_router(backups_router, prefix=prefix)
    app.include_router(monitoring_router, prefix=prefix)
    app.include_router(ai_router, prefix=prefix)
    app.include_router(teams_router, prefix=prefix)
    app.include_router(users_router, prefix=prefix)
    app.include_router(billing_router, prefix=prefix)
    app.include_router(notifications_router, prefix=prefix)
    app.include_router(admin_router, prefix=prefix)

    # ---------------------------------------------------------------------------
    # System endpoints
    # ---------------------------------------------------------------------------

    @app.get("/health", tags=["system"])
    async def health_check():
        return {"status": "ok", "env": settings.APP_ENV}

    @app.get("/health/ready", tags=["system"])
    async def readiness_check():
        checks: dict[str, str] = {}
        overall = "ok"

        try:
            conn = await asyncpg.connect(dsn=settings.DATABASE_URL, timeout=3)
            await conn.fetchval("SELECT 1")
            await conn.close()
            checks["database"] = "ok"
        except Exception as exc:
            checks["database"] = f"error: {exc}"
            overall = "degraded"

        try:
            from core.db import get_redis
            redis = await get_redis()
            await redis.ping()
            checks["redis"] = "ok"
        except Exception as exc:
            checks["redis"] = f"error: {exc}"
            overall = "degraded"

        status_code = 200 if overall == "ok" else 503
        return JSONResponse(
            status_code=status_code,
            content={
                "status": overall,
                "checks": checks,
                "version": "0.1.0",
                "environment": settings.APP_ENV,
            },
        )

    @app.get("/metrics", tags=["system"], include_in_schema=False)
    async def prometheus_metrics():
        from shared.telemetry import get_metrics_response
        from fastapi.responses import Response
        content, content_type = get_metrics_response()
        return Response(content=content, media_type=content_type)

    return app


# ---------------------------------------------------------------------------
# Module-level app instance
# ---------------------------------------------------------------------------

app = create_app()

# core/middleware.py
"""
Middleware stack.

Applied in order (outermost first):
  1. Sentry — request tracing, error capture, performance monitoring
  2. Correlation ID — injects X-Request-ID into every request + response
  3. Request timing — adds X-Process-Time header
  4. CORS — configured from settings.CORS_ORIGINS
  5. Rate limiting — slowapi, per-user or per-IP, Redis-backed
 
Middleware is registered in main.py. This module only defines the
handlers and provides the limiter instance.
"""

from __future__ import annotations

import time
import uuid

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from typing import Callable
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from core.config import settings


# ---------------------------------------------------------------------------
# Rate limiter (module-level — imported by routers that need custom limits)
# ---------------------------------------------------------------------------

def _get_rate_limit_key(request: Request) -> str:
    """
    Rate limit key:
    - Authenticated users: keyed by user ID from JWT sub claim
    - Unauthenticated: keyed by IP address
    """
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth.split(" ", 1)[1]
        try:
            from jose import jwt as _jwt
            payload = _jwt.decode(
                token,
                settings.SECRET_KEY,
                algorithms=[settings.JWT_ALGORITHM],
                options={"verify_exp": False},
            )
            return f"user:{payload.get('sub', 'unknown')}"
        except Exception:
            pass
    return get_remote_address(request)


def _make_limiter() -> Limiter:
    """
    Create the SlowAPI limiter.
    Uses Redis when available; falls back to in-memory storage so the app
    boots cleanly even if Redis is not running (dev mode).
    """
    default_limits: list = [settings.RATE_LIMIT_DEFAULT] if settings.RATE_LIMIT_ENABLED else []
    storage_uri = settings.REDIS_URL if settings.RATE_LIMIT_ENABLED else "memory://"

    if storage_uri.startswith("redis"):
        try:
            import redis as _redis_sync
            r = _redis_sync.from_url(storage_uri, socket_connect_timeout=1)
            r.ping()
        except Exception:
            logger.warning(
                "Rate limiter: Redis unreachable — using in-memory storage. "
                "Per-process only; restart Redis to enable distributed limits."
            )
            storage_uri = "memory://"

    return Limiter(
        key_func=_get_rate_limit_key,
        default_limits=default_limits,
        storage_uri=storage_uri,
        enabled=settings.RATE_LIMIT_ENABLED,
    )


limiter = _make_limiter()


class MaxBodySizeMiddleware:
    """
    Pure ASGI middleware to sever connections if the payload exceeds the limit.
    This runs before FastAPI, Pydantic, or any body parsing begins.
    Protects the async event loop from CPU-bound starvation.
    """
    def __init__(self, app: ASGIApp, max_size: int = 1048576):  # 1MB Hard Limit
        self.app = app
        self.max_size = max_size

    async def __call__(self, scope: dict, receive: Callable, send: Callable) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # 1. Fast rejection based on Content-Length header
        headers = dict(scope.get("headers", []))
        if b"content-length" in headers:
            try:
                content_length = int(headers[b"content-length"])
                if content_length > self.max_size:
                    await self._send_413(send)
                    return
            except ValueError:
                pass

        # 2. Dynamic byte counting for chunked transfers
        total_size = 0

        async def receive_wrapper() -> dict:
            nonlocal total_size
            message = await receive()
            if message["type"] == "http.request":
                total_size += len(message.get("body", b""))
                if total_size > self.max_size:
                    # Abort the stream processing immediately
                    raise RuntimeError("PAYLOAD_TOO_LARGE")
            return message

        try:
            await self.app(scope, receive_wrapper, send)
        except RuntimeError as exc:
            if str(exc) == "PAYLOAD_TOO_LARGE":
                await self._send_413(send)
                return
            raise

    async def _send_413(self, send: Callable) -> None:
        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json")]
        })
        await send({
            "type": "http.response.body",
            "body": b'{"detail": "Payload Too Large. Ingress connection severed."}'
        })


# ---------------------------------------------------------------------------
# Correlation ID middleware
# ---------------------------------------------------------------------------

class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """
    Injects a unique X-Request-ID into every request.
    If the client sends one, it is used (trusted internal services).
    Otherwise a new UUID4 is generated.

    The correlation ID is available on request.state.correlation_id
    and is added to the Sentry scope for log correlation.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = (
            request.headers.get("X-Request-ID")
            or str(uuid.uuid4())
        )
        request.state.correlation_id = correlation_id

        # Bind to Sentry scope for this request
        if settings.sentry_enabled:
            import sentry_sdk
            with sentry_sdk.configure_scope() as scope:
                scope.set_tag("correlation_id", correlation_id)

        response = await call_next(request)
        response.headers["X-Request-ID"] = correlation_id
        return response


# ---------------------------------------------------------------------------
# Request timing middleware
# ---------------------------------------------------------------------------

class TimingMiddleware(BaseHTTPMiddleware):
    """Adds X-Process-Time (ms) header to every response."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        response.headers["X-Process-Time"] = f"{elapsed_ms}ms"
        return response


# ---------------------------------------------------------------------------
# Sentry initialisation
# ---------------------------------------------------------------------------

def init_sentry() -> None:
    """
    Initialise Sentry SDK.
    Called once at startup if SENTRY_DSN is set.
    FastAPI / Starlette integration is automatic via sentry-sdk.
    """
    if not settings.sentry_enabled:
        logger.info("Sentry: disabled (no SENTRY_DSN)")
        return

    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
    from sentry_sdk.integrations.redis import RedisIntegration
    from sentry_sdk.integrations.loguru import LoguruIntegration

    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.APP_ENV,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
        profiles_sample_rate=settings.SENTRY_PROFILES_SAMPLE_RATE,
        integrations=[
            FastApiIntegration(transaction_style="endpoint"),
            SqlalchemyIntegration(),
            RedisIntegration(),
            LoguruIntegration(),
        ],
        # Don't send PII — correlation IDs are enough for debugging
        send_default_pii=False,
    )
    logger.info(f"Sentry: initialised (env={settings.APP_ENV})")


# ---------------------------------------------------------------------------
# Registration helper — called from main.py
# ---------------------------------------------------------------------------

def register_middleware(app: FastAPI) -> None:
    """
    Register all middleware on the FastAPI app.
    Order matters — Starlette applies middleware in reverse registration order,
    so the last registered is the outermost (first to intercept requests).
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID", "X-Process-Time"],
    )

    app.add_middleware(TimingMiddleware)
    app.add_middleware(CorrelationIDMiddleware)
    
    app.add_middleware(SlowAPIMiddleware)
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore

    # THE GATES: Registered LAST so it executes FIRST at the ASGI boundary.
    app.add_middleware(MaxBodySizeMiddleware, max_size=1048576) # 1MB

    logger.info("Middleware: registered (CORS, CorrelationID, Timing, RateLimit, MaxBodySize)")
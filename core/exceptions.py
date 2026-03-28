# core/exceptions.py
"""
Exception hierarchy and FastAPI exception handlers.

All Calyphant-specific exceptions inherit from CalyphantError.
The handlers translate them into consistent JSON error responses
and capture unexpected errors to Sentry.

Error response shape (all errors):
  {
    "error": "human-readable message",
    "code": "MACHINE_READABLE_CODE",
    "detail": {...} | null,
    "request_id": "uuid"
  }
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class CalyphantError(Exception):
    """Base class for all Calyphant application errors."""
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    code: str = "INTERNAL_ERROR"

    def __init__(self, message: str, detail: Any = None):
        super().__init__(message)
        self.message = message
        self.detail = detail


class NotFoundError(CalyphantError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "NOT_FOUND"


class ConflictError(CalyphantError):
    status_code = status.HTTP_409_CONFLICT
    code = "CONFLICT"


class PermissionError(CalyphantError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "FORBIDDEN"


class AuthenticationError(CalyphantError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "UNAUTHENTICATED"


class ValidationError(CalyphantError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "VALIDATION_ERROR"


class ConnectionError(CalyphantError):
    """Raised when a user's target database cannot be reached."""
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "DATABASE_UNREACHABLE"


class SchemaConflictError(CalyphantError):
    """Raised when a schema diff detects conflicting migrations."""
    status_code = status.HTTP_409_CONFLICT
    code = "SCHEMA_CONFLICT"


class MigrationError(CalyphantError):
    """Raised when a migration fails to apply or roll back."""
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    code = "MIGRATION_FAILED"


class TierLimitError(CalyphantError):
    """Raised when a user exceeds their tier's resource limits."""
    status_code = status.HTTP_402_PAYMENT_REQUIRED
    code = "TIER_LIMIT_EXCEEDED"


class BackupError(CalyphantError):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    code = "BACKUP_FAILED"


class AIProviderError(CalyphantError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "AI_PROVIDER_ERROR"


class RateLimitError(CalyphantError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    code = "RATE_LIMITED"


# ---------------------------------------------------------------------------
# Response builder
# ---------------------------------------------------------------------------

def _error_response(
    request: Request,
    status_code: int,
    message: str,
    code: str,
    detail: Any = None,
) -> JSONResponse:
    correlation_id = getattr(request.state, "correlation_id", None)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": message,
            "code": code,
            "detail": detail,
            "request_id": correlation_id,
        },
        headers={"X-Request-ID": correlation_id} if correlation_id else {},
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def calyphant_error_handler(request: Request, exc: CalyphantError) -> JSONResponse:
    """Handle all CalyphantError subclasses."""
    if exc.status_code >= 500:
        logger.error(
            f"{exc.code}: {exc.message} | "
            f"path={request.url.path} "
            f"request_id={getattr(request.state, 'correlation_id', 'unknown')}"
        )
        _capture_to_sentry(exc, request)

    return _error_response(request, exc.status_code, exc.message, exc.code, exc.detail)


async def validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle Pydantic validation errors from request parsing."""
    errors = []
    for err in exc.errors():
        errors.append({
            "field": " → ".join(str(loc) for loc in err["loc"]),
            "message": err["msg"],
            "type": err["type"],
        })

    return _error_response(
        request,
        status.HTTP_422_UNPROCESSABLE_ENTITY,
        "Request validation failed.",
        "VALIDATION_ERROR",
        detail=errors,
    )


async def http_exception_handler(request: Request, exc) -> JSONResponse:
    """Handle FastAPI HTTPException — wrap in our error shape."""
    return _error_response(
        request,
        exc.status_code,
        exc.detail if isinstance(exc.detail, str) else str(exc.detail),
        "HTTP_ERROR",
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catch-all for unexpected exceptions.
    Logs full traceback, captures to Sentry, returns generic 500.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    logger.exception(
        f"Unhandled exception | "
        f"path={request.url.path} "
        f"request_id={correlation_id}"
    )
    _capture_to_sentry(exc, request)

    return _error_response(
        request,
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        "An unexpected error occurred. Our team has been notified.",
        "INTERNAL_ERROR",
    )


# ---------------------------------------------------------------------------
# Sentry capture helper
# ---------------------------------------------------------------------------

def _capture_to_sentry(exc: Exception, request: Request) -> None:
    from core.config import settings
    if not settings.sentry_enabled:
        return
    try:
        import sentry_sdk
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("path", str(request.url.path))
            scope.set_tag("method", request.method)
            scope.set_tag(
                "correlation_id",
                getattr(request.state, "correlation_id", "unknown"),
            )
            sentry_sdk.capture_exception(exc)
    except Exception:
        pass   # Never let Sentry integration crash the handler


# ---------------------------------------------------------------------------
# Registration helper
# ---------------------------------------------------------------------------

def register_exception_handlers(app: FastAPI) -> None:
    """Register all exception handlers on the FastAPI app."""
    from fastapi.exceptions import HTTPException as FastAPIHTTPException
    from starlette.exceptions import HTTPException as StarletteHTTPException

    app.add_exception_handler(CalyphantError, calyphant_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(FastAPIHTTPException, http_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    logger.info("Exception handlers: registered")

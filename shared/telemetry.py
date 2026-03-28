# shared/telemetry.py
"""
Telemetry and observability setup.

Three concerns:
  1. Logging    — Loguru with structured JSON output in production,
                  coloured console in development
  2. Metrics    — Prometheus counters/histograms exposed at /metrics
  3. Analytics  — PostHog server-side event capture (platform intelligence,
                  not user surveillance — no PII is sent)

Call init_telemetry() once at startup (from main.py lifespan).
"""

from __future__ import annotations

import sys
import time
from typing import Any
from uuid import UUID

from loguru import logger


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def init_logging(is_production: bool = False) -> None:
    """
    Configure Loguru.

    Development: coloured, human-readable console output
    Production: structured JSON to stdout (picked up by log aggregators)
    """
    logger.remove()   # Remove default handler

    if is_production:
        logger.add(
            sys.stdout,
            format="{message}",
            level="INFO",
            serialize=True,         # Loguru's built-in JSON serialisation
            backtrace=False,
            diagnose=False,         # Don't include variable values in prod
        )
    else:
        logger.add(
            sys.stderr,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "{message}"
            ),
            level="DEBUG",
            colorize=True,
            backtrace=True,
            diagnose=True,
        )

    logger.info(f"Logging: initialised ({'JSON/prod' if is_production else 'console/dev'})")


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_metrics_initialised = False

try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        CollectorRegistry,
        generate_latest,
        CONTENT_TYPE_LATEST,
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    logger.warning("prometheus_client not installed — metrics endpoint disabled")


def _init_prometheus():
    global _metrics_initialised
    if not _PROMETHEUS_AVAILABLE or _metrics_initialised:
        return

    global REQUEST_COUNT, REQUEST_LATENCY, ACTIVE_CONNECTIONS, DB_QUERY_DURATION, AI_REQUEST_COUNT, BACKUP_COUNT

    REQUEST_COUNT = Counter(
        "calyphant_http_requests_total",
        "Total HTTP requests",
        ["method", "endpoint", "status_code"],
    )
    REQUEST_LATENCY = Histogram(
        "calyphant_http_request_duration_seconds",
        "HTTP request latency",
        ["method", "endpoint"],
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    )
    ACTIVE_CONNECTIONS = Gauge(
        "calyphant_active_db_connections",
        "Active PostgreSQL connections being managed",
    )
    DB_QUERY_DURATION = Histogram(
        "calyphant_db_query_duration_seconds",
        "Database query execution time",
        ["query_type"],
        buckets=[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 30.0],
    )
    AI_REQUEST_COUNT = Counter(
        "calyphant_ai_requests_total",
        "Total AI provider requests",
        ["provider", "operation", "status"],
    )
    BACKUP_COUNT = Counter(
        "calyphant_backups_total",
        "Total backup operations",
        ["format", "status"],
    )

    _metrics_initialised = True
    logger.info("Prometheus: metrics registered")


def get_metrics_response():
    """Returns (content, content_type) for the /metrics endpoint."""
    if not _PROMETHEUS_AVAILABLE:
        return b"# prometheus_client not installed\n", "text/plain"
    return generate_latest(), CONTENT_TYPE_LATEST


# ---------------------------------------------------------------------------
# PostHog analytics
# ---------------------------------------------------------------------------

_posthog_client = None


def init_posthog(api_key: str, host: str, enabled: bool = True) -> None:
    global _posthog_client
    if not enabled or not api_key:
        logger.info("PostHog: disabled")
        return
    try:
        import posthog
        posthog.api_key = api_key
        posthog.host = host
        posthog.disabled = not enabled
        _posthog_client = posthog
        logger.info(f"PostHog: initialised (host={host})")
    except ImportError:
        logger.warning("posthog package not installed — analytics disabled")


def capture_event(
    user_id: UUID | str | None,
    event: str,
    properties: dict[str, Any] | None = None,
    anonymous: bool = False,
) -> None:
    """
    Capture a platform event to PostHog.

    Intelligence events (no PII):
      - "connection.tested" + provider, pg_version
      - "schema.diffed" + change_count, has_destructive
      - "migration.applied" + duration_ms
      - "backup.created" + format, size_bytes
      - "query.executed" + duration_ms, row_count, had_error
      - "ai.completion" + provider, operation, duration_ms
      - "extension.enabled" + extension_name

    We NEVER send: email, name, connection URLs, SQL content, table names.
    """
    if not _posthog_client:
        return

    try:
        distinct_id = str(user_id) if user_id else "anonymous"
        props = properties or {}

        # Strip any accidentally-included sensitive fields
        for sensitive_key in ("email", "password", "url", "sql", "token", "key"):
            props.pop(sensitive_key, None)

        _posthog_client.capture(distinct_id, event=event, properties=props)
    except Exception as exc:
        # Analytics must never crash the application
        logger.warning(f"PostHog capture failed for event '{event}': {exc}")


def identify_user(user_id: UUID | str, tier: str, created_at: str) -> None:
    """
    Associate platform-level properties with a user.
    Only non-PII properties — tier, created_at, plan status.
    """
    if not _posthog_client:
        return
    try:
        _posthog_client.identify(
            str(user_id),
            properties={
                "tier": tier,
                "created_at": created_at,
            },
        )
    except Exception as exc:
        logger.warning(f"PostHog identify failed: {exc}")


# ---------------------------------------------------------------------------
# Master init
# ---------------------------------------------------------------------------

def init_telemetry(is_production: bool = False) -> None:
    """Call once at startup from main.py lifespan."""
    from core.config import settings

    init_logging(is_production)
    _init_prometheus()
    init_posthog(
        api_key=settings.POSTHOG_API_KEY,
        host=settings.POSTHOG_HOST,
        enabled=settings.POSTHOG_ENABLED and bool(settings.POSTHOG_API_KEY),
    )

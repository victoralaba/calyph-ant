# shared/types.py
"""
Shared types and base classes.

Imported by all domains. Keep this file lean — only truly
cross-cutting types belong here. Domain-specific schemas
stay in their router.py files.
"""

from __future__ import annotations

from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase


# ---------------------------------------------------------------------------
# SQLAlchemy declarative base
# All ORM models across all domains inherit from this
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Generic paginated response
# ---------------------------------------------------------------------------

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int

    @property
    def has_more(self) -> bool:
        return self.offset + self.limit < self.total

    @property
    def page(self) -> int:
        return (self.offset // self.limit) + 1 if self.limit else 1


# ---------------------------------------------------------------------------
# Connection-related shared types
# (used between connections/service.py and connections/router.py)
# ---------------------------------------------------------------------------

class ConnectionTestResult(BaseModel):
    success: bool
    host: str | None = None
    port: int | None = None
    database: str | None = None
    pg_version: str | None = None
    pg_version_num: int | None = None
    cloud_provider: str | None = None
    capabilities: dict | None = None
    was_sleeping: bool = False
    error: str | None = None


class ConnectionCreateRequest(BaseModel):
    name: str
    keep_alive_enabled: bool = False
    keep_alive_interval_seconds: int = 300


class ConnectionUpdateRequest(BaseModel):
    name: str | None = None
    keep_alive_enabled: bool | None = None
    keep_alive_interval_seconds: int | None = None


# ---------------------------------------------------------------------------
# Health check response
# ---------------------------------------------------------------------------

class HealthStatus(BaseModel):
    status: str               # "ok" | "degraded" | "down"
    database: str
    redis: str
    version: str
    environment: str

# domains/connections/models.py

"""
ORM models for persisted database connections.
Each connection belongs to a workspace and tracks cloud provider metadata.
"""

from __future__ import annotations

import enum
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from shared.types import Base


class CloudProvider(str, enum.Enum):
    neon = "neon"
    supabase = "supabase"
    railway = "railway"
    render = "render"
    aws_rds = "aws_rds"
    google_cloud_sql = "google_cloud_sql"
    azure = "azure"
    local = "local"
    unknown = "unknown"


class ConnectionStatus(str, enum.Enum):
    active = "active"
    sleeping = "sleeping"
    unreachable = "unreachable"
    degraded = "degraded"


class Connection(Base):
    """
    A persisted PostgreSQL connection belonging to a workspace.
    The raw URL is never stored — only the encrypted form.
    """

    __tablename__ = "connections"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    workspace_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_by: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Identity
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    slug: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    # Encrypted connection URL — never store plaintext
    encrypted_url: Mapped[str] = mapped_column(Text, nullable=False)

    # Resolved metadata (populated on first connect)
    host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    database: Mapped[str | None] = mapped_column(String(120), nullable=True)
    pg_version: Mapped[str | None] = mapped_column(String(120), nullable=True)
    pg_version_num: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Provider detection
    cloud_provider: Mapped[CloudProvider] = mapped_column(
        Enum(CloudProvider, name="cloud_provider_enum"),
        default=CloudProvider.unknown,
        nullable=False,
    )

    # Status tracking
    status: Mapped[ConnectionStatus] = mapped_column(
        Enum(ConnectionStatus, name="connection_status_enum"),
        default=ConnectionStatus.active,
        nullable=False,
    )
    last_connected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Keep-alive config
    keep_alive_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    keep_alive_interval_seconds: Mapped[int] = mapped_column(
        Integer, default=300, nullable=False
    )

    # Detected capabilities stored as JSON
    # e.g. {"has_pgvector": true, "has_postgis": false, "extensions": [...]}
    capabilities: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)

    # Soft delete
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

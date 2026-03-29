# alembic/env.py
"""
Alembic migration environment.

Import order matters for autogenerate: Base must come first, then all
ORM models in dependency order (no FK referencing a model that hasn't
been imported yet).

All ORM models must be imported here for Alembic autogenerate to detect
them. If a model is missing, Alembic will generate DROP TABLE statements
for its table on the next autogenerate run.

Run after adding new models:
  alembic revision --autogenerate -m "describe_change"
  alembic upgrade head

Run manually created migrations:
  alembic upgrade head
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# ---------------------------------------------------------------------------
# ORM models — import order: Base first, then dependency order
#
# Rule: a model with a FK to another model must be imported AFTER the
# model it references. SQLAlchemy resolves FK targets by table name at
# mapper configuration time, so the order here also sets the order in
# which metadata is assembled.
# ---------------------------------------------------------------------------

from shared.types import Base  # noqa — must be first

# Platform-level models (no FK dependencies on domain models)
from domains.platform.extension_catalogue import ExtensionCatalogEntry  # noqa

# Core identity models — no FKs to domain models
from domains.users.service import User, ApiKey  # noqa

# Team models — FK to User
from domains.teams.service import (  # noqa
    Workspace,
    WorkspaceMember,
    WorkspaceInvite,
    WorkspaceShareLink,
)

# Connection models — FK to Workspace, User
from domains.connections.models import Connection as DomainConnection  # noqa

# Migration records — FK to Connection
from domains.migrations.service import MigrationRecord  # noqa

# Backup records — FK to Connection; RestoreDeadLetter has no FKs to user models
from domains.backups.engine import BackupRecord, BackupSchedule, RestoreDeadLetter  # noqa

# Query history + saved queries — FK to Connection
from domains.query.service import QueryHistoryRecord, SavedQuery  # noqa

# Billing — FK to User (implicit via user_id)
from domains.billing.flutterwave import Subscription, PaymentRecord  # noqa

# Notifications — FK to User (implicit)
from domains.notifications.service import Notification  # noqa

# Admin / audit — no FK dependencies on domain models beyond user_id
from domains.admin.service import AuditLog  # noqa

# ---------------------------------------------------------------------------
# Alembic config
# ---------------------------------------------------------------------------

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    from core.config import settings
    return settings.get_sync_url()


# ---------------------------------------------------------------------------
# Offline mode (generates SQL without a live DB connection)
# ---------------------------------------------------------------------------

def run_migrations_offline() -> None:
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online mode (requires a live DB connection)
# ---------------------------------------------------------------------------

def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    config_section = config.get_section(config.config_ini_section, {})
    config_section["sqlalchemy.url"] = get_url()

    connectable = async_engine_from_config(
        config_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

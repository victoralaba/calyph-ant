# domains/migrations/router.py

"""
Migrations router.

Endpoints:
  GET    /migrations/{connection_id}                    — list migrations
  POST   /migrations/{connection_id}                    — create migration
  GET    /migrations/{connection_id}/conflicts          — get conflict report for pending
  GET    /migrations/{connection_id}/{id}               — get single migration
  DELETE /migrations/{connection_id}/{id}               — delete unapplied migration
  POST   /migrations/{connection_id}/{id}/apply         — apply one migration
  POST   /migrations/{connection_id}/{id}/rollback      — rollback one migration
  POST   /migrations/{connection_id}/{id}/resolve       — resolve a conflict
  POST   /migrations/{connection_id}/apply-all          — apply all pending
  GET    /migrations/{connection_id}/{id}/export        — export as Alembic .py

Permission errors
-----------------
When the connection role is read-only, create and apply operations return
HTTP 403 with a message explaining which privilege is missing and how to
fix it. This replaces the raw asyncpg InsufficientPrivilegeError that
would otherwise surface as a 500.

Conflict behaviour on create
-----------------------------
When a migration is created with conflicts, the response includes a
"conflict_report" field with the full ConflictReport. The record is
created (so the developer can see it) but apply is blocked until the
conflict is resolved.

force_apply
-----------
POST /{id}/apply and POST /apply-all accept force_apply=true in the
request body to override conflict blocking after manual review.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from domains.migrations import service
from domains.migrations.service import MigrationRecord

router = APIRouter(prefix="/migrations", tags=["migrations"])


# ---------------------------------------------------------------------------
# WebSocket broadcast helper
# ---------------------------------------------------------------------------

async def _broadcast_migration_event(
    workspace_id: UUID,
    connection_id: UUID,
    event: str,
    migration_version: str,
    migration_label: str,
    error: str | None = None,
) -> None:
    try:
        from domains.teams.service import ws_manager
        payload: dict = {
            "event": event,
            "connection_id": str(connection_id),
            "migration_version": migration_version,
            "migration_label": migration_label,
        }
        if error:
            payload["error"] = error
        await ws_manager.broadcast(workspace_id, payload)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            f"{event} broadcast failed (non-fatal): {exc}"
        )


async def _notify_migration_result(
    db: AsyncSession,
    user_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    migration_label: str,
    migration_version: str,
    succeeded: bool,
    error: str | None = None,
) -> None:
    """
    Write a schema_migration_success or schema_migration_failed Notification
    row to the in-app inbox. Fire-and-forget — never raises.

    UI NOTE: These appear in the user's notification bell/inbox.
    They are NOT emailed (per EVENT_MATRIX: email=False for migration_success,
    email=True for migration_failed — but email for failed is handled separately
    via send_async_email if the workspace has that preference set).
    """
    try:
        from domains.notifications.service import Notification
        from core.db import _session_factory

        if not _session_factory:
            return

        from core.config import settings
        kind = "schema_migration_success" if succeeded else "schema_migration_failed"
        title = (
            f"Migration applied: {migration_label}"
            if succeeded
            else f"Migration failed: {migration_label}"
        )
        body = (
            f"Version {migration_version} applied successfully."
            if succeeded
            else f"Version {migration_version} failed. Error: {(error or '')[:200]}"
        )
        action_url = f"{settings.APP_BASE_URL}/dashboard/connections/{connection_id}/migrations"

        async with _session_factory() as notif_session:
            notif = Notification(
                user_id=user_id,
                workspace_id=workspace_id,
                kind=kind,
                title=title,
                body=body,
                action_url=action_url,
                meta={
                    "migration_version": migration_version,
                    "connection_id": str(connection_id),
                },
            )
            notif_session.add(notif)
            await notif_session.commit()

        # For migration failures, also send an email (per EVENT_MATRIX email=True)
        if not succeeded and error:
            try:
                from domains.notifications.tasks import send_async_email
                from domains.notifications.templates import build_migration_failed_email
                from domains.users.service import get_user

                async with _session_factory() as u_session:
                    user = await get_user(u_session, user_id)
                    if user:
                        subject, html = build_migration_failed_email(
                            workspace_name=str(workspace_id),
                            migration_label=migration_label,
                            migration_version=migration_version,
                            error_detail=error,
                        )
                        send_async_email.apply_async(
                            kwargs={
                                "to_email": user.email,
                                "to_name": user.full_name or user.email,
                                "subject": subject,
                                "html_content": html,
                                "sender_type": "system",
                            },
                            queue="notifications",
                        )
            except Exception as email_exc:
                import logging
                logging.getLogger(__name__).warning(f"Migration failed email dispatch failed (non-fatal): {email_exc}")

    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(f"Migration notification write failed (non-fatal): {exc}")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class CreateMigrationRequest(BaseModel):
    label: str = Field(..., min_length=1, max_length=255)
    up_sql: str = Field(..., min_length=1)
    down_sql: str | None = None
    generated_by: str = "manual"
    skip_validation: bool = Field(
        default=False,
        description=(
            "Skip pre-storage SQL validation. Use when the migration references "
            "objects created by earlier pending migrations that don't exist yet."
        ),
    )
    skip_conflict_check: bool = Field(
        default=False,
        description=(
            "Skip conflict detection against pending migrations. "
            "Use only for programmatic / internal callers."
        ),
    )


class ApplyMigrationRequest(BaseModel):
    force_apply: bool = Field(
        default=False,
        description=(
            "Override conflict blocking. Use after manually reviewing the "
            "migration and confirming it is safe to apply alongside "
            "conflicting pending migrations."
        ),
    )


class ApplyAllRequest(BaseModel):
    force_apply: bool = Field(
        default=False,
        description="Override conflict blocking for the entire pending queue.",
    )


class ResolveConflictRequest(BaseModel):
    strategy: str = Field(
        ...,
        description=(
            "Resolution strategy: "
            "'accept' — clear conflict flag, allow apply; "
            "'rebase' — move after target_migration_id; "
            "'skip' — mark applied without executing SQL; "
            "'squash' — merge into target_migration_id."
        ),
    )
    target_migration_id: UUID | None = Field(
        default=None,
        description="Required for 'rebase' and 'squash' strategies.",
    )


class MigrationResponse(BaseModel):
    id: UUID
    connection_id: UUID
    version: str
    label: str
    checksum: str
    up_sql: str
    down_sql: str | None
    applied: bool
    applied_at: str | None
    rolled_back_at: str | None
    error: str | None
    order_index: int
    generated_by: str
    syntax_valid: bool | None
    has_conflict: bool
    conflict_report: dict | None = None
    created_at: str

    model_config = {"from_attributes": True}

    @classmethod
    def from_record(cls, r: MigrationRecord) -> "MigrationResponse":
        conflict_report = None
        if r.has_conflict and r.meta:
            conflict_report = r.meta.get("conflict_report")
        return cls(
            id=r.id,
            connection_id=r.connection_id,
            version=r.version,
            label=r.label,
            checksum=r.checksum,
            up_sql=r.up_sql,
            down_sql=r.down_sql,
            applied=r.applied,
            applied_at=r.applied_at.isoformat() if r.applied_at else None,
            rolled_back_at=r.rolled_back_at.isoformat() if r.rolled_back_at else None,
            error=r.error,
            order_index=r.order_index,
            generated_by=r.generated_by,
            syntax_valid=r.syntax_valid,
            has_conflict=r.has_conflict,
            conflict_report=conflict_report,
            created_at=r.created_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_pg(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> asyncpg.Connection:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=15)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")


def _permission_error_to_http(exc: PermissionError) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={
            "message": str(exc),
            "code": "INSUFFICIENT_PRIVILEGES",
            "hint": (
                "Connect with a role that has write access on the public schema, "
                "or use a superuser connection for DDL operations."
            ),
        },
    )

def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/{connection_id}", response_model=list[MigrationResponse])
async def list_migrations(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    applied: bool | None = Query(None),
    limit: int = Query(100, le=500),
    offset: int = 0,
):
    records = await service.list_migrations(
        db, connection_id, applied=applied, limit=limit, offset=offset
    )
    return [MigrationResponse.from_record(r) for r in records]


@router.get("/{connection_id}/conflicts")
async def get_pending_conflicts(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Return a full conflict analysis for all currently pending migrations
    on this connection.

    Use this endpoint to get a consolidated view of all conflicts before
    deciding which resolution strategy to apply. The response groups
    conflicts by migration pair and includes resolution options.
    """
    pending = await service.list_migrations(db, connection_id, applied=False)
    conflicted = [m for m in pending if m.has_conflict]

    # Build a consolidated summary across all conflicted migrations
    all_conflicts = []
    for m in conflicted:
        report = m.meta.get("conflict_report", {})
        for conflict in report.get("conflicts", []):
            all_conflicts.append({
                "migration_id": str(m.id),
                "migration_version": m.version,
                "migration_label": m.label,
                **conflict,
            })

    divergent_heads = await service.check_divergent_heads(db, connection_id)

    return {
        "pending_count": len(pending),
        "conflicted_count": len(conflicted),
        "has_divergent_heads": bool(divergent_heads),
        "divergent_head_versions": divergent_heads,
        "conflicts": all_conflicts,
        "resolution_options": (
            ["accept", "rebase", "skip", "squash"] if conflicted else []
        ),
        "summary": (
            f"{len(conflicted)} of {len(pending)} pending migration(s) have conflicts."
            if conflicted
            else f"No conflicts. {len(pending)} pending migration(s) ready to apply."
        ),
    }


@router.post(
    "/{connection_id}",
    response_model=MigrationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_migration(
    connection_id: UUID,
    body: CreateMigrationRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a migration record.

    Permission check: returns 403 if the connection role is read-only.

    Conflict detection: if pending migrations touch the same database
    objects as this migration, the record is created with has_conflict=True
    and the conflict_report field is populated. apply will be blocked until
    the conflict is resolved (or force_apply=true is used).

    SQL validation runs unless skip_validation=true.
    """
    db_url = await get_connection_url(db, connection_id, workspace_id)
    if not db_url:
        raise HTTPException(status_code=404, detail="Connection not found.")

    try:
        record = await service.create_migration(
            db=db,
            connection_id=connection_id,
            workspace_id=workspace_id,
            label=body.label,
            up_sql=body.up_sql,
            down_sql=body.down_sql,
            generated_by=body.generated_by,
            db_url=db_url,
            skip_validation=body.skip_validation,
            skip_conflict_check=body.skip_conflict_check,
        )
    except PermissionError as exc:
        raise _permission_error_to_http(exc)

    return MigrationResponse.from_record(record)


@router.get("/{connection_id}/{migration_id}", response_model=MigrationResponse)
async def get_migration(
    connection_id: UUID,
    migration_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    record = await service.get_migration(db, migration_id)
    if not record or record.connection_id != connection_id:
        raise HTTPException(status_code=404, detail="Migration not found.")
    return MigrationResponse.from_record(record)


@router.delete("/{connection_id}/{migration_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_migration(
    connection_id: UUID,
    migration_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    try:
        deleted = await service.delete_migration(db, migration_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail="Migration not found.")


@router.post("/{connection_id}/{migration_id}/apply", response_model=MigrationResponse)
async def apply_migration(
    connection_id: UUID,
    migration_id: UUID,
    body: ApplyMigrationRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Apply a single migration.

    Returns 403 if the connection role lacks DDL privileges.
    Returns 400 if the migration has unresolved conflicts (use force_apply=true
    to override after manual review, or resolve via POST /resolve first).
    """
    pg_conn = await _get_pg(connection_id, workspace_id, db)
    try:
        record = await service.apply_migration(
            pg_conn, db, migration_id, force_apply=body.force_apply
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except asyncpg.InsufficientPrivilegeError as exc:
        failed_record = await service.get_migration(db, migration_id)
        if failed_record:
            asyncio.create_task(_broadcast_migration_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                event="migration_failed",
                migration_version=failed_record.version,
                migration_label=failed_record.label,
                error=str(exc),
            ))
            # Write in-app failure notification
            asyncio.create_task(_notify_migration_result(
                db=db,
                user_id=user.id,
                workspace_id=workspace_id,
                connection_id=connection_id,
                migration_label=failed_record.label,
                migration_version=failed_record.version,
                succeeded=False,
                error=str(exc),
            ))
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": (
                    f"Insufficient privileges to apply this migration: {exc.args[0]}. "
                    "The connected role needs DDL privileges (CREATE, ALTER, DROP) "
                    "on the public schema."
                ),
                "code": "INSUFFICIENT_PRIVILEGES",
            },
        )
    except Exception as exc:
        failed_record = await service.get_migration(db, migration_id)
        if failed_record:
            asyncio.create_task(_broadcast_migration_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                event="migration_failed",
                migration_version=failed_record.version,
                migration_label=failed_record.label,
                error=str(exc),
            ))
            asyncio.create_task(_notify_migration_result(
                db=db,
                user_id=user.id,
                workspace_id=workspace_id,
                connection_id=connection_id,
                migration_label=failed_record.label,
                migration_version=failed_record.version,
                succeeded=False,
                error=str(exc),
            ))
        raise HTTPException(status_code=500, detail=f"Migration failed: {exc}")
    finally:
        await pg_conn.close()

    asyncio.create_task(_broadcast_migration_event(
        workspace_id=workspace_id,
        connection_id=connection_id,
        event="migration_applied",
        migration_version=record.version,
        migration_label=record.label,
    ))
    # Write in-app success notification
    asyncio.create_task(_notify_migration_result(
        db=db,
        user_id=user.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        migration_label=record.label,
        migration_version=record.version,
        succeeded=True,
    ))

    return MigrationResponse.from_record(record)


@router.post("/{connection_id}/{migration_id}/rollback", response_model=MigrationResponse)
async def rollback_migration(
    connection_id: UUID,
    migration_id: UUID,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    pg_conn = await _get_pg(connection_id, workspace_id, db)
    try:
        record = await service.rollback_migration(pg_conn, db, migration_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except asyncpg.InsufficientPrivilegeError as exc:
        failed_record = await service.get_migration(db, migration_id)
        if failed_record:
            asyncio.create_task(_broadcast_migration_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                event="migration_failed",
                migration_version=failed_record.version,
                migration_label=failed_record.label,
                error=str(exc),
            ))
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": (
                    f"Insufficient privileges to roll back this migration: {exc.args[0]}"
                ),
                "code": "INSUFFICIENT_PRIVILEGES",
            },
        )
    except Exception as exc:
        failed_record = await service.get_migration(db, migration_id)
        if failed_record:
            asyncio.create_task(_broadcast_migration_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                event="migration_failed",
                migration_version=failed_record.version,
                migration_label=failed_record.label,
                error=str(exc),
            ))
        raise HTTPException(status_code=500, detail=f"Rollback failed: {exc}")
    finally:
        await pg_conn.close()

    asyncio.create_task(_broadcast_migration_event(
        workspace_id=workspace_id,
        connection_id=connection_id,
        event="migration_rolled_back",
        migration_version=record.version,
        migration_label=record.label,
    ))

    return MigrationResponse.from_record(record)


@router.post("/{connection_id}/{migration_id}/resolve", response_model=MigrationResponse)
async def resolve_conflict(
    connection_id: UUID,
    migration_id: UUID,
    body: ResolveConflictRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Resolve a conflict on a pending migration.

    Strategies
    ----------
    accept  — Clear the conflict flag. Apply can proceed normally.
              Use when you have reviewed the migrations and confirmed they
              are safe to run sequentially.

    rebase  — Move this migration to run after target_migration_id.
              Requires target_migration_id. Both must be pending.

    skip    — Mark as applied without executing SQL. Use only when the
              schema change was already applied by other means.

    squash  — Merge this migration's SQL into target_migration_id and
              delete this record. Requires target_migration_id (pending).

    After resolving, apply normally via POST /{id}/apply.
    """
    try:
        record = await service.resolve_conflict(
            db=db,
            migration_id=migration_id,
            strategy=body.strategy,
            target_migration_id=body.target_migration_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return MigrationResponse.from_record(record)


@router.post("/{connection_id}/apply-all", response_model=list[MigrationResponse])
async def apply_all_pending(
    connection_id: UUID,
    body: ApplyAllRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Apply all pending migrations in order_index order.

    If any pending migration has unresolved conflicts, apply-all stops
    and returns 400 listing the conflicted versions — unless force_apply=true.

    Returns 403 if the connection role lacks DDL privileges.
    """
    pg_conn = await _get_pg(connection_id, workspace_id, db)
    applied_records: list[MigrationRecord] = []

    try:
        records = await service.apply_pending(
            pg_conn, db, connection_id, force_apply=body.force_apply
        )
        applied_records = records
    except PermissionError as exc:
        raise _permission_error_to_http(exc)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except asyncpg.InsufficientPrivilegeError as exc:
        pending = await service.list_migrations(db, connection_id, applied=False)
        if pending:
            asyncio.create_task(_broadcast_migration_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                event="migration_failed",
                migration_version=pending[0].version,
                migration_label=pending[0].label,
                error=str(exc),
            ))
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": (
                    f"Insufficient privileges to apply migrations: {exc.args[0]}. "
                    "The connected role needs DDL privileges on the public schema."
                ),
                "code": "INSUFFICIENT_PRIVILEGES",
            },
        )
    except Exception as exc:
        pending = await service.list_migrations(db, connection_id, applied=False)
        if pending:
            asyncio.create_task(_broadcast_migration_event(
                workspace_id=workspace_id,
                connection_id=connection_id,
                event="migration_failed",
                migration_version=pending[0].version,
                migration_label=pending[0].label,
                error=str(exc),
            ))
        raise HTTPException(status_code=500, detail=f"Migration run failed: {exc}")
    finally:
        await pg_conn.close()

    for record in applied_records:
        asyncio.create_task(_broadcast_migration_event(
            workspace_id=workspace_id,
            connection_id=connection_id,
            event="migration_applied",
            migration_version=record.version,
            migration_label=record.label,
        ))

    return [MigrationResponse.from_record(r) for r in applied_records]


@router.get("/{connection_id}/{migration_id}/export")
async def export_migration(
    connection_id: UUID,
    migration_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """Export migration as an Alembic-compatible .py file."""
    record = await service.get_migration(db, migration_id)
    if not record or record.connection_id != connection_id:
        raise HTTPException(status_code=404, detail="Migration not found.")
    return {
        "filename": f"{record.version}_{record.label[:40]}.py".replace(" ", "_"),
        "content": service.export_as_alembic_py(record),
    }
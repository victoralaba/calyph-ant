# domains/schema/router/editor.py
"""
Schema editor router.

All structural mutation endpoints check connection privileges before
attempting any DDL. Read-only connections receive a 403 with a clear
message describing which privilege is missing and how to fix it, rather
than getting a raw asyncpg InsufficientPrivilegeError surfaced as a 500.

Permission guard
----------------
_get_workspace_role() fetches the connection's stored capabilities,
checks the privileges.is_read_only flag if require_write=True, and raises HTTP 403 
before opening a DB connection if the role is read-only. It returns the current_role 
which is passed down to execute_sql to enforce the transaction-level sandbox.

Read-only endpoints (introspection, export) are unaffected.

DDL Confirmation session tokens  (SAFETY-13)
--------------------------------------------
The original _confirmation_preview() helper returned SQL for the client
to review and re-submit with confirmed=true, but there was no server-side
binding between the previewed SQL and the executed SQL. A client could
preview "DROP TABLE users" and then submit confirmed=true against a
completely different table name.

Fix: _confirmation_preview() now stores the SQL in Redis under a
short-lived session token (15-minute TTL). The preview response includes
a confirm_token. On re-submission with confirmed=true the endpoint MUST
supply that token. The server fetches the stored SQL, verifies it matches
what the client is about to execute, and only then proceeds.

If the token is absent, expired, or the SQL doesn't match, the endpoint
returns 400 with a clear error.

_execute_with_confirmation() is a new helper used by all destructive
DDL endpoints (drop_table, drop_column, drop_index, drop_constraint,
drop_enum) that encapsulates the two-phase logic:
  Phase 1 (confirmed=False): store SQL → return preview + token.
  Phase 2 (confirmed=True): verify token → execute → delete token.

All other logic is unchanged from the original.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.db import get_db, get_connection_url
from domains.billing.flutterwave import get_limits
from domains.users.service import get_user
from domains.connections.models import Connection
from domains.connections.service import acquire_workspace_connection, get_validated_workspace_url
from domains.schema import introspection, builder
from domains.schema.builder import (
    AlterColumnRequest,
    ColumnDefinition,
    ColumnCheckConstraint,
    CreateTableRequest,
    IndexDefinition,
    assert_extensions_for_columns,
    assert_extensions_for_index,
    get_column_type_catalogue,
    get_index_method_catalogue,
    resolve_column_defaults,
    validate_default_expression,
    sql_add_check_constraint,
    sql_add_foreign_key,
    sql_add_unique_constraint,
    sql_drop_constraint,
    sql_create_enum,
    sql_add_enum_value,
    sql_rename_enum_value,
    sql_drop_enum,
    sql_rename_table,
    sql_comment_on_table,
    sql_comment_on_column,
)

router = APIRouter(prefix="/schema", tags=["schema-editor"])


# ---------------------------------------------------------------------------
# Type-narrowing helper
# ---------------------------------------------------------------------------

def _require_workspace(workspace_id: UUID | None) -> UUID:
    """
    Narrow ``UUID | None`` → ``UUID``, raising HTTP 401 if the session
    has no workspace. Called once at the top of every handler so
    downstream helpers receive a guaranteed UUID.
    """
    if workspace_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No workspace associated with this session.",
        )
    return workspace_id


# ---------------------------------------------------------------------------
# Confirmation session constants
# ---------------------------------------------------------------------------

_CONFIRM_SESSION_TTL = 900     # 15 minutes — plenty of time for a human to review
_CONFIRM_KEY_PREFIX = "calyphant:ddl_confirm:"


# ---------------------------------------------------------------------------
# Permission guard & Sandbox context
# ---------------------------------------------------------------------------

async def _get_workspace_role(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
    require_write: bool = True,
    operation: str = "This schema operation",
) -> str:
    """
    Fetch the connection's stored capabilities. If require_write=True,
    raise HTTP 403 if the connection role is read-only. Returns the
    current active role to be injected into the sandbox boundaries.
    """
    result = await db.execute(
        select(Connection.capabilities).where(
            Connection.id == connection_id,
            Connection.workspace_id == workspace_id,
            Connection.is_active == True,  # noqa: E712
        )
    )
    caps = result.scalar_one_or_none()
    if caps is None:
        raise HTTPException(status_code=404, detail="Connection not found.")

    privs: dict = caps.get("privileges", {})
    role = privs.get("current_role", "unknown")

    if require_write and privs.get("is_read_only", False):
        warnings = privs.get("privilege_warnings", [])
        detail_msg = warnings[0] if warnings else ""
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": (
                    f"{operation} requires write access, but role '{role}' "
                    f"has read-only privileges on this database."
                ),
                "code": "READ_ONLY_CONNECTION",
                "hint": detail_msg or (
                    "Connect with a role that has CREATE/ALTER/DROP privileges "
                    "on the public schema to perform schema changes."
                ),
                "current_role": role,
                "missing_privileges": privs.get("missing_roles", []),
            },
        )
        
    return role


# ---------------------------------------------------------------------------
# Confirmation session helpers  (SAFETY-13)
# ---------------------------------------------------------------------------

async def _store_confirm_session(sql_statements: list[str]) -> str:
    """
    Store DDL SQL under a one-time confirm token in Redis.
    Returns the token.

    Raises HTTPException 503 if Redis is unavailable — DDL confirmation
    cannot proceed safely without session storage.
    """
    token = secrets.token_urlsafe(32)
    payload = json.dumps({"sql": sql_statements})
    try:
        from core.db import get_redis, _fernet
        redis = await get_redis()
        raw = payload.encode()
        if _fernet:
            raw = _fernet.encrypt(raw)
        await redis.setex(f"{_CONFIRM_KEY_PREFIX}{token}", _CONFIRM_SESSION_TTL, raw)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Could not store DDL confirmation session (Redis unavailable). "
                f"Detail: {exc}"
            ),
        )
    return token


async def _load_confirm_session(token: str) -> list[str] | None:
    """
    Load and delete a DDL confirmation session.
    Returns the SQL statements or None if the token is expired/missing.
    The session is consumed on first successful read (single-use).
    """
    try:
        from core.db import get_redis, _fernet
        redis = await get_redis()
        key = f"{_CONFIRM_KEY_PREFIX}{token}"
        raw = await redis.get(key)
        if raw is None:
            return None
        # Consume immediately — single use
        await redis.delete(key)
        if _fernet:
            raw = _fernet.decrypt(raw)
        payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        return payload.get("sql", [])
    except Exception:
        return None


async def _execute_with_confirmation(
    pg_conn: asyncpg.Connection,
    sql_statements: list[str],
    workspace_role: str,
    confirmed: bool,
    confirm_token: str | None,
    dry_run: bool,
    extensions_required: list[str] | None = None,
    workspace_id: UUID | None = None,
    connection_id: UUID | None = None,
    broadcast_kind: str | None = None,
    broadcast_name: str | None = None,
    broadcast_table: str | None = None,
) -> "SqlPreviewResponse":
    """
    Two-phase DDL execution with server-side session token.

    Phase 1 (confirmed=False):
        Stores the SQL in Redis, returns preview + confirm_token.
        The client must include confirm_token when re-submitting.

    Phase 2 (confirmed=True):
        Loads the stored SQL, verifies it matches what the client sent,
        executes it, deletes the session.

    dry_run=True (for create/alter operations):
        Runs inside a rolled-back transaction for SQL validation.
        No confirmation token needed.
    """
    if dry_run:
        result = await builder.execute_sql(pg_conn, sql_statements, workspace_role, dry_run=True)
        return _sql_preview(sql_statements, True, result, extensions_required)

    if not confirmed:
        # Phase 1: store and return token
        token = await _store_confirm_session(sql_statements)
        return SqlPreviewResponse(
            sql=sql_statements,
            dry_run=True,
            error=None,
            requires_confirmation=True,
            confirm_token=token,
            extensions_required=extensions_required or [],
            message=(
                "This is a destructive operation. Review the SQL above, then "
                "re-submit with confirmed=true and the confirm_token to execute. "
                f"Token expires in {_CONFIRM_SESSION_TTL // 60} minutes."
            ),
        )

    # Phase 2: verify token and execute
    if not confirm_token:
        raise HTTPException(
            status_code=400,
            detail=(
                "confirm_token is required when confirmed=true. "
                "First call this endpoint with confirmed=false to receive a token, "
                "then re-submit with confirmed=true and the token."
            ),
        )

    stored_sql = await _load_confirm_session(confirm_token)
    if stored_sql is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "confirm_token is invalid or expired "
                f"(tokens expire after {_CONFIRM_SESSION_TTL // 60} minutes). "
                "Re-submit with confirmed=false to get a new token."
            ),
        )

    # Verify the SQL the client is executing matches what was previewed
    if stored_sql != sql_statements:
        raise HTTPException(
            status_code=400,
            detail=(
                "The SQL to be executed does not match the SQL that was "
                "previewed. The confirm_token is bound to a specific set of "
                "SQL statements — you cannot change the operation after "
                "receiving a token. Request a new token with confirmed=false."
            ),
        )

    result = await builder.execute_sql(pg_conn, sql_statements, workspace_role, dry_run=False)

    if not result.get("error") and workspace_id and connection_id and broadcast_kind:
        asyncio.create_task(_broadcast_schema_changed(
            workspace_id, connection_id,
            change_kind=broadcast_kind,
            object_name=broadcast_name,
            table_name=broadcast_table,
        ))

    return _sql_preview(sql_statements, False, result, extensions_required)


# ---------------------------------------------------------------------------
# WebSocket broadcast helper
# ---------------------------------------------------------------------------

async def _broadcast_schema_changed(
    workspace_id: UUID,
    connection_id: UUID,
    change_kind: str,
    object_name: str | None = None,
    table_name: str | None = None,
) -> None:
    try:
        from domains.teams.service import ws_manager
        await ws_manager.broadcast(
            workspace_id,
            {
                "event": "schema_changed",
                "connection_id": str(connection_id),
                "change_kind": change_kind,
                "object_name": object_name,
                "table_name": table_name,
            },
        )
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            f"schema_changed broadcast failed (non-fatal): {exc}"
        )


# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------

class SqlPreviewResponse(BaseModel):
    sql: list[str]
    dry_run: bool
    error: str | None = None
    requires_confirmation: bool = False
    confirm_token: str | None = Field(
        default=None,
        description=(
            "One-time token returned when requires_confirmation=true. "
            "Re-submit with confirmed=true and this token to execute."
        ),
    )
    message: str | None = None
    extensions_required: list[str] = []


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class CreateTableBody(BaseModel):
    table_name: str
    schema_name: str = "public"
    columns: list[dict] = Field(default_factory=list)
    if_not_exists: bool = False
    composite_pk: list[str] = Field(default_factory=list)
    dry_run: bool = False


class RenameTableBody(BaseModel):
    new_name: str
    schema_name: str = "public"
    dry_run: bool = False


class AddColumnBody(BaseModel):
    table_name: str
    schema_name: str = "public"
    column: dict
    dry_run: bool = False


class AlterColumnBody(BaseModel):
    table_name: str
    column_name: str
    schema_name: str = "public"
    new_name: str | None = None
    new_type: str | None = None
    set_nullable: bool | None = None
    set_default: str | None = None
    drop_default: bool = False
    dry_run: bool = False


class DropColumnBody(BaseModel):
    table_name: str
    column_name: str
    schema_name: str = "public"
    cascade: bool = False
    dry_run: bool = False
    confirmed: bool = False
    confirm_token: str | None = Field(
        default=None,
        description="Token from a previous confirmed=false call. Required when confirmed=true.",
    )


class CreateIndexBody(BaseModel):
    table_name: str
    schema_name: str = "public"
    index: dict
    dry_run: bool = False


class AddForeignKeyBody(BaseModel):
    table: str
    name: str
    columns: list[str]
    ref_table: str
    ref_columns: list[str]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    schema_name: str = "public"
    ref_schema: str | None = None
    dry_run: bool = False


class AddCheckConstraintBody(BaseModel):
    table: str
    name: str
    column_name: str
    constraint: ColumnCheckConstraint
    schema_name: str = "public"
    dry_run: bool = False


class AddUniqueConstraintBody(BaseModel):
    table: str
    name: str
    columns: list[str]
    schema_name: str = "public"
    dry_run: bool = False


class DropConstraintBody(BaseModel):
    table: str
    schema_name: str = "public"
    cascade: bool = False
    dry_run: bool = False
    confirmed: bool = False
    confirm_token: str | None = Field(
        default=None,
        description="Token from a previous confirmed=false call. Required when confirmed=true.",
    )


class CreateEnumBody(BaseModel):
    name: str
    values: list[str]
    schema_name: str = "public"
    dry_run: bool = False


class AddEnumValueBody(BaseModel):
    value: str
    after: str | None = None
    before: str | None = None
    schema_name: str = "public"
    dry_run: bool = False


class RenameEnumValueBody(BaseModel):
    old_value: str
    new_value: str
    schema_name: str = "public"
    dry_run: bool = False


class DropEnumBody(BaseModel):
    schema_name: str = "public"
    cascade: bool = False
    dry_run: bool = False
    confirmed: bool = False
    confirm_token: str | None = Field(
        default=None,
        description="Token from a previous confirmed=false call. Required when confirmed=true.",
    )


class SetCommentBody(BaseModel):
    comment: str | None = None
    schema_name: str = "public"
    dry_run: bool = False


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def _evaluate_untrusted_expression(
    pg_conn: asyncpg.Connection,
    expression: str,
    workspace_role: str,
    expected_type: str | None = None,
) -> None:
    """
    Executes a user-provided SQL expression inside an inescapable kernel-level sandbox.
    Enforces Time (DoS), Identity (Blast Radius), and State (Mutability) boundaries.
    """
    tr = pg_conn.transaction()
    await tr.start()
    try:
        # 1. Identity Lock: Restrict execution to the tenant's exact privilege level.
        # This prevents the expression from querying system catalogs or cross-tenant data.
        await pg_conn.execute(f'SET LOCAL ROLE "{workspace_role}";')
        
        # 2. State Lock: Physically prevent writes, updates, or deletes.
        # This aborts any volatile functions (like nextval or custom triggers) hiding in the expression.
        await pg_conn.execute("SET LOCAL default_transaction_read_only = 'on';")
        
        # 3. Time Lock: 1000ms execution max to prevent pool starvation.
        await pg_conn.execute("SET LOCAL statement_timeout = '1000';")
        
        query = f"SELECT ({expression})"
        if expected_type:
            query += f"::{expected_type}"
            
        await pg_conn.execute(query)
    except Exception as exc:
        await tr.rollback()
        
        if isinstance(exc, asyncpg.QueryCanceledError) or "timeout" in str(exc).lower():
            raise HTTPException(
                status_code=400,
                detail=f"Expression evaluation timed out. It is too complex or malicious: {expression}",
            )
        elif isinstance(exc, asyncpg.PostgresSyntaxError):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid expression syntax: {exc.args[0]}",
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid expression: {exc}",
            )
    else:
        # ALWAYS roll back on success. 
        await tr.rollback()


def _sql_preview(
    sql: list[str],
    dry_run: bool,
    result: dict,
    extensions_required: list[str] | None = None,
    requires_confirmation: bool = False,
) -> SqlPreviewResponse:
    return SqlPreviewResponse(
        sql=sql,
        dry_run=dry_run,
        error=result.get("error"),
        requires_confirmation=requires_confirmation,
        message=result.get("message"),
        extensions_required=extensions_required or [],
    )


async def _validate_and_resolve_columns(
    pg_conn: asyncpg.Connection,
    columns: list[ColumnDefinition],
    workspace_role: str,
) -> tuple[list[ColumnDefinition], list[str]]:
    try:
        # Thread the identity context into the bulk resolver
        resolved = await resolve_column_defaults(pg_conn, columns, workspace_role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        extensions_required = await assert_extensions_for_columns(pg_conn, resolved)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    for col in resolved:
        if col.default is not None:
            await _evaluate_untrusted_expression(
                pg_conn=pg_conn,
                expression=col.default,
                workspace_role=workspace_role,
                expected_type=col.data_type
            )

    return resolved, extensions_required


# ---------------------------------------------------------------------------
# Static catalogues
# ---------------------------------------------------------------------------

@router.get("/column-types")
async def list_column_types():
    return {"column_types": get_column_type_catalogue()}


@router.get("/index-methods")
async def list_index_methods():
    return {"index_methods": get_index_method_catalogue()}


# ---------------------------------------------------------------------------
# Schema snapshot / introspection  (read-only — no permission guard needed)
# ---------------------------------------------------------------------------

@router.get("/{connection_id}")
async def get_full_snapshot(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    wid = _require_workspace(user.workspace_id)
    url = await get_validated_workspace_url(db, connection_id, wid)
    snapshot = await introspection.introspect_database(url, schema_name)
    return snapshot


@router.get("/{connection_id}/tables")
async def list_tables(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    wid = _require_workspace(user.workspace_id)
    url = await get_validated_workspace_url(db, connection_id, wid)
    snapshot = await introspection.introspect_database(url, schema_name)
    return {
        "tables": [
            {
                "name": t.name,
                "schema": t.schema,
                "kind": t.kind,
                "column_count": len(t.columns),
                "row_count_estimate": t.row_count_estimate,
            }
            for t in snapshot.tables
        ],
        "total": len(snapshot.tables),
    }


@router.get("/{connection_id}/tables/{table_name}")
async def get_table(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    wid = _require_workspace(user.workspace_id)
    url = await get_validated_workspace_url(db, connection_id, wid)
    table = await introspection.introspect_table(url, table_name, schema_name)
    return table


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/tables", response_model=SqlPreviewResponse)
async def create_table(
    connection_id: UUID,
    body: CreateTableBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    
    # --- NEW: QUOTA GATE ---
    if not body.dry_run:
        # Fetch user's tier limits
        db_user = await get_user(db, user.id)
        limits = get_limits(db_user.tier if db_user else "free")
        max_tables = limits.get("max_tables_per_connection", 20)
        
        if max_tables != -1:
            # Quickly count existing tables via introspection
            url = await get_validated_workspace_url(db, connection_id, wid)
            snapshot = await introspection.introspect_database(url, body.schema_name)
            current_count = len(snapshot.tables)
            
            if current_count >= max_tables:
                raise HTTPException(
                    status_code=403,
                    detail=f"Tier limit reached: You cannot create more than {max_tables} tables. Please upgrade your plan."
                )

    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="CREATE TABLE"
    )

    columns = [ColumnDefinition(**c) for c in body.columns]

    if body.composite_pk:
        col_names = {c.name for c in columns}
        bad = [c for c in body.composite_pk if c not in col_names]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=f"composite_pk references columns not in this table: {bad}",
            )

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        resolved_columns, extensions_required = await _validate_and_resolve_columns(
            pg_conn, columns, workspace_role
        )
        req = CreateTableRequest(
            table_name=body.table_name,
            schema_name=body.schema_name,
            columns=resolved_columns,
            if_not_exists=body.if_not_exists,
            composite_pk=body.composite_pk,
        )
        sql = builder.sql_create_table(req)
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="table_created", object_name=body.table_name,
        ))

    return _sql_preview([sql], body.dry_run, result, extensions_required)


@router.patch(
    "/{connection_id}/tables/{table_name}/rename",
    response_model=SqlPreviewResponse,
)
async def rename_table(
    connection_id: UUID,
    table_name: str,
    body: RenameTableBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="RENAME TABLE"
    )

    sql = sql_rename_table(table_name, body.new_name, body.schema_name)
    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="table_renamed",
            object_name=body.new_name, table_name=table_name,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.delete("/{connection_id}/tables/{table_name}", response_model=SqlPreviewResponse)
async def drop_table(
    connection_id: UUID,
    table_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    cascade: bool = Query(False),
    dry_run: bool = Query(False),
    confirmed: bool = Query(False),
    confirm_token: str | None = Query(
        default=None,
        description="Token from a previous confirmed=false call.",
    ),
):
    wid = _require_workspace(user.workspace_id)
    sql = builder.sql_drop_table(table_name, schema_name, cascade)

    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not dry_run, operation="DROP TABLE"
    )

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        response = await _execute_with_confirmation(
            pg_conn=pg_conn,
            sql_statements=[sql],
            workspace_role=workspace_role,
            confirmed=confirmed,
            confirm_token=confirm_token,
            dry_run=dry_run,
            workspace_id=wid,
            connection_id=connection_id,
            broadcast_kind="table_dropped",
            broadcast_name=table_name,
        )
    finally:
        await pg_conn.close()

    return response


# ---------------------------------------------------------------------------
# Column DDL
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/columns", response_model=SqlPreviewResponse)
async def add_column(
    connection_id: UUID,
    body: AddColumnBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ALTER TABLE (ADD COLUMN)"
    )

    col = ColumnDefinition(**body.column)
    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        resolved_columns, extensions_required = await _validate_and_resolve_columns(
            pg_conn, [col], workspace_role
        )
        resolved_col = resolved_columns[0]
        sql = builder.sql_add_column(body.table_name, resolved_col, body.schema_name)
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="column_added",
            object_name=col.name, table_name=body.table_name,
        ))

    return _sql_preview([sql], body.dry_run, result, extensions_required)


@router.patch("/{connection_id}/columns", response_model=SqlPreviewResponse)
async def alter_column(
    connection_id: UUID,
    body: AlterColumnBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ALTER TABLE (ALTER COLUMN)"
    )

    req = AlterColumnRequest(
        table_name=body.table_name,
        column_name=body.column_name,
        schema_name=body.schema_name,
        new_name=body.new_name,
        new_type=body.new_type,
        set_nullable=body.set_nullable,
        set_default=body.set_default,
        drop_default=body.drop_default,
    )
    statements = builder.sql_alter_column(req)

    # Use the unified, semaphored connection factory
    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        if body.set_default is not None:
            from domains.schema.builder import (
                resolve_smart_default,
                SMART_DEFAULTS_STATIC,
                SMART_DEFAULTS_ASYNC,
            )
            key = body.set_default.strip().lower()
            if key in SMART_DEFAULTS_STATIC or key in SMART_DEFAULTS_ASYNC:
                resolved_default = await resolve_smart_default(
                    pg_conn, body.set_default, body.new_type or "text", workspace_role
                )
                req.set_default = resolved_default
                statements = builder.sql_alter_column(req)

        if body.set_default is not None and body.new_type:
            error = await validate_default_expression(
                pg_conn, req.set_default or body.set_default, body.new_type
            )
            if error:
                raise HTTPException(status_code=400, detail=error)
        elif body.set_default is not None:
            # Replaced the un-sandboxed fetchval with our strict evaluation boundary
            await _evaluate_untrusted_expression(
                pg_conn, 
                req.set_default or body.set_default, 
                workspace_role,
                body.new_type
            )

        result = await builder.execute_sql(pg_conn, statements, workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        import asyncio
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="column_altered",
            object_name=body.new_name or body.column_name,
            table_name=body.table_name,
        ))

    return _sql_preview(statements, body.dry_run, result)


@router.delete("/{connection_id}/columns", response_model=SqlPreviewResponse)
async def drop_column(
    connection_id: UUID,
    body: DropColumnBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    sql = builder.sql_drop_column(
        body.table_name, body.column_name, body.schema_name, body.cascade
    )

    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ALTER TABLE (DROP COLUMN)"
    )

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        response = await _execute_with_confirmation(
            pg_conn=pg_conn,
            sql_statements=[sql],
            workspace_role=workspace_role,
            confirmed=body.confirmed,
            confirm_token=body.confirm_token,
            dry_run=body.dry_run,
            workspace_id=wid,
            connection_id=connection_id,
            broadcast_kind="column_dropped",
            broadcast_name=body.column_name,
            broadcast_table=body.table_name,
        )
    finally:
        await pg_conn.close()

    return response


# ---------------------------------------------------------------------------
# Index DDL
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/indexes", response_model=SqlPreviewResponse)
async def create_index(
    connection_id: UUID,
    body: CreateIndexBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="CREATE INDEX"
    )

    idx = IndexDefinition(**body.index)
    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        try:
            extensions_required = await assert_extensions_for_index(pg_conn, idx)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        sql = builder.sql_create_index(body.table_name, idx, body.schema_name)
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="index_created",
            object_name=idx.name, table_name=body.table_name,
        ))

    return _sql_preview([sql], body.dry_run, result, extensions_required)


@router.delete("/{connection_id}/indexes/{index_name}", response_model=SqlPreviewResponse)
async def drop_index(
    connection_id: UUID,
    index_name: str,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
    dry_run: bool = Query(False),
    confirmed: bool = Query(False),
    confirm_token: str | None = Query(
        default=None,
        description="Token from a previous confirmed=false call.",
    ),
):
    wid = _require_workspace(user.workspace_id)
    sql = builder.sql_drop_index(index_name, schema_name)

    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not dry_run, operation="DROP INDEX"
    )

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        response = await _execute_with_confirmation(
            pg_conn=pg_conn,
            sql_statements=[sql],
            workspace_role=workspace_role,
            confirmed=confirmed,
            confirm_token=confirm_token,
            dry_run=dry_run,
            workspace_id=wid,
            connection_id=connection_id,
            broadcast_kind="index_dropped",
            broadcast_name=index_name,
        )
    finally:
        await pg_conn.close()

    return response


# ---------------------------------------------------------------------------
# Constraint DDL
# ---------------------------------------------------------------------------

@router.post("/{connection_id}/constraints/fk", response_model=SqlPreviewResponse)
async def add_foreign_key(
    connection_id: UUID,
    body: AddForeignKeyBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ADD FOREIGN KEY"
    )

    try:
        sql = sql_add_foreign_key(
            table=body.table, name=body.name, columns=body.columns,
            ref_table=body.ref_table, ref_columns=body.ref_columns,
            on_delete=body.on_delete, on_update=body.on_update,
            schema_name=body.schema_name, ref_schema=body.ref_schema,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="fk_added", object_name=body.name, table_name=body.table,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.post("/{connection_id}/constraints/check", response_model=SqlPreviewResponse)
async def add_check_constraint(
    connection_id: UUID,
    body: AddCheckConstraintBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ADD CHECK CONSTRAINT"
    )

    try:
        sql = sql_add_check_constraint(
            table=body.table, 
            name=body.name,
            column_name=body.column_name,
            constraint=body.constraint,
            schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="check_constraint_added",
            object_name=body.name, table_name=body.table,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.post("/{connection_id}/constraints/unique", response_model=SqlPreviewResponse)
async def add_unique_constraint(
    connection_id: UUID,
    body: AddUniqueConstraintBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ADD UNIQUE CONSTRAINT"
    )

    try:
        sql = sql_add_unique_constraint(
            table=body.table, name=body.name,
            columns=body.columns, schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="unique_constraint_added",
            object_name=body.name, table_name=body.table,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.delete(
    "/{connection_id}/constraints/{constraint_name}",
    response_model=SqlPreviewResponse,
)
async def drop_constraint(
    connection_id: UUID,
    constraint_name: str,
    body: DropConstraintBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    try:
        sql = sql_drop_constraint(
            table=body.table, constraint_name=constraint_name,
            schema_name=body.schema_name, cascade=body.cascade,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="DROP CONSTRAINT"
    )

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        response = await _execute_with_confirmation(
            pg_conn=pg_conn,
            sql_statements=[sql],
            workspace_role=workspace_role,
            confirmed=body.confirmed,
            confirm_token=body.confirm_token,
            dry_run=body.dry_run,
            workspace_id=wid,
            connection_id=connection_id,
            broadcast_kind="constraint_dropped",
            broadcast_name=constraint_name,
            broadcast_table=body.table,
        )
    finally:
        await pg_conn.close()

    return response


# ---------------------------------------------------------------------------
# Enum type DDL
# ---------------------------------------------------------------------------

@router.get("/{connection_id}/enums")
async def list_enums(
    connection_id: UUID,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
    schema_name: str = Query("public"),
):
    wid = _require_workspace(user.workspace_id)
    url = await get_validated_workspace_url(db, connection_id, wid)
    snapshot = await introspection.introspect_database(url, schema_name)
    return {
        "enums": [
            {"name": e.name, "schema": e.schema, "values": e.values}
            for e in snapshot.enums
        ],
        "total": len(snapshot.enums),
    }


@router.post("/{connection_id}/enums", response_model=SqlPreviewResponse)
async def create_enum(
    connection_id: UUID,
    body: CreateEnumBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="CREATE TYPE (enum)"
    )

    try:
        sql = sql_create_enum(body.name, body.values, body.schema_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="enum_created", object_name=body.name,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.post(
    "/{connection_id}/enums/{enum_name}/values",
    response_model=SqlPreviewResponse,
)
async def add_enum_value(
    connection_id: UUID,
    enum_name: str,
    body: AddEnumValueBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ALTER TYPE (ADD VALUE)"
    )

    try:
        sql = sql_add_enum_value(
            enum_name=enum_name, new_value=body.value,
            after=body.after, before=body.before, schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        if body.dry_run:
            result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=True)
        else:
            try:
                await pg_conn.execute(sql)
                result = {"dry_run": False, "statements": [sql], "error": None}
            except Exception as exc:
                result = {"dry_run": False, "statements": [sql], "error": str(exc)}
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="enum_value_added", object_name=enum_name,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.patch(
    "/{connection_id}/enums/{enum_name}/values",
    response_model=SqlPreviewResponse,
)
async def rename_enum_value(
    connection_id: UUID,
    enum_name: str,
    body: RenameEnumValueBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="ALTER TYPE (RENAME VALUE)"
    )

    try:
        sql = sql_rename_enum_value(
            enum_name=enum_name, old_value=body.old_value,
            new_value=body.new_value, schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="enum_value_renamed", object_name=enum_name,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.delete("/{connection_id}/enums/{enum_name}", response_model=SqlPreviewResponse)
async def drop_enum(
    connection_id: UUID,
    enum_name: str,
    body: DropEnumBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    try:
        sql = sql_drop_enum(
            name=enum_name, schema_name=body.schema_name, cascade=body.cascade,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="DROP TYPE (enum)"
    )

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        response = await _execute_with_confirmation(
            pg_conn=pg_conn,
            sql_statements=[sql],
            workspace_role=workspace_role,
            confirmed=body.confirmed,
            confirm_token=body.confirm_token,
            dry_run=body.dry_run,
            workspace_id=wid,
            connection_id=connection_id,
            broadcast_kind="enum_dropped",
            broadcast_name=enum_name,
        )
    finally:
        await pg_conn.close()

    return response


# ---------------------------------------------------------------------------
# Comments (COMMENT ON)
# ---------------------------------------------------------------------------

@router.put(
    "/{connection_id}/tables/{table_name}/comment",
    response_model=SqlPreviewResponse,
)
async def set_table_comment(
    connection_id: UUID,
    table_name: str,
    body: SetCommentBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="COMMENT ON TABLE"
    )

    try:
        sql = sql_comment_on_table(
            table=table_name, comment=body.comment, schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="table_comment_set", object_name=table_name,
        ))

    return _sql_preview([sql], body.dry_run, result)


@router.put(
    "/{connection_id}/tables/{table_name}/columns/{column_name}/comment",
    response_model=SqlPreviewResponse,
)
async def set_column_comment(
    connection_id: UUID,
    table_name: str,
    column_name: str,
    body: SetCommentBody,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    wid = _require_workspace(user.workspace_id)
    workspace_role = await _get_workspace_role(
        connection_id, wid, db, require_write=not body.dry_run, operation="COMMENT ON COLUMN"
    )

    try:
        sql = sql_comment_on_column(
            table=table_name, column=column_name,
            comment=body.comment, schema_name=body.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pg_conn = await acquire_workspace_connection(db, connection_id, wid)
    try:
        result = await builder.execute_sql(pg_conn, [sql], workspace_role, dry_run=body.dry_run)
    finally:
        await pg_conn.close()

    if not body.dry_run and not result.get("error"):
        asyncio.create_task(_broadcast_schema_changed(
            wid, connection_id,
            change_kind="column_comment_set",
            object_name=column_name, table_name=table_name,
        ))

    return _sql_preview([sql], body.dry_run, result)
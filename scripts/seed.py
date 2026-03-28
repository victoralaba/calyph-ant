# scripts/seed.py
"""
Seed script.

Creates the minimum required data for a fresh Calyphant deployment:
  1. Super admin user
  2. Default personal workspace for super admin
  3. Base feature flag overrides in Redis (optional)

Run once after first migration:
  python -m scripts.seed
  # or via CLI:
  calyphant seed --yes

Environment variables required:
  SUPER_ADMIN_EMAIL    — email address for the super admin account
  SUPER_ADMIN_PASSWORD — password (min 12 chars recommended)
  All core config vars (DATABASE_URL, SECRET_KEY, ENCRYPTION_KEY, REDIS_URL)
"""

from __future__ import annotations

import asyncio
import os
import sys

# Ensure the project root is in the path when run directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main() -> None:
    from loguru import logger

    # ---------------------------------------------------------------------------
    # Bootstrap
    # ---------------------------------------------------------------------------
    from core.config import settings
    from core.db import init_db, init_redis, get_redis
    import core.db as _core_db
    from core.auth import hash_password
    from shared.telemetry import init_logging

    init_logging(is_production=False)
    await init_db()
    await init_redis()

    logger.info("Seed: starting")

    # ---------------------------------------------------------------------------
    # Read seed credentials from environment
    # ---------------------------------------------------------------------------
    admin_email = os.environ.get("SUPER_ADMIN_EMAIL")
    admin_password = os.environ.get("SUPER_ADMIN_PASSWORD")

    if not admin_email or not admin_password:
        logger.error(
            "SUPER_ADMIN_EMAIL and SUPER_ADMIN_PASSWORD must be set in environment."
        )
        sys.exit(1)

    if len(admin_password) < 8:
        logger.error("SUPER_ADMIN_PASSWORD must be at least 8 characters.")
        sys.exit(1)

    # ---------------------------------------------------------------------------
    # Create super admin user
    # ---------------------------------------------------------------------------

    # NOTE: _session_factory is accessed via the module reference, not a stale
    # import-time binding. init_db() sets core.db._session_factory; importing
    # it directly with `from core.db import _session_factory` captures None.
    session_factory = _core_db._session_factory
    if session_factory is None:
        logger.error("Database session factory was not initialised. Aborting.")
        sys.exit(1)

    async with session_factory() as db:
        from sqlalchemy import select
        from domains.users.service import User
        from domains.teams.service import Workspace, WorkspaceMember, WorkspaceRole

        # Check if super admin already exists
        existing = await db.execute(
            select(User).where(User.email == admin_email.lower())
        )
        user = existing.scalar_one_or_none()

        if user:
            logger.info(
                f"Seed: super admin already exists ({admin_email}) — skipping user creation"
            )
        else:
            user = User(
                email=admin_email.lower(),
                full_name="Super Admin",
                password_hash=hash_password(admin_password),
                is_active=True,
                is_verified=True,
                is_superadmin=True,
                tier="enterprise",
            )
            db.add(user)
            await db.flush()  # Populate user.id before workspace creation
            logger.info(f"Seed: created super admin user ({admin_email})")

        # Ensure super admin has a workspace
        ws_result = await db.execute(
            select(Workspace).where(Workspace.created_by == user.id)
        )
        workspace = ws_result.scalar_one_or_none()

        if not workspace:
            workspace = Workspace(
                name="Admin Workspace",
                slug=f"admin-workspace-{user.id.hex[:8]}",
                created_by=user.id,
            )
            db.add(workspace)
            await db.flush()

            member = WorkspaceMember(
                workspace_id=workspace.id,
                user_id=user.id,
                role=WorkspaceRole.owner,
            )
            db.add(member)
            logger.info(f"Seed: created admin workspace ({workspace.slug})")
        else:
            # Make sure the owner membership record exists
            member_result = await db.execute(
                select(WorkspaceMember).where(
                    WorkspaceMember.workspace_id == workspace.id,
                    WorkspaceMember.user_id == user.id,
                )
            )
            if not member_result.scalar_one_or_none():
                member = WorkspaceMember(
                    workspace_id=workspace.id,
                    user_id=user.id,
                    role=WorkspaceRole.owner,
                )
                db.add(member)
                logger.info("Seed: re-created missing owner membership record")

            logger.info(f"Seed: admin workspace already exists ({workspace.slug})")

        await db.commit()

    # ---------------------------------------------------------------------------
    # Seed default feature flags in Redis
    # ---------------------------------------------------------------------------
    try:
        redis = await get_redis()

        from domains.billing.flutterwave import set_user_flag

        all_features = [
            "basic_editor",
            "schema_viewer",
            "query_editor",
            "ai_assist",
            "backups",
            "migrations",
            "extensions",
            "team_collab",
            "monitoring",
            "advanced_diff",
        ]

        for feature in all_features:
            await set_user_flag(
                flag_name=feature,
                user_id=user.id,
                enabled=True,
                redis=redis,
                ttl_seconds=86400 * 3650,  # 10 years
            )

        logger.info(
            f"Seed: feature flags set for super admin ({len(all_features)} flags)"
        )
    except Exception as exc:
        logger.warning(f"Seed: could not set Redis feature flags: {exc}")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    logger.info("=" * 50)
    logger.info("Seed complete.")
    logger.info(f"  Super admin: {admin_email}")
    logger.info(f"  Workspace:   {workspace.slug}")
    logger.info(f"  Tier:        enterprise")
    logger.info("=" * 50)
    logger.info("Next steps:")
    logger.info("  1. Start the API:    uvicorn main:app --reload")
    logger.info("  2. Login at:         POST /auth/login")
    logger.info("  3. Connect a DB:     POST /connections")


if __name__ == "__main__":
    asyncio.run(main())
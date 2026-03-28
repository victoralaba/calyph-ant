"""Add history in db

Revision ID: 98619b81063d
Revises: 80be4cc31e05
Create Date: 2026-03-26 21:07:40.792730
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "98619b81063d"
down_revision: Union[str, None] = "80be4cc31e05"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add the column as nullable first to avoid immediate crash
    op.add_column("query_history", sa.Column("workspace_id", sa.UUID(), nullable=True))
    
    # 2. Clear existing history data so we don't have NULLs in a NOT NULL column
    op.execute("DELETE FROM query_history")
    
    # 3. Now set the column to NOT NULL
    op.alter_column("query_history", "workspace_id", nullable=False)

    # 4. Create indexes and foreign keys
    op.create_index(
        op.f("ix_query_history_workspace_id"),
        "query_history",
        ["workspace_id"],
        unique=False,
    )
    # Note: Named constraints are better practice for easier management later
    op.create_foreign_key(
        "fk_query_history_workspace_id",
        "query_history",
        "workspaces",
        ["workspace_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_query_history_user_id", 
        "query_history", 
        "users", 
        ["user_id"], 
        ["id"], 
        ondelete="CASCADE"
    )
    op.create_foreign_key(
        "fk_saved_queries_workspace_id",
        "saved_queries",
        "workspaces",
        ["workspace_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # Use the specific names we defined in the upgrade for reliability
    op.drop_constraint("fk_saved_queries_workspace_id", "saved_queries", type_="foreignkey")
    op.drop_constraint("fk_query_history_user_id", "query_history", type_="foreignkey")
    op.drop_constraint("fk_query_history_workspace_id", "query_history", type_="foreignkey")
    op.drop_index(op.f("ix_query_history_workspace_id"), table_name="query_history")
    op.drop_column("query_history", "workspace_id")
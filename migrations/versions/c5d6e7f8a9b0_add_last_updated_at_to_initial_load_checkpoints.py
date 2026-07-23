"""add last_updated_at to initial_load_checkpoints

Revision ID: c5d6e7f8a9b0
Revises: b4c5d6e7f8a9
Create Date: 2026-07-23 12:00:00.000000

v1.2.25 Bug 2.3: the GET /connections/{id}/initial-load endpoint aggregates
progress from initial_load_checkpoints, but there is no "last progress" time
available for a running load — only started_at (set once at insert) and
completed_at (set only when state=done). The UI cannot tell a stuck load
(running but no recent checkpoint) from a healthy one.

This migration adds last_updated_at, which the /internal/load-checkpoints
upsert endpoint stamps on every chunk report so the GET /initial-load handler
can surface "last progress N seconds ago" to the UI.
"""
from alembic import op
import sqlalchemy as sa

revision = 'c5d6e7f8a9b0'
down_revision = 'b4c5d6e7f8a9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('last_updated_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('initial_load_checkpoints', 'last_updated_at')

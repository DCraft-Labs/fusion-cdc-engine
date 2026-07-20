"""add transform_overrides to streams

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-05-08 00:00:00.000000

Restores the transform_overrides JSONB column that was dropped in migration
2512af1df83a. This is the canonical store for per-stream column + UDF
transformation specs coming from the UI wizard.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = 'b3c4d5e6f7a8'
down_revision = 'a2b3c4d5e6f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'streams',
        sa.Column(
            'transform_overrides',
            JSONB,
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )


def downgrade() -> None:
    op.drop_column('streams', 'transform_overrides')

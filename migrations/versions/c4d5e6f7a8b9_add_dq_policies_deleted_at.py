"""Add missing dq_policies.deleted_at column

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-07-20

Migration e5f6a7b8c9d0 added dq_policies.is_deleted but the DQPolicy model
(SoftDeleteMixin) also declares deleted_at, which was never added. This left
every /api/v1/data-quality/policies query raising
psycopg2.errors.UndefinedColumn: column dq_policies.deleted_at does not exist.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c4d5e6f7a8b9'
down_revision = 'b3c4d5e6f7a8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('dq_policies', sa.Column(
        'deleted_at',
        sa.DateTime(timezone=True),
        nullable=True,
    ))


def downgrade():
    op.drop_column('dq_policies', 'deleted_at')

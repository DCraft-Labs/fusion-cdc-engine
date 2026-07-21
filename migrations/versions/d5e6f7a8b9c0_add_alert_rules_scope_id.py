"""Add alert_rules.scope_id column

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-07-22

The AlertRule model (control-plane/app/models/alerting.py) declares
`scope_id = Column(UUID(as_uuid=True), nullable=True)`, but the original
`2512af1df83a_add_alerting_tables` migration never created this column. As a
result every POST/GET to /api/v1/alerts/rules raised
psycopg2.errors.UndefinedColumn: column alert_rules.scope_id does not exist
(HTTP 500). This migration adds the missing nullable UUID column so the
alerting API can persist and query scope_id alongside scope_type.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd5e6f7a8b9c0'
down_revision = 'c4d5e6f7a8b9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('alert_rules', sa.Column(
        'scope_id',
        sa.UUID(as_uuid=True),
        nullable=True,
    ))
    op.create_index(
        op.f('ix_alert_rules_scope_id'),
        'alert_rules',
        ['scope_id'],
        unique=False,
    )


def downgrade():
    op.drop_index(op.f('ix_alert_rules_scope_id'), table_name='alert_rules')
    op.drop_column('alert_rules', 'scope_id')

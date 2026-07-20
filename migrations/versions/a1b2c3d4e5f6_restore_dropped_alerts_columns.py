"""restore dropped alerts columns

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-05-07

Migration 2512af1df83a incorrectly dropped 7 columns from the alerts table that
are still required by the model and API:
  - alert_context, acknowledged, acknowledged_at, acknowledged_by,
    resolved, resolved_at, resolution_notes, webhook_sent, webhook_sent_at,
    webhook_response_status
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'f1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add back columns dropped by 2512af1df83a — use IF NOT EXISTS to be idempotent
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS alert_context JSONB")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged BOOLEAN DEFAULT false")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMP WITH TIME ZONE")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_by UUID")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolved BOOLEAN DEFAULT false")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP WITH TIME ZONE")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolution_notes TEXT")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS webhook_sent BOOLEAN DEFAULT false")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS webhook_sent_at TIMESTAMP WITH TIME ZONE")
    op.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS webhook_response_status INTEGER")

    # Make sub_tenant_id and bank_id nullable (superadmin has no tenant context)
    op.execute("ALTER TABLE alerts ALTER COLUMN sub_tenant_id DROP NOT NULL")
    op.execute("ALTER TABLE alerts ALTER COLUMN bank_id DROP NOT NULL")


def downgrade() -> None:
    op.drop_column('alerts', 'alert_context')
    op.drop_column('alerts', 'acknowledged')
    op.drop_column('alerts', 'acknowledged_at')
    op.drop_column('alerts', 'acknowledged_by')
    op.drop_column('alerts', 'resolved')
    op.drop_column('alerts', 'resolved_at')
    op.drop_column('alerts', 'resolution_notes')
    op.drop_column('alerts', 'webhook_sent')
    op.drop_column('alerts', 'webhook_sent_at')
    op.drop_column('alerts', 'webhook_response_status')
    op.alter_column('alerts', 'sub_tenant_id', nullable=False)
    op.alter_column('alerts', 'bank_id', nullable=False)

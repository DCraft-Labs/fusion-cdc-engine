"""add remaining missing columns to connections table

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-05-07

After adding sync_type in c3d4e5f6a7b8, the scheduler still fails because
the K8s DB connections table is also missing schedule_cron and many other
columns that exist in the ORM model but were not in the original DB schema.

All ADD COLUMN statements use IF NOT EXISTS for idempotency.
"""
from alembic import op

revision = 'd4e5f6a7b8c9'
down_revision = 'c3d4e5f6a7b8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE connections
            ADD COLUMN IF NOT EXISTS schedule_cron VARCHAR(100),
            ADD COLUMN IF NOT EXISTS transform_pipeline_id UUID,
            ADD COLUMN IF NOT EXISTS dq_policy_id UUID,
            ADD COLUMN IF NOT EXISTS schema_evolution_policy VARCHAR(50) DEFAULT 'MANUAL_APPROVAL',
            ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS paused_by UUID,
            ADD COLUMN IF NOT EXISTS pause_reason TEXT,
            ADD COLUMN IF NOT EXISTS resumed_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS resumed_by UUID,
            ADD COLUMN IF NOT EXISTS resource_limits JSONB DEFAULT '{}',
            ADD COLUMN IF NOT EXISTS initial_load_completed BOOLEAN DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS initial_load_started_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS initial_load_completed_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS created_by UUID
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE connections
            DROP COLUMN IF EXISTS schedule_cron,
            DROP COLUMN IF EXISTS transform_pipeline_id,
            DROP COLUMN IF EXISTS dq_policy_id,
            DROP COLUMN IF EXISTS schema_evolution_policy,
            DROP COLUMN IF EXISTS paused_at,
            DROP COLUMN IF EXISTS paused_by,
            DROP COLUMN IF EXISTS pause_reason,
            DROP COLUMN IF EXISTS resumed_at,
            DROP COLUMN IF EXISTS resumed_by,
            DROP COLUMN IF EXISTS resource_limits,
            DROP COLUMN IF EXISTS initial_load_completed,
            DROP COLUMN IF EXISTS initial_load_started_at,
            DROP COLUMN IF EXISTS initial_load_completed_at,
            DROP COLUMN IF EXISTS created_by
    """)

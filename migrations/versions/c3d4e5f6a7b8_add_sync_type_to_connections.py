"""add missing columns to connections table

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-05-07

The connections table in the K8s DB was created from an older schema that was
missing several columns that the ORM model and scheduler now query. This
migration adds ALL missing columns using IF NOT EXISTS so it is safe to run
on DBs that already have some of them.

Columns added:
  - sync_type       (scheduler queries this)
  - schedule_cron   (ORM model column)
  - transform_pipeline_id
  - dq_policy_id
  - schema_evolution_policy
  - paused_at, paused_by, pause_reason
  - resumed_at, resumed_by
  - resource_limits
  - initial_load_completed, initial_load_started_at, initial_load_completed_at
  - created_by
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c3d4e5f6a7b8'
down_revision = 'b2c3d4e5f6a7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE connections
            ADD COLUMN IF NOT EXISTS sync_type VARCHAR(50) NOT NULL DEFAULT 'REALTIME',
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
            DROP COLUMN IF EXISTS sync_type,
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

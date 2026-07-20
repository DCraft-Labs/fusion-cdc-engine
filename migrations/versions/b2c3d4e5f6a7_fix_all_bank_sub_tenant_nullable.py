"""fix all bank_id sub_tenant_id nullable across all tables

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-05-07

All bank_id/sub_tenant_id columns must be nullable=True — superadmin users
have no bank/tenant context and should be able to operate on all tables.
Tables fixed: notification_channels, alert_rules, alert_suppressions,
redis_stream_tracking, spark_job_queue, event_dead_letter_queue,
resource_usage, resource_quota_violations, tenant_daily_usage.
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b2c3d4e5f6a7'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None

# All tables that still have NOT NULL on bank_id / sub_tenant_id in the live DB
TABLES = [
    'notification_channels',
    'alert_rules',
    'alert_suppressions',
    'redis_stream_tracking',
    'spark_job_queue',
    'event_dead_letter_queue',
    'resource_usage',
    'resource_quota_violations',
    'tenant_daily_usage',
]


def upgrade() -> None:
    for table in TABLES:
        op.alter_column(table, 'bank_id', nullable=True)
        op.alter_column(table, 'sub_tenant_id', nullable=True)


def downgrade() -> None:
    for table in TABLES:
        op.alter_column(table, 'bank_id', nullable=False)
        op.alter_column(table, 'sub_tenant_id', nullable=False)

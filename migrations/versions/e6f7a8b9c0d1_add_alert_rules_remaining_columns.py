"""Add remaining alert_rules columns declared by the AlertRule model

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-07-22

The v1.2.4 migration `d5e6f7a8b9c0` only added `scope_id`. The AlertRule
model (control-plane/app/models/alerting.py) declares three additional
columns that the original `2512af1df83a_add_alerting_tables` migration
never created:

  - threshold_value (Numeric, nullable=True)
  - consecutive_failures_required (Integer, NOT NULL, server_default 1)
  - cooldown_minutes (Integer, NOT NULL, server_default 15)

Without these, every POST/GET to /api/v1/alerts/rules raised
psycopg2.errors.UndefinedColumn (HTTP 500) because SQLAlchemy emits
INSERT/SELECT statements referencing columns the table does not have.

This migration adds the three missing columns with types/defaults that
exactly match the model declarations. It does NOT drop the legacy
columns that exist in the table but are not referenced by the model
(stream_id, evaluation_interval_minutes, consecutive_failures, group_by,
suppression_window_minutes, last_evaluated_at, last_triggered_at, tags,
custom_labels) — those are harmless extras and dropping them would risk
breaking other readers.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e6f7a8b9c0d1'
down_revision = 'd5e6f7a8b9c0'
branch_labels = None
depends_on = None


def upgrade():
    # threshold_value: Numeric, nullable=True  (alerting.py:100)
    op.add_column(
        'alert_rules',
        sa.Column('threshold_value', sa.Numeric(), nullable=True),
    )
    # consecutive_failures_required: Integer, NOT NULL, server_default 1
    # (alerting.py:103)
    op.add_column(
        'alert_rules',
        sa.Column(
            'consecutive_failures_required',
            sa.Integer(),
            nullable=False,
            server_default=sa.text('1'),
        ),
    )
    # cooldown_minutes: Integer, NOT NULL, server_default 15  (alerting.py:105)
    op.add_column(
        'alert_rules',
        sa.Column(
            'cooldown_minutes',
            sa.Integer(),
            nullable=False,
            server_default=sa.text('15'),
        ),
    )


def downgrade():
    op.drop_column('alert_rules', 'cooldown_minutes')
    op.drop_column('alert_rules', 'consecutive_failures_required')
    op.drop_column('alert_rules', 'threshold_value')

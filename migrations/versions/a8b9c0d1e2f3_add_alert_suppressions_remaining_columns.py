"""Add remaining alert_suppressions columns declared by the AlertSuppression model

Revision ID: a8b9c0d1e2f3
Revises: f7a8b9c0d1e2
Create Date: 2026-07-22

The v1.2.5 migration `f7a8b9c0d1e2` only added the `rule_ids` / `connection_ids`
ARRAY columns (and dropped the legacy single-valued `rule_id` / `connection_id`).
But the `AlertSuppression` model (control-plane/app/models/alerting.py:309-345)
declares three additional columns that NO migration ever created:

  - is_recurring        Boolean, NOT NULL, server_default false  (alerting.py:337)
  - recurrence_pattern  JSONB, nullable=True                    (alerting.py:338)
  - updated_by          UUID, nullable=True                      (alerting.py:345)

Without these, every POST/GET to /api/v1/alerts/suppressions raised
psycopg2.errors.UndefinedColumn: column alert_suppressions.is_recurring
does not exist (HTTP 500) the moment SQLAlchemy emitted INSERT/SELECT
statements referencing the missing columns. The v1.2.7 live audit
confirmed /api/v1/alerts/suppressions still returns 500 with an empty
body even after v1.2.5 shipped `f7a8b9c0d1e2` — because that migration
did not add these three columns.

This migration adds the three missing columns with types/defaults that
exactly match the model declarations. It does NOT drop the legacy
`reason` / `alert_type` columns that exist in the table but are not
referenced by the model — those are harmless extras and dropping them
would risk breaking other readers.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'a8b9c0d1e2f3'
down_revision = 'f7a8b9c0d1e2'
branch_labels = None
depends_on = None


def upgrade():
    # is_recurring: Boolean, NOT NULL, server_default false  (alerting.py:337)
    op.add_column(
        'alert_suppressions',
        sa.Column(
            'is_recurring',
            sa.Boolean(),
            nullable=False,
            server_default=sa.text('false'),
        ),
    )
    # recurrence_pattern: JSONB, nullable=True  (alerting.py:338)
    op.add_column(
        'alert_suppressions',
        sa.Column(
            'recurrence_pattern',
            postgresql.JSONB(),
            nullable=True,
        ),
    )
    # updated_by: UUID, nullable=True  (alerting.py:345)
    op.add_column(
        'alert_suppressions',
        sa.Column(
            'updated_by',
            sa.UUID(as_uuid=True),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column('alert_suppressions', 'updated_by')
    op.drop_column('alert_suppressions', 'recurrence_pattern')
    op.drop_column('alert_suppressions', 'is_recurring')

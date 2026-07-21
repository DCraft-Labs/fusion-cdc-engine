"""Align alert_suppressions with model: rule_ids/connection_ids arrays

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-07-22

The AlertSuppression model (control-plane/app/models/alerting.py:331-332)
and the API schemas (control-plane/app/schemas/alerting.py:522-523) both
declare:

    rule_ids        = Column(ARRAY(UUID(as_uuid=True)), nullable=True)
    connection_ids  = Column(ARRAY(UUID(as_uuid=True)), nullable=True)

because a suppression can target multiple rules / connections at once.

But the original `2512af1df83a_add_alerting_tables` migration created the
columns as single-valued:

    rule_id        UUID  (nullable)
    connection_id  UUID  (nullable)

with indexes `ix_alert_suppressions_rule_id` and
`ix_alert_suppressions_connection_id`.

As a result every POST/GET to /api/v1/alerts/suppressions raised
psycopg2.errors.UndefinedColumn: column alert_suppressions.rule_ids does
not exist (HTTP 500).

The model is the source of truth (the code expects arrays). This migration:
  1. Adds `rule_ids` and `connection_ids` as ARRAY(UUID) nullable.
  2. Back-fills them from the legacy single-valued columns.
  3. Drops the legacy `rule_id` / `connection_id` columns and their indexes.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'f7a8b9c0d1e2'
down_revision = 'e6f7a8b9c0d1'
branch_labels = None
depends_on = None


def upgrade():
    # 1. Add the new array columns the model declares.
    op.add_column(
        'alert_suppressions',
        sa.Column(
            'rule_ids',
            postgresql.ARRAY(sa.UUID(as_uuid=True)),
            nullable=True,
        ),
    )
    op.add_column(
        'alert_suppressions',
        sa.Column(
            'connection_ids',
            postgresql.ARRAY(sa.UUID(as_uuid=True)),
            nullable=True,
        ),
    )

    # 2. Back-fill from the legacy single-valued columns (wrap in array).
    op.execute(
        "UPDATE alert_suppressions "
        "SET rule_ids = ARRAY[rule_id] WHERE rule_id IS NOT NULL"
    )
    op.execute(
        "UPDATE alert_suppressions "
        "SET connection_ids = ARRAY[connection_id] WHERE connection_id IS NOT NULL"
    )

    # 3. Drop legacy indexes and columns that the model no longer references.
    op.drop_index(
        op.f('ix_alert_suppressions_rule_id'),
        table_name='alert_suppressions',
    )
    op.drop_index(
        op.f('ix_alert_suppressions_connection_id'),
        table_name='alert_suppressions',
    )
    op.drop_column('alert_suppressions', 'rule_id')
    op.drop_column('alert_suppressions', 'connection_id')


def downgrade():
    # Restore the legacy single-valued columns and indexes.
    op.add_column(
        'alert_suppressions',
        sa.Column('rule_id', sa.UUID(), nullable=True),
    )
    op.add_column(
        'alert_suppressions',
        sa.Column('connection_id', sa.UUID(), nullable=True),
    )
    op.create_index(
        op.f('ix_alert_suppressions_rule_id'),
        'alert_suppressions',
        ['rule_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_alert_suppressions_connection_id'),
        'alert_suppressions',
        ['connection_id'],
        unique=False,
    )
    # Best-effort: copy first element of the arrays back into the single columns.
    op.execute(
        "UPDATE alert_suppressions SET rule_id = rule_ids[1] "
        "WHERE rule_ids IS NOT NULL AND array_length(rule_ids, 1) >= 1"
    )
    op.execute(
        "UPDATE alert_suppressions SET connection_id = connection_ids[1] "
        "WHERE connection_ids IS NOT NULL AND array_length(connection_ids, 1) >= 1"
    )
    op.drop_column('alert_suppressions', 'connection_ids')
    op.drop_column('alert_suppressions', 'rule_ids')

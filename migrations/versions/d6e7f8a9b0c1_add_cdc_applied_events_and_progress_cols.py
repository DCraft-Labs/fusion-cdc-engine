"""add cdc_applied_events + progress columns to initial_load_checkpoints

Revision ID: d6e7f8a9b0c1
Revises: c5d6e7f8a9b0
Create Date: 2026-07-23 00:00:00.000000

v1.2.29:

Task 3 (real-time UI progress + ETA): add ``pk_start``, ``pk_end`` and
``rows_estimated`` to ``initial_load_checkpoints`` so the control-plane can
compute per-partition progress % and ETA without re-querying the source DB.
  - pk_start       TEXT, nullable — lower PK bound of this partition's range
  - pk_end         TEXT, nullable — upper PK bound of this partition's range
  - rows_estimated BIGINT, nullable — estimated row count for this partition
                                      (NULL when unknown; populated from a
                                      pre-count or MIN/MAX heuristic)

Task 4 (CDC streaming idempotency): create ``cdc_applied_events`` so the CDC
consumer can check whether an ``event_id`` has already been applied before
upserting the row to the destination — this gives exactly-once semantics
across consumer restarts / Redis Stream re-delivery without altering the
schema of every destination table.
  - cdc_applied_events(event_id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                       connection_id TEXT, stream_id TEXT, table_name TEXT)
"""
from alembic import op
import sqlalchemy as sa

revision = 'd6e7f8a9b0c1'
down_revision = 'c5d6e7f8a9b0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Task 3: progress columns on initial_load_checkpoints.
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('pk_start', sa.Text, nullable=True),
    )
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('pk_end', sa.Text, nullable=True),
    )
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('rows_estimated', sa.BigInteger, nullable=True),
    )

    # Task 4: CDC idempotency dedup table.
    op.create_table(
        'cdc_applied_events',
        sa.Column('event_id', sa.Text, primary_key=True, nullable=False),
        sa.Column('applied_at', sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text('now()')),
        sa.Column('connection_id', sa.Text, nullable=True),
        sa.Column('stream_id', sa.Text, nullable=True),
        sa.Column('table_name', sa.Text, nullable=True),
    )
    op.create_index(
        'ix_cdc_applied_events_applied_at',
        'cdc_applied_events',
        ['applied_at'],
    )


def downgrade() -> None:
    op.drop_index('ix_cdc_applied_events_applied_at', table_name='cdc_applied_events')
    op.drop_table('cdc_applied_events')
    op.drop_column('initial_load_checkpoints', 'rows_estimated')
    op.drop_column('initial_load_checkpoints', 'pk_end')
    op.drop_column('initial_load_checkpoints', 'pk_start')

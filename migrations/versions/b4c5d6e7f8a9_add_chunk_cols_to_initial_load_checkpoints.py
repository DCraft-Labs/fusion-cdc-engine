"""add chunk columns to initial_load_checkpoints

Revision ID: b4c5d6e7f8a9
Revises: a8b9c0d1e2f3
Create Date: 2026-07-23 00:00:00.000000

v1.2.17: enables PK-bounded chunked initial loads in the transform-worker.
The existing table only tracked per-stream (per-table) granularity, so a
crash mid-load re-TRUNCATEd and re-did the whole table. These columns let
the worker resume from the last processed PK after a restart / OOM-kill:

  - chunk_seq     BIGINT, default 0  — last completed chunk sequence number
  - last_pk       TEXT, nullable      — stringified last PK value processed
  - total_chunks  BIGINT, nullable    — total chunk count (NULL when unknown,
                                         e.g. when no pre-count is performed)
  - current_chunk BIGINT, default 0   — 1-based index of the chunk in flight
"""
from alembic import op
import sqlalchemy as sa

revision = 'b4c5d6e7f8a9'
down_revision = 'a8b9c0d1e2f3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('chunk_seq', sa.BigInteger, nullable=False, server_default='0'),
    )
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('last_pk', sa.Text, nullable=True),
    )
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('total_chunks', sa.BigInteger, nullable=True),
    )
    op.add_column(
        'initial_load_checkpoints',
        sa.Column('current_chunk', sa.BigInteger, nullable=False, server_default='0'),
    )


def downgrade() -> None:
    op.drop_column('initial_load_checkpoints', 'current_chunk')
    op.drop_column('initial_load_checkpoints', 'total_chunks')
    op.drop_column('initial_load_checkpoints', 'last_pk')
    op.drop_column('initial_load_checkpoints', 'chunk_seq')

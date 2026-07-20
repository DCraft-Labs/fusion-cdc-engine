"""add initial_load_checkpoints table

Revision ID: a2b3c4d5e6f7
Revises: f6a7b8c9d0e1
Create Date: 2026-05-08 00:00:00.000000

Enables per-stream resume of initial load after OOM / crash.
"""
from alembic import op
import sqlalchemy as sa

revision = 'a2b3c4d5e6f7'
down_revision = 'f6a7b8c9d0e1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'initial_load_checkpoints',
        sa.Column('checkpoint_id', sa.dialects.postgresql.UUID(as_uuid=True),
                  server_default=sa.text('uuid_generate_v4()'), primary_key=True),
        sa.Column('connection_id', sa.dialects.postgresql.UUID(as_uuid=True),
                  sa.ForeignKey('connections.connection_id', ondelete='CASCADE'), nullable=False),
        sa.Column('stream_id', sa.dialects.postgresql.UUID(as_uuid=True),
                  sa.ForeignKey('streams.stream_id', ondelete='CASCADE'), nullable=False),
        sa.Column('source_table', sa.String(255), nullable=False),
        sa.Column('rows_written', sa.BigInteger, nullable=False, server_default='0'),
        sa.Column('status', sa.String(50), nullable=False, server_default='running'),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error', sa.Text, nullable=True),
        sa.UniqueConstraint('connection_id', 'stream_id', name='uq_ilc_connection_stream'),
    )
    op.create_index('ix_ilc_connection', 'initial_load_checkpoints', ['connection_id'])


def downgrade() -> None:
    op.drop_index('ix_ilc_connection')
    op.drop_table('initial_load_checkpoints')

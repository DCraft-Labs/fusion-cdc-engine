"""fix worker_heartbeats connection_id nullable

Revision ID: 3a9f2e1b4c8d
Revises: 2512af1df83a
Create Date: 2026-05-07 06:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '3a9f2e1b4c8d'
down_revision: Union[str, Sequence[str], None] = '2512af1df83a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # connection_id was incorrectly set to NOT NULL by the previous migration.
    # A CDC worker that is idle / just starting has no active connection,
    # so this column must be nullable.
    op.alter_column('worker_heartbeats', 'connection_id',
                    existing_type=sa.UUID(),
                    nullable=True)


def downgrade() -> None:
    op.alter_column('worker_heartbeats', 'connection_id',
                    existing_type=sa.UUID(),
                    nullable=False)

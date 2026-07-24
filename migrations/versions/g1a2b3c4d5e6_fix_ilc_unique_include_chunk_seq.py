"""fix initial_load_checkpoints unique constraint to include chunk_seq

Revision ID: g1a2b3c4d5e6
Revises: d6e7f8a9b0c1
Create Date: 2026-07-24 21:00:00.000000

v1.3.6 Bug #4: the original unique constraint was (connection_id, stream_id)
only. Parallel initial load (K>1) needs one checkpoint row per chunk_seq;
without chunk_seq in the unique key, K-1 partitions collide on insert and
lose resume state.

Idempotent: a live ALTER during the throughput investigation may already
have dropped/renamed the constraint — upgrade is a no-op when the new
constraint already exists.
"""
from alembic import op
import sqlalchemy as sa


revision = "g1a2b3c4d5e6"
down_revision = "d6e7f8a9b0c1"
branch_labels = None
depends_on = None


def _unique_constraint_names(bind, table_name: str) -> set[str]:
    insp = sa.inspect(bind)
    try:
        uniques = insp.get_unique_constraints(table_name)
    except Exception:
        return set()
    names = set()
    for u in uniques or []:
        name = u.get("name")
        if name:
            names.add(name)
    return names


def upgrade() -> None:
    bind = op.get_bind()
    names = _unique_constraint_names(bind, "initial_load_checkpoints")
    if "uq_ilc_connection_stream" in names:
        op.drop_constraint(
            "uq_ilc_connection_stream",
            "initial_load_checkpoints",
            type_="unique",
        )
        names.discard("uq_ilc_connection_stream")
    if "uq_ilc_connection_stream_chunk" not in names:
        op.create_unique_constraint(
            "uq_ilc_connection_stream_chunk",
            "initial_load_checkpoints",
            ["connection_id", "stream_id", "chunk_seq"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    names = _unique_constraint_names(bind, "initial_load_checkpoints")
    if "uq_ilc_connection_stream_chunk" in names:
        op.drop_constraint(
            "uq_ilc_connection_stream_chunk",
            "initial_load_checkpoints",
            type_="unique",
        )
    names = _unique_constraint_names(bind, "initial_load_checkpoints")
    if "uq_ilc_connection_stream" not in names:
        op.create_unique_constraint(
            "uq_ilc_connection_stream",
            "initial_load_checkpoints",
            ["connection_id", "stream_id"],
        )

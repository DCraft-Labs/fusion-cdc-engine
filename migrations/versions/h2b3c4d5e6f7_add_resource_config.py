"""add resource_configs table (admission-control resource pool sizing)

Revision ID: h2b3c4d5e6f7
Revises: g1a2b3c4d5e6
Create Date: 2026-07-27 00:00:00.000000

Backend half of the resource-aware admission-control system for initial-load
sizing. ``resource_configs`` is a bank-scoped singleton (one row per
``bank_id``) capturing the one-time admin setup of the total compute pool
(CPU/memory min-max, instance type/count, dedicated-vs-shared). See
``app.models.resource_config.ResourceConfig`` for field rationale and
``app.services.resource_ledger`` / ``app.services.resource_admission`` for
how it feeds the Redis reservation ledger.

Idempotent: safe to re-run if the table already exists (mirrors the
idempotency style of g1a2b3c4d5e6).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "h2b3c4d5e6f7"
down_revision = "g1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if "resource_configs" in insp.get_table_names():
        return

    op.create_table(
        "resource_configs",
        sa.Column("resource_config_id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), nullable=False),
        sa.Column("bank_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("sub_tenant_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("total_cpu_min_millis", sa.Integer(), nullable=False),
        sa.Column("total_cpu_max_millis", sa.Integer(), nullable=False),
        sa.Column("total_memory_min_mi", sa.Integer(), nullable=False),
        sa.Column("total_memory_max_mi", sa.Integer(), nullable=False),
        sa.Column("instance_type", sa.String(length=100), nullable=False),
        sa.Column("instance_count", sa.Integer(), nullable=False),
        sa.Column("pool_scope", sa.String(length=20), server_default=sa.text("'dedicated'::character varying"), nullable=False),
        sa.Column("resource_configured", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("resource_config_id"),
        sa.UniqueConstraint("bank_id", name="uq_resource_config_bank_id"),
    )
    op.create_index(op.f("ix_resource_configs_bank_id"), "resource_configs", ["bank_id"], unique=False)
    op.create_index(op.f("ix_resource_configs_sub_tenant_id"), "resource_configs", ["sub_tenant_id"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if "resource_configs" not in insp.get_table_names():
        return
    op.drop_index(op.f("ix_resource_configs_sub_tenant_id"), table_name="resource_configs")
    op.drop_index(op.f("ix_resource_configs_bank_id"), table_name="resource_configs")
    op.drop_table("resource_configs")

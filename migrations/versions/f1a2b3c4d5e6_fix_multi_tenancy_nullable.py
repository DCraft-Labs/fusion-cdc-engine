"""fix multi-tenancy bank_id sub_tenant_id nullable

Revision ID: f1a2b3c4d5e6
Revises: 3a9f2e1b4c8d
Create Date: 2026-05-07

bank_id and sub_tenant_id must be nullable — superadmin users have no bank/tenant context.
Affects all tables using MultiTenancyMixin: sources, destinations, connections,
transform_pipelines, udf_catalog, dq_policies.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'f1a2b3c4d5e6'
down_revision = '3a9f2e1b4c8d'
branch_labels = None
depends_on = None

TABLES = [
    'sources',
    'destinations',
    'connections',
    'transform_pipelines',
    'udf_catalog',
    'dq_policies',
]


def upgrade() -> None:
    for table in TABLES:
        op.alter_column(table, 'bank_id', nullable=True)
        op.alter_column(table, 'sub_tenant_id', nullable=True)


def downgrade() -> None:
    for table in TABLES:
        op.alter_column(table, 'bank_id', nullable=False)
        op.alter_column(table, 'sub_tenant_id', nullable=False)

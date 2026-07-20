"""Add missing columns: destinations.connection_config and dq_policies.is_deleted

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-05-07 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = 'e5f6a7b8c9d0'
down_revision = 'd4e5f6a7b8c9'
branch_labels = None
depends_on = None


def upgrade():
    # Add connection_config to destinations (nullable first, then populate, then constrain)
    op.add_column('destinations', sa.Column(
        'connection_config',
        JSONB,
        nullable=True,
        server_default=sa.text("'{}'::jsonb")
    ))

    # Populate connection_config from separate columns for existing rows
    op.execute("""
        UPDATE destinations
        SET connection_config = jsonb_strip_nulls(jsonb_build_object(
            'host',             host,
            'port',             port,
            'database_name',    database_name,
            'schema_name',      schema_name,
            'username',         username,
            'password_encrypted', password_encrypted,
            'ssl_enabled',      ssl_enabled
        ))
        WHERE connection_config IS NULL OR connection_config = '{}'::jsonb
    """)

    # Set NOT NULL now that all rows are populated
    op.alter_column('destinations', 'connection_config', nullable=False)

    # Add is_deleted to dq_policies
    op.add_column('dq_policies', sa.Column(
        'is_deleted',
        sa.Boolean(),
        nullable=False,
        server_default=sa.text('false')
    ))


def downgrade():
    op.drop_column('destinations', 'connection_config')
    op.drop_column('dq_policies', 'is_deleted')

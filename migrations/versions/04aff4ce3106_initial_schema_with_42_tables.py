"""Initial schema with 42 tables

Revision ID: 04aff4ce3106
Revises: 
Create Date: 2025-11-30 12:02:50.311747

"""
from typing import Sequence, Union
import os
from pathlib import Path

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '04aff4ce3106'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Upgrade schema by executing the PostgreSQL schema file.
    This creates all 42 tables with proper indexes, constraints, and triggers.
    """
    # Get path to schema file
    schema_dir = Path(__file__).parent.parent.parent / "schemas"
    schema_file = schema_dir / "schema_postgres.sql"
    
    if not schema_file.exists():
        raise FileNotFoundError(
            f"Schema file not found: {schema_file}\n"
            f"Please ensure schema_postgres.sql exists in the schemas directory."
        )
    
    # Read schema file
    with open(schema_file, 'r') as f:
        sql_content = f.read()

    # Use the raw psycopg2 DBAPI cursor to execute the SQL verbatim.
    # Both sa.text() and exec_driver_sql() pass execution options that psycopg2
    # misinterprets as parameters; the raw cursor.execute(sql) with no second
    # argument performs zero parameter substitution — safe for '%' in comments.
    conn = op.get_bind()
    raw_conn = conn.connection.dbapi_connection
    cursor = raw_conn.cursor()
    try:
        cursor.execute(sql_content)
    finally:
        cursor.close()


def downgrade() -> None:
    """
    Downgrade schema by dropping all tables.
    WARNING: This will delete all data!
    """
    # List of all 42 tables in reverse dependency order
    tables = [
        # Drop dependent tables first
        'event_dlq_retry_history',
        'dq_violation_samples',
        'dq_violations',
        'dq_rule_results',
        'transformation_logs',
        'transformation_dependencies',
        'udf_execution_stats',
        'connection_alert_webhooks',
        'connection_health_checks',
        'worker_heartbeats',
        'alerts',
        'resource_quota_violations',
        'resource_usage',
        'tenant_daily_usage',
        'redis_stream_tracking',
        'event_dead_letter_queue',
        'json_schema_evolution',
        'json_flatten_rules',
        'json_schema_cache',
        'schema_change_events',
        'cdc_lag_metrics',
        'cdc_position_history',
        'checkpoint_state',
        'spark_executor_history',
        'spark_executors',
        'spark_job_queue',
        'spark_applications',
        'connection_runs',
        'sync_mode_config',
        'streams',
        'connections',
        'destinations',
        'sources',
        'connector_versions',
        'connector_definitions',
        'udf_catalog',
        'dq_policies',
        'transform_pipelines',
        'maintenance_windows',
        'feature_flags',
        'system_config',
        'audit_log',
        'audit_log_2025_11',  # partitions
        'audit_log_2025_12',
    ]
    
    # Drop views first
    op.execute('DROP VIEW IF EXISTS v_tenant_resource_summary CASCADE')
    op.execute('DROP VIEW IF EXISTS v_cdc_lag_summary CASCADE')
    op.execute('DROP VIEW IF EXISTS v_active_connections CASCADE')
    
    # Drop tables
    for table in tables:
        op.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
    
    # Drop function
    op.execute('DROP FUNCTION IF EXISTS update_updated_at_column CASCADE')
    
    print("All 42 tables dropped successfully")


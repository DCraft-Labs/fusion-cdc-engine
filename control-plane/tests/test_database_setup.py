"""
Database Setup Verification Tests
Tests to verify that all 42 tables are created correctly with proper schema
"""
import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import os


# Database connection URL
DATABASE_URL = "postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata"


@pytest.fixture(scope="module")
def db_engine():
    """Create database engine for testing"""
    engine = create_engine(DATABASE_URL)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def db_session(db_engine):
    """Create database session for testing"""
    SessionLocal = sessionmaker(bind=db_engine)
    session = SessionLocal()
    yield session
    session.close()


def test_database_connection(db_engine):
    """Test that we can connect to the database"""
    with db_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1


def test_table_count(db_engine):
    """Verify that exactly 42 tables exist (including alembic_version)"""
    inspector = inspect(db_engine)
    tables = inspector.get_table_names(schema='public')
    # Total should be 42 (41 application tables + 1 alembic_version)
    assert len(tables) == 42, f"Expected 42 tables but found {len(tables)}: {tables}"
    
    # Filter out alembic_version for application tables count
    app_tables = [t for t in tables if t != 'alembic_version']
    assert len(app_tables) == 41, f"Expected 41 application tables but found {len(app_tables)}"


def test_required_tables_exist(db_engine):
    """Test that all required tables exist"""
    inspector = inspect(db_engine)
    tables = inspector.get_table_names(schema='public')
    
    required_tables = [
        # Core tables
        'connector_definitions',
        'connector_versions',
        'sources',
        'destinations',
        'connections',
        'streams',
        'transform_pipelines',
        'udf_catalog',
        
        # CDC and monitoring
        'checkpoint_state',
        'cdc_position_history',
        'cdc_lag_metrics',
        'worker_heartbeats',
        'redis_stream_tracking',
        
        # Data Quality
        'dq_policies',
        'dq_violations',
        'dq_rule_results',
        'dq_violation_samples',
        
        # Schema management
        'schema_change_events',
        'json_schema_cache',
        'json_schema_evolution',
        'json_flatten_rules',
        
        # Spark
        'spark_job_queue',
        'spark_applications',
        'spark_executors',
        'spark_executor_history',
        
        # System
        'system_config',
        'feature_flags',
        'alerts',
        'maintenance_windows',
        
        # Connection management
        'connection_runs',
        'connection_health_checks',
        'connection_alert_webhooks',
        'sync_mode_config',
        
        # Error handling
        'event_dead_letter_queue',
        'event_dlq_retry_history',
        
        # Resource management
        'resource_usage',
        'resource_quota_violations',
        'tenant_daily_usage',
        
        # Dependencies
        'transformation_dependencies',
        'transformation_logs',
        'udf_execution_stats',
    ]
    
    for table in required_tables:
        assert table in tables, f"Required table '{table}' not found"


def test_connector_definitions_populated(db_session):
    """Test that connector_definitions table has seed data"""
    result = db_session.execute(text("SELECT COUNT(*) FROM connector_definitions"))
    count = result.scalar()
    assert count > 0, "connector_definitions table should have seed data"
    assert count >= 11, f"Expected at least 11 connectors, found {count}"


def test_connector_definitions_structure(db_session):
    """Test that connector definitions have proper structure"""
    result = db_session.execute(text("""
        SELECT connector_name, connector_type, category, supports_cdc, supports_full_refresh
        FROM connector_definitions
        WHERE category = 'source'
        ORDER BY connector_name
    """))
    
    sources = result.fetchall()
    assert len(sources) >= 5, "Should have at least 5 source connectors"
    
    # Check MySQL connector exists and has correct flags
    mysql_connectors = [s for s in sources if s[1] == 'mysql']
    assert len(mysql_connectors) > 0, "MySQL connector should exist"
    
    mysql = mysql_connectors[0]
    assert mysql[3] is True, "MySQL should support CDC"
    assert mysql[4] is True, "MySQL should support full refresh"


def test_connector_versions_populated(db_session):
    """Test that connector_versions table has seed data"""
    result = db_session.execute(text("SELECT COUNT(*) FROM connector_versions"))
    count = result.scalar()
    assert count >= 3, f"connector_versions should have at least 3 versions, found {count}"


def test_system_config_populated(db_session):
    """Test that system_config table has seed data"""
    result = db_session.execute(text("SELECT COUNT(*) FROM system_config"))
    count = result.scalar()
    assert count > 0, "system_config table should have seed data"
    
    # Check specific config keys exist
    result = db_session.execute(text("""
        SELECT config_key, config_value 
        FROM system_config 
        WHERE config_key IN ('redis_host', 'redis_port', 'max_concurrent_connections_per_tenant')
    """))
    configs = dict(result.fetchall())
    
    assert 'redis_host' in configs, "redis_host config should exist"
    assert 'redis_port' in configs, "redis_port config should exist"
    assert 'max_concurrent_connections_per_tenant' in configs, "max_concurrent_connections_per_tenant should exist"


def test_feature_flags_populated(db_session):
    """Test that feature_flags table has seed data"""
    result = db_session.execute(text("SELECT COUNT(*) FROM feature_flags"))
    count = result.scalar()
    assert count >= 4, f"feature_flags should have at least 4 flags, found {count}"
    
    # Check specific flags exist
    result = db_session.execute(text("""
        SELECT flag_name, is_enabled 
        FROM feature_flags 
        WHERE flag_name IN ('enable_schema_auto_apply', 'enable_json_auto_flatten')
    """))
    flags = dict(result.fetchall())
    
    assert 'enable_schema_auto_apply' in flags, "enable_schema_auto_apply flag should exist"
    assert 'enable_json_auto_flatten' in flags, "enable_json_auto_flatten flag should exist"


def test_foreign_key_constraints(db_engine):
    """Test that foreign key constraints are properly set up"""
    inspector = inspect(db_engine)
    
    # Check connector_versions -> connector_definitions FK
    fks = inspector.get_foreign_keys('connector_versions')
    assert len(fks) > 0, "connector_versions should have foreign keys"
    
    connector_fk = [fk for fk in fks if fk['referred_table'] == 'connector_definitions']
    assert len(connector_fk) > 0, "connector_versions should reference connector_definitions"


def test_indexes_exist(db_engine):
    """Test that important indexes are created"""
    inspector = inspect(db_engine)
    
    # Check indexes on sources table
    indexes = inspector.get_indexes('sources')
    index_names = [idx['name'] for idx in indexes]
    
    # Should have some indexes (exact names depend on schema)
    assert len(indexes) > 0, "sources table should have indexes"


def test_uuid_columns_have_defaults(db_engine):
    """Test that UUID primary key columns have default values"""
    inspector = inspect(db_engine)
    
    # Check connector_definitions table
    columns = inspector.get_columns('connector_definitions')
    connector_id_col = [c for c in columns if c['name'] == 'connector_id'][0]
    
    # Should have a default value (uuid_generate_v4())
    assert connector_id_col['default'] is not None, "connector_id should have default UUID generator"


def test_timestamp_columns_have_defaults(db_engine):
    """Test that timestamp columns have default values"""
    inspector = inspect(db_engine)
    
    # Check connector_definitions table
    columns = inspector.get_columns('connector_definitions')
    created_at_col = [c for c in columns if c['name'] == 'created_at'][0]
    
    # Should have a default value (now())
    assert created_at_col['default'] is not None, "created_at should have default timestamp"


def test_jsonb_columns_exist(db_engine):
    """Test that JSONB columns are properly created"""
    inspector = inspect(db_engine)
    
    # Check connector_definitions has JSONB columns
    columns = inspector.get_columns('connector_definitions')
    column_types = {c['name']: str(c['type']) for c in columns}
    
    jsonb_columns = ['default_config', 'required_fields', 'optional_fields', 'default_resource_limits']
    for col in jsonb_columns:
        assert col in column_types, f"Column {col} should exist"
        assert 'JSON' in column_types[col].upper(), f"Column {col} should be JSONB type"


def test_table_ownership(db_engine):
    """Test that tables have correct ownership"""
    with db_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT tablename, tableowner 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename = 'connector_definitions'
        """))
        row = result.fetchone()
        assert row is not None, "connector_definitions table should exist"
        # Owner should be either postgres or fusion_user
        assert row[1] in ['postgres', 'fusion_user'], f"Unexpected table owner: {row[1]}"


def test_alembic_version_exists(db_session):
    """Test that alembic migration was applied"""
    result = db_session.execute(text("SELECT version_num FROM alembic_version"))
    version = result.scalar()
    assert version is not None, "Alembic version should be recorded"
    assert version == '04aff4ce3106', f"Expected migration version 04aff4ce3106, got {version}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

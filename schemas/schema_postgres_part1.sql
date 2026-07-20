-- ============================================================================
-- FUSION CDC ENGINE - POSTGRESQL METADATA SCHEMA
-- ============================================================================
-- Complete production-ready schema optimized for PostgreSQL 14+
-- Integrates with main Fusion application (banks, sub_tenants, users)
-- Total: 42 tables across 8 categories
-- ============================================================================
-- Version: 1.0.0
-- Date: 2025-11-29
-- Compatible with: PostgreSQL 14, 15, 16
-- ============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For text search

-- ============================================================================
-- CATEGORY 1: CONNECTOR DEFINITIONS (Airbyte Pattern)
-- ============================================================================

CREATE TABLE connector_definitions (
    connector_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Connector identification
    connector_name VARCHAR(255) NOT NULL UNIQUE,
    connector_type VARCHAR(50) NOT NULL, -- mysql, postgresql, mongodb, snowflake, bigquery
    category VARCHAR(50) NOT NULL, -- source, destination
    
    -- Version (current latest version)
    latest_version VARCHAR(50) NOT NULL,
    
    -- Default configuration
    default_config JSONB DEFAULT '{}', -- {"port": 3306, "ssl_mode": "preferred"}
    required_fields JSONB DEFAULT '[]', -- ["host", "port", "database", "username", "password"]
    optional_fields JSONB DEFAULT '[]', -- ["ssl_ca_cert", "connection_timeout"]
    
    -- Default resource limits
    default_resource_limits JSONB DEFAULT '{}',
    -- {"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}
    
    -- Capabilities
    supports_cdc BOOLEAN DEFAULT FALSE,
    supports_full_refresh BOOLEAN DEFAULT TRUE,
    supports_incremental BOOLEAN DEFAULT FALSE,
    
    -- Documentation
    documentation_url TEXT,
    icon_url TEXT,
    
    -- Metadata
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID
);

CREATE INDEX idx_connector_defs_type ON connector_definitions(connector_type);
CREATE INDEX idx_connector_defs_category ON connector_definitions(category);

-- ============================================================================

CREATE TABLE connector_versions (
    version_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connector_id UUID NOT NULL REFERENCES connector_definitions(connector_id) ON DELETE CASCADE,
    
    -- Version details
    version VARCHAR(50) NOT NULL,
    
    -- Changes
    release_notes TEXT,
    breaking_changes JSONB DEFAULT '[]', -- ["Removed support for MySQL 5.6", "Changed auth method"]
    new_features JSONB DEFAULT '[]',
    bug_fixes JSONB DEFAULT '[]',
    
    -- Docker image
    docker_image VARCHAR(500),
    docker_tag VARCHAR(100),
    
    -- Metadata
    is_stable BOOLEAN DEFAULT FALSE,
    released_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deprecated_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_connector_version UNIQUE (connector_id, version)
);

CREATE INDEX idx_connector_versions_connector ON connector_versions(connector_id);
CREATE INDEX idx_connector_versions_stable ON connector_versions(is_stable) WHERE is_stable = TRUE;

-- ============================================================================
-- CATEGORY 2: SOURCES & DESTINATIONS
-- ============================================================================

CREATE TABLE sources (
    source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Multi-tenancy (Integration with main Fusion app)
    sub_tenant_id UUID NOT NULL, -- FK to sub_tenants table (main app)
    bank_id UUID NOT NULL, -- FK to banks table (main app)
    
    source_name VARCHAR(255) NOT NULL,
    
    -- Connector reference
    connector_definition_id UUID NOT NULL REFERENCES connector_definitions(connector_id),
    connector_version VARCHAR(50) NOT NULL, -- Pinned version
    
    -- Connection details
    host VARCHAR(500) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password_encrypted TEXT NOT NULL, -- Application-level AES-256-GCM encryption
    
    -- SSL/TLS configuration
    ssl_enabled BOOLEAN DEFAULT FALSE,
    ssl_config JSONB DEFAULT '{}', -- {"mode": "require", "ca_cert": "...", "client_cert": "..."}
    
    -- Source-specific config
    config JSONB DEFAULT '{}',
    -- MySQL: {"server_id": 100, "gtid_enabled": true}
    -- Postgres: {"slot_name": "fusion_slot", "publication": "fusion_pub"}
    -- MongoDB: {"replica_set": "rs0", "auth_source": "admin"}
    
    -- Discovery cache
    discovery_cache JSONB, -- Cached list of schemas/tables
    last_discovery_at TIMESTAMP WITH TIME ZONE,
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft', -- draft, testing, active, paused, error, deleted
    connection_test_status VARCHAR(50), -- success, failed, pending
    connection_test_error TEXT,
    connection_test_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID, -- FK to users (main app)
    
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    CONSTRAINT uq_source_name UNIQUE (sub_tenant_id, source_name)
);

CREATE INDEX idx_sources_sub_tenant ON sources(sub_tenant_id);
CREATE INDEX idx_sources_bank ON sources(bank_id);
CREATE INDEX idx_sources_connector ON sources(connector_definition_id);
CREATE INDEX idx_sources_status ON sources(status);

-- ============================================================================

CREATE TABLE destinations (
    destination_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Multi-tenancy
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    destination_name VARCHAR(255) NOT NULL,
    
    -- Connector reference
    connector_definition_id UUID NOT NULL REFERENCES connector_definitions(connector_id),
    connector_version VARCHAR(50) NOT NULL,
    
    -- Connection configuration (varies by destination type)
    connection_config JSONB NOT NULL,
    -- Postgres warehouse: {"jdbc_url": "...", "schema": "warehouse", "credentials_encrypted": "..."}
    -- Snowflake: {"account": "...", "warehouse": "...", "database": "...", "credentials_encrypted": "..."}
    -- BigQuery: {"project_id": "...", "dataset": "...", "credentials_json_encrypted": "..."}
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft',
    connection_test_status VARCHAR(50),
    connection_test_error TEXT,
    connection_test_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID,
    
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    CONSTRAINT uq_destination_name UNIQUE (sub_tenant_id, destination_name)
);

CREATE INDEX idx_destinations_sub_tenant ON destinations(sub_tenant_id);
CREATE INDEX idx_destinations_bank ON destinations(bank_id);
CREATE INDEX idx_destinations_connector ON destinations(connector_definition_id);

-- ============================================================================
-- CATEGORY 3: TRANSFORMATIONS & DATA QUALITY
-- ============================================================================

CREATE TABLE transform_pipelines (
    pipeline_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    pipeline_name VARCHAR(255) NOT NULL,
    
    -- Transform specification
    spec JSONB NOT NULL,
    -- {"transforms": [
    --   {"id": "t1", "type": "cast", "column": "created_at", "target_type": "timestamp"},
    --   {"id": "t2", "type": "expression", "output_column": "tax", "expression": "amount * 0.18", "depends_on": ["t1"]}
    -- ]}
    
    -- Validation
    is_valid BOOLEAN DEFAULT FALSE,
    validation_errors JSONB,
    last_validated_at TIMESTAMP WITH TIME ZONE,
    
    -- Version control
    version INTEGER DEFAULT 1,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID,
    
    is_deleted BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT uq_pipeline_name UNIQUE (sub_tenant_id, pipeline_name)
);

CREATE INDEX idx_pipelines_sub_tenant ON transform_pipelines(sub_tenant_id);

-- ============================================================================

CREATE TABLE dq_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    policy_name VARCHAR(255) NOT NULL,
    
    -- DQ rules specification
    policy JSONB NOT NULL,
    -- {"rules": [
    --   {"type": "row_count_match", "threshold": 0.02, "severity": "error"},
    --   {"type": "null_ratio_check", "column": "customer_id", "max_null_ratio": 0.01}
    -- ], 
    --  "on_fail": "alert_and_continue"}
    
    -- Alert configuration
    alert_on_failure BOOLEAN DEFAULT TRUE,
    block_sync_on_failure BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID,
    
    is_deleted BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT uq_policy_name UNIQUE (sub_tenant_id, policy_name)
);

CREATE INDEX idx_dq_policies_sub_tenant ON dq_policies(sub_tenant_id);

-- ============================================================================

CREATE TABLE udf_catalog (
    udf_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    udf_name VARCHAR(255) NOT NULL UNIQUE,
    
    -- Function details
    description TEXT,
    language VARCHAR(50) NOT NULL, -- python, scala
    
    -- Function signature
    argument_types JSONB NOT NULL, -- [{"name": "amount", "type": "double"}, {"name": "rate", "type": "double"}]
    return_type VARCHAR(50) NOT NULL,
    
    -- Implementation
    implementation_code TEXT NOT NULL,
    
    -- Dependencies (Python packages)
    dependencies JSONB DEFAULT '[]', -- ["numpy>=1.21.0", "pandas>=1.3.0"]
    
    -- Validation
    is_validated BOOLEAN DEFAULT FALSE,
    validation_errors TEXT,
    test_cases JSONB, -- [{"input": {"amount": 100, "rate": 0.18}, "expected_output": 18.0}]
    
    -- Deployment
    deployed_version VARCHAR(50),
    deployed_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID
);

CREATE INDEX idx_udf_language ON udf_catalog(language);

-- ============================================================================
-- CATEGORY 4: CONNECTIONS & STREAMS
-- ============================================================================

CREATE TABLE connections (
    connection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    connection_name VARCHAR(255) NOT NULL,
    
    -- Source & Destination
    source_id UUID NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    destination_id UUID NOT NULL REFERENCES destinations(destination_id) ON DELETE CASCADE,
    
    -- Sync configuration
    sync_mode VARCHAR(50) NOT NULL, 
    -- FULL_REFRESH_OVERWRITE, FULL_REFRESH_APPEND, INCREMENTAL_APPEND, 
    -- INCREMENTAL_APPEND_DEDUPED, CDC_INCREMENTAL_DEDUPED_HISTORY
    
    sync_type VARCHAR(50) NOT NULL, -- REALTIME, SCHEDULED
    schedule_cron VARCHAR(100), -- Cron expression (required if SCHEDULED)
    
    -- Transformation & DQ
    transform_pipeline_id UUID REFERENCES transform_pipelines(pipeline_id),
    dq_policy_id UUID REFERENCES dq_policies(policy_id),
    
    -- Schema evolution
    schema_evolution_policy VARCHAR(50) DEFAULT 'MANUAL_APPROVAL', -- AUTO_APPLY, MANUAL_APPROVAL
    
    -- ========= OPERATIONAL CONTROLS =========
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft', -- draft, testing, active, paused, error, deleted
    
    -- Pause/Resume tracking
    paused_at TIMESTAMP WITH TIME ZONE,
    paused_by UUID,
    pause_reason TEXT,
    resumed_at TIMESTAMP WITH TIME ZONE,
    resumed_by UUID,
    
    -- Resource limits (connection-specific overrides)
    resource_limits JSONB DEFAULT '{}',
    -- {
    --   "cpu_request": "1000m", "cpu_limit": "4000m",
    --   "memory_request": "2Gi", "memory_limit": "8Gi",
    --   "max_workers": 5,
    --   "max_concurrent_streams": 20,
    --   "rate_limit_events_per_sec": 5000
    -- }
    
    -- Initial load
    initial_load_completed BOOLEAN DEFAULT FALSE,
    initial_load_started_at TIMESTAMP WITH TIME ZONE,
    initial_load_completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID,
    
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    CONSTRAINT uq_connection_name UNIQUE (sub_tenant_id, connection_name),
    CONSTRAINT chk_sync_schedule CHECK (
        (sync_type = 'SCHEDULED' AND schedule_cron IS NOT NULL) OR
        (sync_type = 'REALTIME')
    )
);

CREATE INDEX idx_connections_sub_tenant ON connections(sub_tenant_id);
CREATE INDEX idx_connections_bank ON connections(bank_id);
CREATE INDEX idx_connections_source ON connections(source_id);
CREATE INDEX idx_connections_destination ON connections(destination_id);
CREATE INDEX idx_connections_status ON connections(status);
CREATE INDEX idx_connections_sync_type ON connections(sync_type);

-- ============================================================================

CREATE TABLE streams (
    stream_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    
    -- Stream identification
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Stream configuration
    enabled BOOLEAN DEFAULT TRUE,
    sync_mode VARCHAR(50), -- Override connection sync_mode if needed
    
    -- Cursor/primary key for incremental
    cursor_field VARCHAR(255),
    primary_key JSONB, -- ["id"] or ["tenant_id", "order_id"]
    
    -- Stream-specific transformations
    transform_overrides JSONB,
    
    -- JSON detection and flattening
    json_columns JSONB, 
    -- {"payload_json": {"flatten_mode": "inline", "max_depth": 3}, 
    --  "metadata_json": {"flatten_mode": "child_table", "child_table_name": "orders_metadata"}}
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_stream UNIQUE (connection_id, schema_name, table_name)
);

CREATE INDEX idx_streams_connection ON streams(connection_id);
CREATE INDEX idx_streams_enabled ON streams(enabled) WHERE enabled = TRUE;

-- ============================================================================

CREATE TABLE sync_mode_config (
    config_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE CASCADE,
    
    -- SCD Type 2 configuration (for CDC_INCREMENTAL_DEDUPED_HISTORY mode)
    valid_from_column VARCHAR(100) DEFAULT 'valid_from',
    valid_to_column VARCHAR(100) DEFAULT 'valid_to',
    is_current_column VARCHAR(100) DEFAULT 'is_current',
    end_of_time_value VARCHAR(50) DEFAULT '9999-12-31 23:59:59',
    
    -- Soft delete handling
    soft_delete_handling VARCHAR(50) DEFAULT 'MARK_DELETED', -- KEEP, MARK_DELETED, EXCLUDE
    deleted_flag_column VARCHAR(100) DEFAULT 'is_deleted',
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_sync_config_connection ON sync_mode_config(connection_id);

-- ============================================================================

CREATE TABLE connection_alert_webhooks (
    webhook_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    
    webhook_name VARCHAR(255),
    
    -- Webhook details
    webhook_url TEXT NOT NULL,
    webhook_method VARCHAR(10) DEFAULT 'POST', -- POST, PUT
    webhook_headers JSONB DEFAULT '{}', -- {"Authorization": "Bearer ...", "Content-Type": "application/json"}
    
    -- Payload template (Jinja2 or similar)
    payload_template JSONB DEFAULT '{}',
    -- {
    --   "alert_type": "{{ alert_type }}",
    --   "connection_name": "{{ connection_name }}",
    --   "message": "{{ message }}",
    --   "timestamp": "{{ timestamp }}"
    -- }
    
    -- Trigger configuration
    trigger_events JSONB DEFAULT '[]', 
    -- ["worker_crash", "dq_failure", "high_lag", "schema_change", "sync_failure"]
    
    -- Retry configuration
    max_retries INTEGER DEFAULT 3,
    retry_delay_seconds INTEGER DEFAULT 60,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_triggered_at TIMESTAMP WITH TIME ZONE,
    last_trigger_status VARCHAR(50), -- success, failed
    last_trigger_error TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID
);

CREATE INDEX idx_webhooks_connection ON connection_alert_webhooks(connection_id);
CREATE INDEX idx_webhooks_active ON connection_alert_webhooks(is_active) WHERE is_active = TRUE;

-- ============================================================================
-- CATEGORY 5: EXECUTION & MONITORING
-- ============================================================================

CREATE TABLE connection_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    -- Run details
    run_type VARCHAR(50) NOT NULL, -- initial_load, incremental, scheduled_batch
    run_status VARCHAR(50) DEFAULT 'pending', -- pending, running, success, failed, cancelled
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    
    -- Metrics
    records_read BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,
    
    -- DQ results summary
    dq_checks_passed INTEGER DEFAULT 0,
    dq_checks_failed INTEGER DEFAULT 0,
    dq_results_summary JSONB,
    
    -- Error tracking
    error_message TEXT,
    error_stack_trace TEXT,
    
    -- Job references
    airflow_dag_id VARCHAR(255),
    airflow_run_id VARCHAR(255),
    spark_application_id VARCHAR(255),
    
    -- Logs
    log_s3_path TEXT, -- For heavy logs
    log_url TEXT
);

CREATE INDEX idx_runs_connection ON connection_runs(connection_id);
CREATE INDEX idx_runs_sub_tenant ON connection_runs(sub_tenant_id);
CREATE INDEX idx_runs_status ON connection_runs(run_status);
CREATE INDEX idx_runs_started ON connection_runs(started_at);
CREATE INDEX idx_runs_spark_app ON connection_runs(spark_application_id);

-- ============================================================================
-- CONTINUED IN NEXT PART (Spark, Checkpoint, Schema Evolution, JSON, DQ, Events, etc.)
-- ============================================================================

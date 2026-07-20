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
-- ============================================================================
-- FUSION CDC ENGINE - POSTGRESQL METADATA SCHEMA (PART 2)
-- ============================================================================
-- Continuation of schema_postgres_part1.sql
-- ============================================================================

-- ============================================================================
-- CATEGORY 6: SPARK OPERATOR & JOB QUEUE
-- ============================================================================

CREATE TABLE spark_applications (
    spark_application_id VARCHAR(255) PRIMARY KEY, -- Kubernetes SparkApplication name
    
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    run_id UUID REFERENCES connection_runs(run_id) ON DELETE SET NULL,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    sub_tenant_id UUID NOT NULL,
    
    -- Spark master details (on-demand)
    spark_master_pod_name VARCHAR(255),
    spark_master_url VARCHAR(500),
    spark_master_ui_url VARCHAR(500),
    spark_master_started_at TIMESTAMP WITH TIME ZONE,
    spark_master_stopped_at TIMESTAMP WITH TIME ZONE,
    
    -- Application status
    status VARCHAR(50) DEFAULT 'pending', 
    -- pending, running, success, failed, killed, executor_scaling
    
    -- Resource allocation
    driver_cpu VARCHAR(20),
    driver_memory VARCHAR(20),
    executor_instances INTEGER,
    executor_cpu VARCHAR(20),
    executor_memory VARCHAR(20),
    
    -- Autoscaling
    autoscaling_enabled BOOLEAN DEFAULT TRUE,
    min_executors INTEGER DEFAULT 2,
    max_executors INTEGER DEFAULT 10,
    current_executor_count INTEGER,
    
    -- Timing
    submitted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    
    -- Spark UI
    spark_ui_url TEXT,
    spark_ui_expires_at TIMESTAMP WITH TIME ZONE, -- 48 hours retention
    
    -- Logs
    driver_log_s3_path TEXT,
    executor_logs_s3_path TEXT,
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_spark_apps_connection ON spark_applications(connection_id);
CREATE INDEX idx_spark_apps_run ON spark_applications(run_id);
CREATE INDEX idx_spark_apps_status ON spark_applications(status);
CREATE INDEX idx_spark_apps_submitted ON spark_applications(submitted_at);

-- ============================================================================

CREATE TABLE spark_job_queue (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    sub_tenant_id UUID NOT NULL,
    
    -- Job specification
    job_type VARCHAR(50) NOT NULL, -- initial_load, incremental_sync, backfill
    job_priority INTEGER DEFAULT 5, -- 1 (lowest) to 10 (highest)
    
    -- Spark configuration
    spark_config JSONB NOT NULL,
    -- {
    --   "driver_cpu": "1000m", "driver_memory": "2Gi",
    --   "executor_instances": 3, "executor_cpu": "2000m", "executor_memory": "4Gi",
    --   "autoscaling": {"enabled": true, "min": 2, "max": 10}
    -- }
    
    -- Queue status
    status VARCHAR(50) DEFAULT 'queued', -- queued, pending_resources, running, completed, failed, cancelled
    
    -- Execution
    spark_application_id VARCHAR(255) REFERENCES spark_applications(spark_application_id),
    worker_pod_name VARCHAR(255),
    
    -- Timing
    queued_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    
    -- Resource wait tracking
    waited_for_resources_seconds INTEGER DEFAULT 0,
    resource_wait_started_at TIMESTAMP WITH TIME ZONE,
    
    -- Retry tracking
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_spark_queue_connection ON spark_job_queue(connection_id);
CREATE INDEX idx_spark_queue_status ON spark_job_queue(status);
CREATE INDEX idx_spark_queue_priority ON spark_job_queue(job_priority DESC);
CREATE INDEX idx_spark_queue_queued ON spark_job_queue(queued_at);

-- ============================================================================

CREATE TABLE spark_executors (
    executor_id VARCHAR(255) PRIMARY KEY, -- spark-exec-1, spark-exec-2
    spark_application_id VARCHAR(255) NOT NULL REFERENCES spark_applications(spark_application_id) ON DELETE CASCADE,
    
    -- Executor details
    executor_pod_name VARCHAR(255),
    executor_host VARCHAR(255),
    executor_port INTEGER,
    
    -- Resources
    cpu_allocated VARCHAR(20),
    memory_allocated VARCHAR(20),
    
    -- Status
    status VARCHAR(50) DEFAULT 'starting', -- starting, running, succeeded, failed, killed
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    stopped_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    
    -- Metrics (real-time)
    tasks_completed INTEGER DEFAULT 0,
    tasks_failed INTEGER DEFAULT 0,
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_mb BIGINT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_spark_exec_app ON spark_executors(spark_application_id);
CREATE INDEX idx_spark_exec_status ON spark_executors(status);

-- ============================================================================

CREATE TABLE spark_executor_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spark_application_id VARCHAR(255) NOT NULL REFERENCES spark_applications(spark_application_id) ON DELETE CASCADE,
    
    -- Autoscaling event
    event_type VARCHAR(50) NOT NULL, -- scale_up, scale_down, rebalance
    executor_count_before INTEGER NOT NULL,
    executor_count_after INTEGER NOT NULL,
    
    -- Reason
    scale_reason TEXT,
    -- "High CPU usage: avg 85% across executors"
    -- "Low task queue: avg 2 tasks per executor"
    
    -- Metrics at time of scaling
    metrics_snapshot JSONB,
    -- {
    --   "avg_cpu_percent": 85,
    --   "avg_memory_percent": 60,
    --   "pending_tasks": 1200,
    --   "task_backlog_seconds": 120
    -- }
    
    -- Timing
    triggered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Result
    success BOOLEAN,
    error_message TEXT
);

CREATE INDEX idx_spark_exec_hist_app ON spark_executor_history(spark_application_id);
CREATE INDEX idx_spark_exec_hist_triggered ON spark_executor_history(triggered_at);

-- ============================================================================
-- CATEGORY 7: CHECKPOINT & STATE MANAGEMENT
-- ============================================================================

CREATE TABLE checkpoint_state (
    checkpoint_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    
    -- Checkpoint position
    checkpoint_type VARCHAR(50) NOT NULL, 
    -- mysql_binlog, mysql_gtid, postgres_lsn, mongodb_resume_token, timestamp
    
    checkpoint_value TEXT NOT NULL,
    -- MySQL binlog: {"file": "mysql-bin.000123", "position": 456789}
    -- MySQL GTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    -- Postgres LSN: "0/15D68C0"
    -- MongoDB: "82635A5B00000001"
    -- Timestamp: "2025-11-29T10:30:45.123Z"
    
    -- Additional context
    checkpoint_metadata JSONB,
    -- {"gtid_set": "...", "server_id": 100, "timestamp": 1732876245}
    
    -- Checkpoint tracking
    records_processed_since_last BIGINT DEFAULT 0,
    bytes_processed_since_last BIGINT DEFAULT 0,
    
    -- Timing
    checkpoint_created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    checkpoint_committed_at TIMESTAMP WITH TIME ZONE,
    
    -- Validation
    is_valid BOOLEAN DEFAULT TRUE,
    validation_error TEXT
);

CREATE INDEX idx_checkpoint_connection ON checkpoint_state(connection_id);
CREATE INDEX idx_checkpoint_stream ON checkpoint_state(stream_id);
CREATE INDEX idx_checkpoint_created ON checkpoint_state(checkpoint_created_at DESC);
CREATE INDEX idx_checkpoint_valid ON checkpoint_state(is_valid) WHERE is_valid = TRUE;

-- ============================================================================

CREATE TABLE cdc_position_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE CASCADE,
    
    -- Position snapshot
    position_value TEXT NOT NULL,
    position_metadata JSONB,
    
    -- Metrics at this position
    lag_seconds INTEGER,
    records_behind_source BIGINT,
    
    -- Timestamp
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_cdc_position_connection ON cdc_position_history(connection_id);
CREATE INDEX idx_cdc_position_stream ON cdc_position_history(stream_id);
CREATE INDEX idx_cdc_position_recorded ON cdc_position_history(recorded_at DESC);

-- ============================================================================

CREATE TABLE cdc_lag_metrics (
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE CASCADE,
    
    -- Lag measurements
    lag_seconds INTEGER NOT NULL,
    records_behind_source BIGINT,
    bytes_behind_source BIGINT,
    
    -- Source position
    source_latest_position TEXT,
    consumer_current_position TEXT,
    
    -- Throughput
    events_per_second DECIMAL(10,2),
    bytes_per_second BIGINT,
    
    -- Timestamp
    measured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_cdc_lag_connection ON cdc_lag_metrics(connection_id);
CREATE INDEX idx_cdc_lag_stream ON cdc_lag_metrics(stream_id);
CREATE INDEX idx_cdc_lag_measured ON cdc_lag_metrics(measured_at DESC);
CREATE INDEX idx_cdc_lag_high ON cdc_lag_metrics(lag_seconds) WHERE lag_seconds > 300; -- High lag (>5 min)

-- ============================================================================
-- CATEGORY 8: SCHEMA EVOLUTION & JSON HANDLING
-- ============================================================================

CREATE TABLE schema_change_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    sub_tenant_id UUID NOT NULL,
    
    -- Change details
    change_type VARCHAR(50) NOT NULL, 
    -- ADD_COLUMN, DROP_COLUMN, MODIFY_COLUMN, RENAME_COLUMN, ADD_TABLE, DROP_TABLE
    
    change_details JSONB NOT NULL,
    -- ADD_COLUMN: {"column_name": "email", "data_type": "varchar(255)", "nullable": true, "default": null}
    -- MODIFY_COLUMN: {"column_name": "price", "old_type": "decimal(10,2)", "new_type": "decimal(12,2)"}
    
    -- Detection
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    detected_by VARCHAR(100), -- schema_discovery_job, cdc_worker, manual
    
    -- Schema snapshot
    schema_before JSONB,
    schema_after JSONB,
    
    -- Approval workflow
    approval_status VARCHAR(50) DEFAULT 'pending', 
    -- pending, approved, rejected, auto_applied
    
    approved_by UUID,
    approved_at TIMESTAMP WITH TIME ZONE,
    rejection_reason TEXT,
    
    -- Application
    applied BOOLEAN DEFAULT FALSE,
    applied_at TIMESTAMP WITH TIME ZONE,
    application_error TEXT,
    
    -- Impact analysis
    affected_rows_estimate BIGINT,
    breaking_change BOOLEAN DEFAULT FALSE,
    breaking_change_reason TEXT
);

CREATE INDEX idx_schema_events_connection ON schema_change_events(connection_id);
CREATE INDEX idx_schema_events_stream ON schema_change_events(stream_id);
CREATE INDEX idx_schema_events_approval ON schema_change_events(approval_status);
CREATE INDEX idx_schema_events_detected ON schema_change_events(detected_at DESC);

-- ============================================================================

CREATE TABLE json_schema_cache (
    cache_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    
    -- Detected JSON schema
    json_schema JSONB NOT NULL,
    -- {
    --   "type": "object",
    --   "properties": {
    --     "customer": {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}},
    --     "items": {"type": "array", "items": {"type": "object", "properties": {...}}}
    --   }
    -- }
    
    -- Sample size for detection
    sample_count INTEGER NOT NULL,
    
    -- Detection timing
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Schema stability
    is_stable BOOLEAN DEFAULT FALSE, -- Set to true after consistent schema over N records
    last_verified_at TIMESTAMP WITH TIME ZONE,
    
    CONSTRAINT uq_json_schema UNIQUE (connection_id, stream_id, column_name)
);

CREATE INDEX idx_json_cache_connection ON json_schema_cache(connection_id);
CREATE INDEX idx_json_cache_stream ON json_schema_cache(stream_id);

-- ============================================================================

CREATE TABLE json_flatten_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    
    -- Flatten configuration
    flatten_mode VARCHAR(50) NOT NULL, 
    -- inline (e.g., payload_customer_name), child_table (e.g., orders_payload table)
    
    max_depth INTEGER DEFAULT 3,
    
    -- Child table (if flatten_mode = 'child_table')
    child_table_name VARCHAR(255),
    parent_id_column VARCHAR(100), -- Column linking back to parent
    
    -- Array handling
    array_handling VARCHAR(50) DEFAULT 'child_rows', -- child_rows, json_string, skip
    
    -- Column naming
    naming_pattern VARCHAR(100) DEFAULT 'snake_case', -- snake_case, camel_case, prefix_only
    column_prefix VARCHAR(50), -- e.g., "payload_" → payload_customer_name
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID,
    
    CONSTRAINT uq_flatten_rule UNIQUE (connection_id, stream_id, column_name)
);

CREATE INDEX idx_flatten_rules_connection ON json_flatten_rules(connection_id);
CREATE INDEX idx_flatten_rules_stream ON json_flatten_rules(stream_id);

-- ============================================================================

CREATE TABLE json_schema_evolution (
    evolution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    
    -- Schema change
    old_schema JSONB NOT NULL,
    new_schema JSONB NOT NULL,
    schema_diff JSONB, -- Computed diff: {"added": [...], "removed": [...], "modified": [...]}
    
    -- Detection
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Impact
    is_breaking_change BOOLEAN DEFAULT FALSE,
    impact_summary TEXT,
    
    -- Application
    applied BOOLEAN DEFAULT FALSE,
    applied_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_json_evolution_connection ON json_schema_evolution(connection_id);
CREATE INDEX idx_json_evolution_stream ON json_schema_evolution(stream_id);
CREATE INDEX idx_json_evolution_detected ON json_schema_evolution(detected_at DESC);

-- ============================================================================
-- CATEGORY 9: DATA QUALITY TRACKING
-- ============================================================================

CREATE TABLE dq_rule_results (
    result_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    run_id UUID REFERENCES connection_runs(run_id) ON DELETE CASCADE,
    
    policy_id UUID REFERENCES dq_policies(policy_id) ON DELETE SET NULL,
    
    -- Rule details
    rule_type VARCHAR(100) NOT NULL,
    -- row_count_match, null_ratio_check, unique_constraint, referential_integrity, 
    -- range_check, pattern_match, freshness_check
    
    rule_config JSONB NOT NULL,
    
    -- Result
    status VARCHAR(50) NOT NULL, -- passed, failed, warning, error
    
    -- Metrics
    expected_value JSONB,
    actual_value JSONB,
    deviation DECIMAL(10,4),
    
    -- Details
    message TEXT,
    violation_count BIGINT,
    total_records_checked BIGINT,
    
    -- Timing
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Retention (env var: DQ_VIOLATION_RETENTION_DAYS=90)
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '90 days')
);

CREATE INDEX idx_dq_results_connection ON dq_rule_results(connection_id);
CREATE INDEX idx_dq_results_stream ON dq_rule_results(stream_id);
CREATE INDEX idx_dq_results_run ON dq_rule_results(run_id);
CREATE INDEX idx_dq_results_status ON dq_rule_results(status);
CREATE INDEX idx_dq_results_checked ON dq_rule_results(checked_at DESC);
CREATE INDEX idx_dq_results_expires ON dq_rule_results(expires_at);

-- ============================================================================

CREATE TABLE dq_violations (
    violation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    result_id UUID NOT NULL REFERENCES dq_rule_results(result_id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    
    -- Violation summary
    violation_type VARCHAR(100) NOT NULL,
    severity VARCHAR(50) NOT NULL, -- info, warning, error, critical
    
    violation_message TEXT,
    violation_count BIGINT,
    
    -- Detection
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Retention
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '90 days')
);

CREATE INDEX idx_dq_violations_result ON dq_violations(result_id);
CREATE INDEX idx_dq_violations_connection ON dq_violations(connection_id);
CREATE INDEX idx_dq_violations_severity ON dq_violations(severity);
CREATE INDEX idx_dq_violations_detected ON dq_violations(detected_at DESC);

-- ============================================================================

CREATE TABLE dq_violation_samples (
    sample_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    violation_id UUID NOT NULL REFERENCES dq_violations(violation_id) ON DELETE CASCADE,
    
    -- Sample record
    record_primary_key JSONB, -- {"id": 12345} or {"tenant_id": "T1", "order_id": "O789"}
    record_sample JSONB, -- Actual violating record data
    
    -- Violation details
    violated_column VARCHAR(255),
    violated_constraint TEXT,
    
    actual_value TEXT,
    expected_value TEXT,
    
    -- Metadata
    sampled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Limit samples per violation (e.g., max 100 samples)
    CONSTRAINT chk_sample_limit CHECK (sample_id IS NOT NULL) -- Enforced at application level
);

CREATE INDEX idx_dq_samples_violation ON dq_violation_samples(violation_id);
CREATE INDEX idx_dq_samples_sampled ON dq_violation_samples(sampled_at DESC);

-- ============================================================================
-- CONTINUED IN PART 3 (Transformations, Events, Observability, etc.)
-- ============================================================================
-- ============================================================================
-- FUSION CDC ENGINE - POSTGRESQL METADATA SCHEMA (PART 3)
-- ============================================================================
-- Continuation of schema_postgres_part2.sql
-- ============================================================================

-- ============================================================================
-- CATEGORY 10: TRANSFORMATION EXECUTION
-- ============================================================================

CREATE TABLE transformation_logs (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    run_id UUID REFERENCES connection_runs(run_id) ON DELETE CASCADE,
    
    pipeline_id UUID REFERENCES transform_pipelines(pipeline_id) ON DELETE SET NULL,
    
    -- Transform details
    transform_id VARCHAR(255) NOT NULL, -- ID from pipeline spec (e.g., "t1", "t2")
    transform_type VARCHAR(100) NOT NULL, -- cast, expression, filter, udf, lookup
    
    -- Execution
    status VARCHAR(50) NOT NULL, -- success, failed, skipped
    
    -- Metrics
    records_input BIGINT,
    records_output BIGINT,
    records_filtered BIGINT,
    records_error BIGINT,
    
    execution_time_ms INTEGER,
    
    -- Error tracking
    error_message TEXT,
    error_samples JSONB, -- Array of sample error records (max 10)
    -- [{"record": {...}, "error": "Cannot cast 'abc' to INTEGER"}]
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_transform_logs_connection ON transformation_logs(connection_id);
CREATE INDEX idx_transform_logs_run ON transformation_logs(run_id);
CREATE INDEX idx_transform_logs_pipeline ON transformation_logs(pipeline_id);
CREATE INDEX idx_transform_logs_status ON transformation_logs(status);

-- ============================================================================

CREATE TABLE transformation_dependencies (
    dependency_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES transform_pipelines(pipeline_id) ON DELETE CASCADE,
    
    -- Dependency graph
    transform_id VARCHAR(255) NOT NULL,
    depends_on_transform_id VARCHAR(255) NOT NULL,
    
    -- Auto-detected or manual
    dependency_type VARCHAR(50) DEFAULT 'auto_detected', -- auto_detected, manual
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_transform_dependency UNIQUE (pipeline_id, transform_id, depends_on_transform_id)
);

CREATE INDEX idx_transform_deps_pipeline ON transformation_dependencies(pipeline_id);
CREATE INDEX idx_transform_deps_transform ON transformation_dependencies(transform_id);

-- ============================================================================

CREATE TABLE udf_execution_stats (
    stat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    udf_id UUID NOT NULL REFERENCES udf_catalog(udf_id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    run_id UUID REFERENCES connection_runs(run_id) ON DELETE CASCADE,
    
    -- Execution metrics (per invocation)
    invocation_count BIGINT DEFAULT 0,
    total_execution_time_ms BIGINT DEFAULT 0,
    avg_execution_time_ms DECIMAL(10,2),
    
    min_execution_time_ms INTEGER,
    max_execution_time_ms INTEGER,
    p50_execution_time_ms INTEGER,
    p95_execution_time_ms INTEGER,
    p99_execution_time_ms INTEGER,
    
    -- Error tracking
    error_count BIGINT DEFAULT 0,
    error_rate DECIMAL(5,4), -- 0.0123 = 1.23% errors
    
    -- Timing
    stats_period_start TIMESTAMP WITH TIME ZONE,
    stats_period_end TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_udf_stats_udf ON udf_execution_stats(udf_id);
CREATE INDEX idx_udf_stats_connection ON udf_execution_stats(connection_id);
CREATE INDEX idx_udf_stats_created ON udf_execution_stats(created_at DESC);

-- ============================================================================
-- CATEGORY 11: EVENT TRACKING (Redis + DB)
-- ============================================================================

CREATE TABLE redis_stream_tracking (
    tracking_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    
    -- Redis stream details
    redis_stream_key VARCHAR(500) NOT NULL, -- e.g., "cdc:events:conn123:stream456"
    redis_consumer_group VARCHAR(255) NOT NULL,
    redis_consumer_name VARCHAR(255) NOT NULL,
    
    -- Last processed event
    last_event_id VARCHAR(100), -- Redis stream event ID: "1732876245123-0"
    last_processed_at TIMESTAMP WITH TIME ZONE,
    
    -- Lag tracking
    pending_events_count BIGINT DEFAULT 0,
    lag_seconds INTEGER,
    
    -- Consumer health
    consumer_status VARCHAR(50) DEFAULT 'active', -- active, idle, dead
    last_heartbeat_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_redis_tracking_connection ON redis_stream_tracking(connection_id);
CREATE INDEX idx_redis_tracking_stream ON redis_stream_tracking(stream_id);
CREATE INDEX idx_redis_tracking_consumer ON redis_stream_tracking(redis_consumer_group, redis_consumer_name);
CREATE INDEX idx_redis_tracking_heartbeat ON redis_stream_tracking(last_heartbeat_at);

-- ============================================================================

CREATE TABLE event_dead_letter_queue (
    dlq_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES streams(stream_id) ON DELETE SET NULL,
    
    -- Original event
    event_id VARCHAR(100), -- Original Redis or Kafka event ID
    event_payload JSONB NOT NULL,
    
    -- Failure details
    failure_reason TEXT NOT NULL,
    failure_stack_trace TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Event metadata
    event_source VARCHAR(100), -- redis, kafka, debezium
    event_timestamp TIMESTAMP WITH TIME ZONE,
    
    -- DLQ metadata
    added_to_dlq_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_retry_at TIMESTAMP WITH TIME ZONE,
    
    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,
    
    -- Retention (env var: EVENT_DLQ_RETENTION_DAYS=7)
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '7 days')
);

CREATE INDEX idx_dlq_connection ON event_dead_letter_queue(connection_id);
CREATE INDEX idx_dlq_stream ON event_dead_letter_queue(stream_id);
CREATE INDEX idx_dlq_resolved ON event_dead_letter_queue(resolved);
CREATE INDEX idx_dlq_added ON event_dead_letter_queue(added_to_dlq_at DESC);
CREATE INDEX idx_dlq_expires ON event_dead_letter_queue(expires_at);

-- ============================================================================

CREATE TABLE event_dlq_retry_history (
    retry_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dlq_id UUID NOT NULL REFERENCES event_dead_letter_queue(dlq_id) ON DELETE CASCADE,
    
    -- Retry attempt
    retry_attempt INTEGER NOT NULL,
    retry_strategy VARCHAR(50), -- exponential_backoff, fixed_delay, manual
    
    -- Retry result
    retry_status VARCHAR(50) NOT NULL, -- success, failed, skipped
    retry_error TEXT,
    
    -- Timing
    retried_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    retry_duration_ms INTEGER
);

CREATE INDEX idx_dlq_retry_dlq ON event_dlq_retry_history(dlq_id);
CREATE INDEX idx_dlq_retry_retried ON event_dlq_retry_history(retried_at DESC);

-- ============================================================================
-- CATEGORY 12: RESOURCE & COST TRACKING
-- ============================================================================

CREATE TABLE resource_usage (
    usage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    run_id UUID REFERENCES connection_runs(run_id) ON DELETE SET NULL,
    spark_application_id VARCHAR(255) REFERENCES spark_applications(spark_application_id) ON DELETE SET NULL,
    
    -- Resource metrics
    cpu_seconds BIGINT, -- Total CPU-seconds consumed
    memory_mb_seconds BIGINT, -- Memory MB-seconds
    
    network_bytes_in BIGINT,
    network_bytes_out BIGINT,
    
    storage_bytes_read BIGINT,
    storage_bytes_written BIGINT,
    
    -- Cost estimation (optional)
    estimated_cost_usd DECIMAL(10,4),
    
    -- Time period
    period_start TIMESTAMP WITH TIME ZONE,
    period_end TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_resource_usage_connection ON resource_usage(connection_id);
CREATE INDEX idx_resource_usage_tenant ON resource_usage(sub_tenant_id);
CREATE INDEX idx_resource_usage_run ON resource_usage(run_id);
CREATE INDEX idx_resource_usage_period ON resource_usage(period_start, period_end);

-- ============================================================================

CREATE TABLE tenant_daily_usage (
    usage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    usage_date DATE NOT NULL,
    
    -- Aggregated metrics
    total_cpu_seconds BIGINT DEFAULT 0,
    total_memory_mb_seconds BIGINT DEFAULT 0,
    total_network_bytes BIGINT DEFAULT 0,
    total_storage_bytes BIGINT DEFAULT 0,
    
    -- Record counts
    total_records_processed BIGINT DEFAULT 0,
    
    -- Connection counts
    active_connections_count INTEGER DEFAULT 0,
    
    -- Cost
    total_estimated_cost_usd DECIMAL(10,2) DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_tenant_daily_usage UNIQUE (sub_tenant_id, usage_date)
);

CREATE INDEX idx_tenant_usage_tenant ON tenant_daily_usage(sub_tenant_id);
CREATE INDEX idx_tenant_usage_date ON tenant_daily_usage(usage_date DESC);
CREATE INDEX idx_tenant_usage_bank ON tenant_daily_usage(bank_id);

-- ============================================================================

CREATE TABLE resource_quota_violations (
    violation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    connection_id UUID REFERENCES connections(connection_id) ON DELETE SET NULL,
    
    -- Quota details
    quota_type VARCHAR(100) NOT NULL, 
    -- cpu_limit, memory_limit, storage_limit, records_per_day_limit, connection_count_limit
    
    quota_limit BIGINT NOT NULL,
    actual_usage BIGINT NOT NULL,
    overage BIGINT, -- How much over the limit
    
    -- Action taken
    action_taken VARCHAR(100), 
    -- throttle_connection, pause_connection, send_alert, block_new_connections
    
    -- Timing
    violated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE,
    
    -- Notification
    alert_sent BOOLEAN DEFAULT FALSE,
    alert_sent_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_quota_violations_tenant ON resource_quota_violations(sub_tenant_id);
CREATE INDEX idx_quota_violations_connection ON resource_quota_violations(connection_id);
CREATE INDEX idx_quota_violations_violated ON resource_quota_violations(violated_at DESC);

-- ============================================================================
-- CATEGORY 13: OBSERVABILITY & HEALTH CHECKS
-- ============================================================================

CREATE TABLE connection_health_checks (
    check_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    
    -- Health check type
    check_type VARCHAR(50) NOT NULL, -- source_connectivity, destination_connectivity, cdc_lag, worker_health
    
    -- Result
    status VARCHAR(50) NOT NULL, -- healthy, degraded, unhealthy
    
    -- Details
    check_details JSONB,
    -- {
    --   "response_time_ms": 45,
    --   "lag_seconds": 12,
    --   "error": null
    -- }
    
    -- Metrics
    response_time_ms INTEGER,
    
    -- Timing
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Alert threshold (health check frequency: every 1 minute)
    CONSTRAINT chk_health_frequency CHECK (check_id IS NOT NULL) -- Enforced at application level
);

CREATE INDEX idx_health_checks_connection ON connection_health_checks(connection_id);
CREATE INDEX idx_health_checks_checked ON connection_health_checks(checked_at DESC);
CREATE INDEX idx_health_checks_status ON connection_health_checks(status);

-- ============================================================================

CREATE TABLE worker_heartbeats (
    heartbeat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Worker identification
    worker_id VARCHAR(255) NOT NULL UNIQUE, -- Pod name or unique worker ID
    worker_type VARCHAR(50) NOT NULL, -- cdc_worker, spark_worker, scheduler
    
    connection_id UUID REFERENCES connections(connection_id) ON DELETE SET NULL,
    
    -- Worker details
    worker_pod_name VARCHAR(255),
    worker_node_name VARCHAR(255),
    worker_namespace VARCHAR(100),
    
    -- Status
    status VARCHAR(50) DEFAULT 'running', -- running, idle, shutting_down, crashed
    
    -- Metrics
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_mb BIGINT,
    active_tasks INTEGER DEFAULT 0,
    
    -- Health
    last_heartbeat_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Timeout detection (heartbeat timeout: 30 seconds)
    is_alive BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    stopped_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_worker_heartbeats_worker ON worker_heartbeats(worker_id);
CREATE INDEX idx_worker_heartbeats_connection ON worker_heartbeats(connection_id);
CREATE INDEX idx_worker_heartbeats_last ON worker_heartbeats(last_heartbeat_at DESC);
CREATE INDEX idx_worker_heartbeats_alive ON worker_heartbeats(is_alive) WHERE is_alive = TRUE;

-- ============================================================================

CREATE TABLE alerts (
    alert_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    connection_id UUID REFERENCES connections(connection_id) ON DELETE CASCADE,
    sub_tenant_id UUID NOT NULL,
    bank_id UUID NOT NULL,
    
    -- Alert details
    alert_type VARCHAR(100) NOT NULL,
    -- worker_crash, high_cdc_lag, dq_failure, schema_change_detected, 
    -- sync_failure, resource_quota_exceeded, destination_unreachable
    
    severity VARCHAR(50) NOT NULL, -- info, warning, error, critical
    
    title VARCHAR(500) NOT NULL,
    message TEXT NOT NULL,
    
    -- Context
    alert_context JSONB,
    -- {
    --   "worker_id": "worker-123",
    --   "lag_seconds": 600,
    --   "error": "Connection timeout"
    -- }
    
    -- Alert metadata
    triggered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    acknowledged_by UUID,
    
    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,
    
    -- Notification
    webhook_sent BOOLEAN DEFAULT FALSE,
    webhook_sent_at TIMESTAMP WITH TIME ZONE,
    webhook_response_status INTEGER
);

CREATE INDEX idx_alerts_connection ON alerts(connection_id);
CREATE INDEX idx_alerts_tenant ON alerts(sub_tenant_id);
CREATE INDEX idx_alerts_severity ON alerts(severity);
CREATE INDEX idx_alerts_triggered ON alerts(triggered_at DESC);
CREATE INDEX idx_alerts_unresolved ON alerts(resolved) WHERE resolved = FALSE;

-- ============================================================================

CREATE TABLE audit_log (
    audit_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    
    -- Multi-tenancy
    sub_tenant_id UUID,
    bank_id UUID,
    
    -- User & action
    user_id UUID, -- FK to users (main app)
    username VARCHAR(255),
    
    action VARCHAR(100) NOT NULL,
    -- create_connection, update_connection, delete_connection, pause_connection, resume_connection,
    -- approve_schema_change, test_source, test_destination, create_transform, etc.
    
    -- Resource
    resource_type VARCHAR(50), -- connection, source, destination, transform_pipeline, dq_policy
    resource_id UUID,
    resource_name VARCHAR(255),
    
    -- Change details
    changes JSONB,
    -- {
    --   "before": {"status": "active"},
    --   "after": {"status": "paused"}
    -- }
    
    -- Request metadata
    ip_address INET,
    user_agent TEXT,
    request_id VARCHAR(100),
    
    -- Result
    result VARCHAR(50), -- success, failed
    error_message TEXT,
    
    -- Timing
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (audit_id, timestamp)
)
PARTITION BY RANGE (timestamp);

-- Create partitions (example: monthly partitions)
CREATE TABLE audit_log_2025_11 PARTITION OF audit_log
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE audit_log_2025_12 PARTITION OF audit_log
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE INDEX idx_audit_log_tenant ON audit_log(sub_tenant_id);
CREATE INDEX idx_audit_log_user ON audit_log(user_id);
CREATE INDEX idx_audit_log_action ON audit_log(action);
CREATE INDEX idx_audit_log_resource ON audit_log(resource_type, resource_id);
CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp DESC);

-- ============================================================================
-- CATEGORY 14: SYSTEM CONFIGURATION
-- ============================================================================

CREATE TABLE system_config (
    config_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    config_key VARCHAR(255) NOT NULL UNIQUE,
    config_value TEXT NOT NULL,
    
    -- Metadata
    description TEXT,
    data_type VARCHAR(50), -- string, integer, boolean, json
    
    is_sensitive BOOLEAN DEFAULT FALSE, -- For passwords, API keys
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by UUID
);

CREATE INDEX idx_system_config_key ON system_config(config_key);

-- ============================================================================

CREATE TABLE feature_flags (
    flag_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    flag_name VARCHAR(255) NOT NULL UNIQUE,
    
    -- Flag state
    is_enabled BOOLEAN DEFAULT FALSE,
    
    -- Targeting (optional)
    enabled_for_tenants JSONB DEFAULT '[]', -- ["tenant1", "tenant2"]
    enabled_for_banks JSONB DEFAULT '[]',
    rollout_percentage INTEGER DEFAULT 0, -- 0-100
    
    -- Metadata
    description TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by UUID
);

CREATE INDEX idx_feature_flags_name ON feature_flags(flag_name);
CREATE INDEX idx_feature_flags_enabled ON feature_flags(is_enabled) WHERE is_enabled = TRUE;

-- ============================================================================

CREATE TABLE maintenance_windows (
    window_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    sub_tenant_id UUID, -- NULL = global maintenance
    bank_id UUID,
    
    -- Window details
    window_name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Timing
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Impact
    affects_connections BOOLEAN DEFAULT TRUE,
    connection_ids JSONB, -- ["conn1", "conn2"] or NULL for all
    
    -- Status
    status VARCHAR(50) DEFAULT 'scheduled', -- scheduled, in_progress, completed, cancelled
    
    -- Actions during maintenance
    pause_syncs BOOLEAN DEFAULT TRUE,
    allow_monitoring BOOLEAN DEFAULT TRUE,
    
    -- Notifications
    notify_users BOOLEAN DEFAULT TRUE,
    notification_sent_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID,
    
    CONSTRAINT chk_window_times CHECK (end_time > start_time)
);

CREATE INDEX idx_maintenance_tenant ON maintenance_windows(sub_tenant_id);
CREATE INDEX idx_maintenance_start ON maintenance_windows(start_time);
CREATE INDEX idx_maintenance_status ON maintenance_windows(status);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- Active connections with latest run status
CREATE VIEW v_active_connections AS
SELECT 
    c.connection_id,
    c.connection_name,
    c.sub_tenant_id,
    c.bank_id,
    c.status,
    c.sync_mode,
    c.sync_type,
    s.source_name,
    d.destination_name,
    lr.run_status AS last_run_status,
    lr.completed_at AS last_run_completed_at,
    lr.records_written AS last_run_records_written,
    c.created_at,
    c.updated_at
FROM connections c
LEFT JOIN sources s ON c.source_id = s.source_id
LEFT JOIN destinations d ON c.destination_id = d.destination_id
LEFT JOIN LATERAL (
    SELECT run_status, completed_at, records_written
    FROM connection_runs
    WHERE connection_id = c.connection_id
    ORDER BY started_at DESC
    LIMIT 1
) lr ON TRUE
WHERE c.is_deleted = FALSE;

-- ============================================================================

-- CDC lag summary by connection
CREATE VIEW v_cdc_lag_summary AS
SELECT 
    connection_id,
    MAX(lag_seconds) AS max_lag_seconds,
    AVG(lag_seconds) AS avg_lag_seconds,
    MAX(measured_at) AS last_measured_at
FROM cdc_lag_metrics
WHERE measured_at > NOW() - INTERVAL '1 hour'
GROUP BY connection_id;

-- ============================================================================

-- Tenant resource usage summary
CREATE VIEW v_tenant_resource_summary AS
SELECT 
    sub_tenant_id,
    bank_id,
    usage_date,
    total_cpu_seconds,
    total_memory_mb_seconds,
    total_records_processed,
    total_estimated_cost_usd,
    active_connections_count
FROM tenant_daily_usage
WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY usage_date DESC;

-- ============================================================================
-- FUNCTIONS & TRIGGERS
-- ============================================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to relevant tables
CREATE TRIGGER trg_sources_updated_at BEFORE UPDATE ON sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_destinations_updated_at BEFORE UPDATE ON destinations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_connections_updated_at BEFORE UPDATE ON connections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_transform_pipelines_updated_at BEFORE UPDATE ON transform_pipelines
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_dq_policies_updated_at BEFORE UPDATE ON dq_policies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_connector_definitions_updated_at BEFORE UPDATE ON connector_definitions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_system_config_updated_at BEFORE UPDATE ON system_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_feature_flags_updated_at BEFORE UPDATE ON feature_flags
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INITIAL SYSTEM DATA
-- ============================================================================

-- Insert default system configurations
INSERT INTO system_config (config_key, config_value, description, data_type) VALUES
    ('dq_violation_retention_days', '90', 'Retention period for DQ violation records', 'integer'),
    ('event_dlq_retention_days', '7', 'Retention period for DLQ events', 'integer'),
    ('spark_ui_retention_hours', '48', 'Spark UI availability after job completion', 'integer'),
    ('health_check_interval_seconds', '60', 'Health check frequency', 'integer'),
    ('worker_heartbeat_timeout_seconds', '30', 'Worker heartbeat timeout', 'integer'),
    ('max_concurrent_connections_per_tenant', '50', 'Max connections per tenant', 'integer'),
    ('default_sync_parallelism', '4', 'Default parallel stream count', 'integer'),
    ('redis_stream_retention_hours', '24', 'Redis stream retention', 'integer')
ON CONFLICT (config_key) DO NOTHING;

-- ============================================================================
-- COMMENTS ON TABLES (Documentation)
-- ============================================================================

COMMENT ON TABLE connector_definitions IS 'Airbyte-style connector definitions (sources & destinations types)';
COMMENT ON TABLE connector_versions IS 'Version history for each connector definition';
COMMENT ON TABLE sources IS 'Source database configurations (MySQL, Postgres, MongoDB, etc.)';
COMMENT ON TABLE destinations IS 'Destination warehouse configurations (Snowflake, BigQuery, Postgres, etc.)';
COMMENT ON TABLE connections IS 'CDC connections linking source → destination with sync config';
COMMENT ON TABLE streams IS 'Individual tables/collections being synced within a connection';
COMMENT ON TABLE sync_mode_config IS 'SCD Type 2 and sync mode configuration per connection/stream';
COMMENT ON TABLE transform_pipelines IS 'Transformation pipeline definitions (SQL/Python transforms)';
COMMENT ON TABLE dq_policies IS 'Data quality policies with validation rules';
COMMENT ON TABLE udf_catalog IS 'User-defined functions catalog (Python/Scala UDFs)';
COMMENT ON TABLE connection_runs IS 'Execution history for connection sync runs';
COMMENT ON TABLE spark_applications IS 'Spark application lifecycle tracking (on-demand master/executors)';
COMMENT ON TABLE spark_job_queue IS 'Job queue for Spark applications';
COMMENT ON TABLE spark_executors IS 'Real-time Spark executor tracking';
COMMENT ON TABLE spark_executor_history IS 'Autoscaling history for Spark executors';
COMMENT ON TABLE checkpoint_state IS 'CDC checkpoint positions (binlog, LSN, resume token, etc.)';
COMMENT ON TABLE cdc_lag_metrics IS 'CDC lag metrics for trending and alerting';
COMMENT ON TABLE schema_change_events IS 'Schema evolution tracking with approval workflow';
COMMENT ON TABLE json_schema_cache IS 'Detected JSON schemas for dynamic columns';
COMMENT ON TABLE json_flatten_rules IS 'JSON flattening rules per connection/stream/column';
COMMENT ON TABLE json_schema_evolution IS 'JSON schema evolution tracking';
COMMENT ON TABLE dq_rule_results IS 'Data quality rule execution results';
COMMENT ON TABLE dq_violations IS 'DQ violations summary';
COMMENT ON TABLE dq_violation_samples IS 'Sample records violating DQ rules';
COMMENT ON TABLE transformation_logs IS 'Transformation execution logs with error tracking';
COMMENT ON TABLE transformation_dependencies IS 'Transform dependency DAG (auto-detected + manual)';
COMMENT ON TABLE udf_execution_stats IS 'Per-invocation UDF performance statistics';
COMMENT ON TABLE redis_stream_tracking IS 'Redis stream consumer tracking (in-memory + DB)';
COMMENT ON TABLE event_dead_letter_queue IS 'Failed event tracking with retry capability';
COMMENT ON TABLE event_dlq_retry_history IS 'DLQ retry attempt history';
COMMENT ON TABLE resource_usage IS 'Detailed resource usage per run/application';
COMMENT ON TABLE tenant_daily_usage IS 'Aggregated daily resource usage per tenant';
COMMENT ON TABLE resource_quota_violations IS 'Resource quota violation tracking';
COMMENT ON TABLE connection_health_checks IS 'Connection health check results (1-minute frequency)';
COMMENT ON TABLE worker_heartbeats IS 'CDC worker heartbeat tracking (30-second timeout)';
COMMENT ON TABLE alerts IS 'System alerts (worker crash, high lag, DQ failures, etc.)';
COMMENT ON TABLE audit_log IS 'Audit trail for all user actions (partitioned by month)';
COMMENT ON TABLE system_config IS 'System-wide configuration key-value store';
COMMENT ON TABLE feature_flags IS 'Feature flags for gradual rollout';
COMMENT ON TABLE maintenance_windows IS 'Scheduled maintenance window tracking';

-- ============================================================================
-- SCHEMA COMPLETE: 42 TABLES
-- ============================================================================
-- Integration with main Fusion app:
-- - Foreign keys: sub_tenant_id → sub_tenants, bank_id → banks
-- - User tracking: created_by, updated_by → users
-- - NO separate parents/tenants tables
-- 
-- Database features used:
-- - UUID primary keys with uuid_generate_v4()
-- - JSONB columns with GIN indexes (optional, add separately)
-- - TIMESTAMPTZ for timezone-aware timestamps
-- - Table partitioning for audit_log
-- - Triggers for updated_at auto-update
-- - Views for common queries
-- 
-- Environment variables referenced:
-- - DQ_VIOLATION_RETENTION_DAYS (default: 90)
-- - EVENT_DLQ_RETENTION_DAYS (default: 7)
-- - SPARK_UI_RETENTION_HOURS (default: 48)
-- - HEALTH_CHECK_INTERVAL_SECONDS (default: 60)
-- - WORKER_HEARTBEAT_TIMEOUT_SECONDS (default: 30)
-- ============================================================================

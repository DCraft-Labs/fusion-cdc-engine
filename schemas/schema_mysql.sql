-- ============================================================================
-- FUSION CDC ENGINE - MYSQL 8.0+ METADATA SCHEMA
-- ============================================================================
-- Complete production-ready schema compatible with MySQL 8.0+
-- Integrates with main Fusion application (banks, sub_tenants, users)
-- Total: 42 tables across 8 categories
-- ============================================================================
-- Version: 1.0.0
-- Date: 2025-11-29
-- Compatible with: MySQL 8.0, 8.1, 8.2+
-- ============================================================================
-- NOTE: MySQL version uses portable data types for cross-database compatibility
-- - UUID: CHAR(36) instead of native UUID
-- - JSON: JSON type (MySQL 8.0+)
-- - Boolean: TINYINT(1)
-- - Timestamps: DATETIME(6) with UTC enforcement at application level
-- ============================================================================

-- ============================================================================
-- CATEGORY 1: CONNECTOR DEFINITIONS (Airbyte Pattern)
-- ============================================================================

CREATE TABLE connector_definitions (
    connector_id CHAR(36) PRIMARY KEY,
    
    -- Connector identification
    connector_name VARCHAR(255) NOT NULL UNIQUE,
    connector_type VARCHAR(50) NOT NULL, -- mysql, postgresql, mongodb, snowflake, bigquery
    category VARCHAR(50) NOT NULL, -- source, destination
    
    -- Version (current latest version)
    latest_version VARCHAR(50) NOT NULL,
    
    -- Default configuration
    default_config JSON DEFAULT NULL,
    required_fields JSON DEFAULT NULL,
    optional_fields JSON DEFAULT NULL,
    
    -- Default resource limits
    default_resource_limits JSON DEFAULT NULL,
    
    -- Capabilities
    supports_cdc TINYINT(1) DEFAULT 0,
    supports_full_refresh TINYINT(1) DEFAULT 1,
    supports_incremental TINYINT(1) DEFAULT 0,
    
    -- Documentation
    documentation_url TEXT,
    icon_url TEXT,
    
    -- Metadata
    is_active TINYINT(1) DEFAULT 1,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    INDEX idx_connector_defs_type (connector_type),
    INDEX idx_connector_defs_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE connector_versions (
    version_id CHAR(36) PRIMARY KEY,
    connector_id CHAR(36) NOT NULL,
    
    -- Version details
    version VARCHAR(50) NOT NULL,
    
    -- Changes
    release_notes TEXT,
    breaking_changes JSON DEFAULT NULL,
    new_features JSON DEFAULT NULL,
    bug_fixes JSON DEFAULT NULL,
    
    -- Docker image
    docker_image VARCHAR(500),
    docker_tag VARCHAR(100),
    
    -- Metadata
    is_stable TINYINT(1) DEFAULT 0,
    released_at DATETIME(6) NOT NULL,
    deprecated_at DATETIME(6),
    
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    UNIQUE KEY uq_connector_version (connector_id, version),
    INDEX idx_connector_versions_connector (connector_id),
    INDEX idx_connector_versions_stable (is_stable),
    FOREIGN KEY (connector_id) REFERENCES connector_definitions(connector_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 2: SOURCES & DESTINATIONS
-- ============================================================================

CREATE TABLE sources (
    source_id CHAR(36) PRIMARY KEY,
    
    -- Multi-tenancy (Integration with main Fusion app)
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    source_name VARCHAR(255) NOT NULL,
    
    -- Connector reference
    connector_definition_id CHAR(36) NOT NULL,
    connector_version VARCHAR(50) NOT NULL,
    
    -- Connection details
    host VARCHAR(500) NOT NULL,
    port INT NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password_encrypted TEXT NOT NULL,
    
    -- SSL/TLS configuration
    ssl_enabled TINYINT(1) DEFAULT 0,
    ssl_config JSON DEFAULT NULL,
    
    -- Source-specific config
    config JSON DEFAULT NULL,
    
    -- Discovery cache
    discovery_cache JSON,
    last_discovery_at DATETIME(6),
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft',
    connection_test_status VARCHAR(50),
    connection_test_error TEXT,
    connection_test_at DATETIME(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    is_deleted TINYINT(1) DEFAULT 0,
    deleted_at DATETIME(6),
    
    UNIQUE KEY uq_source_name (sub_tenant_id, source_name),
    INDEX idx_sources_sub_tenant (sub_tenant_id),
    INDEX idx_sources_bank (bank_id),
    INDEX idx_sources_connector (connector_definition_id),
    INDEX idx_sources_status (status),
    FOREIGN KEY (connector_definition_id) REFERENCES connector_definitions(connector_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE destinations (
    destination_id CHAR(36) PRIMARY KEY,
    
    -- Multi-tenancy
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    destination_name VARCHAR(255) NOT NULL,
    
    -- Connector reference
    connector_definition_id CHAR(36) NOT NULL,
    connector_version VARCHAR(50) NOT NULL,
    
    -- Connection configuration
    connection_config JSON NOT NULL,
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft',
    connection_test_status VARCHAR(50),
    connection_test_error TEXT,
    connection_test_at DATETIME(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    is_deleted TINYINT(1) DEFAULT 0,
    deleted_at DATETIME(6),
    
    UNIQUE KEY uq_destination_name (sub_tenant_id, destination_name),
    INDEX idx_destinations_sub_tenant (sub_tenant_id),
    INDEX idx_destinations_bank (bank_id),
    INDEX idx_destinations_connector (connector_definition_id),
    FOREIGN KEY (connector_definition_id) REFERENCES connector_definitions(connector_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 3: TRANSFORMATIONS & DATA QUALITY
-- ============================================================================

CREATE TABLE transform_pipelines (
    pipeline_id CHAR(36) PRIMARY KEY,
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    pipeline_name VARCHAR(255) NOT NULL,
    
    -- Transform specification
    spec JSON NOT NULL,
    
    -- Validation
    is_valid TINYINT(1) DEFAULT 0,
    validation_errors JSON,
    last_validated_at DATETIME(6),
    
    -- Version control
    version INT DEFAULT 1,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    is_deleted TINYINT(1) DEFAULT 0,
    
    UNIQUE KEY uq_pipeline_name (sub_tenant_id, pipeline_name),
    INDEX idx_pipelines_sub_tenant (sub_tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE dq_policies (
    policy_id CHAR(36) PRIMARY KEY,
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    policy_name VARCHAR(255) NOT NULL,
    
    -- DQ rules specification
    policy JSON NOT NULL,
    
    -- Alert configuration
    alert_on_failure TINYINT(1) DEFAULT 1,
    block_sync_on_failure TINYINT(1) DEFAULT 0,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    is_deleted TINYINT(1) DEFAULT 0,
    
    UNIQUE KEY uq_policy_name (sub_tenant_id, policy_name),
    INDEX idx_dq_policies_sub_tenant (sub_tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE udf_catalog (
    udf_id CHAR(36) PRIMARY KEY,
    udf_name VARCHAR(255) NOT NULL UNIQUE,
    
    -- Function details
    description TEXT,
    language VARCHAR(50) NOT NULL,
    
    -- Function signature
    argument_types JSON NOT NULL,
    return_type VARCHAR(50) NOT NULL,
    
    -- Implementation
    implementation_code TEXT NOT NULL,
    
    -- Dependencies
    dependencies JSON DEFAULT NULL,
    
    -- Validation
    is_validated TINYINT(1) DEFAULT 0,
    validation_errors TEXT,
    test_cases JSON,
    
    -- Deployment
    deployed_version VARCHAR(50),
    deployed_at DATETIME(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    INDEX idx_udf_language (language)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 4: CONNECTIONS & STREAMS
-- ============================================================================

CREATE TABLE connections (
    connection_id CHAR(36) PRIMARY KEY,
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    connection_name VARCHAR(255) NOT NULL,
    
    -- Source & Destination
    source_id CHAR(36) NOT NULL,
    destination_id CHAR(36) NOT NULL,
    
    -- Sync configuration
    sync_mode VARCHAR(50) NOT NULL,
    sync_type VARCHAR(50) NOT NULL,
    schedule_cron VARCHAR(100),
    
    -- Transformation & DQ
    transform_pipeline_id CHAR(36),
    dq_policy_id CHAR(36),
    
    -- Schema evolution
    schema_evolution_policy VARCHAR(50) DEFAULT 'MANUAL_APPROVAL',
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft',
    
    -- Pause/Resume tracking
    paused_at DATETIME(6),
    paused_by CHAR(36),
    pause_reason TEXT,
    resumed_at DATETIME(6),
    resumed_by CHAR(36),
    
    -- Resource limits
    resource_limits JSON DEFAULT NULL,
    
    -- Initial load
    initial_load_completed TINYINT(1) DEFAULT 0,
    initial_load_started_at DATETIME(6),
    initial_load_completed_at DATETIME(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    is_deleted TINYINT(1) DEFAULT 0,
    deleted_at DATETIME(6),
    
    UNIQUE KEY uq_connection_name (sub_tenant_id, connection_name),
    INDEX idx_connections_sub_tenant (sub_tenant_id),
    INDEX idx_connections_bank (bank_id),
    INDEX idx_connections_source (source_id),
    INDEX idx_connections_destination (destination_id),
    INDEX idx_connections_status (status),
    INDEX idx_connections_sync_type (sync_type),
    FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE,
    FOREIGN KEY (destination_id) REFERENCES destinations(destination_id) ON DELETE CASCADE,
    FOREIGN KEY (transform_pipeline_id) REFERENCES transform_pipelines(pipeline_id),
    FOREIGN KEY (dq_policy_id) REFERENCES dq_policies(policy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE streams (
    stream_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    
    -- Stream identification
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Stream configuration
    enabled TINYINT(1) DEFAULT 1,
    sync_mode VARCHAR(50),
    
    -- Cursor/primary key
    cursor_field VARCHAR(255),
    primary_key JSON,
    
    -- Transform overrides
    transform_overrides JSON,
    
    -- JSON columns
    json_columns JSON,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    UNIQUE KEY uq_stream (connection_id, schema_name, table_name),
    INDEX idx_streams_connection (connection_id),
    INDEX idx_streams_enabled (enabled),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE sync_mode_config (
    config_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    
    -- SCD Type 2 configuration
    valid_from_column VARCHAR(100) DEFAULT 'valid_from',
    valid_to_column VARCHAR(100) DEFAULT 'valid_to',
    is_current_column VARCHAR(100) DEFAULT 'is_current',
    end_of_time_value VARCHAR(50) DEFAULT '9999-12-31 23:59:59',
    
    -- Soft delete handling
    soft_delete_handling VARCHAR(50) DEFAULT 'MARK_DELETED',
    deleted_flag_column VARCHAR(100) DEFAULT 'is_deleted',
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    INDEX idx_sync_config_connection (connection_id),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE connection_alert_webhooks (
    webhook_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    
    webhook_name VARCHAR(255),
    
    -- Webhook details
    webhook_url TEXT NOT NULL,
    webhook_method VARCHAR(10) DEFAULT 'POST',
    webhook_headers JSON DEFAULT NULL,
    
    -- Payload template
    payload_template JSON DEFAULT NULL,
    
    -- Trigger configuration
    trigger_events JSON DEFAULT NULL,
    
    -- Retry configuration
    max_retries INT DEFAULT 3,
    retry_delay_seconds INT DEFAULT 60,
    
    -- Status
    is_active TINYINT(1) DEFAULT 1,
    last_triggered_at DATETIME(6),
    last_trigger_status VARCHAR(50),
    last_trigger_error TEXT,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    INDEX idx_webhooks_connection (connection_id),
    INDEX idx_webhooks_active (is_active),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- MySQL schema continues in schema_mysql_part2.sql
-- (Due to file size, split into multiple parts similar to Postgres)
-- ============================================================================
-- ============================================================================
-- FUSION CDC ENGINE - MYSQL METADATA SCHEMA (PART 2)
-- ============================================================================
-- Continuation of schema_mysql_part1.sql
-- ============================================================================

-- ============================================================================
-- CATEGORY 5: EXECUTION & MONITORING
-- ============================================================================

CREATE TABLE connection_runs (
    run_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    -- Run details
    run_type VARCHAR(50) NOT NULL,
    run_status VARCHAR(50) DEFAULT 'pending',
    
    -- Timing
    started_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    completed_at DATETIME(6),
    duration_seconds INT,
    
    -- Metrics
    records_read BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,
    
    -- DQ results summary
    dq_checks_passed INT DEFAULT 0,
    dq_checks_failed INT DEFAULT 0,
    dq_results_summary JSON,
    
    -- Error tracking
    error_message TEXT,
    error_stack_trace TEXT,
    
    -- Job references
    airflow_dag_id VARCHAR(255),
    airflow_run_id VARCHAR(255),
    spark_application_id VARCHAR(255),
    
    -- Logs
    log_s3_path TEXT,
    log_url TEXT,
    
    INDEX idx_runs_connection (connection_id),
    INDEX idx_runs_sub_tenant (sub_tenant_id),
    INDEX idx_runs_status (run_status),
    INDEX idx_runs_started (started_at),
    INDEX idx_runs_spark_app (spark_application_id),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 6: SPARK OPERATOR & JOB QUEUE
-- ============================================================================

CREATE TABLE spark_applications (
    spark_application_id VARCHAR(255) PRIMARY KEY,
    
    connection_id CHAR(36) NOT NULL,
    run_id CHAR(36),
    stream_id CHAR(36),
    sub_tenant_id CHAR(36) NOT NULL,
    
    -- Spark master details
    spark_master_pod_name VARCHAR(255),
    spark_master_url VARCHAR(500),
    spark_master_ui_url VARCHAR(500),
    spark_master_started_at DATETIME(6),
    spark_master_stopped_at DATETIME(6),
    
    -- Application status
    status VARCHAR(50) DEFAULT 'pending',
    
    -- Resource allocation
    driver_cpu VARCHAR(20),
    driver_memory VARCHAR(20),
    executor_instances INT,
    executor_cpu VARCHAR(20),
    executor_memory VARCHAR(20),
    
    -- Autoscaling
    autoscaling_enabled TINYINT(1) DEFAULT 1,
    min_executors INT DEFAULT 2,
    max_executors INT DEFAULT 10,
    current_executor_count INT,
    
    -- Timing
    submitted_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    started_at DATETIME(6),
    completed_at DATETIME(6),
    duration_seconds INT,
    
    -- Spark UI
    spark_ui_url TEXT,
    spark_ui_expires_at DATETIME(6),
    
    -- Logs
    driver_log_s3_path TEXT,
    executor_logs_s3_path TEXT,
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    INDEX idx_spark_apps_connection (connection_id),
    INDEX idx_spark_apps_run (run_id),
    INDEX idx_spark_apps_status (status),
    INDEX idx_spark_apps_submitted (submitted_at),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES connection_runs(run_id) ON DELETE SET NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE spark_job_queue (
    job_id CHAR(36) PRIMARY KEY,
    
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    sub_tenant_id CHAR(36) NOT NULL,
    
    -- Job specification
    job_type VARCHAR(50) NOT NULL,
    job_priority INT DEFAULT 5,
    
    -- Spark configuration
    spark_config JSON NOT NULL,
    
    -- Queue status
    status VARCHAR(50) DEFAULT 'queued',
    
    -- Execution
    spark_application_id VARCHAR(255),
    worker_pod_name VARCHAR(255),
    
    -- Timing
    queued_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    started_at DATETIME(6),
    completed_at DATETIME(6),
    duration_seconds INT,
    
    -- Resource wait tracking
    waited_for_resources_seconds INT DEFAULT 0,
    resource_wait_started_at DATETIME(6),
    
    -- Retry tracking
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    INDEX idx_spark_queue_connection (connection_id),
    INDEX idx_spark_queue_status (status),
    INDEX idx_spark_queue_priority (job_priority DESC),
    INDEX idx_spark_queue_queued (queued_at),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL,
    FOREIGN KEY (spark_application_id) REFERENCES spark_applications(spark_application_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE spark_executors (
    executor_id VARCHAR(255) PRIMARY KEY,
    spark_application_id VARCHAR(255) NOT NULL,
    
    -- Executor details
    executor_pod_name VARCHAR(255),
    executor_host VARCHAR(255),
    executor_port INT,
    
    -- Resources
    cpu_allocated VARCHAR(20),
    memory_allocated VARCHAR(20),
    
    -- Status
    status VARCHAR(50) DEFAULT 'starting',
    
    -- Timing
    started_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    stopped_at DATETIME(6),
    duration_seconds INT,
    
    -- Metrics
    tasks_completed INT DEFAULT 0,
    tasks_failed INT DEFAULT 0,
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_mb BIGINT,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    INDEX idx_spark_exec_app (spark_application_id),
    INDEX idx_spark_exec_status (status),
    FOREIGN KEY (spark_application_id) REFERENCES spark_applications(spark_application_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE spark_executor_history (
    history_id CHAR(36) PRIMARY KEY,
    spark_application_id VARCHAR(255) NOT NULL,
    
    -- Autoscaling event
    event_type VARCHAR(50) NOT NULL,
    executor_count_before INT NOT NULL,
    executor_count_after INT NOT NULL,
    
    -- Reason
    scale_reason TEXT,
    
    -- Metrics at time of scaling
    metrics_snapshot JSON,
    
    -- Timing
    triggered_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    completed_at DATETIME(6),
    
    -- Result
    success TINYINT(1),
    error_message TEXT,
    
    INDEX idx_spark_exec_hist_app (spark_application_id),
    INDEX idx_spark_exec_hist_triggered (triggered_at),
    FOREIGN KEY (spark_application_id) REFERENCES spark_applications(spark_application_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 7: CHECKPOINT & STATE MANAGEMENT
-- ============================================================================

CREATE TABLE checkpoint_state (
    checkpoint_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36) NOT NULL,
    
    -- Checkpoint position
    checkpoint_type VARCHAR(50) NOT NULL,
    checkpoint_value TEXT NOT NULL,
    
    -- Additional context
    checkpoint_metadata JSON,
    
    -- Checkpoint tracking
    records_processed_since_last BIGINT DEFAULT 0,
    bytes_processed_since_last BIGINT DEFAULT 0,
    
    -- Timing
    checkpoint_created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    checkpoint_committed_at DATETIME(6),
    
    -- Validation
    is_valid TINYINT(1) DEFAULT 1,
    validation_error TEXT,
    
    INDEX idx_checkpoint_connection (connection_id),
    INDEX idx_checkpoint_stream (stream_id),
    INDEX idx_checkpoint_created (checkpoint_created_at DESC),
    INDEX idx_checkpoint_valid (is_valid),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE cdc_position_history (
    history_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    
    -- Position snapshot
    position_value TEXT NOT NULL,
    position_metadata JSON,
    
    -- Metrics at this position
    lag_seconds INT,
    records_behind_source BIGINT,
    
    -- Timestamp
    recorded_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_cdc_position_connection (connection_id),
    INDEX idx_cdc_position_stream (stream_id),
    INDEX idx_cdc_position_recorded (recorded_at DESC),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE cdc_lag_metrics (
    metric_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    
    -- Lag measurements
    lag_seconds INT NOT NULL,
    records_behind_source BIGINT,
    bytes_behind_source BIGINT,
    
    -- Source position
    source_latest_position TEXT,
    consumer_current_position TEXT,
    
    -- Throughput
    events_per_second DECIMAL(10,2),
    bytes_per_second BIGINT,
    
    -- Timestamp
    measured_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_cdc_lag_connection (connection_id),
    INDEX idx_cdc_lag_stream (stream_id),
    INDEX idx_cdc_lag_measured (measured_at DESC),
    INDEX idx_cdc_lag_high (lag_seconds),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 8: SCHEMA EVOLUTION & JSON HANDLING
-- ============================================================================

CREATE TABLE schema_change_events (
    event_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36) NOT NULL,
    sub_tenant_id CHAR(36) NOT NULL,
    
    -- Change details
    change_type VARCHAR(50) NOT NULL,
    change_details JSON NOT NULL,
    
    -- Detection
    detected_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    detected_by VARCHAR(100),
    
    -- Schema snapshot
    schema_before JSON,
    schema_after JSON,
    
    -- Approval workflow
    approval_status VARCHAR(50) DEFAULT 'pending',
    
    approved_by CHAR(36),
    approved_at DATETIME(6),
    rejection_reason TEXT,
    
    -- Application
    applied TINYINT(1) DEFAULT 0,
    applied_at DATETIME(6),
    application_error TEXT,
    
    -- Impact analysis
    affected_rows_estimate BIGINT,
    breaking_change TINYINT(1) DEFAULT 0,
    breaking_change_reason TEXT,
    
    INDEX idx_schema_events_connection (connection_id),
    INDEX idx_schema_events_stream (stream_id),
    INDEX idx_schema_events_approval (approval_status),
    INDEX idx_schema_events_detected (detected_at DESC),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE json_schema_cache (
    cache_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    
    -- Detected JSON schema
    json_schema JSON NOT NULL,
    
    -- Sample size
    sample_count INT NOT NULL,
    
    -- Detection timing
    detected_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Schema stability
    is_stable TINYINT(1) DEFAULT 0,
    last_verified_at DATETIME(6),
    
    UNIQUE KEY uq_json_schema (connection_id, stream_id, column_name),
    INDEX idx_json_cache_connection (connection_id),
    INDEX idx_json_cache_stream (stream_id),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE json_flatten_rules (
    rule_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    
    -- Flatten configuration
    flatten_mode VARCHAR(50) NOT NULL,
    max_depth INT DEFAULT 3,
    
    -- Child table
    child_table_name VARCHAR(255),
    parent_id_column VARCHAR(100),
    
    -- Array handling
    array_handling VARCHAR(50) DEFAULT 'child_rows',
    
    -- Column naming
    naming_pattern VARCHAR(100) DEFAULT 'snake_case',
    column_prefix VARCHAR(50),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    UNIQUE KEY uq_flatten_rule (connection_id, stream_id, column_name),
    INDEX idx_flatten_rules_connection (connection_id),
    INDEX idx_flatten_rules_stream (stream_id),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE json_schema_evolution (
    evolution_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    
    -- Schema change
    old_schema JSON NOT NULL,
    new_schema JSON NOT NULL,
    schema_diff JSON,
    
    -- Detection
    detected_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Impact
    is_breaking_change TINYINT(1) DEFAULT 0,
    impact_summary TEXT,
    
    -- Application
    applied TINYINT(1) DEFAULT 0,
    applied_at DATETIME(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_json_evolution_connection (connection_id),
    INDEX idx_json_evolution_stream (stream_id),
    INDEX idx_json_evolution_detected (detected_at DESC),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 9: DATA QUALITY TRACKING
-- ============================================================================

CREATE TABLE dq_rule_results (
    result_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    run_id CHAR(36),
    
    policy_id CHAR(36),
    
    -- Rule details
    rule_type VARCHAR(100) NOT NULL,
    rule_config JSON NOT NULL,
    
    -- Result
    status VARCHAR(50) NOT NULL,
    
    -- Metrics
    expected_value JSON,
    actual_value JSON,
    deviation DECIMAL(10,4),
    
    -- Details
    message TEXT,
    violation_count BIGINT,
    total_records_checked BIGINT,
    
    -- Timing
    checked_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Retention (90 days)
    expires_at DATETIME(6),
    
    INDEX idx_dq_results_connection (connection_id),
    INDEX idx_dq_results_stream (stream_id),
    INDEX idx_dq_results_run (run_id),
    INDEX idx_dq_results_status (status),
    INDEX idx_dq_results_checked (checked_at DESC),
    INDEX idx_dq_results_expires (expires_at),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL,
    FOREIGN KEY (run_id) REFERENCES connection_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (policy_id) REFERENCES dq_policies(policy_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE dq_violations (
    violation_id CHAR(36) PRIMARY KEY,
    result_id CHAR(36) NOT NULL,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    
    -- Violation summary
    violation_type VARCHAR(100) NOT NULL,
    severity VARCHAR(50) NOT NULL,
    
    violation_message TEXT,
    violation_count BIGINT,
    
    -- Detection
    detected_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Retention
    expires_at DATETIME(6),
    
    INDEX idx_dq_violations_result (result_id),
    INDEX idx_dq_violations_connection (connection_id),
    INDEX idx_dq_violations_severity (severity),
    INDEX idx_dq_violations_detected (detected_at DESC),
    FOREIGN KEY (result_id) REFERENCES dq_rule_results(result_id) ON DELETE CASCADE,
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE dq_violation_samples (
    sample_id CHAR(36) PRIMARY KEY,
    violation_id CHAR(36) NOT NULL,
    
    -- Sample record
    record_primary_key JSON,
    record_sample JSON,
    
    -- Violation details
    violated_column VARCHAR(255),
    violated_constraint TEXT,
    
    actual_value TEXT,
    expected_value TEXT,
    
    -- Metadata
    sampled_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_dq_samples_violation (violation_id),
    INDEX idx_dq_samples_sampled (sampled_at DESC),
    FOREIGN KEY (violation_id) REFERENCES dq_violations(violation_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- MySQL schema continues in schema_mysql_part3.sql
-- ============================================================================
-- ============================================================================
-- FUSION CDC ENGINE - MYSQL METADATA SCHEMA (PART 3)
-- ============================================================================
-- Continuation of schema_mysql_part2.sql
-- ============================================================================

-- ============================================================================
-- CATEGORY 10: TRANSFORMATION EXECUTION
-- ============================================================================

CREATE TABLE transformation_logs (
    log_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    run_id CHAR(36),
    
    pipeline_id CHAR(36),
    
    -- Transform details
    transform_id VARCHAR(255) NOT NULL,
    transform_type VARCHAR(100) NOT NULL,
    
    -- Execution
    status VARCHAR(50) NOT NULL,
    
    -- Metrics
    records_input BIGINT,
    records_output BIGINT,
    records_filtered BIGINT,
    records_error BIGINT,
    
    execution_time_ms INT,
    
    -- Error tracking
    error_message TEXT,
    error_samples JSON,
    
    -- Timing
    started_at DATETIME(6),
    completed_at DATETIME(6),
    
    INDEX idx_transform_logs_connection (connection_id),
    INDEX idx_transform_logs_run (run_id),
    INDEX idx_transform_logs_pipeline (pipeline_id),
    INDEX idx_transform_logs_status (status),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL,
    FOREIGN KEY (run_id) REFERENCES connection_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (pipeline_id) REFERENCES transform_pipelines(pipeline_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE transformation_dependencies (
    dependency_id CHAR(36) PRIMARY KEY,
    pipeline_id CHAR(36) NOT NULL,
    
    -- Dependency graph
    transform_id VARCHAR(255) NOT NULL,
    depends_on_transform_id VARCHAR(255) NOT NULL,
    
    -- Auto-detected or manual
    dependency_type VARCHAR(50) DEFAULT 'auto_detected',
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    UNIQUE KEY uq_transform_dependency (pipeline_id, transform_id, depends_on_transform_id),
    INDEX idx_transform_deps_pipeline (pipeline_id),
    INDEX idx_transform_deps_transform (transform_id),
    FOREIGN KEY (pipeline_id) REFERENCES transform_pipelines(pipeline_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE udf_execution_stats (
    stat_id CHAR(36) PRIMARY KEY,
    udf_id CHAR(36) NOT NULL,
    connection_id CHAR(36) NOT NULL,
    run_id CHAR(36),
    
    -- Execution metrics
    invocation_count BIGINT DEFAULT 0,
    total_execution_time_ms BIGINT DEFAULT 0,
    avg_execution_time_ms DECIMAL(10,2),
    
    min_execution_time_ms INT,
    max_execution_time_ms INT,
    p50_execution_time_ms INT,
    p95_execution_time_ms INT,
    p99_execution_time_ms INT,
    
    -- Error tracking
    error_count BIGINT DEFAULT 0,
    error_rate DECIMAL(5,4),
    
    -- Timing
    stats_period_start DATETIME(6),
    stats_period_end DATETIME(6),
    
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_udf_stats_udf (udf_id),
    INDEX idx_udf_stats_connection (connection_id),
    INDEX idx_udf_stats_created (created_at DESC),
    FOREIGN KEY (udf_id) REFERENCES udf_catalog(udf_id) ON DELETE CASCADE,
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES connection_runs(run_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 11: EVENT TRACKING (Redis + DB)
-- ============================================================================

CREATE TABLE redis_stream_tracking (
    tracking_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    
    -- Redis stream details
    redis_stream_key VARCHAR(500) NOT NULL,
    redis_consumer_group VARCHAR(255) NOT NULL,
    redis_consumer_name VARCHAR(255) NOT NULL,
    
    -- Last processed event
    last_event_id VARCHAR(100),
    last_processed_at DATETIME(6),
    
    -- Lag tracking
    pending_events_count BIGINT DEFAULT 0,
    lag_seconds INT,
    
    -- Consumer health
    consumer_status VARCHAR(50) DEFAULT 'active',
    last_heartbeat_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    INDEX idx_redis_tracking_connection (connection_id),
    INDEX idx_redis_tracking_stream (stream_id),
    INDEX idx_redis_tracking_consumer (redis_consumer_group, redis_consumer_name),
    INDEX idx_redis_tracking_heartbeat (last_heartbeat_at),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE event_dead_letter_queue (
    dlq_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    stream_id CHAR(36),
    
    -- Original event
    event_id VARCHAR(100),
    event_payload JSON NOT NULL,
    
    -- Failure details
    failure_reason TEXT NOT NULL,
    failure_stack_trace TEXT,
    retry_count INT DEFAULT 0,
    
    -- Event metadata
    event_source VARCHAR(100),
    event_timestamp DATETIME(6),
    
    -- DLQ metadata
    added_to_dlq_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_retry_at DATETIME(6),
    
    -- Resolution
    resolved TINYINT(1) DEFAULT 0,
    resolved_at DATETIME(6),
    resolution_notes TEXT,
    
    -- Retention (7 days)
    expires_at DATETIME(6),
    
    INDEX idx_dlq_connection (connection_id),
    INDEX idx_dlq_stream (stream_id),
    INDEX idx_dlq_resolved (resolved),
    INDEX idx_dlq_added (added_to_dlq_at DESC),
    INDEX idx_dlq_expires (expires_at),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE event_dlq_retry_history (
    retry_id CHAR(36) PRIMARY KEY,
    dlq_id CHAR(36) NOT NULL,
    
    -- Retry attempt
    retry_attempt INT NOT NULL,
    retry_strategy VARCHAR(50),
    
    -- Retry result
    retry_status VARCHAR(50) NOT NULL,
    retry_error TEXT,
    
    -- Timing
    retried_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    retry_duration_ms INT,
    
    INDEX idx_dlq_retry_dlq (dlq_id),
    INDEX idx_dlq_retry_retried (retried_at DESC),
    FOREIGN KEY (dlq_id) REFERENCES event_dead_letter_queue(dlq_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 12: RESOURCE & COST TRACKING
-- ============================================================================

CREATE TABLE resource_usage (
    usage_id CHAR(36) PRIMARY KEY,
    
    connection_id CHAR(36) NOT NULL,
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    run_id CHAR(36),
    spark_application_id VARCHAR(255),
    
    -- Resource metrics
    cpu_seconds BIGINT,
    memory_mb_seconds BIGINT,
    
    network_bytes_in BIGINT,
    network_bytes_out BIGINT,
    
    storage_bytes_read BIGINT,
    storage_bytes_written BIGINT,
    
    -- Cost estimation
    estimated_cost_usd DECIMAL(10,4),
    
    -- Time period
    period_start DATETIME(6),
    period_end DATETIME(6),
    
    -- Metadata
    recorded_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_resource_usage_connection (connection_id),
    INDEX idx_resource_usage_tenant (sub_tenant_id),
    INDEX idx_resource_usage_run (run_id),
    INDEX idx_resource_usage_period (period_start, period_end),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES connection_runs(run_id) ON DELETE SET NULL,
    FOREIGN KEY (spark_application_id) REFERENCES spark_applications(spark_application_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE tenant_daily_usage (
    usage_id CHAR(36) PRIMARY KEY,
    
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    usage_date DATE NOT NULL,
    
    -- Aggregated metrics
    total_cpu_seconds BIGINT DEFAULT 0,
    total_memory_mb_seconds BIGINT DEFAULT 0,
    total_network_bytes BIGINT DEFAULT 0,
    total_storage_bytes BIGINT DEFAULT 0,
    
    -- Record counts
    total_records_processed BIGINT DEFAULT 0,
    
    -- Connection counts
    active_connections_count INT DEFAULT 0,
    
    -- Cost
    total_estimated_cost_usd DECIMAL(10,2) DEFAULT 0,
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    UNIQUE KEY uq_tenant_daily_usage (sub_tenant_id, usage_date),
    INDEX idx_tenant_usage_tenant (sub_tenant_id),
    INDEX idx_tenant_usage_date (usage_date DESC),
    INDEX idx_tenant_usage_bank (bank_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE resource_quota_violations (
    violation_id CHAR(36) PRIMARY KEY,
    
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    connection_id CHAR(36),
    
    -- Quota details
    quota_type VARCHAR(100) NOT NULL,
    
    quota_limit BIGINT NOT NULL,
    actual_usage BIGINT NOT NULL,
    overage BIGINT,
    
    -- Action taken
    action_taken VARCHAR(100),
    
    -- Timing
    violated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    resolved_at DATETIME(6),
    
    -- Notification
    alert_sent TINYINT(1) DEFAULT 0,
    alert_sent_at DATETIME(6),
    
    INDEX idx_quota_violations_tenant (sub_tenant_id),
    INDEX idx_quota_violations_connection (connection_id),
    INDEX idx_quota_violations_violated (violated_at DESC),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- CATEGORY 13: OBSERVABILITY & HEALTH CHECKS
-- ============================================================================

CREATE TABLE connection_health_checks (
    check_id CHAR(36) PRIMARY KEY,
    connection_id CHAR(36) NOT NULL,
    
    -- Health check type
    check_type VARCHAR(50) NOT NULL,
    
    -- Result
    status VARCHAR(50) NOT NULL,
    
    -- Details
    check_details JSON,
    
    -- Metrics
    response_time_ms INT,
    
    -- Timing
    checked_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_health_checks_connection (connection_id),
    INDEX idx_health_checks_checked (checked_at DESC),
    INDEX idx_health_checks_status (status),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE worker_heartbeats (
    heartbeat_id CHAR(36) PRIMARY KEY,
    
    -- Worker identification
    worker_id VARCHAR(255) NOT NULL UNIQUE,
    worker_type VARCHAR(50) NOT NULL,
    
    connection_id CHAR(36),
    
    -- Worker details
    worker_pod_name VARCHAR(255),
    worker_node_name VARCHAR(255),
    worker_namespace VARCHAR(100),
    
    -- Status
    status VARCHAR(50) DEFAULT 'running',
    
    -- Metrics
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_mb BIGINT,
    active_tasks INT DEFAULT 0,
    
    -- Health
    last_heartbeat_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Timeout detection
    is_alive TINYINT(1) DEFAULT 1,
    
    -- Metadata
    started_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    stopped_at DATETIME(6),
    
    INDEX idx_worker_heartbeats_worker (worker_id),
    INDEX idx_worker_heartbeats_connection (connection_id),
    INDEX idx_worker_heartbeats_last (last_heartbeat_at DESC),
    INDEX idx_worker_heartbeats_alive (is_alive),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE alerts (
    alert_id CHAR(36) PRIMARY KEY,
    
    connection_id CHAR(36),
    sub_tenant_id CHAR(36) NOT NULL,
    bank_id CHAR(36) NOT NULL,
    
    -- Alert details
    alert_type VARCHAR(100) NOT NULL,
    severity VARCHAR(50) NOT NULL,
    
    title VARCHAR(500) NOT NULL,
    message TEXT NOT NULL,
    
    -- Context
    alert_context JSON,
    
    -- Alert metadata
    triggered_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    acknowledged TINYINT(1) DEFAULT 0,
    acknowledged_at DATETIME(6),
    acknowledged_by CHAR(36),
    
    -- Resolution
    resolved TINYINT(1) DEFAULT 0,
    resolved_at DATETIME(6),
    resolution_notes TEXT,
    
    -- Notification
    webhook_sent TINYINT(1) DEFAULT 0,
    webhook_sent_at DATETIME(6),
    webhook_response_status INT,
    
    INDEX idx_alerts_connection (connection_id),
    INDEX idx_alerts_tenant (sub_tenant_id),
    INDEX idx_alerts_severity (severity),
    INDEX idx_alerts_triggered (triggered_at DESC),
    INDEX idx_alerts_unresolved (resolved),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE audit_log (
    audit_id CHAR(36) PRIMARY KEY,
    
    -- Multi-tenancy
    sub_tenant_id CHAR(36),
    bank_id CHAR(36),
    
    -- User & action
    user_id CHAR(36),
    username VARCHAR(255),
    
    action VARCHAR(100) NOT NULL,
    
    -- Resource
    resource_type VARCHAR(50),
    resource_id CHAR(36),
    resource_name VARCHAR(255),
    
    -- Change details
    changes JSON,
    
    -- Request metadata
    ip_address VARCHAR(45),
    user_agent TEXT,
    request_id VARCHAR(100),
    
    -- Result
    result VARCHAR(50),
    error_message TEXT,
    
    -- Timing
    timestamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    
    INDEX idx_audit_log_tenant (sub_tenant_id),
    INDEX idx_audit_log_user (user_id),
    INDEX idx_audit_log_action (action),
    INDEX idx_audit_log_resource (resource_type, resource_id),
    INDEX idx_audit_log_timestamp (timestamp DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (TO_DAYS(timestamp)) (
    PARTITION p_2025_11 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p_2025_12 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ============================================================================
-- CATEGORY 14: SYSTEM CONFIGURATION
-- ============================================================================

CREATE TABLE system_config (
    config_id CHAR(36) PRIMARY KEY,
    
    config_key VARCHAR(255) NOT NULL UNIQUE,
    config_value TEXT NOT NULL,
    
    -- Metadata
    description TEXT,
    data_type VARCHAR(50),
    
    is_sensitive TINYINT(1) DEFAULT 0,
    
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    updated_by CHAR(36),
    
    INDEX idx_system_config_key (config_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE feature_flags (
    flag_id CHAR(36) PRIMARY KEY,
    
    flag_name VARCHAR(255) NOT NULL UNIQUE,
    
    -- Flag state
    is_enabled TINYINT(1) DEFAULT 0,
    
    -- Targeting
    enabled_for_tenants JSON DEFAULT NULL,
    enabled_for_banks JSON DEFAULT NULL,
    rollout_percentage INT DEFAULT 0,
    
    -- Metadata
    description TEXT,
    
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    updated_by CHAR(36),
    
    INDEX idx_feature_flags_name (flag_name),
    INDEX idx_feature_flags_enabled (is_enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================

CREATE TABLE maintenance_windows (
    window_id CHAR(36) PRIMARY KEY,
    
    sub_tenant_id CHAR(36),
    bank_id CHAR(36),
    
    -- Window details
    window_name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Timing
    start_time DATETIME(6) NOT NULL,
    end_time DATETIME(6) NOT NULL,
    
    -- Impact
    affects_connections TINYINT(1) DEFAULT 1,
    connection_ids JSON,
    
    -- Status
    status VARCHAR(50) DEFAULT 'scheduled',
    
    -- Actions during maintenance
    pause_syncs TINYINT(1) DEFAULT 1,
    allow_monitoring TINYINT(1) DEFAULT 1,
    
    -- Notifications
    notify_users TINYINT(1) DEFAULT 1,
    notification_sent_at DATETIME(6),
    
    -- Metadata
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    created_by CHAR(36),
    
    INDEX idx_maintenance_tenant (sub_tenant_id),
    INDEX idx_maintenance_start (start_time),
    INDEX idx_maintenance_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

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
    (SELECT run_status FROM connection_runs WHERE connection_id = c.connection_id ORDER BY started_at DESC LIMIT 1) AS last_run_status,
    (SELECT completed_at FROM connection_runs WHERE connection_id = c.connection_id ORDER BY started_at DESC LIMIT 1) AS last_run_completed_at,
    (SELECT records_written FROM connection_runs WHERE connection_id = c.connection_id ORDER BY started_at DESC LIMIT 1) AS last_run_records_written,
    c.created_at,
    c.updated_at
FROM connections c
LEFT JOIN sources s ON c.source_id = s.source_id
LEFT JOIN destinations d ON c.destination_id = d.destination_id
WHERE c.is_deleted = 0;

-- ============================================================================

CREATE VIEW v_cdc_lag_summary AS
SELECT 
    connection_id,
    MAX(lag_seconds) AS max_lag_seconds,
    AVG(lag_seconds) AS avg_lag_seconds,
    MAX(measured_at) AS last_measured_at
FROM cdc_lag_metrics
WHERE measured_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY connection_id;

-- ============================================================================

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
WHERE usage_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY usage_date DESC;

-- ============================================================================
-- INITIAL SYSTEM DATA
-- ============================================================================

INSERT INTO system_config (config_id, config_key, config_value, description, data_type) VALUES
    (UUID(), 'dq_violation_retention_days', '90', 'Retention period for DQ violation records', 'integer'),
    (UUID(), 'event_dlq_retention_days', '7', 'Retention period for DLQ events', 'integer'),
    (UUID(), 'spark_ui_retention_hours', '48', 'Spark UI availability after job completion', 'integer'),
    (UUID(), 'health_check_interval_seconds', '60', 'Health check frequency', 'integer'),
    (UUID(), 'worker_heartbeat_timeout_seconds', '30', 'Worker heartbeat timeout', 'integer'),
    (UUID(), 'max_concurrent_connections_per_tenant', '50', 'Max connections per tenant', 'integer'),
    (UUID(), 'default_sync_parallelism', '4', 'Default parallel stream count', 'integer'),
    (UUID(), 'redis_stream_retention_hours', '24', 'Redis stream retention', 'integer');

-- ============================================================================
-- SCHEMA COMPLETE: 42 TABLES (MySQL 8.0+ Compatible)
-- ============================================================================
-- Data type differences from Postgres:
-- - UUID: CHAR(36) instead of native UUID type
-- - JSON: JSON type (MySQL 8.0+) instead of JSONB
-- - Boolean: TINYINT(1) instead of BOOLEAN
-- - Timestamps: DATETIME(6) instead of TIMESTAMPTZ
-- - IP Address: VARCHAR(45) instead of INET
-- 
-- Note: Application must handle:
-- - UUID generation (use UUID() function or application-level)
-- - Timezone conversions (store all times in UTC)
-- - JSON indexing (MySQL has limited JSON path indexing)
-- - Table partitioning syntax differences
-- ============================================================================

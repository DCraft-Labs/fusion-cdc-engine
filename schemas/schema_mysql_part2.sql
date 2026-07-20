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

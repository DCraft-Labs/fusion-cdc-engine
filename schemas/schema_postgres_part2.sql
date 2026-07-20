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

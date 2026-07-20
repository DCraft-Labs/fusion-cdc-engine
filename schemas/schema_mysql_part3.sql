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

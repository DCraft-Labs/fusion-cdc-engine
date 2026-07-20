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
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
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
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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

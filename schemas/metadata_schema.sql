-- ============================================================================
-- ⚠️  DEPRECATED - DO NOT USE THIS FILE ⚠️
-- ============================================================================
-- This file is DEPRECATED and kept only for reference.
-- 
-- USE INSTEAD:
--   - For PostgreSQL: schemas/schema_postgres.sql (1,853 lines, 42 tables)
--   - For MySQL:      schemas/schema_mysql.sql (1,692 lines, 42 tables)
--
-- MIGRATION NOTES:
--   - New schemas integrate with main Fusion app (banks, sub_tenants)
--   - NO separate parents/tenants tables - uses existing infrastructure
--   - Added 23 new tables for complete CDC functionality
--   - Database-agnostic design with separate DDL files
--
-- NEW TABLES ADDED (23 total):
--   - connector_versions, sync_mode_config, connection_alert_webhooks
--   - spark_applications, spark_job_queue, spark_executors, spark_executor_history
--   - cdc_position_history, cdc_lag_metrics
--   - json_schema_cache, json_flatten_rules, json_schema_evolution
--   - dq_rule_results, dq_violations, dq_violation_samples
--   - transformation_logs, transformation_dependencies, udf_execution_stats
--   - redis_stream_tracking, event_dead_letter_queue, event_dlq_retry_history
--   - resource_quota_violations, connection_health_checks, worker_heartbeats
--
-- See: docs/schema_implementation_plan.md for complete documentation
-- ============================================================================
-- 
-- ORIGINAL SCHEMA (DEPRECATED):
-- ============================================================================
-- FUSION CDC ENGINE - METADATA DATABASE SCHEMA
-- ============================================================================
-- Comprehensive schema supporting multi-tenancy, resource tracking, 
-- cost allocation, and operational controls (pause/resume/limits)
-- Based on Airbyte patterns + banking-grade requirements
-- ============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- PARENT & TENANT MANAGEMENT
-- ============================================================================

CREATE TABLE parents (
    parent_id VARCHAR(64) PRIMARY KEY,
    parent_name VARCHAR(255) NOT NULL,
    parent_type VARCHAR(50) DEFAULT 'bank', -- bank, enterprise, partner
    
    -- Contact & billing
    contact_email VARCHAR(255),
    contact_name VARCHAR(255),
    billing_email VARCHAR(255),
    
    -- Resource limits (parent-level caps)
    max_tenants INTEGER DEFAULT 1000,
    max_sources_per_tenant INTEGER DEFAULT 50,
    max_connections_per_tenant INTEGER DEFAULT 100,
    
    -- Subscription & tier
    subscription_tier VARCHAR(50) DEFAULT 'standard', -- free, standard, premium, enterprise
    subscription_status VARCHAR(50) DEFAULT 'active', -- active, suspended, cancelled
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP
);

CREATE TABLE tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    parent_id VARCHAR(64) NOT NULL REFERENCES parents(parent_id) ON DELETE CASCADE,
    tenant_name VARCHAR(255) NOT NULL,
    
    -- Tenant details
    display_name VARCHAR(255),
    description TEXT,
    
    -- Resource quotas (tenant-specific overrides)
    max_sources INTEGER DEFAULT 10,
    max_destinations INTEGER DEFAULT 5,
    max_connections INTEGER DEFAULT 20,
    max_streams_per_connection INTEGER DEFAULT 100,
    
    -- Compute limits (for cost control)
    max_cpu_cores DECIMAL(10,2) DEFAULT 8.0,
    max_memory_gb DECIMAL(10,2) DEFAULT 32.0,
    max_storage_gb DECIMAL(10,2) DEFAULT 500.0,
    max_events_per_day BIGINT DEFAULT 10000000, -- 10M events/day
    
    -- Cost tracking
    cost_center VARCHAR(100),
    billing_enabled BOOLEAN DEFAULT TRUE,
    
    -- Operational controls
    status VARCHAR(50) DEFAULT 'active', -- active, paused, suspended, deleted
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    CONSTRAINT uq_tenant_name UNIQUE (parent_id, tenant_name)
);

CREATE INDEX idx_tenants_parent ON tenants(parent_id);
CREATE INDEX idx_tenants_status ON tenants(status);

-- ============================================================================
-- SOURCE & DESTINATION CONFIGURATION
-- ============================================================================

CREATE TABLE sources (
    source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    source_name VARCHAR(255) NOT NULL,
    
    -- Source type
    source_type VARCHAR(50) NOT NULL, -- mysql, postgresql, mongodb, oracle, mssql
    
    -- Connection details (encrypted)
    host VARCHAR(500) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password_encrypted TEXT NOT NULL, -- pgcrypto encrypted
    
    -- SSL/TLS configuration
    ssl_enabled BOOLEAN DEFAULT FALSE,
    ssl_ca_cert TEXT,
    ssl_client_cert TEXT,
    ssl_client_key_encrypted TEXT,
    
    -- Source-specific config (JSONB for flexibility)
    config JSONB DEFAULT '{}',
    -- MySQL: {"server_id": 100, "gtid_enabled": true, "binlog_format": "ROW"}
    -- Postgres: {"slot_name": "fusion_slot", "publication": "fusion_pub"}
    -- MongoDB: {"replica_set": "rs0", "auth_source": "admin"}
    
    -- Discovery cache
    discovery_cache JSONB, -- Cached schema/table list
    last_discovery_at TIMESTAMP,
    
    -- Operational status
    status VARCHAR(50) DEFAULT 'draft', -- draft, testing, active, paused, error, deleted
    connection_test_status VARCHAR(50), -- success, failed, pending
    connection_test_error TEXT,
    connection_test_at TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    CONSTRAINT uq_source_name UNIQUE (tenant_id, source_name)
);

CREATE INDEX idx_sources_tenant ON sources(tenant_id);
CREATE INDEX idx_sources_type ON sources(source_type);
CREATE INDEX idx_sources_status ON sources(status);

CREATE TABLE destinations (
    destination_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    destination_name VARCHAR(255) NOT NULL,
    
    -- Destination type
    destination_type VARCHAR(50) NOT NULL, -- postgres_warehouse, iceberg, bigquery, snowflake
    
    -- Connection details (encrypted)
    connection_config JSONB NOT NULL,
    -- Postgres: {"jdbc_url": "...", "username": "...", "password_encrypted": "...", "schema": "warehouse"}
    -- Iceberg: {"catalog_type": "glue", "warehouse_path": "s3://...", "credentials_encrypted": "..."}
    
    -- Operational status
    status VARCHAR(50) DEFAULT 'draft',
    connection_test_status VARCHAR(50),
    connection_test_error TEXT,
    connection_test_at TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    CONSTRAINT uq_destination_name UNIQUE (tenant_id, destination_name)
);

CREATE INDEX idx_destinations_tenant ON destinations(tenant_id);
CREATE INDEX idx_destinations_type ON destinations(destination_type);

-- ============================================================================
-- TRANSFORMATION PIPELINES & DATA QUALITY POLICIES
-- ============================================================================

CREATE TABLE transform_pipelines (
    pipeline_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(255) NOT NULL,
    
    -- Transform specification (JSON array of transform steps)
    spec JSONB NOT NULL,
    -- Example: {"transforms": [{"id": "cast1", "type": "cast", "column": "created_at", ...}]}
    
    -- Validation
    is_valid BOOLEAN DEFAULT FALSE,
    validation_errors JSONB,
    last_validated_at TIMESTAMP,
    
    -- Version control
    version INTEGER DEFAULT 1,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    CONSTRAINT uq_pipeline_name UNIQUE (tenant_id, pipeline_name)
);

CREATE INDEX idx_pipelines_tenant ON transform_pipelines(tenant_id);

CREATE TABLE dq_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    policy_name VARCHAR(255) NOT NULL,
    
    -- DQ rules specification
    policy JSONB NOT NULL,
    -- Example: {"rules": [{"type": "row_count_match", "threshold": 0.02}, ...], "on_fail": "alert"}
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    CONSTRAINT uq_policy_name UNIQUE (tenant_id, policy_name)
);

CREATE INDEX idx_dq_policies_tenant ON dq_policies(tenant_id);

-- ============================================================================
-- CONNECTIONS & STREAMS (Core sync configuration)
-- ============================================================================

CREATE TABLE connections (
    connection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    connection_name VARCHAR(255) NOT NULL,
    
    -- Source & Destination
    source_id UUID NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    destination_id UUID NOT NULL REFERENCES destinations(destination_id) ON DELETE CASCADE,
    
    -- Sync configuration
    sync_mode VARCHAR(50) NOT NULL, -- FULL_REFRESH_OVERWRITE, INCREMENTAL_APPEND, CDC_INCREMENTAL_DEDUPED_HISTORY, etc.
    sync_type VARCHAR(50) NOT NULL, -- REALTIME, SCHEDULED
    schedule_cron VARCHAR(100), -- Cron expression for scheduled syncs
    
    -- Transformation & DQ
    transform_pipeline_id UUID REFERENCES transform_pipelines(pipeline_id),
    dq_policy_id UUID REFERENCES dq_policies(policy_id),
    
    -- Schema evolution
    schema_evolution_policy VARCHAR(50) DEFAULT 'MANUAL_APPROVAL', -- AUTO_APPLY, MANUAL_APPROVAL
    
    -- ========= OPERATIONAL CONTROLS (KEY REQUIREMENTS) =========
    
    -- Status control
    status VARCHAR(50) DEFAULT 'draft', -- draft, testing, active, paused, error, deleted
    
    -- Pause/Resume tracking
    paused_at TIMESTAMP,
    paused_by VARCHAR(100),
    pause_reason TEXT,
    resumed_at TIMESTAMP,
    resumed_by VARCHAR(100),
    
    -- Resource limits (connection-specific overrides)
    resource_limits JSONB DEFAULT '{}',
    -- {
    --   "cpu_request": "500m", "cpu_limit": "2000m",
    --   "memory_request": "1Gi", "memory_limit": "4Gi",
    --   "max_workers": 3,
    --   "max_concurrent_streams": 10,
    --   "rate_limit_events_per_sec": 1000
    -- }
    
    -- Initial load configuration
    initial_load_completed BOOLEAN DEFAULT FALSE,
    initial_load_started_at TIMESTAMP,
    initial_load_completed_at TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    -- Audit
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    CONSTRAINT uq_connection_name UNIQUE (tenant_id, connection_name),
    CONSTRAINT chk_sync_schedule CHECK (
        (sync_type = 'SCHEDULED' AND schedule_cron IS NOT NULL) OR
        (sync_type = 'REALTIME' AND schedule_cron IS NULL)
    )
);

CREATE INDEX idx_connections_tenant ON connections(tenant_id);
CREATE INDEX idx_connections_source ON connections(source_id);
CREATE INDEX idx_connections_destination ON connections(destination_id);
CREATE INDEX idx_connections_status ON connections(status);
CREATE INDEX idx_connections_sync_type ON connections(sync_type);

-- Streams within a connection (table/collection level)
CREATE TABLE streams (
    stream_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    
    -- Stream identification
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Stream-specific overrides
    enabled BOOLEAN DEFAULT TRUE,
    sync_mode VARCHAR(50), -- Can override connection sync_mode per stream
    
    -- Cursor/primary key for incremental syncs
    cursor_field VARCHAR(255), -- e.g., "updated_at" for INCREMENTAL_APPEND
    primary_key JSONB, -- ["id"] or ["tenant_id", "order_id"]
    
    -- Stream-specific transformations (overrides pipeline)
    transform_overrides JSONB,
    
    -- JSON detection and flattening config
    json_columns JSONB, -- {"payload_json": {"flatten_mode": "inline", "schema": {...}}}
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uq_stream UNIQUE (connection_id, schema_name, table_name)
);

CREATE INDEX idx_streams_connection ON streams(connection_id);
CREATE INDEX idx_streams_enabled ON streams(enabled);

-- ============================================================================
-- CHECKPOINT STATE (Dual-layer checkpointing)
-- ============================================================================

CREATE TABLE checkpoint_state (
    checkpoint_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Source identification
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    connection_id UUID REFERENCES connections(connection_id) ON DELETE CASCADE,
    
    -- Stream identification
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Checkpoint data (source-specific)
    checkpoint_data JSONB NOT NULL,
    -- MySQL: {"log_file": "mysql-bin.000123", "log_pos": 45678, "gtid": "..."}
    -- Postgres: {"lsn": "16/B374D848", "slot_name": "fusion_slot"}
    -- MongoDB: {"resume_token": "..."}
    
    -- Checkpoint metadata
    event_count BIGINT DEFAULT 0, -- Events processed since last checkpoint
    last_event_ts TIMESTAMP, -- Timestamp of last event
    checkpoint_ts TIMESTAMP DEFAULT NOW(), -- When checkpoint was written
    
    -- Checkpoint type
    checkpoint_type VARCHAR(50) DEFAULT 'central', -- central (this table), local (SQLite)
    
    -- Worker identification (for debugging)
    worker_id VARCHAR(100),
    worker_pod_name VARCHAR(255),
    
    CONSTRAINT uq_checkpoint UNIQUE (source_id, schema_name, table_name)
);

CREATE INDEX idx_checkpoint_tenant ON checkpoint_state(tenant_id);
CREATE INDEX idx_checkpoint_source ON checkpoint_state(source_id);
CREATE INDEX idx_checkpoint_connection ON checkpoint_state(connection_id);
CREATE INDEX idx_checkpoint_ts ON checkpoint_state(checkpoint_ts);

-- ============================================================================
-- SCHEMA EVOLUTION TRACKING
-- ============================================================================

CREATE TABLE schema_change_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Source identification
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    
    -- Schema change details
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Change type
    change_type VARCHAR(50) NOT NULL, -- COLUMN_ADDED, COLUMN_REMOVED, TYPE_CHANGED, TABLE_ADDED, TABLE_REMOVED
    
    -- Change details
    change_details JSONB NOT NULL,
    -- COLUMN_ADDED: {"column": "email", "type": "VARCHAR(255)", "nullable": true}
    -- TYPE_CHANGED: {"column": "amount", "old_type": "INT", "new_type": "DECIMAL(10,2)"}
    
    -- Diff (before/after snapshot)
    schema_before JSONB,
    schema_after JSONB,
    
    -- Detection
    detected_at TIMESTAMP DEFAULT NOW(),
    detection_method VARCHAR(50) DEFAULT 'periodic_introspection', -- periodic_introspection, manual_trigger
    
    -- Approval workflow
    status VARCHAR(50) DEFAULT 'pending', -- pending, approved, rejected, auto_applied
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP,
    review_comment TEXT,
    
    -- Applied
    applied BOOLEAN DEFAULT FALSE,
    applied_at TIMESTAMP,
    applied_by VARCHAR(100),
    
    -- Affected connections (for notification)
    affected_connections JSONB -- Array of connection_ids
);

CREATE INDEX idx_schema_events_tenant ON schema_change_events(tenant_id);
CREATE INDEX idx_schema_events_source ON schema_change_events(source_id);
CREATE INDEX idx_schema_events_status ON schema_change_events(status);
CREATE INDEX idx_schema_events_detected ON schema_change_events(detected_at);

-- ============================================================================
-- USAGE TRACKING & COST ALLOCATION
-- ============================================================================

CREATE TABLE resource_usage (
    usage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Tenant identification
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    connection_id UUID REFERENCES connections(connection_id) ON DELETE SET NULL,
    
    -- Time window (hourly rollup)
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    
    -- Resource metrics
    cpu_core_hours DECIMAL(15,4) DEFAULT 0, -- Core-hours consumed
    memory_gb_hours DECIMAL(15,4) DEFAULT 0, -- GB-hours consumed
    storage_gb_hours DECIMAL(15,4) DEFAULT 0, -- GB-hours of storage
    
    -- Event metrics
    events_processed BIGINT DEFAULT 0,
    events_failed BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,
    
    -- Worker metrics
    worker_hours DECIMAL(10,2) DEFAULT 0, -- Number of worker-hours
    
    -- Spark metrics
    spark_driver_hours DECIMAL(10,2) DEFAULT 0,
    spark_executor_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Cost calculation (USD)
    estimated_cost_usd DECIMAL(10,4) DEFAULT 0,
    
    -- Metadata
    collected_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uq_usage_window UNIQUE (tenant_id, connection_id, window_start)
);

CREATE INDEX idx_usage_tenant ON resource_usage(tenant_id);
CREATE INDEX idx_usage_connection ON resource_usage(connection_id);
CREATE INDEX idx_usage_window ON resource_usage(window_start, window_end);

-- Daily tenant rollup (for billing)
CREATE TABLE tenant_daily_usage (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    usage_date DATE NOT NULL,
    
    -- Aggregated metrics
    total_cpu_core_hours DECIMAL(15,4) DEFAULT 0,
    total_memory_gb_hours DECIMAL(15,4) DEFAULT 0,
    total_storage_gb_hours DECIMAL(15,4) DEFAULT 0,
    total_events_processed BIGINT DEFAULT 0,
    total_bytes_processed BIGINT DEFAULT 0,
    
    -- Cost
    total_cost_usd DECIMAL(10,4) DEFAULT 0,
    
    -- Metadata
    computed_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (tenant_id, usage_date)
);

CREATE INDEX idx_daily_usage_date ON tenant_daily_usage(usage_date);

-- ============================================================================
-- CONNECTION RUNS (Execution history)
-- ============================================================================

CREATE TABLE connection_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES connections(connection_id) ON DELETE CASCADE,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    
    -- Run details
    run_type VARCHAR(50) NOT NULL, -- initial_load, incremental, scheduled_batch
    run_status VARCHAR(50) DEFAULT 'pending', -- pending, running, success, failed, cancelled
    
    -- Timing
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    
    -- Metrics
    records_read BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,
    
    -- DQ results
    dq_checks_passed INTEGER DEFAULT 0,
    dq_checks_failed INTEGER DEFAULT 0,
    dq_results JSONB,
    
    -- Error tracking
    error_message TEXT,
    error_stack_trace TEXT,
    
    -- Airflow/Spark job references
    airflow_dag_id VARCHAR(255),
    airflow_run_id VARCHAR(255),
    spark_application_id VARCHAR(255),
    
    -- Logs
    log_url TEXT
);

CREATE INDEX idx_runs_connection ON connection_runs(connection_id);
CREATE INDEX idx_runs_tenant ON connection_runs(tenant_id);
CREATE INDEX idx_runs_status ON connection_runs(run_status);
CREATE INDEX idx_runs_started ON connection_runs(started_at);

-- ============================================================================
-- UDF CATALOG (Registered user-defined functions)
-- ============================================================================

CREATE TABLE udf_catalog (
    udf_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    udf_name VARCHAR(255) NOT NULL UNIQUE,
    
    -- Function details
    description TEXT,
    language VARCHAR(50) NOT NULL, -- python, scala
    
    -- Function signature
    argument_types JSONB NOT NULL, -- [{"name": "amount", "type": "double"}, ...]
    return_type VARCHAR(50) NOT NULL, -- double, string, int, etc.
    
    -- Implementation
    implementation_code TEXT NOT NULL,
    
    -- Validation
    is_validated BOOLEAN DEFAULT FALSE,
    validation_errors TEXT,
    
    -- Deployment
    deployed_version VARCHAR(50),
    deployed_at TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100)
);

-- ============================================================================
-- SYSTEM CONFIGURATION & SECRETS
-- ============================================================================

CREATE TABLE system_config (
    config_key VARCHAR(255) PRIMARY KEY,
    config_value JSONB NOT NULL,
    config_type VARCHAR(50) DEFAULT 'general', -- general, redis, spark, airflow, secrets
    
    -- Encryption for sensitive values
    is_encrypted BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    description TEXT,
    updated_at TIMESTAMP DEFAULT NOW(),
    updated_by VARCHAR(100)
);

-- Insert default configs
INSERT INTO system_config (config_key, config_value, config_type, description) VALUES
('redis.cluster.hosts', '["redis-0:6379", "redis-1:6379", "redis-2:6379"]', 'redis', 'Redis cluster endpoints'),
('spark.default.executor.cores', '4', 'spark', 'Default Spark executor cores'),
('cost.cpu_core_hour_usd', '0.05', 'billing', 'Cost per CPU core-hour in USD'),
('cost.memory_gb_hour_usd', '0.01', 'billing', 'Cost per GB-hour of memory'),
('cost.event_per_million_usd', '0.10', 'billing', 'Cost per million events processed');

-- ============================================================================
-- ALERTS & NOTIFICATIONS
-- ============================================================================

CREATE TABLE alerts (
    alert_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    
    -- Alert details
    alert_type VARCHAR(100) NOT NULL, -- worker_crash, redis_outage, dq_failure, high_lag, schema_change
    severity VARCHAR(50) DEFAULT 'warning', -- info, warning, error, critical
    
    -- Alert message
    title VARCHAR(500) NOT NULL,
    message TEXT NOT NULL,
    
    -- Context
    source_id UUID REFERENCES sources(source_id) ON DELETE SET NULL,
    connection_id UUID REFERENCES connections(connection_id) ON DELETE SET NULL,
    
    -- Related entities
    related_data JSONB,
    
    -- Status
    status VARCHAR(50) DEFAULT 'active', -- active, acknowledged, resolved
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_alerts_tenant ON alerts(tenant_id);
CREATE INDEX idx_alerts_type ON alerts(alert_type);
CREATE INDEX idx_alerts_severity ON alerts(severity);
CREATE INDEX idx_alerts_status ON alerts(status);
CREATE INDEX idx_alerts_created ON alerts(created_at);

-- ============================================================================
-- AUDIT LOG (Complete activity tracking)
-- ============================================================================

CREATE TABLE audit_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Actor
    user_id VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(64) REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    
    -- Action
    action VARCHAR(100) NOT NULL, -- create_source, update_connection, pause_connection, approve_schema_change, etc.
    entity_type VARCHAR(50) NOT NULL, -- source, destination, connection, tenant, etc.
    entity_id VARCHAR(100),
    
    -- Changes
    old_value JSONB,
    new_value JSONB,
    
    -- Request context
    ip_address INET,
    user_agent TEXT,
    request_id VARCHAR(100),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Create partitions (monthly)
CREATE TABLE audit_log_2025_11 PARTITION OF audit_log
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE audit_log_2025_12 PARTITION OF audit_log
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE INDEX idx_audit_user ON audit_log(user_id);
CREATE INDEX idx_audit_tenant ON audit_log(tenant_id);
CREATE INDEX idx_audit_action ON audit_log(action);
CREATE INDEX idx_audit_created ON audit_log(created_at);

-- ============================================================================
-- FUNCTIONS & TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to all tables with updated_at
CREATE TRIGGER update_parents_updated_at BEFORE UPDATE ON parents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_tenants_updated_at BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_sources_updated_at BEFORE UPDATE ON sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_destinations_updated_at BEFORE UPDATE ON destinations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_connections_updated_at BEFORE UPDATE ON connections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_pipelines_updated_at BEFORE UPDATE ON transform_pipelines
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_policies_updated_at BEFORE UPDATE ON dq_policies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to calculate tenant daily usage (called by cron job)
CREATE OR REPLACE FUNCTION compute_tenant_daily_usage(p_date DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO tenant_daily_usage (
        tenant_id, usage_date,
        total_cpu_core_hours, total_memory_gb_hours, total_storage_gb_hours,
        total_events_processed, total_bytes_processed, total_cost_usd
    )
    SELECT 
        tenant_id,
        p_date,
        SUM(cpu_core_hours),
        SUM(memory_gb_hours),
        SUM(storage_gb_hours),
        SUM(events_processed),
        SUM(bytes_processed),
        SUM(estimated_cost_usd)
    FROM resource_usage
    WHERE DATE(window_start) = p_date
    GROUP BY tenant_id
    ON CONFLICT (tenant_id, usage_date) 
    DO UPDATE SET
        total_cpu_core_hours = EXCLUDED.total_cpu_core_hours,
        total_memory_gb_hours = EXCLUDED.total_memory_gb_hours,
        total_storage_gb_hours = EXCLUDED.total_storage_gb_hours,
        total_events_processed = EXCLUDED.total_events_processed,
        total_bytes_processed = EXCLUDED.total_bytes_processed,
        total_cost_usd = EXCLUDED.total_cost_usd,
        computed_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- VIEWS (Convenience queries)
-- ============================================================================

-- Active connections with source/destination details
CREATE VIEW v_active_connections AS
SELECT 
    c.connection_id,
    c.tenant_id,
    t.tenant_name,
    c.connection_name,
    c.status,
    c.sync_type,
    c.sync_mode,
    s.source_name,
    s.source_type,
    s.host as source_host,
    d.destination_name,
    d.destination_type,
    c.created_at,
    c.resource_limits
FROM connections c
JOIN tenants t ON c.tenant_id = t.tenant_id
JOIN sources s ON c.source_id = s.source_id
JOIN destinations d ON c.destination_id = d.destination_id
WHERE c.status = 'active' AND c.is_deleted = FALSE;

-- Tenant resource summary
CREATE VIEW v_tenant_resource_summary AS
SELECT 
    t.tenant_id,
    t.tenant_name,
    t.status,
    COUNT(DISTINCT s.source_id) as total_sources,
    COUNT(DISTINCT d.destination_id) as total_destinations,
    COUNT(DISTINCT c.connection_id) as total_connections,
    COUNT(DISTINCT CASE WHEN c.status = 'active' THEN c.connection_id END) as active_connections,
    COALESCE(SUM(du.total_events_processed), 0) as events_last_30_days,
    COALESCE(SUM(du.total_cost_usd), 0) as cost_last_30_days
FROM tenants t
LEFT JOIN sources s ON t.tenant_id = s.tenant_id AND s.is_deleted = FALSE
LEFT JOIN destinations d ON t.tenant_id = d.tenant_id AND d.is_deleted = FALSE
LEFT JOIN connections c ON t.tenant_id = c.tenant_id AND c.is_deleted = FALSE
LEFT JOIN tenant_daily_usage du ON t.tenant_id = du.tenant_id 
    AND du.usage_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY t.tenant_id, t.tenant_name, t.status;

-- ============================================================================
-- SAMPLE DATA (For testing)
-- ============================================================================

-- Insert sample parent
INSERT INTO parents (parent_id, parent_name, parent_type, subscription_tier) 
VALUES ('acme_bank', 'ACME Bank', 'bank', 'enterprise');

-- Insert sample tenant
INSERT INTO tenants (tenant_id, parent_id, tenant_name, display_name, status) 
VALUES ('tenant_001', 'acme_bank', 'Retail Banking Division', 'Retail Banking', 'active');

-- ============================================================================
-- GRANT PERMISSIONS (Adjust based on your roles)
-- ============================================================================

-- CREATE ROLE fusion_api;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fusion_api;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fusion_api;

-- CREATE ROLE fusion_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO fusion_readonly;

COMMENT ON DATABASE postgres IS 'Fusion CDC Engine - Metadata Database';

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

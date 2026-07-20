-- ============================================================================
-- SEED DATA: Sample Connector Definitions
-- ============================================================================
-- Populate connector_definitions with common source and destination types
-- Run after schema_postgres.sql or schema_mysql.sql
-- ============================================================================

-- PostgreSQL: Use uuid_generate_v4()
-- MySQL: Use UUID()

-- ============================================================================
-- SOURCE CONNECTORS
-- ============================================================================

INSERT INTO connector_definitions (
    connector_id,
    connector_name,
    connector_type,
    category,
    latest_version,
    default_config,
    required_fields,
    optional_fields,
    default_resource_limits,
    supports_cdc,
    supports_full_refresh,
    supports_incremental,
    documentation_url,
    created_by
) VALUES

-- MySQL Source
(
    'a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d', -- Replace with uuid_generate_v4() for Postgres, UUID() for MySQL
    'MySQL',
    'mysql',
    'source',
    '8.0.35',
    '{"port": 3306, "ssl_mode": "preferred"}',
    '["host", "port", "database", "username", "password"]',
    '["ssl_ca_cert", "ssl_client_cert", "ssl_client_key", "connection_timeout", "server_id"]',
    '{"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}',
    TRUE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/mysql',
    NULL
),

-- PostgreSQL Source
(
    'b2c3d4e5-f6a7-4b5c-9d0e-1f2a3b4c5d6e',
    'PostgreSQL',
    'postgresql',
    'source',
    '14.10',
    '{"port": 5432, "ssl_mode": "prefer"}',
    '["host", "port", "database", "username", "password"]',
    '["ssl_ca_cert", "ssl_client_cert", "ssl_client_key", "replication_slot", "publication", "schema"]',
    '{"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}',
    TRUE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/postgresql',
    NULL
),

-- MongoDB Source
(
    'c3d4e5f6-a7b8-4c5d-0e1f-2a3b4c5d6e7f',
    'MongoDB',
    'mongodb',
    'source',
    '6.0.12',
    '{"port": 27017, "auth_source": "admin"}',
    '["host", "port", "database", "username", "password"]',
    '["replica_set", "tls", "tls_ca_file", "connection_timeout", "auth_mechanism"]',
    '{"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}',
    TRUE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/mongodb',
    NULL
),

-- Oracle Source
(
    'd4e5f6a7-b8c9-4d5e-1f2a-3b4c5d6e7f8a',
    'Oracle',
    'oracle',
    'source',
    '21c',
    '{"port": 1521, "service_name": "ORCL"}',
    '["host", "port", "service_name", "username", "password"]',
    '["sid", "tns_admin", "wallet_location", "connection_timeout"]',
    '{"cpu_request": "1000m", "cpu_limit": "4000m", "memory_request": "2Gi", "memory_limit": "8Gi"}',
    TRUE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/oracle',
    NULL
),

-- SQL Server Source
(
    'e5f6a7b8-c9d0-4e5f-2a3b-4c5d6e7f8a9b',
    'SQL Server',
    'sqlserver',
    'source',
    '2022',
    '{"port": 1433, "encrypt": true}',
    '["host", "port", "database", "username", "password"]',
    '["instance", "trust_server_certificate", "connection_timeout", "application_intent"]',
    '{"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}',
    TRUE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/sqlserver',
    NULL
);

-- ============================================================================
-- DESTINATION CONNECTORS
-- ============================================================================

INSERT INTO connector_definitions (
    connector_id,
    connector_name,
    connector_type,
    category,
    latest_version,
    default_config,
    required_fields,
    optional_fields,
    default_resource_limits,
    supports_cdc,
    supports_full_refresh,
    supports_incremental,
    documentation_url,
    created_by
) VALUES

-- Snowflake Destination
(
    'f6a7b8c9-d0e1-4f5a-3b4c-5d6e7f8a9b0c',
    'Snowflake',
    'snowflake',
    'destination',
    '8.2.1',
    '{"warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}',
    '["account", "warehouse", "database", "schema", "username", "password"]',
    '["role", "warehouse_size", "keep_alive", "client_session_keep_alive"]',
    '{"cpu_request": "1000m", "cpu_limit": "4000m", "memory_request": "2Gi", "memory_limit": "8Gi"}',
    FALSE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/snowflake',
    NULL
),

-- BigQuery Destination
(
    'a7b8c9d0-e1f2-4a5b-4c5d-6e7f8a9b0c1d',
    'BigQuery',
    'bigquery',
    'destination',
    '2.34.1',
    '{"location": "US"}',
    '["project_id", "dataset", "credentials_json"]',
    '["location", "billing_project", "maximum_billing_tier", "default_dataset_expiration"]',
    '{"cpu_request": "1000m", "cpu_limit": "4000m", "memory_request": "2Gi", "memory_limit": "8Gi"}',
    FALSE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/bigquery',
    NULL
),

-- Databricks Destination
(
    'b8c9d0e1-f2a3-4b5c-5d6e-7f8a9b0c1d2e',
    'Databricks',
    'databricks',
    'destination',
    '13.3',
    '{"catalog": "main", "schema": "default"}',
    '["server_hostname", "http_path", "access_token", "catalog", "schema"]',
    '["staging_location", "purge_staging_data", "enable_schema_evolution"]',
    '{"cpu_request": "1000m", "cpu_limit": "4000m", "memory_request": "2Gi", "memory_limit": "8Gi"}',
    FALSE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/databricks',
    NULL
),

-- PostgreSQL Destination (Data Warehouse)
(
    'c9d0e1f2-a3b4-4c5d-6e7f-8a9b0c1d2e3f',
    'PostgreSQL Warehouse',
    'postgresql',
    'destination',
    '14.10',
    '{"port": 5432, "ssl_mode": "require", "schema": "warehouse"}',
    '["host", "port", "database", "schema", "username", "password"]',
    '["ssl_ca_cert", "ssl_client_cert", "ssl_client_key", "batch_size", "connection_pool_size"]',
    '{"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}',
    FALSE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/postgresql-warehouse',
    NULL
),

-- Amazon S3 Destination (Data Lake)
(
    'd0e1f2a3-b4c5-4d6e-7f8a-9b0c1d2e3f4a',
    'Amazon S3',
    's3',
    'destination',
    '1.4.2',
    '{"region": "us-east-1", "format": "parquet", "compression": "snappy"}',
    '["bucket_name", "bucket_path", "access_key_id", "secret_access_key", "region"]',
    '["format", "compression", "partition_columns", "file_size_mb", "enable_partitioning"]',
    '{"cpu_request": "500m", "cpu_limit": "2000m", "memory_request": "1Gi", "memory_limit": "4Gi"}',
    FALSE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/s3',
    NULL
),

-- Redshift Destination
(
    'e1f2a3b4-c5d6-4e7f-8a9b-0c1d2e3f4a5b',
    'Amazon Redshift',
    'redshift',
    'destination',
    '1.0.38',
    '{"port": 5439}',
    '["host", "port", "database", "schema", "username", "password"]',
    '["ssl", "s3_bucket_name", "s3_bucket_region", "purge_staging_data"]',
    '{"cpu_request": "1000m", "cpu_limit": "4000m", "memory_request": "2Gi", "memory_limit": "8Gi"}',
    FALSE,
    TRUE,
    TRUE,
    'https://docs.dcraftfusion.io/connectors/redshift',
    NULL
);

-- ============================================================================
-- CONNECTOR VERSIONS (Sample version history for MySQL connector)
-- ============================================================================

INSERT INTO connector_versions (
    version_id,
    connector_id,
    version,
    release_notes,
    breaking_changes,
    new_features,
    bug_fixes,
    docker_image,
    docker_tag,
    is_stable,
    released_at
) VALUES

-- MySQL 8.0.35 (latest)
(
    'f2a3b4c5-d6e7-4f8a-9b0c-1d2e3f4a5b6c',
    'a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d', -- MySQL connector_id
    '8.0.35',
    'Improved CDC performance with optimized binlog parsing. Added support for MySQL 8.0.35.',
    '[]',
    '["Optimized binlog parser", "Support for MySQL 8.0.35", "Improved GTID handling"]',
    '["Fixed memory leak in long-running CDC", "Corrected timezone handling for DATETIME columns"]',
    'fusion/mysql-connector',
    'v8.0.35',
    TRUE,
    '2025-11-15 10:00:00'
),

-- MySQL 8.0.33
(
    'a3b4c5d6-e7f8-4a9b-0c1d-2e3f4a5b6c7d',
    'a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d',
    '8.0.33',
    'Stable release with improved error handling.',
    '[]',
    '["Better error messages", "Connection retry logic"]',
    '["Fixed issue with special characters in passwords"]',
    'fusion/mysql-connector',
    'v8.0.33',
    TRUE,
    '2025-09-20 14:30:00'
),

-- MySQL 8.0.30
(
    'b4c5d6e7-f8a9-4b0c-1d2e-3f4a5b6c7d8e',
    'a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d',
    '8.0.30',
    'Legacy version, deprecated.',
    '["Removed support for MySQL 5.6"]',
    '[]',
    '[]',
    'fusion/mysql-connector',
    'v8.0.30',
    FALSE,
    '2025-06-10 09:00:00'
);

-- ============================================================================
-- SYSTEM CONFIGURATION (Additional entries)
-- ============================================================================

INSERT INTO system_config (config_id, config_key, config_value, description, data_type, is_sensitive)
VALUES
    ('c5d6e7f8-a9b0-4c1d-2e3f-4a5b6c7d8e9f', 'kafka_bootstrap_servers', 'localhost:9092', 'Kafka bootstrap servers for event streaming', 'string', FALSE),
    ('d6e7f8a9-b0c1-4d2e-3f4a-5b6c7d8e9f0a', 'redis_host', 'localhost', 'Redis host for stream tracking', 'string', FALSE),
    ('e7f8a9b0-c1d2-4e3f-4a5b-6c7d8e9f0a1b', 'redis_port', '6379', 'Redis port', 'integer', FALSE),
    ('f8a9b0c1-d2e3-4f4a-5b6c-7d8e9f0a1b2c', 's3_bucket_logs', 'fusion-cdc-logs', 'S3 bucket for Spark logs', 'string', FALSE),
    ('a9b0c1d2-e3f4-4a5b-6c7d-8e9f0a1b2c3d', 'prometheus_pushgateway_url', 'http://pushgateway:9091', 'Prometheus Pushgateway URL', 'string', FALSE);

-- ============================================================================
-- FEATURE FLAGS (Sample flags)
-- ============================================================================

INSERT INTO feature_flags (flag_id, flag_name, is_enabled, description, rollout_percentage)
VALUES
    ('b0c1d2e3-f4a5-4b6c-7d8e-9f0a1b2c3d4e', 'enable_json_auto_flatten', TRUE, 'Automatically detect and flatten JSON columns', 100),
    ('c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f', 'enable_schema_auto_apply', FALSE, 'Automatically apply non-breaking schema changes', 0),
    ('d2e3f4a5-b6c7-4d8e-9f0a-1b2c3d4e5f6a', 'enable_spark_autoscaling', TRUE, 'Enable Spark executor autoscaling', 100),
    ('e3f4a5b6-c7d8-4e9f-0a1b-2c3d4e5f6a7b', 'enable_dq_auto_remediation', FALSE, 'Automatically remediate DQ failures', 0);

-- ============================================================================
-- NOTES:
-- ============================================================================
-- For PostgreSQL:
--   Replace static UUIDs with: DEFAULT uuid_generate_v4()
--   Example: connector_id UUID DEFAULT uuid_generate_v4()
--
-- For MySQL:
--   Replace static UUIDs with: UUID()
--   Example: VALUES (UUID(), 'MySQL', 'mysql', ...)
--
-- This seed data provides:
--   - 5 source connector definitions (MySQL, Postgres, MongoDB, Oracle, SQL Server)
--   - 6 destination connector definitions (Snowflake, BigQuery, Databricks, Postgres, S3, Redshift)
--   - 3 version history entries for MySQL connector
--   - 5 additional system config entries
--   - 4 feature flag entries
-- ============================================================================

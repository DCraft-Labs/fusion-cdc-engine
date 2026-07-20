-- ============================================================================
-- POSTGRES JSONB GIN INDEXES
-- ============================================================================
-- Optional performance indexes for JSONB columns
-- Run this after schema_postgres.sql if using JSONB querying extensively
-- ============================================================================

-- Connector Definitions
CREATE INDEX idx_connector_defs_default_config_gin ON connector_definitions USING GIN (default_config);
CREATE INDEX idx_connector_defs_resource_limits_gin ON connector_definitions USING GIN (default_resource_limits);

-- Connector Versions
CREATE INDEX idx_connector_versions_breaking_changes_gin ON connector_versions USING GIN (breaking_changes);
CREATE INDEX idx_connector_versions_features_gin ON connector_versions USING GIN (new_features);

-- Sources
CREATE INDEX idx_sources_ssl_config_gin ON sources USING GIN (ssl_config);
CREATE INDEX idx_sources_config_gin ON sources USING GIN (config);
CREATE INDEX idx_sources_discovery_cache_gin ON sources USING GIN (discovery_cache);

-- Destinations
CREATE INDEX idx_destinations_connection_config_gin ON destinations USING GIN (connection_config);

-- Transform Pipelines
CREATE INDEX idx_transform_pipelines_spec_gin ON transform_pipelines USING GIN (spec);

-- DQ Policies
CREATE INDEX idx_dq_policies_policy_gin ON dq_policies USING GIN (policy);

-- UDF Catalog
CREATE INDEX idx_udf_catalog_argument_types_gin ON udf_catalog USING GIN (argument_types);
CREATE INDEX idx_udf_catalog_dependencies_gin ON udf_catalog USING GIN (dependencies);
CREATE INDEX idx_udf_catalog_test_cases_gin ON udf_catalog USING GIN (test_cases);

-- Connections
CREATE INDEX idx_connections_resource_limits_gin ON connections USING GIN (resource_limits);

-- Streams
CREATE INDEX idx_streams_primary_key_gin ON streams USING GIN (primary_key);
CREATE INDEX idx_streams_transform_overrides_gin ON streams USING GIN (transform_overrides);
CREATE INDEX idx_streams_json_columns_gin ON streams USING GIN (json_columns);

-- Connection Webhooks
CREATE INDEX idx_webhooks_headers_gin ON connection_alert_webhooks USING GIN (webhook_headers);
CREATE INDEX idx_webhooks_payload_template_gin ON connection_alert_webhooks USING GIN (payload_template);
CREATE INDEX idx_webhooks_trigger_events_gin ON connection_alert_webhooks USING GIN (trigger_events);

-- Connection Runs
CREATE INDEX idx_runs_dq_results_gin ON connection_runs USING GIN (dq_results_summary);

-- Spark Job Queue
CREATE INDEX idx_spark_queue_config_gin ON spark_job_queue USING GIN (spark_config);

-- Spark Executor History
CREATE INDEX idx_spark_exec_hist_metrics_gin ON spark_executor_history USING GIN (metrics_snapshot);

-- Checkpoint State
CREATE INDEX idx_checkpoint_metadata_gin ON checkpoint_state USING GIN (checkpoint_metadata);

-- CDC Position History
CREATE INDEX idx_cdc_position_metadata_gin ON cdc_position_history USING GIN (position_metadata);

-- Schema Change Events
CREATE INDEX idx_schema_events_change_details_gin ON schema_change_events USING GIN (change_details);
CREATE INDEX idx_schema_events_schema_before_gin ON schema_change_events USING GIN (schema_before);
CREATE INDEX idx_schema_events_schema_after_gin ON schema_change_events USING GIN (schema_after);

-- JSON Schema Cache
CREATE INDEX idx_json_cache_json_schema_gin ON json_schema_cache USING GIN (json_schema);

-- JSON Schema Evolution
CREATE INDEX idx_json_evolution_old_schema_gin ON json_schema_evolution USING GIN (old_schema);
CREATE INDEX idx_json_evolution_new_schema_gin ON json_schema_evolution USING GIN (new_schema);
CREATE INDEX idx_json_evolution_diff_gin ON json_schema_evolution USING GIN (schema_diff);

-- DQ Rule Results
CREATE INDEX idx_dq_results_rule_config_gin ON dq_rule_results USING GIN (rule_config);
CREATE INDEX idx_dq_results_expected_value_gin ON dq_rule_results USING GIN (expected_value);
CREATE INDEX idx_dq_results_actual_value_gin ON dq_rule_results USING GIN (actual_value);

-- DQ Violation Samples
CREATE INDEX idx_dq_samples_record_pk_gin ON dq_violation_samples USING GIN (record_primary_key);
CREATE INDEX idx_dq_samples_record_sample_gin ON dq_violation_samples USING GIN (record_sample);

-- Transformation Logs
CREATE INDEX idx_transform_logs_error_samples_gin ON transformation_logs USING GIN (error_samples);

-- UDF Execution Stats (none - no JSONB columns)

-- Redis Stream Tracking (none - no JSONB columns)

-- Event Dead Letter Queue
CREATE INDEX idx_dlq_event_payload_gin ON event_dead_letter_queue USING GIN (event_payload);

-- Resource Usage (none - no JSONB columns)

-- Connection Health Checks
CREATE INDEX idx_health_checks_details_gin ON connection_health_checks USING GIN (check_details);

-- Alerts
CREATE INDEX idx_alerts_context_gin ON alerts USING GIN (alert_context);

-- Audit Log
CREATE INDEX idx_audit_log_changes_gin ON audit_log USING GIN (changes);

-- Feature Flags
CREATE INDEX idx_feature_flags_tenants_gin ON feature_flags USING GIN (enabled_for_tenants);
CREATE INDEX idx_feature_flags_banks_gin ON feature_flags USING GIN (enabled_for_banks);

-- Maintenance Windows
CREATE INDEX idx_maintenance_connection_ids_gin ON maintenance_windows USING GIN (connection_ids);

-- ============================================================================
-- JSONB PATH INDEXES (Specific paths for common queries)
-- ============================================================================

-- Example: Query connections by specific resource limit
CREATE INDEX idx_connections_cpu_limit ON connections USING GIN ((resource_limits -> 'cpu_limit'));
CREATE INDEX idx_connections_memory_limit ON connections USING GIN ((resource_limits -> 'memory_limit'));

-- Example: Query sources by specific config properties
CREATE INDEX idx_sources_config_server_id ON sources USING GIN ((config -> 'server_id'));
CREATE INDEX idx_sources_config_gtid ON sources USING GIN ((config -> 'gtid_enabled'));

-- Example: Query transforms by type
CREATE INDEX idx_transform_spec_transforms ON transform_pipelines USING GIN ((spec -> 'transforms'));

-- ============================================================================
-- NOTES:
-- ============================================================================
-- GIN indexes are larger than B-tree indexes but much faster for:
-- - Containment queries: config @> '{"key": "value"}'
-- - Existence queries: config ? 'key'
-- - Path queries: config -> 'nested' ->> 'property'
--
-- Trade-off: Higher write overhead, more storage space
-- Recommendation: Add these indexes only if:
-- 1. You have complex JSONB queries in your application
-- 2. Query performance is more critical than write performance
-- 3. Storage is not a constraint
-- ============================================================================

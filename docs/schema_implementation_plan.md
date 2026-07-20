# CDC Engine - Complete Schema Implementation Plan

## 🎯 Integration Strategy

### Main Fusion App Integration
**REUSE existing tables from main application:**
- `banks` - Parent organizations (already exists)
- `sub_tenants` - Tenant isolation with dedicated databases (already exists)
- `users`, `user_banks`, `user_sub_tenants`, `user_products` - User management (already exists)
- `products`, `product_roles`, `product_role_permissions` - Product licensing (already exists)

**CDC Engine will be:**
- One entry in `products` table
- Use `sub_tenant_id` foreign keys instead of custom `tenant_id`
- Leverage existing Keycloak authentication

---

## 📊 Complete Table List (42 Tables Total)

### Category 1: Core CDC Configuration (11 tables)
1. `connector_definitions` - Connector templates (MySQL source v1.2.3)
2. `connector_versions` - Version history for upgrades
3. `sources` - Source database instances
4. `destinations` - Destination warehouse instances
5. `connections` - Source→Destination sync config
6. `streams` - Table/collection level configuration
7. `connection_alert_webhooks` - User-defined alert endpoints
8. `sync_mode_config` - SCD Type 2 configuration
9. `transform_pipelines` - Transformation specifications
10. `dq_policies` - Data quality policies
11. `udf_catalog` - User-defined functions

### Category 2: Execution & Monitoring (8 tables)
12. `connection_runs` - Job execution history
13. `spark_applications` - Spark app lifecycle tracking
14. `spark_job_queue` - Job queue with priorities
15. `spark_executors` - Active executor tracking
16. `spark_executor_history` - Autoscaling history
17. `checkpoint_state` - CDC checkpointing
18. `cdc_lag_metrics` - Replication lag tracking
19. `cdc_position_history` - Binlog/WAL position history

### Category 3: Schema Evolution & JSON Handling (4 tables)
20. `schema_change_events` - Schema evolution tracking
21. `json_schema_cache` - Detected JSON schemas
22. `json_flatten_rules` - Column-level flatten config
23. `json_schema_evolution` - JSON structure changes

### Category 4: Transformations & DQ (6 tables)
24. `transformation_logs` - Per-transform execution
25. `transformation_dependencies` - DAG tracking
26. `udf_execution_stats` - UDF performance metrics
27. `dq_rule_results` - Per-run DQ evaluation
28. `dq_violations` - Failed DQ checks
29. `dq_violation_samples` - Sample failed records

### Category 5: Event Tracking (3 tables)
30. `redis_stream_tracking` - Active Redis streams
31. `event_dead_letter_queue` - Failed events
32. `event_dlq_retry_history` - DLQ retry attempts

### Category 6: Resource & Cost Tracking (3 tables)
33. `resource_usage` - Hourly resource consumption
34. `tenant_daily_usage` - Daily rollup for billing
35. `resource_quota_violations` - Quota breach tracking

### Category 7: Observability (4 tables)
36. `alerts` - System alerts
37. `connection_health_checks` - Regular health checks
38. `worker_heartbeats` - CDC worker liveness
39. `audit_log` - Complete activity tracking (partitioned)

### Category 8: System Configuration (3 tables)
40. `system_config` - Key-value configuration
41. `feature_flags` - Feature toggles per tenant
42. `maintenance_windows` - Scheduled maintenance

---

## 🔧 Database Compatibility Strategy

### Approach: Dual DDL Files
**File 1: `schema_postgres.sql`**
- Optimized for Postgres features
- Native UUID type
- JSONB with GIN indexes
- Native BOOLEAN
- Advanced partitioning
- Array types where beneficial

**File 2: `schema_mysql.sql`**
- MySQL 8.0+ compatible
- CHAR(36) or VARCHAR(36) for UUIDs
- JSON (not JSONB)
- TINYINT(1) for booleans
- MySQL partitioning syntax
- JSON arrays instead of native arrays

**File 3: `schema_common.sql`** (optional)
- Shared base structure
- Used by migration tools

### Data Type Mapping

| Logical Type | Postgres | MySQL |
|--------------|----------|-------|
| UUID | `UUID` | `CHAR(36)` |
| JSON | `JSONB` | `JSON` |
| Boolean | `BOOLEAN` | `TINYINT(1)` |
| Timestamp | `TIMESTAMP WITH TIME ZONE` | `DATETIME` (UTC) |
| Large Text | `TEXT` | `TEXT` |
| Encrypted Data | Application-level AES-256-GCM (same for both) |

---

## 🗂️ Schema Details by Category

### Category 1: Core CDC Configuration

#### `connector_definitions`
```sql
Purpose: Template for each connector type (mysql_source, postgres_destination)
Key columns:
- connector_id (PK)
- connector_type (mysql, postgresql, mongodb, snowflake, etc.)
- version
- default_config (JSON) - Default ports, required fields
- default_resource_limits (JSON) - CPU/memory defaults
- is_active
```

#### `connector_versions`
```sql
Purpose: Version history for connector upgrades
Key columns:
- version_id (PK)
- connector_id (FK)
- version (e.g., "1.2.3")
- release_notes
- breaking_changes
- released_at
```

#### `sources`
```sql
Purpose: Source database instances
Key columns:
- source_id (PK)
- sub_tenant_id (FK → sub_tenants)
- bank_id (FK → banks)
- connector_definition_id (FK)
- connector_version (pinned version)
- source_name
- host, port, database_name
- credentials_encrypted (app-level encryption)
- ssl_config (JSON)
- status (draft, testing, active, paused, error)
```

#### `connections`
```sql
Purpose: Source→Destination sync configuration
Key columns:
- connection_id (PK)
- sub_tenant_id (FK)
- bank_id (FK)
- source_id (FK)
- destination_id (FK)
- sync_mode (FULL_REFRESH_OVERWRITE, CDC_INCREMENTAL_DEDUPED_HISTORY, etc.)
- sync_type (REALTIME, SCHEDULED)
- schedule_cron
- resource_limits (JSON) - Per-connection overrides
- status (active, paused, error)
- paused_at, paused_by, pause_reason
```

#### `connection_alert_webhooks`
```sql
Purpose: User-defined webhook endpoints for alerts
Key columns:
- webhook_id (PK)
- connection_id (FK)
- webhook_url
- webhook_method (POST, PUT)
- webhook_headers (JSON)
- payload_template (JSON)
- trigger_events (JSON array) - ["worker_crash", "dq_failure", "high_lag"]
- is_active
```

#### `sync_mode_config`
```sql
Purpose: SCD Type 2 configuration
Key columns:
- config_id (PK)
- connection_id (FK)
- stream_id (FK)
- valid_from_column (default: 'valid_from')
- valid_to_column (default: 'valid_to')
- is_current_column (default: 'is_current')
- end_of_time_value (default: '9999-12-31 23:59:59')
- soft_delete_handling (KEEP, MARK_DELETED, EXCLUDE)
```

### Category 2: Execution & Monitoring

#### `spark_applications`
```sql
Purpose: Track Spark application lifecycle
Key columns:
- application_id (PK, from Spark)
- connection_id (FK)
- stream_id (FK) - NULL for connection-level jobs
- job_type (realtime_streaming, batch_scheduled, initial_load)
- spark_ui_url (expires after 48 hours)
- driver_pod_name
- executor_count
- status (pending, running, success, failed)
- started_at, completed_at
- logs_s3_path (for heavy logs)
```

#### `spark_job_queue`
```sql
Purpose: Job queue with priority
Key columns:
- queue_id (PK)
- connection_id (FK)
- priority (1=highest, 10=lowest)
- requested_at
- scheduled_at
- claimed_by_worker
- status (queued, claimed, running, completed)
```

#### `spark_executors`
```sql
Purpose: Active executor tracking
Key columns:
- executor_id (PK)
- application_id (FK)
- pod_name
- cpu_cores
- memory_gb
- started_at
- last_heartbeat_at
```

#### `spark_executor_history`
```sql
Purpose: Autoscaling history
Key columns:
- history_id (PK)
- application_id (FK)
- timestamp
- min_executors
- max_executors
- current_executors
- scaling_reason (high_lag, low_utilization, manual)
```

#### `cdc_lag_metrics`
```sql
Purpose: Real-time lag tracking
Key columns:
- metric_id (PK)
- source_id (FK)
- connection_id (FK)
- current_position (JSON) - {"log_file": "...", "log_pos": ...}
- master_position (JSON)
- lag_events (events behind)
- lag_seconds (time behind)
- measured_at
```

#### `cdc_position_history`
```sql
Purpose: Historical position tracking (time-series)
Key columns:
- history_id (PK)
- source_id (FK)
- timestamp
- position (JSON)
- events_processed_since_last
- retention: 30 days
```

### Category 3: Schema Evolution & JSON Handling

#### `json_schema_cache`
```sql
Purpose: Cached detected JSON schemas
Key columns:
- cache_id (PK)
- stream_id (FK)
- column_name
- detected_schema (JSON) - Full JSON Schema specification
- sample_count (records sampled)
- confidence_score (0.0-1.0)
- detected_at
- version (incremented on evolution)
```

#### `json_flatten_rules`
```sql
Purpose: Per-column JSON flattening configuration
Key columns:
- rule_id (PK)
- connection_id (FK)
- stream_id (FK)
- column_name
- flatten_mode (inline, child_table, none)
- child_table_name (if child_table mode)
- max_depth (default: 3)
- flatten_arrays (boolean)
- is_active
```

#### `json_schema_evolution`
```sql
Purpose: Track JSON structure changes
Key columns:
- evolution_id (PK)
- json_schema_cache_id (FK)
- old_schema (JSON)
- new_schema (JSON)
- changes_detected (JSON) - {"added_fields": [...], "removed_fields": [...]}
- detected_at
- auto_applied (boolean)
```

### Category 4: Transformations & DQ

#### `transformation_logs`
```sql
Purpose: Per-transformation execution tracking
Key columns:
- log_id (PK)
- connection_run_id (FK)
- transform_id (reference to transform in pipeline)
- transform_type (cast, mask, expression, udf)
- records_processed
- records_failed
- execution_time_ms
- error_message
- error_samples (JSON) - First 10 errors
```

#### `transformation_dependencies`
```sql
Purpose: DAG tracking for transform order
Key columns:
- dependency_id (PK)
- pipeline_id (FK)
- transform_id
- depends_on_transform_id
- dependency_type (column_reference, udf_call, manual)
- auto_detected (boolean)
```

#### `udf_execution_stats`
```sql
Purpose: Per-invocation UDF performance
Key columns:
- stat_id (PK)
- connection_run_id (FK)
- udf_name
- invocation_count
- total_execution_ms
- avg_execution_ms
- max_execution_ms
- min_execution_ms
- error_count
- memory_peak_mb
```

#### `dq_rule_results`
```sql
Purpose: Per-run DQ evaluation
Key columns:
- result_id (PK)
- connection_run_id (FK)
- stream_id (FK)
- rule_type (row_count_match, null_ratio_check, etc.)
- status (PASS, WARN, FAIL)
- expected_value
- actual_value
- threshold
- evaluated_at
```

#### `dq_violations`
```sql
Purpose: Failed DQ checks
Key columns:
- violation_id (PK)
- dq_rule_result_id (FK)
- violation_count
- severity (warning, error, critical)
- action_taken (alert, block_sync, manual_review)
```

#### `dq_violation_samples`
```sql
Purpose: Sample failed records for debugging
Key columns:
- sample_id (PK)
- dq_violation_id (FK)
- sample_record (JSON) - Full record that violated rule
- violation_reason
- row_number
- retention: 90 days (env configurable)
```

### Category 5: Event Tracking

#### `redis_stream_tracking`
```sql
Purpose: Active Redis stream keys
Key columns:
- tracking_id (PK)
- connection_id (FK)
- stream_key (e.g., "cdc:bank_001:tenant_001:source_001:public:orders")
- last_event_id
- message_count_estimate
- consumer_group_name
- last_consumed_at
```

#### `event_dead_letter_queue`
```sql
Purpose: Failed events that couldn't be processed
Key columns:
- dlq_id (PK)
- connection_id (FK)
- original_stream_key
- event_payload (JSON)
- error_message
- retry_count
- max_retries (default: 3)
- first_failed_at
- last_retry_at
- status (pending_retry, exhausted, manual_review)
- retention: 7 days (env configurable)
```

#### `event_dlq_retry_history`
```sql
Purpose: DLQ retry attempts
Key columns:
- retry_id (PK)
- dlq_id (FK)
- retry_attempt
- retry_at
- retry_result (success, failed)
- error_message
```

### Category 6: Resource & Cost Tracking

#### `resource_usage`
```sql
Purpose: Hourly resource consumption
Key columns:
- usage_id (PK)
- sub_tenant_id (FK)
- bank_id (FK)
- connection_id (FK)
- window_start, window_end
- cpu_core_hours
- memory_gb_hours
- storage_gb_hours
- events_processed
- spark_driver_hours
- spark_executor_hours
- estimated_cost_usd
```

#### `resource_quota_violations`
```sql
Purpose: Track quota breaches
Key columns:
- violation_id (PK)
- sub_tenant_id (FK)
- quota_type (cpu, memory, events_per_day)
- limit_value
- actual_value
- violated_at
- alert_sent (boolean)
```

### Category 7: Observability

#### `connection_health_checks`
```sql
Purpose: Regular health checks (every 1 minute)
Key columns:
- check_id (PK)
- connection_id (FK)
- check_type (connectivity, lag, resource_usage)
- status (healthy, degraded, unhealthy)
- response_time_ms
- error_message
- checked_at
```

#### `worker_heartbeats`
```sql
Purpose: CDC worker liveness (30 second timeout)
Key columns:
- heartbeat_id (PK)
- worker_id
- worker_pod_name
- connection_id (FK)
- last_heartbeat_at
- status (alive, stale, dead)
- cpu_usage_percent
- memory_usage_mb
```

---

## 🚀 Implementation Sequence

### Phase 1: Core Tables (Week 1)
1. Create `schema_postgres.sql` with connector definitions, sources, destinations, connections
2. Create `schema_mysql.sql` equivalent
3. Add integration points (sub_tenant_id, bank_id foreign keys)

### Phase 2: Execution & Monitoring (Week 1-2)
4. Add Spark operator tables
5. Add CDC lag tracking
6. Add checkpoint state

### Phase 3: Advanced Features (Week 2)
7. Add JSON flattening tables
8. Add DQ execution tables
9. Add transformation tracking

### Phase 4: Observability & Events (Week 3)
10. Add event tracking & DLQ
11. Add health checks & heartbeats
12. Add alert webhooks

### Phase 5: Testing & Migration (Week 3-4)
13. Create Alembic migrations for Postgres
14. Create Flyway migrations for MySQL
15. Integration testing with main Fusion app

---

## 🔑 Key Design Decisions

1. **UUID Strategy**: VARCHAR(36) in both databases for portability
2. **JSON Storage**: JSONB in Postgres (indexed), JSON in MySQL
3. **Boolean**: BOOLEAN in Postgres, TINYINT(1) in MySQL
4. **Timestamps**: TIMESTAMPTZ in Postgres, DATETIME (UTC) in MySQL
5. **Encryption**: Application-level (Python cryptography library)
6. **Partitioning**: Database-specific syntax in separate files
7. **Multi-tenancy**: Use existing sub_tenants.database_name for isolation
8. **Authentication**: Leverage existing Keycloak integration
9. **Permissions**: Use product_role_permissions from main app

---

## 📝 Environment Variables

```bash
# DQ retention
DQ_VIOLATION_RETENTION_DAYS=90

# DLQ retention
EVENT_DLQ_RETENTION_DAYS=7

# Health check frequency
HEALTH_CHECK_INTERVAL_SECONDS=60

# Worker heartbeat timeout
WORKER_HEARTBEAT_TIMEOUT_SECONDS=30

# Spark UI URL expiry
SPARK_UI_URL_RETENTION_HOURS=48

# CDC lag alert threshold
CDC_LAG_ALERT_THRESHOLD_SECONDS=60
```

---

## ✅ Next Steps

1. ✅ Create complete `schema_postgres.sql`
2. ✅ Create complete `schema_mysql.sql`
3. Create migration scripts (Alembic + Flyway)
4. Document integration with main Fusion app
5. Create API specifications for metadata service

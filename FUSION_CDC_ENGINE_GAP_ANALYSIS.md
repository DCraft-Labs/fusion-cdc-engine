# Fusion CDC Engine — Complete Gap Analysis & Implementation Status

> **As of:** April 2026  
> **Overall Completion:** ~42% of total planned system  
> **Control Plane:** ~75% complete  
> **Data Plane:** 0% complete  
> **Spark Consumer:** 0% complete

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [Overall Completion Dashboard](#2-overall-completion-dashboard)
3. [Database Schema — All 42+ Tables](#3-database-schema--all-42-tables)
4. [Control Plane — Detailed Status](#4-control-plane--detailed-status)
5. [Data Plane — Gap (Phase 2)](#5-data-plane--gap-phase-2)
6. [Spark Consumer — Gap (Phase 3)](#6-spark-consumer--gap-phase-3)
7. [Infrastructure & DevOps — Gap (Phase 5)](#7-infrastructure--devops--gap-phase-5)
8. [What Was Last Completed (Stop Point)](#8-what-was-last-completed-stop-point)
9. [Where to Resume — Concrete Next Steps](#9-where-to-resume--concrete-next-steps)
10. [Full Development Roadmap](#10-full-development-roadmap)
11. [File-by-File Status Inventory](#11-file-by-file-status-inventory)

---

## 1. Project Summary

Fusion CDC Engine is a **multi-tenant, log-based Change Data Capture platform** built as a FastAPI + Python + Postgres + Redis + Spark stack. It is organized into two architectural layers:

```
┌─────────────────────────────────────────────────────────────┐
│                   CONTROL PLANE  (FastAPI)                   │
│   Auth + RBAC | Sources | Destinations | Connections         │
│   Data Quality | Alerting | Schema Evolution | Monitoring     │
│   → Backed by PostgreSQL 16 metadata database (42 tables)    │
└─────────────────────────────────────────────────────────────┘
                            │ assigns / orchestrates
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     DATA PLANE  ← NOT BUILT YET              │
│   CDC Workers: MySQL binlog | PG WAL | MongoDB change streams│
│   → Publish events to Redis Streams                          │
│   → Dual-layer checkpoint (SQLite local + Postgres central)  │
└─────────────────────────────────────────────────────────────┘
                            │ events
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  SPARK CONSUMER  ← NOT BUILT YET             │
│   Structured Streaming | Transform pipeline execution        │
│   DQ checks | Write to Postgres DW + Iceberg (S3/Azure)      │
└─────────────────────────────────────────────────────────────┘
```

**Tech Stack:**
| Component | Technology |
|-----------|-----------|
| Control Plane API | FastAPI 0.109, Python 3.11+ |
| ORM | SQLAlchemy 2.0 |
| Migrations | Alembic 1.13 |
| Metadata DB | PostgreSQL 16 |
| Auth | JWT (python-jose) + Keycloak |
| Event Transport | Redis 7.x (Redis Streams) |
| Data Processing | PySpark 3.4+ (not yet started) |
| CDC Libraries | python-mysql-replication, psycopg2 replication API, pymongo (not yet started) |
| Destinations | Postgres DW (UPSERT), Apache Iceberg (S3/Azure/GCS) |
| Container | Docker + Kubernetes |
| Monitoring | Prometheus + Grafana |

---

## 2. Overall Completion Dashboard

```
PHASE 1: Foundation & Control Plane
████████████████████████░░░░  ~78%

PHASE 2: CDC Workers / Data Plane
░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0%

PHASE 3: Spark Consumer + Transformations
░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0%

PHASE 4: Integration & Testing
██░░░░░░░░░░░░░░░░░░░░░░░░░░   ~8% (DB setup tests only)

PHASE 5: Production / K8s / Docs
░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0%

PHASE 6: Frontend (Deferred)
░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0%

─────────────────────────────
TOTAL SYSTEM COMPLETION: ~42%
```

### Component-Level Breakdown

| Component | Status | Completion |
|-----------|--------|-----------|
| Database Schema (42 tables) | ✅ Complete | 100% |
| SQLAlchemy ORM Models | ✅ Complete | 100% |
| Alembic Migrations | ✅ Complete | 100% |
| JWT Authentication | ✅ Complete | 100% |
| RBAC (Roles + Permissions) | ✅ Complete | 100% |
| Tenant Isolation Middleware | ✅ Complete | 100% |
| Sources API (CRUD + test + discovery) | ✅ Complete | 95% |
| Destinations API | ✅ Complete | 90% |
| Connections API (17 endpoints) | ✅ Complete | 90% |
| Streams API | ✅ Complete | 85% |
| Connector Definitions API | ✅ Complete | 90% |
| Data Quality API (18 endpoints) | ✅ Complete | 85% |
| Alerting API (11 models, 33 schemas) | ✅ Complete | 85% |
| Schema Evolution API | ⚠️ Stub routes | 10% |
| Transformations API | ⚠️ Stub routes | 10% |
| UDFs API | ⚠️ Stub routes | 10% |
| Monitoring API | ⚠️ Stub routes | 10% |
| MySQL CDC Connector | ❌ Not started | 0% |
| PostgreSQL CDC Connector | ❌ Not started | 0% |
| MongoDB CDC Connector | ❌ Not started | 0% |
| Polling Connector | ❌ Not started | 0% |
| CDC Worker Base Framework | ❌ Not started | 0% |
| Redis Stream Publisher | ❌ Not started | 0% |
| Checkpoint Manager (SQLite + PG) | ❌ Not started | 0% |
| Fallback Disk Queue | ❌ Not started | 0% |
| Spark CDC Consumer (Streaming) | ❌ Not started | 0% |
| Spark Batch Consumer (Scheduled) | ❌ Not started | 0% |
| Transform Pipeline Executor (Spark) | ❌ Not started | 0% |
| DQ Engine in Spark | ❌ Not started | 0% |
| Postgres DW Writer (UPSERT) | ❌ Not started | 0% |
| Iceberg Writer (MERGE INTO) | ❌ Not started | 0% |
| Worker Orchestration (K8s) | ❌ Not started | 0% |
| Prometheus Metrics | ❌ Not started | 0% |
| Grafana Dashboards | ❌ Not started | 0% |
| Kubernetes Manifests | ❌ Not started | 0% |
| Docker Images | ❌ Not started | 0% |

---

## 3. Database Schema — All 42+ Tables

The complete schema is created by two migrations:
1. `04aff4ce3106_initial_schema_with_42_tables.py` → executes `schemas/schema_postgres.sql`
2. `2512af1df83a_add_alerting_tables.py` → adds 8 alerting tables

### Group 1: Core Connector Catalog

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `connector_definitions` | connector_id, connector_name, connector_type, category, supports_cdc, supports_full_refresh, capabilities JSONB, default_config JSONB, required_fields JSONB | Catalog of connector types (MySQL, PG, MongoDB, Snowflake, S3, etc.) |
| `connector_versions` | version_id, connector_id, version, changelog, is_stable | Version history for each connector type |

**Seed data exists** in `schemas/seed_data.sql` — MySQL, PostgreSQL, MongoDB, Snowflake, S3, Azure Blob, BigQuery connectors pre-populated.

### Group 2: Source & Destination Configuration

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `sources` | source_id, bank_id, sub_tenant_id, connector_definition_id, host, port, database_name, username, password_encrypted, ssl_config JSONB, discovery_cache JSONB, status | Source DB connection config per tenant |
| `destinations` | destination_id, bank_id, sub_tenant_id, connector_definition_id, connection_info JSONB, load_config JSONB | Destination connection config per tenant |

### Group 3: Connections & Streams

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `connections` | connection_id, source_id, destination_id, sync_mode, sync_type, schedule_cron, transform_pipeline_id, dq_policy_id, schema_evolution_policy, status, initial_load_completed | Central connection config linking source→destination |
| `streams` | stream_id, connection_id, schema_name, table_name, enabled, pk_columns JSONB, cursor_column, sync_mode_override, transform_spec JSONB | Per-table/collection stream config |
| `sync_mode_config` | config_id, connection_id, sync_mode, changed_by, changed_at, reason | History of sync mode changes |

### Group 4: CDC State & Checkpointing

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `checkpoint_state` | checkpoint_id, source_id, stream_id, worker_id, binlog_file, binlog_position, gtid_set, lsn, resume_token, checkpoint_data JSONB, records_processed, bytes_processed | Current CDC position per worker/source/stream |
| `cdc_position_history` | history_id, source_id, worker_id, position_data JSONB, lag_seconds, events_processed | Historical CDC position snapshots |
| `cdc_lag_metrics` | metric_id, source_id, stream_id, lag_seconds, lag_events, measured_at | Time-series lag metrics |
| `worker_heartbeats` | heartbeat_id, worker_id, host, source_type, assigned_sources JSONB, current_load, capacity, last_seen | Worker pod health tracking |
| `redis_stream_tracking` | tracking_id, stream_key, consumer_group, last_delivered_id, pending_count | Redis stream metadata |

### Group 5: Connection Execution

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `connection_runs` | run_id, connection_id, run_type, status, started_at, completed_at, rows_read, rows_written, error_message, run_metadata JSONB | Every sync/CDC run history |
| `connection_health_checks` | check_id, connection_id, check_type, status, response_time_ms, error_detail, checked_at | Connection health check history |
| `connection_alert_webhooks` | webhook_id, connection_id, url, events JSONB, is_active | Per-connection webhook configs |

### Group 6: Transformations & UDFs

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `transform_pipelines` | pipeline_id, bank_id, sub_tenant_id, pipeline_name, spec_json JSONB, version, is_active | Transformation pipeline spec (cast/mask/expression/UDF/JSON flatten) |
| `transformation_dependencies` | dependency_id, pipeline_id, depends_on_pipeline_id | Pipeline dependency graph |
| `transformation_logs` | log_id, pipeline_id, connection_id, run_id, status, records_processed, errors JSONB, executed_at | Execution history per pipeline |
| `udf_catalog` | udf_id, bank_id, sub_tenant_id, function_name, language, return_type, argument_types JSONB, function_body, is_active | Registered user-defined functions |
| `udf_execution_stats` | stat_id, udf_id, invocation_count, avg_duration_ms, error_count, last_executed_at | UDF performance metrics |

### Group 7: Data Quality

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `dq_policies` | policy_id, bank_id, sub_tenant_id, connection_id, stream_id, rules_json JSONB, on_fail_action, is_active | DQ policy with rule definitions |
| `dq_rule_results` | result_id, policy_id, run_id, rule_type, status (PASS/WARN/FAIL), metric_value, threshold, evaluated_at | Per-rule execution results |
| `dq_violations` | violation_id, policy_id, run_id, rule_type, severity, description, affected_rows, resolved_at | Violation records |
| `dq_violation_samples` | sample_id, violation_id, record_data JSONB | Sample rows that triggered the violation |

### Group 8: Schema Evolution (JSON + Relational)

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `schema_change_events` | event_id, source_id, stream_id, table_name, change_type, old_schema JSONB, new_schema JSONB, schema_diff JSONB, status (pending/approved/rejected), is_breaking, applied_at | Schema change detection and approval workflow |
| `json_schema_cache` | cache_id, source_id, table_name, column_name, json_schema JSONB, sample_count | Cached inferred JSON schemas from sampling |
| `json_schema_evolution` | evolution_id, cache_id, old_schema JSONB, new_schema JSONB, detected_at | JSON column schema change history |
| `json_flatten_rules` | rule_id, stream_id, column_name, flatten_mode (inline/child_table), target_table, field_mappings JSONB | JSON flatten configuration per column |

### Group 9: Reliability & Dead-Letter Queue

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `event_dead_letter_queue` | dlq_id, source_id, stream_id, event_data JSONB, error_type, error_message, retry_count, status | Failed events awaiting retry |
| `event_dlq_retry_history` | retry_id, dlq_id, attempted_at, result, error_detail | DLQ retry attempt history |

### Group 10: Spark Job Management

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `spark_job_queue` | job_id, job_name, job_type, pipeline_id, connection_id, priority, spark_config JSONB, status, retry_count | Pending Spark jobs |
| `spark_applications` | app_id, connection_id, app_name, app_type (streaming/batch), spark_app_id, master_url, status, started_at | Running/historical Spark apps |
| `spark_executors` | executor_id, app_id, executor_num, host, cores, memory_mb, status | Spark executor metadata |
| `spark_executor_history` | history_id, executor_id, event_type, event_data JSONB, recorded_at | Executor lifecycle events |

### Group 11: Multi-Tenancy & Usage Tracking

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `tenant_daily_usage` | usage_id, bank_id, sub_tenant_id, date, events_processed, bytes_processed, compute_seconds, api_calls | Daily usage per tenant |
| `resource_usage` | resource_id, bank_id, sub_tenant_id, resource_type, quantity, unit, recorded_at | Real-time resource consumption |
| `resource_quota_violations` | violation_id, bank_id, sub_tenant_id, resource_type, quota_limit, actual_usage, violated_at | Quota breach events |

### Group 12: Auth & RBAC

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `users` | user_id, username, email, password_hash, bank_id, sub_tenant_id, is_active, is_superuser, failed_login_attempts, locked_until, preferences JSONB | User accounts |
| `roles` | role_id, role_name, display_name, description, is_active | Role definitions |
| `permissions` | permission_id, permission_name, resource_type, action | Granular permission atoms |
| `user_roles` | user_id, role_id | Many-to-many mapping |
| `role_permissions` | role_id, permission_id | Many-to-many mapping |
| `refresh_tokens` | token_id, user_id, token_hash, expires_at, is_revoked | JWT refresh token store |

### Group 13: System & Operations

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `system_config` | config_key (PK), config_value, value_type, is_sensitive | Key-value system config |
| `feature_flags` | flag_name (PK), is_enabled, rollout_percentage, target_tenants JSONB | Feature toggle management |
| `maintenance_windows` | window_id, start_at, end_at, affected_services JSONB, created_by | Planned maintenance tracking |
| `audit_log` | log_id, bank_id, sub_tenant_id, user_id, action, resource_type, resource_id, old_value JSONB, new_value JSONB, ip_address, recorded_at | Immutable audit trail (partitioned by month) |
| `alerts` | alert_id, bank_id, sub_tenant_id, alert_type, severity, title, message, source_ref, status, acknowledged_by, resolved_at | System alert records |

### Group 14: Alerting (Migration 2)

| Table | Columns (key) | Purpose |
|-------|--------------|---------|
| `notification_channels` | channel_id, bank_id, sub_tenant_id, channel_name, channel_type (email/slack/webhook/pagerduty/msteams/sms), config JSONB, is_verified, rate_limit | Notification delivery channels |
| `alert_rules` | rule_id, bank_id, sub_tenant_id, rule_name, alert_type (12 types), severity, scope_type, condition_type, condition_definition JSONB, evaluation_interval_minutes, auto_resolve, is_active | Alert trigger rules |
| `alert_rule_channels` | rule_id, channel_id, priority, min_severity, is_active | Many-to-many rule↔channel with priority |
| `alert_escalation_policies` | policy_id, rule_id, escalation_level, delay_minutes, message_template | Multi-level escalation |
| `alert_evaluations` | evaluation_id, rule_id, evaluated_at, result, metric_value, consecutive_failures, duration_ms | Rule evaluation history |
| `alert_history` | history_id, alert_id, status, changed_by, change_reason, changed_at | Alert lifecycle audit |
| `alert_notification_log` | log_id, alert_id, channel_id, status, delivered_at, retry_count | Notification delivery log |
| `alert_suppressions` | suppression_id, bank_id, sub_tenant_id, scope_type, rule_id, starts_at, ends_at, reason | Temporary alert muting |

### Views (3 SQL views)

| View | Purpose |
|------|---------|
| `v_tenant_resource_summary` | Aggregated resource usage per tenant |
| `v_cdc_lag_summary` | Current lag across all CDC streams |
| `v_active_connections` | All active connections with status |

---

## 4. Control Plane — Detailed Status

### 4.1 Authentication & Security — ✅ Fully Implemented

**Files:** `app/auth/jwt.py`, `app/auth/password.py`, `app/auth/rbac.py`, `app/auth/dependencies.py`, `app/middleware/auth.py`, `app/middleware/tenant_isolation.py`

What's working:
- JWT access + refresh token issuance and verification
- `python-jose[cryptography]` with HS256 algorithm
- `passlib[bcrypt]` password hashing
- Keycloak integration (`python-keycloak==3.9.1`) — optional external IdP
- Full RBAC: `get_user_permissions()`, `has_permission()`, `require_permission()` dependency
- Role hierarchy: `superadmin(100) > bank_admin(80) > tenant_admin(60) > user(40) > viewer(20)`
- `TenantIsolationMiddleware` — extracts `bank_id` and `sub_tenant_id` from JWT into `ContextVar` per request, ensuring all DB queries are tenant-scoped
- Account lockout on failed login attempts

**API Endpoints:** `POST /api/v1/auth/login`, `POST /api/v1/auth/refresh`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me`

### 4.2 Sources API — ✅ ~95% Implemented

**File:** `app/api/sources.py`

What's working:
- Full CRUD with tenant filtering (`sub_tenant_id` enforced on every query)
- `POST /sources` — create source, validates connector_definition, encrypts password (`encrypted_{password}` placeholder — needs real Fernet encryption)
- `GET /sources` — paginated list with filters (status, connector_type, search)
- `GET /sources/{id}` — full detail with connector definition joined
- `PATCH /sources/{id}` — partial update
- `DELETE /sources/{id}` — soft delete
- `POST /sources/{id}/test-connection` — **placeholder** (returns success after simulated 100ms delay — not real connection)
- `POST /sources/{id}/discover-schema` — **placeholder** (returns empty schemas — not real introspection)
- `GET /sources/{id}/cdc-config` — returns CDC configuration info
- `GET /sources/{id}/stats` — connection run statistics

**Gap:** `_test_database_connection()` and `_discover_database_schemas()` are placeholder stubs. Real implementation needs `pymysql`, `psycopg2`, `pymongo` connections.

### 4.3 Destinations API — ✅ ~90% Implemented

**File:** `app/api/destinations.py`

Full CRUD with same pattern as Sources. `POST /destinations/{id}/test-connection` is also a placeholder.

### 4.4 Connections API — ✅ ~90% Implemented (17 endpoints)

**File:** `app/api/connections.py`

What's working (real implementation, not stubs):
- `POST /connections` — creates connection with validation, checks source/destination compatibility (CDC support via connector capabilities), prevents duplicates, creates associated streams
- `GET /connections` — paginated list with filters (status, sync_mode, source_id, destination_id)
- `GET /connections/{id}` — full detail with source + destination + streams joined
- `PATCH /connections/{id}` — partial update
- `DELETE /connections/{id}` — soft delete
- `POST /connections/validate` — dry-run compatibility check
- `POST /connections/{id}/activate` — changes status to active (actual worker start is TODO)
- `POST /connections/{id}/pause` and `POST /connections/{id}/resume`
- `POST /connections/{id}/trigger-sync` — queues manual sync (writes to `spark_job_queue` — not connected to Airflow yet)
- `GET /connections/{id}/stats` — reads from `connection_runs` table
- Stream sub-resource: `GET/POST/PATCH/DELETE /connections/{id}/streams`

**Gap:** `POST /activate` changes DB status but doesn't actually start a CDC worker or Airflow DAG. This requires Phase 2.

### 4.5 Connector Definitions API — ✅ ~90% Implemented

**File:** `app/api/connector_definitions.py`

CRUD for the connector catalog. Pre-seeded with MySQL, PostgreSQL, MongoDB, Snowflake, S3, Azure Blob connectors. Read-only for non-superadmin users.

### 4.6 Data Quality API — ✅ ~85% Implemented (18 endpoints)

**File:** `app/api/data_quality.py`

Real implementation with DB queries:
- DQ Policy CRUD (5 endpoints) — fully wired to `dq_policies` table
- Rule execution endpoint — `POST /data-quality/policies/{id}/execute` — writes to `dq_rule_results` table but actual rule evaluation logic is a placeholder
- Violations management — reads/updates `dq_violations` and `dq_violation_samples`
- Quality metrics — `GET /data-quality/connections/{id}/quality-metrics` — aggregates from `dq_rule_results`
- Quality dashboard — aggregated view
- Data profiling — `POST /data-quality/profile` — placeholder (needs real Spark execution)
- Rule templates — CRUD for reusable templates
- Anomaly detection config — CRUD

**Gap:** Actual rule evaluation (null ratio computation, range check, freshness check) is not implemented. This runs in Spark — Phase 3.

### 4.7 Alerting API — ✅ ~85% Implemented

**File:** `app/api/alerting.py`, `app/models/alerting.py`

Real implementation:
- Notification channels CRUD (6 endpoints) — email, Slack, webhook, PagerDuty, MS Teams, SMS
- Alert rules CRUD (7 endpoints) — 12 alert types, 4 condition types
- Alert rules ↔ channel association
- Escalation policies CRUD
- Alert lifecycle (list, acknowledge, resolve, bulk operations)
- Alert suppression windows
- `POST /alerts/channels/{id}/test` — placeholder (doesn't actually send notification)

**Gap:** Alert evaluation engine (the background process that queries `cdc_lag_metrics` and evaluates `alert_rules`) is not built. Actual notification dispatch (Slack HTTP client, Teams webhook, email SMTP) not implemented.

### 4.8 Schema Evolution API — ⚠️ Stub Only (10%)

**File:** `app/api/schema_evolution.py`

```python
@router.get("/connections/{connection_id}/schema-changes")
async def list_schema_changes(...):
    # TODO: Implement
    return {"schema_changes": []}
```

Three routes exist as empty TODO stubs. The `SchemaChangeEvent` model is fully built in `app/models/schema_evolution.py` with all fields (old_schema JSONB, new_schema JSONB, schema_diff JSONB, status, is_breaking, impact_assessment). The DB table exists and is ready. **The API logic just needs to be written.**

### 4.9 Transformations API — ⚠️ Stub Only (10%)

**File:** `app/api/transformations.py`

All 6 routes exist as TODO stubs returning hardcoded responses. The `TransformPipeline` model and `transform_pipelines` DB table are fully defined. The JSON spec structure is designed. **The CRUD logic just needs to be written** (same pattern as Sources/DQ APIs).

### 4.10 UDFs API — ⚠️ Stub Only (10%)

**File:** `app/api/udfs.py`

4 routes as TODO stubs. `UDFCatalog` model is complete.

### 4.11 Monitoring API — ⚠️ Stub Only (10%)

**File:** `app/api/monitoring.py`

All 8 routes are TODO stubs. `CDCLagMetrics`, `WorkerHeartbeat`, `ConnectionRun`, `CheckpointState` models are fully built and tables exist. The routes just need to query these tables.

```python
# All look like this:
@router.get("/connections/{connection_id}/lag")
async def connection_lag(connection_id: str, db: Session = Depends(get_db)):
    # TODO: Implement
    return {"lag_seconds": 0, "lag_events": 0}
```

### 4.12 Missing: Real Encryption for Credentials

**File:** `app/api/sources.py`

```python
def _encrypt_password(password: str) -> str:
    # TODO: Implement proper encryption using Fernet or similar
    return f"encrypted_{password}"  # ← this is NOT real encryption
```

Source passwords are stored as `encrypted_<plaintext>` in the DB. Before production, replace with `cryptography.fernet.Fernet` (the dependency is already in requirements.txt).

---

## 5. Data Plane — Gap (Phase 2)

**Directory:** `cdc-workers/` — **completely empty**

This is the largest gap. Nothing exists in `cdc-workers/cdc_worker/` or `cdc-workers/connectors/`. Every file needs to be created from scratch.

### 5.1 What Needs to Be Built

#### Base CDC Worker Framework

```
cdc-workers/
  cdc_worker/
    __init__.py
    worker.py            ← Main worker process (asyncio event loop)
    config.py            ← Worker config (reads from control plane API)
    checkpoint.py        ← SQLite local + Postgres central dual checkpoint
    fallback_queue.py    ← SQLite/file disk queue when Redis is down
    event_envelope.py    ← Canonical event envelope builder + event_id SHA-256
    redis_publisher.py   ← XADD to Redis streams + XTRIM
    routing.py           ← (schema, table) → tenant stream key lookup
    heartbeat.py         ← Worker heartbeat to control plane /api/v1/workers/heartbeat
  connectors/
    base.py              ← Abstract base connector class
    mysql.py             ← BinLogStreamReader (python-mysql-replication)
    postgres.py          ← psycopg2 LogicalReplicationConnection + wal2json
    mongodb.py           ← pymongo collection.watch() + resume tokens
    polling.py           ← Cursor-based polling fallback
```

#### Checkpoint Manager (per spec)

Local SQLite schema:
```sql
CREATE TABLE checkpoints (
    source_id   TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    lsn         TEXT NOT NULL,          -- binlog pos / WAL LSN / mongo resume token
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, schema_name, table_name)
);
```

Central Postgres sync — upsert to `checkpoint_state` table (already exists in DB).

#### MySQL Connector

- Library: `python-mysql-replication` (add to requirements)
- Needs: MySQL `REPLICATION SLAVE` + `SELECT` user, `binlog_format=ROW`
- Handles: `WriteRowsEvent` → `op='c'`, `UpdateRowsEvent` → `op='u'`, `DeleteRowsEvent` → `op='d'`, `QueryEvent` → DDL notification
- `event_id` = `SHA-256(table + pk_values + log_file + log_pos)`

#### PostgreSQL Connector

- Library: `psycopg2[extras]` (already in requirements)
- Needs: `wal_level=logical`, replication slot with `wal2json` plugin
- **Critical:** `send_feedback(flush_lsn=msg.data_start)` every 10 seconds to prevent WAL accumulation
- Handles: `kind: insert/update/delete` from wal2json JSON payload

#### MongoDB Connector

- Library: `pymongo` (add to requirements)
- Needs: Replica set (not standalone)
- Handles: `insert/update/replace/delete` operations
- `full_document='updateLookup'` for before+after images
- Resume token stored in `checkpoint_state.resume_token`
- Error: `ChangeStreamHistoryLost` → alert + trigger resync

#### Redis Stream Publisher

```python
# Stream key pattern
def stream_key(bank_id, tenant_id, source_id, schema, table):
    return f"cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}"

# Publish
redis_client.xadd(
    key, 
    fields=envelope_dict,
    maxlen=100_000,        # XTRIM ~ 100k events per stream
    approximate=True
)
```

#### Event Envelope Builder

```python
import hashlib, json

def build_event_id(pk_values: dict, lsn: str) -> str:
    payload = json.dumps({**pk_values, "lsn": lsn}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()

def build_envelope(op, before, after, schema, table, lsn, 
                   ts_ms, tenant_id, bank_id, source_id, pk_values, metadata):
    return {
        "tenant_id": tenant_id,
        "bank_id": bank_id,
        "source_id": source_id,
        "schema": schema,
        "table": table,
        "op": op,            # c / u / d
        "before": before,    # None for inserts
        "after": after,      # None for deletes
        "ts_ms": ts_ms,
        "lsn": lsn,
        "event_id": build_event_id(pk_values, lsn),
        "metadata": metadata,
    }
```

#### New Requirements Needed for Phase 2

```txt
# Add to cdc-workers/requirements.txt
python-mysql-replication==0.44
pymongo[srv]==4.6.0
redis==5.0.1
aioredis==2.0.1
aiosqlite==0.19.0
```

---

## 6. Spark Consumer — Gap (Phase 3)

**Directory:** `spark-consumer/` — **completely empty**

### 6.1 What Needs to Be Built

```
spark-consumer/
  consumer/
    __init__.py
    streaming_consumer.py     ← Spark Structured Streaming main (REALTIME)
    batch_consumer.py         ← Spark batch job main (SCHEDULED via Airflow)
    redis_source.py           ← Custom Redis stream source for Spark
    transform_executor.py     ← Execute TransformPipeline spec from control plane
    dq_executor.py            ← Execute DQPolicy rules on Spark DataFrame
    checkpoint_handler.py     ← Spark checkpoint (S3/HDFS for exactly-once)
  writers/
    postgres_writer.py        ← INSERT ... ON CONFLICT DO UPDATE
    iceberg_writer.py         ← MERGE INTO via Nessie / Glue catalog
    dlq_writer.py             ← Write failed records to event_dead_letter_queue
  udf/
    registry.py               ← Load and register UDFs from udf_catalog table
    builtins.py               ← Built-in UDFs (risk_score, address_normalize, etc.)
  jobs/
    full_load_job.py          ← Initial snapshot Spark job
    incremental_job.py        ← Incremental batch job
```

#### Streaming Consumer (Realtime)

```python
# spark-consumer/consumer/streaming_consumer.py
df = (spark.readStream
      .format("redis-streams")           # custom source or existing library
      .option("stream.keys", stream_key)
      .option("consumer.group", "spark-cdc")
      .load())

transformed_df = TransformExecutor(pipeline_spec).apply(df)
dq_passed_df, dq_failed_df = DQExecutor(dq_policy).check(transformed_df)

(dq_passed_df.writeStream
 .foreachBatch(postgres_writer.upsert_batch)
 .option("checkpointLocation", f"s3://fusion-checkpoints/{connection_id}")
 .trigger(processingTime="5 seconds")
 .start())
```

#### Transform Pipeline Executor

Must implement all 10 transform step types:
- `cast` — `df.withColumn(col, df[col].cast(to_type))`
- `string_op` — `upper()`, `lower()`, `trim()`, `substring()`
- `math_op` — `df.withColumn(col, expr("amount * fx_rate"))`
- `date_op` — `date_add()`, `year()`, `month()`
- `json_extract` — `get_json_object()`
- `json_flatten_inline` — `from_json()` + `select(*)`
- `json_flatten_child` — `explode()` + separate write
- `mask` — `regexp_replace()` for last4, `sha2()` for hash
- `expression` — `df.selectExpr(spark_sql_expression)`
- `udf` — `spark.udf.register()` + `df.withColumn()`

#### Postgres Writer (Upsert)

```python
def upsert_batch(batch_df, batch_id):
    # Stage → swap pattern (or direct ON CONFLICT)
    batch_df.write.format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", f"staging_{table}") \
        .mode("overwrite").save()
    
    conn.execute(f"""
        INSERT INTO {schema}.{table}
        SELECT * FROM staging_{table}
        ON CONFLICT ({', '.join(pk_columns)})
        DO UPDATE SET {update_set_clause}
        WHERE {table}.updated_at < EXCLUDED.updated_at
    """)
```

#### Iceberg Writer (MERGE INTO)

```python
batch_df.createOrReplaceTempView("cdc_updates")
spark.sql(f"""
    MERGE INTO {catalog}.{namespace}.{table} AS t
    USING cdc_updates AS s
    ON t.{pk} = s.{pk}
    WHEN MATCHED AND s.op IN ('u') THEN UPDATE SET *
    WHEN MATCHED AND s.op = 'd' THEN DELETE
    WHEN NOT MATCHED AND s.op = 'c' THEN INSERT *
""")
```

---

## 7. Infrastructure & DevOps — Gap (Phase 5)

### What Needs to Be Built

```
kubernetes/
  control-plane/
    deployment.yaml         ← FastAPI control plane deployment
    service.yaml
    ingress.yaml            ← HTTPS via nginx/traefik
    hpa.yaml
  cdc-workers/
    mysql-worker.yaml       ← StatefulSet with PVC for SQLite checkpoint
    postgres-worker.yaml
    mongodb-worker.yaml
    hpa.yaml                ← Scale on CPU + assigned_streams metric
  redis/
    redis-cluster.yaml      ← 3 masters + 3 replicas
    redis-exporter.yaml
  spark/
    spark-operator.yaml
    streaming-job.yaml
  monitoring/
    prometheus.yaml
    grafana.yaml
    alertmanager.yaml
    fusion-dashboard.json

docker/
  Dockerfile.control-plane
  Dockerfile.cdc-worker
  Dockerfile.spark-consumer
  docker-compose.yml        ← Local dev environment
```

---

## 8. What Was Last Completed (Stop Point)

Based on TODO completion reports and timestamps:

### Chronological Completion Order

| TODO | Date | What Was Done |
|------|------|---------------|
| TODO #3 | Nov 30, 2025 | Database setup — 42 tables verified, 15 passing tests |
| TODO #4 | ~Dec 1, 2025 | All 42 SQLAlchemy ORM models created (10 model files) |
| TODO #9 | ~Dec 5, 2025 | Connection Management API — 17 endpoints, 23 schemas, full CRUD |
| TODO #10 | ~Dec 6, 2025 | Data Quality API — 18 endpoints, 32 schemas, 100% test coverage |
| TODO #11 | Dec 8, 2025 | Alerting system — 11 models, 33 schemas, multi-channel notifications |

### **Last Stop Point: TODO #11 (December 8, 2025)**

The last confirmed completed work was the **Alert Management system** (TODO #11). After that, the stub APIs (Transformations, Schema Evolution, UDFs, Monitoring) remain as TODO stubs, and the entire data plane (Phase 2) and Spark consumer (Phase 3) were never started.

### What Is Half-Done

These APIs have **route stubs registered** in `main.py` but contain only `# TODO: Implement` comments:

| API File | Routes Present | Logic Implemented |
|----------|---------------|-------------------|
| `api/schema_evolution.py` | 3 routes | ❌ None — all return static responses |
| `api/transformations.py` | 6 routes | ❌ None — all return static responses |
| `api/udfs.py` | 4 routes | ❌ None — all return static responses |
| `api/monitoring.py` | 8 routes | ❌ None — all return static responses |

---

## 9. Where to Resume — Concrete Next Steps

### Immediate Priority Order

#### Step 1 — Complete the Stub APIs (2–3 days, low risk)
These require no new infrastructure, just writing CRUD logic following the existing pattern from `sources.py` and `data_quality.py`.

**Transformations API** (`api/transformations.py`):
```python
# Follow exact same pattern as sources.py
@router.post("", status_code=201, response_model=TransformPipelineResponse)
async def create_transformation(
    data: TransformPipelineCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("transformations:create"))
):
    pipeline = TransformPipeline(
        pipeline_name=data.pipeline_name,
        spec_json=data.spec_json,
        bank_id=user.bank_id,
        sub_tenant_id=user.sub_tenant_id,
        version=1,
    )
    db.add(pipeline); db.commit(); db.refresh(pipeline)
    return pipeline
```

**Monitoring API** (`api/monitoring.py`):
```python
@router.get("/connections/{connection_id}/lag")
async def connection_lag(connection_id: UUID, db: Session = Depends(get_db)):
    metric = db.query(CDCLagMetrics).filter(
        CDCLagMetrics.source_id == connection_id
    ).order_by(CDCLagMetrics.measured_at.desc()).first()
    return {"lag_seconds": metric.lag_seconds if metric else 0}
```

**Schema Evolution API** (`api/schema_evolution.py`):
```python
@router.get("/connections/{connection_id}/schema-changes")
async def list_schema_changes(connection_id: UUID, db: Session = Depends(get_db)):
    events = db.query(SchemaChangeEvent).filter(
        SchemaChangeEvent.source_id == connection_id
    ).order_by(SchemaChangeEvent.detected_at.desc()).all()
    return {"schema_changes": events}
```

#### Step 2 — Fix Credential Encryption (0.5 day, security critical)

**File:** `app/api/sources.py` lines ~90–100

Replace placeholder encryption:
```python
from cryptography.fernet import Fernet

# In settings: ENCRYPTION_KEY already defined
fernet = Fernet(settings.ENCRYPTION_KEY.encode())

def _encrypt_password(password: str) -> str:
    return fernet.encrypt(password.encode()).decode()

def _decrypt_password(encrypted: str) -> str:
    return fernet.decrypt(encrypted.encode()).decode()
```

#### Step 3 — Build the CDC Worker Base Framework (Phase 2 start, ~1 week)

Create `cdc-workers/cdc_worker/` package:

1. `event_envelope.py` — canonical envelope + SHA-256 event_id (1 day)
2. `checkpoint.py` — SQLite local + Postgres API sync (1 day)
3. `redis_publisher.py` — XADD with XTRIM (0.5 day)
4. `worker.py` — asyncio event loop + semaphore concurrency (1 day)
5. `heartbeat.py` — periodic heartbeat to control plane (0.5 day)

#### Step 4 — Implement MySQL Connector (~3 days)

```bash
pip install python-mysql-replication
```

Key implementation points:
- `BinLogStreamReader` with `server_id`, `only_events`, `only_schemas`, `only_tables` filters
- Handle `WriteRowsEvent`, `UpdateRowsEvent`, `DeleteRowsEvent`
- `ALTER TABLE` via `QueryEvent` → send to control plane schema evolution API
- Checkpoint: store `(log_file, log_pos)` in SQLite

#### Step 5 — Implement PostgreSQL Connector (~3 days)

Key implementation points:
- `psycopg2.extras.LogicalReplicationConnection`
- Create replication slot: `pg_create_logical_replication_slot('fusion_{source_id}', 'wal2json')`
- `cursor.start_replication(slot_name=..., start_lsn=last_lsn)`
- **Must call** `msg.cursor.send_feedback(flush_lsn=msg.data_start)` every 10 seconds
- Checkpoint: store LSN string in SQLite

#### Step 6 — Implement MongoDB Connector (~2 days)

Key implementation points:
- `collection.watch(pipeline, resume_after=token, full_document='updateLookup')`
- Map `insert→c`, `update/replace→u`, `delete→d`
- Checkpoint: store BSON resume token as string
- Handle `ChangeStreamHistoryLost` → call control plane to trigger resync

#### Step 7 — Spark Consumer (Phase 3, ~2 weeks)

After connectors are writing to Redis, build:
1. Redis → Spark source (custom or library)
2. Transform pipeline executor (10 step types)
3. Postgres DW writer (UPSERT pattern)
4. Iceberg writer (MERGE INTO pattern)
5. DQ rules execution in Spark

---

## 10. Full Development Roadmap

```
COMPLETED (Dec 2025)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 ✅ PostgreSQL schema (42 tables + migrations)
 ✅ SQLAlchemy ORM models (41 classes, 10 files)
 ✅ JWT Auth + RBAC + Keycloak integration
 ✅ Tenant isolation middleware
 ✅ Sources API (CRUD + test + schema discover stub)
 ✅ Destinations API (CRUD)
 ✅ Connections API (17 endpoints, full lifecycle)
 ✅ Streams API (sub-resource of connections)
 ✅ Connector Definitions API (read-only catalog)
 ✅ Data Quality API (18 endpoints, violations, metrics)
 ✅ Alerting API (notification channels, rules, escalation)
 ✅ DB setup + verification tests (15 tests passing)

PENDING (Resume from here)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WEEK 1 — Complete Control Plane (finish 78% → 100%)
 ⬜ Transformations API — real CRUD (1 day)
 ⬜ UDFs API — real CRUD (0.5 day)
 ⬜ Schema Evolution API — real CRUD + approval workflow (1 day)
 ⬜ Monitoring API — wire to CDCLagMetrics, WorkerHeartbeat tables (1 day)
 ⬜ Fix credential encryption with Fernet (0.5 day)
 ⬜ Real source connection test (pymysql/psycopg2/pymongo) (1 day)
 ⬜ Real schema discovery from source DBs (1 day)

WEEK 2-3 — CDC Worker Base + MySQL Connector (Phase 2 start)
 ⬜ cdc-workers/ package skeleton
 ⬜ Event envelope + event_id (SHA-256)
 ⬜ Local SQLite checkpoint manager
 ⬜ Central Postgres checkpoint sync
 ⬜ Redis stream publisher (XADD + XTRIM)
 ⬜ Fallback disk queue
 ⬜ Asyncio worker with semaphore concurrency
 ⬜ Worker heartbeat to control plane
 ⬜ MySQL connector (BinLogStreamReader)

WEEK 4 — PostgreSQL + MongoDB Connectors
 ⬜ PostgreSQL connector (WAL + wal2json)
 ⬜ MongoDB connector (change streams + resume tokens)
 ⬜ Polling connector (cursor-based fallback)
 ⬜ Worker orchestration (control plane assigns sources to workers)

WEEK 5-7 — Spark Consumer (Phase 3)
 ⬜ Redis stream source for Spark
 ⬜ Transform pipeline executor (10 step types)
 ⬜ DQ rules execution in Spark
 ⬜ Postgres DW upsert writer
 ⬜ Iceberg MERGE INTO writer
 ⬜ Full load snapshot Spark job
 ⬜ Incremental batch job (Airflow integration)

WEEK 8-10 — Integration Testing (Phase 4)
 ⬜ End-to-end test: MySQL→CDC worker→Redis→Spark→Postgres DW
 ⬜ End-to-end test: MongoDB→CDC worker→Redis→Spark→Iceberg
 ⬜ Multi-tenant isolation verification
 ⬜ Prometheus metrics from all components
 ⬜ Grafana dashboards (3 per spec: lag, throughput, DQ)
 ⬜ Load testing (throughput, latency, failover)

WEEK 11-12 — Production / K8s (Phase 5)
 ⬜ Dockerfile for control-plane
 ⬜ Dockerfile for cdc-worker (per source type)
 ⬜ Dockerfile for spark-consumer
 ⬜ Kubernetes StatefulSet for CDC workers (PVC for SQLite)
 ⬜ Kubernetes Deployment for control plane
 ⬜ HPA for workers (CPU + custom stream metric)
 ⬜ Redis Cluster config (3 masters + 3 replicas)
 ⬜ NetworkPolicy (workers only talk to their DB + Redis + control plane)
 ⬜ Secrets management (External Secrets Operator or Sealed Secrets)
 ⬜ Runbooks
```

---

## 11. File-by-File Status Inventory

### Control Plane (`control-plane/app/`)

| File | Status | Notes |
|------|--------|-------|
| `config.py` | ✅ Complete | All settings via pydantic-settings, reads `.env` |
| `database.py` | ✅ Complete | SQLAlchemy session factory |
| `main.py` | ✅ Complete | FastAPI app with all routers registered, CORS, middleware |
| `models/base.py` | ✅ Complete | TimestampMixin, SoftDeleteMixin, MultiTenancyMixin, BaseModel |
| `models/auth.py` | ✅ Complete | User, Role, Permission, RefreshToken, user_roles, role_permissions |
| `models/connector.py` | ✅ Complete | ConnectorDefinition, ConnectorVersion |
| `models/source_destination.py` | ✅ Complete | Source, Destination (with all CDC config fields) |
| `models/connection.py` | ✅ Complete | Connection, Stream, SyncModeConfig |
| `models/monitoring.py` | ✅ Complete | CheckpointState (binlog_file, binlog_position, lsn, resume_token), CDCPositionHistory, CDCLagMetrics, WorkerHeartbeat, RedisStreamTracking, ConnectionRun, ConnectionHealthCheck, ConnectionAlertWebhook |
| `models/transformation.py` | ✅ Complete | TransformPipeline, TransformationDependency, TransformationLog, UDFCatalog, UDFExecutionStats |
| `models/data_quality.py` | ✅ Complete | DQPolicy, DQViolation, DQViolationSample, DQRuleResult |
| `models/schema_evolution.py` | ✅ Complete | SchemaChangeEvent, JSONSchemaCache, JSONSchemaEvolution, JSONFlattenRule |
| `models/system.py` | ✅ Complete | SystemConfig, FeatureFlag, MaintenanceWindow, AuditLog, Alert |
| `models/spark.py` | ✅ Complete | SparkJobQueue, SparkApplication, SparkExecutor, SparkExecutorHistory |
| `models/alerting.py` | ✅ Complete | NotificationChannel, AlertRule, AlertRuleChannel, AlertEscalationPolicy, AlertEvaluation, AlertHistory, AlertNotificationLog, AlertSuppression |
| `auth/jwt.py` | ✅ Complete | Token issuance, verification, refresh |
| `auth/password.py` | ✅ Complete | bcrypt hash/verify |
| `auth/rbac.py` | ✅ Complete | Permission checking, role hierarchy |
| `auth/dependencies.py` | ✅ Complete | FastAPI dependencies: get_current_user, require_permission |
| `middleware/auth.py` | ✅ Complete | Auth middleware |
| `middleware/tenant_isolation.py` | ✅ Complete | ContextVar-based tenant isolation per request |
| `api/auth.py` | ✅ Complete | Login, refresh, logout, me endpoints |
| `api/sources.py` | ✅ 95% | CRUD complete; connection test + schema discovery are stubs |
| `api/destinations.py` | ✅ 90% | CRUD complete; connection test is stub |
| `api/connections.py` | ✅ 90% | 17 endpoints, full lifecycle; activate doesn't start workers |
| `api/streams.py` | ✅ 85% | CRUD as sub-resource of connections |
| `api/connector_definitions.py` | ✅ 90% | Read-only catalog CRUD |
| `api/data_quality.py` | ✅ 85% | 18 endpoints; rule execution is stub (no Spark) |
| `api/alerting.py` | ✅ 85% | Full CRUD; notification dispatch + evaluation loop are stubs |
| `api/dq_policies.py` | ✅ Present | (Duplicate/older DQ routes — verify vs data_quality.py) |
| `api/schema_evolution.py` | ⚠️ 10% | 3 route stubs, no logic |
| `api/transformations.py` | ⚠️ 10% | 6 route stubs, no logic |
| `api/udfs.py` | ⚠️ 10% | 4 route stubs, no logic |
| `api/monitoring.py` | ⚠️ 10% | 8 route stubs, no logic |
| `services/` | ❌ Empty | No service layer — logic is inline in API handlers |
| `schemas/` | ✅ Complete | Pydantic v2 schemas for all implemented APIs |

### CDC Workers (`cdc-workers/`)

| File | Status |
|------|--------|
| `cdc_worker/` | ❌ Empty |
| `connectors/` | ❌ Empty |

### Spark Consumer (`spark-consumer/`)

| File | Status |
|------|--------|
| Entire directory | ❌ Empty |

### Database (`schemas/`, `migrations/`)

| File | Status |
|------|--------|
| `schemas/schema_postgres.sql` | ✅ Complete — 42 tables + indexes + views + triggers |
| `schemas/seed_data.sql` | ✅ Complete — connector definitions pre-populated |
| `schemas/postgres_jsonb_gin_indexes.sql` | ✅ Complete — GIN indexes for JSONB columns |
| `schemas/schema_mysql_part*.sql` | ✅ Present — MySQL-compatible schema (alternative) |
| `migrations/versions/04aff4ce3106_*.py` | ✅ Complete — initial 42-table migration |
| `migrations/versions/2512af1df83a_*.py` | ✅ Complete — 8 alerting tables migration |
| `flyway/sql/V1__initial_schema.sql` | ✅ Present — Flyway alternative |

---

*Generated by deep analysis of all source files in fusion-cdc-engine. Last confirmed completion: TODO #11 (Alerting System, December 8, 2025).*

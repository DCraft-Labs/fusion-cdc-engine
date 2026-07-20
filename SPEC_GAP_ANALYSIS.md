# Fusion CDC Engine — Full Spec vs Implementation Gap Analysis
**Date:** 1 May 2026  
**Method:** Fresh extraction of all 5 PDFs, line-by-line cross-check against every source file

---

## How to Read This Document

Each section maps to one PDF. Every spec requirement is listed with a status:

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully implemented |
| ⚠️ | Partially implemented / stub with TODO |
| ❌ | Not implemented at all |

Gaps are collected at the end per PDF, then consolidated into a **Master Gap Table**.

---

## PDF 1 — Overview and Product Specification (`Artifact analysis options.pdf`)

### Tenancy Model
| Requirement | Status | Location |
|---|---|---|
| Parent → Tenant hierarchy (bank → sub-tenant) | ✅ | `models/auth.py`, `middleware/tenant_isolation.py` |
| Tenant admin sees only own sources/destinations | ✅ | `auth/rbac.py` + middleware |
| Parent admin can view/manage all tenants under parent | ✅ | `auth/rbac.py` `can_access_bank()` |
| Per-tenant CDC process isolation | ✅ | Redis stream key prefixed by bank+tenant |
| Per-tenant pause/resume operations | ✅ | `connections.py` `/pause` `/resume` endpoints |
| Central TenantConfig record with resource limits and tier assignments | ⚠️ | `Connection.resource_limits` JSONB field exists; no dedicated TenantConfig model with tier/quota enforcement |

### Source Structure
| Requirement | Status | Location |
|---|---|---|
| MySQL source (binlog, row-based mode) | ✅ | `connectors/mysql.py` |
| MySQL fallback polling when binlog disabled | ✅ | `connectors/polling.py` |
| PostgreSQL logical replication / WAL | ✅ | `connectors/postgres.py` |
| MongoDB change streams + resume tokens | ✅ | `connectors/mongodb.py` |
| Source stores connection details (host, port, db, user, cred reference, SSL) | ✅ | `models/source_destination.py` |
| CDC configuration (binlog format/GTID, slot/publication name) | ✅ | Source.config JSONB |
| Metadata discovery cache (schemas, tables, column types, JSON detection) | ✅ | `sources.py` `_discover_database_schemas()` |

### Destination Structure
| Requirement | Status | Location |
|---|---|---|
| Postgres Data Warehouse destination | ✅ | `writers/postgres_writer.py` |
| Iceberg Tables on object storage (S3/Azure/GCS) | ✅ | `writers/iceberg_writer.py` |
| Additional sinks via output factory | ⚠️ | DLQ writer exists; Elasticsearch/BigQuery/webhook destinations not implemented |

### Connection Model
| Requirement | Status | Location |
|---|---|---|
| `connection_id`, `source_id`, `destination_id` | ✅ | `models/connection.py` |
| `sync_mode`: FULL_REFRESH_OVERWRITE, FULL_REFRESH_APPEND, INCREMENTAL_APPEND, INCREMENTAL_APPEND_DEDUPED, CDC_INCREMENTAL_DEDUPED_HISTORY | ✅ | `models/connection.py` `sync_mode` column |
| `sync_type`: REALTIME or SCHEDULED | ✅ | `models/connection.py` `sync_type` column |
| `schedule`: cron expression or manual | ✅ | `models/connection.py` `sync_frequency` |
| `schema_evolution_policy`: AUTO_APPLY or MANUAL_APPROVAL | ✅ | `models/connection.py` |
| `transform_pipeline_id` | ✅ | `models/connection.py` |
| `dq_policy_id` | ✅ | `models/connection.py` |

### Streams (per-table config)
| Requirement | Status | Location |
|---|---|---|
| Enable/disable syncing per table | ✅ | `Stream.is_enabled` |
| Inspect and flatten JSON columns | ✅ | `schema_evolution.py` JSON flatten rules |
| Per-column transformations and type mappings | ✅ | `models/transformation.py` |
| Per-stream sync mode override | ✅ | `SyncModeConfig` model |
| Primary keys / cursor fields when CDC unavailable | ✅ | `Stream.primary_keys`, `Stream.cursor_field` |
| Dedicated streams CRUD API (list, update, enable, disable) | ❌ | `api/streams.py` is **entirely TODO stubs** — all 4 handlers return hardcoded mock data |

### UI Flow
| Requirement | Status | Location |
|---|---|---|
| Select Parent/Tenant step | ✅ | Auth + tenant context |
| Add Source with connection test + schema introspection | ✅ | `sources.py` `/test-connection`, `/discover-schema` |
| Add Destination with connectivity verification | ⚠️ | `destinations.py` `/test-connection` is a **TODO placeholder** (line 88-89) |
| Configure Connection: select tables, sync mode, schedule | ✅ | `connections.py` |
| Column transforms, JSON flattening, primary key, DQ rules, schema policy, destination overrides | ✅ | respective endpoints |
| Review and preview sample data before saving | ✅ | `transformations.py` `/{pipeline_id}/preview` |
| **Initial full load triggered when connection is saved/activated** | ❌ | `connections.py` line 640: `# TODO: Trigger initial sync if not skip_initial_sync` → `pass` |

### Architecture Components
| Requirement | Status | Location |
|---|---|---|
| UI & API Gateway (REST + GraphQL) | ⚠️ | REST API fully implemented; **GraphQL API completely absent** |
| Metadata Service (Postgres) | ✅ | `database.py` + models |
| Scheduler/Orchestrator (assigns workers, generates Airflow DAGs) | ⚠️ | Worker assignment via `assigned_worker_id` config field only; no active scheduler allocating workers to hosts at runtime |
| Metrics & Alert Manager | ✅ | Prometheus metrics + `alerting.py` |
| Tenant Registry | ⚠️ | Tenant data stored in DB; no in-memory catalogue or Kubernetes scale-up/scale-down triggered on tenant add/remove |
| DB Host Pod (per-host CDC worker) | ✅ | `cdc_worker/worker.py` + `connectors/` |
| Redis Streams with `cdc:<bank>:<tenant>:<source>:<schema>:<table>` key | ✅ | `redis_publisher.py` |
| Spark CDC Consumer (realtime + batch) | ✅ | `streaming_consumer.py` + `batch_consumer.py` |
| Checkpoint Manager (local SQLite + central Postgres) | ✅ | `checkpoint.py` |
| Fallback Queue | ✅ | `fallback_queue.py` |
| Prometheus Exporter (`/metrics` on each component) | ✅ | `metrics.py` (workers), `main.py` (control plane) |

---

## PDF 1 Gaps Summary
| # | Gap | Severity |
|---|-----|----------|
| P1-1 | `api/streams.py` — all 4 handlers are full stubs (TODO, return hardcoded data) | **High** |
| P1-2 | Initial full load never triggered on connection create/activate (`# TODO … pass`) | **High** |
| P1-3 | `trigger-sync` endpoint is also a TODO stub (line 744) | **High** |
| P1-4 | GraphQL API completely absent — spec says "REST/GraphQL APIs" | **Medium** |
| P1-5 | `dq_policies.py` — all 7 CRUD handlers are full stubs | **High** |
| P1-6 | Destination connection test is a TODO placeholder | **Medium** |
| P1-7 | No active Scheduler/Orchestrator dynamically assigning workers to DB hosts | **Medium** |
| P1-8 | No Tenant Registry or K8s scale-up when tenants added/removed | **Low** |

---

## PDF 2 — Detailed Architecture & Design (`Artifact analysis options (1).pdf`)

### Canonical Event Envelope
| Requirement | Status | Location |
|---|---|---|
| `tenant_id`, `bank_id`, `source_id`, `schema`, `table` | ✅ | `event_envelope.py` |
| `op`: c / u / d | ✅ | `event_envelope.py` |
| `before` / `after` row images | ✅ | `event_envelope.py` |
| `ts_ms` (epoch ms from source) | ✅ | `event_envelope.py` |
| `lsn` (binlog pos / WAL LSN / Mongo resume token) | ✅ | `event_envelope.py` |
| `event_id` = SHA-256(pk_values + lsn) | ✅ | `event_envelope.py` `compute_event_id()` |
| `metadata` dict (transaction id, user, host, row tx id) | ✅ | `event_envelope.py` |

### Connectors
| Requirement | Status | Location |
|---|---|---|
| MySQL: SHOW MASTER STATUS / SHOW BINLOG EVENTS for initial position | ✅ | `mysql.py` `_get_resume_position()` |
| MySQL: BinLogStreamReader per server ID, filtered by db/table | ✅ | `mysql.py` |
| MySQL: WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent with before+after | ✅ | `mysql.py` |
| MySQL: DDL events (ALTER, CREATE, DROP) → invalidate schema cache + notify control plane | ✅ | `mysql.py` `_notify_ddl_change()` |
| **MySQL: Heartbeat events (wait_event) to detect idle periods and keep stream alive** | ❌ | No `HeartbeatLogEvent` handler — `only_events` filter doesn't include it |
| PostgreSQL: Check/create replication slot with wal2json or pgoutput | ✅ | `postgres.py` |
| PostgreSQL: START_REPLICATION + WAL decoding | ✅ | `postgres.py` |
| PostgreSQL: Periodic standby_status_update (send_feedback) to advance confirmed_flush_lsn | ✅ | `postgres.py` (every ≤10s) |
| PostgreSQL: DDL detection → notify control plane | ✅ | `postgres.py` `_notify_schema_mismatch()` |
| MongoDB: change stream with resume_after token | ✅ | `mongodb.py` |
| MongoDB: insert/update/replace/delete handling | ✅ | `mongodb.py` |
| MongoDB: fullDocument enabled for full before/after | ✅ | `mongodb.py` `full_document="updateLookup"` |
| MongoDB: ChangeStreamHistoryLost → alert + resync | ✅ | `mongodb.py` + `/resync-request` endpoint |
| Polling connector (cursor-based SELECT diff) | ✅ | `connectors/polling.py` |
| **Polling connector: tombstone events for deleted rows** | ⚠️ | Polling emits op=c/u only (line 74: `op="c"` — cannot distinguish); no tombstone/delete events for rows deleted between polls |

### Event Routing & Redis
| Requirement | Status | Location |
|---|---|---|
| `cdc:<bank_id>:<tenant_id>:<source_id>:<schema>:<table>` key pattern | ✅ | `redis_publisher.py` |
| Stream MAXLEN trimming | ✅ | `redis_publisher.py` approximate MAXLEN |
| Fallback to local queue when Redis unavailable | ✅ | `redis_publisher.py` → `fallback_queue.py` |
| Per-tenant isolation via key prefix | ✅ | Key structure |
| Multi-tenant same DB → duplicate events to multiple tenant streams | ⚠️ | Basic routing implemented; multi-tenant duplication when same physical DB serves multiple tenants needs routing table entries populated per tenant |

### Checkpointing
| Requirement | Status | Location |
|---|---|---|
| Local SQLite checkpoints (source_id, schema_name, table_name, lsn) | ✅ | `checkpoint.py` `LocalCheckpointManager` |
| Atomic batch write (temp-table swap) | ✅ | `checkpoint.py` `set_batch_atomic()` |
| Central Postgres checkpoint sync via control plane API | ✅ | `checkpoint.py` `CentralCheckpointSync` |
| On restart: try local first, fall back to central | ✅ | `worker.py` startup logic |
| Persistent volume for SQLite (K8s config) | ✅ | K8s YAML in `kubernetes/` |

### Orchestration & Scheduling
| Requirement | Status | Location |
|---|---|---|
| Worker heartbeat to control plane | ✅ | `heartbeat.py` → `internal.py` `/workers/heartbeat` |
| Concurrency limit via asyncio.Semaphore | ✅ | `worker.py` `MAX_CONCURRENT_TABLES` |
| Worker assignment: scheduler checks existing workers per host | ⚠️ | Assignment stored in `source.config["assigned_worker_id"]`; no active scheduler logic to query load and reuse/create workers |
| **Leader election to avoid split-brain** | ❌ | Not implemented — no Redis-based or K8s leader election |
| Scale-down worker when all sources removed | ❌ | Not implemented — no K8s API calls to scale deployments |

### Kubernetes Design
| Requirement | Status | Location |
|---|---|---|
| Deployments/StatefulSets for workers | ✅ | `kubernetes/deployment.yaml` |
| HPA for workers | ✅ | `kubernetes/hpa.yaml` |
| Node affinity / tolerations | ✅ | K8s YAML |
| Persistent volumes for checkpoints | ✅ | K8s YAML + deployment.yaml |
| ConfigMaps & Secrets | ✅ | `kubernetes/configmap.yaml` + `secrets.yaml` |
| Liveness and readiness probes | ✅ | K8s YAML |

---

## PDF 2 Gaps Summary
| # | Gap | Severity |
|---|-----|----------|
| P2-1 | MySQL heartbeat event (HeartbeatLogEvent) not handled — stream may stall during idle | **Medium** |
| P2-2 | Polling connector emits no tombstone/delete events for rows deleted between polls | **Medium** |
| P2-3 | Leader election for scheduler not implemented (split-brain risk on multi-pod control plane) | **Medium** |
| P2-4 | No automatic K8s scale-down when worker has no remaining sources | **Low** |

---

## PDF 3 — Transformations & JSON Handling (`Artifact analysis options (2).pdf`)

### Transform Step Types (10 total)
| Step Type | Requirement | Status | Location |
|---|---|---|---|
| `cast` | Change data type (string → timestamp etc.) | ✅ | `executor.py` `_apply_cast()` |
| `string_op` | upper, lower, trim, substring | ✅ | `executor.py` `_apply_string_op()` |
| `math_op` | add, subtract, multiply on numeric fields | ✅ | `executor.py` `_apply_math_op()` |
| `date_op` | add days, extract year | ✅ | `executor.py` `_apply_date_op()` |
| `json_extract` | Extract value from JSON struct/string | ✅ | `executor.py` `_apply_json_extract()` |
| `json_flatten_inline` | Flatten JSON keys into same table | ✅ | `executor.py` `_apply_json_flatten_inline()` |
| `json_flatten_child` | Flatten JSON/arrays into child table | ✅ | `executor.py` `_apply_json_flatten_child()` |
| `mask` | Mask sensitive fields (last4, hash) | ✅ | `executor.py` `_apply_mask()` |
| `expression` | Spark SQL or SEL DSL expressions | ✅ | `executor.py` `_apply_expression()` |
| `udf` | Call registered user-defined functions | ✅ | `executor.py` `_apply_udf()` |

### Expression DSL (SEL)
| Requirement | Status | Location |
|---|---|---|
| IF, IN, SUBSTRING, UPPER, arithmetic operators | ✅ | `transform/sel_parser.py` |
| Safe deterministic function whitelist for Spark SQL | ✅ | `executor.py` (uses `selectExpr`) |
| UI validation on save (expression syntax check) | ✅ | `transformations.py` `/validate` endpoint |

### UDF Registration
| Requirement | Status | Location |
|---|---|---|
| Register UDF with name, arg types, return type | ✅ | `api/udfs.py` + `models/transformation.py` `UDFCatalog` |
| Per-tenant UDF catalog | ✅ | `udfs.py` tenant-scoped |
| Spark executor calls registered UDF via URL | ✅ | `executor.py` `_apply_udf()` fetches UDF def from control plane |

### JSON Detection & Flattening
| Requirement | Status | Location |
|---|---|---|
| Auto-detect JSON string columns during discovery | ✅ | `sources.py` `_detect_json_column()` |
| Mode A (inline flatten): column per key, keep_original | ✅ | `executor.py` `_apply_json_flatten_inline()` |
| Mode B (child table): foreign key, array support via array_path | ✅ | `executor.py` `_apply_json_flatten_child()` |

### Transformation Preview
| Requirement | Status | Location |
|---|---|---|
| Preview endpoint applies transform to sample rows | ✅ | `transformations.py` `/{pipeline_id}/preview` |
| Preview updates instantly as user toggles steps | ✅ | Endpoint re-evaluates steps per request |
| **Preview for realtime: sample from latest CDC events** | ⚠️ | Preview only supports user-supplied `sample_rows`; no sampling from live Redis stream |

### Execution in Spark
| Requirement | Status | Location |
|---|---|---|
| Spark reads from Redis, applies transform steps | ✅ | `streaming_consumer.py` + `executor.py` |
| JSON parse errors → DLQ | ✅ | `dlq_writer.py` |
| Write to Postgres (upsert) or Iceberg | ✅ | `postgres_writer.py`, `iceberg_writer.py` |

---

## PDF 3 Gaps Summary
| # | Gap | Severity |
|---|-----|----------|
| P3-1 | Transform preview doesn't sample from live CDC events (only accepts user-supplied rows) | **Low** |

---

## PDF 4 — Schema Evolution, Data Quality & Metrics (`Artifact analysis options (3).pdf`)

### Schema Discovery
| Requirement | Status | Location |
|---|---|---|
| Schemas/tables/columns from information_schema (MySQL, Postgres) | ✅ | `sources.py` `_discover_mysql()`, `_discover_postgres()` |
| MongoDB: sample documents to infer field names/types | ✅ | `sources.py` `_discover_mongodb()` |
| JSON-like string column detection by sampling values | ✅ | `sources.py` `_detect_json_column()` |
| Primary keys / unique indexes | ✅ | `sources.py` queries STATISTICS / table_constraints |
| Discovery cache stored on Source record | ✅ | `Source.discovery_cache` JSONB |

### Periodic Re-Introspection
| Requirement | Status | Location |
|---|---|---|
| Daily (configurable) re-introspection background task | ✅ | `main.py` `_periodic_reintrospection()` |
| Detect: new columns, removed columns, type changes, table additions/removals | ✅ | `main.py` `_diff_discovery_cache()` |
| Record Schema Change Events for each detected change | ✅ | `main.py` creates `SchemaChangeEvent` records |
| Update discovery_cache and last_discovery_at after change detected | ✅ | `main.py` |

### Schema Evolution Policies
| Requirement | Status | Location |
|---|---|---|
| AUTO_APPLY: new columns auto-propagate to transform spec and destination | ⚠️ | `schema_evolution.py` creates SchemaChangeEvent and notifies Spark; **does not actually update the TransformPipeline spec** to add the new column to any flatten step |
| AUTO_APPLY: type changes applied if compatible | ❌ | Not implemented — type changes create events but no compatibility check or auto-transform-spec update |
| MANUAL_APPROVAL: changes held for review, approve/reject in UI | ✅ | `schema_evolution.py` approve/reject endpoints |
| Approved → update transform spec + notify Spark | ⚠️ | Spark is notified via `_notify_spark_schema_reload()`; **transform spec is not actually updated** with the new column/type |
| Removed columns: mark deprecated, stop reading values | ❌ | No deprecated column tracking — removed columns simply disappear from discovery cache |
| `applied_at` timestamp when change is applied | ✅ | `schema_evolution.py` (fixed in prior session) |

### Schema Change Notification Flow
| Requirement | Status | Location |
|---|---|---|
| Discovery detects change → SchemaChangeEvent created | ✅ | `main.py` + `schema_evolution.py` |
| AUTO_APPLY: immediately update connection transform spec | ❌ | Not implemented |
| AUTO_APPLY: send webhook to Spark to reload | ✅ | `_notify_spark_schema_reload()` |
| MANUAL_APPROVAL: pending changes in UI | ✅ | list_schema_changes endpoint |
| After apply: log SchemaChangeApplied audit event | ✅ | `schema_evolution.py` `_audit("schema_change.applied")` |

### Data Quality Rules (7 types)
| Rule Type | Status | Location |
|---|---|---|
| `row_count_match` | ✅ | `dq/executor.py` |
| `null_ratio_check` | ✅ | `dq/executor.py` |
| `range_check` | ✅ | `dq/executor.py` |
| `regex_check` | ✅ | `dq/executor.py` |
| `freshness_check` | ✅ | `dq/executor.py` |
| `enum_check` | ✅ | `dq/executor.py` |
| `referential_integrity` | ✅ | `dq/executor.py` |
| on_fail: `alert`, `block`, `continue` | ✅ | `dq/executor.py` |
| **DQ Policy CRUD API (create, list, get, update, delete, executions, violations)** | ❌ | `api/dq_policies.py` — **all 7 handlers are TODO stubs** |

### Prometheus Metrics
| Metric | Status | Location |
|---|---|---|
| `cdc_events_total` (tenant, source, stream, op) | ✅ | `cdc_worker/metrics.py` |
| `cdc_stream_lag_seconds` (tenant, source, stream) | ✅ | `cdc_worker/metrics.py` |
| `cdc_stream_throughput_per_second` (tenant, source, stream) | ✅ | `cdc_worker/metrics.py` |
| `cdc_errors_total` (tenant, source, error_type) | ✅ | `cdc_worker/metrics.py` |
| `cdc_checkpoint_age_seconds` (tenant, source) | ✅ | `cdc_worker/metrics.py` |
| `cdc_sqlite_checkpoint_size_bytes` (tenant, source) | ✅ | `cdc_worker/metrics.py` |
| `cdc_fallback_queue_length` (tenant, source) | ✅ | `cdc_worker/metrics.py` |
| `dq_rule_status` (tenant, connection, rule_type) | ✅ | `dq/executor.py` |
| `dq_rule_violation_total` (tenant, connection, rule_type) | ✅ | `dq/executor.py` |
| `dq_freshness_seconds` (tenant, connection) | ✅ | `dq/executor.py` |
| `api_request_duration_seconds` (method, endpoint) | ✅ | `control-plane/app/main.py` |
| `api_request_count` (method, endpoint, status) | ✅ | `control-plane/app/main.py` |

### Structured Logging
| Requirement | Status | Location |
|---|---|---|
| JSON format with tenant_id, source_id, stream, event_id, log_level, message, error_type, trace_id | ✅ | `main.py` `_JsonFormatter` |
| Distributed trace_id propagated via X-Trace-Id header | ✅ | `main.py` middleware |
| trace_id attached to log entries | ✅ | `log_requests()` middleware |

---

## PDF 4 Gaps Summary
| # | Gap | Severity |
|---|-----|----------|
| P4-1 | `api/dq_policies.py` — all 7 CRUD handlers are TODO stubs; DQ policies cannot be created/managed via API | **High** |
| P4-2 | AUTO_APPLY does not update TransformPipeline spec when new column detected | **High** |
| P4-3 | Manual schema change approval also does not update TransformPipeline spec | **High** |
| P4-4 | Removed/deprecated columns not tracked — no `deprecated` flag, no `stop reading` logic | **Medium** |
| P4-5 | Type change compatibility check not implemented for AUTO_APPLY | **Medium** |

---

## PDF 5 — Deployment, Operations & Security (`Artifact analysis options (4).pdf`)

### Deployment
| Requirement | Status | Location |
|---|---|---|
| On-Premises deployment (bare-metal / private K8s) | ✅ | K8s YAMLs + docker-compose |
| Single cloud region (GKE/AKS/EKS) | ✅ | K8s YAMLs |
| Multi-region (active-active/active-passive control plane) | ⚠️ | No multi-region config in K8s YAMLs; docs mention it but no implementation |
| Ingress with HTTPS/TLS termination | ✅ | `kubernetes/service.yaml` |
| Stateless control plane services, horizontally scalable | ✅ | FastAPI stateless design |
| Managed Postgres or K8s StatefulSet for metadata DB | ✅ | Docker/K8s configs |
| Config reload without restarts | ⚠️ | Periodic re-introspection reloads schema; transform spec hot-swap in Spark works; worker sources fetched via API periodically — but no event-driven reload when control plane config changes |

### Spark and Airflow Integration
| Requirement | Status | Location |
|---|---|---|
| Airflow DAG generation for SCHEDULED connections | ✅ | `orchestration/dag_generator.py` |
| SparkKubernetesOperator in DAGs | ✅ | `dag_generator.py` |
| Spark Structured Streaming for REALTIME | ✅ | `streaming_consumer.py` |
| Spark checkpoint directory on durable filesystem | ✅ | `consumer/checkpoint_handler.py` |
| Dynamic executor allocation | ⚠️ | Not configured in Spark job submission; documented but not implemented |
| **Airflow updates connection run status after completion** | ❌ | DAG generator creates Spark job tasks but no callback to update Connection.status / create ConnectionRun record after completion |
| Custom Spark image with all dependencies | ⚠️ | Dockerfile exists in `docker/` but does not include all required dependencies (Redis connector, Iceberg connector, UDF libs) |

### Destinations
| Requirement | Status | Location |
|---|---|---|
| Postgres: INSERT ON CONFLICT for SCD type 1 | ✅ | `postgres_writer.py` `_upsert()` |
| Postgres: SCD type 2 valid_from/valid_to | ✅ | `postgres_writer.py` `_scd2_upsert()` |
| **SCD type 2 DELETE: close validity range, mark as deleted — NOT hard delete** | ❌ | `postgres_writer.py` `_delete()` does `DELETE FROM table WHERE pk=?`; spec says "a delete closes the validity range of a record and optionally marks it as deleted" |
| Iceberg: Nessie/Glue/Hive catalog | ✅ | `iceberg_writer.py` |
| Iceberg: partitioning strategies per table | ⚠️ | `iceberg_writer.py` `_build_create_sql()` creates table but partitioning spec not configurable per-connection |

### Security & Compliance
| Requirement | Status | Location |
|---|---|---|
| Kubernetes Secrets for credentials | ✅ | `kubernetes/secrets.yaml` |
| **External Secrets Operator / cloud key vault integration** | ❌ | No ExternalSecret K8s manifests; no Vault/AWS Secrets Manager integration |
| Fernet encryption for credentials at rest | ✅ | `utils/crypto.py` |
| TLS for database connections | ⚠️ | SSL params stored in source config; TLS enforcement and cert validation not coded in connectors |
| Kubernetes NetworkPolicies | ❌ | No NetworkPolicy YAML files in `kubernetes/` directory |
| OIDC/SAML identity provider integration | ❌ | Auth uses JWT only; no OIDC/SAML provider integration |
| RBAC with `tenant_admin`, `parent_admin`, `viewer`, `operator` roles | ⚠️ | Roles defined: `superadmin`, `bank_admin`, `tenant_admin`, `user`, `viewer` — **`operator` role missing; `parent_admin` mapped to `bank_admin`** |
| Fine-grained permissions (only control plane can insert/update config) | ✅ | `require_permission()` guards on all mutation endpoints |
| Worker service accounts — authenticated with worker token only | ✅ | `_verify_worker_token()` in `internal.py` |

### Compliance Features
| Requirement | Status | Location |
|---|---|---|
| Delete propagation: DELETE from source → DELETE in warehouse | ✅ | `postgres_writer.py` for SCD1; Iceberg merge deletes |
| SCD type 2 DELETE closes validity range (NOT hard delete) | ❌ | See above — currently hard deletes |
| Audit log of configuration changes | ✅ | `AuditLog` model + `_audit()` helper |
| Audit log of schema changes | ✅ | `schema_evolution.py` `_audit()` calls |
| **Audit log of DQ violations** | ❌ | DQ violations are emitted as Prometheus metrics; not written to audit_logs table |
| **Immutable audit log (append-only, no DELETE/UPDATE)** | ⚠️ | AuditLog model exists but no database-level immutability constraint or API enforcement |

### Operational Practices
| Requirement | Status | Location |
|---|---|---|
| Prometheus dashboards (lag, throughput, errors, DQ status) | ✅ | `monitoring/dashboards/` JSON files |
| Alert rules for lag, errors, DQ fail | ✅ | `monitoring/alerts/cdc_alerts.yaml` |
| Redis monitoring (memory, replication lag) | ⚠️ | Configured in alerting YAML but no dedicated Redis exporter sidecar defined in K8s YAMLs |
| Airflow DAG monitoring | ⚠️ | No Prometheus export from Airflow configured |
| HPA for workers at 70% CPU | ✅ | `kubernetes/hpa.yaml` |
| Connection stats endpoint | ⚠️ | `connections.py` `/stats` returns placeholder data (TODO line 781-782) |
| Source stats endpoint | ⚠️ | `sources.py` `/stats` returns placeholder data (TODO line 958) |
| Destination stats endpoint | ⚠️ | `destinations.py` `/stats` returns placeholder data (TODO line 709-710) |

---

## PDF 5 Gaps Summary
| # | Gap | Severity |
|---|-----|----------|
| P5-1 | SCD type 2 DELETE does hard-delete instead of closing valid_to and marking as deleted | **High** |
| P5-2 | No Kubernetes NetworkPolicy YAMLs — pods unrestricted by network | **High** |
| P5-3 | OIDC/SAML integration absent — JWT only | **Medium** |
| P5-4 | `operator` role missing from RBAC hierarchy | **Medium** |
| P5-5 | DQ violations not written to audit log — only emitted as Prometheus metrics | **Medium** |
| P5-6 | External Secrets Operator / cloud key vault integration absent | **Medium** |
| P5-7 | Airflow callback to update connection run status after job completion missing | **Medium** |
| P5-8 | Connection/Source/Destination `/stats` endpoints return placeholder data | **Medium** |
| P5-9 | Iceberg partitioning not configurable per-connection | **Low** |
| P5-10 | TLS cert validation not enforced in connectors | **Low** |
| P5-11 | AuditLog has no database-level immutability (no append-only enforcement) | **Low** |

---

## Master Gap Table (All PDFs Combined)

| ID | PDF | Area | Gap Description | Severity |
|----|-----|------|-----------------|----------|
| **P1-1** | PDF1 | Streams API | `api/streams.py` all 4 handlers are full TODO stubs | **High** |
| **P1-2** | PDF1 | Initial Full Load | Connection activate/create never triggers initial snapshot (`pass` stub) | **High** |
| **P1-3** | PDF1 | Trigger Sync | `trigger-sync` endpoint is a TODO stub | **High** |
| **P1-4** | PDF1 | DQ Policies API | `api/dq_policies.py` all 7 CRUD handlers are full TODO stubs | **High** |
| **P1-5** | PDF1 | GraphQL API | GraphQL API completely absent — REST only | **Medium** |
| **P1-6** | PDF1 | Destination Test | `destinations.py` connection test is a TODO placeholder | **Medium** |
| **P1-7** | PDF1 | Scheduler | No active Scheduler dynamically assigning workers to DB hosts | **Medium** |
| **P1-8** | PDF1 | Tenant Registry | No K8s scale-up/down when tenants added/removed | **Low** |
| **P2-1** | PDF2 | MySQL Connector | `HeartbeatLogEvent` not handled — stream may stall during idle periods | **Medium** |
| **P2-2** | PDF2 | Polling Connector | No tombstone/delete events emitted for rows deleted between polls | **Medium** |
| **P2-3** | PDF2 | Leader Election | No Redis-based or K8s leader election — split-brain risk | **Medium** |
| **P2-4** | PDF2 | K8s Scale-Down | Workers not automatically scaled down when all sources removed | **Low** |
| **P3-1** | PDF3 | Transform Preview | Preview doesn't sample from live CDC events — only user-supplied rows | **Low** |
| **P4-1** | PDF4 | Schema AUTO_APPLY | AUTO_APPLY does not update TransformPipeline spec with new columns | **High** |
| **P4-2** | PDF4 | Schema Approval | Manual approval also does not update TransformPipeline spec | **High** |
| **P4-3** | PDF4 | Deprecated Columns | Removed columns not tracked with deprecated flag / stop-reading logic | **Medium** |
| **P4-4** | PDF4 | Type Change | AUTO_APPLY type change compatibility check not implemented | **Medium** |
| **P5-1** | PDF5 | SCD2 Delete | SCD2 mode hard-deletes rows — should close valid_to and mark deleted | **High** |
| **P5-2** | PDF5 | NetworkPolicy | No Kubernetes NetworkPolicy YAMLs — all pod traffic unrestricted | **High** |
| **P5-3** | PDF5 | OIDC/SAML | No identity provider integration — JWT only | **Medium** |
| **P5-4** | PDF5 | RBAC Roles | `operator` role missing; `parent_admin` exists as `bank_admin` not `parent_admin` | **Medium** |
| **P5-5** | PDF5 | DQ Audit | DQ violations not written to audit_logs — Prometheus only | **Medium** |
| **P5-6** | PDF5 | Secret Manager | No ExternalSecret / Vault / AWS Secrets Manager integration | **Medium** |
| **P5-7** | PDF5 | Airflow Callback | DAG doesn't update Connection run status after Spark job completes | **Medium** |
| **P5-8** | PDF5 | Stats Endpoints | Connection, Source, Destination `/stats` return placeholder data | **Medium** |
| **P5-9** | PDF5 | Iceberg Partition | Iceberg partition strategy not configurable per-connection | **Low** |
| **P5-10** | PDF5 | TLS Connectors | TLS cert validation not enforced in MySQL/Postgres/MongoDB connectors | **Low** |
| **P5-11** | PDF5 | Audit Immutability | AuditLog has no DB-level append-only enforcement | **Low** |

---

## Prioritised Action Plan

### 🔴 High Priority (Core Functionality Broken / Missing)
1. **P1-1** — Implement `api/streams.py` (list, update, enable, disable stream)
2. **P1-2** — Implement initial full load trigger in `activate_connection()` (dispatch Airflow/Spark job)
3. **P1-3** — Implement `trigger-sync` endpoint (dispatch batch job for connection)
4. **P1-4** — Implement `api/dq_policies.py` full CRUD (all 7 handlers)
5. **P4-1** — AUTO_APPLY: after `_notify_spark_schema_reload()`, also update the `TransformPipeline.spec` to add the new column to the relevant step
6. **P4-2** — Manual approval: same TransformPipeline spec update on approve
7. **P5-1** — SCD2 delete: change `_delete()` to `UPDATE … SET valid_to = now()` when `scd2_mode=True`
8. **P5-2** — Add `kubernetes/network_policy.yaml` restricting pod-to-pod traffic

### 🟡 Medium Priority (Spec-Required Features Missing)
9. **P2-1** — Add `HeartbeatLogEvent` to MySQL `only_events` filter and handle it (update last-seen timestamp)
10. **P2-2** — Polling connector: hash-based row comparison to detect deletes and emit `op='d'` tombstones
11. **P4-3** — Track deprecated columns: add `deprecated_columns` field on Source; stop reading values for removed columns
12. **P4-4** — Implement type-change compatibility check (INT→FLOAT = compatible; VARCHAR→INT = breaking)
13. **P5-3** — OIDC integration (add `python-jose` + OIDC middleware or document external IdP proxy)
14. **P5-4** — Add `operator` role to `ROLE_HIERARCHY` and role initialization script
15. **P5-5** — Write DQ violations to `audit_logs` table in `dq/executor.py` `_emit_metrics()`
16. **P5-7** — Airflow DAG: add final task to POST connection run status back to control plane
17. **P5-8** — Implement real stats for Connection, Source, Destination endpoints

### 🟢 Low Priority (Nice to Have / Operational)
18. **P1-5** — GraphQL API layer
19. **P2-3** — Leader election (Redis SETNX-based or K8s lease)
20. **P3-1** — Live CDC event sampling for transform preview
21. **P5-6** — External Secrets Operator manifests
22. **P5-9** — Configurable Iceberg partitioning per-connection
23. **P5-10** — TLS cert validation in connectors
24. **P5-11** — DB trigger or application guard for audit log immutability

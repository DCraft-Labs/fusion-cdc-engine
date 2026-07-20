# Fusion CDC Engine — Data Flow & Pipeline

> **Audience:** Engineers who need to understand exactly how data moves from a source database row change through to the destination warehouse — step by step, byte by byte.

---

## Table of Contents

1. [The Three Pipeline Modes](#1-the-three-pipeline-modes)
2. [Initial Full Load Flow](#2-initial-full-load-flow)
3. [Realtime CDC Event Flow](#3-realtime-cdc-event-flow)
4. [Scheduled Batch Flow](#4-scheduled-batch-flow)
5. [Trigger-Sync Lifecycle](#5-trigger-sync-lifecycle)
6. [Event Lifecycle Deep-Dive](#6-event-lifecycle-deep-dive)
7. [Transformation Pipeline](#7-transformation-pipeline)
8. [Data Quality Pipeline](#8-data-quality-pipeline)
9. [Schema Evolution Flow](#9-schema-evolution-flow)
10. [Failure & Recovery Flows](#10-failure--recovery-flows)
11. [Checkpoint Lifecycle](#11-checkpoint-lifecycle)
12. [End-to-End Latency Budget](#12-end-to-end-latency-budget)

---

## 1. The Three Pipeline Modes

Every connection in Fusion has a `sync_type` that determines which pipeline path it follows:

```
Connection
  └── sync_type
        ├── REALTIME  → CDC Worker → Redis → Spark Structured Streaming
        ├── SCHEDULED → CDC Worker → Redis → Airflow-triggered Spark Batch
        └── BATCH     → Airflow DAG → Direct Spark JDBC → Destination
```

| Mode | Latency | Use case |
|------|---------|---------|
| `REALTIME` | < 1 second | Transaction tables, audit trails, fraud detection feeds |
| `SCHEDULED` | Minutes to hours (configurable cron) | Daily reports, regulatory snapshots |
| `BATCH` | Hours (full refresh) | Initial loads, tables without CDC support |

---

## 2. Initial Full Load Flow

Before CDC streaming can begin, the destination needs a baseline snapshot of the source table. This flow runs once when a connection is first activated:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 1 — User Configures Connection in UI                               │
│                                                                         │
│  Select source → select destination → choose tables (streams)           │
│  Set sync_mode (INCREMENTAL_APPEND_DEDUPED, CDC_INCREMENTAL, etc.)     │
│  Optionally attach transform pipeline and DQ policy                     │
└─────────────────────────────────────────────┬───────────────────────────┘
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 2 — Control Plane Validates & Introspects                          │
│                                                                         │
│  POST /api/v1/sources/{id}/test-connection → verify DB reachable        │
│  GET  /api/v1/sources/{id}/schema → introspect INFORMATION_SCHEMA       │
│  Store schema snapshot in sources.discovery_cache (JSONB)               │
└─────────────────────────────────────────────┬───────────────────────────┘
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 3 — Record Current Log Position (LSN Bookmark)                     │
│                                                                         │
│  MySQL:  SHOW MASTER STATUS → get current binlog file + position        │
│  PG:     SELECT pg_current_wal_lsn() → get current WAL LSN             │
│  Mongo:  Use current cluster time as start resume token                 │
│                                                                         │
│  This bookmark is the "start-CDC-from" position — all changes AFTER     │
│  this position will be captured; changes BEFORE are loaded from batch.  │
└─────────────────────────────────────────────┬───────────────────────────┘
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 4 — Airflow Full Load DAG                                          │
│                                                                         │
│  SparkKubernetesOperator runs batch Spark job:                         │
│    SELECT * FROM source_table                                           │
│    → apply transformation pipeline                                      │
│    → run DQ checks                                                      │
│    → write to destination (UPSERT or OVERWRITE)                        │
│                                                                         │
│  Airflow callbacks control plane: run status = "completed"              │
│  Control plane sets: connection.initial_load_completed = True           │
└─────────────────────────────────────────────┬───────────────────────────┘
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STEP 5 — CDC Worker Starts Streaming from Bookmark                      │
│                                                                         │
│  Worker resumes from the LSN bookmark captured in Step 3                │
│  Only events AFTER that bookmark are processed                          │
│  No gap, no duplicates (the snapshot is atomic at the bookmark point)   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Realtime CDC Event Flow

This is the **steady-state operation** of the system. For every INSERT, UPDATE, or DELETE in the source database:

```
SOURCE DATABASE
  MySQL / PostgreSQL / MongoDB
  │
  │  DML occurs (INSERT/UPDATE/DELETE)
  │  Written to binlog / WAL / oplog
  │
  ▼
CONNECTOR (inside CDC Worker)
  │
  ├── MySQL: BinLogStreamReader.fetchone()
  │     returns WriteRowsEvent / UpdateRowsEvent / DeleteRowsEvent
  │     Event includes: schema, table, affected rows (before + after values)
  │
  ├── PostgreSQL: cursor.consume_stream(msg_handler)
  │     WAL message decoded from wal2json: {schema, table, kind, columns, values}
  │     msg.cursor.send_feedback() called every 10s (CRITICAL — prevents WAL bloat)
  │
  └── MongoDB: collection.watch() change stream
        Event includes: operationType, ns.db, ns.coll, fullDocument, fullDocumentBeforeChange
  │
  ▼
EVENT ENVELOPE BUILDER
  │
  ├── Map op type: insert→"c", update→"u", delete→"d"
  ├── Build before dict (None for inserts)
  ├── Build after dict (None for deletes)
  ├── Capture pk_values from primary key columns
  ├── Capture ts_ms from event timestamp
  ├── Compute event_id = SHA256(json({pk_values, lsn}))
  └── Build CDCEvent dataclass
  │
  ▼
ROUTING TABLE LOOKUP
  │
  ├── Input: (schema_name, table_name)
  ├── Lookup: exact → (schema, *) → (*, *) wildcard
  └── Output: [{bank_id, tenant_id, source_id}, ...]   (may have 0 or N entries)
  │
  │  If no routing found → log DEBUG "No routing for X.Y — skipping"
  │
  ▼
REDIS STREAM PUBLISHER
  │
  ├── For each routing entry:
  │     stream_key = f"cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}"
  │     XADD {stream_key} MAXLEN ~ 100000 * {all CDCEvent fields as strings}
  │
  ├── On success → update LocalCheckpointManager in-memory state
  │
  └── On RedisError → FallbackQueue.enqueue(event)
                         write to /var/lib/cdc/fallback.db (SQLite)
  │
  ▼
REDIS STREAM (per-tenant per-table)
  Key: cdc:{bank}:{tenant}:{source}:{schema}:{table}
  │
  ▼
SPARK CONSUMER (XREADGROUP)
  │
  ├── Read batch: XREADGROUP GROUP fusion-spark consumer-1 COUNT 100 BLOCK 2000 STREAMS key >
  ├── Deserialize CDCEvent from Redis hash fields
  ├── Apply transformation pipeline (cast, mask, expression, UDF, JSON flatten)
  ├── Run DQ rules (null ratio, range, freshness, regex, enum, ref integrity)
  │     PASS/WARN → continue
  │     FAIL + on_fail=block → send to event_dead_letter_queue table, skip write
  ├── Write to destination:
  │     Postgres DW → INSERT ... ON CONFLICT DO UPDATE (idempotent via event_id)
  │     Iceberg      → MERGE INTO ... (idempotent via pk match + op)
  │
  └── XACK {stream_key} fusion-spark {message_id}  ← commit offset
  │
  ▼
DESTINATION
  Row visible in Postgres DW or Iceberg table
```

---

## 4. Scheduled Batch Flow

For `sync_type = SCHEDULED` connections, CDC events accumulate in Redis and are processed on a cron schedule:

```
Cron expression fires (e.g. "0 */6 * * *")
       │
       ▼
Airflow scheduler triggers DAG
       │
       ▼
Airflow task: fetch config from control plane
  GET /api/v1/connections/{id}
  GET /api/v1/transformations/{pipeline_id}
  GET /api/v1/data-quality/{policy_id}
       │
       ▼
Airflow task: SparkKubernetesOperator
  Spark batch job:
    XREAD all pending events from Redis stream
    Apply transform pipeline
    Run DQ checks
    Write to destination
       │
       ▼
Airflow task: callback to control plane
  POST /api/v1/connections/{id}/runs/{run_id}/callback
    {status: "completed", records_written: N, duration_seconds: T}
       │
       ▼
Control plane updates ConnectionRun:
  status = "completed"
  records_written = N
  completed_at = now()
```

---

## 5. Trigger-Sync Lifecycle

When a user manually clicks "Trigger Sync" from the UI:

```
User clicks "Trigger Sync"
       │
       ▼
POST /api/v1/connections/{id}/trigger-sync
       │
       ├── [CDC/Realtime Connection]
       │     │
       │     ├── 1. Create ConnectionRun (status="running")
       │     ├── 2. Publish Redis pub/sub command to "fusion:commands"
       │     │       {"action": "start-streaming", "connection_id": "..."}
       │     ├── 3. POST http://cdc-worker:8081/internal/start-streaming
       │     ├── 4. GET http://cdc-worker:8081/health → check active_sources
       │     ├── 5. Query checkpoint_state → get current binlog position
       │     ├── 6. Query Redis streams → get event counts per table
       │     ├── 7. Store snapshot in run_config.snapshot
       │     └── 8. Mark run status="completed" (trigger = point-in-time action)
       │
       └── [Batch/Scheduled Connection]
             │
             ├── 1. Mark old stale "running" runs as "failed" (>10 min)
             ├── 2. Create ConnectionRun (status="running")
             ├── 3. Trigger Airflow DAG via Airflow REST API
             └── 4. Return immediately (Airflow will callback with result)

ConnectionRun status lifecycle:
  created → running → completed ✓
                    → failed ✗ (worker unreachable, DQ block, Airflow error)
                    → cancelled (user manually cancelled)
```

**CDC Run Statuses Explained:**

| Status | Meaning |
|--------|---------|
| `completed` | Worker confirmed healthy, source is actively streaming, snapshot captured |
| `failed` | Worker was unreachable at trigger time, or DQ rules blocked the write |
| `running` | (Legacy / batch only) — Airflow job still in progress |
| `cancelled` | User manually stopped |

> Note: For CDC connections, a "completed" run does NOT mean streaming has stopped. It means the trigger was successfully confirmed and streaming is active. The actual CDC streaming runs continuously in the worker process.

---

## 6. Event Lifecycle Deep-Dive

### Single Row Update — Complete Trace

**Source action:**
```sql
UPDATE customers SET city = 'Dubai' WHERE id = 24;
```

**MySQL binlog entry:**
```
# at 10708
#260504 21:46:39 server id 1 end_log_pos 10756
# Update_rows: table id 94 flags: STMT_END_F
### UPDATE fusion_cdc_metadata.customers
### WHERE @1=24 @6='TestCity'
### SET   @1=24 @6='Dubai'
```

**CDCEvent built by worker:**
```python
CDCEvent(
    event_id    = "292c330ed612d850a24d0c9fe70614b2...",  # SHA256
    op          = "u",
    source_id   = "1dda26ae-67ff-46b7-9134-94685055bc30",
    bank_id     = "00000000-0000-4000-a000-000000000001",
    tenant_id   = "00000000-0000-4000-a000-000000000002",
    schema_name = "fusion_cdc_metadata",
    table_name  = "customers",
    lsn         = "mysql-bin.000003:10708",
    ts_ms       = 1777931199000,
    pk_values   = {"id": 24},
    before      = {"id": 24, "city": "TestCity", ...},
    after       = {"id": 24, "city": "Dubai", ...},
    processed_at= 1777931199386
)
```

**Redis XADD payload:**
```
XADD cdc:00000000-0000-4000-a000-000000000001:00000000-0000-4000-a000-000000000002:1dda26ae-...:fusion_cdc_metadata:customers MAXLEN ~ 100000 *
  event_id    "292c330ed612d850..."
  op          "u"
  source_id   "1dda26ae-67ff-46b7-9134-94685055bc30"
  bank_id     "00000000-0000-4000-a000-000000000001"
  tenant_id   "00000000-0000-4000-a000-000000000002"
  schema_name "fusion_cdc_metadata"
  table_name  "customers"
  lsn         "mysql-bin.000003:10708"
  ts_ms       "1777931199000"
  pk_values   '{"id": 24}'
  before      '{"id": 24, "city": "TestCity", ...}'
  after       '{"id": 24, "city": "Dubai", ...}'
  processed_at "1777931199386"
```

**Spark consumer reads and writes:**
```python
# After applying transforms and DQ:
cursor.execute("""
    INSERT INTO fusion_dw.customers 
        (id, city, updated_at, _cdc_event_id, _cdc_op, _cdc_ts)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        city = EXCLUDED.city,
        updated_at = EXCLUDED.updated_at,
        _cdc_event_id = EXCLUDED._cdc_event_id
""", (24, 'Dubai', ..., '292c330e...', 'u', 1777931199000))
```

---

## 7. Transformation Pipeline

Transformations are **declarative JSON specs** stored in the `transform_pipelines` table and executed in Spark at consumption time (not in the CDC worker).

### 7.1 Pipeline Spec Structure

```json
{
  "pipeline_id": "pl_uuid",
  "name": "Customer Data Transform",
  "transforms": [
    {
      "id": "cast_created_at",
      "type": "cast",
      "column": "created_at",
      "to_type": "timestamp",
      "output_column": "created_at"
    },
    {
      "id": "mask_phone",
      "type": "mask",
      "column": "phone",
      "strategy": "last4",
      "output_column": "phone_masked"
    },
    {
      "id": "normalize_name",
      "type": "string_op",
      "column": "name",
      "op": "upper",
      "output_column": "name_normalized"
    },
    {
      "id": "risk_score",
      "type": "udf",
      "function": "compute_risk_score",
      "args": ["amount", "country"],
      "output_column": "risk_score"
    }
  ]
}
```

### 7.2 Step Types Reference

| Type | What it does | Config keys |
|------|-------------|------------|
| `cast` | Convert column to another type (string→timestamp, string→double) | `column`, `to_type`, `output_column` |
| `string_op` | upper / lower / trim / substring / regexp_replace | `column`, `op`, `params`, `output_column` |
| `math_op` | Arithmetic on numeric fields | `expression` (e.g. `amount * 1.1`) |
| `date_op` | Date arithmetic, extract year/month/day | `column`, `op`, `params` |
| `json_extract` | Extract a field from a JSON string column | `column`, `json_path`, `to_type` |
| `json_flatten_inline` | Flatten JSON object keys → additional columns in same row | `column`, `json_schema`, `output_columns`, `keep_original` |
| `json_flatten_child` | Flatten JSON array → separate child table | `column`, `child_table`, `parent_keys`, `array_path` |
| `mask` | PII masking: `last4`, `hash`, `nullify`, `email_domain` | `column`, `strategy` |
| `expression` | Arbitrary Spark SQL expression | `expression`, `output_column`, `language` |
| `udf` | Call a registered UDF | `function`, `args`, `output_column` |

### 7.3 JSON Flattening — Inline Mode

For JSON columns containing objects, flatten keys into the row:

```
Input row: { id: 1, payload_json: '{"amount": 100, "currency": "USD"}' }

Transform spec:
  type: json_flatten_inline
  column: payload_json
  output_columns: { amount: "pay_amount", currency: "pay_currency" }
  keep_original: false

Output row: { id: 1, pay_amount: 100.0, pay_currency: "USD" }
```

### 7.4 JSON Flattening — Child Table Mode

For JSON array columns, emit a separate child table:

```
Input row: { txn_id: 1, line_items: '[{"sku":"A","qty":2},{"sku":"B","qty":1}]' }

Transform spec:
  type: json_flatten_child
  column: line_items
  child_table: txn_line_items
  parent_keys: [txn_id]
  array_path: "$[*]"

Output rows in txn_line_items:
  { txn_id: 1, sku: "A", qty: 2 }
  { txn_id: 1, sku: "B", qty: 1 }
```

### 7.5 Masking Strategies

PII masking is applied **before** events are written to Redis — sensitive data never enters the stream:

| Strategy | Input | Output |
|---------|-------|--------|
| `last4` | `+971-555-1234` | `****1234` |
| `hash` | `user@bank.com` | `SHA256: a1b2c3...` |
| `nullify` | `4111-1111-1111-1111` | `null` |
| `email_domain` | `john.doe@gmail.com` | `***@gmail.com` |

---

## 8. Data Quality Pipeline

DQ rules run inside Spark **after** transformations but **before** writing to the destination.

### 8.1 Rule Evaluation Flow

```
Transformed DataFrame
       │
       ▼
DQ Policy Engine
       │
       ├── Rule 1: null_ratio_check(column="amount", max_ratio=0.01)
       │     count(null) / count(*) ≤ 0.01?
       │     PASS ✓ / FAIL ✗
       │
       ├── Rule 2: range_check(column="risk_score", min=0.0, max=1.0)
       │     all values in [0.0, 1.0]?
       │     PASS ✓ / WARN ⚠ / FAIL ✗
       │
       ├── Rule 3: freshness_check(column="created_at", max_age=3600)
       │     now() - max(created_at) ≤ 3600s?
       │     PASS ✓ / FAIL ✗
       │
       └── Rule 4: regex_check(column="account_no", pattern="^[0-9]{8,20}$")
             all values match regex?
             PASS ✓ / FAIL ✗
       │
       ▼
Aggregated result: PASS | WARN | FAIL
       │
       ├── PASS or WARN (on_fail=alert) → write to destination + send alert
       │
       └── FAIL (on_fail=block)
             → send failing rows to event_dead_letter_queue table
             → skip destination write
             → create DQViolation record
             → trigger alert rule
```

### 8.2 DQ Rule Types Reference

| Rule Type | What It Checks | Parameters |
|-----------|---------------|-----------|
| `row_count_match` | Source count ≈ destination count (±threshold%) | `threshold` |
| `null_ratio_check` | Null % ≤ max_null_ratio | `column`, `max_null_ratio` |
| `range_check` | min_value ≤ column ≤ max_value | `column`, `min_value`, `max_value` |
| `freshness_check` | now() - max(timestamp) ≤ max_age_seconds | `column`, `max_age_seconds` |
| `regex_check` | All values match regex pattern | `column`, `pattern` |
| `enum_check` | All values in allowed set | `column`, `allowed_values` |
| `referential_integrity` | FK values exist in parent table | `column`, `parent_table`, `parent_key` |

### 8.3 On-Fail Actions

| Action | Behaviour |
|--------|---------|
| `alert` | Write to destination AND send notification to configured channels |
| `block` | Do NOT write batch, send failing rows to DLQ, emit alert |
| `continue` | Log violation only, write normally |

### 8.4 Dead Letter Queue

Failed events accumulate in the `event_dead_letter_queue` table:

```sql
SELECT * FROM event_dead_letter_queue
WHERE connection_id = '79b50f04-...'
  AND status = 'pending';
-- → inspect failed rows, fix source data, trigger retry
```

---

## 9. Schema Evolution Flow

When the source database schema changes (new column, removed column, type change):

```
Daily re-introspection background task (runs every 24h)
       │
       ▼
For each active source:
  Query INFORMATION_SCHEMA.COLUMNS
  Diff against sources.discovery_cache
       │
       ├── No changes → log "schema unchanged", update checked_at
       │
       └── Changes found → create SchemaChangeEvent records
             change_type = "COLUMN_ADDED" | "COLUMN_REMOVED" | "TYPE_CHANGED" | "TABLE_ADDED"
             old_def = {previous schema}
             new_def = {current schema}
             status = "pending"
             │
             ▼
       Check connection.schema_evolution_policy
             │
             ├── "AUTO_APPLY"
             │     → update transform spec JSON (add new column, remove dropped column)
             │     → send webhook to Spark consumer: reload schema
             │     → ALTER TABLE at destination (if compatible type change)
             │     → update SchemaChangeEvent.status = "applied"
             │     → create AuditLog entry
             │
             └── "MANUAL_APPROVAL"
                   → SchemaChangeEvent.status = "pending" (stays)
                   → UI shows pending changes badge
                   → User reviews in /schema-evolution page:
                         old type ←→ new type diff
                         Approve → same as AUTO_APPLY
                         Reject  → status = "rejected"
                                   column excluded from sync
                                   marked deprecated in discovery_cache
```

**Mid-stream safety:** Schema changes apply without pausing CDC streaming. The Spark consumer reloads schema metadata on its next micro-batch cycle.

---

## 10. Failure & Recovery Flows

### 10.1 Worker Pod Crash

```
Worker pod crashes (OOM, segfault, K8s preemption)
       │
K8s restart policy: recreate pod (restartPolicy: Always)
       │
Worker startup:
       │
       ├── PVC mounted? YES
       │     Read SQLite: SELECT * FROM checkpoints
       │     Resume each source from last persisted LSN
       │     ⚠ May re-emit last N events (at-least-once delivery)
       │     Spark deduplicates via ON CONFLICT (event_id) — no duplicates in DW
       │
       └── PVC missing? (new node, PVC not reattached)
             POST /api/v1/internal/checkpoints?source_id=...
             Get last central checkpoint → resume from that LSN
             If no central checkpoint → start from current log position
             (no historical events replayed — only new changes captured)
```

### 10.2 Redis Unavailable

```
Redis connection timeout / cluster failure
       │
Worker: XADD fails → RedisError caught
       │
       ▼
FallbackQueue.enqueue(event)
  → INSERT INTO fallback_events (stream_key, event_json, created_at)
  → stored at /var/lib/cdc/fallback.db
       │
       │  fallback_drain_loop runs every 30s
       │
       ▼
FallbackQueue.drain()
  → SELECT * FROM fallback_events WHERE flushed = 0 ORDER BY created_at LIMIT 1000
  → For each event: attempt XADD
  → On success: UPDATE fallback_events SET flushed = 1
       │
Metric: cdc_fallback_queue_length tracks backlog size
Alert:  fires if fallback_queue_length > threshold for 5 minutes
```

### 10.3 PostgreSQL Replication Slot Leak

PostgreSQL accumulates WAL until the replication slot consumer advances its LSN. If a Postgres connector worker crashes without dropping the slot:

```
Postgres WAL accumulates on disk
       │
Alert: pg_replication_slots WHERE active = false AND age > 1 hour
       │
Manual runbook:
  docker exec fusion-pg-source psql -U cdc_user source_db \
    -c "SELECT * FROM pg_replication_slots;"
  -- If slot active = false for > 1 hour:
  SELECT pg_drop_replication_slot('fusion_slot_name');
  -- Then restart the CDC worker → it recreates the slot on startup
```

### 10.4 MongoDB Resume Token Expiry

MongoDB change streams have a limited oplog window. If a worker is down longer than the oplog retention period, the resume token expires:

```
Worker restarts
  → collection.watch(resume_after=old_token)
  → raises ChangeStreamHistoryLost exception
       │
Worker catches → logs error → emits alert
       │
Recovery options:
  Option A: Trigger full resync (re-load all data from scratch for affected tables)
  Option B: Resume from current position (accept data gap)
       │
Monitor: alert on checkpoint_age > 2 hours for MongoDB sources
Prevent: ensure MongoDB oplog size is large enough (minimum 5GB recommended)
```

### 10.5 DQ Block — Dead Letter Queue Processing

```
DQ rule FAIL + on_fail=block
       │
Spark writes failing rows to event_dead_letter_queue
       │
Alert: DQRuleFail fires
       │
Human investigation:
  1. Query event_dead_letter_queue WHERE connection_id = ... AND status = 'pending'
  2. Inspect failing rows (what value violated the rule?)
  3. Options:
     a. Fix source data → rows will flow again on next trigger
     b. Adjust DQ rule threshold → update dq_policy
     c. Mark as acceptable → UPDATE event_dead_letter_queue SET status = 'acknowledged'
     d. Retry → UPDATE event_dead_letter_queue SET status = 'pending' (re-trigger)
```

---

## 11. Checkpoint Lifecycle

Checkpoints track the current position in the database log so the worker can resume without data loss or full replay:

```
WORKER PROCESSING EVENT #1000 (lsn="mysql-bin.000003:10708")
       │
       ├── In-memory state updated:
       │     _checkpoints[("src1", "schema", "table")] = "mysql-bin.000003:10708"
       │
       │   [every 1000 events OR every 30 seconds — whichever comes first]
       │
       ├── SQLite flush:
       │     UPDATE checkpoints SET lsn = "mysql-bin.000003:10708", updated_at = now()
       │     WHERE source_id = "src1" AND schema_name = "schema" AND table_name = "table"
       │
       │   [every 60 seconds — CHECKPOINT_SYNC_INTERVAL]
       │
       └── Central Postgres sync:
             POST /api/v1/internal/checkpoints/batch
             Body: {
               "worker_id": "worker-dev-1",
               "source_id": "1dda26ae-...",
               "checkpoints": [
                 {"schema_name": "fusion_cdc_metadata", "table_name": "customers", "lsn": "mysql-bin.000003:10708"}
               ]
             }
             → 200 OK  (upserts into checkpoint_state table)
```

**Checkpoint Payload Format:**
```json
{
  "worker_id": "worker-dev-1",
  "source_id": "1dda26ae-67ff-46b7-9134-94685055bc30",
  "checkpoints": [
    {
      "schema_name": "fusion_cdc_metadata",
      "table_name": "customers",
      "lsn": "mysql-bin.000003:10708"
    },
    {
      "schema_name": "fusion_cdc_metadata",
      "table_name": "orders",
      "lsn": "mysql-bin.000003:10200"
    }
  ]
}
```

---

## 12. End-to-End Latency Budget

For a typical MySQL INSERT reaching the Postgres DW in realtime mode:

| Stage | Latency | Notes |
|-------|---------|-------|
| MySQL commit → binlog write | ~1ms | MySQL InnoDB sync |
| Binlog read by worker | ~5-50ms | python-mysql-replication polling |
| Event envelope build + routing | ~1ms | In-memory |
| Redis XADD | ~1-5ms | Local network (dev) / <10ms (prod) |
| Spark micro-batch cycle | ~5 seconds | `processingTime="5 seconds"` trigger |
| Transform + DQ execution | ~10-100ms | Depends on complexity |
| Postgres upsert | ~5-20ms | Per-row or batched |
| **Total (median)** | **~6-10 seconds** | Dominated by Spark micro-batch interval |
| **Total (peak/busy)** | **~15-30 seconds** | Under high load or transformation complexity |

To reduce latency:
- Decrease Spark micro-batch interval: `.trigger(processingTime="1 second")`
- Use continuous processing mode: `.trigger(continuous="1 second")`
- Increase Spark parallelism (more executors)

For sub-second requirements, events are visible in Redis immediately after the worker processes them. Redis XLEN shows real-time count even before Spark processes them.

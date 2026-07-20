# Fusion CDC Engine — Architecture

> **Audience:** Engineers, architects, and technical leaders who need a complete mental model of how the system is designed, why it is designed that way, and how each layer connects.

---

## Table of Contents

1. [What Is Fusion?](#1-what-is-fusion)
2. [Why CDC Over Batch ETL](#2-why-cdc-over-batch-etl)
3. [Tenancy Model](#3-tenancy-model)
4. [System Planes Overview](#4-system-planes-overview)
5. [Full Architecture Diagram](#5-full-architecture-diagram)
6. [Control Plane Architecture](#6-control-plane-architecture)
7. [Data Plane Architecture](#7-data-plane-architecture)
8. [Transport Layer — Redis Streams](#8-transport-layer--redis-streams)
9. [Consumer Layer — Spark & Airflow](#9-consumer-layer--spark--airflow)
10. [Multi-Tenancy Enforcement](#10-multi-tenancy-enforcement)
11. [Technology Stack Summary](#11-technology-stack-summary)
12. [Development Environment](#12-development-environment)
13. [Kubernetes Production Architecture](#13-kubernetes-production-architecture)

---

## 1. What Is Fusion?

Fusion CDC Engine is a **production-grade, multi-tenant, log-based Change Data Capture (CDC) platform** that replaces traditional periodic batch ETL with real-time or scheduled streaming data integration.

It captures every INSERT, UPDATE, and DELETE from source databases (MySQL, PostgreSQL, MongoDB) at the moment they happen, normalises them into a canonical event envelope, routes them through Redis Streams to per-tenant channels, and delivers them to destination data stores (PostgreSQL Data Warehouse, Apache Iceberg on object storage).

### Core Goals

| Goal | How |
|------|-----|
| **Sub-minute latency** | Log-based streaming — no polling |
| **Delete visibility** | `op='d'` events flow end-to-end |
| **Zero source load** | Sequential log reads, not full table scans |
| **Multi-tenancy at scale** | One replication connection per physical DB host, routes to N tenants |
| **Exactly-once delivery** | SHA-256 `event_id` + idempotent upsert at destination |
| **Schema-change resilience** | Auto-apply or manual-approval policies; no job restarts |
| **Observability** | Prometheus metrics, structured JSON logs, distributed trace IDs |

---

## 2. Why CDC Over Batch ETL

Traditional batch ETL (the previous DCraft Labs approach) has structural limitations:

```
┌─────────────────────────────────────────────────────┐
│ BATCH ETL PROBLEMS                                  │
├─────────────────────┬───────────────────────────────┤
│ Latency             │ 6+ hours between data loads   │
│ Missing deletes     │ SELECT cannot see deleted rows │
│ Source DB spikes    │ Full table scans every run     │
│ Schema changes      │ Spark job hard-fails           │
│ Multi-tenancy cost  │ N Spark jobs for N tenants     │
│ Observability       │ Only Airflow task status       │
└─────────────────────┴───────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ CDC BENEFITS                                        │
├─────────────────────┬───────────────────────────────┤
│ Latency             │ < 1 second (binlog read)      │
│ Delete handling     │ op='d' events, full before row │
│ Source DB load      │ Sequential log read (minimal)  │
│ Schema changes      │ Auto-apply or approval flow    │
│ Multi-tenancy       │ 1 worker per DB host, N routes │
│ Observability       │ 12 Prometheus metrics + logs   │
└─────────────────────┴───────────────────────────────┘
```

The key insight is that **database transaction logs already contain everything** — which rows changed, how they changed, when, and what the old values were. CDC is simply reading that log efficiently.

---

## 3. Tenancy Model

### Hierarchy

```
Parent Organisation (Bank / Financial Institution)
  └── Tenant (Business Unit / Product)
        ├── Source A  ─ one physical MySQL/PG/Mongo host
        ├── Source B  ─ another DB host (or same)
        └── Destination ─ Postgres DW or Iceberg
```

### Critical Design Insight: Shared Physical Hosts

Multiple tenants often share the **same physical database server**. For example, both `ahli_pay` and `ahli_pay_insta` run on the same Qatar MySQL host.

The per-host worker model reads the binlog **once** and routes events to per-tenant Redis stream keys:

```
Tenant 1 (ahli_pay)  ──┐
Tenant 2 (ahli_pay_insta) ─┤── Same MySQL host
Tenant 3 (wps)       ──┘    │
                             ▼
                    CDC Worker (1 replication connection)
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
           Redis Stream            Redis Stream
           cdc:dcraftlabs:ahli_pay:...    cdc:dcraftlabs:wps:...
```

This avoids N replication slots / N connections for N tenants — which would crash source databases.

### Tenant Identifiers in Code

Every model and stream key carries two IDs:

| Field | Purpose |
|-------|---------|
| `bank_id` | Parent organisation UUID |
| `sub_tenant_id` (= `tenant_id`) | Business unit UUID |

All database queries in the control plane are automatically scoped by `sub_tenant_id` via `TenantIsolationMiddleware`.

---

## 4. System Planes Overview

Fusion is divided into two planes with strict separation:

```
┌──────────────────────────────────┐
│         CONTROL PLANE            │  FastAPI (Python)
│   Config · Auth · Metadata · API │  Port 8000
│   Postgres metadata DB           │  Port 5432
└──────────────┬───────────────────┘
               │ assigns sources, provides routing/checkpoints
               ▼
┌──────────────────────────────────┐
│          DATA PLANE              │  asyncio Python workers
│   CDC Workers · Redis Streams    │  Worker: Port 8081
│   Spark Consumer · Airflow       │  Airflow: Port 8080
└──────────────────────────────────┘
```

| Plane | Responsibility | Technology |
|-------|---------------|-----------|
| Control Plane | Configuration, authentication, metadata storage, API, scheduling, monitoring aggregation | FastAPI, SQLAlchemy, PostgreSQL, Redis |
| Data Plane | Log capture, event publication, event consumption, data transformation, DQ, destination writes | Python asyncio, Redis Streams, PySpark, Airflow |

---

## 5. Full Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                          CONTROL PLANE                                       ║
║                                                                              ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              ║
║  │  React Frontend │  │  FastAPI Server  │  │  PostgreSQL DB  │              ║
║  │  Port 5173      │◄─┤  Port 8000      │◄─┤  Port 5432      │              ║
║  │  Vite/TypeScript│  │  JWT Auth        │  │  98 tables       │              ║
║  └─────────────────┘  │  REST API        │  │  42 core tables  │              ║
║                        │  GraphQL API     │  │  Audit logs      │              ║
║  ┌─────────────────┐  │  Prometheus /met.│  └─────────────────┘              ║
║  │  Airflow UI     │  └────────┬────────┘                                    ║
║  │  Port 8080      │           │ worker auth token                           ║
║  │  DAG management │           │ assigns sources                             ║
║  └─────────────────┘           │ routing table                              ║
║                                 │ checkpoint API                             ║
╚═════════════════════════════════╪════════════════════════════════════════════╝
                                  │
╔═════════════════════════════════╪════════════════════════════════════════════╗
║                          DATA PLANE                                          ║
║                                 ▼                                            ║
║  ┌────────────────────────────────────────────────────────────────────────┐  ║
║  │                     CDC WORKER  (Port 8081)                            │  ║
║  │                                                                        │  ║
║  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐ │  ║
║  │  │ MySQL       │  │ PostgreSQL  │  │ MongoDB     │  │ Polling      │ │  ║
║  │  │ Connector   │  │ Connector   │  │ Connector   │  │ Connector    │ │  ║
║  │  │ (binlog ROW)│  │ (WAL/wal2j.)│  │ (change     │  │ (fallback)   │ │  ║
║  │  └──────┬──────┘  └──────┬──────┘  │  streams)   │  └─────┬────────┘ │  ║
║  │         │                │         └──────┬───────┘        │          │  ║
║  │         └────────────────┴────────────────┘────────────────┘          │  ║
║  │                                    │                                   │  ║
║  │                                    ▼                                   │  ║
║  │  ┌─────────────────────────────────────────────────────────────────┐  │  ║
║  │  │  CDCEvent Envelope Builder                                       │  │  ║
║  │  │  event_id = SHA256(pk_values + lsn)  op ∈ {c,u,d}              │  │  ║
║  │  │  before + after + ts_ms + lsn + metadata                        │  │  ║
║  │  └───────────────────────────────┬─────────────────────────────────┘  │  ║
║  │                                  │                                    │  ║
║  │  ┌──────────────────┐            │                                    │  ║
║  │  │ Routing Table    │            │  route event                       │  ║
║  │  │ (schema,table)   │◄───────────┘                                   │  ║
║  │  │  → tenant keys   │                                                 │  ║
║  │  └──────────┬───────┘                                                 │  ║
║  │             │  publish via XADD                                       │  ║
║  │  ┌──────────▼───────────────────────────────────────────────────────┐│  ║
║  │  │  Redis Stream Publisher                                           ││  ║
║  │  │  Key: cdc:{bank}:{tenant}:{source}:{schema}:{table}              ││  ║
║  │  │  Fallback: SQLite disk queue if Redis unavailable                 ││  ║
║  │  └──────────────────────────────────────────────────────────────────┘│  ║
║  │                                                                        │  ║
║  │  Background loops:                                                     │  ║
║  │    - Checkpoint sync (every 60s) → control plane API                  │  ║
║  │    - Fallback drain loop (every 30s) → retry queued events             │  ║
║  │    - Heartbeat (every 30s) → worker_heartbeats table                   │  ║
║  │    - HTTP server (:8081/health, :8081/internal/start-streaming)        │  ║
║  └────────────────────────────────────────────────────────────────────────┘  ║
║                                                                              ║
║  ┌────────────────────────────────────────────────────────────────────────┐  ║
║  │                    REDIS STREAMS  (Port 6379)                          │  ║
║  │                                                                        │  ║
║  │  cdc:bank1:tenant1:src1:fusion_cdc_metadata:customers              │  ║
║  │  cdc:bank1:tenant1:src1:fusion_cdc_metadata:orders                 │  ║
║  │  cdc:bank1:tenant1:src1:fusion_cdc_metadata:products               │  ║
║  │                                                                        │  ║
║  │  MAXLEN ~ 100,000 events per stream                                    │  ║
║  │  Consumer group: "fusion-spark"                                    │  ║
║  └────────────────────────────────────────────────────────────────────────┘  ║
║                                                                              ║
║  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   ║
║  │  SPARK CONSUMER             │  │  AIRFLOW  (Port 8080)               │   ║
║  │  (Realtime: Structured      │  │  Batch/Scheduled syncs              │   ║
║  │   Streaming mode)           │  │  SparkKubernetesOperator DAGs       │   ║
║  │  XREADGROUP from Redis      │  │  admin / admin                      │   ║
║  │  → transforms + DQ          │  │  Calls control plane callback API   │   ║
║  │  → write to Postgres DW     │  └─────────────────────────────────────┘   ║
║  │    or Iceberg/S3            │                                            ║
║  └─────────────────────────────┘                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
                            │                    │
               ┌────────────┘                    └────────────┐
               ▼                                              ▼
    ┌────────────────────┐                       ┌────────────────────┐
    │  Postgres DW       │                       │  Iceberg / S3      │
    │  Port 5433         │                       │  Azure Blob / GCS  │
    │  upsert SCD Type 1 │                       │  MERGE INTO        │
    │  or SCD Type 2     │                       │  via Nessie catalog│
    └────────────────────┘                       └────────────────────┘

    SOURCE DATABASES (all containerised in dev)
    ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
    │ MySQL Source  │  │ PG Source     │  │ MongoDB Source│
    │ Port 3307     │  │ Port 5434     │  │ Port 27018    │
    │ binlog ROW    │  │ wal_level=log.│  │ replica set   │
    └───────────────┘  └───────────────┘  └───────────────┘
```

---

## 6. Control Plane Architecture

### 6.1 FastAPI Application Structure

```
control-plane/
  app/
    main.py              ← FastAPI app, middleware, startup tasks
    config.py            ← Settings (env vars)
    database.py          ← SQLAlchemy session factory
    middleware/
      auth.py            ← JWT validation, bearer token extraction
      tenant_isolation.py← Scopes all DB queries to sub_tenant_id
    models/
      auth.py            ← User, Role, AuditLog
      connection.py      ← Connection, Stream, ConnectionRun
      monitoring.py      ← CheckpointState, ConnectionRun (CDC metrics)
      source_destination.py← Source, Destination
      schema_evolution.py← SchemaChangeEvent
    api/
      auth.py            ← Login, refresh token, user management
      sources.py         ← Source CRUD + schema introspection
      destinations.py    ← Destination CRUD
      connections.py     ← Connection CRUD + trigger-sync endpoint
      streams.py         ← Stream CRUD (per-table config)
      transformations.py ← Transform pipeline CRUD
      data_quality.py    ← DQ policy CRUD
      monitoring.py      ← Metrics, health, lag, run history
      alerting.py        ← Alert rules and notification channels
      schema_evolution.py← Schema change events and approval flow
      internal.py        ← Worker-facing endpoints (routing, checkpoints)
      graphql.py         ← GraphQL API (Strawberry)
      udfs.py            ← UDF registration and management
```

### 6.2 API Modules

| Module | Base Path | Key Endpoints |
|--------|-----------|--------------|
| `auth` | `/api/v1/auth` | POST `/login`, POST `/refresh`, GET `/me` |
| `sources` | `/api/v1/sources` | CRUD, GET `/schema`, POST `/test-connection` |
| `destinations` | `/api/v1/destinations` | CRUD, POST `/test-connection` |
| `connections` | `/api/v1/connections` | CRUD, POST `/{id}/trigger-sync`, GET `/{id}/runs`, GET `/{id}/health` |
| `streams` | `/api/v1/streams` | GET/PATCH per-table stream config |
| `transformations` | `/api/v1/transformations` | Pipeline CRUD and preview |
| `data_quality` | `/api/v1/data-quality` | DQ policy CRUD and violation history |
| `monitoring` | `/api/v1/monitoring` | Metrics, lag history, throughput history |
| `alerting` | `/api/v1/alerting` | Alert rule CRUD and notification channels |
| `schema_evolution` | `/api/v1/schema-evolution` | Pending changes, approve/reject |
| `internal` | `/api/v1/internal` | Worker routing table, checkpoint batch upsert |
| `graphql` | `/graphql` | GraphQL query interface |

### 6.3 Authentication & Authorisation

```
Request → AuthMiddleware
            │
            ├─ Bearer token? → Validate JWT (HS256, SECRET_KEY)
            │                   Extract: user_id, sub_tenant_id, roles
            │
            └─ Worker token? → Compare against WORKER_TOKEN env var
                               Used by CDC worker for internal API calls

All authenticated requests → TenantIsolationMiddleware
                              Injects sub_tenant_id into DB session context
                              All queries automatically filtered by tenant
```

**JWT payload:**
```json
{
  "sub": "<user_id>",
  "tenant_id": "<sub_tenant_id>",
  "bank_id": "<bank_id>",
  "roles": ["tenant_admin"],
  "exp": 1778000000
}
```

### 6.4 Metadata Database

98 tables total. The key ones for CDC:

| Table | Purpose |
|-------|---------|
| `users` | User accounts |
| `connections` | Connection configurations (source→dest, sync mode, schedule) |
| `connection_runs` | History of every sync trigger with status, duration, snapshot |
| `streams` | Per-table sync config within a connection |
| `sources` | Source DB config (host, port, credentials ref, CDC config) |
| `destinations` | Destination config |
| `checkpoint_state` | Central checkpoint store — `(source_id, schema, table) → LSN` |
| `transform_pipelines` | JSON transform specs |
| `dq_policies` | Data quality rule sets |
| `schema_change_events` | Schema drift events with approval state |
| `worker_heartbeats` | Last-seen timestamps per worker pod |
| `cdc_lag_metrics` | Rolling lag history per stream |
| `redis_stream_tracking` | Event counts per Redis stream key |
| `audit_logs` | Immutable audit trail of all config changes |
| `alert_rules` | Threshold rules for Prometheus alerting |
| `notification_channels` | Webhook / Slack / PagerDuty targets |

### 6.5 Background Tasks (Startup)

On application startup (`lifespan` event), the control plane starts two background asyncio tasks:

1. **Periodic Schema Re-introspection** — every `SCHEMA_REINTROSPECT_INTERVAL_HOURS` hours (default 24):
   - Queries `information_schema.columns` for all active sources
   - Diffs against stored `discovery_cache`
   - Creates `SchemaChangeEvent` records for additions, removals, and type changes
   - Applies AUTO_APPLY policy changes automatically

2. **Prometheus metrics scraping** — exposed at `GET /metrics`

### 6.6 Trigger-Sync Flow (CDC Type)

When a user clicks "Trigger Sync" for a CDC connection:

```
POST /api/v1/connections/{id}/trigger-sync
          │
          ▼
1. Validate: connection exists, user has access, sync_type is CDC
2. Mark old stale BATCH runs as failed (CDC runs are exempt)
3. Create ConnectionRun record (status="running", orchestration="streaming")
4. Call _trigger_dag_or_worker():
     → Publish "start-streaming" command to Redis pub/sub
     → POST /internal/start-streaming to CDC worker HTTP
5. Check worker health: GET http://cdc-worker:8081/health
6. Capture CDC snapshot:
     - Worker ID and active sources
     - Binlog position from checkpoint_state
     - Redis event counts per table
     - Monitored tables list
7. Mark run status="completed", store snapshot in run_config.snapshot
8. Return SyncTriggerResponse
```

---

## 7. Data Plane Architecture

### 7.1 CDC Worker

The CDC worker is a long-running asyncio Python process. Each worker instance is assigned to one or more database sources. It runs entirely independently of the control plane after startup.

**Worker internal components:**

```
Worker
  ├── Source Tasks (one asyncio coroutine per source)
  │     └── Connector (MySQL/PG/MongoDB/Polling)
  │           └── stream_events() → CDCEvent generator
  ├── RedisStreamPublisher
  │     └── FallbackQueue (SQLite, if Redis unavailable)
  ├── LocalCheckpointManager (SQLite)
  ├── CentralCheckpointSync (pushes to control plane API every 60s)
  ├── RoutingTable (refreshed from control plane every N seconds)
  ├── HeartbeatSender (POST /api/v1/internal/heartbeat every 30s)
  ├── HTTP Server (:8081)
  │     ├── GET /health → {"status":"healthy","active_sources":[...]}
  │     └── POST /internal/start-streaming → {"connection_id":"..."}
  └── Redis Command Listener (pub/sub channel "fusion:commands")
```

**Asyncio semaphore:** `asyncio.Semaphore(MAX_CONCURRENT_TABLES)` limits how many connector coroutines run simultaneously. Default: 20 in dev.

### 7.2 Connector Architecture

All connectors implement the `BaseConnector` interface:

```python
class BaseConnector:
    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        """Yield CDCEvent objects indefinitely until stopped."""
```

| Connector | Library | Log Mechanism | Resume Token |
|-----------|---------|--------------|-------------|
| `MySQLConnector` | `python-mysql-replication` | Binary log (ROW format) | `(log_file, log_pos)` |
| `PostgresConnector` | `psycopg2` (replication extras) | Logical replication slot + `wal2json` | WAL LSN |
| `MongoDBConnector` | `pymongo` | Change streams (`db.watch()`) | `_id` (resume token) |
| `PollingConnector` | `pymysql`/`psycopg2` | SELECT with cursor column | cursor column max value |

### 7.3 CDCEvent Envelope

Every change from every connector is normalised into a `CDCEvent`:

```python
@dataclass
class CDCEvent:
    event_id:    str   # SHA-256(pk_values + lsn) — dedup key
    op:          str   # "c" | "u" | "d"
    source_id:   str   # UUID of the Source record
    bank_id:     str   # Parent org UUID
    tenant_id:   str   # Business unit UUID
    schema_name: str   # Database/schema name
    table_name:  str   # Table name
    lsn:         str   # Log position (file:pos / WAL LSN / resume token)
    ts_ms:       int   # Source event time in epoch milliseconds
    pk_values:   dict  # Primary key column → value
    before:      dict  # Row state before change (None for inserts)
    after:       dict  # Row state after change (None for deletes)
    metadata:    dict  # Transaction ID, host, etc.
    processed_at: int  # When worker processed this (epoch ms)
```

The `event_id` is the **idempotency key** — the same event always gets the same ID, so:
- Redis XADD can be retried safely
- Spark's `ON CONFLICT DO UPDATE` is a no-op for duplicates

### 7.4 Dual-Layer Checkpointing

```
                 Worker Pod
        ┌───────────────────────────────────┐
        │  In-memory state                  │
        │  {(source_id, schema, table): LSN} │
        └──────────────┬────────────────────┘
                       │ every N events OR every T seconds
                       ▼
        ┌───────────────────────────────────┐
        │  Local SQLite                     │
        │  /var/lib/cdc/checkpoints.db      │ ◄── fast, survives process restart
        │  Table: checkpoints               │
        │    (source_id, schema, table, lsn)│
        └──────────────┬────────────────────┘
                       │ every CHECKPOINT_SYNC_INTERVAL seconds (default 60)
                       │ via POST /api/v1/internal/checkpoints/batch
                       ▼
        ┌───────────────────────────────────┐
        │  Central Postgres                 │
        │  Table: checkpoint_state          │ ◄── survives pod deletion/PVC loss
        │  UPSERT on (worker_id, source_id, │
        │             schema, table)        │
        └───────────────────────────────────┘
```

**Recovery priority:**
1. SQLite present → resume from local checkpoint (faster, avoids API call)
2. SQLite missing → fetch from central Postgres API
3. Both missing → start from current log position (no historical replay)

### 7.5 Fallback Queue

When Redis is unavailable, events are written to a local SQLite fallback queue:

```
Redis XADD fails
      │
      ▼
FallbackQueue.enqueue(event)  →  /var/lib/cdc/fallback.db
                                  (SQLite table: fallback_events)
      │
      │  (when Redis recovers)
      │
FallbackQueue.drain() every 30s → replay events → XADD
```

### 7.6 Routing Table

The routing table answers: "given a change on `(schema, table)`, which Redis stream key(s) should I publish to?"

```
GET /api/v1/internal/workers/{worker_id}/routing
  → [{
      "schema_name": "*",   ← wildcard matches all tables
      "table_name": "*",
      "bank_id": "00000000-0000-4000-a000-000000000001",
      "tenant_id": "00000000-0000-4000-a000-000000000002",
      "source_id": "1dda26ae-67ff-46b7-9134-94685055bc30"
    }]
```

Lookup logic (in priority order):
1. Exact match: `(schema, table)`
2. Table wildcard: `(*, table)`
3. Full wildcard: `(*, *)`

The routing table is refreshed from the control plane API on startup and periodically thereafter.

---

## 8. Transport Layer — Redis Streams

### 8.1 Stream Key Convention

```
cdc:{bank_id}:{tenant_id}:{source_id}:{schema_name}:{table_name}
```

Real example from dev:
```
cdc:00000000-0000-4000-a000-000000000001:00000000-0000-4000-a000-000000000002:1dda26ae-67ff-46b7-9134-94685055bc30:fusion_cdc_metadata:customers
```

This hierarchical key enables:
- Per-tenant isolation (stream is only readable with tenant ID)
- Per-table granularity (consumers can subscribe to specific tables)
- Sharding in Redis Cluster (consistent hash of key)

### 8.2 Stream Operations

```bash
# Worker publishes:
XADD cdc:b1:t1:src1:public:orders MAXLEN ~ 100000 * \
  event_id "abc123..." op "c" source_id "..." \
  schema_name "public" table_name "orders" \
  lsn "mysql-bin.000003:10708" ts_ms "1777931199000" \
  pk_values '{"id": 24}' before "null" after '{"id":24,"name":"..."}'

# Consumer reads:
XREADGROUP GROUP fusion-spark spark-consumer-1 COUNT 100 BLOCK 2000 \
  STREAMS cdc:b1:t1:src1:public:orders >

# Consumer acknowledges:
XACK cdc:b1:t1:src1:public:orders fusion-spark <message-id>
```

### 8.3 Memory Management

Each stream is capped with `XADD ... MAXLEN ~ 100000` (approximate trim):
- ~100k events per table × N tables = bounded memory
- `noeviction` policy in Redis — never silently drop events
- Fallback queue on disk absorbs events if Redis is full

### 8.4 Consumer Groups

Consumer groups enable multiple Spark instances to coordinate consumption without duplicates:
```
Stream: cdc:b1:t1:src1:public:orders
  Consumer group: fusion-spark
    Consumer 1: spark-consumer-dev-1
    Consumer 2: spark-consumer-dev-2 (scale-out)
```

---

## 9. Consumer Layer — Spark & Airflow

### 9.1 Realtime Mode (PySpark Structured Streaming)

For `sync_type = REALTIME` connections:

```python
# Long-running Spark job
df = spark.readStream \
    .format("redis") \
    .option("stream.keys", "cdc:b1:t1:src1:public:orders") \
    .option("consumer.group", "fusion-spark") \
    .load()

transformed_df = apply_transform_pipeline(df, pipeline_spec)
dq_pass, dq_fail = apply_dq_policy(transformed_df, dq_policy)

dq_pass.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .trigger(processingTime="5 seconds") \
    .start()
```

### 9.2 Scheduled Mode (Airflow + Spark Batch)

For `sync_type = SCHEDULED` connections:

```python
# Airflow DAG
@dag(schedule_interval='0 */6 * * *')
def sync_tenant_data():
    spark_job = SparkKubernetesOperator(
        task_id='run_spark_batch',
        application_file='batch-job-spec.yaml',
    )
```

Airflow calls back to the control plane on completion:
- `POST /api/v1/connections/{id}/runs/{run_id}/callback` — updates run status, records_written

### 9.3 Destination Writers

**PostgreSQL DW (SCD Type 1 upsert):**
```sql
INSERT INTO table (...)
VALUES (...)
ON CONFLICT (event_id) DO UPDATE SET ...
```

**Iceberg (MERGE INTO):**
```sql
MERGE INTO catalog.namespace.table AS target
USING batch_updates AS source
ON target.id = source.id
WHEN MATCHED AND source.op IN ('u') THEN UPDATE SET *
WHEN MATCHED AND source.op = 'd' THEN DELETE
WHEN NOT MATCHED AND source.op = 'c' THEN INSERT *
```

---

## 10. Multi-Tenancy Enforcement

Tenancy isolation is enforced at every layer:

| Layer | Mechanism |
|-------|----------|
| API | `TenantIsolationMiddleware` scopes all DB queries to `sub_tenant_id` |
| Redis | Stream key prefix includes `bank_id:tenant_id` |
| Workers | Routing table maps `(schema,table)` → `(bank_id, tenant_id, source_id)` |
| Checkpoints | Checkpoint records keyed by `(worker_id, source_id, schema, table)` |
| Destinations | Per-tenant schema/namespace in Postgres DW and Iceberg |
| Audit | All actions logged with `user_id` and `tenant_id` |

---

## 11. Technology Stack Summary

| Component | Technology | Version | Port |
|-----------|-----------|---------|------|
| Control plane API | FastAPI (Python) | 3.11 | 8000 |
| Control plane DB | PostgreSQL | 16 | 5432 |
| Frontend | React + Vite + TypeScript | Node 18 | 5173 |
| CDC Workers | Python asyncio | 3.11 | 8081 |
| MySQL source | MySQL | 8.0 | 3307 |
| PostgreSQL source | PostgreSQL | 16 (wal_level=logical) | 5434 |
| MongoDB source | MongoDB | 7.0 (replica set) | 27018 |
| Destination DW | PostgreSQL | 16 | 5433 |
| Event transport | Redis Streams | 7.2 | 6379 |
| Batch orchestration | Apache Airflow | 2.9.3 | 8080 |
| Stream processing | PySpark Structured Streaming | 3.x | — |
| Containerisation | Docker Compose (dev) | — | — |
| Production orchestration | Kubernetes + HPA | — | — |
| Monitoring | Prometheus + Grafana | — | — |
| Secrets | K8s Secrets / External Secrets Operator | — | — |

---

## 12. Development Environment

### Service Map

| Container | Port | Credentials | Purpose |
|-----------|------|-------------|---------|
| `fusion-postgres` | 5432 | `fusion_user` / `fusion_password` | Control plane metadata DB |
| `fusion-postgres-dest` | 5433 | `dw_user` / `dw_password` | Destination data warehouse |
| `fusion-pg-source` | 5434 | `cdc_user` / `cdc_password` | PostgreSQL CDC source |
| `fusion-mysql` | 3307 | `fusion_user` / `fusion_password`, root: `root_password` | MySQL CDC source |
| `fusion-mongo` | 27018 | (no auth in dev) | MongoDB CDC source |
| `fusion-redis` | 6379 | (no auth in dev) | Event transport |
| `fusion-airflow` | 8080 | `admin` / `admin` | Batch job orchestration |
| `fusion-cdc-worker` | 8081 | Worker token: `dev-worker-token` | CDC streaming worker |
| Control plane | 8000 | JWT via `/api/v1/auth/login` | FastAPI API server |
| Frontend | 5173 | — | React dev server |

### Key Environment Variables

```bash
# Control plane (.env)
DATABASE_URL=postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata
REDIS_URL=redis://localhost:6379/0
JWT_SECRET_KEY=your-super-secret-jwt-key-change-this-in-production
JWT_ALGORITHM=HS256
JWT_EXPIRATION_MINUTES=30
ENCRYPTION_KEY=your-32-byte-encryption-key-for-aes256
CORS_ORIGINS=http://localhost:3000,http://localhost:5173

# CDC Worker
CONTROL_PLANE_URL=http://host.docker.internal:8000
WORKER_TOKEN=dev-worker-token
WORKER_ID=worker-dev-1
REDIS_URL=redis://redis:6379/0
CHECKPOINT_DB_PATH=/var/lib/cdc/checkpoints.db
FALLBACK_DB_PATH=/var/lib/cdc/fallback.db
MAX_CONCURRENT_TABLES=20
HEARTBEAT_INTERVAL=30
CHECKPOINT_SYNC_INTERVAL=60
```

### Quick Start

```bash
# Start all infrastructure
cd fusion-cdc-engine
docker compose -f docker/docker-compose.dev.yml up -d

# Start control plane (local dev, hot-reload)
cd control-plane
source ../.venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Start frontend
cd frontend
npm run dev

# Verify CDC worker
curl http://localhost:8081/health
# → {"status":"healthy","worker_id":"worker-dev-1","active_sources":["..."]}

# Check Redis streams
docker exec fusion-redis redis-cli keys "cdc:*"
```

---

## 13. Kubernetes Production Architecture

### Resource Layout

| Resource | Count | Purpose |
|----------|-------|---------|
| `Deployment` | 1× control plane | FastAPI server, horizontally scalable |
| `StatefulSet` | 1× per DB host | CDC worker with persistent volume |
| `HPA` | 1× per worker | CPU + custom metric autoscaling |
| `PersistentVolumeClaim` | 1× per worker pod | SQLite checkpoints + fallback queue |
| `ConfigMap` | 1× per service | Non-secret config |
| `Secret` | 1× per tenant | DB credentials, Redis URL |
| `Ingress` | 1× | HTTPS to UI/API |
| `NetworkPolicy` | Multiple | Restrict inter-pod traffic |

### HPA Scaling Triggers

```yaml
metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        averageUtilization: 70
  - type: External
    external:
      metric:
        name: fusion_assigned_streams_count
      target:
        value: "8"   # scale out when > 8 streams per pod
```

### Network Isolation

Workers only allow outbound to:
- Source DB ports (3306, 5432, 27017)
- Redis (6379)
- Control plane (443)

All other traffic is denied by `NetworkPolicy`.

### Redis Cluster (Production)

```
3 Masters + 3 Replicas
  maxmemory-policy: noeviction
  appendfsync: everysec
  cluster-enabled: yes
```

Events are sharded across cluster nodes via consistent hashing of the stream key.

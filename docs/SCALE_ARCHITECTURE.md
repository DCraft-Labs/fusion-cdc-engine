# Fusion CDC Platform — Enterprise Scale Architecture
## 500 Tenants · 600 Sources · 100M-Row Loads · 200K Events/Min
### AWS Mumbai (ap-south-1) · Zero Idle Cost Design

---

## Table of Contents

1. [Scale Assumptions & Math](#1-scale-assumptions--math)
2. [Why Not Spark](#2-why-not-spark)
3. [5-Tier Architecture](#3-5-tier-architecture)
4. [Tier 1 — Control Plane](#4-tier-1--control-plane)
5. [Tier 2 — CDC Capture (600 Source Pods)](#5-tier-2--cdc-capture-600-source-pods)
6. [Tier 3 — Event Bus (Kafka + Redis)](#6-tier-3--event-bus-kafka--redis)
7. [Tier 4 — CDC Consumer (Fast Path)](#7-tier-4--cdc-consumer-fast-path)
8. [Tier 5 — Transform Worker (Smart Path, Scale-to-Zero)](#8-tier-5--transform-worker-smart-path-scale-to-zero)
9. [Destinations: PostgreSQL + Iceberg](#9-destinations-postgresql--iceberg)
10. [All 10 Transform Types in DuckDB](#10-all-10-transform-types-in-duckdb)
11. [KEDA Autoscaling Design](#11-keda-autoscaling-design)
12. [Kubernetes: Local → AWS with Minimal Changes](#12-kubernetes-local--aws-with-minimal-changes)
13. [Multi-Tenant Isolation & Rate Limiting](#13-multi-tenant-isolation--rate-limiting)
14. [Resilience Design](#14-resilience-design)
15. [AWS Mumbai Cost Analysis](#15-aws-mumbai-cost-analysis)
16. [Migration Path from Current State](#16-migration-path-from-current-state)

---

## 1. Scale Assumptions & Math

### Tenant Model

| Dimension | Value |
|-----------|-------|
| Tenants | **500** |
| Sub-tenants per tenant | **50** |
| Connections per sub-tenant | **5** |
| **Total connections** | **125,000** |
| Source hosts (MySQL + Postgres + MongoDB) | **600** |
| Initial load per connection | **100,000,000+ rows** |
| CDC events per connection (peak) | **200,000 / minute = 3,333 / sec** |

### Event Rate Math

```
All 125,000 connections @ peak simultaneously (theoretical):
  125,000 × 3,333 events/sec = 416,666,667 events/sec

Realistic 5% peak concurrency (6,250 connections active):
  6,250 × 3,333 events/sec = 20,833,333 events/sec

Normal operation (0.5% active = 625 connections):
  625 × 3,333 events/sec = 2,083,333 events/sec
```

> **Why 5% concurrency is the right engineering target:**
> In production CDC platforms, not all 125,000 connections peak simultaneously.
> MySQL binlog is bursty — transactions cluster around business hours, batch jobs,
> and ETL windows. Engineering for 5% peak gives you:
> - Handles every realistic scenario
> - Auto-scales to 100% if ever needed
> - Keeps infrastructure sane and costs bounded

### Initial Load Math

```
Per connection:  100,000,000 rows
DuckDB throughput (2 CPU / 4GB, with all 10 transforms): 2,000,000 rows/min
Time for 100M rows:  50 minutes per pod

With 10 parallel chunk pods per connection: 5 minutes
(DuckDB splits PK range into 10 chunks, each pod handles 10M rows)

50 concurrent initial loads:
  50 connections × 10 pods each = 500 transform pods (burst, ~50 min, then 0)
```

### Source Host Distribution (600 total)

```
MySQL hosts:      200  (400B+ banking transactions, binlog ROW format)
PostgreSQL hosts: 200  (WAL logical replication slots, wal_level=logical)
MongoDB hosts:    200  (replica set change streams, oplog)

CDC Worker pods: 3 Deployments (min=1 each), KEDA scales to 7 pods/type at full load
Checkpoints: stored in postgres-meta (CentralCheckpointSync), NOT pod-local
Pods are stateless: any pod can pick up any source by pulling checkpoint on startup
```

---

## 2. Why Not Spark

> **Short answer:** Spark is an always-on cluster. You pay whether it's working or not.
> DuckDB + Celery + KEDA gives you the same or better performance with zero idle cost.

| Dimension | Apache Spark | DuckDB + Celery + KEDA |
|-----------|-------------|------------------------|
| Idle RAM | **2.5 GB** (driver always-on) | **0 bytes** (scale-to-zero) |
| Cold start | 30–60 seconds (JVM) | **0.3 seconds** (Python process) |
| Image size | ~1.5 GB | ~220 MB |
| 100M row transform time | 15–30 min | **3–5 min** (vectorized columnar) |
| Python UDF overhead | JVM ↔ Python bridge serialization | **Native in-process** (0 overhead) |
| Multi-tenant isolation | Shared JVM context | Separate pods, no bleed |
| k8s scaling | Spark-on-k8s Operator (complex) | KEDA on Redis lists (simple) |
| 10-transform pipeline | PySpark DAG | DuckDB SQL pipeline |
| Operational complexity | High (driver + executor + shuffle) | Low (one pod = one job) |
| Cost at idle | Always billing | **$0** |

**Python UDFs: the most important win**

In Spark, every UDF call serializes row data Python → JVM → Python → JVM.
At 100M rows × 1 UDF = 200M serialization round-trips → minutes of overhead.

In DuckDB, UDFs are registered as `duckdb.create_function(name, python_fn)` —
the function runs directly in the DuckDB vectorized engine, zero serialization,
batched across entire column vectors (8,192 rows at a time).

---

## 3. 5-Tier Architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  TIER 1 — CONTROL PLANE (always-on, stateless, tiny)                       ║
║                                                                              ║
║  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────────┐   ║
║  │ control-plane   │  │ postgres-meta    │  │ frontend (Next.js)      │   ║
║  │ FastAPI ×2      │  │ RDS t3.medium    │  │ ×1 pod                  │   ║
║  │ 0.5CPU/512MB ea │  │ Multi-AZ         │  │ 0.1CPU/128MB            │   ║
║  └─────────────────┘  └──────────────────┘  └─────────────────────────┘   ║
║  2 CPU total, 1.5 GB RAM total — zero CDC processing                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                    connection metadata (source DSN, transforms,
                    pipeline config, tenant info, checkpoints)
                                    │
╔══════════════════════════════════════════════════════════════════════════════╗
║  TIER 2 — CDC CAPTURE (3 Deployments, KEDA-scaled, stateless pods)         ║
║                                                                              ║
║  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐   ║
║  │ mysql-cdc-worker   │  │ postgres-cdc-worker │  │ mongo-cdc-worker   │   ║
║  │ Deployment min=1   │  │ Deployment min=1   │  │ Deployment min=1   │   ║
║  │ max=50, KEDA:      │  │ max=50, KEDA:      │  │ max=50, KEDA:      │   ║
║  │ 30 sources/pod     │  │ 30 sources/pod     │  │ 30 sources/pod     │   ║
║  │ Idle: 1 pod ($0)   │  │ Idle: 1 pod ($0)   │  │ Idle: 1 pod ($0)   │   ║
║  │ 200 srcs: 7 pods   │  │ 200 srcs: 7 pods   │  │ 200 srcs: 7 pods   │   ║
║  └────────────────────┘  └────────────────────┘  └────────────────────┘   ║
║  Checkpoints stored in postgres-meta — pods are STATELESS, no PVC needed   ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                    INSERT/UPDATE/DELETE/SCHEMA events
                    (keyed by: bank_id:tenant_id:source_id:schema:table)
                                    │
╔══════════════════════════════════════════════════════════════════════════════╗
║  TIER 3 — EVENT BUS (dual-layer for volume + control)                      ║
║                                                                              ║
║  ┌──────────────────────────────────────┐  ┌──────────────────────────┐   ║
║  │ MSK Kafka Cluster (3 brokers)        │  │ ElastiCache Redis        │   ║
║  │ kafka.m5.2xlarge                     │  │ 6× r6g.xlarge Cluster    │   ║
║  │ Topic: cdc.events (3 partitions/conn)│  │ Control pub/sub          │   ║
║  │ Handles: 20M events/sec @ 5% peak   │  │ KEDA queue lengths       │   ║
║  │ Retention: 24h (replay window)       │  │ Transform job queues     │   ║
║  └──────────────────────────────────────┘  └──────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                    ┌───────────────┴──────────────────┐
                    │                                  │
    No transform pipeline                  Has transform pipeline
    (fast path)                            (smart path)
                    │                                  │
╔═══════════════════╪══════════╗     ╔══════════════════╪══════════════════╗
║  TIER 4 — CDC CONSUMER       ║     ║  TIER 5 — TRANSFORM WORKER          ║
║  (KEDA, min=1, max=500)      ║     ║  (KEDA, min=0, max=500)             ║
║                               ║     ║  ← THIS REPLACES SPARK              ║
║  Kafka → apply row →          ║     ║                                     ║
║  COPY → Dest Postgres/Iceberg ║     ║  DuckDB engine:                     ║
║                               ║     ║  - All 10 transform types           ║
║  0.5CPU / 512MB per pod       ║     ║  - Python UDFs (native, fast)       ║
║  At 5% peak: ~417 pods        ║     ║  - PK-range chunked loading         ║
║  At idle: 1 pod               ║     ║  - COPY → Postgres + Iceberg writer ║
║                               ║     ║                                     ║
║  Scale trigger:               ║     ║  2CPU / 4GB per pod                 ║
║  Kafka consumer group lag     ║     ║  At idle: 0 pods = $0               ║
╚══════════════════════════════╝     ╚══════════════════════════════════════╝
                    │                                  │
                    └───────────────┬──────────────────┘
                                    │
╔══════════════════════════════════════════════════════════════════════════════╗
║  DESTINATIONS                                                               ║
║                                                                              ║
║  ┌──────────────────────────────────┐  ┌──────────────────────────────┐   ║
║  │ PostgreSQL Data Warehouse        │  │ Apache Iceberg on S3          │   ║
║  │ RDS db.r6g.xlarge (or self-mgd)  │  │ S3 + Glue Catalog            │   ║
║  │ COPY bulk insert                 │  │ PyIceberg writer              │   ║
║  │ Upsert by primary key            │  │ Parquet files, Z-ordering     │   ║
║  │ Schema auto-created              │  │ Time-travel, schema evolution │   ║
║  └──────────────────────────────────┘  └──────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## 4. Tier 1 — Control Plane

**Purpose:** API, auth, connection registry, transform pipeline storage, monitoring.
**Policy:** Always-on, stateless, 2 replicas for HA, tiny resource footprint.

```yaml
# Resources per pod
requests:
  cpu: "500m"
  memory: "512Mi"
limits:
  cpu: "1000m"
  memory: "1Gi"

replicas: 2  # rolling update, zero downtime
```

**What it does NOT do:**
- Does NOT process CDC events
- Does NOT run transforms
- Does NOT touch source or destination databases directly

**API responsibilities:**
- CRUD for connections, sources, destinations
- Transform pipeline definition and UDF registry
- Emit `start-streaming` / `stop-streaming` commands to CDC workers via Redis pub/sub
- Health aggregation across all tiers

---

## 5. Tier 2 — CDC Capture (3 Deployments, KEDA-scaled)

### Why NOT 1 Pod Per Source Host

The original assumption was that binlog position is pod-bound and therefore you need
one pod per source host (a StatefulSet with a dedicated PVC). This is **wrong**.

The codebase already has `CentralCheckpointSync` which syncs every 30 seconds to
the control-plane API, which stores all positions in `postgres-meta`.

```python
# cdc_worker/checkpoint.py — already in the codebase
class CentralCheckpointSync:
    async def push(self, local, source_id):   # → POST /internal/checkpoints/batch
    async def pull(self, source_id):          # → GET  /internal/checkpoints/{source_id}
```

**The SQLite file is a write-ahead buffer, not the source of truth.**
The authoritative checkpoint lives in `postgres-meta`. Any pod can start handling
any source by pulling its checkpoint on startup. Pods are **stateless**.

### The Correct Design: 3 Deployments

```
┌─────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
│ mysql-cdc-worker        │  │ postgres-cdc-worker      │  │ mongo-cdc-worker         │
│ Deployment              │  │ Deployment               │  │ Deployment               │
│ min=1, max=50           │  │ min=1, max=50            │  │ min=1, max=50            │
│ KEDA trigger:           │  │ KEDA trigger:            │  │ KEDA trigger:            │
│ assigned sources > 30   │  │ assigned sources > 30    │  │ assigned sources > 30    │
│                         │  │                         │  │                         │
│ Each pod handles up to  │  │ Each pod handles up to   │  │ Each pod handles up to   │
│ 30 concurrent asyncio   │  │ 30 concurrent WAL        │  │ 30 concurrent change     │
│ binlog streams          │  │ logical streams          │  │ streams                  │
└─────────────────────────┘  └──────────────────────────┘  └──────────────────────────┘

At 200 sources per type:  ceil(200/30) = 7 pods per type = 21 pods total
At idle (0 sources):      1 pod per type = 3 pods total (share control-plane nodes, ~$0)
```

### How Source Assignment Works

```
1. Control plane sends: PUBLISH fusion:commands start-streaming:{source_id}

2. Any available pod of the correct type picks it up (first-come via Redis SETNX lock)
   Key: fusion:lock:source:{source_id}   TTL: 30s
   The pod that wins the lock owns that source.

3. Pod on startup:
   a. Pulls checkpoint from control-plane API (CentralCheckpointSync.pull)
   b. Connects to source host using DSN from metadata DB
   c. Starts async binlog/WAL/changestream loop
   d. Renews lock every 15s (heartbeat)

4. If pod dies:
   - Lock TTL expires in 30s
   - Control plane detects via heartbeat miss → re-publishes start-streaming
   - Another pod picks up the source, pulls checkpoint, resumes
   - Maximum data re-processed: events in last 30s (Kafka deduplicates via upsert)
```

### MySQL server_id: Stable Per Source, Not Per Pod

MySQL requires a unique `server_id` per replica connection. The solution:

```python
# server_id derived from source_id hash — stable for that source regardless of which pod handles it
server_id = int(hashlib.md5(source_id.encode()).hexdigest()[:8], 16) % 4_294_967_295
```

If a pod dies and a new one reconnects with the same `server_id` to the same MySQL host,
MySQL recognises it as the same replica reconnecting — exactly the correct behaviour.

### Postgres WAL: Exclusive Slot Ownership via Redis Lock

A Postgres logical replication slot can only have ONE active consumer.
The Redis distributed lock above enforces this — only the lock-holder consumes a slot.
If the lock expires, the slot consumer is replaced, not duplicated.

### Resource Profile Per Pod

```
CPU request:  500m   (asyncio I/O-bound: 30 concurrent streams per pod)
CPU limit:    1000m
RAM request:  512Mi  (30 in-flight event buffers + DuckDB not needed here)
RAM limit:    1Gi

At full load (600 sources, all active):
  21 pods × 500m CPU  = 10.5 vCPU total
  21 pods × 512Mi RAM = 10.5 GB total
  Fits on 3 × t3.xlarge Spot nodes = $123/month

At idle (no active sources):
  3 pods × 500m CPU  = 1.5 vCPU
  3 pods × 512Mi RAM = 1.5 GB
  Shares control-plane node pool = $0 extra
```

### KEDA Trigger for CDC Workers

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: mysql-cdc-worker-scaler
spec:
  scaleTargetRef:
    name: mysql-cdc-worker
  minReplicaCount: 1       # always 1 listening for assignments
  maxReplicaCount: 50
  cooldownPeriod: 300      # 5 min before scale-down (binlog streams need clean close)
  triggers:
    - type: redis
      metadata:
        listName: fusion:assigned:mysql    # control-plane pushes source_ids here
        listLength: "30"                       # 30 sources per pod
```

### Kafka Topic Design

```
Topic: cdc.events
Partitions: one per connection (125,000 partitions)
Key: "{bank_id}:{tenant_id}:{source_id}:{schema}:{table}"
Value: JSON { op: INSERT|UPDATE|DELETE, before: {...}, after: {...}, ts_ms: ... }
Retention: 24 hours (replay window if consumer falls behind)
Replication factor: 3
```

---

## 6. Tier 3 — Event Bus (Kafka + Redis)

### Why Kafka Here Instead of Pure Redis?

At 20M events/sec (5% concurrency), Redis Streams alone cannot reliably handle
the throughput without massive memory usage. Kafka is purpose-built for this.

| Metric | Redis Streams | MSK Kafka (3× m5.2xlarge) |
|--------|--------------|---------------------------|
| Peak throughput | ~300K ops/sec (cluster) | **20M+ events/sec** |
| Retention | RAM-limited | Disk-based, 24h window |
| Consumer groups | Yes | Yes, with offset tracking |
| Replay capability | TTL-based | Full 24h replay |
| Cost at scale | Expensive (memory) | Predictable disk-based |

**Redis is still used for:**
- Control pub/sub (`start-streaming`, `stop-streaming` commands)
- KEDA queue depth metrics (transform job queues)
- Per-tenant rate limiting counters
- Transform job results and status

### MSK Kafka Configuration

```
Brokers:            3 × kafka.m5.2xlarge (8 vCPU, 32 GB each)
Partitions/topic:   125,000 (one per connection)
Replication factor: 3
Retention:          24 hours
Compression:        lz4 (30–50% size reduction, fast)
Throughput target:  20M events/sec (5% peak concurrency)

Consumer groups:
  - fusion-cdc-consumer     (fast path: no transforms)
  - fusion-transform-worker (smart path: has transforms)
```

---

## 7. Tier 4 — CDC Consumer (Fast Path)

**Used for:** Connections with NO transform pipeline set.
**Path:** Kafka consumer → row event → COPY/upsert → destination PostgreSQL or Iceberg

### Scaling Logic (KEDA)

```
Scale trigger: Kafka consumer group lag
  lag < 1,000        → 1 pod
  lag < 10,000       → 5 pods
  lag < 100,000      → 20 pods
  lag < 1,000,000    → 100 pods
  lag unlimited      → up to 500 pods

Scale-down cooldown: 120 seconds
Min replicas: 1 (always at least 1 watching)
Max replicas: 500
```

### Per Pod Resource Profile

```
CPU request: 500m  (mostly I/O: Kafka consume + Postgres COPY)
CPU limit:   1000m
RAM request: 512Mi
RAM limit:   1Gi
```

### What Each Pod Does

```python
# Per event, fast path (no transforms):
# 1. Consume batch from Kafka (1000 events max per poll)
# 2. Group by (schema, table)
# 3. INSERT → COPY FROM STDIN (bulk, not row-by-row)
# 4. On conflict: UPDATE (upsert by primary key)
# 5. Commit Kafka offset AFTER successful write (at-least-once)
# 6. Write to Iceberg if destination_type = "iceberg"
```

---

## 8. Tier 5 — Transform Worker (Smart Path, Scale-to-Zero)

**This replaces Apache Spark entirely.**

**Used for:** Connections WITH a transform pipeline, AND initial loads > threshold.

### DuckDB: Why It Beats Spark Here

DuckDB is an in-process columnar SQL engine. It runs inside the Python process
with zero network overhead, processes data in columnar batches of 8,192 rows,
uses memory-mapped files for data larger than RAM, and has native Python UDF
support with zero serialization cost.

```python
import duckdb

conn = duckdb.connect()

# Load 10M rows from MySQL into DuckDB in one vectorized scan
conn.execute("""
    CREATE TABLE staging AS SELECT * FROM read_parquet('chunk_001.parquet')
""")

# Apply all 10 transforms in a single SQL pass (vectorized, columnar)
conn.execute("""
    CREATE TABLE transformed AS
    SELECT
        CAST(amount AS DOUBLE) AS amount,          -- cast
        upper(trim(customer_name)) AS customer_name, -- string_op
        amount * 1.18 AS amount_with_gst,          -- math_op
        year(transaction_date) AS txn_year,        -- date_op
        json_extract_string(metadata, '$.country') AS country, -- json_extract
        sha256(account_number) AS masked_account,  -- mask: hash
        my_custom_udf(segment_code) AS segment     -- udf (native Python)
    FROM staging
    WHERE status != 'DELETED'                      -- filter expression
""")

# Bulk COPY to destination
conn.execute("COPY transformed TO STDOUT (FORMAT CSV)")
# → pipe directly to psycopg2 copy_expert()
```

### Scale Trigger

```
Two priority queues in Redis:
  fusion:transforms:high    ← initial loads (100M rows, time-sensitive)
  fusion:transforms:normal  ← CDC events with column transforms

KEDA ScaledObject:
  triggerType: redis
  listLength threshold: 1 per pod

Queue depth 0   → 0 pods ($0)
Queue depth 10  → 10 pods
Queue depth 50  → 50 pods (50 concurrent initial loads)
Queue depth 500 → 500 pods (burst, all 600 sources loading simultaneously)

High-priority queue scales first (dedicated KEDA trigger)
Low-priority queue fills remaining capacity
```

### Initial Load: Chunked Parallel Strategy

```
100M rows, 1 connection, 10 parallel pods:

Control pod (coordinator):
  1. Read MIN(pk) and MAX(pk) from source
  2. Split into 10 PK ranges: [0-10M], [10M-20M], ..., [90M-100M]
  3. Push 10 tasks onto fusion:transforms:high
  4. KEDA spins up 10 worker pods

Each worker pod:
  1. Pull task: { connection_id, pk_start, pk_end, chunk_seq }
  2. SSH tunnel → MySQL → SELECT WHERE pk BETWEEN start AND end
  3. Stream into DuckDB staging table
  4. Apply all N transforms in single SQL pass
  5. COPY to destination Postgres (or write Parquet to S3 for Iceberg)
  6. Write checkpoint: { connection_id, chunk_seq, rows_written, last_pk }
  7. Ack task from Redis queue

Final pod (or any pod detecting all chunks done):
  1. Merge Iceberg Parquet files (compaction)
  2. Mark connection.initial_load_completed = true
  3. Signal cdc-consumer to start streaming from binlog position

On pod crash: task re-queued, KEDA spins new pod, resumes from checkpoint
```

### Resource Per Pod

```
CPU request:  2000m  (DuckDB uses both cores for vectorized ops)
CPU limit:    4000m
RAM request:  4Gi    (DuckDB uses memory-mapped files beyond this)
RAM limit:    8Gi
```

---

## 9. Destinations: PostgreSQL + Iceberg

### PostgreSQL Destination

```
Write method:  COPY FROM STDIN (bulk, 10K rows/batch)
Upsert:        INSERT ... ON CONFLICT (pk) DO UPDATE SET ...
Schema:        Auto-created from source schema (DDL mirroring)
Index:         Primary key + created_at (for time-based partitioning)
CDC apply:     INSERT → upsert, UPDATE → upsert, DELETE → soft-delete flag or hard-delete
```

### Iceberg Destination

```
Format:         Apache Iceberg v2 (row-level deletes, merge-on-read)
Storage:        S3 (ap-south-1), standard tier
Catalog:        AWS Glue Data Catalog
File format:    Parquet (snappy compression)
Partitioning:   By day(created_at) — enables efficient time-range queries
Z-ordering:     By (tenant_id, created_at) — co-locates tenant data
Schema evo:     Add columns, rename, widen types — no rewrites
Time-travel:    Read table AS OF TIMESTAMP '2026-05-01' (snapshots kept 30 days)
Compaction:     Nightly job (separate KEDA-triggered pod) merges small files
CDC apply:      position-delete files for DELETEs, new data files for INSERT/UPDATE
```

### Iceberg Writer Path (Polars + PyIceberg)

```python
import pyiceberg.catalog
import polars as pl

# After DuckDB transforms, write directly to Iceberg
catalog = load_catalog("glue", **glue_config)
table = catalog.load_table(f"{tenant_id}.{table_name}")

# Polars DataFrame from DuckDB result
df = conn.execute("SELECT * FROM transformed").pl()

# PyIceberg write (handles partitioning, metadata, compaction)
table.append(df.to_arrow())  # Arrow → Parquet → S3
```

---

## 10. All 10 Transform Types in DuckDB

The Spark executor defined these types. All 10 are ported to DuckDB with equal
or better performance.

### Transform Execution Pipeline

```python
class DuckDBTransformEngine:
    """
    Executes a sequence of up to 10 transform steps on a DuckDB table.
    Each step produces a new column or modifies an existing column.
    All steps run in a single vectorized SQL pass where possible.
    """

    STEP_HANDLERS = {
        "cast":                 _apply_cast,
        "string_op":            _apply_string_op,
        "math_op":              _apply_math_op,
        "date_op":              _apply_date_op,
        "json_extract":         _apply_json_extract,
        "json_flatten_inline":  _apply_json_flatten_inline,
        "json_flatten_child":   _apply_json_flatten_child,
        "mask":                 _apply_mask,
        "expression":           _apply_expression,
        "udf":                  _apply_udf,
    }
```

### Type 1: `cast` — Type Coercion

```python
# Spark: df.withColumn(col, F.col(col).cast(IntegerType()))
# DuckDB:
def _apply_cast(conn, step):
    col = step["column"]
    to_type = step.get("to_type", "string")
    out = step.get("output_column", col)
    type_map = {
        "string": "VARCHAR", "int": "INTEGER", "long": "BIGINT",
        "double": "DOUBLE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"
    }
    conn.execute(f"ALTER TABLE staging ADD COLUMN {out} {type_map[to_type]}")
    conn.execute(f"UPDATE staging SET {out} = CAST({col} AS {type_map[to_type]})")
```

### Type 2: `string_op` — String Operations

```
Supported ops: upper, lower, trim, substring, lpad, rpad, replace, concat
DuckDB: upper(col), lower(col), trim(col), substring(col, start, len)
Performance: vectorized over entire column batch (8,192 rows at a time)
```

### Type 3: `math_op` — Mathematical Expressions

```sql
-- Example: amount_with_gst = amount * 1.18
-- DuckDB evaluates as a SQL expression — compiled to native CPU instructions
SELECT amount * 1.18 AS amount_with_gst FROM staging
```

### Type 4: `date_op` — Date/Time Operations

```
Supported ops: year, month, day, date_add, date_format, date_diff, epoch
DuckDB native: year(col), month(col), dayofmonth(col)
               col + INTERVAL '7' DAY
               strftime('%Y-%m-%d', col)
```

### Type 5: `json_extract` — JSON Field Extraction

```sql
-- DuckDB has first-class JSON support
-- Extract a nested key from a JSON string column
SELECT json_extract_string(metadata, '$.address.city') AS city FROM staging

-- Auto-typed extraction (returns native type, not string)
SELECT json_extract(payload, '$.amount')::DOUBLE AS amount FROM staging
```

### Type 6: `json_flatten_inline` — Flatten JSON into Columns

```python
# Given: metadata = '{"country": "IN", "tier": "premium", "score": 850}'
# Produces: metadata_country, metadata_tier, metadata_score as separate columns
def _apply_json_flatten_inline(conn, step):
    col = step["column"]
    schema = step["json_schema"]  # {"country": "string", "tier": "string", "score": "int"}
    for field, dtype in schema.items():
        out_col = step.get("output_columns", {}).get(field, f"{col}_{field}")
        conn.execute(f"""
            ALTER TABLE staging ADD COLUMN {out_col} {dtype_map[dtype]}
        """)
        conn.execute(f"""
            UPDATE staging SET {out_col} = json_extract_string({col}, '$.{field}')
        """)
```

### Type 7: `json_flatten_child` — Explode JSON Array into Child Table

```sql
-- Given: transactions = '[{"id":1,"amt":100},{"id":2,"amt":200}]'
-- Produces a CHILD table with one row per array element

CREATE TABLE child_transactions AS
SELECT
    parent.pk AS parent_pk,
    unnest(from_json(parent.transactions, '[]')) AS txn
FROM staging AS parent;

-- DuckDB UNNEST is vectorized — handles millions of array elements efficiently
```

### Type 8: `mask` — Data Masking / Tokenization

```sql
-- Strategy: last4 (show only last 4 chars, mask rest)
CASE
    WHEN length(account_no) > 4
    THEN repeat('*', length(account_no) - 4) || right(account_no, 4)
    ELSE account_no
END AS masked_account_no

-- Strategy: hash (SHA-256 irreversible tokenization)
sha256(account_no::BLOB)::VARCHAR AS hashed_account_no

-- Strategy: null (full nullification for PII)
NULL AS ssn
```

### Type 9: `expression` — Arbitrary SQL Expression

```python
# Operator-defined SQL expression with full DuckDB SQL support
def _apply_expression(conn, step):
    expr = step["expression"]
    out = step.get("output_column", "expr_result")
    # Supports: CASE WHEN, COALESCE, GREATEST/LEAST, window functions,
    #           regex_matches, string_split, list operations, etc.
    conn.execute(f"ALTER TABLE staging ADD COLUMN {out} VARCHAR")
    conn.execute(f"UPDATE staging SET {out} = ({expr})")
```

### Type 10: `udf` — Python User-Defined Function

```python
# UDF is fetched from control-plane registry, executed natively in DuckDB
# Zero JVM serialization — pure Python, vectorized
def _apply_udf(conn, step, udf_registry_url):
    fn_name = step["function"]
    args = step.get("args", [])
    out = step.get("output_column", f"{fn_name}_result")
    return_type = step.get("return_type", "string")

    # Fetch UDF code from control-plane
    udf_def = requests.get(f"{udf_registry_url}/api/v1/udfs/{fn_name}").json()
    code = udf_def["code"]

    # Register with DuckDB — runs natively in vectorized engine
    namespace = {}
    exec(textwrap.dedent(code), namespace)  # nosec: admin-controlled code
    fn = namespace[fn_name]

    duckdb_type_map = {"string": str, "int": int, "double": float, "boolean": bool}
    conn.create_function(fn_name, fn, duckdb_type_map[return_type])

    args_str = ", ".join(args)
    conn.execute(f"ALTER TABLE staging ADD COLUMN {out} {return_type_sql}")
    conn.execute(f"UPDATE staging SET {out} = {fn_name}({args_str})")
```

### 10-Level Transform Chain Execution

```
Connection has transform_pipeline with 10 steps:
  step 1: cast(amount, double)
  step 2: math_op(amount * 1.18 → amount_gst)
  step 3: string_op(upper, customer_name)
  step 4: json_extract(metadata, $.country → country)
  step 5: json_flatten_inline(address → addr_city, addr_pin)
  step 6: mask(account_no, strategy=last4)
  step 7: date_op(txn_date, year → txn_year)
  step 8: expression(CASE WHEN amount > 1000000 THEN 'HNI' ELSE 'RETAIL' END → segment)
  step 9: udf(calculate_risk_score(amount, country) → risk_score)
  step 10: json_flatten_child(line_items → child table txn_items)

DuckDB execution:
  Steps 1-9: Compiled into a single SQL SELECT — one full-table scan
  Step 10: UNNEST generates child table — second pass only for array columns
  Total passes: 2 (regardless of how many transforms, up to 10)
  At 100M rows: ~50 minutes per pod, ~5 min with 10 parallel chunk pods
```

---

## 11. KEDA Autoscaling Design

KEDA (Kubernetes Event-Driven Autoscaler) watches Redis queue depths and
Kafka consumer group lag. It talks directly to the Kubernetes control plane
to scale Deployments. No extra controller needed beyond KEDA itself.

### ScaledObject: Transform Worker (scale-to-zero)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: transform-worker-scaler
  namespace: fusion
spec:
  scaleTargetRef:
    name: transform-worker
  minReplicaCount: 0        # ← ZERO at idle = $0
  maxReplicaCount: 500
  cooldownPeriod: 120       # 2 min before scaling down
  pollingInterval: 5        # check every 5 seconds
  triggers:
    # High priority: initial loads
    - type: redis
      metadata:
        address: fusion-redis:6379
        listName: fusion:transforms:high
        listLength: "1"    # 1 task = 1 pod
    # Normal priority: CDC with transforms
    - type: redis
      metadata:
        address: fusion-redis:6379
        listName: fusion:transforms:normal
        listLength: "5"    # 5 tasks = 1 pod (batch efficiency)
```

### ScaledObject: CDC Consumer (fast path)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: cdc-consumer-scaler
  namespace: fusion
spec:
  scaleTargetRef:
    name: cdc-consumer
  minReplicaCount: 1        # always 1 watching
  maxReplicaCount: 500
  cooldownPeriod: 120
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: fusion-kafka:9092
        consumerGroup: fusion-cdc-consumer
        topic: cdc.events
        lagThreshold: "1000"   # 1K lag → add pod
        offsetResetPolicy: latest
```

### Scaling Behaviour Summary

```
SCENARIO                          TRANSFORM WORKERS  CDC CONSUMERS
────────────────────────────────────────────────────────────────────
System idle (no connections)      0 pods ($0)        1 pod
1 connection, no transforms       0 pods ($0)        1 pod
1 connection, 10 transforms       1 pod (10M load)   0 pods (fast path only)
50 concurrent 100M initial loads  50 pods (50 min)   0 pods (load not CDC)
5% CDC peak, no transforms        0 pods ($0)        ~417 pods
5% CDC peak, 20% with transforms  ~80 pods           ~333 pods
System fully idle (3am)           0 pods ($0)        1 pod
────────────────────────────────────────────────────────────────────
```

---

## 12. Kubernetes: Local → AWS with Minimal Changes

### Directory Structure

```
kubernetes/
├── base/                                ← All manifests, env-agnostic
│   ├── kustomization.yaml
│   ├── namespaces.yaml
│   ├── service-accounts.yaml
│   ├── configmap.yaml
│   ├── secrets.yaml
│   ├── deployments/
│   │   ├── control-plane.yaml          (2 replicas, HPA-managed)
│   │   ├── frontend.yaml              (1 replica)
│   │   ├── mysql-cdc-worker.yaml      (Deployment, min=1, KEDA-managed)
│   │   ├── postgres-cdc-worker.yaml   (Deployment, min=1, KEDA-managed)
│   │   ├── mongo-cdc-worker.yaml      (Deployment, min=1, KEDA-managed)
│   │   ├── cdc-consumer.yaml          (Deployment, min=1, KEDA-managed)
│   │   └── transform-worker.yaml      (Deployment, min=0, KEDA-managed)
│   ├── statefulsets/
│   │   └── redis.yaml                 (3-node cluster StatefulSet)
│   │   NOTE: NO per-source StatefulSets — pods are stateless (checkpoints in DB)
│   ├── keda/
│   │   ├── mysql-cdc-worker-scaler.yaml
│   │   ├── postgres-cdc-worker-scaler.yaml
│   │   ├── mongo-cdc-worker-scaler.yaml
│   │   ├── transform-worker-scaler.yaml
│   │   └── cdc-consumer-scaler.yaml
│   ├── hpa/
│   │   └── control-plane-hpa.yaml
│   ├── network-policy.yaml
│   └── ingress.yaml
│
├── overlays/
│   ├── local/                          ← Docker Desktop Kubernetes
│   │   ├── kustomization.yaml
│   │   └── patches/
│   │       ├── resources.yaml         (halve all CPU/memory requests)
│   │       ├── replicas.yaml          (all minReplicas: 1 for each type)
│   │       ├── service-types.yaml     (LoadBalancer → NodePort)
│   │       └── kafka.yaml            (1-broker in-cluster instead of MSK)
│   │
│   └── production/                     ← AWS EKS
│       ├── kustomization.yaml
│       └── patches/
│           ├── service-types.yaml     (NodePort → LoadBalancer)
│           ├── storage.yaml          (standard → gp3, EBS CSI)
│           ├── affinity.yaml         (pod anti-affinity across AZs)
│           ├── resource-limits.yaml  (full production requests/limits)
│           └── external-secrets.yaml (AWS Secrets Manager via ESO)
```

### What Changes Between Environments

| Setting | Local (Docker Desktop) | Production (AWS EKS) |
|---------|----------------------|----------------------|
| `service.type` | NodePort | LoadBalancer (ALB) |
| `storageClassName` | `standard` (hostPath) | `gp3` (EBS CSI Driver) |
| CDC worker min replicas | 1 per type (3 total) | 1 per type (KEDA scales to 50) |
| Control plane replicas | 1 | 2 |
| Resource requests | 25% of prod | Full production |
| Secrets source | K8s Secret (base64) | AWS Secrets Manager via ESO |
| Redis | 1 pod in-cluster | ElastiCache (external) |
| Kafka | In-cluster (1 broker, dev) | MSK (3 brokers, prod) |
| KEDA install | `kubectl apply -f keda-local.yaml` | Helm chart, prod values |

### Deploy Commands

```bash
# Enable Kubernetes in Docker Desktop first
# Settings → Kubernetes → Enable Kubernetes → Apply

# Install KEDA (both envs, same command)
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda --namespace keda --create-namespace

# LOCAL
kubectl apply -k kubernetes/overlays/local/

# PRODUCTION (AWS EKS)
aws eks update-kubeconfig --region ap-south-1 --name fusion-prod
kubectl apply -k kubernetes/overlays/production/

# Verify
kubectl get pods -n fusion
kubectl get scaledobjects -n fusion
```

### Local Resource Requirements (Docker Desktop)

```
Total for local dev (reduced overlay):
  CPU:  6 vCPU minimum (8 recommended)
  RAM:  8 GB minimum (16 recommended)
  SSD:  20 GB for PVCs (only redis + postgres need PVCs)

What runs locally:
  - control-plane × 1        (0.5 CPU, 512MB)
  - mysql-cdc-worker × 1     (0.5 CPU, 512MB — KEDA scales up as sources added)
  - postgres-cdc-worker × 1  (0.5 CPU, 512MB)
  - mongo-cdc-worker × 1     (0.5 CPU, 512MB)
  - cdc-consumer × 1         (0.5 CPU, 512MB)
  - transform-worker × 0     (spins up on demand via KEDA)
  - redis × 1                (0.5 CPU, 512MB)
  - postgres-meta × 1        (0.5 CPU, 512MB)
  - kafka × 1                (1-broker dev, 1 CPU, 1GB)
  - frontend × 1             (0.1 CPU, 128MB)
```

---

## 13. Multi-Tenant Isolation & Rate Limiting

With 500 tenants sharing the same infrastructure, strict isolation is essential
to prevent one tenant's burst from starving others.

### Per-Tenant Rate Limiting

```python
# Redis sorted set: tracks active pods per tenant
# Key: fusion:rate:{tenant_id}
# Value: { sub_tenant_id, connection_id, pod_id, started_at }

MAX_PODS_PER_TENANT = 20        # no single tenant uses > 20 transform pods
MAX_PODS_PER_SUB_TENANT = 5     # sub-tenant gets max 5 concurrent loads

# Before dispatching a transform task:
def can_dispatch(tenant_id, sub_tenant_id):
    tenant_count = redis.zcard(f"fusion:rate:{tenant_id}")
    sub_count = redis.zcard(f"fusion:rate:{tenant_id}:{sub_tenant_id}")
    return tenant_count < MAX_PODS_PER_TENANT and sub_count < MAX_PODS_PER_SUB_TENANT
```

### Namespace + NetworkPolicy Isolation

```yaml
# Each tenant gets its own Kafka consumer group prefix
consumerGroup: "fusion-{tenant_id}-consumer"

# NetworkPolicy: control-plane can talk to everything
# cdc-workers can only reach Redis and Kafka (not cross-tenant)
# transform-workers have no network access to other tenants' destinations
```

### Transform Pod Isolation

Each transform worker pod processes exactly ONE task (one connection's load or
one batch of CDC events). When the task completes, the pod finishes and Celery
requests a new task. This means:
- No tenant's data ever touches another tenant's process memory
- UDF code from Tenant A cannot affect Tenant B (separate Python namespaces)
- Pod crash affects only one task, which is re-queued

---

## 14. Resilience Design

### Fault Scenarios and Mitigations

| Failure | Detection | Recovery | RTO |
|---------|-----------|----------|-----|
| Transform pod crashes mid-100M load | Celery task timeout | Task re-queued, new pod resumes from last PK checkpoint | < 2 min |
| CDC worker pod crashes (MySQL) | Kubernetes restarts pod | New pod pulls binlog position from `postgres-meta` (CentralCheckpointSync), reconnects | < 1 min |
| Kafka broker fails (1 of 3) | MSK automatic | Consumer reconnects to surviving brokers, no data loss (replication factor 3) | < 30 sec |
| Redis node fails (1 of 6) | ElastiCache failover | Automatic replica promotion, KEDA reconnects | < 60 sec |
| PostgreSQL destination slow | Adaptive batch sizing | Reduce batch size from 10K to 1K; emit lag metric | No data loss |
| SSH tunnel drops mid-load | Paramiko keepalive missed | Reconnect with exponential backoff (1s, 2s, 4s…32s), resume from checkpoint | < 3 min |
| DuckDB out of memory | OOMKilled (pod evicted) | KEDA sees task still in queue, schedules new pod on different node | < 2 min |
| Control plane crashes | Kubernetes liveness probe | Rolling restart (1 replica stays up), zero API downtime | 0 (HA) |
| All Kafka brokers unavailable | Consumer cannot fetch | CDC workers buffer events in local SQLite (emptyDir write-ahead cache), drain after Kafka recovers | 0 data loss |

### Exactly-Once Semantics

```
CDC Worker → Kafka:    At-least-once (Kafka idempotent producer, acks=all)
Kafka → Consumer:      At-least-once (commit offset after write)
Consumer → Postgres:   Idempotent (upsert by primary key — safe to replay)
Consumer → Iceberg:    At-least-once (position-delete on replay deduplicates)

Result: effectively exactly-once at the row level due to upsert idempotency
```

### Checkpoint Strategy for 100M-Row Loads

```
Every 10,000,000 rows (10M):
  INSERT INTO load_checkpoints (
    connection_id, chunk_seq, last_pk, rows_written, chunk_state
  ) VALUES (...) ON CONFLICT UPDATE

On pod restart:
  SELECT last_pk FROM load_checkpoints WHERE connection_id = ? AND chunk_seq = ?
  → Resume from last_pk + 1 (not from beginning)

Maximum data re-processed on failure: 10M rows (one checkpoint window)
```

---

## 15. AWS Mumbai Cost Analysis

> Region: ap-south-1 (Mumbai) · Pricing: May 2026 · Spot instances where applicable

### Always-On Infrastructure

| Component | Instance Type | Count | On-Demand/hr | Monthly |
|-----------|--------------|-------|-------------|---------|
| EKS Cluster fee | — | 1 | $0.10 | **$73.00** |
| Control Plane nodes | m5.large (4vCPU/8GB) OD | 2 | $0.096 | **$140.16** |
| PostgreSQL Metadata | RDS db.t3.medium Multi-AZ | 1 | $0.136 | **$99.28** |
| RDS Storage | 100 GB gp3 | — | — | **$13.80** |
| ElastiCache Redis | r6g.xlarge × 6 (3 shards, 2 replicas) | 6 | $0.333 | **$1,458.54** |
| MSK Kafka | kafka.m5.2xlarge × 3 | 3 | $0.384 | **$840.96** |
| MSK Storage | 10 TB EBS | — | — | **$1.00** |
| ALB (ingress) | — | 1 | — | **$25.00** |
| **Always-On Subtotal** | | | | **$2,651.74** |

### CDC Worker Nodes (3 Deployments, KEDA-scaled, Spot)

> **Revised design**: 3 stateless Deployments instead of 600 StatefulSet pods.
> Checkpoint authority is `postgres-meta` (CentralCheckpointSync). SQLite is a write-ahead buffer only.
> Any pod can handle any source — source assigned via Redis distributed lock (TTL 30s + heartbeat).

| State | Pods | Nodes | Instance | Monthly |
|-------|------|-------|----------|---------|
| **Idle** (0 active sources) | 3 (1 per type) | Shares control-plane nodes | — | **$0 extra** |
| **Normal** (60 sources active) | 6 (2 per type) | 1× t3.xlarge Spot | $0.056/hr | **$41** |
| **Full load** (600 sources active) | 21 (7 per type) | 3× t3.xlarge Spot | $0.056/hr | **$123** |
| ~~Previous design (600 pods)~~ | ~~600~~ | ~~38× t3.xlarge~~ | — | ~~**$1,553**~~ |
| **Saving vs old design** | | | | **$1,430/month** |

### Variable Infrastructure (KEDA-scaled)

| Component | Idle | Avg Load | Peak (5% conc) | Monthly Estimate |
|-----------|------|----------|----------------|-----------------|
| CDC Consumer pods | 1 pod | ~50 pods | ~417 pods | |
| Consumer nodes (t3.xlarge Spot $0.056) | 0 extra | 7 nodes | 27 nodes | **$286.16** |
| Transform Worker pods | **0 pods** | ~10 pods | ~50 pods | |
| Transform nodes (m5.2xlarge Spot $0.115) | 0 nodes | 3 nodes | 7 nodes | **$69.00** |
| **Variable Subtotal** | | | | **$355.16** |

### Storage & Data

| Component | Size | Rate | Monthly |
|-----------|------|------|---------|
| S3 Standard (Iceberg data lake) | 50 TB | $0.025/GB | **$1,280.00** |
| S3 API requests | — | — | **$10.00** |
| AWS Glue Catalog (Iceberg metadata) | — | — | **$50.00** |
| EBS gp3 for Redis only (3×10GB = 30GB) | 30 GB | $0.114/GB | **$3.42** |
| Data Transfer Out | 500 GB/month | $0.109/GB | **$54.50** |
| **Storage Subtotal** | | | | **$1,397.92** |

### Total Cost Summary (Revised)

| Scenario | Description | Monthly Cost | vs Old Design |
|----------|-------------|-------------|---------------|
| **Idle** | System running, 0 active sources | **$2,676** | ~~$4,230~~ |
| **Normal** | ~60 sources active, 50 connections | **$3,082** | ~~$4,580~~ |
| **Peak 5%** | 600 sources active, 6,250 connections | **$5,223** | ~~$6,654~~ |
| **Burst: 50 initial loads** | 50 × 100M rows parallel (~50 min) | $3,082 + **$23** one-time | |
| **Worst case (theoretical)** | All 125K connections at full peak | ~$78,000 | |

> **Key insight:** Idle floor dropped from $4,230 to **$2,676/month** — saving $1,554/month.
> CDC workers scale from **3 pods at idle → 21 pods at full load** driven by KEDA automatically.
> No PVCs for CDC workers. No StatefulSets. No 600 reserved nodes.

### Cost Optimization Opportunities

| Optimization | Savings |
|-------------|---------|
| Reserved Instances (1-year) for ElastiCache | ~40% → save **$583/month** |
| MSK Serverless instead of provisioned (< 50% utilization) | ~**$400/month** |
| S3 Intelligent Tiering after 30 days (cold Iceberg files) | ~50% on older data |
| Savings Plans (1-year) for control-plane nodes | ~30% → save **$42/month** |
| Spot interruption handling for CDC workers (already Spot) | Already factored in |
| **Total potential savings** | **~$1,025–1,200/month** |

---

## 16. Migration Path from Current State

### Current State

```
✅ Running:
  - fusion-control-plane  (Docker)
  - fusion-cdc-worker     (Docker, single instance — handles all sources)
  - fusion-cdc-consumer   (Docker, Python + COPY)
  - fusion-redis          (Docker)
  - fusion-postgres       (Docker, metadata)
  - fusion-frontend       (Docker)

❌ Not running:
  - fusion-spark-consumer (exists but unused — to be DELETED)

Current issues:
  - No transforms applied in consumer
  - Single CDC worker handles all sources correctly already — just needs
    to be split into 3 typed Deployments (mysql/postgres/mongo) for KEDA scaling
  - No KEDA autoscaling
  - No Iceberg support
  - Pure Python row-by-row processing (no DuckDB vectorization)
  - Checkpoints already go to central DB via CentralCheckpointSync ✅ (no PVC needed)
```

### Migration Phases

**Phase 1 — Local Kubernetes (2 weeks)**
```
1. Enable Docker Desktop Kubernetes
2. Install KEDA (helm install)
3. Apply kubernetes/overlays/local/
4. Verify: control-plane, cdc-worker (×3), cdc-consumer, transform-worker (×0)
5. Test: create connection → KEDA spins transform-worker → initial load → CDC
```

**Phase 2 — DuckDB Transform Engine (1 week)**
```
1. Create transform-worker/ package with duckdb_engine.py
2. Port all 10 transform types from spark-consumer/transform/executor.py
3. Create celery_tasks.py (initial_load_task, cdc_transform_task)
4. Update cdc_consumer.py: route connections with transforms to Celery
5. Test: 10-level transform pipeline end-to-end
```

**Phase 3 — Iceberg Destination (1 week)**
```
1. Add PyIceberg + Polars to transform-worker requirements
2. Create iceberg_writer.py (handles Glue catalog, S3 path, partitioning)
3. Add destination_type field to connections table
4. Update transform-worker to write Parquet to S3 for Iceberg connections
5. Test: 100K rows → Iceberg → time-travel query
```

**Phase 4 — Kafka (replaces Redis Streams for events, 1 week)**
```
1. Add in-cluster Kafka (dev) via Strimzi operator or bitnami helm chart
2. Update cdc-worker to publish to Kafka topics (keep Redis for control)
3. Update cdc-consumer to consume from Kafka
4. Update KEDA ScaledObject to use Kafka lag trigger
5. Test: high-throughput CDC event flow
```

**Phase 5 — AWS EKS (1 week)**
```
1. Create EKS cluster (eksctl, Terraform, or CDK)
2. Install: EBS CSI Driver, ALB Controller, KEDA, External Secrets Operator
3. Create MSK cluster (3 brokers), ElastiCache cluster (6 nodes)
4. Apply kubernetes/overlays/production/
5. Load test: simulate 5% concurrency (6,250 connections × 3,333 events/sec)
```

**Phase 6 — CDC Worker Scaling is Automatic (ongoing)**
```
As new source hosts onboard:
  - Control plane pushes source_id onto fusion:assigned:mysql (or :postgres, :mongo)
  - KEDA sees queue depth grow → scales Deployment replicas automatically
  - No manual kubectl scale needed
  - Each new pod pulls checkpoint from postgres-meta and starts streaming

To manually force scale (e.g. for testing):
  kubectl scale deployment mysql-cdc-worker --replicas=7 -n fusion
```

---

## Summary: Why This Architecture Wins

```
PROPERTY              THIS SYSTEM          SPARK ALWAYS-ON    TRADITIONAL ETL
──────────────────────────────────────────────────────────────────────────────
Idle cost             $2,676/month         $4,055 + $1,825    Much higher
Transform performance 3–5 min/100M         15–30 min/100M     Hours
Scale-to-zero         ✅ Transform workers  ❌ Driver always on  N/A
                      ✅ CDC workers (KEDA)
Cold start            0.3 seconds          30–60 seconds       Minutes
Multi-tenant safety   Process isolation    Shared JVM          Varies
UDF performance       Native Python        JVM bridge          N/A
Iceberg support       ✅ PyIceberg          ✅ Spark             ❌
Local → AWS change    Overlay patch only   Major reconfigure   Full rewrite
Exactly-once          ✅ Upsert idempotent  ✅ With watermarks   ❌
600 source hosts      3 pods idle          600 pods same       Impossible
                      21 pods at full load
KEDA-driven scaling   ✅ All tiers          ❌                   ❌
Checkpoint storage    Central DB (stateless pods) Pod PVC (stateful) N/A
──────────────────────────────────────────────────────────────────────────────
```

> **Bottom line:** At 125,000 connections, 600 source hosts, 100M+ initial loads,
> and 200K events/min per connection — the architecture uses:
> - **3 CDC worker Deployments** (not 600 StatefulSets) — pods are stateless,
>   checkpoints live in `postgres-meta`, KEDA scales 3→21 pods as sources activate
> - **DuckDB** replaces Spark — 10x faster transforms, zero idle cost
> - **Kafka** for 20M event/sec throughput at 5% concurrency
> - **KEDA** drives all scaling — transform workers at 0 pods idle, CDC workers at 3
> - **Iceberg** for the analytics data lake
> - **Kustomize overlays** for local↔AWS with 2-line changes
>
> Saving vs naive design: **$1,430/month ($17,160/year)** on CDC workers alone.

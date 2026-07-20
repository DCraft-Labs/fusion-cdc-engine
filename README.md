# Fusion CDC Engine

## Complete Multi-Tenant Change Data Capture Platform

---

## Table of Contents

1. [Introduction](#introduction)
2. [System Overview](#system-overview)
3. [Key Features](#key-features)
4. [Architecture](#architecture)
5. [Tenancy Model](#tenancy-model)
6. [Source & Destination Configuration](#source--destination-configuration)
7. [Connections & Streams](#connections--streams)
8. [Transformations & JSON Handling](#transformations--json-handling)
9. [Schema Evolution](#schema-evolution)
10. [Data Quality & Metrics](#data-quality--metrics)
11. [Deployment](#deployment)
12. [Operations & Security](#operations--security)
13. [Getting Started](#getting-started)
14. [Implementation Roadmap](#implementation-roadmap)

---

## Introduction

Modern data-driven organisations can no longer rely on periodic batch jobs to move data from operational systems into analytics platforms. Customers expect **real-time metrics**, enterprises run automated workflows off of database events, and compliance rules require that deletions and updates flow end-to-end. At the same time, the shift to software-as-a-service (SaaS) means that **multi-tenant systems** must safely isolate customer workloads without creating operational sprawl.

**Fusion CDC Engine** addresses these needs by providing a **multi-tenant, change-data-capture (CDC) based data integration platform**. It is designed to capture database changes from **MySQL, PostgreSQL and MongoDB** in real time, transform those changes in-flight, and deliver them into analytics destinations such as **Postgres data warehouses** and **Iceberg tables** on object storage.

The platform is built for environments where a single **"parent" entity** (for example, a bank) hosts **multiple tenants** (the bank's customers). Each tenant may have its own databases, schemas and tables. Fusion CDC Engine allows tenants to onboard themselves through an intuitive user interface, configure sources and destinations, choose a sync mode (real-time or scheduled), define rich transformations and data quality checks, and monitor health and metrics via Prometheus.

Under the hood, a horizontally scalable **control plane** manages metadata and orchestrates workers while a **data plane** performs the actual log capture and streaming.

---

## System Overview

Fusion CDC Engine consists of two primary architectural layers:

### Control Plane
- **UI & API Gateway**: Serves the web UI and exposes REST/GraphQL APIs for programmatic access
- **Metadata Service**: Stores tenants, sources, destinations, connections, streams, transformation specs and DQ policies in a central Postgres database
- **Scheduler & Orchestrator**: Generates and schedules Airflow DAGs for scheduled syncs and manages worker assignment
- **Metrics & Alert Manager**: Aggregates Prometheus metrics, applies thresholds and sends alerts
- **Tenant Registry**: In-memory catalogue of active tenants and their assigned resources

### Data Plane
- **DB Host Workers**: Long-running processes that read change logs from physical database servers (MySQL binlog, PostgreSQL WAL, MongoDB change streams)
- **Redis Streams**: Primary transport mechanism with keys formatted as `cdc:<bank_id>:<tenant_id>:<source_id>:<schema>:<table>`
- **Spark CDC Consumer**: Structured Streaming jobs (realtime) or batch jobs (scheduled) that apply transformations, DQ checks and write to destinations
- **Checkpoint Manager**: Local SQLite + Central Postgres checkpointing for fault tolerance
- **Fallback Queue**: Local disk queue for events when Redis is unavailable
- **Prometheus Exporter**: Exposes detailed metrics via `/metrics` endpoints

### Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              CONTROL PLANE                                        │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────┐  ┌──────────────┐│
│  │   UI & API     │  │    Metadata      │  │   Scheduler &  │  │   Metrics &  ││
│  │    Gateway     │──│     Service      │──│  Orchestrator  │──│    Alerts    ││
│  │ (REST/GraphQL) │  │   (Postgres)     │  │   (Airflow)    │  │ (Prometheus) ││
│  └────────────────┘  └──────────────────┘  └────────────────┘  └──────────────┘│
│                              │                       │                           │
└──────────────────────────────┼───────────────────────┼───────────────────────────┘
                               │                       │
                               │   Assignment          │   Metrics
                               │   & Config            │   Collection
                               ▼                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                               DATA PLANE                                          │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        DB HOST WORKERS                                   │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │    │
│  │  │    MySQL     │  │  PostgreSQL  │  │   MongoDB    │                  │    │
│  │  │    Binlog    │  │     WAL      │  │    Change    │                  │    │
│  │  │   Connector  │  │  Connector   │  │   Streams    │                  │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                  │    │
│  │         │                 │                  │                           │    │
│  │         └─────────────────┼──────────────────┘                           │    │
│  │                           │                                               │    │
│  │                    ┌──────▼────────┐                                     │    │
│  │                    │  Checkpoint   │                                     │    │
│  │                    │    Manager    │                                     │    │
│  │                    │ (SQLite+PG)   │                                     │    │
│  │                    └──────┬────────┘                                     │    │
│  └───────────────────────────┼──────────────────────────────────────────────┘    │
│                              │                                                    │
│                              ▼                                                    │
│                    ┌──────────────────┐         ┌─────────────────┐             │
│                    │  Redis Streams   │         │ Fallback Queue  │             │
│                    │ cdc:<bank>:      │◄────────│  (Local Disk)   │             │
│                    │ <tenant>:<src>:  │  backup │                 │             │
│                    │ <schema>:<table> │         └─────────────────┘             │
│                    └────────┬─────────┘                                          │
│                             │                                                    │
│                             ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                     SPARK CDC CONSUMER                                   │   │
│  │  ┌────────────────────────────────────────────────────────────────┐     │   │
│  │  │  Read Stream → Apply Transformations → DQ Checks → Write Sink  │     │   │
│  │  └────────────────────────────────────────────────────────────────┘     │   │
│  │                                                                           │   │
│  │  Realtime: Structured Streaming   |   Scheduled: Batch via Airflow      │   │
│  └─────────────────────────┬─────────────────────────────────────────────────┘ │
│                            │                                                    │
└────────────────────────────┼────────────────────────────────────────────────────┘
                             │
                             ▼
               ┌─────────────────────────────┐
               │      DESTINATIONS           │
               │  ┌──────────┐  ┌──────────┐│
               │  │ Postgres │  │ Iceberg  ││
               │  │Warehouse │  │ on S3/   ││
               │  │          │  │Azure/GCS ││
               │  └──────────┘  └──────────┘│
               └─────────────────────────────┘
```

---

## Key Features

### Multi-Tenant Isolation
Tenants are logically grouped under parent organisations (e.g., banks). Each tenant may register multiple sources and destinations. Isolation is enforced at the data plane so that a failure or throttling event for one tenant does not impact others.

### Connector Coverage
Native connectors for:
- **MySQL**: Binlog-based CDC with row-based replication format
- **PostgreSQL**: WAL logical replication using replication slots
- **MongoDB**: Change streams with resume tokens (replica set and sharded cluster support)
- **Fallback Polling**: For environments where logs are unavailable or restricted

A connector factory chooses the correct implementation based on the source configuration.

### Real-Time or Scheduled Sync
Users can select:
- **Real-time CDC**: Continuous streaming with low-latency event delivery
- **Scheduled Sync**: Periodic batch ingestion via Airflow and Spark

Both modes share the same transformation logic and destination models.

### Initial Full Load
When a new connection is created, the platform performs a **full snapshot** of the selected tables to establish the current state. Subsequent changes are then captured via the CDC engine.

### Transformations
A rich transformation pipeline supports:
- **Simple Operations**: Casting, trimming, math, date arithmetic
- **Complex Expressions**: Spark SQL or Simple Expression Language (SEL)
- **JSON Flattening**: Inline or child table modes
- **Masking**: Sensitive data obfuscation (e.g., credit card last 4 digits)
- **Custom UDFs**: User-defined functions in Python or Scala

Transformations are configured entirely through the UI and stored as a structured specification that is executed in Spark.

### Schema Evolution
The platform continuously discovers schemas from sources and detects additions, removals or type changes. Users can choose:
- **AUTO_APPLY**: Automatic application of new columns
- **MANUAL_APPROVAL**: Require manual review for every change

### JSON Handling
String columns containing JSON documents are automatically detected. Users can:
- Flatten nested keys into new columns in the main table
- Output them into a child table with a parent/child relationship
- Treat as raw string (no flattening)

### Data Quality & Sync Quality
Data quality rules can be attached to each connection:
- Null ratios
- Range checks
- Row-count reconciliation
- Freshness checks
- Enum validation
- Referential integrity

Sync quality metrics (lag, throughput, error rates) are continuously published to Prometheus and surfaced in the UI.

### Robust Checkpointing
Every connector maintains:
- **Local SQLite checkpoint**: For fast restart
- **Central Postgres checkpoint**: For failover and disaster recovery

### Observability
- Comprehensive metrics (12+ Prometheus metrics)
- Structured JSON logs with trace identifiers
- Error classifications
- Health and liveness endpoints
- Alerting via Prometheus Alertmanager

### Deployment Flexibility
Runs on Kubernetes with:
- Pod auto-scaling (HPA)
- Node-selector/toleration support
- Persistent volumes for state
- Cluster or standalone Redis topologies
- Multi-region compatibility

Can also run on bare-metal or single host for on-prem deployments.

---

## Architecture

### Architectural Principles

Fusion CDC Engine is built around several key principles:

#### 1. Separation of Control and Data Planes
Configuration, metadata management and orchestration reside in the **control plane**. Change log reading, event enrichment, transformation and loading happen in the **data plane**. This separation allows the platform to scale workers independently of the control plane and to ensure that metadata changes cannot directly impact data flow.

#### 2. Per-Host CDC Workers with Tenant-Aware Routing
Instead of one connector per tenant, the data plane runs a **single worker per physical database host**. This worker reads the change log once and routes events by schema or collection to the appropriate tenant. This design dramatically reduces load on source databases while preserving tenant isolation through filtering and stream keys.

#### 3. Deterministic Event Envelope
All change events are normalised into a canonical envelope containing:
- Operation type (create, update, delete)
- Before/after images
- Metadata (timestamps, log positions, tenant identifiers)
- Deterministic `event_id`

Consumers can deduplicate using the `event_id` to achieve at-least-once semantics in the presence of retries.

#### 4. Layered Resilience
The engine maintains:
- **Local state** (SQLite) for fast restarts
- **Central state** (Postgres) for failover
- **Fallback queues** when Redis is unavailable
- **Circuit breakers** for misbehaving sources
- **Careful backoff strategies** to minimise impact on source databases

#### 5. Pluggable Connectors and Outputs
New source types and destinations can be added without changing the core. Connectors implement a base interface and register themselves with a factory. Outputs (Redis, Kafka, Webhook, etc.) are similarly pluggable.

### Control Plane Responsibilities

#### Metadata Persistence
The control plane stores tenants, sources, destinations, connections, streams, transformation specifications and DQ policies in a central relational database (Postgres). Tables are versioned and changes are audited.

#### Orchestration
When a new connection is created, the scheduler determines which DB host worker should handle the source. It ensures concurrency limits are respected (e.g., only 2–3 connections active per worker) and schedules Airflow DAGs for scheduled syncs.

#### API & UI
The control plane exposes REST/GraphQL APIs and serves the UI. It applies authentication and authorisation, performs request validation and serialisation, and enforces tenant boundaries.

#### Policy Enforcement
Schema evolution policies, DQ rules and transformation specs are stored and enforced centrally. Workers query the control plane when they start up to retrieve the latest spec.

#### Monitoring & Alerts
Control plane services export their own metrics. They also aggregate metrics from workers and expose dashboards and alert rules. For example, a high number of read errors on a particular source triggers a warning.

### Data Plane Responsibilities

#### Log Capture
Each worker uses an appropriate client library to read change logs:
- **MySQL**: `python-mysql-replication` library to consume binlog
- **PostgreSQL**: `psycopg` replication API to create logical replication slots and decode WAL entries
- **MongoDB**: `PyMongo` change stream API
- **Fallback Polling**: Periodic SELECT queries with diff computation

#### Event Normalisation
Raw log entries are parsed into a canonical event envelope:

```json
{
  "tenant_id": "t123",
  "bank_id": "b1",
  "source_id": "src1",
  "schema": "public",
  "table": "orders",
  "op": "u",
  "before": {"id": 1, ...},
  "after": {"id": 1, ...},
  "ts_ms": 1638999988000,
  "lsn": "16/CAFE1234",
  "event_id": "sha256(...)",
  "metadata": {...}
}
```

#### CDC Event Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         CDC EVENT FLOW - END TO END                           │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│  Source DB   │  (MySQL / Postgres / MongoDB)
│  - Tenant A  │
│  - Tenant B  │  Multiple tenants on same physical host
│  - Tenant C  │
└──────┬───────┘
       │
       │ Binlog / WAL / Change Streams
       │
       ▼
┌──────────────────────────────────────────────────────────────────┐
│              DB HOST WORKER                                       │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  1. Read Change Log (Single Stream)                     │    │
│  │     - MySQL: python-mysql-replication                   │    │
│  │     - Postgres: psycopg replication slot                │    │
│  │     - MongoDB: PyMongo change stream cursor             │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                            │                                      │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │  2. Parse & Filter by Tenant                            │    │
│  │     - Extract schema/collection name                    │    │
│  │     - Match to tenant mapping                           │    │
│  │     - Filter out irrelevant schemas                     │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                            │                                      │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │  3. Normalize to Canonical Envelope                     │    │
│  │     {                                                    │    │
│  │       "tenant_id": "t123",                              │    │
│  │       "bank_id": "b1",                                  │    │
│  │       "source_id": "src1",                              │    │
│  │       "schema": "public",                               │    │
│  │       "table": "orders",                                │    │
│  │       "op": "u",  // c=create, u=update, d=delete       │    │
│  │       "before": {...},                                  │    │
│  │       "after": {...},                                   │    │
│  │       "ts_ms": 1638999988000,                           │    │
│  │       "lsn": "16/CAFE1234",                             │    │
│  │       "event_id": "deterministic_hash"                  │    │
│  │     }                                                    │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                            │                                      │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │  4. Checkpoint Local SQLite                             │    │
│  │     - Save LSN/position                                 │    │
│  │     - Enables fast restart                              │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                            │                                      │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │  5. Push to Redis Stream                                │    │
│  │     Key: cdc:<bank>:<tenant>:<source>:<schema>:<table>  │    │
│  │     XADD with event_id as message ID                    │    │
│  │                                                          │    │
│  │     If Redis unavailable → Fallback Queue (disk)        │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                            │                                      │
│  ┌─────────────────────────▼───────────────────────────────┐    │
│  │  6. Periodic Checkpoint to Central Postgres             │    │
│  │     - Batched every N events or M seconds               │    │
│  │     - Used for failover recovery                        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
└───────────────────────────┬───────────────────────────────────────┘
                            │
                            │ Redis Streams
                            │
                            ▼
         ┌──────────────────────────────────┐
         │       Redis Cluster              │
         │  Stream Keys:                    │
         │  cdc:b1:t123:src1:public:orders  │
         │  cdc:b1:t456:src2:sales:items    │
         │  ...                             │
         └──────────────┬───────────────────┘
                        │
                        │ XREAD / XREADGROUP
                        │
                        ▼
         ┌─────────────────────────────────────────────────┐
         │      SPARK CDC CONSUMER (Realtime/Batch)        │
         │                                                  │
         │  ┌───────────────────────────────────────────┐  │
         │  │  1. Read from Redis Stream                │  │
         │  │     (Structured Streaming or Batch)       │  │
         │  └──────────────────┬────────────────────────┘  │
         │                     │                            │
         │  ┌──────────────────▼───────────────────────┐   │
         │  │  2. Load Transformation Spec             │   │
         │  │     (from Metadata Service)              │   │
         │  └──────────────────┬───────────────────────┘   │
         │                     │                            │
         │  ┌──────────────────▼───────────────────────┐   │
         │  │  3. Apply Transformations                │   │
         │  │     - Cast, Mask, JSON Flatten           │   │
         │  │     - UDFs, Expressions                  │   │
         │  └──────────────────┬───────────────────────┘   │
         │                     │                            │
         │  ┌──────────────────▼───────────────────────┐   │
         │  │  4. Run DQ Checks                        │   │
         │  │     - Row count, null ratio, freshness   │   │
         │  │     - Emit metrics                       │   │
         │  └──────────────────┬───────────────────────┘   │
         │                     │                            │
         │  ┌──────────────────▼───────────────────────┐   │
         │  │  5. Write to Destination                 │   │
         │  │     - Upsert to Postgres Warehouse       │   │
         │  │     - Commit to Iceberg on S3/Azure/GCS  │   │
         │  └──────────────────────────────────────────┘   │
         │                                                  │
         └──────────────────┬───────────────────────────────┘
                            │
                            │
                            ▼
              ┌──────────────────────────────┐
              │      DESTINATIONS            │
              │                              │
              │  ┌────────────────────────┐  │
              │  │  Postgres Warehouse    │  │
              │  │  - Per-tenant schemas  │  │
              │  │  - Upsert logic        │  │
              │  └────────────────────────┘  │
              │                              │
              │  ┌────────────────────────┐  │
              │  │  Iceberg Tables        │  │
              │  │  - S3 / Azure / GCS    │  │
              │  │  - Partitioned by date │  │
              │  │  - ACID transactions   │  │
              │  └────────────────────────┘  │
              │                              │
              └──────────────────────────────┘
```

#### Worker Placement Strategy

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WORKER PLACEMENT ARCHITECTURE                     │
└─────────────────────────────────────────────────────────────────────┘

                     ┌────────────────────────┐
                     │  Metadata Service      │
                     │  (Control Plane)       │
                     │                        │
                     │  Tenant Mappings:      │
                     │  t1 → db-host-1        │
                     │  t2 → db-host-1        │
                     │  t3 → db-host-2        │
                     └───────────┬────────────┘
                                 │
                                 │ Worker assignments
                                 ▼
                ┌────────────────────────────────┐
                │  Scheduler & Orchestrator      │
                │  - Assigns workers to hosts    │
                │  - Respects concurrency limits │
                │  - Handles failover            │
                └────────┬───────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
┌─────────────────┐ ┌─────────────┐ ┌──────────────┐
│  Worker Pod 1   │ │ Worker Pod 2│ │ Worker Pod 3 │
│  Assigned to:   │ │ Assigned to │ │ Assigned to  │
│  db-host-1      │ │ db-host-2   │ │ db-host-3    │
│                 │ │             │ │              │
│  Tenants:       │ │ Tenants:    │ │ Tenants:     │
│  - t1           │ │ - t3        │ │ - t5         │
│  - t2           │ │ - t4        │ │ - t6         │
└────────┬────────┘ └──────┬──────┘ └──────┬───────┘
         │                 │                │
         │ Reads binlog    │ Reads WAL      │ Reads change
         │ ONCE            │ ONCE           │ stream ONCE
         ▼                 ▼                ▼
┌─────────────────┐ ┌─────────────┐ ┌──────────────┐
│  db-host-1      │ │ db-host-2   │ │  db-host-3   │
│  (MySQL)        │ │ (Postgres)  │ │  (MongoDB)   │
│                 │ │             │ │              │
│  Schemas:       │ │ Schemas:    │ │ Collections: │
│  - t1_schema    │ │ - t3_public │ │ - t5_db      │
│  - t2_schema    │ │ - t4_sales  │ │ - t6_db      │
└─────────────────┘ └─────────────┘ └──────────────┘

Benefits:
✓ Single binlog/WAL read per host (reduces source DB load)
✓ Worker filters events by schema/collection → tenant
✓ Tenant isolation via Redis stream keys
✓ Easy horizontal scaling (add workers for new hosts)
```

#### Dual-Layer Checkpointing Strategy

```
┌──────────────────────────────────────────────────────────────────────────┐
│                  DUAL-LAYER CHECKPOINT ARCHITECTURE                       │
└──────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│                         DB HOST WORKER                              │
│                                                                     │
│  Event Processing Loop:                                            │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  1. Read binlog event                                        │ │
│  │     LSN/Position: 16/CAFE1234                                │ │
│  └───────────────────────┬──────────────────────────────────────┘ │
│                          │                                         │
│  ┌───────────────────────▼─────────────────────────────────────┐  │
│  │  2. Normalize event                                         │  │
│  │     Create canonical envelope                               │  │
│  └───────────────────────┬─────────────────────────────────────┘  │
│                          │                                         │
│  ┌───────────────────────▼─────────────────────────────────────┐  │
│  │  3. LAYER 1: Write to Local SQLite Checkpoint              │  │
│  │                                                             │  │
│  │     File: /var/lib/cdc/checkpoints/src1.db                 │  │
│  │                                                             │  │
│  │     UPDATE checkpoint SET                                  │  │
│  │       lsn = '16/CAFE1234',                                 │  │
│  │       ts = now(),                                          │  │
│  │       event_count = event_count + 1                        │  │
│  │     WHERE source_id = 'src1'                               │  │
│  │                                                             │  │
│  │     ✓ FAST (local disk, <1ms)                              │  │
│  │     ✓ Enables quick restart after pod failure              │  │
│  │     ✗ Lost if PersistentVolume fails                       │  │
│  └───────────────────────┬─────────────────────────────────────┘  │
│                          │                                         │
│  ┌───────────────────────▼─────────────────────────────────────┐  │
│  │  4. Push event to Redis                                     │  │
│  │     XADD cdc:b1:t1:src1:public:orders * event_json          │  │
│  └───────────────────────┬─────────────────────────────────────┘  │
│                          │                                         │
│                          │                                         │
│  ┌───────────────────────▼─────────────────────────────────────┐  │
│  │  5. LAYER 2: Periodic Sync to Central Postgres             │  │
│  │                                                             │  │
│  │     Trigger: Every 100 events OR every 10 seconds          │  │
│  │                                                             │  │
│  │     INSERT INTO central_checkpoints                        │  │
│  │       (bank_id, tenant_id, source_id, stream,              │  │
│  │        lsn, ts, event_count)                               │  │
│  │     VALUES (...)                                           │  │
│  │     ON CONFLICT (source_id, stream)                        │  │
│  │     DO UPDATE SET lsn = EXCLUDED.lsn, ...                  │  │
│  │                                                             │  │
│  │     ✓ DURABLE (replicated Postgres, cross-AZ)              │  │
│  │     ✓ Used for failover and disaster recovery              │  │
│  │     ✗ SLOWER (network RTT, ~10-50ms)                       │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

RECOVERY SCENARIOS:

Scenario 1: Worker Pod Restart (PersistentVolume intact)
─────────────────────────────────────────────────────────
1. Pod restarts
2. Worker reads local SQLite checkpoint
3. Finds LSN: 16/CAFE1234
4. Resumes binlog from that position
   ✓ Fast recovery (seconds)

Scenario 2: PersistentVolume Lost
──────────────────────────────────
1. Pod starts with no local checkpoint
2. Worker queries central Postgres checkpoint table
3. Finds last synced LSN: 16/CAFE1200
4. Resumes binlog from 16/CAFE1200
   ✓ Data safety preserved
   ⚠ May replay ~100 events (deduplicated by event_id)

Scenario 3: Both Local + Central Lost (Disaster)
─────────────────────────────────────────────────
1. No checkpoints available
2. Option A: Resume from current binlog position (data loss)
3. Option B: Trigger full refresh sync
   ✗ Rare scenario, requires manual intervention
```
```

#### Tenant Routing
Workers examine the schema/table field and use lookup tables populated by the control plane to determine which tenant owns the data. They then publish the event into a Redis stream key prefixed with the tenant and bank. If multiple tenants share the same physical database, each event may be duplicated across multiple tenant streams depending on the mapping.

#### Transformation Hooks
Workers attach the transformation pipeline ID and DQ policy ID from the connection but do not perform heavy transformations. Instead, they only perform minimal work such as normalising data types, converting timestamps to ISO8601 and ensuring JSON strings are valid JSON. Heavy transformations are left to the **Spark consumer**.

#### Checkpointing
Each worker stores its last processed LSN/binlog position for every (schema, table) combination in a local SQLite database. It periodically commits these positions to the central Postgres via the control plane. On restart, the worker tries to use the local checkpoint first; if none exists (e.g., new pod on another node) it fetches the last committed position from Postgres.

Checkpoints are stored in an idempotent manner to avoid corruption: each commit writes to a temp table and then atomically swaps it into place.

#### Fallback Queue
If Redis is unavailable or the stream trim policy would discard unsent events, the worker stores events in a local disk queue (e.g., using SQLite or a simple file append log). Once Redis is healthy, the queue is flushed. The fallback queue also acts as a buffer when the Spark consumer slows down.

#### Worker Orchestration
Workers run as pods in Kubernetes. Each pod advertises its capabilities (source type, host it is reading from, concurrency limit) via a heartbeat to the control plane. The scheduler assigns sources to pods. When a new source is added, the scheduler either reuses an existing worker reading from the same host or spins up a new one. Pods scale down when sources are removed.

### Concurrency Limits & Scheduling

A key design challenge is limiting the number of concurrent CDC connections per worker to avoid overwhelming the database host. The control plane imposes per-worker concurrency limits defined in the `TenantConfig` or globally.

For example, a MySQL DB host worker might allow **three concurrent schemas** to be captured at once. If a tenant selects 10 tables on that host, the worker loads three tables concurrently and queues the others. As tasks finish, they free up slots for waiting tables.

This concurrency scheduling is implemented within the worker process using `asyncio` tasks and a semaphore to restrict the number of active replication tasks.

---

## Tenancy Model

### Parent → Tenant Hierarchy

Tenants are organised under a **parent entity**. In the banking example, `Bank1` might own `tenant1`, `tenant2` and `tenant3`. Each tenant has its own data sources and destinations but may reside on shared physical database servers.

```
                        ┌─────────────────────┐
                        │      Parent         │
                        │   (e.g., Bank)      │
                        │   bank_id: acme     │
                        └──────────┬──────────┘
                                   │
                 ┌─────────────────┼─────────────────┐
                 │                 │                  │
        ┌────────▼────────┐ ┌─────▼──────┐  ┌───────▼────────┐
        │   Tenant 001    │ │ Tenant 002 │  │  Tenant 003    │
        │ tenant_id: 001  │ │tenant_id:  │  │ tenant_id: 003 │
        │                 │ │    002     │  │                │
        └────────┬────────┘ └─────┬──────┘  └───────┬────────┘
                 │                │                  │
         ┌───────┴────────┐       │          ┌───────┴────────┐
         │ Sources:       │       │          │ Sources:       │
         │ - MySQL DB     │       │          │ - MongoDB      │
         │ - Postgres DB  │       │          │ - MySQL DB     │
         │                │       │          │                │
         │ Destinations:  │       │          │ Destinations:  │
         │ - Warehouse    │       │          │ - Iceberg      │
         │ - Iceberg      │       │          │                │
         └────────────────┘       │          └────────────────┘
                                  │
                          ┌───────┴────────┐
                          │ Sources:       │
                          │ - PostgreSQL   │
                          │                │
                          │ Destinations:  │
                          │ - Warehouse    │
                          └────────────────┘
```

The tenancy model affects both the UI and the control plane:

#### User Access
- **Tenant Administrator**: Sees only their sources, destinations and connections
- **Parent Administrator**: Can view and manage all tenants under the parent

#### Resource Isolation
Each tenant's CDC processes are isolated either in separate pods (per-tenant) or grouped under DB host pods with logical filtering. Isolation prevents one tenant's heavy workload from impacting others and enables per-tenant pause/resume operations.

#### Configuration Isolation
Source and destination credentials, sync schedules, transformation specs and DQ rules are stored per tenant. A central `TenantConfig` record holds resource limits and tier assignments.

### Tenant Onboarding UI Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        TENANT ONBOARDING WORKFLOW                             │
└──────────────────────────────────────────────────────────────────────────────┘

    User (Tenant Admin)
         │
         │ 1. Login via Parent Portal
         ▼
    ┌─────────────────┐
    │   Parent UI     │
    │  (Bank Portal)  │
    └────────┬────────┘
             │
             │ 2. Select "Add New Tenant"
             ▼
    ┌─────────────────────────┐
    │  Tenant Registration    │
    │  - Name                 │
    │  - Contact Info         │
    │  - Subscription Tier    │
    └────────┬────────────────┘
             │
             │ 3. Create Tenant → API Call
             ▼
    ┌─────────────────────────┐
    │  Metadata Service       │
    │  INSERT INTO tenants    │
    │  (bank_id, tenant_id,   │
    │   name, tier)           │
    └────────┬────────────────┘
             │
             │ 4. Tenant Created → Redirect
             ▼
    ┌─────────────────────────┐
    │  Tenant Dashboard       │
    │  - Add Source           │
    │  - Add Destination      │
    │  - Create Connection    │
    └────────┬────────────────┘
             │
             │ 5. User Clicks "Add Source"
             ▼
    ┌─────────────────────────────────────────┐
    │  Source Configuration Form               │
    │  - Source Type (MySQL/PG/Mongo)         │
    │  - Host, Port, Database                 │
    │  - Username, Password                   │
    │  - SSL Options                          │
    └────────┬────────────────────────────────┘
             │
             │ 6. Test Connection
             ▼
    ┌─────────────────────────────────────────┐
    │  Connector Factory                       │
    │  - Validate credentials                 │
    │  - Check network connectivity           │
    │  - Retrieve schema list                 │
    └────────┬────────────────────────────────┘
             │
             │ 7. Connection Successful ✓
             ▼
    ┌─────────────────────────────────────────┐
    │  Save Source Configuration              │
    │  - Store encrypted credentials          │
    │  - Perform schema discovery             │
    │  - Save to metadata DB                  │
    └────────┬────────────────────────────────┘
             │
             │ 8. User Adds Destination (Warehouse/Iceberg)
             ▼
    ┌─────────────────────────────────────────┐
    │  Destination Configuration              │
    │  - Type: Postgres Warehouse / Iceberg   │
    │  - Connection Details                   │
    │  - Schema/Catalog Name                  │
    └────────┬────────────────────────────────┘
             │
             │ 9. Create Connection
             ▼
    ┌──────────────────────────────────────────────────────┐
    │  Connection Creation Wizard                           │
    │                                                       │
    │  Step 1: Choose Source & Destination                 │
    │  Step 2: Select Tables/Collections                   │
    │  Step 3: Choose Sync Mode                            │
    │         ┌────────────────────────────────┐           │
    │         │ ○ Realtime CDC                 │           │
    │         │ ○ Scheduled (Hourly/Daily)     │           │
    │         └────────────────────────────────┘           │
    │  Step 4: Configure Transformations (Optional)        │
    │  Step 5: Define Data Quality Rules (Optional)        │
    │  Step 6: Review & Activate                           │
    └────────┬─────────────────────────────────────────────┘
             │
             │ 10. Initial Full Load Triggered
             ▼
    ┌─────────────────────────────────────────┐
    │  Full Snapshot Job                      │
    │  - Airflow DAG created                  │
    │  - Spark batch job reads source         │
    │  - Writes to destination                │
    │  - Status: RUNNING → COMPLETED          │
    └────────┬────────────────────────────────┘
             │
             │ 11. CDC Worker Assigned
             ▼
    ┌─────────────────────────────────────────┐
    │  Scheduler & Orchestrator               │
    │  - Assign worker to source host         │
    │  - Worker starts reading binlog/WAL     │
    │  - Events pushed to Redis               │
    └────────┬────────────────────────────────┘
             │
             │ 12. Monitor Dashboard
             ▼
    ┌─────────────────────────────────────────┐
    │  Tenant Monitoring View                 │
    │  - Stream lag                           │
    │  - Events/second                        │
    │  - DQ rule status                       │
    │  - Schema changes (pending)             │
    └─────────────────────────────────────────┘
```

---

## Source & Destination Configuration

### Source Structure

Each tenant may configure multiple **Sources**, each representing a database connection. Supported source types include:

#### MySQL
- Connects to a MySQL server
- Reads the binary log (binlog) in row-based replication mode
- Uses **`python-mysql-replication`** library
- Normalises events
- Fallback polling mode available when binary logging is disabled

**MySQL Connector Implementation Details:**

```python
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent

class MySQLConnector:
    def __init__(self, config):
        self.config = config
        self.stream = BinLogStreamReader(
            connection_settings={
                'host': config['host'],
                'port': config['port'],
                'user': config['user'],
                'passwd': config['password']
            },
            server_id=config.get('server_id', 100),
            blocking=True,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            only_schemas=[config['database']],
            only_tables=config.get('tables', None)
        )
    
    def read_events(self):
        """
        Yields normalized CDC events from MySQL binlog
        """
        for binlog_event in self.stream:
            for row in binlog_event.rows:
                event = {
                    'tenant_id': self.config['tenant_id'],
                    'source_id': self.config['source_id'],
                    'schema': binlog_event.schema,
                    'table': binlog_event.table,
                    'op': self._map_operation(binlog_event),
                    'ts_ms': binlog_event.timestamp * 1000,
                    'lsn': f"{binlog_event.packet.log_file}:{binlog_event.packet.log_pos}",
                    'event_id': self._compute_event_id(binlog_event, row)
                }
                
                if isinstance(binlog_event, WriteRowsEvent):
                    event['after'] = row['values']
                elif isinstance(binlog_event, UpdateRowsEvent):
                    event['before'] = row['before_values']
                    event['after'] = row['after_values']
                elif isinstance(binlog_event, DeleteRowsEvent):
                    event['before'] = row['values']
                
                yield event
    
    def checkpoint_position(self):
        """
        Returns current binlog position for checkpointing
        """
        return {
            'log_file': self.stream.log_file,
            'log_pos': self.stream.log_pos
        }
```

**MySQL Configuration Requirements:**
- `binlog_format = ROW` in my.cnf
- `binlog_row_image = FULL` (for before/after images)
- REPLICATION CLIENT privilege for user
- Optional: GTID enabled for easier failover

#### PostgreSQL
- Uses PostgreSQL's logical replication and replication slots
- Streams write ahead log (WAL) changes
- Uses **`psycopg`** with replication protocol
- Decodes changes via `wal2json` or `pgoutput` output plugin

**PostgreSQL Connector Implementation Details:**

```python
import psycopg
from psycopg.replication import LogicalReplicationConnection
import json

class PostgreSQLConnector:
    def __init__(self, config):
        self.config = config
        self.conn = LogicalReplicationConnection.connect(
            host=config['host'],
            port=config['port'],
            dbname=config['database'],
            user=config['user'],
            password=config['password']
        )
        
        # Create replication slot if not exists
        self.slot_name = config.get('slot_name', f"fusion_{config['source_id']}")
        self.publication_name = config.get('publication', 'fusion_pub')
    
    def create_slot_and_publication(self):
        """
        Setup logical replication slot and publication
        """
        with psycopg.connect(**self.config) as conn:
            with conn.cursor() as cur:
                # Create publication (source tables)
                cur.execute(f"""
                    CREATE PUBLICATION {self.publication_name}
                    FOR TABLE {', '.join(self.config['tables'])}
                """)
                
        # Create replication slot
        try:
            self.conn.create_replication_slot(
                self.slot_name,
                output_plugin='pgoutput'
            )
        except psycopg.errors.DuplicateObject:
            pass  # Slot already exists
    
    def read_events(self):
        """
        Yields normalized CDC events from PostgreSQL WAL
        """
        self.conn.start_replication(
            slot_name=self.slot_name,
            options={
                'publication_names': self.publication_name,
                'proto_version': '1'
            }
        )
        
        for msg in self.conn:
            if msg.data_start:
                # Decode WAL message
                payload = json.loads(msg.payload)
                
                for change in payload.get('change', []):
                    event = {
                        'tenant_id': self.config['tenant_id'],
                        'source_id': self.config['source_id'],
                        'schema': change['schema'],
                        'table': change['table'],
                        'op': change['kind'],  # 'insert', 'update', 'delete'
                        'ts_ms': msg.send_time,
                        'lsn': str(msg.data_start),
                        'event_id': self._compute_event_id(msg, change)
                    }
                    
                    if 'columnvalues' in change:
                        event['after'] = change['columnvalues']
                    if 'oldkeys' in change:
                        event['before'] = change['oldkeys']
                    
                    yield event
            
            # Send feedback to confirm processing
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
```

**PostgreSQL Configuration Requirements:**
- `wal_level = logical` in postgresql.conf
- `max_replication_slots >= 1`
- `max_wal_senders >= 1`
- REPLICATION privilege for user
- Create publication and replication slot

#### MongoDB
- Uses MongoDB's change streams and resume tokens
- Supports both replica set and sharded cluster configurations
- Uses **`PyMongo`** with change stream API

**MongoDB Connector Implementation Details:**

```python
from pymongo import MongoClient
from bson import json_util

class MongoDBConnector:
    def __init__(self, config):
        self.config = config
        self.client = MongoClient(
            host=config['host'],
            port=config['port'],
            username=config['user'],
            password=config['password'],
            replicaSet=config.get('replica_set')
        )
        self.db = self.client[config['database']]
        self.resume_token = config.get('resume_token')
    
    def read_events(self):
        """
        Yields normalized CDC events from MongoDB change streams
        """
        pipeline = [
            {'$match': {
                'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}
            }}
        ]
        
        # Watch all collections or specific ones
        if self.config.get('collections'):
            collections = [self.db[name] for name in self.config['collections']]
        else:
            collections = [self.db]
        
        for collection in collections:
            with collection.watch(
                pipeline=pipeline,
                resume_after=self.resume_token
            ) as stream:
                for change in stream:
                    event = {
                        'tenant_id': self.config['tenant_id'],
                        'source_id': self.config['source_id'],
                        'schema': self.config['database'],
                        'table': change['ns']['coll'],
                        'op': self._map_operation(change['operationType']),
                        'ts_ms': change['clusterTime'].time * 1000,
                        'lsn': str(change['_id']),  # Resume token
                        'event_id': str(change['_id'])
                    }
                    
                    if 'fullDocument' in change:
                        event['after'] = json_util.dumps(change['fullDocument'])
                    if 'fullDocumentBeforeChange' in change:
                        event['before'] = json_util.dumps(change['fullDocumentBeforeChange'])
                    
                    yield event
                    
                    # Update resume token for checkpointing
                    self.resume_token = change['_id']
```

**MongoDB Configuration Requirements:**
- Replica set or sharded cluster (change streams require replica set)
- MongoDB 3.6+ for change streams
- `readAnyDatabase` role for user (or specific database read permissions)
- Enable `changeStreamPreAndPostImages` for full document tracking

#### Connector Factory Pattern

```python
class ConnectorFactory:
    """
    Factory for creating database connectors based on source type
    """
    
    @staticmethod
    def create_connector(source_config):
        source_type = source_config['type'].lower()
        
        if source_type == 'mysql':
            return MySQLConnector(source_config)
        elif source_type == 'postgresql':
            return PostgreSQLConnector(source_config)
        elif source_type == 'mongodb':
            return MongoDBConnector(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    @staticmethod
    def test_connection(source_config):
        """
        Test database connectivity before activating source
        """
        connector = ConnectorFactory.create_connector(source_config)
        try:
            connector.connect()
            schemas = connector.discover_schemas()
            return {'status': 'success', 'schemas': schemas}
        except Exception as e:
            return {'status': 'failed', 'error': str(e)}
```

Each source stores:
- **Connection Details**: Host, port, database name, user, credential reference, SSL settings
- **CDC Configuration**: Binlog format/GTID for MySQL, slot/publication name for Postgres, replica set for MongoDB
- **Metadata Discovery Cache**: List of schemas and tables, column types, JSON detection
- **Checkpoint State**: Log position (MySQL), LSN (Postgres), resume token (MongoDB)

### Destination Structure

Destinations represent the target systems where changed data is delivered. The engine separates the CDC capture from the loading job; loading can be handled by Spark or other downstream jobs that consume events from Redis.

Currently supported destinations include:

#### Postgres Data Warehouse
- Events are transformed and merged into analytical Postgres tables (SCD type 1 or type 2)
- Ideal for reporting and operational analytics

#### Iceberg Tables on Object Storage
- Events are written as Parquet files and committed to Iceberg tables
- Stored on S3, Azure Blob Storage or GCS
- Enables open table format features such as time travel and atomic commits

#### Other
- The architecture allows additional sinks like Elasticsearch, BigQuery or custom webhooks to be added via the output factory pattern

Each destination stores:
- **Connection Information**: JDBC URL, bucket and credentials for object storage
- **Loading Configuration**: Upsert vs append
- **Performance Tuning Parameters**: Batch sizes, parallelism

---

## Connections & Streams

### Connection Configuration

A **Connection** ties together a Source and a Destination for a given tenant. It defines what tables/collections to sync and how to sync them.

A connection contains:

- `connection_id` – Unique identifier
- `source_id` and `destination_id`
- `sync_mode` – One of:
  - `FULL_REFRESH_OVERWRITE`
  - `FULL_REFRESH_APPEND`
  - `INCREMENTAL_APPEND`
  - `INCREMENTAL_APPEND_DEDUPED`
  - `CDC_INCREMENTAL_DEDUPED_HISTORY` (log-based CDC with history)
- `sync_type` – `REALTIME` or `SCHEDULED`
- `schedule` – Cron expression or manual for scheduled jobs
- `schema_evolution_policy` – `AUTO_APPLY` or `MANUAL_APPROVAL`
- `transform_pipeline_id` – Pointer to a transformation specification
- `dq_policy_id` – Pointer to a data quality policy

### Sync Modes Explained

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         SYNC MODES - DETAILED COMPARISON                      │
└──────────────────────────────────────────────────────────────────────────────┘

1. FULL_REFRESH_OVERWRITE
──────────────────────────
Behavior: Complete table snapshot, destination TRUNCATED before load
Use Case: Dimension tables, reference data, small tables

Source Table (orders):          Destination Table:              After Sync:
┌────┬────────┐                ┌────┬────────┐                ┌────┬────────┐
│ id │ amount │                │ id │ amount │                │ id │ amount │
├────┼────────┤                ├────┼────────┤  TRUNCATE      ├────┼────────┤
│ 1  │  100   │                │ 1  │  100   │  +             │ 1  │  150   │
│ 2  │  200   │   ─────────►   │ 2  │  200   │  INSERT ALL    │ 2  │  200   │
│ 3  │  300   │                │ 3  │  300   │  ═══════►      │ 3  │  300   │
└────┴────────┘                └────┴────────┘                │ 4  │  400   │
                                (old data)                     └────┴────────┘
                                                               (current snapshot)

2. FULL_REFRESH_APPEND
───────────────────────
Behavior: Complete snapshot, APPEND to existing data
Use Case: Historical tracking, audit tables

Source Table:                  Destination Table:              After Sync:
┌────┬────────┐                ┌────┬────────┬────────────┐  ┌────┬────────┬────────────┐
│ id │ amount │                │ id │ amount │ loaded_at  │  │ id │ amount │ loaded_at  │
├────┼────────┤                ├────┼────────┼────────────┤  ├────┼────────┼────────────┤
│ 1  │  150   │                │ 1  │  100   │ 2025-11-27 │  │ 1  │  100   │ 2025-11-27 │
│ 2  │  200   │   ─────────►   │ 2  │  200   │ 2025-11-27 │  │ 2  │  200   │ 2025-11-27 │
│ 3  │  300   │                │ 3  │  300   │ 2025-11-27 │  │ 3  │  300   │ 2025-11-27 │
│ 4  │  400   │                └────┴────────┴────────────┘  │ 1  │  150   │ 2025-11-28 │◄─ New
└────┴────────┘                                               │ 2  │  200   │ 2025-11-28 │  rows
                                                              │ 3  │  300   │ 2025-11-28 │  added
                                                              │ 4  │  400   │ 2025-11-28 │
                                                              └────┴────────┴────────────┘

3. INCREMENTAL_APPEND
──────────────────────
Behavior: Only NEW rows since last sync (uses cursor column like created_at)
Use Case: Immutable logs, events, append-only tables

Source Table:                  Last Cursor: 2025-11-28 10:00
┌────┬────────┬────────────────┐
│ id │ amount │  created_at    │  Only rows where created_at > last cursor
├────┼────────┼────────────────┤
│ 1  │  100   │ 2025-11-28 09:00│ ← Skip (old)
│ 2  │  200   │ 2025-11-28 09:30│ ← Skip (old)
│ 3  │  300   │ 2025-11-28 10:15│ ◄─ SELECT (new)
│ 4  │  400   │ 2025-11-28 10:20│ ◄─ SELECT (new)
└────┴────────┴────────────────┘

Destination gets only rows 3 & 4 APPENDED

4. INCREMENTAL_APPEND_DEDUPED
──────────────────────────────
Behavior: Incremental + UPSERT based on primary key
Use Case: Mutable dimension tables, customer profiles

Source Changes:                Destination (before):           After UPSERT:
┌────┬────────┬────────────┐  ┌────┬────────┬────────────┐  ┌────┬────────┬────────────┐
│ id │ amount │ updated_at │  │ id │ amount │ updated_at │  │ id │ amount │ updated_at │
├────┼────────┼────────────┤  ├────┼────────┼────────────┤  ├────┼────────┼────────────┤
│ 2  │  250   │ 10:15      │  │ 1  │  100   │ 09:00      │  │ 1  │  100   │ 09:00      │
│ 4  │  400   │ 10:20      │  │ 2  │  200   │ 09:30      │  │ 2  │  250   │ 10:15      │◄─ Updated
└────┴────────┴────────────┘  │ 3  │  300   │ 10:00      │  │ 3  │  300   │ 10:00      │
     (incremental)            └────┴────────┴────────────┘  │ 4  │  400   │ 10:20      │◄─ Inserted
                                                            └────┴────────┴────────────┘

SQL: INSERT ... ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount, ...

5. CDC_INCREMENTAL_DEDUPED_HISTORY (SCD Type 2)
────────────────────────────────────────────────
Behavior: Track all changes with validity windows
Use Case: Regulatory compliance, full audit trail

CDC Events:                    Destination (SCD Type 2):
┌────┬────────┬──────────┐   ┌────┬────────┬────────────┬────────────┬─────────┐
│ id │ amount │    op    │   │ id │ amount │ valid_from │  valid_to  │ current │
├────┼────────┼──────────┤   ├────┼────────┼────────────┼────────────┼─────────┤
│ 2  │  250   │  UPDATE  │   │ 2  │  200   │ 09:30      │ 10:15      │  false  │◄─ Old
│ 2  │  250   │  (after) │   │ 2  │  250   │ 10:15      │ NULL       │  true   │◄─ Current
└────┴────────┴──────────┘   │ 3  │  300   │ 10:00      │ NULL       │  true   │
                              │ 4  │  400   │ 10:20      │ NULL       │  true   │
                              └────┴────────┴────────────┴────────────┴─────────┘

Every UPDATE creates new row, closes old row's valid_to
DELETE sets valid_to but keeps row for history
```

### Sync Mode Selection Guide

| Sync Mode | Data Mutability | History Tracking | Storage Efficiency | Use Case |
|-----------|----------------|------------------|-------------------|----------|
| `FULL_REFRESH_OVERWRITE` | Any | ❌ No | ⭐⭐⭐ High | Reference tables, dimensions |
| `FULL_REFRESH_APPEND` | Any | ✅ Snapshots | ⚠️ Low (grows unbounded) | Historical snapshots |
| `INCREMENTAL_APPEND` | Immutable | ❌ No | ⭐⭐⭐ High | Logs, events, append-only |
| `INCREMENTAL_APPEND_DEDUPED` | Mutable | ❌ Latest only | ⭐⭐ Medium | Customer profiles, products |
| `CDC_INCREMENTAL_DEDUPED_HISTORY` | Mutable | ✅ Full history | ⚠️ Medium (one row per change) | Regulatory, audit, compliance |

### Streams

Each connection defines one or more **Streams**. A stream corresponds to a specific table (for relational sources) or collection (for MongoDB).

For each stream, the user can:
- Enable or disable syncing of that table
- Inspect and flatten JSON columns
- Configure per-column transformations and type mappings
- Override the global sync mode (some tables may be full refresh while others use incremental or CDC)
- Define primary keys or cursor fields when CDC is not available

---

## Transformations & JSON Handling

### Motivation

Raw change events from source databases rarely match the shape required in analytics sinks. Enterprises often need to:

- Cast types (e.g., string to timestamp, numeric scaling)
- Perform arithmetic or string operations (e.g., convert amounts to a single currency)
- Flatten nested JSON documents into columns or separate tables
- Mask sensitive data (e.g., obfuscate credit card numbers)
- Apply business rules via conditionals and custom functions
- Rename or recompute columns
- Ensure consistent schemas across tenants despite differences in source naming conventions

A flexible yet safe transformation engine is therefore required.

### Transformation Pipeline Design

#### Basic Concept

Each connection has an associated **transformation pipeline**. A pipeline consists of an ordered list of **transform steps**. Each step takes one or more input columns from the canonical event and produces one or more output columns.

The pipeline operates on a row at a time (or document for MongoDB) and is declaratively defined in the control plane.

The pipeline is stored as a JSON specification in the metadata database and loaded by the Spark consumer. This allows dynamic updates without redeploying code. The UI exposes a friendly interface for defining transformations which is ultimately serialised into this spec.

### Transform Step Types

Transform steps fall into several categories:

| Type | Purpose | Configuration |
|------|---------|---------------|
| `cast` | Change data type (e.g., string → timestamp) | `column`, `to_type`, `output_column` |
| `string_op` | Simple string ops (upper, lower, trim, substring) | `column`, `op`, `params`, `output_column` |
| `math_op` | Arithmetic on numeric fields (add, subtract, multiply) | `expression`, `output_column` |
| `date_op` | Date/time calculations (add days, extract year) | `column`, `op`, `params`, `output_column` |
| `json_extract` | Extract value from JSON struct or string | `column`, `json_path`, `output_column`, `to_type` |
| `json_flatten_inline` | Flatten keys from JSON into columns in the same table | `column`, `json_schema`, `output_columns`, `keep_original` |
| `json_flatten_child` | Flatten keys/arrays into a child table | `column`, `json_schema`, `child_table`, `parent_keys`, `output_columns`, `array_path`, `keep_original` |
| `mask` | Mask sensitive fields | `column`, `strategy` (e.g., last4, hash), `output_column` |
| `expression` | Arbitrary expression using safe DSL or SQL subset | `expression`, `language`, `output_column` |
| `udf` | Apply a registered user defined function | `function`, `args`, `output_column` |

### Specification Format

The transformation pipeline specification is stored as a JSON object with a list of steps. Each step has a unique `id`, a `type`, configuration parameters specific to that type and the name of the output column(s).

**Example:**

```json
{
  "transforms": [
    {
      "id": "cast_created_at",
      "type": "cast",
      "column": "created_at",
      "to_type": "timestamp",
      "output_column": "created_at"
    },
    {
      "id": "normalize_currency",
      "type": "expression",
      "output_column": "amount_usd",
      "expression": "CASE WHEN currency = 'USD' THEN amount ELSE amount * fx_rate END",
      "language": "spark_sql"
    },
    {
      "id": "mask_card_number",
      "type": "mask",
      "column": "card_number",
      "strategy": "last4",
      "output_column": "card_number"
    },
    {
      "id": "compute_risk",
      "type": "udf",
      "function": "compute_risk_score",
      "args": ["amount_usd", "shipping_country", "device_id"],
      "output_column": "risk_score"
    }
  ]
}
```

This spec instructs the pipeline to:
1. Cast `created_at` to a timestamp
2. Compute an amount in USD using an expression referencing the original fields
3. Mask the credit card number by showing only the last four digits
4. Call a user-defined function `compute_risk_score` with three arguments to output a risk score

### Expression DSL

The `expression` type allows users to write custom calculations without coding. Fusion supports two languages:

#### Spark SQL
Users write a SQL expression using column names and built-in functions. The Spark consumer simply passes this string to `selectExpr`. Only safe deterministic functions are whitelisted.

#### Simple Expression Language (SEL)
A minimal DSL for non-technical users with functions like:
- `IF(condition, trueValue, falseValue)`
- `IN(list)`
- `SUBSTRING`
- `UPPER`
- Arithmetic operators

The system parses SEL into Spark Column expressions.

The UI can provide a code editor with syntax highlighting and autocomplete for available columns and functions. Validation occurs on save: if the expression is invalid, the user is notified immediately.

### Registered UDFs

For complex transformations beyond built-ins, engineers can register custom user-defined functions (UDFs) into the Spark environment or the CDC consumer. For example, a function might normalise addresses, compute risk scores or decode base64 strings.

UDFs are written in Python or Scala and registered with the system. Each UDF is given a name, expected argument types and return type. In the UI, users can select from a dropdown of registered UDFs and map columns to arguments.

**Example registration:**

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

@F.udf(returnType=DoubleType())
def compute_risk_score(amount_usd, shipping_country, device_id):
    # custom logic here
    risk = ...
    return risk

spark.udf.register("compute_risk_score", compute_risk_score)
```

### JSON Flattening

JSON columns are common in modern databases. Fusion treats JSON flattening as a special transformation step. The system first detects JSON string columns during schema discovery by sampling values and attempting to parse them.

When editing the stream configuration in the UI, users can choose one of three options:

#### 1. Treat as Raw String
No flattening; the column remains a JSON string.

#### 2. Flatten Inline (Mode A)
Convert the string to a struct and produce one column per key in the same table. For example, a `payload_json` with keys `a`, `b`, `c` becomes `payload_a`, `payload_b`, `payload_c`. The original string may be kept or dropped.

**JSON Flattening - Inline Mode Diagram:**

```
BEFORE (Source Table):
┌──────────┬─────────┬──────────────────────────────────────────────┐
│ order_id │  amount │          payload_json                        │
├──────────┼─────────┼──────────────────────────────────────────────┤
│   1001   │  150.00 │ {"country":"US","items":[...]}              │
│   1002   │  220.50 │ {"country":"UK","items":[...]}              │
└──────────┴─────────┴──────────────────────────────────────────────┘

                    ↓ Apply json_flatten_inline

AFTER (Destination Table):
┌──────────┬─────────┬──────────────────────────┬─────────┐
│ order_id │  amount │    payload_json          │ country │
├──────────┼─────────┼──────────────────────────┼─────────┤
│   1001   │  150.00 │ {"country":"US",...}     │   US    │
│   1002   │  220.50 │ {"country":"UK",...}     │   UK    │
└──────────┴─────────┴──────────────────────────┴─────────┘
                       (kept if keep_original=true)

Benefits:
✓ Simple queries: SELECT country FROM orders
✓ Indexable columns
✓ Type safety (country is STRING)
```

**Inline Flatten Spec:**

```json
{
  "id": "flatten_payload",
  "type": "json_flatten_inline",
  "column": "payload_json",
  "json_schema": {
    "a": "int",
    "b": "string",
    "c": "boolean",
    "d": "double"
  },
  "output_columns": {
    "a": "payload_a",
    "b": "payload_b",
    "c": "payload_c",
    "d": "payload_d"
  },
  "keep_original": true
}
```

`json_schema` defines the expected types; the system will parse the JSON and cast values accordingly. `output_columns` allows renaming of keys. `keep_original` controls whether the raw JSON string is retained.

#### 3. Flatten to Child Table (Mode B)
Parse the JSON into a separate table with a foreign key back to the parent. This mode supports both objects (1:1) and arrays (1:N). Arrays produce multiple rows in the child table, each containing the parent key and the flattened fields.

**JSON Flattening - Child Table Mode Diagram:**

```
BEFORE (Source Table):
┌──────────┬─────────┬────────────────────────────────────────────────────┐
│ order_id │  amount │          items_json                                │
├──────────┼─────────┼────────────────────────────────────────────────────┤
│   1001   │  150.00 │ [{"sku":"A1","qty":2,"price":50.00},              │
│          │         │  {"sku":"B2","qty":1,"price":50.00}]              │
├──────────┼─────────┼────────────────────────────────────────────────────┤
│   1002   │  220.50 │ [{"sku":"C3","qty":3,"price":73.50}]              │
└──────────┴─────────┴────────────────────────────────────────────────────┘

                ↓ Apply json_flatten_child
                  (creates child table orders_items)

AFTER:

Parent Table (orders):
┌──────────┬─────────┐
│ order_id │  amount │
├──────────┼─────────┤
│   1001   │  150.00 │
│   1002   │  220.50 │
└──────────┴─────────┘

Child Table (orders_items):
┌───────────┬──────┬──────────┬────────────┐
│  order_id │  sku │ quantity │ item_price │  ← parent_keys + flattened fields
├───────────┼──────┼──────────┼────────────┤
│   1001    │  A1  │    2     │   50.00    │  ← Array element 1
│   1001    │  B2  │    1     │   50.00    │  ← Array element 2
│   1002    │  C3  │    3     │   73.50    │  ← Array element 1
└───────────┴──────┴──────────┴────────────┘

Benefits:
✓ Normalized schema (1:N relationships)
✓ Easy JOINs: SELECT * FROM orders JOIN orders_items USING(order_id)
✓ Each item is a separate row with strong types
```

**Child Table Flatten Spec:**

```json
{
  "id": "flatten_items",
  "type": "json_flatten_child",
  "column": "items_json",
  "json_schema": {
    "sku": "string",
    "qty": "int",
    "price": "double"
  },
  "child_table": "orders_items",
  "parent_keys": ["order_id"],
  "output_columns": {
    "sku": "sku",
    "qty": "quantity",
    "price": "item_price"
  },
  "array_path": "$.items",
  "keep_original": false
}
```

In this example, the JSON string contains an `items` array. For each element of the array, a row is written to the `orders_items` table. Each row includes the `order_id` (from the parent) and the flattened fields `sku`, `quantity` and `item_price`. If `array_path` is omitted, the object is treated as 1:1 rather than an array.

### Expression Language: SEL vs Spark SQL

Fusion supports two expression languages for the `expression` transform type:

#### Simple Expression Language (SEL)
A custom, sandboxed DSL that provides basic operations without allowing arbitrary code execution. SEL is parsed into a syntax tree and evaluated safely.

**SEL Features:**
- Arithmetic: `+`, `-`, `*`, `/`, `%`, `^`
- Comparisons: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- String functions: `UPPER()`, `LOWER()`, `TRIM()`, `CONCAT()`, `SUBSTRING()`
- Conditionals: `IF(condition, true_val, false_val)`, `CASE WHEN ... END`
- Date functions: `DATE_ADD()`, `DATE_DIFF()`, `YEAR()`, `MONTH()`, `DAY()`

**Example SEL expressions:**
```
"IF(amount > 1000, amount * 0.9, amount)"
"UPPER(TRIM(customer_name))"
"CASE WHEN country = 'US' THEN amount ELSE amount * fx_rate END"
```

#### Spark SQL
Full Spark SQL expressions with access to all built-in functions. More powerful but requires careful validation to prevent injection attacks.

**Spark SQL Features:**
- All SEL features plus:
- Window functions: `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`
- Aggregate functions in expressions: `SUM()`, `AVG()`, `COUNT()`
- Complex type operations: `ARRAY()`, `MAP()`, `STRUCT()`
- Advanced string/date/math functions from Spark SQL catalog

**Example Spark SQL expressions:**
```sql
"CASE WHEN currency = 'USD' THEN amount ELSE amount * fx_rate END"
"DATE_ADD(created_at, INTERVAL 7 DAY)"
"CONCAT_WS('-', country_code, CAST(customer_id AS STRING))"
```

#### Comparison Table

| Feature | SEL | Spark SQL |
|---------|-----|-----------|
| **Safety** | Sandboxed, no code injection | Requires validation |
| **Performance** | Fast (simple AST evaluation) | Very fast (Catalyst optimizer) |
| **Complexity** | Basic operations only | Full SQL power |
| **Window Functions** | ❌ | ✅ |
| **Aggregations** | ❌ | ✅ |
| **UDFs** | ❌ | ✅ (via separate UDF transform) |
| **Learning Curve** | Low (simple syntax) | Medium (SQL knowledge) |
| **Use Case** | Simple transforms, masks | Complex business logic |

**Recommendation:**
- Use **SEL** for straightforward type conversions, string operations, and basic conditionals
- Use **Spark SQL** for complex expressions requiring window functions, aggregations, or advanced date manipulation
- Both are validated at configuration time to catch syntax errors before activation

### Executing Transformations in Spark

Spark Structured Streaming reads from Redis streams and processes individual events. The following illustrates how the transformation pipeline is applied:

1. **Read & Parse**: Events are read as JSON strings. Spark uses a schema (matching the canonical envelope) to parse them into a DataFrame.

2. **Filter & Explode**: Based on the stream key, Spark partitions by table and applies the selected stream configuration (disabled tables are skipped).

3. **Apply Steps**: For each row, Spark iterates over the transform steps defined for the stream:
   - For `cast`, `string_op`, `math_op` and `date_op`, Spark uses built-in functions (e.g., `col`, `cast`, `upper`, `expr`)
   - For `json_extract` and flatten steps, Spark uses `from_json`, `get_json_object`, `explode` and other JSON functions. JSON parsing errors are logged and cause rows to be sent to a DLQ or marked failed
   - For `expression`, Spark uses `selectExpr` or a custom parser to convert the expression into a column expression
   - For `udf`, Spark calls the registered UDF with the specified arguments

4. **Select Output**: The transformed DataFrame now has new columns. Depending on the connection's sync mode and destination, Spark then writes the result to Postgres (via JDBC with upsert logic) or Iceberg (via the Iceberg Spark connector).

**Simplified PySpark snippet:**

```python
from pyspark.sql import functions as F

df = spark.readStream.format("redis")...

for step in transform_spec["transforms"]:
    if step["type"] == "cast":
        df = df.withColumn(step["output_column"], 
                          df[step["column"]].cast(step["to_type"]))
    elif step["type"] == "expression":
        df = df.withColumn(step["output_column"], 
                          F.expr(step["expression"]))
    elif step["type"] == "json_extract":
        df = df.withColumn(step["output_column"],
                          F.get_json_object(df[step["column"]], 
                                          step["json_path"]).cast(step["to_type"]))
    # ... handle other types ...

# After applying transforms, write to sink
df.writeStream.foreachBatch(lambda batch_df, epoch_id: load_sink(batch_df)).start()
```

### Registered User-Defined Functions (UDFs)

For complex transformations beyond built-in functions, engineers can register custom **User-Defined Functions (UDFs)** into the Spark environment or the CDC consumer. UDFs enable custom business logic such as:

- Normalizing addresses or phone numbers
- Computing risk scores based on multiple factors
- Decoding base64 or encrypted strings  
- Custom date/time parsing for non-standard formats
- Applying proprietary algorithms or models

UDFs are written in **Python or Scala** and registered with the system. Each UDF is given a:
- **Name**: Unique identifier for the function
- **Expected argument types**: Column types the function accepts
- **Return type**: Type of the computed result

In the UI, users can select from a **dropdown of registered UDFs** and map source columns to the function arguments.

#### UDF Registration Example (Python)

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

@F.udf(returnType=DoubleType())
def compute_risk_score(amount_usd, shipping_country, device_id):
    """
    Custom risk scoring logic based on transaction details
    """
    risk = 0.0
    
    # High-risk countries
    if shipping_country in ['XX', 'YY', 'ZZ']:
        risk += 0.3
    
    # Large transactions
    if amount_usd > 1000:
        risk += 0.2
    
    # Known suspicious device patterns
    if device_id and device_id.startswith('bot_'):
        risk += 0.5
    
    return min(risk, 1.0)  # Cap at 1.0

# Register UDF with Spark
spark.udf.register("compute_risk_score", compute_risk_score)
```

#### UDF Registration Example (Scala)

```scala
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType

val normalizePhone = udf((phone: String) => {
  // Remove all non-digits
  val digits = phone.replaceAll("[^0-9]", "")
  // Format as +1-XXX-XXX-XXXX
  if (digits.length == 10) {
    s"+1-${digits.substring(0,3)}-${digits.substring(3,6)}-${digits.substring(6)}"
  } else {
    phone  // Return original if invalid
  }
})

spark.udf.register("normalize_phone", normalizePhone)
```

#### Using UDFs in Transform Spec

Once registered, UDFs appear in the metadata service's UDF catalog. The transformation spec references them:

```json
{
  "id": "compute_risk",
  "type": "udf",
  "function": "compute_risk_score",
  "args": ["amount_usd", "country", "device_id"],
  "output_column": "risk_score"
}
```

#### UDF Management Workflow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      UDF REGISTRATION & DEPLOYMENT                        │
└──────────────────────────────────────────────────────────────────────────┘

1. Engineer writes UDF
   ┌──────────────────────────────────┐
   │ def my_custom_function(...):     │
   │     # business logic             │
   │     return result                │
   └────────────┬─────────────────────┘
                │
                ▼
2. Register in Spark initialization script
   ┌──────────────────────────────────┐
   │ spark.udf.register(              │
   │   "my_custom_function",          │
   │   my_custom_function             │
   │ )                                │
   └────────────┬─────────────────────┘
                │
                ▼
3. Add metadata to UDF catalog
   ┌──────────────────────────────────┐
   │ INSERT INTO udf_catalog VALUES   │
   │ ('my_custom_function',           │
   │  'Computes X from Y',            │
   │  ['col1:string', 'col2:int'],    │
   │  'double')                       │
   └────────────┬─────────────────────┘
                │
                ▼
4. Deploy updated Spark image
   ┌──────────────────────────────────┐
   │ Build Docker image with UDF code │
   │ Push to registry                 │
   │ kubectl rollout restart          │
   │   deployment/spark-consumer      │
   └────────────┬─────────────────────┘
                │
                ▼
5. UI shows UDF in dropdown
   ┌──────────────────────────────────┐
   │ Available UDFs:                  │
   │ ☑ compute_risk_score             │
   │ ☑ normalize_phone                │
   │ ☑ my_custom_function             │
   └────────────┬─────────────────────┘
                │
                ▼
6. User selects & maps columns
   ┌──────────────────────────────────┐
   │ Function: my_custom_function     │
   │ Arg 1 (string): customer_name    │
   │ Arg 2 (int): order_count         │
   │ Output column: custom_score      │
   └──────────────────────────────────┘
```

#### UDF Best Practices

- **Keep UDFs pure**: No side effects, no external API calls within UDF (slow)
- **Handle nulls gracefully**: Check for None/null inputs
- **Use appropriate return types**: Matches destination schema
- **Test thoroughly**: Unit test UDFs independently before registration
- **Document parameters**: Clear docstrings for UI tooltips
- **Version control**: Track UDF changes in git alongside transform specs
- **Performance**: For heavy computation, consider Pandas UDFs (vectorized)

#### Pandas UDF Example (High Performance)

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(DoubleType())
def compute_risk_batch(amounts: pd.Series, countries: pd.Series) -> pd.Series:
    """
    Vectorized UDF processes entire column at once (much faster)
    """
    risk = pd.Series(0.0, index=amounts.index)
    risk[amounts > 1000] += 0.2
    risk[countries.isin(['XX', 'YY'])] += 0.3
    return risk.clip(0, 1.0)

spark.udf.register("compute_risk_batch", compute_risk_batch)
```

### Transformation Previews

The UI provides a **Preview** feature when configuring a connection. It displays a sample of rows from the source table, applies the selected transform steps and shows the resulting columns.

This is achieved by running the transformation pipeline on a small DataFrame built from a query against the source database (for scheduled connections) or by sampling the latest CDC events (for realtime connections). Preview results update instantly as the user toggles transform steps.

### Role of the CDC Engine

The Python CDC engine is intentionally kept lean: it does not execute complex transformations. It only adds minimal normalisation (e.g., cast dates to ISO strings) and attaches the transform pipeline ID to events.

All heavy transforms run in Spark, ensuring consistency across realtime and scheduled modes and offloading CPU intensive operations to the distributed cluster. This division of labour also simplifies unit testing of transformations: the pipeline specification can be tested independently of the CDC engine.

### Complete Example

To illustrate the power of the transformation system, consider a table `orders` with columns: `id`, `amount`, `currency`, `created_at`, `payload_json`, `card_number`, `device_id`.

We want to:
1. Cast `created_at` from string to timestamp
2. Convert `amount` to USD using an FX rate
3. Mask the credit card number leaving only the last four digits
4. Extract `country` and `items` from `payload_json`. `country` is flattened inline; `items` is an array flattened into a child table
5. Compute a custom `risk_score` using a UDF

#### Complete Transformation Example Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    COMPLETE TRANSFORMATION PIPELINE EXAMPLE                   │
└──────────────────────────────────────────────────────────────────────────────┘

SOURCE TABLE: orders
┌────┬────────┬──────────┬─────────────────────┬─────────────────┬──────────────┬───────────┐
│ id │ amount │ currency │    created_at       │  card_number    │ payload_json │ device_id │
├────┼────────┼──────────┼─────────────────────┼─────────────────┼──────────────┼───────────┤
│ 1  │ 100.00 │   EUR    │ "2025-11-28 10:00" │ 4532-1234-5678  │ {"country":  │ dev_123   │
│    │        │          │                     │ -9012           │  "US",       │           │
│    │        │          │                     │                 │  "items":[   │           │
│    │        │          │                     │                 │   {sku:...}]}│           │
└────┴────────┴──────────┴─────────────────────┴─────────────────┴──────────────┴───────────┘

                              ↓ TRANSFORMATION PIPELINE

┌─────────────────────────────────────────────────────────────────────────────┐
│ Transform 1: cast_created_at                                                 │
│ Type: cast                                                                   │
│ created_at: "2025-11-28 10:00" → TIMESTAMP(2025-11-28T10:00:00Z)           │
└─────────────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Transform 2: fx_conversion                                                   │
│ Type: expression (SEL)                                                       │
│ IF(currency = 'USD', amount, amount * fx_rate)                              │
│ amount_usd = 100.00 * 1.10 = 110.00 (EUR → USD conversion)                 │
└─────────────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Transform 3: mask_card                                                       │
│ Type: mask (strategy: last4)                                                │
│ card_number: "4532-1234-5678-9012" → "****-****-****-9012"                 │
└─────────────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Transform 4: extract_country                                                 │
│ Type: json_extract                                                           │
│ JSON path: $.country                                                         │
│ Extract "US" from payload_json → country = "US"                            │
└─────────────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Transform 5: flatten_items                                                   │
│ Type: json_flatten_child                                                     │
│ Array path: $.items                                                          │
│ Creates child table: orders_items                                           │
│ Flattens: [{"sku":"A1","qty":2,"price":50}, {"sku":"B2","qty":1,"price":60}]│
└─────────────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Transform 6: compute_risk                                                    │
│ Type: udf (function: compute_risk_score)                                    │
│ Args: [amount_usd, country, device_id]                                      │
│ risk_score = compute_risk_score(110.00, "US", "dev_123") → 0.35            │
└─────────────────────────────────────────────────────────────────────────────┘

                              ↓ RESULT

DESTINATION - MAIN TABLE: orders_transformed
┌────┬─────────────────────┬────────────┬─────────┬──────────────────┬────────────┬──────────────┐
│ id │    created_at       │ amount_usd │ country │   card_number    │ risk_score │ payload_json │
├────┼─────────────────────┼────────────┼─────────┼──────────────────┼────────────┼──────────────┤
│ 1  │2025-11-28T10:00:00Z │   110.00   │   US    │ ****-****-9012   │    0.35    │ {"country":  │
│    │                     │            │         │                  │            │  "US",...}   │
└────┴─────────────────────┴────────────┴─────────┴──────────────────┴────────────┴──────────────┘

DESTINATION - CHILD TABLE: orders_items
┌───────────┬──────────┬──────────┬────────────┐
│ parent_id │ item_sku │ item_qty │ item_price │
├───────────┼──────────┼──────────┼────────────┤
│     1     │    A1    │    2     │   50.00    │
│     1     │    B2    │    1     │   60.00    │
└───────────┴──────────┴──────────┴────────────┘

TRANSFORMATION BENEFITS:
✓ Type safety: created_at is now TIMESTAMP
✓ Currency normalization: all amounts in USD
✓ PII protection: card numbers masked
✓ JSON flattened: country queryable as column
✓ Normalized structure: items in separate table (1:N)
✓ Business logic: risk score computed via UDF
```

**Transformation spec (simplified):**

```json
{
  "transforms": [
    {"id": "cast_created_at", "type": "cast", "column": "created_at", "to_type": "timestamp", "output_column": "created_at"},
    {"id": "fx_conversion", "type": "expression", "output_column": "amount_usd", "expression": "IF(currency = 'USD', amount, amount * fx_rate)", "language": "SEL"},
    {"id": "mask_card", "type": "mask", "column": "card_number", "strategy": "last4", "output_column": "card_number"},
    {"id": "extract_country", "type": "json_extract", "column": "payload_json", "json_path": "$.country", "output_column": "country", "to_type": "string"},
    {"id": "flatten_items", "type": "json_flatten_child", "column": "payload_json", "json_schema": {"sku": "string", "qty": "int", "price": "double"}, "child_table": "orders_items", "parent_keys": ["id"], "output_columns": {"sku": "item_sku", "qty": "item_qty", "price": "item_price"}, "array_path": "$.items", "keep_original": true},
    {"id": "compute_risk", "type": "udf", "function": "compute_risk_score", "args": ["amount_usd", "country", "device_id"], "output_column": "risk_score"}
  ]
}
```

When applied, the main table `orders_transformed` would have columns: `id`, `created_at`, `amount_usd`, `country`, `card_number`, `risk_score`, `payload_json`, and the child table `orders_items` would have columns: `parent_id`, `item_sku`, `item_qty`, `item_price`.

---

## Schema Evolution

### Schema Discovery

Upon source registration, the engine introspects the database or collection to retrieve:
- Schemas/databases and their tables/collections
- Column names, data types, nullability and default values
- Detection of JSON-like string columns by sampling values
- Primary keys or unique indexes (if available)

For relational databases, the engine queries the `information_schema` tables; for MongoDB, it samples a number of documents to infer field names and types. The result is stored in the `discovery_cache` associated with the Source. This cache allows quick rendering of table/column lists in the UI and forms the baseline for detecting changes.

### Periodic Re-Introspection

The engine periodically (e.g., daily) performs a light re-introspection to detect schema changes. It compares the newly discovered schema with the previous version and identifies:

- **New columns** – added since last introspection
- **Removed columns** – missing in the current schema
- **Type changes** – column type has changed (e.g., INT → VARCHAR)
- **Table/collection additions or removals**

These changes are recorded as **Schema Change Events** in a dedicated table. Each event includes the affected entity, change type, time and an optional diff of the old vs new definition. A background job processes these events and, depending on the connection's policy, either applies them automatically or requires user approval.

### Schema Evolution Policies

Connections can be configured with a `schema_evolution_policy` of:

#### AUTO_APPLY
All new columns detected during re-introspection automatically propagate to the transformation spec and destination schema. Type changes may also be applied if compatible (e.g., INT → FLOAT). Removed columns are marked as deprecated but remain in the destination for history.

#### MANUAL_APPROVAL
Schema changes are held for review. The UI displays a list of pending changes. Users can approve or reject individual columns. Approved additions result in corresponding transform spec updates. If a column is removed at the source, the system may mark it deprecated but stop reading values.

When a schema change is approved or applied, an event is sent to the Spark consumer so that it can reload its schema and update its transformation logic. For CDC streams, the change is applied mid-stream without pausing ingestion. For scheduled syncs, the new schema will take effect on the next run.

### Schema Change Notification Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                   SCHEMA EVOLUTION - CHANGE DETECTION FLOW                    │
└──────────────────────────────────────────────────────────────────────────────┘

 ┌─────────────────────────────────────────────────────────┐
 │  Periodic Re-Introspection Job (e.g., Daily Cron)       │
 │  - Query information_schema / sample MongoDB docs       │
 └──────────────────────────┬──────────────────────────────┘
                            │
                            ▼
          ┌──────────────────────────────────┐
          │  Compare with Cached Schema      │
          │  (stored in discovery_cache)     │
          └──────────┬───────────────────────┘
                     │
                     │ Detect changes
                     ▼
     ┌───────────────────────────────────────────┐
     │  Schema Change Detected:                  │
     │  - New column: "email" VARCHAR(255)       │
     │  - Removed column: "legacy_field"         │
     │  - Type change: "amount" INT → DECIMAL    │
     └───────────────┬───────────────────────────┘
                     │
                     ▼
     ┌───────────────────────────────────────────┐
     │  Create SchemaChangeEvent Record          │
     │  {                                         │
     │    "source_id": "src1",                   │
     │    "table": "orders",                     │
     │    "change_type": "COLUMN_ADDED",         │
     │    "column": "email",                     │
     │    "data_type": "VARCHAR(255)",           │
     │    "detected_at": "2025-11-28T10:00:00Z" │
     │  }                                         │
     └───────────────┬───────────────────────────┘
                     │
                     │
         ┌───────────┴────────────┐
         │                        │
         ▼                        ▼
┌────────────────────┐   ┌────────────────────┐
│  AUTO_APPLY Policy │   │ MANUAL_APPROVAL    │
│                    │   │      Policy        │
└────────┬───────────┘   └────────┬───────────┘
         │                        │
         │                        │
         ▼                        ▼
┌──────────────────────────┐  ┌─────────────────────────────┐
│ Automatic Application:   │  │ Pending Review:             │
│                          │  │                             │
│ 1. Update Transform Spec │  │ 1. Store change as PENDING  │
│    - Add "email" column  │  │ 2. Notify UI dashboard      │
│ 2. Update Destination    │  │ 3. Wait for user action     │
│    Schema (add column)   │  │                             │
│ 3. Send Webhook to Spark │  │    User Reviews in UI:      │
│    Consumer: "reload"    │  │    ┌─────────────────┐      │
│                          │  │    │ ✓ Approve       │      │
│                          │  │    │ ✗ Reject        │      │
│                          │  │    └────────┬────────┘      │
└──────────┬───────────────┘  └─────────────┼──────────────┘
           │                                 │
           │                                 │ If approved
           │                                 ▼
           │                  ┌─────────────────────────────┐
           │                  │ Apply Change:               │
           │                  │ 1. Update Transform Spec    │
           │                  │ 2. Update Destination Schema│
           │                  │ 3. Send Webhook to Spark    │
           │                  └──────────┬──────────────────┘
           │                             │
           └─────────────────┬───────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │  Spark Consumer Receives Reload Signal   │
          │  - Reloads transformation spec from API  │
          │  - Updates in-memory schema              │
          │  - For streaming: applies mid-stream     │
          │  - For batch: next run uses new schema   │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │  Log SchemaChangeApplied Event           │
          │  - Audit trail for compliance            │
          │  - Emit metric: schema_changes_applied   │
          └──────────────────────────────────────────┘

EXAMPLES:

New Column Added:
─────────────────
Source: ALTER TABLE orders ADD COLUMN email VARCHAR(255);
Detection: Re-introspection finds "email" column
AUTO_APPLY: Automatically add to transform spec & destination
MANUAL: Show in UI, user approves, then apply

Column Removed:
───────────────
Source: ALTER TABLE orders DROP COLUMN legacy_field;
Detection: Column missing in new introspection
AUTO_APPLY: Mark as deprecated, stop reading values
MANUAL: User decides whether to keep in destination or drop

Type Change:
────────────
Source: ALTER TABLE orders MODIFY COLUMN amount DECIMAL(10,2);
Detection: Data type changed from INT to DECIMAL
AUTO_APPLY: If compatible (INT → DECIMAL), apply automatically
MANUAL: User reviews and approves type change
```

1. **Discovery detects a change** during periodic re-introspection
2. A **SchemaChangeEvent** record is created with details of the change
3. For `AUTO_APPLY`, the Metadata Service immediately updates the connection's transformation spec (e.g., adds the new column to a flatten step) and sends a webhook to the Spark consumer to reload
4. For `MANUAL_APPROVAL`, the change remains pending. The UI displays the change and allows the user to accept or decline. Acceptance triggers the update described above
5. After the change is applied, the system logs a **SchemaChangeApplied** event for auditing and metrics

---

## Data Quality & Metrics

### DQ Policy Model

A **DQ Policy** defines rules that must be satisfied by data produced through a connection or stream. Each policy has an identifier and a list of rules. Rules can be attached at the connection or stream level. They evaluate batches of transformed data in Spark and emit metrics and status flags.

### Rule Types

The following rule types are supported:

| Rule Type | Purpose | Configuration Parameters |
|-----------|---------|-------------------------|
| `row_count_match` | Compare source and destination row counts (full loads) | `threshold` (e.g., ±5%), `max_delay` |
| `null_ratio_check` | Ensure null ratio for a column is below a threshold | `column`, `max_null_ratio` |
| `range_check` | Ensure values fall within a numeric or timestamp range | `column`, `min_value`, `max_value` |
| `regex_check` | Ensure strings match a regular expression | `column`, `pattern` |
| `freshness_check` | Ensure latest timestamp is within a freshness window | `column`, `max_age_seconds` |
| `enum_check` | Ensure value belongs to an allowed set | `column`, `allowed_values` |
| `referential_integrity` | Ensure foreign key values exist in parent table | `column`, `parent_table`, `parent_key` |

Rules produce one of three statuses: **PASS**, **WARN** or **FAIL** depending on whether the computed metric falls within configured thresholds. The policy can specify actions on FAIL such as blocking the write to the destination, generating alerts or sending events to a DLQ.

### DQ Policy Specification

The specification is stored in JSON. Example:

```json
{
  "dq_policy_id": "dq_orders",
  "rules": [
    {"type": "row_count_match", "threshold": 0.02},
    {"type": "null_ratio_check", "column": "amount_usd", "max_null_ratio": 0.01},
    {"type": "range_check", "column": "risk_score", "min_value": 0.0, "max_value": 1.0},
    {"type": "freshness_check", "column": "created_at", "max_age_seconds": 3600}
  ],
  "on_fail": "alert" // other options: "block", "continue"
}
```

Spark applies these rules after transformations but before writing data to the sink. For example, the `row_count_match` rule only applies to full load tasks: it queries the source table to count rows, compares with the batch DataFrame's row count and raises a warning if the difference exceeds 2%. The `freshness_check` computes the difference between the current time and the maximum value in the `created_at` column. If older than one hour, it triggers an alert.

### Data Quality Rule Evaluation Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY RULE EVALUATION PIPELINE                      │
└──────────────────────────────────────────────────────────────────────────────┘

 ┌────────────────────────────────────┐
 │  Spark Batch DataFrame             │
 │  (After Transformations Applied)   │
 │                                    │
 │  Columns:                          │
 │  - order_id, amount_usd,           │
 │    created_at, risk_score, ...     │
 └──────────────┬─────────────────────┘
                │
                ▼
 ┌────────────────────────────────────────────────────────┐
 │  Load DQ Policy for Connection                         │
 │  (from Metadata Service)                               │
 │                                                        │
 │  {                                                     │
 │    "dq_policy_id": "dq_orders",                       │
 │    "rules": [                                         │
 │      {"type": "row_count_match", ...},                │
 │      {"type": "null_ratio_check", ...},               │
 │      {"type": "range_check", ...},                    │
 │      {"type": "freshness_check", ...}                 │
 │    ],                                                 │
 │    "on_fail": "alert"                                 │
 │  }                                                     │
 └────────────────────────┬───────────────────────────────┘
                          │
                          │ Evaluate each rule
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         RULE EVALUATION (in Spark)                            │
│                                                                               │
│  Rule 1: row_count_match (threshold: 0.02)                                   │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │ 1. Count source rows: SELECT COUNT(*) FROM source_table     │            │
│  │    Result: 10,000                                           │            │
│  │ 2. Count batch DataFrame rows: df.count()                   │            │
│  │    Result: 9,950                                            │            │
│  │ 3. Calculate difference: |10000 - 9950| / 10000 = 0.005     │            │
│  │ 4. Compare to threshold: 0.005 < 0.02 → PASS ✓             │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                                                                               │
│  Rule 2: null_ratio_check (column: amount_usd, max_null_ratio: 0.01)        │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │ 1. Count nulls: df.filter(col("amount_usd").isNull()).count()│           │
│  │    Result: 5                                                │            │
│  │ 2. Total rows: 9,950                                        │            │
│  │ 3. Null ratio: 5 / 9950 = 0.0005                            │            │
│  │ 4. Compare: 0.0005 < 0.01 → PASS ✓                         │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                                                                               │
│  Rule 3: range_check (column: risk_score, min: 0.0, max: 1.0)               │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │ 1. Find violations:                                         │            │
│  │    df.filter((col("risk_score") < 0.0) |                   │            │
│  │              (col("risk_score") > 1.0)).count()            │            │
│  │    Result: 3 rows                                           │            │
│  │ 2. Violation ratio: 3 / 9950 = 0.0003                       │            │
│  │ 3. Threshold exceeded? → WARN ⚠                            │            │
│  │ 4. Log violations to DLQ                                    │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                                                                               │
│  Rule 4: freshness_check (column: created_at, max_age_seconds: 3600)        │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │ 1. Find max timestamp: df.agg(max("created_at")).collect()  │            │
│  │    Result: 2025-11-28T09:45:00Z                             │            │
│  │ 2. Current time: 2025-11-28T10:00:00Z                       │            │
│  │ 3. Age in seconds: (now - max_ts) = 900 seconds             │            │
│  │ 4. Compare: 900 < 3600 → PASS ✓                            │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                                                                               │
│  OVERALL STATUS:                                                              │
│  - Rule 1: PASS                                                               │
│  - Rule 2: PASS                                                               │
│  - Rule 3: WARN (3 violations)                                                │
│  - Rule 4: PASS                                                               │
│                                                                               │
│  Policy Action: on_fail = "alert" → Send warning (not blocking)              │
└────────────────────────────┬──────────────────────────────────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │  Emit Prometheus Metrics                 │
          │  - dq_rule_status{rule="row_count"} = 1  │
          │  - dq_rule_status{rule="null_ratio"} = 1 │
          │  - dq_rule_status{rule="range"} = 0      │ ← WARN
          │  - dq_rule_status{rule="freshness"} = 1  │
          │  - dq_rule_violation_total{} += 3        │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │  Take Action Based on Policy             │
          │                                          │
          │  on_fail = "block":                      │
          │    → Stop write, send to DLQ             │
          │                                          │
          │  on_fail = "alert":                      │
          │    → Send warning, continue write        │
          │                                          │
          │  on_fail = "continue":                   │
          │    → Log only, no alert                  │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │  Write to Destination                    │
          │  - Upsert to Postgres Warehouse          │
          │  - Commit to Iceberg                     │
          │                                          │
          │  (If not blocked by DQ failure)          │
          └──────────────────────────────────────────┘
```

### Sync Quality Metrics

Sync quality refers to metrics describing the real-time health of CDC ingestion and loading. Key metrics include:

#### Lag
Difference in seconds between the current wall clock and the timestamp of the last processed event for a stream. Calculated as `(now() - max(ts_ms))`. High lag indicates processing backlog.

#### Throughput
Number of events processed per second. Reported per tenant, per source and per stream.

#### Error Rate
Number of errors per unit time. Errors can be classified by type (`connection error`, `parse error`, `transform error`, `sink error`). High error rates may indicate misconfiguration or data issues.

#### Checkpoint Age
Time since the last checkpoint commit. Large values may indicate that checkpointing is stuck or the worker is not flushing state.

#### Memory & CPU Usage
Captured via Kubernetes metrics. May help tune resource requests and spot leaks.

### Prometheus Instrumentation

All components in Fusion CDC Engine expose metrics via HTTP endpoints. Prometheus scrapes these endpoints and stores time series data. The following list summarises key metrics:

| Metric Name | Labels | Description |
|-------------|--------|-------------|
| `cdc_events_total` | tenant, source, stream, op | Total number of events processed |
| `cdc_stream_lag_seconds` | tenant, source, stream | Event time lag |
| `cdc_stream_throughput_per_second` | tenant, source, stream | Events per second |
| `cdc_errors_total` | tenant, source, error_type | Total number of errors |
| `cdc_checkpoint_age_seconds` | tenant, source | Time since last checkpoint flush |
| `cdc_sqlite_checkpoint_size_bytes` | tenant, source | Size of local checkpoint file |
| `cdc_fallback_queue_length` | tenant, source | Number of events in fallback queue |
| `dq_rule_status` | tenant, connection, rule_type | Gauge: 1=PASS, 0=WARN, -1=FAIL |
| `dq_rule_violation_total` | tenant, connection, rule_type | Total number of rule violations |
| `dq_freshness_seconds` | tenant, connection | Freshness metric (e.g., max timestamp age) |
| `api_request_duration_seconds` | method, endpoint | Latency of control plane API calls |
| `api_request_count` | method, endpoint, status | Number of requests |

These metrics feed dashboards showing per-tenant health, system throughput and quality trends. Operators can configure Alertmanager rules such as:

- Alert when `cdc_stream_lag_seconds` exceeds 300 seconds for any stream
- Alert when `cdc_errors_total` increases rapidly
- Alert when `dq_rule_status` is -1 (FAIL) for critical rules

### Structured Logging

In addition to metrics, components emit structured logs in JSON format with fields such as `tenant_id`, `source_id`, `stream`, `event_id`, `log_level`, `message`, `error_type` and `trace_id`.

A distributed tracing ID is propagated across services and attached to log entries, allowing end-to-end tracing of a single event from source to sink. Logs are ingested into a log management system (e.g., ELK, Loki) where they can be searched and correlated with metrics.

---

## Deployment

### Deployment Targets

Fusion is designed to run in multiple environments:

#### 1. On-Premises
Deployed onto bare-metal servers or a private Kubernetes cluster. Databases, Redis and Postgres may be hosted on dedicated machines. Persistent volumes map to local disks or network-attached storage.

#### 2. Single Cloud Region
Deployed on a managed Kubernetes service (e.g., GKE, AKS, EKS) using cloud storage (S3, Azure Blob) and managed databases (RDS, Cloud SQL). Persistent volumes use block storage (EBS, Premium Disk) and secrets are stored in cloud key vaults.

#### 3. Multi-Region
Control plane services can run active-active or active-passive across regions. Data plane workers are deployed near their source databases to minimise latency. Redis and Postgres use replication or managed multi-region services. Cross-region replication of Iceberg tables is configured at the storage layer.

### Kubernetes Deployment Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    KUBERNETES DEPLOYMENT - SINGLE REGION                      │
└──────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          KUBERNETES CLUSTER (GKE/AKS/EKS)                    │
│                                                                              │
│  Namespace: fusion-control-plane                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐   │ │
│  │  │  UI & API       │  │   Metadata      │  │   Scheduler &       │   │ │
│  │  │  Gateway        │  │   Service       │  │   Orchestrator      │   │ │
│  │  │  Deployment     │  │   StatefulSet   │  │   Deployment        │   │ │
│  │  │  Replicas: 3    │  │   Replicas: 1   │  │   Replicas: 2       │   │ │
│  │  │                 │  │                 │  │                     │   │ │
│  │  │  Resources:     │  │  PVC:           │  │   Resources:        │   │ │
│  │  │  CPU: 1000m     │  │  postgres-data  │  │   CPU: 500m         │   │ │
│  │  │  Memory: 2Gi    │  │  Size: 100Gi    │  │   Memory: 1Gi       │   │ │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘   │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │  │  Ingress Controller (NGINX)                                      │  │ │
│  │  │  - fusion.example.com → UI Service                          │  │ │
│  │  │  - api.fusion.example.com → API Service                     │  │ │
│  │  │  - TLS Termination (cert-manager)                               │  │ │
│  │  └─────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Namespace: fusion-data-plane                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐    │ │
│  │  │  MySQL Workers   │  │ Postgres Workers │  │ MongoDB Workers  │    │ │
│  │  │  StatefulSet     │  │  StatefulSet     │  │  StatefulSet     │    │ │
│  │  │  Replicas: 5     │  │  Replicas: 3     │  │  Replicas: 2     │    │ │
│  │  │                  │  │                  │  │                  │    │ │
│  │  │  PVC per pod:    │  │  PVC per pod:    │  │  PVC per pod:    │    │ │
│  │  │  checkpoint-vol  │  │  checkpoint-vol  │  │  checkpoint-vol  │    │ │
│  │  │  Size: 10Gi      │  │  Size: 10Gi      │  │  Size: 10Gi      │    │ │
│  │  │                  │  │                  │  │                  │    │ │
│  │  │  Resources:      │  │  Resources:      │  │  Resources:      │    │ │
│  │  │  CPU: 500m       │  │  CPU: 500m       │  │  CPU: 500m       │    │ │
│  │  │  Memory: 1Gi     │  │  Memory: 1Gi     │  │  Memory: 1Gi     │    │ │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘    │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │  │  HorizontalPodAutoscaler (HPA)                                   │  │ │
│  │  │  - Target: CPU 70%                                               │  │ │
│  │  │  - Min Replicas: 2, Max Replicas: 10                            │  │ │
│  │  └─────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Namespace: fusion-processing                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐ │ │
│  │  │  Spark Operator                                                   │ │ │
│  │  │  - Manages Spark Application CRDs                                │ │ │
│  │  │  - Dynamic executor allocation                                   │ │ │
│  │  └──────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  Spark Applications (one per realtime connection):                     │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │ │
│  │  │ Spark App 1 │  │ Spark App 2 │  │ Spark App 3 │  ...              │ │
│  │  │ Driver Pod  │  │ Driver Pod  │  │ Driver Pod  │                   │ │
│  │  │   +         │  │   +         │  │   +         │                   │ │
│  │  │ Executors   │  │ Executors   │  │ Executors   │                   │ │
│  │  │ (dynamic)   │  │ (dynamic)   │  │ (dynamic)   │                   │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                   │ │
│  │                                                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Namespace: fusion-monitoring                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  ┌───────────────┐  ┌───────────────┐  ┌──────────────────────────┐  │ │
│  │  │  Prometheus   │  │ Alertmanager  │  │  Grafana Dashboards      │  │ │
│  │  │  StatefulSet  │  │  Deployment   │  │  Deployment              │  │ │
│  │  └───────────────┘  └───────────────┘  └──────────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

EXTERNAL SERVICES (Managed or Separate VMs):
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────────────┐  │
│  │  Redis Cluster   │  │  Postgres        │  │  Object Storage         │  │
│  │  (Elasticache/   │  │  (RDS/CloudSQL)  │  │  (S3/Azure Blob/GCS)    │  │
│  │   Azure Cache)   │  │                  │  │                         │  │
│  │  3 masters       │  │  Primary + 2     │  │  Iceberg Tables:        │  │
│  │  3 replicas      │  │  Read Replicas   │  │  - warehouse/           │  │
│  │                  │  │                  │  │  - metadata/            │  │
│  │  Multi-AZ        │  │  Multi-AZ        │  │  - checkpoints/         │  │
│  └──────────────────┘  └──────────────────┘  └─────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Multi-Region Deployment Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          MULTI-REGION ARCHITECTURE                            │
└──────────────────────────────────────────────────────────────────────────────┘

REGION 1: US-EAST-1                          REGION 2: EU-WEST-1
┌────────────────────────────────────┐       ┌────────────────────────────────┐
│  CONTROL PLANE (Active-Active)     │       │  CONTROL PLANE (Active-Active) │
│  ┌──────────────────────────────┐  │       │  ┌──────────────────────────┐ │
│  │  UI/API Gateway              │◄─┼───────┼─►│  UI/API Gateway          │ │
│  │  - Global Load Balancer      │  │  Sync │  │  - Global Load Balancer  │ │
│  └──────────────────────────────┘  │       │  └──────────────────────────┘ │
│                                     │       │                                │
│  ┌──────────────────────────────┐  │       │  ┌──────────────────────────┐ │
│  │  Metadata Service            │  │       │  │  Metadata Service        │ │
│  │  Postgres (Primary)          │◄─┼───────┼─►│  Postgres (Read Replica) │ │
│  └──────────────────────────────┘  │  Repl │  └──────────────────────────┘ │
│                                     │       │                                │
│  DATA PLANE                         │       │  DATA PLANE                    │
│  ┌──────────────────────────────┐  │       │  ┌──────────────────────────┐ │
│  │  Workers → US Source DBs     │  │       │  │  Workers → EU Source DBs │ │
│  │  - MySQL (us-east-1)         │  │       │  │  - MySQL (eu-west-1)     │ │
│  │  - Postgres (us-east-1)      │  │       │  │  - Postgres (eu-west-1)  │ │
│  └──────────────┬───────────────┘  │       │  └──────────────┬───────────┘ │
│                 │                   │       │                 │              │
│                 ▼                   │       │                 ▼              │
│  ┌──────────────────────────────┐  │       │  ┌──────────────────────────┐ │
│  │  Redis Cluster (US)          │  │       │  │  Redis Cluster (EU)      │ │
│  │  - No cross-region sync      │  │       │  │  - No cross-region sync  │ │
│  └──────────────┬───────────────┘  │       │  └──────────────┬───────────┘ │
│                 │                   │       │                 │              │
│                 ▼                   │       │                 ▼              │
│  ┌──────────────────────────────┐  │       │  ┌──────────────────────────┐ │
│  │  Spark Consumers (US)        │  │       │  │  Spark Consumers (EU)    │ │
│  └──────────────┬───────────────┘  │       │  └──────────────┬───────────┘ │
│                 │                   │       │                 │              │
│                 ▼                   │       │                 ▼              │
│  ┌──────────────────────────────┐  │       │  ┌──────────────────────────┐ │
│  │  Iceberg on S3 (us-east-1)   │◄─┼───────┼─►│  Iceberg on S3           │ │
│  │  - Warehouse tables          │  │ Cross │  │  (cross-region replica)  │ │
│  └──────────────────────────────┘  │ Repl  │  └──────────────────────────┘ │
│                                     │       │                                │
└─────────────────────────────────────┘       └────────────────────────────────┘
                                     
BENEFITS:
✓ Low latency: Workers near source DBs
✓ Data sovereignty: EU data stays in EU region
✓ High availability: Control plane active-active
✓ Disaster recovery: Cross-region replication of Iceberg
✓ Read scaling: Read replicas in both regions
```

### Control Plane Deployment

Control plane services include the UI/API gateway, metadata service, scheduler/orchestrator and metrics/alerting. These can be packaged together or as separate microservices depending on scale.

Key considerations:

#### Stateless vs Stateful
The API gateway and orchestrator are stateless; they can be scaled horizontally. The metadata service requires a stateful Postgres instance. Use managed Postgres or run Postgres on K8s with a StatefulSet and a persistent volume.

#### Ingress
Expose the API over HTTPS using an ingress controller (e.g., NGINX or Traefik) with TLS termination. Use path-based routing to separate UI and API endpoints.

#### Secrets Management
Source and destination credentials must be stored securely. In Kubernetes, store them in Secrets and mount into pods. Integrate with cloud secret managers (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault) via sidecars or external secrets controllers.

#### High Availability
Run multiple replicas of stateless control plane services behind a service load balancer. For Postgres, use replication with one primary and read replicas, or a managed service with built-in failover.

#### Configuration Reload
Control plane services should watch configuration changes and reload their caches without requiring restarts. For example, the scheduler monitors new connections and reschedules workers accordingly.

### Data Plane Deployment

#### DB Host Workers

**Deployment Pattern**: Use a Deployment or StatefulSet per source type. Each pod runs an instance of the CDC worker. The worker automatically discovers which physical host to connect to based on the sources assigned by the scheduler. If the host mapping changes, the scheduler updates the worker via a gRPC or REST call.

**Resource Limits**: Start with conservative CPU and memory requests (e.g., 500m CPU, 1Gi memory) and adjust based on throughput and event size. Limit concurrency (e.g., 3 parallel tables) via `--max-connections` command line argument or environment variable.

**Persistent Volumes**: Mount a PersistentVolumeClaim for storing SQLite checkpoints and fallback queue data. Use ReadWriteOnce volumes for each pod. For multi-zone clusters, prefer ReadWriteMany volumes if available or pin pods to specific zones.

**Scaling**: Use an HPA to scale workers based on CPU or custom metrics (e.g., number of assigned streams). When adding a new tenant, the scheduler can explicitly scale up the worker deployment.

**Logging & Tracing**: Mount a sidecar or use fluent-bit/EFK to ship logs. Propagate correlation IDs from the control plane into the worker logs to enable traceability.

### Redis Cluster

Redis is central to event transport. To prevent the issues observed with Kafka in your environment, configure Redis carefully:

#### Topology
For production, run Redis in cluster mode with at least three masters and three replicas. Use consistent hashing to shard streams. For on-prem or small deployments, a single Redis instance may suffice but must be monitored closely.

#### Persistence
Enable both AOF (append only file) and RDB snapshots. Use `appendfsync everysec` to balance durability and latency. For managed Redis (AWS Elasticache, Azure Cache for Redis) enable multi-AZ and snapshot export.

#### Memory Management
Set `maxmemory` and use `maxmemory-policy noeviction` to avoid unexpected evictions. Rely on stream trimming (`XTRIM`) configured in the application to bound memory usage. Each stream should specify a maximum length or time-based retention (e.g., keep last 48h of events).

#### Monitoring
Expose Redis metrics via the Redis Exporter and monitor stream lengths, memory usage, replication lag and commands per second. Alerts should trigger if memory usage approaches the limit or replication lag grows.

### Spark and Airflow Integration

Spark is used for both realtime CDC consumption and scheduled batch processing. Airflow orchestrates scheduled Spark jobs.

Consider the following design patterns:

#### Spark on Kubernetes
Use the Spark Operator or Spark's native `spark-submit --master k8s://...` to run jobs in cluster mode. Each job creates a driver pod and executor pods. Configure dynamic allocation so that the number of executors scales with the input volume.

#### Spark Image
Build a custom Spark image that includes the dependencies: Redis connector, Iceberg connector, JDBC drivers, transformation library and your registered UDFs. Parameterise the image with environment variables (Redis host, JDBC URLs).

#### Realtime Mode
For `sync_type=REALTIME`, deploy a long-running Spark Structured Streaming application per tenant or per connection. Spark's checkpoint directory (not to be confused with CDC checkpoints) should be persisted on a durable filesystem (e.g., S3, HDFS or PersistentVolume). The job reads from Redis using a Redis source, applies transformations and writes to the sink. If the application fails, Spark's checkpoint allows it to resume exactly where it left off.

#### Scheduled Mode
For `sync_type=SCHEDULED`, Airflow triggers a Spark batch job via the SparkSubmitOperator. The job reads from the source database directly or from a Redis backlog, applies the transformation spec, runs DQ checks and writes to the sink. After completion, Airflow updates the connection run status and logs any quality issues.

**Complete Airflow DAG Example:**

```python
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

@dag(
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['fusion', 'cdc', 'orders']
)
def sync_orders():
    """
    Scheduled sync for orders table from MySQL to Postgres warehouse
    """
    
    @task
    def prepare_sync():
        """
        Fetch connection metadata, transform spec, and DQ policy
        """
        # Query metadata service
        metadata_hook = PostgresHook(postgres_conn_id='metadata_db')
        
        connection_config = metadata_hook.get_first("""
            SELECT c.connection_id, c.source_id, c.destination_id,
                   c.transform_pipeline_id, c.dq_policy_id,
                   s.host as source_host, s.database as source_db,
                   d.jdbc_url as dest_url
            FROM connections c
            JOIN sources s ON c.source_id = s.source_id
            JOIN destinations d ON c.destination_id = d.destination_id
            WHERE c.connection_id = 'conn_orders_001'
        """)
        
        transform_spec = metadata_hook.get_first("""
            SELECT spec FROM transform_pipelines
            WHERE pipeline_id = %s
        """, parameters=(connection_config['transform_pipeline_id'],))
        
        dq_policy = metadata_hook.get_first("""
            SELECT policy FROM dq_policies
            WHERE policy_id = %s
        """, parameters=(connection_config['dq_policy_id'],))
        
        return {
            'connection_id': connection_config['connection_id'],
            'source_host': connection_config['source_host'],
            'source_db': connection_config['source_db'],
            'dest_url': connection_config['dest_url'],
            'transform_spec': transform_spec['spec'],
            'dq_policy': dq_policy['policy']
        }
    
    @task
    def create_spark_job_spec(config: dict):
        """
        Generate Spark job YAML specification
        """
        spec = {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {
                'name': f"sync-orders-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                'namespace': 'fusion'
            },
            'spec': {
                'type': 'Python',
                'mode': 'cluster',
                'image': 'registry.example.com/fusion/spark:3.5.0',
                'mainApplicationFile': 's3://fusion/jobs/batch_sync.py',
                'arguments': [
                    '--connection-id', config['connection_id'],
                    '--source-host', config['source_host'],
                    '--source-db', config['source_db'],
                    '--dest-url', config['dest_url'],
                    '--transform-spec', json.dumps(config['transform_spec']),
                    '--dq-policy', json.dumps(config['dq_policy'])
                ],
                'sparkVersion': '3.5.0',
                'driver': {
                    'cores': 2,
                    'memory': '4g',
                    'serviceAccount': 'spark-sa'
                },
                'executor': {
                    'cores': 4,
                    'instances': 3,
                    'memory': '8g'
                }
            }
        }
        # Write to temp file or return as XCom
        return spec
    
    @task
    def update_run_status(status: str, connection_id: str):
        """
        Update connection run status in metadata DB
        """
        metadata_hook = PostgresHook(postgres_conn_id='metadata_db')
        metadata_hook.run("""
            INSERT INTO connection_runs 
            (connection_id, run_ts, status, duration_sec)
            VALUES (%s, NOW(), %s, 0)
        """, parameters=(connection_id, status))
    
    # DAG flow
    config = prepare_sync()
    spec = create_spark_job_spec(config)
    
    start_status = update_run_status('RUNNING', config['connection_id'])
    
    spark_job = SparkKubernetesOperator(
        task_id='run_spark_batch_job',
        namespace='fusion',
        application_file='/tmp/spark-job-spec.yaml',  # spec written here
        do_xcom_push=False,
        on_failure_callback=lambda ctx: update_run_status('FAILED', config['connection_id']),
    )
    
    end_status = update_run_status('SUCCESS', config['connection_id'])
    
    config >> spec >> start_status >> spark_job >> end_status

sync_orders()
```

**Corresponding Spark Batch Job (`batch_sync.py`):**

```python
from pyspark.sql import SparkSession
import argparse
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-id', required=True)
    parser.add_argument('--source-host', required=True)
    parser.add_argument('--source-db', required=True)
    parser.add_argument('--dest-url', required=True)
    parser.add_argument('--transform-spec', required=True)
    parser.add_argument('--dq-policy', required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName(f"BatchSync-{args.connection_id}") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0") \
        .getOrCreate()
    
    # Read from source
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{args.source_host}/{args.source_db}") \
        .option("dbtable", "orders") \
        .option("user", "cdc_user") \
        .option("password", "***") \
        .load()
    
    # Apply transformations
    transform_spec = json.loads(args.transform_spec)
    for step in transform_spec['transforms']:
        # Apply each transformation step
        df = apply_transform(df, step)
    
    # Run DQ checks
    dq_policy = json.loads(args.dq_policy)
    dq_results = run_dq_checks(df, dq_policy)
    
    if dq_results['status'] == 'FAIL' and dq_policy['on_fail'] == 'block':
        raise Exception(f"DQ check failed: {dq_results}")
    
    # Write to destination
    df.write \
        .format("jdbc") \
        .option("url", args.dest_url) \
        .option("dbtable", "orders_transformed") \
        .option("user", "warehouse_user") \
        .option("password", "***") \
        .mode("overwrite") \
        .save()
    
    spark.stop()

if __name__ == '__main__':
    main()
```

### Postgres & Iceberg Destinations

#### Postgres Warehouse
Run Postgres with adequate resources. Use separate schemas per tenant or a single schema with table prefixes. Upsert logic in Spark uses `INSERT ... ON CONFLICT` to merge changes. For history tables (SCD type 2), Spark writes insert and update records into a dedicated table with `valid_from`/`valid_to` columns.

#### Iceberg Tables
Configure an Iceberg catalog backed by the object store (S3, Azure Blob, GCS). Spark writes Parquet files and commits them to Iceberg using transactional metadata updates. Partitioning strategies (e.g., by date or tenant) must be defined per table in metadata. For multi-region, replicate Iceberg tables across regions using the object store replication features.

---

## Operations & Security

### Security & Compliance

Given that Fusion handles potentially sensitive data (bank records, PII), security and compliance are paramount.

#### Secrets & Credentials

- Use Kubernetes Secrets for storing source and destination credentials. Avoid storing plain passwords in ConfigMaps or environment variables
- Integrate with external secret managers. For example, use the External Secrets Operator to sync secrets from AWS Secrets Manager into Kubernetes Secrets
- Encrypt credentials at rest and in transit. Use TLS for all database connections. Validate certificates and enforce strong cipher suites

#### Network Policies

- Define Kubernetes NetworkPolicies to restrict pod communication. Only allow DB host workers to connect to source databases, Redis, Postgres and the control plane. Deny all other traffic
- For multi-tenant clusters, ensure that tenants cannot access each other's data by isolating namespaces and limiting cross-namespace traffic

#### Authentication & Authorisation

- The API gateway should integrate with the organisation's identity provider (OIDC, SAML)
- Support role-based access control with scopes such as `tenant_admin`, `parent_admin`, `viewer`, `operator`
- Use fine-grained permissions on the metadata service: only the control plane can insert or update configuration. Workers authenticate with service accounts and are authorised only for the operations they need

#### Compliance Features

- **Delete propagation**: When a delete happens in the source database, the event propagates to the warehouse and Iceberg tables. For SCD type 2, a delete closes the validity range of a record and optionally marks it as deleted
- **Audit log**: Provide an audit log of configuration changes, schema changes and DQ violations. Store these logs immutably (e.g., append-only logs) and include who performed the change

### Operational Practices

#### Monitoring & Alerting

Operators should set up dashboards and alerts covering:

1. **System health**: CPU, memory, restart counts, disk usage for all pods. Use the Kubernetes dashboard or Prometheus metrics
2. **Lag and throughput**: At a minimum, track `cdc_stream_lag_seconds`, `cdc_stream_throughput_per_second` and `dq_rule_status`. Visualise lag per tenant and table to detect backlogs
3. **Error rates**: Monitor `cdc_errors_total` by type. A spike in `parse_errors` may indicate schema drift; a spike in `sink_errors` suggests issues with Postgres or Iceberg
4. **Redis metrics**: Memory usage, replication lag, cluster slot distribution. Alert when memory reaches 80% or when a master goes down
5. **Airflow DAGs**: Use Airflow's built-in monitoring for task success and failure rates. Optionally export Airflow metrics to Prometheus

#### Scaling Guidelines

- **Workers**: Start with one worker per source host type. Measure throughput and adjust concurrency limit and pod resources. Use HPA to add pods when CPU reaches 70% utilisation
- **Spark**: For realtime streaming, choose micro-batch intervals based on latency targets (e.g., 5 seconds). Allocate executors per table or per group of tables. For batch, size the cluster to process the full snapshot within the expected window
- **Redis**: Scale cluster nodes when memory usage consistently exceeds 70%. Monitor client connections and throughput to avoid hitting connection limits
- **Postgres**: Partition large tables by tenant or date. Use `VACUUM` and `ANALYZE` regularly. Scale CPU/IO resources to handle upsert workloads

#### Failover and Disaster Recovery

- **Control Plane**: Deploy API gateway and orchestrator across availability zones. Use managed Postgres with multi-AZ. Take automated snapshots and test restores
- **Redis**: Use Redis cluster with replicas. In the event of master failure, a replica is promoted. For multi-region, replicate writes to another region using active-active Redis if supported
- **Checkpoints**: Regularly sync local checkpoints to the central database. Back up the central checkpoint table. If a worker loses its local volume, it can recover from the central state
- **Spark**: For streaming, use Spark's checkpointing to resume after driver failures. For batch, Airflow will retry jobs on failure
- **Iceberg**: Rely on object store durability and Iceberg's atomic commit model. Use cross-region replication if required

#### Runbooks

Create runbooks for common scenarios:

1. **Worker Crash**: Identify pod crash, check logs for cause. If due to malformed event, inspect source data. Restart pod; it resumes from checkpoint
2. **Redis Outage**: Fallback queue will accumulate events. Bring Redis back online and monitor the backlog draining. Check memory usage after recovery
3. **Schema Change Detected**: For manual approval, review pending changes in UI. Accept or reject. For auto apply, verify that the change makes sense
4. **DQ Rule Fail**: Inspect DQ dashboard. Identify offending rows via Spark job logs. Fix source data or adjust thresholds
5. **Full Load Failure**: Airflow reports failure. Check logs for database connectivity or transformation errors. Re-run the DAG after addressing the issue

### Operational Runbooks - Detailed Flowcharts

#### Runbook 1: Worker Crash Recovery

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      RUNBOOK: WORKER CRASH RECOVERY                           │
└──────────────────────────────────────────────────────────────────────────────┘

    Alert: Worker Pod Crashed
         │
         ▼
    ┌─────────────────────────────────┐
    │ 1. Check Kubernetes Events      │
    │    kubectl describe pod          │
    │    cdc-worker-mysql-0            │
    └──────────┬──────────────────────┘
               │
               ▼
    ┌─────────────────────────────────┐
    │ 2. Examine Pod Logs              │
    │    kubectl logs -p               │
    │    cdc-worker-mysql-0            │
    │                                  │
    │    Look for:                     │
    │    - OOM kill                    │
    │    - Connection errors           │
    │    - Parse errors                │
    │    - Segmentation fault          │
    └──────────┬──────────────────────┘
               │
               ├─────────────┬────────────────┬──────────────┐
               ▼             ▼                ▼              ▼
    ┌──────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────────┐
    │ OOM Killed   │ │ Parse Error  │ │ Connection  │ │ Unknown      │
    │              │ │              │ │ Timeout     │ │ Crash        │
    └──────┬───────┘ └──────┬───────┘ └──────┬──────┘ └──────┬───────┘
           │                │                 │                │
           ▼                ▼                 ▼                ▼
    ┌──────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────────┐
    │ Increase     │ │ Find          │ │ Check DB    │ │ Collect      │
    │ memory       │ │ malformed     │ │ firewall    │ │ core dump    │
    │ limit to     │ │ event in      │ │ rules       │ │ & report     │
    │ 4Gi          │ │ source        │ │             │ │ to dev team  │
    └──────┬───────┘ └──────┬───────┘ └──────┬──────┘ └──────┬───────┘
           │                │                 │                │
           │                ▼                 │                │
           │         ┌──────────────┐         │                │
           │         │ Add event_id │         │                │
           │         │ to skip list │         │                │
           │         │ in metadata  │         │                │
           │         └──────┬───────┘         │                │
           │                │                 │                │
           └────────────────┴─────────────────┴────────────────┘
                            │
                            ▼
               ┌─────────────────────────────────┐
               │ 3. Pod Auto-Restarts            │
               │    (Kubernetes restartPolicy)   │
               │                                 │
               │    Worker reads checkpoint:     │
               │    - Try local SQLite first     │
               │    - Fallback to Postgres       │
               └──────────┬──────────────────────┘
                          │
                          ▼
               ┌─────────────────────────────────┐
               │ 4. Verify Recovery               │
               │    - Check metrics:              │
               │      cdc_events_total increasing │
               │    - Check lag stable            │
               │    - Monitor for 15 minutes      │
               └──────────┬──────────────────────┘
                          │
                          ▼
                     [ RESOLVED ]
```

#### Runbook 2: Redis Outage Recovery

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      RUNBOOK: REDIS OUTAGE RECOVERY                           │
└──────────────────────────────────────────────────────────────────────────────┘

    Alert: Redis Cluster Unreachable
         │
         ▼
    ┌─────────────────────────────────┐
    │ 1. Verify Outage                 │
    │    redis-cli ping                │
    │    Check Elasticache dashboard   │
    └──────────┬──────────────────────┘
               │
               ▼
    ┌─────────────────────────────────┐
    │ 2. Worker Behavior During Outage│
    │    - Events written to FALLBACK  │
    │      QUEUE (local disk)          │
    │    - Metrics:                    │
    │      cdc_fallback_queue_length   │
    │      increases                   │
    │    - Processing continues!       │
    └──────────┬──────────────────────┘
               │
               ▼
    ┌─────────────────────────────────┐
    │ 3. Diagnose Redis Issue          │
    │                                  │
    │    Common causes:                │
    │    - Memory exhausted            │
    │    - Network partition           │
    │    - Master failover in progress │
    │    - Too many connections        │
    └──────────┬──────────────────────┘
               │
               ├─────────────┬────────────────┐
               ▼             ▼                ▼
    ┌──────────────┐ ┌──────────────┐ ┌─────────────┐
    │ Memory Full  │ │ Network      │ │ Master      │
    │              │ │ Partition    │ │ Failover    │
    └──────┬───────┘ └──────┬───────┘ └──────┬──────┘
           │                │                 │
           ▼                ▼                 ▼
    ┌──────────────┐ ┌──────────────┐ ┌─────────────┐
    │ Scale up     │ │ Check         │ │ Wait for    │
    │ Redis        │ │ security      │ │ automatic   │
    │ cluster or   │ │ groups        │ │ promotion   │
    │ trim old     │ │ & routes      │ │ (2-5 min)   │
    │ streams      │ │               │ │             │
    └──────┬───────┘ └──────┬───────┘ └──────┬──────┘
           │                │                 │
           └────────────────┴─────────────────┘
                            │
                            ▼
               ┌─────────────────────────────────┐
               │ 4. Redis Back Online             │
               │    Workers detect reconnection   │
               │    Start draining fallback queue │
               └──────────┬──────────────────────┘
                          │
                          ▼
               ┌─────────────────────────────────┐
               │ 5. Monitor Backlog Draining      │
               │    - cdc_fallback_queue_length   │
               │      decreasing                  │
               │    - cdc_stream_lag_seconds      │
               │      may spike temporarily       │
               │    - Alert if drain takes > 1hr  │
               └──────────┬──────────────────────┘
                          │
                          ▼
               ┌─────────────────────────────────┐
               │ 6. Post-Recovery Check           │
               │    - Verify Redis memory usage   │
               │    - Check replication lag       │
               │    - Fallback queue empty        │
               │    - Lag back to normal          │
               └──────────┬──────────────────────┘
                          │
                          ▼
                     [ RESOLVED ]
```

#### Runbook 3: DQ Rule Failure Response

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                   RUNBOOK: DATA QUALITY RULE FAILURE                          │
└──────────────────────────────────────────────────────────────────────────────┘

    Alert: DQ Rule FAIL (dq_rule_status = -1)
         │
         ▼
    ┌─────────────────────────────────┐
    │ 1. Identify Rule & Connection    │
    │    Check Alertmanager labels:    │
    │    - tenant_id                   │
    │    - connection_id               │
    │    - rule_type                   │
    └──────────┬──────────────────────┘
               │
               ▼
    ┌─────────────────────────────────┐
    │ 2. Open DQ Dashboard             │
    │    Navigate to connection        │
    │    View rule violation details   │
    │                                  │
    │    Example:                      │
    │    - null_ratio_check FAILED     │
    │    - Column: email               │
    │    - Null ratio: 15% (max: 1%)   │
    └──────────┬──────────────────────┘
               │
               ▼
    ┌─────────────────────────────────┐
    │ 3. Query DLQ for Violating Rows  │
    │    SELECT * FROM dq_violations   │
    │    WHERE rule_id = 'null_check'  │
    │    AND batch_ts > NOW() - '1h'   │
    │                                  │
    │    Sample violating records      │
    └──────────┬──────────────────────┘
               │
               ▼
    ┌─────────────────────────────────┐
    │ 4. Root Cause Analysis           │
    │                                  │
    │    Questions:                    │
    │    - Was there schema change?    │
    │    - Application bug upstream?   │
    │    - ETL logic error?            │
    │    - Threshold too strict?       │
    └──────────┬──────────────────────┘
               │
               ├──────────────┬────────────────┐
               ▼              ▼                ▼
    ┌──────────────┐ ┌───────────────┐ ┌──────────────┐
    │ Source Data  │ │ Transform     │ │ Threshold    │
    │ Issue        │ │ Bug           │ │ Too Strict   │
    └──────┬───────┘ └───────┬───────┘ └──────┬───────┘
           │                 │                 │
           ▼                 ▼                 ▼
    ┌──────────────┐ ┌───────────────┐ ┌──────────────┐
    │ Contact app  │ │ Fix transform │ │ Adjust       │
    │ team to fix  │ │ spec in UI    │ │ threshold    │
    │ upstream     │ │ Redeploy      │ │ from 1% to   │
    │              │ │               │ │ 5%           │
    └──────┬───────┘ └───────┬───────┘ └──────┬───────┘
           │                 │                 │
           └─────────────────┴─────────────────┘
                            │
                            ▼
               ┌─────────────────────────────────┐
               │ 5. Re-run or Resume Pipeline     │
               │    - For batch: Re-trigger DAG   │
               │    - For realtime: Skip bad      │
               │      batch or fix and replay     │
               └──────────┬──────────────────────┘
                          │
                          ▼
               ┌─────────────────────────────────┐
               │ 6. Monitor for Recurrence        │
               │    - Watch dq_rule_status        │
               │    - Set up preventive checks    │
               │    - Document in incident log    │
               └──────────┬──────────────────────┘
                          │
                          ▼
                     [ RESOLVED ]
```

### Example Kubernetes Deployment YAML

Below is an example YAML fragment for a DB host worker deployment with resource limits, node selectors and a persistent volume:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cdc-mysql-worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cdc-worker
      source-type: mysql
  template:
    metadata:
      labels:
        app: cdc-worker
        source-type: mysql
    spec:
      nodeSelector:
        role: db-workers
      tolerations:
        - key: "dedicated"
          operator: "Equal"
          value: "cdc"
          effect: "NoSchedule"
      containers:
        - name: cdc-worker
          image: registry.example.com/fusion/cdc-worker:latest
          args: ["--source-type", "mysql", "--host", "dbhost1"]
          env:
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: cdc-secrets
                  key: redis-url
          resources:
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              cpu: "2000m"
              memory: "4Gi"
          volumeMounts:
            - name: checkpoint-volume
              mountPath: /var/lib/cdc
      volumes:
        - name: checkpoint-volume
          persistentVolumeClaim:
            claimName: cdc-mysql-worker-pvc
```

---

## Getting Started

### Prerequisites

- Python 3.8+
- Kubernetes cluster (or Docker for local development)
- Redis (cluster or standalone)
- PostgreSQL (for metadata and data warehouse)
- Apache Spark
- Apache Airflow (for scheduled syncs)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd fusion-cdc-engine
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   - Set up database connections
   - Configure Redis connection
   - Set up secret management (AWS Secrets Manager, Azure Key Vault, etc.)

4. **Initialize metadata database**:
   ```bash
   # Run database migrations
   # Create initial schema for tenants, sources, destinations, connections
   ```

5. **Deploy to Kubernetes**:
   ```bash
   kubectl apply -f k8s/control-plane/
   kubectl apply -f k8s/data-plane/
   ```

### Quick Start Guide

1. **Access the UI**: Navigate to the deployed UI endpoint
2. **Create a Parent**: Set up your parent organization (e.g., Bank1)
3. **Add Tenants**: Create tenants under the parent
4. **Configure Source**: Add a MySQL/PostgreSQL/MongoDB source with connection details
5. **Configure Destination**: Add a Postgres warehouse or Iceberg destination
6. **Create Connection**: Link source and destination, select tables/collections
7. **Configure Transformations**: Define transformation pipeline, JSON flattening, DQ rules
8. **Start Sync**: Enable the connection and monitor in the dashboard

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)
- [ ] Set up project structure and core modules
- [ ] Implement metadata service with Postgres schema
- [ ] Build basic REST API for tenant/source/destination management
- [ ] Create MySQL binlog connector
- [ ] Implement local SQLite checkpointing

### Phase 2: Core CDC Engine (Weeks 5-8)
- [ ] PostgreSQL WAL connector
- [ ] MongoDB change streams connector
- [ ] Redis stream integration
- [ ] Event envelope normalization
- [ ] Central Postgres checkpointing
- [ ] Fallback queue implementation

### Phase 3: Transformations (Weeks 9-12)
- [ ] Transformation pipeline engine
- [ ] Implement all 11 transform types
- [ ] JSON flattening (inline and child table)
- [ ] Expression DSL parser (Spark SQL + SEL)
- [ ] UDF registration system
- [ ] Spark consumer for realtime mode

### Phase 4: Schema & Quality (Weeks 13-16)
- [ ] Schema discovery and introspection
- [ ] Periodic re-introspection
- [ ] Schema evolution policies (AUTO_APPLY, MANUAL_APPROVAL)
- [ ] All 7 DQ rule types
- [ ] Prometheus instrumentation
- [ ] Structured logging with trace IDs

### Phase 5: Orchestration (Weeks 17-20)
- [ ] Airflow integration for scheduled syncs
- [ ] Worker orchestration and scaling
- [ ] Concurrency limits and scheduling
- [ ] Spark batch job integration
- [ ] Full load snapshot logic

### Phase 6: UI & UX (Weeks 21-24)
- [ ] Web UI for tenant management
- [ ] Source/destination configuration wizards
- [ ] Connection setup workflow
- [ ] Transformation preview
- [ ] DQ dashboard
- [ ] Metrics visualization

### Phase 7: Deployment & Ops (Weeks 25-28)
- [ ] Kubernetes manifests (Deployments, StatefulSets, Services)
- [ ] Helm charts
- [ ] Secret management integration
- [ ] Network policies
- [ ] RBAC implementation
- [ ] Runbooks and documentation

### Phase 8: Testing & Hardening (Weeks 29-32)
- [ ] Unit tests for all components
- [ ] Integration tests
- [ ] End-to-end tests
- [ ] Performance testing and optimization
- [ ] Security audit
- [ ] Load testing

### Phase 9: Production Readiness (Weeks 33-36)
- [ ] Multi-region deployment testing
- [ ] Disaster recovery procedures
- [ ] Monitoring and alerting setup
- [ ] Production deployment guides
- [ ] User documentation
- [ ] Training materials

---

## Project Structure

```
fusion-cdc-engine/
├── src/
│   ├── control_plane/
│   │   ├── api/              # REST/GraphQL API
│   │   ├── metadata/         # Metadata service
│   │   ├── scheduler/        # Worker orchestration
│   │   └── ui/               # Web interface
│   ├── data_plane/
│   │   ├── connectors/       # MySQL, PostgreSQL, MongoDB connectors
│   │   │   ├── mysql.py
│   │   │   ├── postgresql.py
│   │   │   └── mongodb.py
│   │   ├── workers/          # CDC worker processes
│   │   ├── checkpoints/      # Checkpoint management
│   │   └── fallback/         # Fallback queue
│   ├── transformations/
│   │   ├── pipeline.py       # Transformation engine
│   │   ├── types/            # Transform step implementations
│   │   ├── json_handler.py   # JSON flattening
│   │   └── udf_registry.py   # UDF management
│   ├── schema/
│   │   ├── discovery.py      # Schema introspection
│   │   ├── evolution.py      # Schema change handling
│   │   └── cache.py          # Discovery cache
│   ├── quality/
│   │   ├── rules/            # DQ rule implementations
│   │   ├── metrics.py        # Sync quality metrics
│   │   └── prometheus.py     # Prometheus integration
│   ├── spark/
│   │   ├── consumer.py       # Spark Structured Streaming
│   │   ├── batch.py          # Batch processing
│   │   └── sinks/            # Postgres, Iceberg sinks
│   └── utils/
│       ├── logger.py         # Structured logging
│       ├── config.py         # Configuration management
│       └── auth.py           # Authentication/authorization
├── config/
│   ├── database.yaml         # Database configurations
│   ├── redis.yaml            # Redis configuration
│   └── spark.yaml            # Spark configuration
├── k8s/
│   ├── control-plane/        # Control plane manifests
│   ├── data-plane/           # Data plane manifests
│   └── redis/                # Redis cluster manifests
├── airflow/
│   └── dags/                 # Airflow DAG definitions
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── docs/
│   ├── architecture.md
│   ├── transformations.md
│   ├── schema_evolution.md
│   ├── deployment.md
│   └── api_reference.md
├── schemas/                  # Data schemas
├── logs/                     # Application logs
├── data/                     # Sample/test data
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Technology Stack

### Core Components
- **Python 3.9+**: Primary development language
- **PostgreSQL**: Metadata storage and data warehouse
- **Redis**: Event streaming and transport
- **Apache Spark**: Transformation processing
- **Apache Airflow**: Workflow orchestration

### CDC Libraries
- `python-mysql-replication`: MySQL binlog reading
- `psycopg2`: PostgreSQL WAL replication
- `pymongo`: MongoDB change streams

### Data Processing
- `pandas`: Data manipulation
- `pyarrow`: Columnar data format
- `pyspark`: Spark integration

### Quality & Monitoring
- `prometheus-client`: Metrics exposition
- `great-expectations`: Data quality validation
- `pydantic`: Data validation

### Kubernetes & Deployment
- `kubernetes`: Python Kubernetes client
- Helm charts for deployment
- Docker for containerization

### Testing
- `pytest`: Testing framework
- `pytest-cov`: Coverage reporting
- `pytest-mock`: Mocking support

---

## Contributing

This is an internal project based on comprehensive architectural specifications. Contributions should follow the established patterns and architectural principles outlined in this document.

### Development Guidelines

1. Follow the architectural principles of control/data plane separation
2. Maintain tenant isolation at all levels
3. Ensure all transformations are deterministic and idempotent
4. Add comprehensive tests for all new features
5. Update documentation alongside code changes
6. Follow Python PEP 8 style guidelines
7. Add Prometheus metrics for new components
8. Include structured logging with trace IDs

---

## License

TBD

---

## References

This README is based on the complete technical specifications across 5 documents:

1. **Fusion CDC Engine – Overview and Product Specification** (9 pages)
2. **Fusion CDC Engine – Detailed Architecture & Design** (8 pages)
3. **Fusion CDC Engine – Transformations & JSON Handling** (8 pages)
4. **Fusion CDC Engine – Schema Evolution, Data Quality & Metrics** (5 pages)
5. **Fusion CDC Engine – Deployment, Operations & Security** (6 pages)

**Total**: 36 pages of comprehensive technical documentation

---

## Contact & Support

For questions, issues, or support regarding Fusion CDC Engine, please contact the development team or refer to the detailed documentation in the `docs/` directory.

---

**Built with precision. Designed for scale. Ready for production.**

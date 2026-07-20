# Fusion CDC Engine — Documentation Index

> **Start here.** This index links to all documentation for the Fusion CDC Engine platform. Whether you are a new engineer onboarding, an operator responding to an incident, or a developer extending the system, the right document is one link away.

---

## What Is Fusion?

Fusion is a **multi-tenant Change Data Capture (CDC) platform** that captures every insert, update, and delete from relational and NoSQL source databases and streams them — in real-time — to downstream data warehouses, analytics systems, and event consumers. It eliminates batch ETL windows, provides millisecond-level data freshness, and supports the full data lifecycle from raw CDC events through transformation, data quality enforcement, and schema evolution management.

---

## Documentation Map

| # | Document | Who Should Read It | What It Covers |
|---|----------|--------------------|----------------|
| [01](01_ARCHITECTURE.md) | **Architecture** | All engineers, architects | System overview, component diagram, control/data plane split, technology stack, tenancy model |
| [02](02_DATA_FLOW.md) | **Data Flow & Pipeline** | Backend engineers, data engineers | End-to-end event trace, pipeline modes (REALTIME/SCHEDULED/BATCH), transformation pipeline, data quality, failure recovery |
| [03](03_COMPONENT_REFERENCE.md) | **Component Reference** | Backend engineers, connector developers | Detailed specs for every class/module, REST API endpoints, data models, metrics catalogue |
| [04](04_OPERATIONS_RUNBOOK.md) | **Operations & Runbook** | DevOps, SREs, on-call engineers | Start/stop procedures, health checks, incident runbooks (RB-001 through RB-009), scaling, deployments |

---

## Quick Navigation by Role

### New Engineer Onboarding
1. Read [01_ARCHITECTURE.md](01_ARCHITECTURE.md) front to back (30 min)
2. Follow the **Dev Environment Start** procedure in [04_OPERATIONS_RUNBOOK.md §2](04_OPERATIONS_RUNBOOK.md#2-starting-and-stopping-the-system)
3. Run the **Quick Full-Stack Health Check** script in [04_OPERATIONS_RUNBOOK.md §3](04_OPERATIONS_RUNBOOK.md#3-health-verification)
4. Read [02_DATA_FLOW.md §6](02_DATA_FLOW.md#6-event-lifecycle-deep-dive) to trace a single event end-to-end

### Responding to an Incident
1. Go directly to [04_OPERATIONS_RUNBOOK.md §5](04_OPERATIONS_RUNBOOK.md#5-common-runbooks)
2. Find the matching runbook by symptom:
   - Worker not streaming → RB-001
   - Run stuck → RB-002
   - Checkpoint not syncing → RB-003
   - Redis memory high → RB-004
   - Worker crash loop → RB-005
   - Postgres slot orphaned → RB-006
   - High replication lag → RB-007
   - DQ rule failures → RB-008
   - Schema change blocked → RB-009

### Adding a New Connector (Source Type)
1. Read [03_COMPONENT_REFERENCE.md §3](03_COMPONENT_REFERENCE.md#3-connector-specifications) for base class contract
2. Read [02_DATA_FLOW.md §3](02_DATA_FLOW.md#3-initial-full-load-flow) for the initial load pattern
3. Look at `cdc-workers/cdc_worker/connectors/mysql.py` as the reference implementation

### Extending the REST API
1. Read [03_COMPONENT_REFERENCE.md §4](03_COMPONENT_REFERENCE.md#4-rest-api-reference) for existing endpoints
2. Read [03_COMPONENT_REFERENCE.md §1](03_COMPONENT_REFERENCE.md#1-control-plane-components) for middleware and auth patterns
3. See [03_COMPONENT_REFERENCE.md §6](03_COMPONENT_REFERENCE.md#6-data-models-reference) for data models

### Understanding a Pipeline Failure
1. Read [02_DATA_FLOW.md §11](02_DATA_FLOW.md#11-failure--recovery-flows) for failure scenarios
2. Check [04_OPERATIONS_RUNBOOK.md §10](04_OPERATIONS_RUNBOOK.md#10-incident-response) for severity triage

---

## Docker Images & Containers

The platform is made up of **3 custom-built images** and **6 upstream images**. Everything runs via a single `docker compose` command.

### Custom-Built Images (from `docker/`)

| Image Tag | Dockerfile | Built From | Exposes | Entry Point |
|-----------|------------|------------|---------|-------------|
| `docker-cdc-worker:latest` | `Dockerfile.cdc-worker` | `python:3.11-slim` (multi-stage) | `:8081` | `python -m cdc_worker.worker` |
| `fusion-control-plane:latest` | `Dockerfile.control-plane` | `python:3.11-slim` (multi-stage) | `:8000` | `alembic upgrade head && uvicorn app.main:app` |
| `fusion-spark-consumer:latest` | `Dockerfile.spark-consumer` | `bitnami/spark:3.5` (multi-stage) | `:4040 :8080` | `python -m consumer.streaming_consumer` |

### Upstream Images (pulled automatically)

| Container Name | Image | Port (host:container) | Purpose |
|---------------|-------|-----------------------|---------|
| `fusion-postgres` | `postgres:16-alpine` | `5432:5432` | Control plane metadata DB |
| `fusion-postgres-dest` | `postgres:16-alpine` | `5433:5432` | Destination data warehouse |
| `fusion-pg-source` | `postgres:16-alpine` | `5434:5432` | CDC source PostgreSQL (WAL logical) |
| `fusion-mysql` | `mysql:8.0` | `3307:3306` | CDC source MySQL (binlog ROW) |
| `fusion-mongo` | `mongo:7.0` | `27018:27017` | CDC source MongoDB (replica set) |
| `fusion-mongo-init` | `mongo:7.0` | — | One-shot replica set init (exits after) |
| `fusion-redis` | `redis:7.2-alpine` | `6379:6379` | Event transport (Redis Streams) |
| `fusion-airflow` | `apache/airflow:2.9.3-python3.11` | `8080:8080` | Batch/Scheduled job orchestration |

### Build Commands

```bash
# Build CDC worker (most common):
docker build \
  -f /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/docker/Dockerfile.cdc-worker \
  -t docker-cdc-worker:latest \
  /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine

# Build control plane:
docker build \
  -f /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/docker/Dockerfile.control-plane \
  -t fusion-control-plane:latest \
  /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine

# Build Spark consumer:
docker build \
  -f /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/docker/Dockerfile.spark-consumer \
  -t fusion-spark-consumer:latest \
  /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine

# Build ALL via docker compose:
docker compose -f docker/docker-compose.dev.yml build
```

### Container Startup Order & Dependencies

```
postgres-meta ──────────────────────────────────────┐
postgres-dest ──────────────────────────┐            │
pg-source     (independent)             │            │
mysql-source  (independent)             │            │
mongo-source  (independent)             │            │
  └─→ mongo-init (one-shot, then exits) │            │
redis ──────────────────────────────────┤            │
                                        │            │
                                    spark-consumer   airflow
                                                         │
                                    cdc-worker ──────────┘
                                    (depends on: redis + postgres-meta)
```

### What Is Up After `docker compose up -d`

```
CONTAINER NAME              IMAGE                        PORTS           STATUS
fusion-postgres         postgres:16-alpine           5432→5432       healthy
fusion-postgres-dest    postgres:16-alpine           5433→5432       healthy
fusion-pg-source        postgres:16-alpine           5434→5432       healthy
fusion-mysql            mysql:8.0                    3307→3306       healthy
fusion-mongo            mongo:7.0                    27018→27017     healthy
fusion-mongo-init       mongo:7.0                    —               exited (0) ✓
fusion-redis            redis:7.2-alpine             6379→6379       healthy
fusion-airflow          apache/airflow:2.9.3-...     8080→8080       healthy
fusion-cdc-worker       docker-cdc-worker:latest     8081→8081       healthy
fusion-spark-consumer   fusion-spark-consumer    4040,8080       running
```

### Named Volumes

| Volume | Mounted In | Contains |
|--------|-----------|----------|
| `pg_meta_data` | `fusion-postgres` | Control plane PostgreSQL data files |
| `pg_dest_data` | `fusion-postgres-dest` | Destination DW PostgreSQL data files |
| `pg_source_data` | `fusion-pg-source` | Source PostgreSQL data files |
| `mysql_data` | `fusion-mysql` | MySQL data files + binlog files |
| `mongo_data` | `fusion-mongo` | MongoDB BSON data files |
| `airflow_dags` | `fusion-airflow` | Airflow DAG definitions |
| `airflow_logs` | `fusion-airflow` | Airflow task execution logs |
| `cdc_worker_data` | `fusion-cdc-worker` | SQLite checkpoint DB + fallback queue DB |
| `spark_checkpoints` | `fusion-spark-consumer` | Spark structured streaming offset checkpoints |

---

## Architecture at a Glance

```
Source DBs (MySQL / PostgreSQL / MongoDB)
     │
     │  binlog / WAL replication / Change Streams
     ▼
┌─────────────────────┐
│   CDC Worker        │  ← Python asyncio worker
│   (port 8081)       │    captures raw change events
│                     │    checkpoints every 60s
└──────────┬──────────┘
           │  XADD  (stream key: cdc:{bank}:{tenant}:{source}:{schema}:{table})
           ▼
┌─────────────────────┐
│   Redis Streams     │  ← Event transport layer
│   (port 6379)       │    MAXLEN ~100,000 per table
└──────────┬──────────┘
           │  XREADGROUP
           ▼
┌──────────────────────────┐    ┌─────────────────────┐
│   Spark Streaming        │    │   Airflow Batch DAGs │
│   (Consumer Group)       │    │   (Scheduled pulls) │
└──────────┬───────────────┘    └──────────┬──────────┘
           │ Transform → DQ → Write         │
           ▼                               ▼
    Destination DB / DW / Data Lake

           ▲  (manages everything above)
┌──────────┴───────────────┐
│   Control Plane          │  ← FastAPI on port 8000
│   (port 8000)            │    multi-tenant API
│   + Frontend (port 5173) │    React UI
└──────────────────────────┘
```

---

## Key Facts

| Property | Value |
|----------|-------|
| Control plane | FastAPI, port 8000 |
| Frontend | React + Vite, port 5173 |
| CDC Worker | Python asyncio, port 8081 |
| Event transport | Redis Streams |
| Control plane DB | PostgreSQL 15, port 5432 |
| MySQL source (dev) | port 3307, user `fusion_user`, DB `fusion_cdc_metadata` |
| Worker auth | `X-Worker-Token: dev-worker-token` |
| JWT algorithm | HS256, 30-minute expiry |
| Checkpoint interval | 60 seconds (local SQLite → central Postgres) |
| Stream key format | `cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}` |
| Max events per stream | ~100,000 (MAXLEN approximate trim) |

---

## Artifact

A comprehensive 1600-line analysis document covering all architecture decisions, design rationale, and implementation notes is available at:

```
artifacts/FUSION_CDC_ANALYSIS.md
```

This document predates the structured documentation above and is useful for understanding the **why** behind key design decisions.

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-05 | Initial documentation suite created (4 files) |
| 2026-05-05 | CDC trigger fixed: runs complete immediately, snapshot stored in run_config |
| 2026-05-05 | Frontend updated with expandable run detail rows showing CDC snapshot |
| 2026-05-05 | Stale run detection fixed: streaming runs no longer auto-failed |
| 2026-05-05 | Checkpoint sync fixed: correct JSON envelope sent to /internal/checkpoints/batch |

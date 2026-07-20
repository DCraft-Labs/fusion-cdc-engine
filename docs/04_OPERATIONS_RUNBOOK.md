# Fusion CDC Engine — Operations & Runbook

> **Audience:** DevOps engineers, SREs, and on-call engineers who need to operate, monitor, troubleshoot, and recover the system in production and development environments.

---

## Table of Contents

1. [Day-to-Day Operations Checklist](#1-day-to-day-operations-checklist)
2. [Starting and Stopping the System](#2-starting-and-stopping-the-system)
3. [Health Verification](#3-health-verification)
4. [Monitoring Reference](#4-monitoring-reference)
5. [Common Runbooks](#5-common-runbooks)
6. [Scaling Guidelines](#6-scaling-guidelines)
7. [Database Maintenance](#7-database-maintenance)
8. [Security Operations](#8-security-operations)
9. [Deployment Procedures](#9-deployment-procedures)
10. [Incident Response](#10-incident-response)

---

## 1. Day-to-Day Operations Checklist

### Morning Health Check
```bash
# 1. All containers running?
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep fusion

# 2. CDC worker healthy?
curl -s http://localhost:8081/health | python3 -m json.tool

# 3. Any events stuck in Redis?
docker exec fusion-redis redis-cli keys "cdc:*" | wc -l

# 4. Recent run failures?
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata \
  -c "SELECT run_number, status, error_message, started_at FROM connection_runs WHERE status='failed' ORDER BY started_at DESC LIMIT 5;"

# 5. Checkpoint freshness?
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata \
  -c "SELECT worker_id, lsn, checkpoint_at, now() - checkpoint_at AS age FROM checkpoint_state ORDER BY checkpoint_at DESC LIMIT 5;"

# 6. DQ violations?
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata \
  -c "SELECT count(*) FROM event_dead_letter_queue WHERE status='pending';"
```

---

## 2. Starting and Stopping the System

### Docker Images Reference

Three images must be **built locally** before `docker compose up` will work. Six upstream images are pulled automatically from Docker Hub.

#### Custom Images (must be built)

| Image Tag | Dockerfile | Source Dir Copied In | Exposes | Runs As |
|-----------|------------|----------------------|---------|---------|
| `docker-cdc-worker:latest` | `docker/Dockerfile.cdc-worker` | `cdc-workers/` | `:8081` | non-root `fusion` user |
| `fusion-control-plane:latest` | `docker/Dockerfile.control-plane` | `control-plane/` | `:8000` | non-root `fusion` user |
| `fusion-spark-consumer:latest` | `docker/Dockerfile.spark-consumer` | `spark-consumer/` | `:4040`, `:8080` | user `1001` |

Build context is always the **repo root** (`fusion-cdc-engine/`), not the docker subdirectory.

```bash
# Build all three custom images at once:
docker compose -f docker/docker-compose.dev.yml build

# Or build individually (all require full repo root as context):
docker build -f docker/Dockerfile.cdc-worker         -t docker-cdc-worker:latest         .
docker build -f docker/Dockerfile.control-plane      -t fusion-control-plane:latest  .
docker build -f docker/Dockerfile.spark-consumer     -t fusion-spark-consumer:latest .
```

#### Upstream Images (auto-pulled)

| Container Name | Image | Host Port | Purpose |
|---------------|-------|-----------|---------|
| `fusion-postgres` | `postgres:16-alpine` | `5432` | Control plane metadata DB |
| `fusion-postgres-dest` | `postgres:16-alpine` | `5433` | Destination data warehouse |
| `fusion-pg-source` | `postgres:16-alpine` | `5434` | CDC source (WAL logical replication) |
| `fusion-mysql` | `mysql:8.0` | `3307` | CDC source (binlog ROW format) |
| `fusion-mongo` | `mongo:7.0` | `27018` | CDC source (replica set `rs0`) |
| `fusion-mongo-init` | `mongo:7.0` | — | One-shot: initialises replica set, then exits 0 |
| `fusion-redis` | `redis:7.2-alpine` | `6379` | Event transport (Redis Streams) |
| `fusion-airflow` | `apache/airflow:2.9.3-python3.11` | `8080` | Batch/Scheduled job orchestration |

#### Named Volumes

| Volume Name | Used By | Contents |
|-------------|---------|----------|
| `pg_meta_data` | `fusion-postgres` | Control plane DB files |
| `pg_dest_data` | `fusion-postgres-dest` | DW DB files |
| `pg_source_data` | `fusion-pg-source` | Source DB files |
| `mysql_data` | `fusion-mysql` | MySQL data + binlog files |
| `mongo_data` | `fusion-mongo` | MongoDB BSON data |
| `airflow_dags` | `fusion-airflow` | DAG Python files |
| `airflow_logs` | `fusion-airflow` | Task execution logs |
| `cdc_worker_data` | `fusion-cdc-worker` | `checkpoints.db` + `fallback.db` (SQLite) |
| `spark_checkpoints` | `fusion-spark-consumer` | Spark structured streaming offsets |

---

### Full Dev Environment Start

```bash
cd /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine

# Step 1: Start all Docker infrastructure
docker compose -f docker/docker-compose.dev.yml up -d

# Step 2: Wait for health checks to pass (30-60 seconds)
docker compose -f docker/docker-compose.dev.yml ps

# Step 3: Apply DB migrations (first run only, or after schema changes)
cd control-plane
source ../.venv/bin/activate
alembic upgrade head

# Step 4: Start control plane with hot-reload
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &

# Step 5: Start frontend dev server
cd ../frontend
npm run dev &

# Everything running:
# Control plane: http://localhost:8000
# Frontend:      http://localhost:5173
# Airflow:       http://localhost:8080  (admin/admin)
# CDC Worker:    http://localhost:8081/health
```

### Service-by-Service Start Order

```
1. postgres-meta    (control plane DB — must start first)
2. postgres-dest    (destination DW)
3. pg-source        (PostgreSQL CDC source)
4. mysql-source     (MySQL CDC source)
5. mongo-source     (MongoDB CDC source)
6. mongo-init       (one-shot replica set init)
7. redis            (event transport)
8. control-plane    (FastAPI — depends on postgres-meta)
9. cdc-worker       (depends on redis + control-plane)
10. airflow         (depends on postgres-meta + redis)
11. spark-consumer  (depends on redis + postgres-dest)
12. frontend        (depends on control-plane)
```

### Graceful Shutdown

```bash
# Stop control plane
kill $(lsof -ti:8000)

# Stop frontend
kill $(lsof -ti:5173)

# Stop Docker services (preserves volumes)
docker compose -f docker/docker-compose.dev.yml stop

# Full teardown (DESTROYS ALL DATA — volumes deleted)
docker compose -f docker/docker-compose.dev.yml down -v
```

### Restart CDC Worker Only

```bash
# If worker code changed — rebuild image first
docker build -f /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/docker/Dockerfile.cdc-worker \
  -t docker-cdc-worker:latest \
  /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine

# Restart container with new image
docker compose -f docker/docker-compose.dev.yml up -d --force-recreate cdc-worker

# Verify startup
docker logs fusion-cdc-worker -f --since 30s
curl http://localhost:8081/health
```

---

## 3. Health Verification

### Quick Full-Stack Health Check

```bash
#!/bin/bash
echo "=== FUSION HEALTH CHECK ==="

echo ""
echo "--- Docker containers ---"
docker ps --format "{{.Names}}: {{.Status}}" | grep fusion | sort

echo ""
echo "--- Control plane ---"
curl -s http://localhost:8000/health | python3 -m json.tool 2>/dev/null || echo "UNREACHABLE"

echo ""
echo "--- CDC Worker ---"
curl -s http://localhost:8081/health | python3 -m json.tool 2>/dev/null || echo "UNREACHABLE"

echo ""
echo "--- Redis streams ---"
docker exec fusion-redis redis-cli keys "cdc:*" 2>/dev/null || echo "Redis unreachable"

echo ""
echo "--- MySQL binlog ---"
docker exec fusion-mysql mysql -u root -proot_password -e "SHOW MASTER STATUS\G" 2>/dev/null | grep -E "File|Position"

echo ""
echo "--- Latest checkpoint ---"
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t \
  -c "SELECT worker_id || ' | ' || lsn || ' | age: ' || EXTRACT(EPOCH FROM (now() - checkpoint_at))::int || 's' FROM checkpoint_state ORDER BY checkpoint_at DESC LIMIT 1;" 2>/dev/null

echo ""
echo "--- Recent run status ---"
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -t \
  -c "SELECT '#' || run_number || ' ' || status || ' @ ' || started_at FROM connection_runs ORDER BY started_at DESC LIMIT 3;" 2>/dev/null
```

### Expected Healthy State

```
Docker containers:
  fusion-postgres:      Up N minutes (healthy)
  fusion-postgres-dest: Up N minutes (healthy)
  fusion-pg-source:     Up N minutes (healthy)
  fusion-mysql:         Up N minutes (healthy)
  fusion-mongo:         Up N minutes (healthy)
  fusion-redis:         Up N minutes (healthy)
  fusion-airflow:       Up N minutes (healthy)
  fusion-cdc-worker:    Up N minutes (healthy)

Control plane:    {"status": "ok"} at /health
CDC Worker:       {"status": "healthy", "active_sources": ["..."]}
Redis streams:    cdc:...:customers (and others if data was inserted)
MySQL binlog:     File: mysql-bin.000003, Position: 10708+
Checkpoint:       age < 120s (checkpoint syncs every 60s)
Recent runs:      status=completed for CDC triggers
```

---

## 4. Monitoring Reference

### Prometheus Metrics (Worker)

Access at `http://localhost:9090/metrics` (worker) or via Grafana dashboard.

**Key queries to run:**

```promql
# Events per second per table:
rate(cdc_events_total[5m])

# Replication lag — alert if > 300s:
cdc_stream_lag_seconds

# Checkpoint staleness — alert if > 600s:
cdc_checkpoint_age_seconds

# Fallback queue size — alert if > 10000:
cdc_fallback_queue_length

# DQ violations:
dq_rule_status == -1
```

### Structured Logs (JSON)

All components emit JSON logs. Key fields to filter on:

```bash
# Control plane errors:
docker logs fusion-cdc-worker 2>&1 | grep '"level":"ERROR"'

# Checkpoint syncs:
docker logs fusion-cdc-worker 2>&1 | grep -i checkpoint | tail -20

# Event routing issues:
docker logs fusion-cdc-worker 2>&1 | grep "No routing for" | tail -20

# Worker heartbeats:
docker logs fusion-cdc-worker 2>&1 | grep heartbeat | tail -10
```

### Grafana Dashboards

Dashboard files in `monitoring/dashboards/`:
- `overview.json` — Cross-connection lag, event rates, error counts
- `mysql.json` — MySQL-specific binlog position, tables per second
- `postgres.json` — WAL LSN advancement, slot age
- `mongodb.json` — Change stream lag, resume token age

### Key Redis Monitoring Commands

```bash
# Stream lengths (event backlog):
docker exec fusion-redis redis-cli XLEN "cdc:..."

# Consumer group lag (events not yet consumed by Spark):
docker exec fusion-redis redis-cli XPENDING "cdc:..." fusion-spark - + 100

# Memory usage:
docker exec fusion-redis redis-cli INFO memory | grep used_memory_human

# All stream keys and their lengths:
for key in $(docker exec fusion-redis redis-cli keys "cdc:*"); do
  echo "$key: $(docker exec fusion-redis redis-cli XLEN $key) events"
done
```

---

## 5. Common Runbooks

### RB-001: CDC Worker Not Streaming

**Symptom:** `cdc_stream_lag_seconds` rising; Redis stream XLEN not increasing after DB changes.

**Diagnosis:**
```bash
# 1. Is worker running?
curl http://localhost:8081/health
# Expected: {"status":"healthy","active_sources":["..."]}
# If unreachable: worker is down → go to RB-005

# 2. Are sources active in worker?
curl http://localhost:8081/health | python3 -c "import sys,json; d=json.load(sys.stdin); print('Active sources:', d['active_sources'])"
# If active_sources is empty → source not started → trigger sync from UI

# 3. Is routing table populated?
curl -s -H "X-Worker-Token: dev-worker-token" \
  http://localhost:8000/api/v1/internal/workers/worker-dev-1/routing
# Expected: [{schema_name:"*", table_name:"*", bank_id:"...", ...}]

# 4. Check worker logs for errors:
docker logs fusion-cdc-worker 2>&1 | grep ERROR | tail -20

# 5. Test MySQL connectivity:
docker exec fusion-mysql mysql -u fusion_user -pfusion_password fusion_cdc_metadata -e "SELECT 1;"
```

**Resolution:**
- Worker down → restart via `docker compose up -d --force-recreate cdc-worker`
- No active sources → trigger sync from UI → "Trigger Sync" button
- Routing table empty → check connection configuration in UI has correct source_id
- MySQL connectivity fail → verify credentials, firewall rules, binlog format

---

### RB-002: Run Stuck in "Running" State

**Symptom:** Sync runs show "Running" badge indefinitely in UI.

**Context:** This should no longer happen after the `connections.py` fix. CDC runs now complete immediately. If you see "Running":
- Batch/Scheduled runs that haven't received Airflow callback within 10 minutes
- Legacy runs from before the streaming fix

**Fix stuck batch runs:**
```bash
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
UPDATE connection_runs
SET status = 'failed',
    completed_at = NOW(),
    error_message = 'Run timed out — no worker response within 10 minutes'
WHERE status = 'running'
  AND started_at < NOW() - INTERVAL '10 minutes'
  AND run_config->>'orchestration' != 'streaming'
RETURNING run_id, run_number, started_at;"
```

**Fix stuck streaming runs (from before fix):**
```bash
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
UPDATE connection_runs
SET status = 'completed',
    completed_at = NOW(),
    error_message = NULL
WHERE status = 'running'
  AND run_config->>'orchestration' = 'streaming'
RETURNING run_id, run_number, status, completed_at;"
```

---

### RB-003: Checkpoints Not Syncing (HTTP 422)

**Symptom:** Worker logs show `checkpoint sync failed` with HTTP 422 errors. Checkpoints not updating in `checkpoint_state` table.

**Diagnosis:**
```bash
# Check checkpoint sync logs:
docker logs fusion-cdc-worker 2>&1 | grep -i "checkpoint\|422" | tail -20

# Test checkpoint API manually:
curl -s -X POST \
  -H "X-Worker-Token: dev-worker-token" \
  -H "Content-Type: application/json" \
  -d '{"worker_id":"worker-dev-1","source_id":"1dda26ae-67ff-46b7-9134-94685055bc30","checkpoints":[{"schema_name":"fusion_cdc_metadata","table_name":"customers","lsn":"mysql-bin.000003:10708"}]}' \
  http://localhost:8000/api/v1/internal/checkpoints/batch
# Expected: {"upserted": 1}
# 422 → request body format wrong
```

**Root cause:** Old bug — checkpoint.py `push()` was sending a raw list instead of the nested `{worker_id, source_id, checkpoints: [...]}` structure. Fixed in current version.

**Verify fix is deployed:**
```bash
docker exec fusion-cdc-worker cat /app/cdc_worker/checkpoint.py | grep -A 5 "def push"
# Should show: payload = {"worker_id": ..., "source_id": ..., "checkpoints": [...]}
```

---

### RB-004: Redis Stream Memory Too High

**Symptom:** `docker exec fusion-redis redis-cli INFO memory` shows high used_memory; eviction happening.

**Immediate relief:**
```bash
# Trim all CDC streams to 10,000 events (adjust as needed):
for key in $(docker exec fusion-redis redis-cli keys "cdc:*"); do
  docker exec fusion-redis redis-cli XTRIM "$key" MAXLEN 10000
  echo "Trimmed: $key"
done
```

**Verify no eviction:**
```bash
docker exec fusion-redis redis-cli CONFIG GET maxmemory-policy
# Must be: noeviction
# If not: docker exec fusion-redis redis-cli CONFIG SET maxmemory-policy noeviction
```

**Long-term fix:**
- Reduce `REDIS_STREAM_MAXLEN` in worker env vars (currently 100,000)
- Ensure Spark consumer is processing events promptly (check consumer group lag)
- Upgrade Redis memory allocation

---

### RB-005: Worker Container Crash Loop

**Symptom:** `docker ps` shows `fusion-cdc-worker` in `Restarting` state.

**Diagnosis:**
```bash
# View crash reason:
docker logs fusion-cdc-worker --tail 50

# Common causes:
# - CONTROL_PLANE_URL unreachable (control plane not running)
# - REDIS_URL unreachable (Redis not running)
# - Missing ENCRYPTION_KEY (must match control plane)
# - Import error (Python dependency missing in image)

# Check environment:
docker inspect fusion-cdc-worker | python3 -c "
import sys,json; d=json.load(sys.stdin)
env = d[0]['Config']['Env']
for e in env: print(e)
" | grep -E "CONTROL_PLANE|REDIS|WORKER"
```

**Resolution by cause:**

| Cause | Fix |
|-------|-----|
| Control plane unreachable | Start control plane: `uvicorn app.main:app --port 8000` |
| Redis unreachable | Start Redis: `docker compose up -d redis` |
| Wrong image | Rebuild: `docker build ... -t docker-cdc-worker:latest` |
| Missing env var | Update `docker-compose.dev.yml` and recreate |

---

### RB-006: PostgreSQL Replication Slot Orphaned

**Symptom:** Postgres disk usage growing; `pg_replication_slots.active = false` for > 1 hour.

**Diagnosis:**
```bash
docker exec fusion-pg-source psql -U cdc_user source_db -c "
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_behind
FROM pg_replication_slots;"
```

**Immediate action:**
```bash
# Identify orphaned slots (active = false):
SLOT_NAME="fusion_<source_uuid>"

# Drop the orphaned slot:
docker exec fusion-pg-source psql -U cdc_user source_db \
  -c "SELECT pg_drop_replication_slot('${SLOT_NAME}');"

# Restart CDC worker — it will recreate the slot:
docker compose -f docker/docker-compose.dev.yml restart cdc-worker
```

**Prevention:**
- Always stop CDC worker gracefully (sends controlled shutdown signal)
- Monitor with alert: `CDCPGSlotOrphan`
- Never hard-kill Postgres while replication slot is active

---

### RB-007: High Replication Lag

**Symptom:** `cdc_stream_lag_seconds > 300` alert fires; data in destination is stale.

**Diagnosis tree:**
```
High lag detected
      │
      ├── Is CDC worker processing events?
      │     docker logs fusion-cdc-worker | grep "Published event" | tail -10
      │     If no recent events:
      │       → Check MySQL binlog activity: INSERT test row → verify event appears
      │
      ├── Are events in Redis? (worker side OK, consumer side slow)
      │     docker exec fusion-redis redis-cli XLEN "cdc:...:customers"
      │     If count growing fast → Spark consumer is slow
      │       → Check Spark executor count
      │       → Reduce micro-batch interval from 5s to 1s
      │
      ├── Is Spark consumer running?
      │     docker logs fusion-spark-consumer | tail -20
      │     If stopped → restart: docker compose up -d spark-consumer
      │
      └── Consumer group pending events?
            docker exec fusion-redis redis-cli XPENDING "cdc:...:customers" fusion-spark - + 100
            If thousands pending → consumer too slow
              → Increase consumer parallelism
```

---

### RB-008: DQ Rule Failures

**Symptom:** Alert `DQRuleFail` fires; rows not being written to destination.

**Investigation:**
```bash
# View DLQ entries:
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
SELECT connection_id, created_at, error_reason, payload->>'table_name' AS table
FROM event_dead_letter_queue
WHERE status = 'pending'
ORDER BY created_at DESC
LIMIT 20;"

# View violation detail:
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
SELECT rule_type, column_name, violation_count, sample_values, created_at
FROM dq_violations
WHERE connection_id = '79b50f04-...'
ORDER BY created_at DESC
LIMIT 10;"
```

**Resolution options:**

```bash
# Option A: Acknowledge violations (data acceptable as-is):
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
UPDATE event_dead_letter_queue
SET status = 'acknowledged', updated_at = NOW()
WHERE status = 'pending' AND connection_id = '79b50f04-...';"

# Option B: Retry DLQ events (after fixing source data):
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
UPDATE event_dead_letter_queue
SET status = 'pending', retry_count = retry_count + 1, updated_at = NOW()
WHERE status = 'acknowledged' AND connection_id = '79b50f04-...';"

# Option C: Loosen DQ rule threshold via UI:
# Settings → Data Quality → Edit Policy → adjust max_null_ratio or range bounds
```

---

### RB-009: Schema Change Detected

**Symptom:** `schema_change_events` table has pending entries; alert fires if policy = `manual_approval`.

**For MANUAL_APPROVAL policy:**
```bash
# View pending changes:
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
SELECT source_id, entity, change_type, old_def, new_def, created_at
FROM schema_change_events
WHERE status = 'pending'
ORDER BY created_at DESC;"
```

Then review in UI: Schema Evolution tab → accept or reject each change.

**For AUTO_APPLY policy:**
Changes are applied automatically. Verify in logs:
```bash
grep "schema change applied" /path/to/control-plane-logs
```

---

## 6. Scaling Guidelines

### When to Scale Each Component

| Component | Scale Signal | Action |
|-----------|-------------|--------|
| CDC Worker pods | CPU > 70% OR > 8 active streams per pod | Add pod via HPA or manual replicas |
| Redis | Memory > 70% of maxmemory | Add cluster shard (production) or increase container RAM (dev) |
| Spark streaming | Micro-batch duration > trigger interval | Add executors; reduce batch interval to 1s |
| Spark batch | Job duration > SLA | Increase executor count in SparkKubernetesOperator spec |
| Control plane | API p95 > 200ms | Add replicas; add PostgreSQL read replicas |
| PostgreSQL | Connection pool exhaustion | Increase max_connections; add pgBouncer |

### CDC Worker — Per-Source Capacity

Each worker pod can handle approximately:
- 10–20 concurrent tables (bounded by `MAX_CONCURRENT_TABLES` semaphore)
- ~10,000 events/second aggregate throughput (CPU-bound at envelope building + Redis XADD)

For a database host with 50 tables, consider 3 worker pods × ~17 tables each.

### Redis Cluster Sizing

| Events/day | Required Redis RAM |
|-----------|------------------|
| < 1M | 512MB |
| 1-10M | 2GB |
| 10-100M | 8GB |
| > 100M | Redis Cluster (3+3) |

---

## 7. Database Maintenance

### Vacuum Aging Tables

Connection runs accumulate rapidly. Archive old runs weekly:

```sql
-- Archive runs older than 30 days:
INSERT INTO connection_runs_archive SELECT * FROM connection_runs
WHERE completed_at < NOW() - INTERVAL '30 days';

DELETE FROM connection_runs
WHERE completed_at < NOW() - INTERVAL '30 days';

-- Vacuum and analyze:
VACUUM ANALYZE connection_runs;
```

### Redis Stream Cleanup

```bash
# Delete all events for a specific table (when connection is deleted):
docker exec fusion-redis redis-cli DEL "cdc:bank:tenant:source:schema:table"

# Trim all streams to last 1000 events:
for key in $(docker exec fusion-redis redis-cli keys "cdc:*"); do
  docker exec fusion-redis redis-cli XTRIM "$key" MAXLEN 1000
done
```

### SQLite Checkpoint Cleanup

SQLite checkpoints on the worker pod are replaced on every write, but the file can grow:

```bash
docker exec fusion-cdc-worker sqlite3 /var/lib/cdc/checkpoints.db "VACUUM;"
docker exec fusion-cdc-worker sqlite3 /var/lib/cdc/fallback.db "DELETE FROM fallback_events WHERE flushed = 1 AND created_at < datetime('now', '-7 days'); VACUUM;"
```

### Resetting a Connection for Re-Sync

To start a connection completely from scratch:

```bash
# 1. Delete checkpoint state:
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata -c "
DELETE FROM checkpoint_state WHERE source_id = '1dda26ae-...';
UPDATE connections SET initial_load_completed = false, initial_load_completed_at = null
WHERE connection_id = '79b50f04-...';"

# 2. Delete Redis streams for this connection:
for key in $(docker exec fusion-redis redis-cli keys "cdc:*:1dda26ae-*"); do
  docker exec fusion-redis redis-cli DEL "$key"
done

# 3. Restart CDC worker (picks up cleared checkpoint → restarts from binlog head):
docker compose -f docker/docker-compose.dev.yml restart cdc-worker

# 4. Trigger sync from UI — runs full initial load + starts CDC
```

---

## 8. Security Operations

### Rotating Secrets

**JWT Secret Key:**
```bash
# 1. Generate new secret:
python3 -c "import secrets; print(secrets.token_hex(32))"

# 2. Update .env:
JWT_SECRET_KEY=<new_secret>

# 3. Restart control plane — all existing JWTs will be invalidated
# Users must log in again
kill $(lsof -ti:8000)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &
```

**Encryption Key (AES-256 for DB credentials):**
```bash
# ⚠ WARNING: If you rotate this key, all stored source/destination credentials
#   become unreadable. You must re-enter credentials for all sources.

# 1. Export all plaintext credentials first:
docker exec fusion-postgres psql -U fusion_user -d fusion_cdc_metadata \
  -c "SELECT source_id, decrypt_creds(password_encrypted) FROM sources;"

# 2. Generate new key:
python3 -c "import os; print(os.urandom(32).hex())"

# 3. Update ENCRYPTION_KEY in .env AND in CDC worker env
# 4. Re-encrypt and update all credentials via API
```

**Worker Token:**
```bash
# 1. Generate new token:
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# 2. Update .env: WORKER_TOKEN=<new>
# 3. Update docker-compose.dev.yml: WORKER_TOKEN=<new>
# 4. Restart both control plane and CDC worker
```

### Audit Log Review

```sql
-- Recent admin actions:
SELECT user_id, action, resource_type, resource_id, created_at, ip_address
FROM audit_logs
WHERE created_at > NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC
LIMIT 50;

-- Failed login attempts:
SELECT user_id, created_at, ip_address
FROM audit_logs
WHERE action = 'login_failed'
  AND created_at > NOW() - INTERVAL '1 hour';
```

### Network Security Checks

```bash
# Verify Redis is not exposed to internet (should only bind to localhost in dev):
docker inspect fusion-redis | grep '"HostIp"'
# Expected: "0.0.0.0" (dev) — change to "127.0.0.1" in staging

# Verify control plane CORS allows only expected origins:
curl -s http://localhost:8000/openapi.json | python3 -c "import sys,json; print('OK')"
# Also test CORS header:
curl -H "Origin: http://evil.com" -I http://localhost:8000/api/v1/connections 2>&1 | grep -i access-control
```

---

## 9. Deployment Procedures

### Deploying a Control Plane Update

```bash
cd /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/control-plane

# 1. Run migrations if schema changed:
alembic upgrade head

# 2. The control plane uses --reload in dev, so code changes auto-apply.
# In production:
# docker build -f docker/Dockerfile.control-plane -t fusion-control-plane:latest .
# kubectl rollout restart deployment/fusion-control-plane

# 3. Verify startup:
curl http://localhost:8000/health
```

### Deploying a CDC Worker Update

```bash
# 1. Build new image (MUST use absolute path for build context):
docker build \
  -f /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/docker/Dockerfile.cdc-worker \
  -t docker-cdc-worker:latest \
  /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine

# 2. Stop old container and start with new image:
docker compose -f /Users/rishikeshsrinivas/Workspace/cdc/fusion-cdc-engine/docker/docker-compose.dev.yml \
  up -d --force-recreate cdc-worker

# 3. Verify new image is running:
docker inspect fusion-cdc-worker | grep '"Image"'
docker logs fusion-cdc-worker --tail 20

# 4. Verify health:
curl http://localhost:8081/health
```

> **⚠ Important:** Do NOT use `docker restart fusion-cdc-worker` — this reuses the old image. Always use `docker compose up -d --force-recreate` to pick up the new build.

### Database Migration (Alembic)

```bash
# Create migration after model changes:
cd control-plane
alembic revision --autogenerate -m "describe_your_change"

# Review generated migration file in migrations/versions/
# Apply:
alembic upgrade head

# Rollback:
alembic downgrade -1

# View history:
alembic history --verbose
```

---

## 10. Incident Response

### Severity Levels

| Level | Condition | Response Time |
|-------|-----------|--------------|
| P0 | Data loss risk, DW writes stopped completely | Immediate |
| P1 | Lag > 5 minutes on production streams | 15 minutes |
| P2 | Single connection failing, others OK | 1 hour |
| P3 | DQ violations, monitoring alerts only | Next business day |

### P0 Response: Data Pipeline Stopped

```bash
# 1. Identify which layer is broken:
echo "=== Layer 1: Is worker reading from DB? ==="
docker logs fusion-cdc-worker 2>&1 | grep "Published\|event" | tail -5

echo "=== Layer 2: Are events in Redis? ==="
for key in $(docker exec fusion-redis redis-cli keys "cdc:*"); do
  echo "$key: $(docker exec fusion-redis redis-cli XLEN $key)"
done

echo "=== Layer 3: Is Spark consuming? ==="
docker logs fusion-spark-consumer 2>&1 | tail -20

echo "=== Layer 4: Are rows landing in DW? ==="
docker exec fusion-postgres-dest psql -U dw_user -d fusion_dw \
  -c "SELECT tablename, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 5;"
```

**If Layer 1 broken** → restart CDC worker → re-trigger streaming
**If Layer 2 broken (Redis full)** → trim streams → verify maxmemory-policy=noeviction
**If Layer 3 broken** → restart Spark consumer → check DQ policy isn't blocking all rows
**If Layer 4 broken** → check destination Postgres connectivity, disk space, locks

### P1 Response: High Lag

See RB-007 above.

### Post-Incident Review Template

```markdown
## Incident Summary
- Date/Time: 
- Duration:
- Impact:

## Timeline
- HH:MM Alert fired
- HH:MM Investigation started
- HH:MM Root cause identified
- HH:MM Mitigation applied
- HH:MM Resolved

## Root Cause
<describe what caused the incident>

## Resolution
<what was done to fix it>

## Prevention
<what changes will prevent recurrence>
```

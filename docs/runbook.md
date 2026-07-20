# Fusion CDC Engine — Operations Runbook

> **Document Status:** Production  
> **Last Updated:** 2025  
> **Owner:** Platform Engineering  
> **On-call Escalation:** `#fusion-oncall` on Slack

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Service Startup & Shutdown](#2-service-startup--shutdown)
3. [Scaling Procedures](#3-scaling-procedures)
4. [Incident Response Playbooks](#4-incident-response-playbooks)
   - [4.1 Control Plane Down](#41-control-plane-down)
   - [4.2 CDC Worker Down / Stalled](#42-cdc-worker-down--stalled)
   - [4.3 Redis Down](#43-redis-down)
   - [4.4 Stream Lag SLA Breach](#44-stream-lag-sla-breach)
   - [4.5 PostgreSQL Replication Slot Bloat](#45-postgresql-replication-slot-bloat)
   - [4.6 Fallback Queue Growing](#46-fallback-queue-growing)
5. [Backup & Restore](#5-backup--restore)
6. [Log Aggregation](#6-log-aggregation)
7. [Common kubectl Commands](#7-common-kubectl-commands)
8. [Deployment & Rollback](#8-deployment--rollback)
9. [Maintenance Procedures](#9-maintenance-procedures)
10. [Alert Reference](#10-alert-reference)

---

## 1. Architecture Overview

```
                    ┌──────────────────┐
  API Clients ───── │  Control Plane   │ ─── PostgreSQL (meta)
                    │  (FastAPI/8000)  │
                    └────────┬─────────┘
                             │ REST
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
     │ CDC Worker   │ │ CDC Worker   │ │ CDC Worker   │
     │ (Postgres)   │ │  (MySQL)     │ │ (MongoDB)    │
     └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
            └────────────────┼────────────────┘
                             │ Redis Streams
                             ▼
                    ┌──────────────────┐
                    │  Spark Consumer  │
                    │  (PySpark 3.5)   │ ─── PostgreSQL (dest)
                    └──────────────────┘
```

**Key SLAs:**
- Stream lag: < 30 seconds P99
- Availability: 99.9% control plane, 99.5% workers
- RTO: < 15 minutes | RPO: < 5 minutes (checkpoint-based)

---

## 2. Service Startup & Shutdown

### Local (docker-compose)

```bash
# Start all services
cd fusion-cdc-engine
cp .env.example .env   # fill in secrets
docker compose -f docker/docker-compose.prod.yml up -d

# Graceful shutdown (preserves data volumes)
docker compose -f docker/docker-compose.prod.yml down

# Hard shutdown with volume removal (⚠️ data loss)
docker compose -f docker/docker-compose.prod.yml down -v
```

### Kubernetes (production)

```bash
# Start control plane only
kubectl scale deploy/fusion-prod-control-plane --replicas=2 -n fusion

# Stop all CDC workers gracefully (drains connections/checkpoints)
kubectl scale sts/fusion-prod-cdc-worker-postgres --replicas=0 -n fusion
kubectl scale sts/fusion-prod-cdc-worker-mysql    --replicas=0 -n fusion
kubectl scale sts/fusion-prod-cdc-worker-mongodb  --replicas=0 -n fusion

# Stop Spark consumer (Recreate strategy — single replica)
kubectl scale deploy/fusion-prod-spark-consumer --replicas=0 -n fusion

# Full deployment pause (maintenance window)
kubectl annotate ns fusion dcraftfusion.io/maintenance="true"
for deploy in $(kubectl get deploy -n fusion -o name); do
  kubectl scale $deploy --replicas=0 -n fusion
done
```

---

## 3. Scaling Procedures

### Scale control plane replicas

```bash
# Via Helm (preferred — persists across restarts)
helm upgrade fusion-prod fusion-cdc-engine/helm/fusion-cdc/ \
  --reuse-values \
  --set controlPlane.replicaCount=4 \
  -n fusion

# Via kubectl (ephemeral — HPA will override)
kubectl scale deploy/fusion-prod-control-plane --replicas=4 -n fusion
```

### Scale CDC workers

```bash
# Workers are StatefulSets — scale per source type
helm upgrade fusion-prod fusion-cdc-engine/helm/fusion-cdc/ \
  --reuse-values \
  --set cdcWorkers.postgres.replicas=3 \
  -n fusion
```

### HPA tuning

```bash
# Check current HPA status
kubectl get hpa -n fusion

# Manually trigger scale-out (testing)
kubectl patch hpa fusion-prod-control-plane-hpa -n fusion \
  --patch '{"spec":{"minReplicas":3}}'
```

---

## 4. Incident Response Playbooks

### 4.1 Control Plane Down

**Alert:** `ControlPlaneDown`  
**Severity:** Critical

**Diagnosis:**
```bash
# Check pod status
kubectl get pods -n fusion -l app.kubernetes.io/component=control-plane

# View recent events
kubectl describe deploy/fusion-prod-control-plane -n fusion | tail -30

# Tail logs from crashed pod
kubectl logs -n fusion -l app.kubernetes.io/component=control-plane \
  --previous --tail=200

# Check database connectivity
kubectl exec -n fusion deploy/fusion-prod-control-plane -- \
  python -c "import psycopg2; psycopg2.connect('$DATABASE_URL'); print('DB OK')"
```

**Resolution:**
1. If OOMKilled → increase memory limit: `helm upgrade ... --set controlPlane.resources.limits.memory=1Gi`
2. If CrashLoopBackOff → check env vars in secrets: `kubectl get secret fusion-prod-secrets -n fusion -o yaml`
3. If ImagePullBackOff → verify image tag exists in registry
4. Database unreachable → check PostgreSQL pod and NetworkPolicy

---

### 4.2 CDC Worker Down / Stalled

**Alert:** `CDCWorkerDown`  
**Severity:** Critical

**Diagnosis:**
```bash
# Identify which worker is down
kubectl get pods -n fusion -l app.kubernetes.io/component=cdc-worker

# Check StatefulSet rollout
kubectl rollout status sts/fusion-prod-cdc-worker-postgres -n fusion

# Check checkpoint file (inside pod)
kubectl exec -n fusion fusion-prod-cdc-worker-postgres-0 -- \
  ls -la /var/lib/cdc/checkpoints/

# Tail worker logs
kubectl logs -n fusion fusion-prod-cdc-worker-postgres-0 --tail=100 -f
```

**Resolution:**
1. Worker stuck at startup → delete pod to trigger restart: `kubectl delete pod fusion-prod-cdc-worker-postgres-0 -n fusion`
2. Corrupted checkpoint → remove checkpoint file and restart: 
   ```bash
   kubectl exec -n fusion fusion-prod-cdc-worker-postgres-0 -- \
     rm /var/lib/cdc/checkpoints/postgres_*.json
   kubectl delete pod fusion-prod-cdc-worker-postgres-0 -n fusion
   ```
3. Source DB unreachable → check NetworkPolicy and source credentials

---

### 4.3 Redis Down

**Alert:** `RedisDown`  
**Severity:** Critical

**Impact:** CDC event transport disrupted. Workers activate fallback queues to local disk. No data loss for up to `cdc_fallback_queue_max_size` events.

**Diagnosis:**
```bash
# Check Redis pod
kubectl get pods -n fusion -l app.kubernetes.io/name=fusion-prod-redis

# Redis CLI
kubectl exec -n fusion fusion-prod-redis-0 -c redis -- redis-cli ping

# Check AOF / RDB
kubectl exec -n fusion fusion-prod-redis-0 -c redis -- \
  redis-cli INFO persistence
```

**Resolution:**
1. Pod crash → `kubectl delete pod fusion-prod-redis-0 -n fusion`
2. OOM → check `maxmemory` policy: `redis-cli CONFIG GET maxmemory-policy`
3. After recovery — fallback queues drain automatically within 60s
4. Check drain rate in Grafana: `cdc_fallback_queue_drained_total`

---

### 4.4 Stream Lag SLA Breach

**Alert:** `CDCStreamLagCritical` (>30s)  
**Severity:** Critical

**Diagnosis:**
```bash
# Check lag metric per tenant
kubectl port-forward svc/fusion-prod-control-plane-svc 8000:8000 -n fusion &
curl -s http://localhost:8000/api/v1/monitoring/metrics | \
  grep cdc_stream_lag_seconds | sort -k2 -n | tail -20

# Check Spark consumer logs
kubectl logs -n fusion deploy/fusion-prod-spark-consumer --tail=200
```

**Resolution:**
1. Spark consumer slow → check JVM GC pressure, increase `sparkConsumer.resources.limits.memory`
2. Redis backpressure → check Redis memory and throughput
3. Postgres destination slow → VACUUM ANALYZE on destination tables
4. Scale up Spark consumer (change strategy to Recreate already set)

---

### 4.5 PostgreSQL Replication Slot Bloat

**Symptoms:** Source Postgres disk usage growing; WAL files not being cleaned.

**Diagnosis:**
```bash
# Connect to source postgres
kubectl port-forward svc/pg-source-svc 5434:5432 -n fusion &
psql -h localhost -p 5434 -U fusion_user -d source_db -c \
  "SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes FROM pg_replication_slots;"
```

**Resolution:**
```sql
-- If worker is active but lagging, restart the worker pod
-- If slot is stale (active=false, no worker using it), drop it:
SELECT pg_drop_replication_slot('fusion_slot_<tenant>');

-- Re-create slot after restarting CDC worker
-- Worker auto-creates slot on startup
```

---

### 4.6 Fallback Queue Growing

**Alert:** `CDCFallbackQueueGrowing` (>1000) / `CDCFallbackQueueCritical` (>10000)

**Diagnosis:**
```bash
# Check Redis connection from worker
kubectl exec -n fusion fusion-prod-cdc-worker-postgres-0 -- \
  python -c "import redis; r=redis.Redis.from_url('redis://fusion-prod-redis-master:6379/0'); print(r.ping())"

# Check fallback queue files
kubectl exec -n fusion fusion-prod-cdc-worker-postgres-0 -- \
  ls -lh /var/lib/cdc/fallback/
```

**Resolution:**
1. Fix Redis (see 4.3) — queue drains automatically
2. If PVC almost full — scale up PVC or manually drain oldest events

---

## 5. Backup & Restore

### PostgreSQL metadata backup

```bash
# Manual backup
kubectl exec -n fusion $(kubectl get pod -n fusion \
  -l app.kubernetes.io/name=postgres-meta -o name | head -1) -- \
  pg_dump -U fusion_user fusion_cdc_metadata > backup_$(date +%Y%m%d).sql

# Restore
kubectl exec -n fusion <postgres-pod> -- \
  psql -U fusion_user fusion_cdc_metadata < backup_20250101.sql
```

### Redis backup (AOF)

```bash
# Trigger BGSAVE
kubectl exec -n fusion fusion-prod-redis-0 -c redis -- redis-cli BGSAVE

# Copy dump.rdb from PVC
kubectl cp fusion/fusion-prod-redis-0:/data/dump.rdb ./redis-backup-$(date +%Y%m%d).rdb
```

### CDC Worker checkpoints

```bash
# Backup all checkpoint files
for i in 0 1 2; do
  kubectl cp fusion/fusion-prod-cdc-worker-postgres-$i:/var/lib/cdc/checkpoints/ \
    ./checkpoint-backup/postgres-$i/
done
```

---

## 6. Log Aggregation

### Direct kubectl logs

```bash
# All control-plane pods, last 1 hour
kubectl logs -n fusion -l app.kubernetes.io/component=control-plane \
  --since=1h --timestamps

# Stream all CDC worker logs
kubectl logs -n fusion -l app.kubernetes.io/component=cdc-worker \
  -f --max-log-requests=10

# Search for errors across all pods
kubectl logs -n fusion -l app.kubernetes.io/part-of=fusion-cdc \
  --since=30m | grep -E '"level":"error"|ERROR|CRITICAL'
```

### Structured log queries (if using Loki)

```logql
# High-severity events
{namespace="fusion"} |= `"level":"error"` | json | line_format "{{.ts}} {{.pod}} {{.msg}}"

# Events for specific tenant
{namespace="fusion", container="cdc-worker"} |= `"tenant":"acme"` | json
```

---

## 7. Common kubectl Commands

```bash
# Set context for fusion
kubectl config set-context --current --namespace=fusion

# Get all fusion resources
kubectl get all -n fusion

# Describe failing pod
kubectl describe pod <pod-name> -n fusion

# Get events sorted by time
kubectl get events -n fusion --sort-by='.lastTimestamp'

# Check resource usage
kubectl top pods -n fusion
kubectl top nodes

# Force delete stuck pod (only if graceful delete hangs >30s)
kubectl delete pod <pod-name> -n fusion --grace-period=0 --force

# Execute shell in running container
kubectl exec -it <pod-name> -n fusion -- /bin/sh

# Copy file from pod
kubectl cp fusion/<pod-name>:/path/to/file ./local-file

# Port-forward for local debugging
kubectl port-forward svc/fusion-prod-control-plane-svc 8000:8000 -n fusion
```

---

## 8. Deployment & Rollback

### Deploy new version

```bash
# Tag and push (triggers CD pipeline automatically)
git tag v2.1.0 && git push origin v2.1.0

# Or manual Helm deploy
helm upgrade fusion-prod fusion-cdc-engine/helm/fusion-cdc/ \
  --reuse-values \
  --set controlPlane.image.tag=2.1.0 \
  --set cdcWorkers.image.tag=2.1.0 \
  --set sparkConsumer.image.tag=2.1.0 \
  -n fusion --wait
```

### Rollback

```bash
# View Helm history
helm history fusion-prod -n fusion

# Rollback to previous revision
helm rollback fusion-prod -n fusion

# Rollback to specific revision
helm rollback fusion-prod 3 -n fusion

# Verify rollback
helm status fusion-prod -n fusion
kubectl rollout status deploy/fusion-prod-control-plane -n fusion
```

---

## 9. Maintenance Procedures

### Rotate secrets

```bash
helm upgrade fusion-prod fusion-cdc-engine/helm/fusion-cdc/ \
  --reuse-values \
  --set controlPlane.secrets.secretKey=$(openssl rand -hex 32) \
  --set controlPlane.secrets.encryptionKey=$(openssl rand -hex 16) \
  --set controlPlane.secrets.workerTokenSecret=$(openssl rand -hex 32) \
  --set cdcWorkers.workerToken=$(openssl rand -hex 32) \
  -n fusion
# Workers reconnect with new token within 60s
```

### Database schema migration

```bash
# Run Alembic migration (runs automatically on control-plane startup)
kubectl exec -n fusion deploy/fusion-prod-control-plane -- \
  alembic upgrade head

# Check migration status
kubectl exec -n fusion deploy/fusion-prod-control-plane -- \
  alembic current
```

### Certificate renewal (cert-manager)

```bash
# Check certificate expiry
kubectl get certificate -n fusion
kubectl describe certificate fusion-tls -n fusion

# Force renewal
kubectl annotate certificate fusion-tls \
  cert-manager.io/issuer-name=letsencrypt-prod \
  --overwrite -n fusion
```

---

## 10. Alert Reference

| Alert | Severity | Threshold | Action |
|-------|----------|-----------|--------|
| `ControlPlaneDown` | Critical | >1m | See §4.1 |
| `CDCWorkerDown` | Critical | >2m | See §4.2 |
| `RedisDown` | Critical | >1m | See §4.3 |
| `CDCStreamLagWarning` | Warning | >10s for 5m | Monitor |
| `CDCStreamLagCritical` | Critical | >30s for 5m | See §4.4 |
| `CDCErrorRateHigh` | Warning | >1/s for 5m | Check logs |
| `CDCFallbackQueueGrowing` | Warning | >1000 for 5m | See §4.6 |
| `CDCFallbackQueueCritical` | Critical | >10000 for 2m | See §4.6 |
| `CDCWorkerMemoryHigh` | Warning | >85% for 10m | Scale up |
| `PVCAlmostFull` | Warning | <15% free for 5m | Expand PVC |

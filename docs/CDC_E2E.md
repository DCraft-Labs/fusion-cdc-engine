# CDC End-to-End Test — DuckDB Lake Path

This document describes the end-to-end CDC test that gates the v1.2.0 release.
Spark is **not** required for any of these flows.

## Topology

```
mysql-source (fusion_e2e DB, 2GB) ──┐
                                    ├─→ CDC worker → Redis → transform-worker ─┬─→ postgres-dest (fusion_dw)
                                    │                                          └─→ Iceberg on MinIO via Nessie
                                    └─ (binlog ROW, server-id=1)
```

## Prerequisites

1. Docker Desktop with Kubernetes enabled (16 GB / 8 CPU — see `Phase 0`).
2. Compose stack up:
   ```bash
   docker compose -f docker/docker-compose.dev.yml up -d
   ```
3. Seed connectors + admin:
   ```bash
   docker compose -f docker/docker-compose.dev.yml exec postgres-meta \
     psql -U fusion_user -d fusion_cdc_metadata -f /seed/seed-admin.sql
   ```
4. MySQL schema loaded:
   ```bash
   docker compose -f docker/docker-compose.dev.yml exec -T mysql-source \
     mysql -u fusion_user -pfusion_password < scripts/e2e/mysql-init-schema.sql
   ```

## Steps

### 1. Load 2 GB into MySQL source
```bash
python scripts/e2e/mysql-load.py --target-gb 2 --truncate \
  --dsn mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e
```
Expected: ~40M order rows + 200K customers + 10K products. Wall time on a laptop
with Docker Desktop (8c, 16GB): ~3–7.5 hours at 15–40 MB/s. For a smoke test use
`--target-gb 0.05` (~50 MB, ~1–2 min).

### 2. Run the E2E driver
```bash
kubectl -n dcraft-local port-forward svc/fusion-cdc-control-plane 18000:8000 &
python scripts/e2e/cdc_e2e.py \
  --base-url http://127.0.0.1:18000 \
  --mysql-dsn mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e \
  --pg-dest-dsn postgresql://dw_user:dw_password@localhost:5433/fusion_dw \
  --target-gb 2 --churn-mb 500
```

The driver:
1. Logs in as `admin / Admin@123`
2. Creates MySQL source + Postgres destination (or reuses seeded ones)
3. Creates a CDC connection with `orders`, `customers`, `products` streams
4. Triggers initial sync and polls `connection_runs` until complete
5. Runs `mysql-churn.py` (500 MB I/U/D mix)
6. Waits for CDC catch-up
7. Repeats 3–6 for the Iceberg (MinIO + Nessie) destination via the DuckDB/PyIceberg path
8. Verifies row counts in both destinations match the source

### 3. Apply 500 MB churn
```bash
python scripts/e2e/mysql-churn.py --target-mb 500 \
  --dsn mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e
```
Generates ~60% inserts / 30% updates / 10% deletes against `orders`.

## Acceptance

- [ ] Initial sync completes for both Postgres and Iceberg destinations
- [ ] Row counts in `dw.orders`, `dw.customers`, `dw.products` match source ±1%
- [ ] Iceberg tables queryable via DuckDB:
      ```sql
      SELECT count(*), count_distinct(customer_id) FROM iceberg_scan('s3://iceberg-warehouse/fusion-cdc/fusion/orders');
      ```
- [ ] After churn, CDC lag drains to < 60s in steady state
- [ ] Worker restart (`kubectl delete pod -l app=transform-worker`) does not lose events
- [ ] Pause/resume connection preserves exactly-once semantics (no duplicate rows)

## Failure modes tested

- Source binlog rotation during sync
- transform-worker OOM restart (KEDA scale-from-zero)
- Redis stream consumer-group rebalance
- Iceberg commit conflict on concurrent upserts (PyIceberg retry)

## Time budget

| Stage | Laptop (8c/16GB) | Beefy VM (16c/64GB) |
|-------|------------------|---------------------|
| 2 GB MySQL load | 3–7.5 h | 1–2.5 h |
| Initial sync → Postgres | 1–3 h | 20–45 min |
| Initial sync → Iceberg | 1.5–4 h | 25–60 min |
| 500 MB churn + catch-up | 15–45 min | 5–15 min |
| **Total** | **5.5–15 h** | **~1.5–4 h** |

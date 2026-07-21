# Post-Deploy Checklist — Fusion CDC Engine

Operational steps an operator MUST run after a fresh deploy / seed before the
UI is fully exercisable. These are **operational** steps, not code bugs — they
require the source DB to be reachable from the control-plane pod.

## 1. Verify the self-healing seed ran

The control-plane's startup hook (`control-plane/app/seed/seed_admin.py`)
auto-seeds the admin user + 6 connector definitions + 1 sample source + 2
sample destinations + 1 sample connection on every boot when it finds the
metadata DB empty.

Confirm via the API (HTTP-only):

```bash
TOKEN=$(curl -s -X POST http://<cdc-host>:<port>/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"Admin@123"}' | jq -r .access_token)

curl -s http://<cdc-host>:<port>/api/v1/connector-definitions \
  -H "Authorization: Bearer $TOKEN" | jq '.total'   # expect 6
curl -s http://<cdc-host>:<port>/api/v1/sources \
  -H "Authorization: Bearer $TOKEN" | jq '.total'   # expect 1
curl -s http://<cdc-host>:<port>/api/v1/destinations \
  -H "Authorization: Bearer $TOKEN" | jq '.total'   # expect 2
curl -s http://<cdc-host>:<port>/api/v1/connections \
  -H "Authorization: Bearer $TOKEN" | jq '.total'  # expect 1
```

## 2. Run discovery on the seeded source (REQUIRED for the connection wizard's stream editor)

The seeded `pg-source` ships with **no discovered streams** (`discovery_cache`
is null, `last_discovery_at` is null). The Create Connection wizard's Step 3
"Streams & Transforms" — including the per-stream Iceberg partition editor —
can only render streams that the source has discovered.

**Run discovery after seeding** (and after every fresh deploy that wipes the
metadata DB):

```bash
SOURCE_ID=$(curl -s http://<cdc-host>:<port>/api/v1/sources \
  -H "Authorization: Bearer $TOKEN" | jq -r '.sources[0].id')

curl -s -X POST "http://<cdc-host>:<port>/api/v1/sources/$SOURCE_ID/discover" \
  -H "Authorization: Bearer $TOKEN" | jq .
```

This requires the source Postgres (`pg-source`) to be reachable from the
control-plane pod (network policy + `wal_level=logical` + replication slot
`fusion_slot` all configured by `infra/local-dev/k8s/00-infra.yaml`).

After discovery completes, re-open the Create Connection wizard — Step 3 will
list the discovered tables and the per-stream Iceberg partition editor becomes
exercisable.

## 3. Verify Kafka health is observable (v1.2.3+)

```bash
curl -s http://<cdc-host>:<port>/api/v1/monitoring/health | jq .
# Expect "services": { "database": "healthy", "redis": "healthy", "kafka": "healthy" }
```

If `kafka` reports `not_configured`, set `KAFKA_BOOTSTRAP_SERVERS` on the
control-plane deployment (the chart wires this automatically when
`kafka.enabled=true`). If `unhealthy`, the broker is unreachable from the
control-plane pod — check network policy + the in-cluster Kafka service name.

## 4. Verify Fusion kernel auth (community / local-dev)

The Fusion kernel's protected API routes require either:
- `FUSION_AUTH_MODE=dev` (local-dev overlay sets this; bypasses bearer auth), OR
- `FUSION_OIDC_JWKS_URL` pointing at the kernel's own JWKS endpoint
  (`http://<release>-control-plane-kernel:8080/oidc/jwks`).

Confirm with a bearer-token request:

```bash
FTOKEN=$(curl -s -X POST http://<fusion-host>:<port>/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"founder@dcraftlabs.com","password":"changeme-founder"}' \
  | jq -r .access_token)

curl -s http://<fusion-host>:<port>/api/v1/auth/me \
  -H "Authorization: Bearer $FTOKEN"   # expect 200, not 401
```

If you see `401 {"error":"Get \"\": unsupported protocol scheme \"\"}`, the
JWKS URL is empty and `FUSION_AUTH_MODE` is not `dev` — fix one of the two
above env vars on the kernel deployment.

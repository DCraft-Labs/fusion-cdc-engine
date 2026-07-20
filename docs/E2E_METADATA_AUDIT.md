# E2E Metadata Audit

This file is **generated** by `scripts/e2e/audit_metadata.py` after a CDC E2E run.
It records per-table row counts, audit-log events, and any missing audit hooks.

## Regenerate

```bash
# After running the E2E driver (see docs/CDC_E2E.md)
python scripts/e2e/audit_metadata.py \
  --dsn postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata \
  --out docs/E2E_METADATA_AUDIT.md
```

## What the audit checks

### Expected tables
- `users`, `roles`, `user_roles` — RBAC seeded by `seed-admin.sql`
- `connector_definitions` — 6 connectors (3 sources + 3 destinations)
- `sources`, `destinations`, `connections` — created during E2E
- `connection_runs`, `connection_run_events` — one run per sync trigger
- `checkpoint_state` — one row per (worker, source, schema, table) during initial load
- `audit_logs` — every state-changing operation

### Expected audit events (`audit_logs.action`)
| Event | Fired by |
|-------|----------|
| `user.login` | auth login handler |
| `source.create` / `source.update` | sources API |
| `destination.create` / `destination.update` / `destination.test` | destinations API |
| `connection.create` / `connection.update` / `connection.sync` | connections API |
| `connection_run.start` / `connection_run.complete` | sync orchestrator |
| `checkpoint.update` | control-plane `/api/v1/internal/checkpoints/batch` (called by transform-worker) |
| `connection.batch_run.success` / `.error` | `/api/v1/internal/connections/{id}/run-complete` (Airflow callback) |

## Verdict

> The committed version of this file is a placeholder. Run the audit script
> after a full E2E to populate it. A green audit (no missing events, no empty
> tables) is a release gate for v1.2.0.

## Known gaps to fix before release

The audit script will report any of these if they are still missing at audit
time. Track them in the v1.2.0 milestone:

1. **`destination.test`** — the test-connection endpoint must write an audit
   entry with the test result + checks array.
2. **`checkpoint.update`** — the transform-worker calls
   `/internal/load-checkpoints` after each chunk; the control-plane handler
   must write an audit entry.
3. **`connection_run.complete`** — the sync orchestrator must write the audit
   entry on both success and failure paths (currently only success).
4. **Iceberg writer heartbeats** — `cdc_workers` / `worker_heartbeats` should
   show the transform-worker as healthy with `last_heartbeat_at` within 60s.

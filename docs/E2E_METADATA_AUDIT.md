# E2E Metadata Audit

Generated: `2026-07-20T16:27:50.229415Z`
DSN: `postgresql://fusion:fusion_local@127.0.0.1:55432/fusion_cdc_metadata`

## Table row counts

| Table | Rows | Has data |
|-------|------|----------|
| `users` | 1 | yes |
| `roles` | 1 | yes |
| `user_roles` | 1 | yes |
| `connector_definitions` | 6 | yes |
| `sources` | 1 | yes |
| `destinations` | 2 | yes |
| `connections` | 1 | yes |
| `connection_runs` | 0 | NO |
| `connection_run_events` | -1 | NO |
| `checkpoint_state` | -1 | NO |
| `audit_logs` | -1 | NO |

## Audit log events

_(audit_log table empty or not queryable)_

## Missing audit events

The following expected audit events did not fire during the E2E run:

- `user.login`
- `source.create`
- `source.update`
- `destination.create`
- `destination.update`
- `destination.test`
- `connection.create`
- `connection.update`
- `connection.sync`
- `connection_run.start`
- `connection_run.complete`
- `checkpoint.update`

### Recommended fixes

Add audit-log writes in the control-plane handlers listed below. Each
should call `audit_log.record(user_id, event_type, entity_id, payload)`
after the DB transaction commits:

| Event | Handler |
|-------|---------|
| `user.login` | `app/api/auth.py::login` |
| `source.create` | `app/api/sources.py::create_source` |
| `destination.create` | `app/api/destinations.py::create_destination` |
| `destination.test` | `app/api/destinations.py::test_connection` |
| `connection.create` | `app/api/connections.py::create_connection` |
| `connection.sync` | `app/api/connections.py::trigger_sync` |
| `connection_run.start` | `app/services/sync_orchestrator.py::start_run` |
| `connection_run.complete` | `app/services/sync_orchestrator.py::complete_run` |
| `checkpoint.update` | `transform-worker/loader.py::_mark_chunk_done` (via control-plane `/internal/load-checkpoints`) |

## Empty tables

The following tables have zero rows after E2E — verify the E2E actually
exercised the code path that writes them:

- `connection_runs`

## Missing tables

The following tables could not be queried — they may not exist or the
schema is out of date. Run `alembic upgrade head` in the control plane:

- `connection_run_events`
- `checkpoint_state`
- `audit_logs`

## Verdict

❌ **FAIL** — see sections above for missing items.


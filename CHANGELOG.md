# Changelog

All notable changes to Fusion CDC Engine (private repo) are documented here.
This project follows [Keep a Changelog](https://keepachangelog.com/) and
uses [Semantic Versioning](https://semver.org/).

## [1.2.2] — 2026-07-21

Self-healing CDC seed. Fixes the v1.2.1 regression where the CDC metadata DB
stayed empty (`connector-definitions`, `sources`, `destinations`,
`connections` all returned `total: 0`) even though `deploy.ps1` reported a
successful seed and the admin user existed.

### Fixed
- **`scripts/seed-admin.sql` — broken INSERTs rolled back the whole
  seed (BLOCKER, root cause).** The seed ran as a single atomic
  `DO $$ ... $$;` block, but two INSERTs referenced columns that do not
  exist on the live schema (post-Alembic):
  - `destinations` INSERT listed `host`, `port`, `database_name`,
    `schema_name`, `username`, `password_encrypted`, `ssl_enabled`,
    `ssl_config`, `config`. The `destinations` table has none of these —
    all connection fields live inside the `connection_config` JSONB
    column (`schemas/schema_postgres.sql:154`,
    `app/models/source_destination.py:88`). The first missing-column
    error (`column "host" does not exist`) aborted the DO block.
  - `connections` INSERT listed `sync_enabled`, `replication_slot`,
    `publication`, `namespace_definition`, `namespace_format`,
    `stream_prefix`, `config`. None of these exist on `connections`
    (`schemas/schema_postgres.sql:306`, `app/models/connection.py:11`).
    `replication_slot`/`publication` belong on the SOURCE `config`
    JSONB (already set in the sources INSERT), not the connection.
  Because the DO block is atomic, the failure rolled back the
  `connector_definitions`, `sources`, and `user_roles` INSERTs that ran
  earlier in the same block — leaving every CDC table at `total: 0`.
  The admin `users` row existed only because it had been registered
  manually via `/auth/register` (the control-plane has NO startup
  admin-creation hook — confirmed by reading `app/main.py` lifespan and
  every `User(...)` construction site). Rewrote both INSERTs against
  the real schema: `destinations` now writes a single `connection_config`
  JSONB; `connections` drops the non-existent columns and keeps only
  `connection_name, source_id, destination_id, sync_mode, sync_type,
  status, resource_limits, schema_evolution_policy,
  initial_load_completed, created_by, created_at, updated_at`.

- **Self-healing seed on control-plane startup (the actual fix).**
  `kubectl cp + psql -f` from `deploy.ps1` is fragile — it depends on
  the postgres pod being ready, the file path being correct, the SQL
  being valid, and no transient kubectl errors. Even with v1.2.1
  fail-fast, the seed silently no-op'd whenever the DO block rolled
  back. Added a startup seed hook to the control-plane that runs AFTER
  Alembic migrations on every boot and is bulletproof:
  - New module `control-plane/app/seed/` (`__init__.py`,
    `seed_admin.py`, `seed-admin.sql`) bakes the corrected seed SQL
    into the Docker image (the Dockerfile already `COPY control-plane/ ./`
    so no Dockerfile change is needed). The seed no longer depends on
    `kubectl cp` or any external file.
  - `seed_admin.run_seed(db)` checks
    `SELECT COUNT(*) FROM connector_definitions`; if 0, it executes
    the baked-in `seed-admin.sql` against the metadata DB; if >0, it
    skips (the seed SQL is idempotent anyway). Logs clearly on every
    path: `"Seed: connector_definitions empty, running seed..."` /
    `"Seed: N connector definition(s) already present, skipping
    auto-seed."` / `"Seed: applied successfully — connector_definitions
    now has N row(s)."` / `"Seed: FAILED to apply seed SQL — <error>"`.
  - Errors are logged loudly (with `exc_info=True`) but DO NOT crash
    the app — the control-plane must still start so operators can
    debug. The seed is retried on the next pod restart.
  - Wired into `app/main.py` `lifespan()` as the first startup step,
    before the periodic re-introspection task and the worker scheduler.
  This makes the deployment self-healing: no matter what happens with
  `deploy.ps1`, `kubectl cp`, or postgres restarts, the control-plane
  re-seeds itself whenever it starts and finds an empty DB.

### Changed
- **Control-plane version string.** `control-plane/app/main.py`
  FastAPI `version="1.2.1"` → `version="1.2.2"` (so `/api/openapi.json`
  reports the new release).
- **Private Helm chart version.** `helm/fusion-cdc/Chart.yaml` bumped
  `1.2.1` → `1.2.2` (both `version` and `appVersion`).

### Notes
- `deploy.ps1` (in the public repo) is updated to note that the
  `kubectl cp + psql -f` path is now a FALLBACK for manual re-seeding;
  the primary seed mechanism is the control-plane startup hook. The
  seed SQL is idempotent so running both is a no-op when the DB is
  already populated.

## [1.2.1] — 2026-07-21

Hotfix release addressing the three hard blockers found when verifying v1.2.0
against the remote (192.168.1.10) deployment, plus the missing Kafka
dependency for the CDC pipeline.

### Fixed
- **CDC `/settings/audit-logs` blank page (BLOCKER).** The frontend had an
  `AuditLogsPage` route (`/settings/audit-logs`) and a Settings card linking
  to it, but the control-plane has no `/api/v1/settings/audit-logs`
  endpoint — the `audit_logs` table exists but no router reads from it.
  Navigating to the page produced a blank/broken UI. Removed the "Audit
  Logs" card from `frontend/src/pages/settings/SettingsPage.tsx` and the
  route + import from `frontend/src/App.tsx`. The `AuditLogsPage.tsx` file
  is retained for the follow-up that adds the backend endpoint.

### Changed
- **Control-plane version string.** `control-plane/app/main.py` initialized
  FastAPI with `version="0.1.0"`, so `/api/openapi.json` reported the wrong
  version. Bumped to `version="1.2.1"`.
- **Private Helm chart version.** `helm/fusion-cdc/Chart.yaml` bumped from
  `2.0.0` to `1.2.1` (both `version` and `appVersion`) to match the public
  chart and the app release.

### Notes
- The Kafka manifest in `kubernetes/base/kafka.yaml` is unchanged in this
  repo — the user-facing fix lives in the public `dcraft-fusion` /
  `fusion-cdc` Helm charts (see that repo's CHANGELOG). The kustomize
  manifest remains the source of truth for the in-cluster broker shape and
  was used as the basis for the new `templates/kafka.yaml` in the public
  chart.
- **Follow-up (not in this release):** implement a minimal
  `/api/v1/settings/audit-logs` endpoint (paginated list, filters by
  user/action/resource/date) reading from the existing `audit_logs` table,
  then re-add the `AuditLogsPage` route + Settings card.

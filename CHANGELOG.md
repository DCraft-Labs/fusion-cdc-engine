# Changelog

All notable changes to Fusion CDC Engine (private repo) are documented here.
This project follows [Keep a Changelog](https://keepachangelog.com/) and
uses [Semantic Versioning](https://semver.org/).

## [1.2.26] — 2026-07-23

### Initial Load — Multi-pod Parallelism (Task 1, the big one — Section 3.5)
- **Intra-table parallelism (Task 1a/1b):** the producer
  (`connections._enqueue_initial_load_tasks`) now partitions each stream's
  `[min(pk), max(pk)]` range into `K` disjoint sub-ranges (K =
  `resource_limits.parallelism`, clamped to [1, 16], default 4) and enqueues
  `K` independent `initial_load` tasks — one per range — each stamped with
  `chunk_seq` (0..K-1), `pk_start`, `pk_end`, and `total_chunks=K`. KEDA
  then scales the transform-worker to K concurrent pods, giving true
  intra-table parallelism (a 118M-row table with K=16 loads in ~1/16 the
  wall-clock time, bounded by the slowest partition).
  - Partitioning strategy: `SELECT MIN(pk), MAX(pk), COUNT(*)` (one query);
    for tables > 1M rows, approximate-percentile PK sampling
    (`SELECT pk ... LIMIT 1 OFFSET o` at K-1 evenly-spaced offsets) builds
    robust split points resilient to PK gaps from deletes; for smaller
    tables, a naive even split of the numeric range. MongoDB uses K=1
    (ObjectId is non-numeric; inter-table parallelism still applies).
  - Pure helpers extracted to `control-plane/app/services/partitioning.py`
    (single source of truth, unit-tested).
- **Worker reads partition bounds (Task 1c):** `InitialLoadTask.run` reads
  `pk_start`/`pk_end`/`chunk_seq`/`total_chunks` from the task payload,
  resumes from `max(last_pk, pk_start)`, and the per-DB fetchers
  (`_fetch_pg_chunk`, `_fetch_mysql_chunk`) now apply `AND pk <= pk_end` so
  each pod's fetches stay within its disjoint range — this is the
  correctness invariant (no row fetched by two pods, no row skipped).
- **Composite checkpoint key (Task 1c):** checkpoint upsert/lookup now keys
  on `(connection_id, stream_id, chunk_seq)` so K concurrent pods each
  write their own checkpoint row (no stomping). New 3-segment route
  `GET /internal/load-checkpoints/last/{connection_id}/{stream_id}/{chunk_seq}`;
  the legacy 2-segment route is kept for backward compat (returns chunk_seq=0).
- **Connection completion aggregation:** `upsert_load_checkpoint` now sets
  `connection.initial_load_completed = True` only when ALL K partitions
  report `state=done` (K read from `max(total_chunks)` across the stream's
  checkpoint rows), so the connection's overall initial load is marked
  complete only when every partition finishes.

### Initial Load — Performance (Tasks 4, 5, 7)
- **Adaptive chunk sizing (Task 4):** the worker auto-tunes the chunk size
  at runtime from observed per-chunk latency — doubled after
  `ADAPTIVE_FAST_STREAK` (5) consecutive chunks under
  `ADAPTIVE_FAST_LATENCY_S` (2s), halved after `ADAPTIVE_SLOW_STREAK` (2)
  consecutive chunks over `ADAPTIVE_SLOW_LATENCY_S` (30s), bounded by
  `[ADAPTIVE_MIN_CHUNK=1000, ADAPTIVE_MAX_CHUNK=100000]`. The producer's
  configured `chunk_size` is the starting point (capped at MAX, never
  clamped UP to MIN) so an operator who sets a small chunk_size gets it.
  All tunable via env vars.
- **Fetch/write overlap (Task 5):** a background thread prefetches chunk N+1
  from the source DB while the main thread converts + writes chunk N, hiding
  read latency behind write latency. Bounded queue (`PIPELINE_QUEUE_SIZE=2`)
  keeps memory bounded to ~2 chunks.
- **Commit batching (Task 7):** for Iceberg destinations, `N` chunks
  (`INITIAL_LOAD_COMMIT_BATCH`, default 1 = legacy) are buffered into a
  single `table.append` (one commit), reducing the commit count and the
  manifest-accumulation cost. The final partial batch is always flushed.
  Concurrent Iceberg writes from sibling pods are safe (PyIceberg catalog
  commit is an optimistic CAS with retry).

### UI (Task 3)
- **Max parallel workers field:** added a "Max parallel workers
  (initial-load intra-table parallelism, 1-16)" input to the
  CreateConnectionWizard and EditConnectionPage, mapped to
  `resource_limits.parallelism`. Default 4.

### Tests
- `control-plane/tests/test_partitioning.py` — covers `naive_numeric_ranges`,
  `ranges_from_splits` (disjoint-cover invariant), and `clamp_parallelism`.
- `transform-worker/tests/test_initial_load_checkpoint.py` — covers the
  composite-key wire format (`_report_checkpoint` sends `chunk_seq` +
  `total_chunks`; `_get_last_checkpoint` hits the 3-segment URL).

### Deferred (documented in REPORT_v126.md)
- **Task 6 (native DuckDB bulk export):** researched; not implemented this
  release (risk to correctness outweighs the win for the current scale).
- **Task 7 items 3-4 (disable Iceberg snapshot-inheritance checks,
  fast-append):** not exposed by PyIceberg 0.7.1; deferred to the PyIceberg
  upgrade (tracked separately — do NOT upgrade in this release).

### Version
- `control-plane/app/main.py` API version + `helm/fusion-cdc/Chart.yaml`
  bumped to `1.2.26`.

## [1.2.25] — 2026-07-23

### Reliability
- **Bug 2.1 (checkpoint persistence):** `transform-worker/loader.py` was
  calling `/internal/load-checkpoints` (404) instead of
  `/api/v1/internal/load-checkpoints`, leaving `initial_load_checkpoints`
  empty and causing duplicate rows on worker restart. Fixed the URL prefix
  in `_get_last_checkpoint` and `_report_checkpoint`, and made
  `_report_checkpoint` re-raise on HTTP failure so the worker retry/dead-letter
  path handles it instead of silently swallowing the error. Resume now starts
  from `last_pk + 1`, preventing duplicate rows.
- **Bug 2.2 (sync_frequency → schedule_cron):** `POST /connections/{id}/resume`,
  `POST /{id}/schedule`, `GET /{id}/schedule`, and `PATCH /{id}` referenced
  `connection.sync_frequency` which does not exist on the ORM (the column is
  `schedule_cron`), causing 500s and silent no-ops. All four call sites now
  use `connection.schedule_cron`; the public Pydantic field `sync_frequency`
  is preserved via a `model_validator(mode="before")` that maps
  `schedule_cron` → `sync_frequency` on responses.
- **Bug 2.3 (progress reporting):** `GET /connections/{id}/initial-load` now
  aggregates from `initial_load_checkpoints` and exposes `last_updated_at`
  (per-table + connection-level) plus `chunk_seq`/`last_pk`/`current_chunk`/
  `total_chunks` so the UI can show real progress and detect stuck loads.

### Optimization
- **Manifest compaction (Task 5):** `IcebergWriter.compact_manifests()` is now
  invoked every `INITIAL_LOAD_COMPACTION_INTERVAL` chunks (default 50) during
  long initial loads. `commit.manifest.min-count-to-merge=1` is set as a
  default for initial-load destinations to auto-merge manifests on every
  commit, flattening the ~30% throughput degradation curve.
- **Retry backoff + dead-letter (Task 6):** `transform-worker/worker.py` now
  applies exponential backoff (1, 2, 4, 8, 16, 32, 60s cap) between retries,
  caps at `MAX_TASK_RETRIES` (default 10), and moves exhausted tasks to the
  `fusion:transforms:dead-letter` Redis list. New endpoints surface and
  requeue dead-lettered tasks: `GET /connections/{id}/tasks/dead-letter`,
  `POST /tasks/dead-letter/{task_id}/requeue`, and the dead-letter count is
  reported in `/api/v1/monitoring/health`.
- **Delete-after-commit (Task 7):** `write.metadata.delete-after-commit.enabled=true`
  is now the default for initial-load destinations (operators can opt out via
  `write_metadata_delete_after_commit=false`), reducing metadata accumulation.

### Infrastructure
- **Remove Kafka (Task 1):** Kafka was unused dead infrastructure — CDC uses
  Redis Streams (XADD/XREADGROUP) and KEDA scales on Redis list depth. Removed
  `KAFKA_BOOTSTRAP_SERVERS` from `config.py`, `requirements.txt`
  (`kafka-python`), `monitoring.py` (`_check_kafka`), and all Kubernetes
  manifests (`kafka.yaml`, `kustomization.yaml`, `cdc-consumer.yaml`,
  `configmap.yaml`, local `resources.yaml` patch). Frees ~480Mi.

### Schema
- Alembic migration `c5d6e7f8a9b0` adds `last_updated_at` to
  `initial_load_checkpoints`.

## [1.2.24] — 2026-07-23

**CI fix for v1.2.23.** The v1.2.23 `test` job failed because
`transform-worker/loader.py` does `import redis` at module level and the
CI test image did not have `redis` installed.

### Fixed
- `.github/workflows/publish-images.yml` `test` job now also installs
  `redis==5.0.4` before running the transform-worker suite. The full
  transform-worker test dep set is now: `pyarrow`, `duckdb`, `requests`,
  `pymysql`, `psycopg2-binary`, `redis` — all already in
  `transform-worker/requirements.txt`.

### Changed
- `control-plane/app/main.py` FastAPI `version` → `1.2.24`.
- `helm/fusion-cdc/Chart.yaml` `version` / `appVersion` → `1.2.24`.

## [1.2.23] — 2026-07-23

**CI fix for v1.2.22.** The v1.2.22 `test` CI job failed because the
transform-worker unit tests import `engine.py` (which does
`import requests` at module level) and patch `pymysql.connect` /
`psycopg2.connect` (which requires those modules to be importable), but
the CI test image only had `pytest` + `pyarrow` + `duckdb` installed.

### Fixed
- `.github/workflows/publish-images.yml` `test` job now also installs
  `requests==2.31.0`, `pymysql==1.1.1`, `psycopg2-binary==2.9.9` before
  running the transform-worker suite. All 31 transform-worker tests pass
  locally with this exact dep set.

### Changed
- `control-plane/app/main.py` FastAPI `version` → `1.2.23`.
- `helm/fusion-cdc/Chart.yaml` `version` / `appVersion` → `1.2.23`.

## [1.2.22] — 2026-07-23

**Critical fix release.** Two confirmed blocking bugs in the transform-worker
(Iceberg destination path) plus a compute-efficiency regression that blocked
the source DB during the 118M-row MySQL → Iceberg load.

### Fixed
- **Bug A (all-NULL columns → `pa.null()` → PyIceberg rejects).**
  `transform-worker/iceberg_writer.py` `_rows_to_arrow()` now accepts an
  explicit `pa.Schema` and uses it for `pa.Table.from_pylist(rows, schema=...)`
  so all-NULL columns keep their declared type (e.g. `pa.string()`) instead
  of being inferred as `pa.null()` (which PyIceberg rejects with
  `ValueError: Cannot write DataType null`). New `_get_source_schema()`
  fetches the source table's column types ONCE from `information_schema`
  (MySQL/Postgres) or by sampling one document (Mongo) and caches the result
  for the entire stream — no per-chunk type inference.
- **Bug B (DuckDB `$1` binding fails for `list[dict]`).**
  `transform-worker/engine.py` `execute_pipeline()` no longer binds the
  Python row list via `conn.execute("CREATE TABLE staging AS SELECT * FROM
  $1", [rows])` (which raised `duckdb.InvalidInputException: Unsupported
  parameter type for binding $1`). Rows are now converted to a PyArrow
  Table with the explicit source schema, registered as a view, and
  materialised into `staging` via `CREATE TABLE staging AS SELECT * FROM
  rows_view`. The transformed schema is captured from DuckDB's staging
  table via `fetch_arrow_table().schema` so all-NULL columns keep their
  type through the round-trip.
- **3 additional step-handler bugs found during testing (Fix B2):**
  - `_apply_date_op`: `year(col)`/`month(col)`/etc. now cast the input to
    `TIMESTAMP` first — DuckDB's date functions do not accept `VARCHAR`
    (raised `Binder Error: No function matches 'year(VARCHAR)'`).
  - `_apply_json_flatten_child`: replaced
    `unnest(from_json(parent.col, '[]'))` (raised `Binder Error: Too many
    values in array of JSON structure`) with
    `unnest(CAST(json_extract(parent.col, '$') AS JSON[]))` and unquotes
    string elements via `json_extract_string`.
  - `_apply_mask` `hash` strategy: `sha256(col::BLOB)` → `sha256(col::VARCHAR)`
    (DuckDB's `sha256` takes `VARCHAR`, not `BLOB`).
  - `_apply_udf`: `duckdb.create_function(...)` → `conn.create_function(...)`
    (the module-level helper returns a function object that is never
    attached to the in-memory connection, so the subsequent `UPDATE` raised
    `Table Function "fn_name" not found`).

### Changed (compute efficiency — Fix C)
- `transform-worker/loader.py` `InitialLoadTask.run` fetches the source
  schema ONCE per stream (not per chunk) and passes it to
  `engine.execute_pipeline` and `IcebergWriter.write_batch` on every chunk.
  The transformed schema is captured from the first chunk and reused.
- `_fetch_pg_chunk` now uses `BEGIN READ ONLY` + `COMMIT` with
  `conn.autocommit = True` so the chunk SELECT does not hold a transaction
  open across the destination write (Fix C3 — source DB no longer locked).
- `_fetch_mysql_chunk` now connects with `autocommit=True` (Fix C3).
- Each chunk's memory is released (`del rows, transformed, child_tables`)
  before fetching the next (Fix C4 — stream, don't accumulate).

### Added
- `transform-worker/tests/test_iceberg_writer.py` — Bug A + type mapping +
  schema drift (15 tests).
- `transform-worker/tests/test_engine.py` — Bug B + all 10 step handlers
  (13 tests).
- `transform-worker/tests/test_compute_efficiency.py` — Fix C1/C3
  (schema fetched once, READ ONLY transactions) (3 tests).
- `.github/workflows/publish-images.yml` `test` job now installs
  `pyarrow==16.0.0` + `duckdb==0.10.3` and runs the transform-worker suite.

### Changed
- `control-plane/app/main.py` FastAPI `version` → `1.2.22`.
- `helm/fusion-cdc/Chart.yaml` `version` / `appVersion` → `1.2.22`.

## [1.2.21] — 2026-07-23

**CI fix for v1.2.20.** The v1.2.20 `test` CI job failed because
`cdc_consumer._decrypt()` does `from cryptography.fernet import Fernet` at
call time, and `cryptography` is not installed in the bare CI test image
(only `pytest` is). The 3 `test_do_initial_load_postgres_*` tests that
exercise `_do_initial_load_postgres` hit this import and failed with
`ModuleNotFoundError: No module named 'cryptography'`. (Locally the tests
passed because `cryptography` was installed.)

### Fixed
- **`cdc-workers/tests/test_initial_load_postgres.py`** `_install_stubs()`
  now also installs a lightweight `cryptography.fernet.Fernet` stub so the
  import inside `_decrypt` succeeds in the bare CI environment. The tests
  never exercise real decryption (`src_pw_enc=""` short-circuits at
  `if not ciphertext: return ""` before `Fernet()` is instantiated), so the
  stub is safe. Verified by running the suite with `cryptography` blocked
  via a meta-path finder — all 6 tests pass.

### Changed
- `control-plane/app/main.py` FastAPI `version` → `1.2.21`.
- `helm/fusion-cdc/Chart.yaml` `version` / `appVersion` → `1.2.21`.

## [1.2.20] — 2026-07-23

**Bulletproof connection lifecycle for every source × destination combo.**
This release closes the architectural gaps that left 4 of the 6
source × destination combinations silently broken (Iceberg destinations,
Postgres sources). The routing decision is now centralized in a single
helper (`_dest_needs_transform_worker`) that the producer, the CDC
stream consumer, and the transform-worker all consult, so they can
never disagree on who owns a connection.

### Added (Fix B — Postgres source initial load)
- **`cdc_consumer._do_initial_load_postgres`** (`cdc-workers/cdc_consumer.py`):
  new initial-load path for Postgres sources. Uses a psycopg2 **server-side
  cursor** (named cursor, `itersize=10000`) so multi-GB tables stream from
  the source without being materialised in memory — the Postgres equivalent
  of MySQL's `SSDictCursor`. Honours per-stream checkpointing, column
  mapping, selected-columns whitelist and transform overrides exactly like
  the MySQL/Mongo paths. Before this, a Postgres source fell through to
  `_do_initial_load_mysql` (wrong driver, wrong SQL) and crashed at connect
  time.
- **`_do_initial_load` router** now dispatches on `postgres`/`postgresql`
  to the new Postgres loader.

### Fixed (Fix C — route CDC streaming by destination type)
- **Double-write bug for Postgres destinations.** Before this release the
  `cdc-worker` published every CDC event to BOTH the `cdc:*` Redis streams
  (consumed by `cdc_consumer.py`) AND the `fusion:transforms:normal` list
  (consumed by `transform-worker`). For Postgres destinations both
  consumers wrote the same row → duplicates. The
  `control-plane/app/api/internal.py::get_transform_route` resolver now
  skips connections whose destination is Postgres with
  `snapshot_mode=inline` (cdc_consumer.py owns those), so the
  transform-worker only receives events for Iceberg / MySQL / Mongo
  destinations and Postgres-`transform_worker` destinations.
- **Silent no-op for Iceberg destinations with default `snapshot_mode`.**
  Before this release an Iceberg destination with the default
  `snapshot_mode=inline` was handed to `cdc_consumer.py`, which can only
  write to Postgres — the load silently failed. `control-plane/app/api/
  connections.py::_enqueue_initial_load_tasks` now enqueues initial-load
  tasks for Iceberg / MySQL / Mongo destinations **regardless of
  `snapshot_mode`**, so the transform-worker always owns non-Postgres
  destinations.
- **`cdc_consumer.py` skips connections owned by the transform-worker.**
  `cdc_consumer.py` now consults `_dest_needs_transform_worker` at
  startup and in the new-connection poller, and skips any connection
  whose destination is Iceberg / MySQL / Mongo or Postgres with
  `snapshot_mode=transform_worker`. This prevents the consumer from
  trying to connect to an Iceberg catalog as if it were a Postgres host
  (which logged a confusing "Cannot connect to destination" error on
  every poll cycle).

### Added (Fix D — connection lifecycle ordering)
- The connection-create → initial-load → CDC ordering was already
  enforced by `_trigger_dag_or_worker` calling
  `_enqueue_initial_load_tasks` after publishing the start-streaming
  command. This release adds a **contract test**
  (`tests/integration/test_connection_lifecycle.py`) that pins the
  ordering so a future refactor cannot silently drop the
  `_enqueue_initial_load_tasks` call and leave Iceberg destinations
  empty.

### Tests (regression net)
- `control-plane/tests/test_connections/test_routing_v120.py` — 37 unit
  tests pinning the `_dest_needs_transform_worker` rule across all three
  call sites (cdc_consumer, connections, internal) and every
  source × destination combination.
- `cdc-workers/tests/test_initial_load_postgres.py` — 6 unit tests for
  the new `_do_initial_load_postgres` (router dispatch, server-side
  cursor streaming, checkpoint skip, no-streams short-circuit).
- `tests/integration/test_connection_lifecycle.py` — 19 contract tests
  asserting every (source, dest) combination has a capable consumer,
  the initial-load path is wired, the CDC streaming path is wired, and
  the lifecycle ordering is enforced.
- `.github/workflows/publish-images.yml` — new `test` job runs all three
  test suites before the `publish` job, so images cannot ship if any
  routing/contract test fails.

### Changed
- `control-plane/app/main.py` FastAPI `version` → `1.2.20`.
- `helm/fusion-cdc/Chart.yaml` `version` / `appVersion` → `1.2.20`.

### Architecture (chosen flow)
```
connection create (POST /connections, status=active)
  └─ _trigger_dag_or_worker
       ├─ publish start-streaming → cdc-worker (Redis pub/sub + HTTP)
       └─ _enqueue_initial_load_tasks
            └─ if dest needs transform-worker (Iceberg/MySQL/Mongo, or
                Postgres+transform_worker): LPUSH initial_load task →
                fusion:transforms:high → transform-worker InitialLoadTask
            └─ else (Postgres+inline): no task; cdc_consumer.py owns the
                snapshot on its next poll

CDC streaming (continuous):
  cdc-worker publishes event →
    ├─ XADD cdc:{bank}:{tenant}:{source}:{schema}:{table}  (Redis stream)
    └─ TransformBridge.publish_event
         └─ GET /internal/workers/{id}/transform-route/...
              └─ returns [] for Postgres-inline (cdc_consumer owns it)
              └─ returns route for Iceberg/MySQL/Mongo/PG-transform_worker
                   → LPUSH cdc_transform task → fusion:transforms:normal
                   → transform-worker CDCTransformTask

Consumers:
  cdc_consumer.py        → reads cdc:* streams → writes to Postgres ONLY
                          (skips Iceberg/MySQL/Mongo/PG-transform_worker)
  transform-worker       → reads fusion:transforms:* lists → writes to
                          Postgres / MySQL / Mongo / Iceberg
```

## [1.2.19] — 2026-07-23

CRITICAL FIX: restore `cdc_consumer.py` (wrongly deleted in v1.2.18).
`cdc_consumer.py` is the CDC consumer (destination side of the pipeline)
deployed via `kubernetes/base/cdc-consumer.yaml` with
`command: ["python", "cdc_consumer.py"]`. It is **NOT** orphaned — the
v1.2.18 deletion was a regression. This release reverts that deletion and
restores the original `inline` snapshot-mode default, while keeping every
other v1.2.18 fix.

### Fixed (P0 regression)
- **`cdc-workers/cdc_consumer.py` restored** from v1.2.17 (commit `929f16a`).
  The file is 2051 lines, syntax-checked, and reads `METADATA_DB_DSN` from
  env (per the `kubernetes/base/cdc-consumer.yaml` secret injection). The
  `kubernetes/base/cdc-consumer.yaml` manifest
  (`command: ["python", "cdc_consumer.py"]`) is once again valid.
- **`snapshot_mode` default reverted to `inline`** in
  `control-plane/app/api/connections.py` (`_enqueue_initial_load_tasks`).
  The v1.2.18 change that made `transform_worker` the default and treated
  `inline` as deprecated has been reverted. Both modes are valid:
  - `inline` (default) = `cdc_consumer.py` performs the initial load
    (the original, production path via `kubernetes/base/cdc-consumer.yaml`).
  - `transform_worker` (opt-in) = `transform-worker/loader.py:InitialLoadTask`
    performs the initial load (the new path, useful for Iceberg/lake
    destinations where DuckDB/PyIceberg is needed).
  The deprecation warning that fired when `snapshot_mode=inline` was
  selected has been removed; `inline` is the canonical default again.
- **`snapshot_mode` UI labels corrected** in
  `frontend/src/components/iceberg/IcebergDestinationForm.tsx` and
  `frontend/src/lib/iceberg-config.ts`: `inline` is now labeled
  "Inline (cdc_consumer — default)" and `transform_worker` is labeled
  "Transform Worker (for Iceberg/lake)". The previous "deprecated — does
  nothing" / "recommended" labeling has been removed. The form default is
  `inline`.

### Kept from v1.2.18 (unrelated to the deletion, all correct)
- `POST /connections/{id}/retry-initial-load` API + UI button (Issue 2).
- Chart `podSecurityContext` for transformWorker (Issue 3a).
- `METADATA_DB_DSN` → `DATABASE_URL` rename in `transform-worker/worker.py`
  (Issue 3b). `cdc_consumer.py` is **intentionally left** reading
  `METADATA_DB_DSN` — it is deployed via `kubernetes/base/cdc-consumer.yaml`
  which injects `METADATA_DB_DSN`.
- LimitRange max memory 1Gi → 2Gi (Issue 4).
- `snapshot_mode` UI field on the destination form (Issue 5) — labels
  corrected as above.

### Honest acknowledgment
The v1.2.18 release notes claimed `cdc_consumer.py` was "orphaned dead code"
because the chart's `Dockerfile.cdc-worker` runs `python -m cdc_worker.worker`
and never imports it. That was wrong: `cdc_consumer.py` is a standalone
script deployed via its own manifest (`kubernetes/base/cdc-consumer.yaml`),
not via `Dockerfile.cdc-worker`. It is the destination side of the CDC
pipeline. The deletion broke the inline snapshot path and invalidated the
`cdc-consumer.yaml` manifest. Apologies for the regression; the v1.2.19
release restores the file and the original `inline` default while keeping
the unrelated v1.2.18 fixes.

## [1.2.18] — 2026-07-23

Follow-up to v1.2.17 (which fixed the `fetchall()` OOM regression in the
transform-worker). v1.2.18 fixes the chart + UX issues found in the user
investigation of v1.2.16 that prevented the transform-worker from starting
at all and blocked the user from recovering a failed initial load without
deleting + recreating the connection.

### Removed
- **`cdc-workers/cdc_consumer.py` deleted.** It was orphaned dead code:
  the chart's `Dockerfile.cdc-worker` runs `python -m cdc_worker.worker`,
  which never imported `cdc_consumer.py`. The connector classes
  (`connectors/mysql.py`, etc.) only implement `stream_events()` (binlog
  tailing) — no snapshot method — so the `inline` snapshot_mode (the
  previous default) did nothing. The transform-worker is now the canonical
  snapshot path.

### Changed
- **Default `snapshot_mode` is now `transform_worker`** in
  `control-plane/app/api/connections.py` (`_enqueue_initial_load_tasks`).
  Previously the default was `inline`, which was a no-op (cdc_consumer.py
  was never invoked). If a destination's `connection_config.snapshot_mode`
  is explicitly set to `inline`, a warning is logged and the producer falls
  back to `transform_worker` so the snapshot still runs.
- **`METADATA_DB_DSN` renamed to `DATABASE_URL`** in
  `transform-worker/worker.py` to match the env var the public Helm chart
  already injects via the `fusion-cdc-secrets` Secret. The previous
  mismatch broke the transform-worker on the public chart
  (`CreateContainerConfigError` / missing env var) unless the operator
  manually applied `patch-cdc-worker-metadata-dsn.json`. `METADATA_DB_DSN`
  is still accepted as a fallback for older deployments.
- Bumped `control-plane/app/main.py` FastAPI `version` → `1.2.18`.
- Bumped private `helm/fusion-cdc/Chart.yaml` → `version: 1.2.18` /
  `appVersion: "1.2.18"`.

### Added
- **`POST /api/v1/connections/{id}/retry-initial-load`** endpoint in
  `control-plane/app/api/connections.py`. Resets
  `initial_load_completed = false` and `initial_load_started_at = now()`,
  then re-invokes `_enqueue_initial_load_tasks`. Only valid for
  CDC/REALTIME connections (BATCH/SCHEDULED use Airflow). Returns
  `{ "ok": true, "tasks_enqueued": n }`.
- **`snapshot_mode` select field** in the Iceberg destination form
  (`frontend/src/components/iceberg/IcebergDestinationForm.tsx`) and the
  `IcebergDestinationConfig` type
  (`frontend/src/lib/iceberg-config.ts`). Options: `transform_worker`
  (default, label "Transform Worker (recommended)") and `inline` (label
  "Inline (deprecated — does nothing)"). The form sends `snapshot_mode`
  inside `connection_config` when creating/updating the destination.
- **"Retry Initial Load" button** on the connection detail page
  (`frontend/src/pages/connections/ConnectionDetailPage.tsx`). Calls the
  new endpoint and surfaces success/failure as an alert.

### Fixed
- The transform-worker now actually starts on the public Helm chart
  (combined with the public-chart `podSecurityContext` + `DATABASE_URL`
  fixes shipped in the public repo v1.2.18).

### Known gaps (honest)
- The private `kubernetes/base/cdc-consumer.yaml` manifest still references
  the deleted `cdc_consumer.py`. It is legacy (the public chart does not
  use it) and is left untouched in v1.2.18 to keep the diff surgical. It
  will be removed in a future cleanup release.
- `cdc_worker/worker.py` (the CDC worker, not transform-worker) does not
  read `METADATA_DB_DSN` or `DATABASE_URL` (it uses `CONTROL_PLANE_URL` +
  `WORKER_TOKEN` to fetch sources via the control-plane API), so no rename
  was needed there.

## [1.2.17] — 2026-07-23

Fixes the v1.2.16 transform-worker initial-load regression. The v1.2.16
`InitialLoadTask` fetched the **entire source table into memory** for all
three connector types (Postgres `SELECT * … fetchall()`, MySQL
`SELECT * … list(fetchall())`, MongoDB `list(find(no_cursor_timeout=True))`),
which OOM-killed the worker on any table larger than ~1 GB. The producer
also enqueued a single chunk per stream with `pk_start=None, pk_end=None`,
so there was no way to resume a partial load.

### Added
- **PK-bounded chunked initial load in the transform-worker.**
  `transform-worker/loader.py:InitialLoadTask.run` now loops over
  PK-bounded chunks of `chunk_size` rows (default 10000, configurable via
  `INITIAL_LOAD_CHUNK_SIZE` env var on the control-plane producer). Each
  connector has a dedicated chunked fetch:
  - Postgres `_fetch_pg_chunk`: `SELECT * FROM t WHERE pk > $last ORDER BY pk LIMIT $n`
  - MySQL `_fetch_mysql_chunk`: `SELECT * FROM t WHERE pk > $last ORDER BY pk LIMIT $n`
  - MongoDB `_fetch_mongo_chunk`: `find({_id: {$gt: last}}).sort({_id:1}).limit(n)`
  Memory is now bounded to one chunk (~a few MB) instead of the whole table.
- **Checkpoint resume.** New control-plane endpoint
  `GET /internal/load-checkpoints/last/{connection_id}/{stream_id}` returns
  the last checkpoint for a stream. `InitialLoadTask.run` calls it at start
  and resumes from `last_pk + 1` when `status != completed`. The existing
  `POST /internal/load-checkpoints` endpoint now persists `last_pk`,
  `chunk_seq`, and `current_chunk` after every chunk.
- **Alembic migration `b4c5d6e7f8a9`** adds four columns to
  `initial_load_checkpoints`: `chunk_seq`, `last_pk`, `total_chunks`,
  `current_chunk`. The model `InitialLoadCheckpoint` in
  `control-plane/app/models/monitoring.py` is updated to match.
- **Graceful drain.** `transform-worker/worker.py` now sets a module-level
  `STOP_EVENT` on SIGTERM/SIGINT; the chunk loop in `InitialLoadTask.run`
  checks it after each chunk and exits cleanly, leaving the checkpoint in
  `running` state so the next worker resumes from the last completed chunk.

### Fixed
- **OOM on large initial loads.** A 2 GB source table no longer
  materializes into a Python list of dicts in the transform-worker; it is
  streamed one chunk at a time. The inline `cdc_consumer.py` path already
  streamed via server-side cursors (SSDictCursor / Mongo cursor) and is
  unchanged — only the transform-worker regression is fixed here.

### Known gaps (honest)
- `total_chunks` is left `NULL` because the producer does not pre-count
  the source table (a `COUNT(*)` on a 2 GB table is expensive and would
  block the producer). Progress is reported as `chunk_seq` /
  `current_chunk` only.
- The inline `cdc_consumer.py` path streams but does **not** PK-chunk and
  does **not** resume mid-table — a crash mid-load re-TRUNCATEs and
  re-does the whole table. This is a pre-existing limitation, not a
  regression; the transform-worker path now does better.
- If the transform-worker pod is force-killed (SIGKILL, not SIGTERM) the
  in-flight chunk's rows are lost and the checkpoint is not advanced; the
  next run resumes from the last **completed** chunk, re-doing the
  in-flight chunk. Idempotent because the destination uses COPY into a
  TRUNCATEd table per stream (no duplicates).

## [1.2.16] — 2026-07-23

Closes the three remaining gaps from v1.2.14. The transform-worker
`InitialLoadTask` was correct but idle (no producer enqueued `initial_load`
tasks), users could not create MySQL/MongoDB destinations (no connector
definitions), and the task still called two non-existent control-plane
endpoints (`/internal/data-proxy/fetch` and `/internal/load-checkpoints`).

### Added
- **Initial-load producer (Gap 1).** New
  `control-plane/app/api/connections.py:_enqueue_initial_load_tasks` builds
  one `initial_load` task per enabled stream and LPUSHes it to
  `fusion:transforms:high` when the destination's `connection_config` sets
  `snapshot_mode: transform_worker`. Default mode is `inline` — the existing
  `cdc_consumer.py` snapshot path remains canonical and untouched. The
  producer is dispatched from `_trigger_dag_or_worker` (so both `activate` and
  `trigger-sync` pick it up) and is a no-op when mode is `inline`.
  `cdc-workers/cdc_consumer.py` now respects `snapshot_mode=transform_worker`
  in both the startup and poller paths, skipping the inline load to avoid a
  double snapshot.
- **MySQL & MongoDB destination connector definitions (Gap 2).** Added
  `MySQL Destination` (connector_type=`mysql`, category=`destination`,
  default port 3306, ssl_mode) and `MongoDB Destination` (connector_type=
  `mongodb`, category=`destination`, default port 27017, auth_source,
  replica_set) to `control-plane/app/seed/seed-admin.sql`. Mirrors the
  PostgreSQL Destination structure. Idempotent via `ON CONFLICT (connector_name)
  DO UPDATE`. Users can now create MySQL/MongoDB destinations (the v1.2.14
  DSN builders `_mysql_dsn_from_dest` / `_mongo_dsn_from_dest` now have
  matching connector defs).
- **`POST /internal/load-checkpoints` endpoint (Gap 3).** New control-plane
  endpoint in `control-plane/app/api/internal.py:upsert_load_checkpoint`
  upserts into `initial_load_checkpoints` keyed by (connection_id, stream_id).
  Replaces the non-existent endpoint the worker 404'd on.

### Fixed
- **`InitialLoadTask._fetch_rows` (Gap 3).** Rewrote
  `transform-worker/loader.py:InitialLoadTask._fetch_rows` to connect to the
  source DB directly using the `source` block in the task payload (psycopg2
  for Postgres, pymysql for MySQL, pymongo for MongoDB) instead of proxying
  through the non-existent `/internal/data-proxy/fetch` endpoint. Added
  `pymysql==1.1.1` and `pymongo==4.7.3` to `transform-worker/requirements.txt`.
- **`InitialLoadTask._mark_chunk_done` (Gap 3).** Now passes `stream_id` +
  `source_table` so the new `/internal/load-checkpoints` endpoint can upsert
  by (connection_id, stream_id). Failures are logged but never raise.

### Changed
- Bumped `control-plane/app/main.py` FastAPI `version` to `1.2.16`.
- Bumped `helm/fusion-cdc/Chart.yaml` to `version: 1.2.16` / `appVersion: "1.2.16"`.

## [1.2.15] — 2026-07-23

Fixes the Iceberg write path so the validate-write endpoint AND the real
CDC write path actually commit rows to an Iceberg table backed by
Nessie/REST + MinIO/S3. Two bugs broke writes end-to-end; a frontend
warehouse-hint tweak prevents operators from mis-configuring the warehouse
field for Nessie/REST catalogs.

### Fixed
- **`ImportError: cannot import name 'TableNotFound' from 'pyiceberg.exceptions'`
  in the Iceberg validate-write path.**
  `control-plane/app/utils/iceberg_tester.py:327` imported `TableNotFound`,
  which does not exist in pyiceberg 0.7.1 (only `NoSuchTableError` is defined
  there). The import raised `ImportError` before the test-connection /
  write-permission check could run, so every Iceberg destination validation
  500'd. Dropped `TableNotFound` from the import; the `except` clause now
  catches only `NoSuchTableError` (the only exception actually raised by
  `catalog.load_table` for a missing table in 0.7.1).
- **`ModuleNotFoundError: No module named 's3fs'` on `table.append()` /
  `table.upsert()` / `table.delete()` for Nessie/REST + MinIO/S3.**
  pyiceberg 0.7.1 resolves to the fsspec-based S3 FileIO
  (`pyiceberg.io.fsspec.FsspecFileIO`) for Nessie/REST/Hive catalogs backed
  by S3 / MinIO, and fsspec's S3 implementation lives in the separate
  `s3fs` package — which was not pinned in either requirements file. The
  very first write therefore failed for BOTH the control-plane
  validate-write endpoint (`iceberg_tester.py`) AND the real CDC write path
  (`transform-worker/iceberg_writer.py:IcebergWriter._apply`). Added
  `s3fs==2024.6.1` (latest stable; compatible with
  `fsspec==2024.6.1.*` and pyiceberg 0.7.1's `fsspec>=2023.1.0` pin) to both
  `control-plane/requirements.txt` and `transform-worker/requirements.txt`.

### Changed
- **Catalog-type-aware warehouse hint in the Create Destination form.**
  For `nessie` / `rest` catalogs the `warehouse` field is the
  Nessie-registered warehouse NAME (e.g. `iceberg-warehouse`), NOT an S3
  path — Nessie resolves the name to the physical S3 location. The previous
  placeholder (`s3://iceberg-warehouse/fusion-cdc/`) was wrong for these
  catalog types and caused Nessie `load_catalog` to fail with
  "warehouse not found". Added `warehouseHint(catalogType)` to
  `frontend/src/lib/iceberg-config.ts` and wired
  `IcebergDestinationForm.tsx` to use it for the Warehouse field's
  placeholder + help text. `hive` / `glue` / `sql` / `dynamodb` keep the
  S3-path placeholder.
- Bumped `control-plane/app/main.py` FastAPI `version` to `1.2.15`.
- Bumped `helm/fusion-cdc/Chart.yaml` to `version: 1.2.15` / `appVersion: "1.2.15"`.

## [1.2.14] — 2026-07-23

Closes the two real gaps the v1.2.13 audit flagged after shipping. The
initial-load (snapshot) path was broken — the transform-worker
`InitialLoadTask` called a non-existent control-plane endpoint to fetch the
destination DSN, so every initial-load chunk 404'd and the snapshot never
completed. This is *more* critical than the CDC path because initial load is
the first thing that happens when a connection is created — without it, CDC
never starts. The second gap: the v1.2.13 `dest_dsn` derivation only handled
Postgres, so MySQL/MongoDB destinations could not be routed.

### Fixed
- **Initial-load `dest_dsn` is now derived from the task payload (Gap 1).**
  `transform-worker/loader.py:InitialLoadTask._get_dest_dsn` previously called
  `GET /internal/connections/{id}/dest-dsn`, an endpoint that does not exist
  on the control-plane internal router (mounted at `/api/v1/internal/*` —
  see `control-plane/app/main.py:505`). Every initial-load chunk therefore
  raised `requests.exceptions.HTTPError` on the 404 and the snapshot failed.
  The fix mirrors the v1.2.13 CDC pattern: the destination block (with the
  decrypted plaintext `password` in `connection_config`) is already part of
  the task payload, so `InitialLoadTask.run` now derives the DSN locally via
  the new `_dest_dsn_from_dest` dispatcher and the dead `_get_dest_dsn`
  method has been removed. If the destination block is missing/incomplete or
  the connector_type is unsupported, the chunk logs an error and drops the
  rows instead of crashing or silently no-op'ing.
- **MySQL and MongoDB DSN builders added (Gap 2).** The v1.2.13 fix only
  handled Postgres (`_pg_dsn_from_dest`). New module-level builders
  `_mysql_dsn_from_dest` (`mysql+pymysql://{user}:{password}@{host}:{port}/{database}`)
  and `_mongo_dsn_from_dest` (`mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin`,
  mirroring the existing source-side Mongo URI in `cdc_consumer.py`) have been
  added, plus a dispatcher `_dest_dsn_from_dest(dest)` that branches on
  `dest.connector_type`. `CDCTransformTask.run` and `InitialLoadTask.run`
  both call the dispatcher instead of the Postgres-only helper. Unknown
  connector types return `""` so the batch is logged + dropped. Note: MySQL
  and MongoDB destination connector definitions are not seeded in
  `seed-admin.sql` today (they are source-only connectors); the builders are
  in place for when those destination definitions are added.

### Added
- `.tmp/v114-verify/verify_v114.py` — live E2E verification script the
  operator runs after deploying v1.2.14. Extends the v1.2.13 script with:
  (a) DSN-builder unit assertions for Postgres / MySQL / MongoDB, including
  the unknown-type log+drop path; (b) a Postgres initial-load (snapshot)
  E2E that creates a fresh connection, triggers sync, and polls the
  initial-load status endpoint until the snapshot is reported complete
  (the critical Gap 1 regression check).

### Changed
- Bumped `control-plane/app/main.py` FastAPI `version` to `1.2.14`.
- Bumped `helm/fusion-cdc/Chart.yaml` to `version: 1.2.14` / `appVersion: "1.2.14"`.

## [1.2.13] — 2026-07-23

Closes the three honest gaps the v1.2.12 worker audit flagged. No new
features — only the wiring needed to make Postgres-bound CDC actually
sync and the frontend wizard actually block on write-permission failure.

### Fixed
- **Postgres-bound CDC now derives `dest_dsn` from the destination block.**
  The `cdc_transform` task payload produced by
  `cdc_worker/transform_bridge.py` included the destination block but not
  `dest_dsn`, and `transform-worker/loader.py:CDCTransformTask.run` read
  `task.get("dest_dsn", "")` — so for any Postgres destination the upsert
  branch was silently skipped (`elif dest_dsn:` was false) and CDC events
  were dropped. Two fixes:
  - `control-plane/app/api/internal.py:get_transform_route` now decrypts the
    destination's `password_encrypted` and includes the plaintext `password`
    in the `connection_config` copy sent to the transform-worker (the stored
    row still keeps only `password_encrypted`).
  - `transform-worker/loader.py:CDCTransformTask.run` now derives
    `dest_dsn = "postgresql://{user}:{password}@{host}:{port}/{database}"`
    from the destination block when `dest_dsn` is not explicitly set on the
    task, via the new `_pg_dsn_from_dest` helper. If the destination block is
    missing/incomplete it logs an error and drops the batch instead of
    silently no-op'ing.
- **Frontend Create Destination wizard now calls validate-write.** The
  backend `POST /destinations/{id}/validate-write-permissions` endpoint
  existed since v1.2.12 but the wizard's "Finish Setup" button proceeded
  without ever calling it. The wizard now auto-triggers validate-write
  after a successful test-connection, shows a "Validating write permissions..."
  loading state, disables "Finish Setup" until validate-write returns
  `ok: true`, and renders a clear error (via `formatApiDetail` from
  `lib/api-errors.ts`) if it fails (`frontend/src/pages/destinations/
  CreateDestinationWizard.tsx`).

### Added
- `.tmp/v113-verify/verify_v113.py` — live E2E verification script the
  operator runs after deploying v1.2.13. Logs in as admin, finds the seeded
  Iceberg destination, asserts test-connection returns `ok: true` with
  populated `checks`, asserts validate-write returns `ok: true`, creates a
  Postgres source + Postgres destination + connection, triggers sync, and
  documents the manual `kubectl exec ... psql -c 'INSERT ...'` step plus
  the destination-side `SELECT` to confirm the row landed.

### Changed
- Bumped `control-plane/app/main.py` FastAPI `version` to `1.2.13`.
- Bumped `helm/fusion-cdc/Chart.yaml` to `version: 1.2.13` / `appVersion: "1.2.13"`.

## [1.2.12] — 2026-07-23

Real Iceberg Test Connection, real Iceberg write-permission check, and a
CDC → transform-worker bridge so CDC events actually reach the destination.

### Fixed
- **Iceberg Test Connection is now real.** The control-plane
  `POST /destinations/{id}/test-connection` endpoint previously had no
  Iceberg branch — Iceberg destinations fell through to a generic
  `socket.create_connection((host, port))` on an empty host, so the test
  never validated the catalog, the S3 warehouse, or the credentials. Added
  `control-plane/app/utils/iceberg_tester.py` (mirroring the catalog factory
  from `transform-worker/iceberg_writer.py`) which loads the PyIceberg
  catalog, lists namespaces, and runs `HeadBucket` on the warehouse bucket.
  The endpoint now returns a structured `checks` array the frontend renders
  as a checklist (`destinations.py`, `schemas/destination.py`).
- **Iceberg write-permission check is now real.** The
  `POST /destinations/{id}/validate-write-permissions` endpoint had no
  Iceberg branch — it returned `has_write_permissions: true` without doing
  anything. The new tester creates a throwaway `__dcraft_test` namespace +
  `__write_check` table, appends one row, deletes it, then drops both,
  returning `{can_create_table, can_insert, can_delete}`.
- **CDC now bridges into the transform-worker queue.** The cdc-worker
  published CDC events to Redis Streams (`cdc:*` via XADD) but the
  transform-worker only reads Redis lists (`fusion:transforms:*` via BRPOP) —
  different data structures / key namespaces, so CDC events were never
  consumed and CDC never synced. Added `cdc_worker/transform_bridge.py`
  which resolves `(source, schema, table) → Connection/Destination/Stream`
  via a new control-plane internal endpoint
  `GET /api/v1/internal/workers/{id}/transform-route/...` and LPUSHes a
  `cdc_transform` task to `fusion:transforms:normal` for each event. The
  original XADD to `cdc:*` is kept for metrics/observability.
- **Seeded Iceberg `auth_mode: "static"` now resolves.** The seeded MinIO
  Iceberg destination uses `auth_mode: "static"` with `s3_access_key_id` /
  `s3_secret_access_key`, which the writer's `_resolve_credentials`
  rejected. Both `transform-worker/iceberg_writer.py` and the new
  `control-plane/app/utils/iceberg_tester.py` now treat `static` like
  `access_key` and read the `s3_*` keys.

### Changed
- Bumped FastAPI app `version="1.2.11"` → `"1.2.12"`
  (`control-plane/app/main.py`).
- Bumped `fusion-cdc` Helm chart to `version: 1.2.12` / `appVersion: "1.2.12"`
  (`helm/fusion-cdc/Chart.yaml`).
- Added `pyiceberg[glue,s3]==0.7.1`, `pyarrow==16.0.0`, `boto3==1.34.101`
  to `control-plane/requirements.txt` so the control-plane can run a real
  Iceberg Test Connection / write check in-process.

## [1.2.11] — 2026-07-22

Coordinated re-tag with the public `dcraft-fusion` v1.2.11. The v1.2.10 tag in
the public repo shipped corrupted Helm `values.yaml` files (UTF-8 mojibake
from a PowerShell re-encode pass) which failed `helm lint` in the `Publish
Helm charts` workflow. v1.2.11 re-tags the CDC images to `1.2.11` so the
public chart's image references resolve to a green, lint-passing release.

### Changed
- Bumped FastAPI app `version="1.2.10"` → `"1.2.11"`
  (`control-plane/app/main.py`).
- Bumped `fusion-cdc` Helm chart to `version: 1.2.11` / `appVersion: "1.2.11"`
  (`helm/fusion-cdc/Chart.yaml`).

No semantic code changes vs v1.2.10 — all seven blocker fixes from v1.2.10
are unchanged.

## [1.2.10] — 2026-07-22

CDC runtime + local-dev infrastructure repair release. v1.2.9 verified the UI
was live and solid, but the E2E CDC audit found the runtime non-functional:
workers were not assigned to connections, MongoDB connections 404'd, the
seeded Iceberg destination had an empty config, and Postgres schema discovery
returned 0 tables. v1.2.10 fixes all seven blockers.

### Fixed
- **CDC worker assignment (BLOCKER 1)** — `trigger-sync` no longer fails with
  "CDC worker is not running" when a live worker is heartbeating. The
  `_check_worker_reachable()` helper now consults the `worker_heartbeats`
  table (any heartbeat within the last 90s counts as alive) before falling
  back to the HTTP `/health` probe, which was unreachable across pods
  (`control-plane/app/api/connections.py`).
- **MongoDB connections 404 (BLOCKER 2)** — `POST /connections` and
  `POST /sources/{id}/discover` no longer 404 for MongoDB sources. The
  tenant filter on the source/destination lookup is now bypassed for
  superusers, so the seeded admin (whose `sub_tenant_id` is NULL) can wire
  up sources created under any tenant context
  (`control-plane/app/api/connections.py`, `control-plane/app/api/sources.py`).
- **Seeded Iceberg destination `config:{}` (BLOCKER 3)** — the
  `Local Iceberg (MinIO + Nessie)` destination now ships with a populated
  `connection_config` (catalog_uri, warehouse, s3_endpoint, credentials).
  A new `ensure_iceberg_destination_config()` startup hook repairs
  clusters upgraded from v1.2.9 on every boot
  (`control-plane/app/seed/seed-admin.sql`, `control-plane/app/seed/seed_admin.py`,
  `control-plane/app/main.py`).
- **Postgres `discover` returns 0 tables (BLOCKER 4)** — `_discover_postgres`
  now lists tables from `pg_publication_tables` when a `publication` is
  configured in the source `config`, so the seeded pg source with
  `fusion_pub` returns `users` + `orders`. Falls back to the broad
  `information_schema.tables` scan when no publication is configured
  (`control-plane/app/api/sources.py`).
- **UI forms missing CDC fields (BLOCKER 5)** — the source wizard now exposes
  `replication_slot` (default `cdc_slot`) and `publication` (default
  `fusion_pub`) for Postgres, and `server_id` (random 1–4294967295) for MySQL
  (`frontend/src/pages/sources/CreateSourceWizard.tsx`).
- **Wrong port defaults (BLOCKER 6)** — source wizard now defaults pg → 5432,
  mongo → 27017, mysql → 3306 (previously everything defaulted to 3306).
  PostgreSQL destination wizard default port corrected from 5433 → 5432
  (`frontend/src/pages/sources/CreateSourceWizard.tsx`,
  `frontend/src/pages/destinations/CreateDestinationWizard.tsx`).

### Added
- **Local-dev infra pods (BLOCKER 7)** — `infra/local-dev/k8s/00-infra.yaml`
  now deploys `mysql-source` (MySQL 8.0 with binlog + self-seeding init SQL),
  `mongo-source` (MongoDB 7, no auth), `minio` (S3-compatible API + console
  + PVC + bucket-init Job), and `nessie` (Iceberg REST catalog). All with
  startup/readiness probes and small (128Mi–256Mi) resource requests.
  LOCAL-DEV ONLY — production must use managed equivalents.

### Changed
- Bumped FastAPI app `version="1.2.9"` → `"1.2.10"`
  (`control-plane/app/main.py`).
- Bumped `fusion-cdc` Helm chart to `version: 1.2.10` / `appVersion: "1.2.10"`
  (`helm/fusion-cdc/Chart.yaml`).

## [1.2.9] — 2026-07-22

UI polish pass on top of the verified-stable v1.2.8 backend. The v1.2.8 UX
audit found that superadmin users only saw 2 of 9 settings cards (BLOCKER
— the frontend read `user.role` but `/auth/me` returned `roles[]` +
`is_superuser` with no `role` field), the connectors page showed
"Used by: 0" for every connector, and a localhost GraphQL link broke when
accessed from a remote browser. This release fixes all three plus the
emilkowalski button `:active` scale and keyboard-accessible dropdowns.

### Fixed
- **Superadmin role mapping (BLOCKER)**
  (`control-plane/app/api/auth.py:489-510`, `control-plane/app/schemas/auth.py:259-264`):
  `/auth/me` now returns a computed `role: string` field
  (`"superadmin" if is_superuser else (roles[0] if roles else "viewer")`)
  so the frontend `SettingsPage` role gate renders all 9 cards for
  superadmins. The `CurrentUserResponse` schema gained an optional `role`
  field. Frontend `auth-store.ts` `login()` now fetches `/auth/me` after
  storing the token (the previous code set `user: data.user` which was
  always `undefined` since `TokenResponse` has no `user` field), and
  `MainLayout` calls `loadUser()` on mount when authenticated but `user`
  is null (handles page refresh).
- **Connectors "Used by" count**
  (`control-plane/app/api/connector_definitions.py:82-119`,
  `control-plane/app/schemas/connector.py:60-71`): the
  `/connector-definitions` list endpoint now computes `usage_count`
  (sources + destinations, excluding soft-deleted) per connector in two
  grouped queries and attaches it to each `ConnectorDefinitionResponse`.
  The frontend already read `conn.usage_count ?? 0` — it was always 0
  because the field was absent.
- **localhost GraphQL link**
  (`frontend/src/pages/graphql/GraphQLPage.tsx:9`,
  `frontend/src/pages/monitoring/MonitoringPage.tsx:112`): replaced
  `http://localhost:30800/graphql` with the relative `/api/v1/graphql`
  so the "Open GraphiQL UI" link works behind the nginx proxy from any
  browser, not just localhost.

### Changed
- **Button `:active` scale** (`frontend/src/components/ui/button.tsx:6`):
  added `active:scale-[0.97]` to the `buttonVariants` base class. The
  CDC button already used `transition-colors` (not `transition-all`), so
  no transition-property change needed.
- **Keyboard-accessible dropdowns**
  (`frontend/src/components/ui/select.tsx`): the Radix Select wrapper
  already forwards props to the primitives (keyboard works by default),
  but the trigger only had `focus:` (mouse) ring styles and items only
  had `focus:bg-accent`. Added `focus-visible:ring-2` to the trigger and
  `data-[highlighted]:bg-accent data-[highlighted]:text-accent-foreground`
  to `SelectItem` so keyboard-navigated items are visually distinct.
- Bumped FastAPI app `version="1.2.8"` → `"1.2.9"`
  (`control-plane/app/main.py:349`).
- Bumped `fusion-cdc` Helm chart to `version: 1.2.9` / `appVersion: "1.2.9"`
  (`helm/fusion-cdc/Chart.yaml`).

### Notes
- No database migrations in this release.
- Coordinated with the public `dcraft-fusion` v1.2.9 release which
  re-tags all images to `1.2.9` and ships the Fusion-side UI polish
  (workspace nav active state, Audit Center timestamps, Recent Runs
  semantic colors, button active scale).

## [1.2.8] — 2026-07-22

Follow-up to v1.2.7. The v1.2.7 live stability audit found that
`/api/v1/alerts/suppressions` still returned HTTP 500 with an empty body
even after the v1.2.5 `f7a8b9c0d1e2` migration shipped, and
`/api/v1/data-quality/templates` returned HTTP 501 from a stub handler.
This release completes the `alert_suppressions` migration and replaces the
templates stub with an empty 200 response.

### Fixed
- **`alert_suppressions` remaining columns**
  (`migrations/versions/a8b9c0d1e2f3_add_alert_suppressions_remaining_columns.py`):
  the v1.2.5 migration `f7a8b9c0d1e2` only added the `rule_ids` /
  `connection_ids` ARRAY columns (and dropped the legacy single-valued
  `rule_id` / `connection_id`). But the `AlertSuppression` model
  (`control-plane/app/models/alerting.py:309-345`) declares three
  additional columns that NO migration ever created:
  `is_recurring` (Boolean, NOT NULL, default false),
  `recurrence_pattern` (JSONB, nullable), and `updated_by` (UUID,
  nullable). Without these, every POST/GET to
  `/api/v1/alerts/suppressions` raised
  `psycopg2.errors.UndefinedColumn: column alert_suppressions.is_recurring
  does not exist` (HTTP 500). This migration adds the three missing
  columns with types/defaults that exactly match the model declarations.
  Alembic chain: `f7a8b9c0d1e2` → `a8b9c0d1e2f3`.
- **`/api/v1/data-quality/templates` 501 → 200**
  (`control-plane/app/api/data_quality.py:192`): the GET handler was a
  stub that raised `HTTPException(501, "Rule templates not yet
  implemented")`. Replaced with an empty `RuleTemplateListResponse`
  (`templates=[], total=0, page, page_size, total_pages=0`) so the
  endpoint answers 200. The API contract stays intact for when
  `RuleTemplate` rows are eventually seeded. (The POST
  `/templates` create handler is left as 501 — it is out of scope for
  this release and not exercised by the live audit.)

### Changed
- Bumped FastAPI app version `1.2.7` → `1.2.8`
  (`control-plane/app/main.py:349`).
- Bumped private `fusion-cdc` Helm chart to `version: 1.2.8` /
  `appVersion: "1.2.8"` (`helm/fusion-cdc/Chart.yaml`).

### Notes
- pg-source discovery returning 0 tables is operational (the seeded
  source ships with no discovered streams). Already documented in
  `docs/POST_DEPLOY_CHECKLIST.md` §2 — no code change.

## [1.2.7] — 2026-07-22

Coordinated release with the public `dcraft-fusion` v1.2.7. The v1.2.6
CDC `Publish CDC images` workflow already succeeded; this is a
version-bump-only release to keep the CDC image tags and the private
`fusion-cdc` Helm chart aligned with the public repo's v1.2.7 chart
(which fixes the public CI `npm audit` gate via a Vite override).

### Changed
- Bumped FastAPI app version `1.2.6` → `1.2.7`
  (`control-plane/app/main.py:349`).
- Bumped private `fusion-cdc` Helm chart to `version: 1.2.7` /
  `appVersion: "1.2.7"` (`helm/fusion-cdc/Chart.yaml`).

### Notes
- The CI fix (Vite override for `npm audit`) lives in the public
  `dcraft-fusion` repo and ships in its v1.2.7 release.

## [1.2.6] — 2026-07-22

Coordinated release with the public `dcraft-fusion` v1.2.6. The CDC
`Publish CDC images` workflow was already green for v1.2.4/v1.2.5, so
this is a version-bump-only release to keep the CDC control-plane /
frontend / worker image tags and the private `fusion-cdc` Helm chart
aligned with the public repo's v1.2.6 chart.

### Changed
- Bumped FastAPI app version `1.2.5` → `1.2.6`
  (`control-plane/app/main.py:349`).
- Bumped private `fusion-cdc` Helm chart to `version: 1.2.6` /
  `appVersion: "1.2.6"` (`helm/fusion-cdc/Chart.yaml`).

### Notes
- The CI fixes (auth.ts UUID type, superadmin overview test) live in the
  public `dcraft-fusion` repo and ship in its v1.2.6 release.

## [1.2.5] — 2026-07-22

Follow-up to v1.2.4. The v1.2.4 live verification + codebase map audit
found that the v1.2.4 `alert_rules` migration was incomplete (only added
`scope_id`; `threshold_value` + `consecutive_failures_required` +
`cooldown_minutes` were still missing → HTTP 500 on
`/api/v1/alerts/rules`), the `alert_suppressions` table had a
`rule_id`/`rule_ids` schema mismatch (HTTP 500 on
`/api/v1/alerts/suppressions`), and several security fail-fast gaps
remained. This release completes the migrations, aligns the models with
the real DB schema, and adds production fail-fast for default secrets.

### Fixed (alerting migrations — live verification)
- **`alert_rules` remaining columns**
  (`migrations/versions/e6f7a8b9c0d1_add_alert_rules_remaining_columns.py`):
  the v1.2.4 migration `d5e6f7a8b9c0` only added `scope_id`. This migration
  adds the three columns declared by the `AlertRule` model that the
  original `2512af1df83a` migration never created:
    - `threshold_value` (Numeric, nullable=True)
    - `consecutive_failures_required` (Integer, NOT NULL, server_default 1)
    - `cooldown_minutes` (Integer, NOT NULL, server_default 15)
  Without these, every POST/GET to `/api/v1/alerts/rules` raised
  `psycopg2.errors.UndefinedColumn` (HTTP 500).
- **`alert_suppressions` rule_ids/connection_ids arrays**
  (`migrations/versions/f7a8b9c0d1e2_align_alert_suppressions_rule_ids.py`):
  the model and API schemas declare `rule_ids` / `connection_ids` as
  `ARRAY(UUID)` (a suppression can target multiple rules/connections),
  but the original migration created single-valued `rule_id` /
  `connection_id`. This migration adds the array columns, back-fills them
  from the legacy single-valued columns, and drops the legacy columns +
  indexes. Fixes HTTP 500 on `/api/v1/alerts/suppressions`.

### Fixed (models vs. real DB schema — codebase map audit)
- **`system_alerts` model mismatch** (`control-plane/app/models/system.py`):
  removed `TimestampMixin` from the `Alert` model. The real `alerts`
  table (per `schemas/schema_postgres.sql`) does NOT have `created_at` /
  `updated_at` columns, so every INSERT/SELECT against it raised
  `psycopg2.errors.UndefinedColumn`. Deleted the dead duplicate
  `control-plane/app/models/system_alert.py` (no callers imported it).
- **`dq_policies` router dedup** (`control-plane/app/api/`): removed the
  duplicate `dq_policies.py` router. It mirrored `data_quality.py`'s
  policy endpoints but was never registered in `main.py` and not called
  by the frontend. The canonical router is `data_quality.py` (mounted at
  `/api/v1/data-quality`).

### Fixed (security fail-fast — codebase map audit)
- **JWT_SECRET_KEY** (`control-plane/app/config.py`): in any non-dev
  `APP_ENV`, raises `RuntimeError` at startup if `JWT_SECRET_KEY` is
  unset or still equals the default public string. Prevents anyone with
  the source from minting valid tokens.
- **ENCRYPTION_KEY** (`control-plane/app/config.py`): same fail-fast —
  credentials at rest must not be decryptable by anyone with the source.
- **WORKER_SHARED_SECRET** (`control-plane/app/config.py`): in non-dev
  `APP_ENV`, raises `RuntimeError` if `WORKER_SHARED_SECRET` is empty.
  Previously empty=disabled meant any pod could call `/internal/heartbeat`,
  `/internal/checkpoint`, `/internal/event-failed`.

### Fixed (observability — codebase map audit)
- **Seed health surface** (`control-plane/app/api/monitoring.py`,
  `control-plane/app/seed/seed_admin.py`): the `/api/v1/monitoring/health`
  endpoint now includes a `seed` field in the `services` object
  (`{"services":{"database":"healthy","redis":"healthy","kafka":"healthy",
  "seed":"applied"}}`). The seed module tracks the last auto-seed outcome
  in a module-level variable (`applied` / `not_applied` / `skipped` /
  `not_run`), so operators can detect a failed self-healing seed without
  scraping logs.

### Changed
- Bumped FastAPI app `version="1.2.5"` in `control-plane/app/main.py`.
- Bumped `fusion-cdc` Helm chart to `version: 1.2.5` / `appVersion: "1.2.5"`
  (`helm/fusion-cdc/Chart.yaml`).

### Notes
- The Fusion-SPA-side fixes (Dockerfile rebuild, CI freshness check,
  kernel context scoping, HostPath PV cross-platform) live in the public
  `dcraft-fusion` repo and ship in its v1.2.5 release.

## [1.2.4] — 2026-07-22

Follow-up to v1.2.3. The v1.2.3 verification + Fusion UI audit (HTTP-only,
192.168.1.10:8088) found 7 issues. This release fixes the 3 CDC-side issues
(2 MEDIUM, 1 LOW). The 4 Fusion-SPA-side fixes live in the public
`dcraft-fusion` repo (crypto polyfill, JWT header injection, Test
Connection UI, demo-data banner).

### Fixed
- **`/api/v1/alerts/rules` returned 500 — `alert_rules.scope_id` column
  missing (MEDIUM).** The `AlertRule` model
  (`control-plane/app/models/alerting.py`) declares
  `scope_id = Column(UUID(as_uuid=True), nullable=True)`, but the original
  `2512af1df83a_add_alerting_tables` migration never created this column.
  Every POST/GET to `/alerts/rules` raised
  `psycopg2.errors.UndefinedColumn: column alert_rules.scope_id does not
  exist`. Added migration `d5e6f7a8b9c0_add_alert_rules_scope_id` (revises
  `c4d5e6f7a8b9`) that adds the nullable UUID column plus an index on it,
  mirroring the `c4d5e6f7a8b9_add_dq_policies_deleted_at` pattern.
- **`/alerts/statistics|dashboard|suppressions` returned 422 — route
  shadowed by `/alerts/{alert_id}` (MEDIUM).** FastAPI matches routes in
  declaration order; the GET `/{alert_id}` route (with `alert_id: UUID`)
  was declared before the static `/statistics`, `/dashboard`, and
  `/suppressions` routes, so requests to those static paths matched
  `/{alert_id}` first and 422'd on UUID validation of `"statistics"` etc.
  Moved the `get_alert` GET `/{alert_id}` handler to the END of
  `control-plane/app/api/alerting.py` so all static sub-paths
  (`/channels`, `/rules`, `/suppressions`, `/statistics`, `/dashboard`)
  are registered first. The `/{alert_id}/acknowledge|resolve|history|
  notifications` sub-routes were left in place since they don't shadow any
  static path.
- **Dashboard "System Health" widget didn't surface Kafka (LOW).**
  `frontend/src/pages/dashboard/DashboardPage.tsx` only rendered
  PostgreSQL, Redis, and CDC Workers rows, even though
  `/api/v1/monitoring/health` already returns `services.kafka`. Added a
  Kafka row that reads `health?.services?.kafka` and renders the three
  possible states (`healthy` → green "OK", `unhealthy` → amber, and
  `not_configured` → slate "n/a" so an operator who hasn't wired Kafka
  doesn't see a false alarm).

### Changed
- Bumped FastAPI app `version="1.2.4"` in `control-plane/app/main.py`.
- Bumped `fusion-cdc` Helm chart to `version: 1.2.4` / `appVersion: "1.2.4"`
  (`helm/fusion-cdc/Chart.yaml`).

## [1.2.3] — 2026-07-21

Follow-up to v1.2.2. The v1.2.2 remote retest (192.168.1.10) confirmed the
self-healing seed worked end-to-end (6 connectors, 1 source, 2 destinations,
1 connection all present — the headline blocker is FIXED), but found 4
remaining issues. This release fixes the 3 code-side issues found in the
private repo; the Fusion-kernel-side chart-config fix lives in the public
`dcraft-fusion` repo.

### Fixed
- **`/settings/audit-logs` route rendered a blank page (LOW).** The v1.2.1
  commit removed the `AuditLogsPage` import and the `/settings/audit-logs`
  route from `frontend/src/App.tsx`, but the router had no catch-all — so
  direct navigation to `/settings/audit-logs` fell through to the
  `MainLayout` `<Outlet />` with no matching child route, rendering a blank
  page with just the "Settings > Audit logs" breadcrumb (computed from the
  URL by `TopBar.tsx`). Added an explicit
  `<Route path="settings/audit-logs" element={<Navigate to="/settings" replace />} />`
  so direct nav redirects to `/settings` instead of rendering blank.
- **CDC frontend static-text mojibake (LOW, cosmetic).** Static strings in
  `frontend/src/pages/destinations/CreateDestinationWizard.tsx` had
  double-encoded bytes (UTF-8 misinterpreted as Windows-1252 then re-encoded
  as UTF-8): the "Next" button read `Next â†'` (should be `Next →`), the
  connector capability bullets read `Â·` (should be `·`), the connector
  emoji icons read `ðŸ§Š` / `ðŸ"Š` / `ðŸŒ` (should be `🧊` / `📊` / `🔌`),
  the password placeholder read `â€¢â€¢â€¢â€¢â€¢â€¢` (should be `••••••`),
  and the SCD write-mode description read `wins â€" overwrites` (should be
  `wins — overwrites`). API-sourced text rendered correctly because it
  flows through JSON; only the frontend's own bundled strings were
  mis-encoded. Fixed by replacing the corrupted byte sequences with the
  correct UTF-8 characters in the source. Also added
  `ENV LANG=C.UTF-8 LC_ALL=C.UTF-8` to the `docker/Dockerfile.frontend`
  build stage so Vite/esbuild reads source files as UTF-8 regardless of
  the host locale (defensive against future re-encoding).
- **Kafka health not observable in CDC monitoring/health (LOW).**
  `GET /api/v1/monitoring/health` returned only `database` and `redis`
  services — Kafka was not listed as a health dependency, so API-level
  Kafka health was not observable. Added a Kafka health check to
  `control-plane/app/api/monitoring.py` that probes
  `KAFKA_BOOTSTRAP_SERVERS` (new setting on `control-plane/app/config.py`)
  via a lightweight TCP socket open with a 2s timeout. Reports
  `"kafka": "healthy"` (reachable), `"kafka": "unhealthy"` (configured but
  unreachable), or `"kafka": "not_configured"` (env var empty). The overall
  `status` downgrades to `"degraded"` only when Kafka is expected but down;
  `not_configured` does not penalize the overall status. The health
  endpoint never crashes if Kafka is down. Added `kafka-python==2.0.2` to
  `control-plane/requirements.txt` as a soft dependency (the health check
  falls back to a raw TCP socket probe when the library is unavailable).

### Added
- **`docs/POST_DEPLOY_CHECKLIST.md`.** Operators MUST run
  `POST /api/v1/sources/{id}/discover` after seeding before the Create
  Connection wizard's stream-level Iceberg partition editor can be
  exercised (the seeded `pg-source` ships with `discovery_cache: null`).
  This is operational, not a code bug — discovery requires the source DB
  to be reachable, so it cannot be auto-run in code.

### Changed
- **FastAPI `version="1.2.2"` → `version="1.2.3"`** in
  `control-plane/app/main.py` (so `/api/openapi.json` reports `1.2.3`).
- **`helm/fusion-cdc/Chart.yaml`:** `1.2.2` → `1.2.3` (both `version` and
  `appVersion`).

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

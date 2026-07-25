"""Connections API endpoints"""

import os
from datetime import datetime, timedelta
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import func, and_
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permission
from app.models.auth import AuditLog, User
from app.models.connection import Connection, Stream
from app.models.source_destination import Source, Destination
from app.models.connector import ConnectorDefinition
from app.models.monitoring import ConnectionRun, CheckpointState, InitialLoadCheckpoint
from app.services.audit_log import record_audit
from app.services.partitioning import partition_pk_ranges
from app.schemas.connection import (
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionResponse,
    ConnectionListResponse,
    ConnectionSearchFilters,
    ConnectionValidationRequest,
    ConnectionValidationResponse,
    ScheduleConfig,
    ScheduleConfigResponse,
    ConnectionActivateRequest,
    ConnectionActivateResponse,
    SyncTriggerRequest,
    SyncTriggerResponse,
    ConnectionStats,
    StreamCreate,
    StreamUpdate,
    StreamResponse,
)

router = APIRouter()


# ===========================
# v1.2.26: Multi-pod INTRA-table parallelism config
# ===========================
# K = number of disjoint PK-range partitions enqueued per stream for the
# initial load. KEDA then scales the transform-worker to ``maxReplicaCount``
# pods so the K ranges are consumed concurrently by different pods (true
# intra-table parallelism — see Task 1 in REPORT_v126.md). Configurable per
# connection via ``resource_limits.parallelism`` (1..MAX_PARALLELISM) and
# globally via the ``INITIAL_LOAD_DEFAULT_PARALLELISM`` env var.
MAX_PARALLELISM = 16
DEFAULT_PARALLELISM = max(1, min(MAX_PARALLELISM, int(os.environ.get(
    "INITIAL_LOAD_DEFAULT_PARALLELISM", "4"))))
# Tables above this row count use approximate-percentile PK sampling for
# even row distribution across the K partitions; smaller tables use a naive
# even split of the numeric [min,max] PK range (cheaper, fine when PKs are
# dense).
PARTITION_SAMPLE_THRESHOLD = 1_000_000
# Default chunk size (rows per chunk within a single partition's PK range).
DEFAULT_CHUNK_SIZE = int(os.environ.get("INITIAL_LOAD_CHUNK_SIZE", "10000"))

# When ``resource_limits.bulk_mode`` is ``"auto"`` (or unset — auto is the
# default so users aren't forced to guess), a partition whose estimated row
# count is at or above this threshold gets ``"duckdb"`` (the native-scanner
# bulk path — higher throughput, more scratch-disk/CPU overhead); smaller
# partitions get ``"python"`` (lower overhead, no DuckDB attach needed).
# Load-tested on a 35.86M-row MySQL table: duckdb ~97k rows/sec vs python
# ~55k rows/sec on the same table, so the crossover favors duckdb well below
# that scale — 1M rows is a conservative default. Override globally via
# AUTO_BULK_MODE_ROW_THRESHOLD; a connection can still force "duckdb" /
# "python" explicitly via resource_limits.bulk_mode to bypass this entirely.
AUTO_BULK_MODE_ROW_THRESHOLD = int(os.environ.get("AUTO_BULK_MODE_ROW_THRESHOLD", "1000000"))


def _resolve_bulk_mode(resource_limits: dict, rows_estimated, src_connector_type: str = "") -> str | None:
    """Resolve ``resource_limits.bulk_mode`` into the effective per-partition
    value sent to the worker. ``"duckdb"``/``"python"`` pass through as an
    explicit operator override. ``"auto"`` (or unset — auto is the default)
    picks based on ``rows_estimated`` for THIS partition against
    ``AUTO_BULK_MODE_ROW_THRESHOLD``. Returns ``None`` only when there's no
    estimate to decide on (falls back to the worker's own env-var default).

    MongoDB has no DuckDB scanner (the worker forces ``bulk_mode="none"``
    for it regardless — see loader.py), so auto-resolution never suggests
    "duckdb" for a mongo source; an explicit override still passes through
    unchanged (the worker's own guard is the final authority either way).
    """
    mode = str((resource_limits or {}).get("bulk_mode") or "auto").lower()
    if mode in ("duckdb", "python"):
        return mode
    if (src_connector_type or "").lower() in ("mongodb", "mongo"):
        return "python"
    if rows_estimated is None:
        return None
    try:
        return "duckdb" if int(rows_estimated) >= AUTO_BULK_MODE_ROW_THRESHOLD else "python"
    except (TypeError, ValueError):
        return None


# v1.3.9: CDC batching config (Redis Streams migration — see
# transform-worker/cdc_stream_consumer.py). A connection's CDC events can be
# applied to the destination one at a time ("per_event", the old behavior)
# or accumulated by the transform-worker's consumer-side read-loop into a
# single upsert-commit + single delete-commit per batch ("per_batch") —
# whichever of max_events/max_wait_minutes is hit first. Deliberately ONE
# combined threshold (not separate insert/update/delete thresholds): after
# PK-based compaction every batch only ever has 2 real buckets (upsert,
# delete), since INSERT/UPDATE both collapse to "upsert" for Iceberg.
DEFAULT_CDC_BATCH_MAX_EVENTS = int(os.environ.get("DEFAULT_CDC_BATCH_MAX_EVENTS", "500"))
DEFAULT_CDC_BATCH_MAX_WAIT_MINUTES = float(os.environ.get("DEFAULT_CDC_BATCH_MAX_WAIT_MINUTES", "1"))


def _resolve_cdc_batch_config(resource_limits: dict) -> dict:
    """Resolve ``resource_limits.cdc_batch_mode``/``cdc_batch_max_events``/
    ``cdc_batch_max_wait_minutes`` into the effective config the
    transform-worker's CDC stream consumer applies for this connection.
    ``"per_event"`` (unset default — matches the pre-v1.3.9 behavior) means
    no batching at all; ``"per_batch"`` accumulates via a read-loop up to
    ``max_events`` OR ``max_wait_minutes``, whichever comes first.
    """
    rl = resource_limits or {}
    mode = str(rl.get("cdc_batch_mode") or "per_event").lower()
    if mode not in ("per_event", "per_batch"):
        mode = "per_event"
    try:
        max_events = int(rl.get("cdc_batch_max_events") or DEFAULT_CDC_BATCH_MAX_EVENTS)
    except (TypeError, ValueError):
        max_events = DEFAULT_CDC_BATCH_MAX_EVENTS
    try:
        max_wait_minutes = float(rl.get("cdc_batch_max_wait_minutes") or DEFAULT_CDC_BATCH_MAX_WAIT_MINUTES)
    except (TypeError, ValueError):
        max_wait_minutes = DEFAULT_CDC_BATCH_MAX_WAIT_MINUTES
    return {
        "mode": mode,
        "max_events": max(1, max_events),
        "max_wait_minutes": max(0.05, max_wait_minutes),
    }

# v1.2.27: in-process initial-load partitioning state tracker. Used by the
# async ``retry-initial-load`` endpoint (returns 202 immediately) and the
# ``GET /connections/{id}/initial-load/status`` endpoint. With
# ``--workers 1`` (the production control-plane config) there is a single
# process so this dict is the authoritative state. With multiple workers,
# state is per-worker — the status endpoint returns the calling worker's
# view (acceptable for a P0; a future release can move this to a DB table).
_initial_load_state: dict = {}


def _get_initial_load_state(connection_id: str) -> dict:
    """Return (or create) the partitioning state dict for a connection."""
    s = _initial_load_state.get(connection_id)
    if s is None:
        s = {
            "phase": "idle",        # idle|partitioning|enqueued|running|completed|failed
            "task_id": None,
            "partitions": 0,        # K (number of ranges)
            "rows_estimated": None, # approximate row count from information_schema
            "error": None,
            "started_at": None,
            "updated_at": None,
        }
        _initial_load_state[connection_id] = s
    return s


def _set_initial_load_phase(connection_id: str, phase: str, **fields) -> None:
    """Update the partitioning state for a connection (timestamped)."""
    from datetime import datetime as _dt
    s = _get_initial_load_state(connection_id)
    s["phase"] = phase
    s["updated_at"] = _dt.utcnow().isoformat()
    for key, val in fields.items():
        s[key] = val
    if phase == "partitioning" and s.get("started_at") is None:
        s["started_at"] = s["updated_at"]


# ===========================
# Helper Functions
# ===========================

def _get_connection_by_id(
    db: Session,
    connection_id: UUID,
    user: User,
    include_relations: bool = True,
) -> Connection:
    """Get connection by ID with tenant filtering"""
    query = db.query(Connection).filter(
        Connection.connection_id == connection_id,
        Connection.sub_tenant_id == user.sub_tenant_id,
        Connection.is_deleted == False,
    )
    
    if include_relations:
        query = query.options(
            joinedload(Connection.source).joinedload(Source.connector_definition),
            joinedload(Connection.destination).joinedload(Destination.connector_definition),
            joinedload(Connection.streams),
        )
    
    connection = query.first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection {connection_id} not found",
        )
    
    return connection


def _validate_connection_compatibility(
    db: Session,
    source: Source,
    destination: Destination,
    sync_mode: str,
) -> tuple[bool, str, List[str]]:
    """
    Validate if source and destination are compatible
    Returns: (is_valid, message, issues_list)
    """
    issues = []
    
    # Check if source and destination exist and are active
    if source.status not in ["active", "draft"]:
        issues.append(f"Source '{source.source_name}' is not active (status: {source.status})")
    
    if destination.status not in ["active", "draft"]:
        issues.append(f"Destination '{destination.destination_name}' is not active (status: {destination.status})")
    
    # Validate sync mode support
    source_connector = source.connector_definition
    dest_connector = destination.connector_definition
    
    # Check CDC support
    if sync_mode == "cdc":
        source_capabilities = source_connector.capabilities or {}
        if not source_capabilities.get("supports_cdc", False):
            issues.append(f"Source connector '{source_connector.connector_name}' does not support CDC")
    
    # Check write mode compatibility
    dest_capabilities = dest_connector.capabilities or {}
    dest_write_modes = dest_capabilities.get("supported_write_modes", ["append"])
    
    if not dest_write_modes:
        issues.append(f"Destination connector '{dest_connector.connector_name}' has no configured write modes")
    
    # Validate connection test results
    if source.connection_test_status == "failed":
        issues.append(f"Source connection test failed: {source.connection_test_error}")
    
    if destination.connection_test_status == "failed":
        issues.append(f"Destination connection test failed: {destination.connection_test_error}")
    
    is_valid = len(issues) == 0
    message = "Connection is valid and compatible" if is_valid else "Connection validation failed"
    
    return is_valid, message, issues


def _calculate_next_sync_time(cron_expression: str) -> Optional[datetime]:
    """
    Calculate next sync time from cron expression
    Returns None for 'manual' frequency
    """
    if cron_expression == "manual":
        return None
    
    # TODO: Implement actual cron parsing with croniter library
    # For now, return a simple estimate
    # Common patterns:
    # */15 * * * * = every 15 minutes
    # 0 * * * * = every hour
    # 0 0 * * * = daily at midnight
    
    # Simple estimation for testing
    now = datetime.utcnow()
    if "*/15" in cron_expression:
        return now + timedelta(minutes=15)
    elif "*/30" in cron_expression:
        return now + timedelta(minutes=30)
    elif "0 *" in cron_expression:
        return now + timedelta(hours=1)
    else:
        return now + timedelta(days=1)


async def _trigger_dag_or_worker(connection: Connection, db: Session) -> None:
    """
    Spec §1 (P1-2/P1-3): Trigger an initial full load or manual sync.

    Sync type routing:
      BATCH / SCHEDULED → trigger Airflow DAG (runs BatchConsumer)
      CDC / REALTIME    → publish start-streaming command to cdc-worker via Redis
                          + optional HTTP POST to worker

    For manual trigger-sync, BATCH connections trigger an Airflow DAG run.
    For CDC/REALTIME connections, the worker is already streaming — trigger-sync
    re-fetches sources and starts any new ones.

    v1.2.27: now async — ``_enqueue_initial_load_tasks`` is awaited (it
    offloads partitioning to a threadpool). Failures are logged but must not
    abort the activate/trigger-sync response.
    """
    import os
    import json as _json
    import logging as _logging
    import httpx

    from app.config import settings

    log = _logging.getLogger(__name__)
    sync_type = (getattr(connection, "sync_type", "") or "").upper()
    connection_id = str(connection.connection_id)

    # Normalise sync_type: CDC → REALTIME, BATCH → SCHEDULED for routing
    is_batch = sync_type in ("SCHEDULED", "BATCH")

    if is_batch:
        # Trigger the Airflow DAG via REST API
        airflow_url = os.environ.get(
            "AIRFLOW_API_URL",
            getattr(settings, "AIRFLOW_API_URL", "http://localhost:8080"),
        )
        airflow_user = os.environ.get("AIRFLOW_USER", getattr(settings, "AIRFLOW_USER", "admin"))
        airflow_pass = os.environ.get("AIRFLOW_PASSWORD", getattr(settings, "AIRFLOW_PASSWORD", "admin"))
        dag_id = f"fusion_cdc_{connection_id.replace('-', '_')}"

        try:
            resp = httpx.post(
                f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns",
                json={
                    "conf": {
                        "connection_id": connection_id,
                        "sync_mode": connection.sync_mode,
                        "source_id": str(connection.source_id),
                        "destination_id": str(connection.destination_id),
                    },
                },
                auth=(airflow_user, airflow_pass),
                timeout=10.0,
            )
            log.info("Triggered Airflow DAG %s → HTTP %s", dag_id, resp.status_code)
            if resp.status_code >= 400:
                log.warning("Airflow DAG trigger response: %s", resp.text[:500])
        except Exception as exc:
            log.warning("Could not trigger Airflow DAG %s: %s", dag_id, exc)
    else:
        # CDC / REALTIME — notify the worker via Redis pub/sub AND HTTP
        worker_url = os.environ.get("WORKER_CONTROL_URL", getattr(settings, "WORKER_CONTROL_URL", ""))
        worker_token = os.environ.get("WORKER_SHARED_SECRET", getattr(settings, "WORKER_SHARED_SECRET", ""))

        # Build a rich command payload with source and connection context
        command_payload = _json.dumps({
            "action": "start-streaming",
            "connection_id": connection_id,
            "source_id": str(connection.source_id),
            "destination_id": str(connection.destination_id),
            "sync_mode": connection.sync_mode,
        })

        # Publish to Redis so the cdc-worker command listener picks it up
        try:
            import redis
            redis_url = os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
            r = redis.from_url(redis_url)
            r.publish("fusion:commands", command_payload)
            log.info("Published start-streaming command to Redis for connection=%s", connection_id)
        except Exception as exc:
            log.warning("Could not publish to Redis (connection=%s): %s", connection_id, exc)

        # Also attempt HTTP POST directly to the worker
        if not worker_url:
            # Default to the docker service name in dev
            worker_url = os.environ.get("CDC_WORKER_URL", "http://localhost:8081")

        try:
            resp = httpx.post(
                f"{worker_url}/internal/start-streaming",
                json={"connection_id": connection_id},
                headers={"X-Worker-Token": worker_token},
                timeout=5.0,
            )
            log.info("Notified worker to start streaming connection=%s HTTP %s", connection_id, resp.status_code)
        except Exception as exc:
            log.warning("Could not notify worker via HTTP (connection=%s): %s", connection_id, exc)

    # v1.2.16+: when the destination's snapshot_mode is "transform_worker",
    # enqueue initial_load tasks to fusion:transforms:high so the
    # transform-worker performs the snapshot instead of cdc_consumer.py.
    # No-op when mode is "inline" (the default — cdc_consumer.py owns the
    # snapshot). See Gap 1 in the v1.2.16 release notes.
    # v1.2.19: cdc_consumer.py is NOT orphaned — it is the inline snapshot
    # path deployed via kubernetes/base/cdc-consumer.yaml. The v1.2.18
    # deletion was a regression and has been reverted.
    try:
        await _enqueue_initial_load_tasks(connection, db)
    except Exception as exc:
        log.warning(
            "initial_load producer dispatch failed for connection=%s: %s",
            connection_id, exc, exc_info=True,
        )


def _check_worker_reachable(db: Optional[Session] = None) -> bool:
    """Determine whether at least one CDC worker is alive.

    The CDC workers run in separate pods and their HTTP /health endpoint is
    not exposed via a Service — the control plane cannot reach them via
    ``localhost:8081`` in a Kubernetes deployment.  The authoritative liveness
    signal is the ``worker_heartbeats`` table, which workers upsert every
    ``HEARTBEAT_INTERVAL`` seconds (default 30s).

    This function returns True if any worker has heartbeated within the last
    90 seconds.  If a ``db`` session is not supplied, the HTTP probe is used
    as a best-effort fallback (useful in unit tests / standalone scripts).
    """
    import os
    from datetime import datetime, timedelta

    if db is not None:
        try:
            from app.models.monitoring import WorkerHeartbeat
            cutoff = datetime.utcnow() - timedelta(seconds=90)
            recent = (
                db.query(WorkerHeartbeat)
                .filter(WorkerHeartbeat.last_heartbeat_at >= cutoff)
                .filter(WorkerHeartbeat.status.in_(["running", "idle", "healthy"]))
                .first()
            )
            if recent is not None:
                return True
        except Exception:
            pass

    # Fallback: HTTP probe (works when control-plane and worker share a pod
    # or when CDC_WORKER_URL is explicitly set to the worker Service).
    import httpx
    from app.config import settings
    worker_url = os.environ.get("CDC_WORKER_URL", os.environ.get("WORKER_CONTROL_URL", "http://localhost:8081"))
    try:
        resp = httpx.get(f"{worker_url}/health", timeout=6.0)
        return resp.status_code < 500
    except Exception:
        return False


def _delete_redis_cdc_keys(source_id: str) -> None:
    """
    Delete all Redis CDC stream keys belonging to a given source_id.
    Key format: cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}
    Scans for 'cdc:*:{source_id}:*' and deletes them all.
    """
    import os, logging as _logging
    from app.config import settings
    log = _logging.getLogger(__name__)
    try:
        import redis
        redis_url = os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
        r = redis.from_url(redis_url)
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"cdc:*:{source_id}:*", count=100)
            if keys:
                r.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        if deleted:
            log.info("Deleted %d Redis CDC stream key(s) for source_id=%s", deleted, source_id)
    except Exception as exc:
        log.warning("Could not clean Redis CDC keys for source_id=%s: %s", source_id, exc)


def _stop_worker_streaming(connection: Connection) -> None:
    """
    Publish a stop-streaming command to the cdc-worker via Redis pub/sub.
    Used when pausing a CDC/REALTIME connection.
    """
    import os
    import json as _json
    import logging as _logging

    from app.config import settings

    log = _logging.getLogger(__name__)
    sync_type = (getattr(connection, "sync_type", "") or "").upper()
    connection_id = str(connection.connection_id)

    if sync_type in ("SCHEDULED", "BATCH"):
        return  # Nothing to stop for batch — Airflow handles its own lifecycle

    try:
        import redis
        redis_url = os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
        r = redis.from_url(redis_url)
        r.publish("fusion:commands", _json.dumps({
            "action": "stop-streaming",
            "connection_id": connection_id,
            "source_id": str(connection.source_id),
        }))
        log.info("Published stop-streaming command to Redis for connection=%s", connection_id)
    except Exception as exc:
        log.warning("Could not publish stop-streaming to Redis (connection=%s): %s", connection_id, exc)


# ===========================
# Initial-load producer (transform-worker snapshot path)
# ===========================

def _dest_needs_transform_worker(dest_connector_type: str, snapshot_mode: str) -> bool:
    """v1.2.20: single source of truth for the CDC routing decision.

    Returns True when the transform-worker should own this connection's
    snapshot + CDC streaming; False when ``cdc_consumer.py`` owns it
    (Postgres destination with ``snapshot_mode=inline``).

    Mirrors ``cdc_consumer._dest_needs_transform_worker`` and
    ``internal._dest_needs_transform_worker`` so the producer, the CDC
    stream consumer, and the transform-worker all agree on who owns each
    connection. See the v1.2.20 release notes for the full
    source × destination matrix.
    """
    ctype = (dest_connector_type or "").lower()
    if ctype in ("iceberg", "mysql", "mongodb", "mongo"):
        return True
    if ctype in ("postgres", "postgresql"):
        return str(snapshot_mode or "inline").lower() == "transform_worker"
    return True


def _connection_parallelism(connection: Connection) -> int:
    """v1.2.26: resolve the per-connection intra-table parallelism (K).

    Reads ``connection.resource_limits["parallelism"]`` (set by the UI
    "Max parallel workers" field); falls back to the
    ``INITIAL_LOAD_DEFAULT_PARALLELISM`` env var (default 4). Clamped to
    [1, MAX_PARALLELISM]. K=1 means a single task per stream (legacy
    v1.2.25 behaviour) — no intra-table parallelism.
    """
    rl = connection.resource_limits or {}
    if isinstance(rl, str):
        try:
            import json as _json
            rl = _json.loads(rl) if rl else {}
        except Exception:
            rl = {}
    try:
        k = int(rl.get("parallelism") or DEFAULT_PARALLELISM)
    except (TypeError, ValueError):
        k = DEFAULT_PARALLELISM
    return max(1, min(MAX_PARALLELISM, k))


def _connection_chunk_size(connection: Connection) -> int:
    """v1.2.26 Task 4: per-connection chunk size override (rows per chunk).

    Reads ``connection.resource_limits["chunk_size"]``; falls back to the
    ``INITIAL_LOAD_CHUNK_SIZE`` env var (default 10000). The worker's
    adaptive chunk sizer may further adjust this at runtime.
    """
    rl = connection.resource_limits or {}
    if isinstance(rl, str):
        try:
            import json as _json
            rl = _json.loads(rl) if rl else {}
        except Exception:
            rl = {}
    try:
        cs = int(rl.get("chunk_size") or DEFAULT_CHUNK_SIZE)
    except (TypeError, ValueError):
        cs = DEFAULT_CHUNK_SIZE
    return max(1, cs)


async def _enqueue_initial_load_tasks(connection: Connection, db: Session) -> int:
    """Enqueue ``initial_load`` tasks to ``fusion:transforms:high`` when the
    connection's destination is configured with ``snapshot_mode=transform_worker``.

    The default snapshot mode is ``inline`` — the cdc_consumer.py process
    performs the full-table snapshot directly (see ``cdc_consumer._do_initial_load``).
    Setting ``snapshot_mode`` to ``transform_worker`` in the destination's
    ``connection_config`` JSONB opts a connection into the transform-worker
    snapshot path: this producer builds one ``initial_load`` task per enabled
    stream and LPUSHes it to the high-priority Redis list consumed by
    ``transform-worker/worker.py`` (``InitialLoadTask``).

    v1.2.19: ``inline`` is the canonical default again (reverting the v1.2.18
    change that wrongly treated cdc_consumer.py as orphaned). Both modes are
    valid: ``inline`` for the production cdc_consumer.py path, ``transform_worker``
    as an opt-in for Iceberg/lake destinations where DuckDB/PyIceberg is needed.

    The task payload includes:
      - ``source`` block (host/port/database/username + decrypted password) so
        the worker can fetch rows directly from the source DB (no data-proxy
        round-trip — see Gap 3 in the v1.2.16 release notes).
      - ``destination`` block (connector_type + connection_config with the
        decrypted plaintext password) so the worker can derive the dest DSN
        via ``_dest_dsn_from_dest`` (mirrors the CDC transform path).
      - ``transform_steps`` from the stream's ``transform_overrides.transforms``.
      - ``source_schema`` / ``source_table`` / ``dest_schema`` / ``dest_table``.

    Returns the number of tasks LPUSHed (0 when mode is inline or on error).
    Never raises — the inline path remains the canonical fallback.
    """
    import os
    import json as _json
    import logging as _logging

    from app.config import settings
    from app.models.connection import Stream
    from app.models.source_destination import Source, Destination
    from app.api.sources import _decrypt_password

    log = _logging.getLogger(__name__)
    try:
        dest = (
            db.query(Destination)
            .filter(Destination.destination_id == connection.destination_id)
            .first()
        )
        if not dest:
            return 0
        dest_config_raw = dest.connection_config or {}
        if isinstance(dest_config_raw, str):
            try:
                dest_config_raw = _json.loads(dest_config_raw)
            except Exception:
                dest_config_raw = {}
        dest_connector_type = "postgres"
        if dest.connector_definition:
            dest_connector_type = dest.connector_definition.connector_type
        snapshot_mode = str(dest_config_raw.get("snapshot_mode") or "inline").lower()
        # v1.2.20: route the initial load to the transform-worker whenever
        # the destination is NOT a Postgres-inline connection. Iceberg /
        # MySQL / Mongo destinations always go to the transform-worker
        # (cdc_consumer.py cannot write to them), and Postgres destinations
        # go to the transform-worker only when snapshot_mode=transform_worker.
        # This closes the gap where an Iceberg destination with the default
        # snapshot_mode=inline was wrongly handed to cdc_consumer.py (which
        # cannot write to Iceberg) and silently no-op'd.
        if not _dest_needs_transform_worker(dest_connector_type, snapshot_mode):
            return 0  # inline Postgres mode: cdc_consumer.py handles the snapshot

        source = (
            db.query(Source)
            .filter(Source.source_id == connection.source_id)
            .first()
        )
        if not source:
            log.warning(
                "initial_load producer: source %s not found for connection %s",
                connection.source_id, connection.connection_id,
            )
            return 0

        # Build destination block with decrypted plaintext password.
        dest_config = dict(dest_config_raw)
        enc = dest_config.get("password_encrypted")
        if enc and not dest_config.get("password"):
            try:
                dest_config["password"] = _decrypt_password(enc)
            except Exception:
                pass
        # dest_connector_type already resolved above (v1.2.20 routing check)
        dest_block = {
            "connector_type": dest_connector_type,
            "connection_config": dest_config,
        }

        # Build source block with decrypted plaintext password.
        src_pw = ""
        if source.password_encrypted:
            try:
                src_pw = _decrypt_password(source.password_encrypted)
            except Exception:
                pass
        src_connector_type = "postgres"
        if source.connector_definition:
            src_connector_type = source.connector_definition.connector_type
        source_block = {
            "connector_type": src_connector_type,
            "host": source.host,
            "port": source.port,
            "database_name": source.database_name,
            "username": source.username,
            "password": src_pw,
            "config": source.config or {},
            "ssh_config": source.ssh_config or {},
        }

        streams = (
            db.query(Stream)
            .filter(
                Stream.connection_id == connection.connection_id,
                Stream.is_enabled == True,  # noqa: E712
            )
            .all()
        )
        if not streams:
            log.warning(
                "initial_load producer: no enabled streams for connection %s",
                connection.connection_id,
            )
            return 0

        import redis as _redis
        redis_url = os.environ.get(
            "REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"),
        )
        r = _redis.from_url(redis_url)
        high_queue = os.environ.get("HIGH_PRIORITY_QUEUE", "fusion:transforms:high")

        pushed = 0
        # v1.2.26 Task 1: per-connection intra-table parallelism (K) and
        # chunk size. K partitions are enqueued per stream so KEDA can scale
        # the transform-worker to K concurrent pods, each consuming one
        # disjoint PK range of the same table (true intra-table parallelism).
        k = _connection_parallelism(connection)
        chunk_size = _connection_chunk_size(connection)
        _rl = connection.resource_limits or {}
        task_committer_mode = _rl.get("committer_mode")
        src_connector_type = (source.connector_definition.connector_type
                              if source.connector_definition else "postgres")
        # v1.2.30 Defect C fix: import the estimate-aware partitioner so each
        # task payload carries a density-based ``rows_estimated`` (computed
        # from the instant information_schema/pg_class count + MIN/MAX). The
        # worker stamps this on the FIRST checkpoint for the partition and
        # never overwrites it with rows_written, so progress_pct reflects
        # real progress instead of always reading 100%.
        from app.services.partitioning import partition_with_estimates
        for stream in streams:
            to = stream.transform_overrides or {}
            steps = to.get("transforms", []) if isinstance(to, dict) else []
            pk = stream.primary_keys
            if isinstance(pk, list):
                pk_str = ",".join(str(kc) for kc in pk) if pk else "id"
            elif isinstance(pk, dict):
                pk_str = ",".join(str(kc) for kc in pk.keys()) if pk else "id"
            else:
                pk_str = str(pk) if pk else "id"
            # First PK column drives the PK-bounded chunking (identity-style
            # composite PKs). MongoDB chunks on the immutable _id field.
            pk_col = str(pk_str).split(",")[0].strip() or "id"
            if src_connector_type == "mongodb":
                pk_col = "_id"
            # v1.2.26 Task 1a / v1.2.30 Defect C: partition the table's
            # [min(pk), max(pk)] range into K disjoint sub-ranges WITH a
            # density-based per-partition row estimate. Falls back to
            # [{pk_start:None, pk_end:None, rows_estimated:None}] (a single
            # unbounded range with no estimate) on error or when K<=1 —
            # preserving the legacy v1.2.25 single-task-per-stream behaviour.
            # v1.2.27 P0 fix: offload the DB-touching partition call to a
            # worker thread so the uvicorn event loop is NOT blocked while
            # MIN/MAX/information_schema queries run against the source DB.
            import asyncio as _asyncio
            parts = await _asyncio.to_thread(
                partition_with_estimates,
                source_block, stream.source_schema_name or "",
                stream.source_table_name, pk_col, src_connector_type, k,
            )

            # Dynamic committer provisioning: an Iceberg destination needs a
            # committer process draining staged files for THIS (connection,
            # table) pair. Ensure it exists (create-or-update, idempotent)
            # BEFORE enqueueing tasks that will stage files for it — no
            # manual helm values.committer.targets edit + `helm upgrade`
            # required. Never blocks/raises on failure (see
            # committer_provisioner.ensure_committer docstring); tasks still
            # enqueue even if provisioning couldn't run, matching the
            # existing "producer never raises" contract for this function.
            if dest_connector_type == "iceberg":
                dest_table_name = stream.destination_table_name or stream.source_table_name
                stream_rows_estimated = sum(
                    (p.get("rows_estimated") or 0) for p in parts
                ) or None
                try:
                    from app.services.committer_provisioner import ensure_committer
                    ensure_committer(
                        connection_id=str(connection.connection_id),
                        table=dest_table_name,
                        catalog_config=dest_config,
                        resource_limits=_rl,
                        k=k,
                        rows_estimated_total=stream_rows_estimated,
                        dest_namespace=stream.stream_namespace or dest_config.get("namespace") or "fusion",
                    )
                except Exception:
                    log.exception(
                        "initial_load producer: committer auto-provisioning "
                        "failed for connection=%s table=%s — continuing to "
                        "enqueue tasks regardless",
                        connection.connection_id, dest_table_name,
                    )

            for seq, part in enumerate(parts):
                pk_start = part.get("pk_start")
                pk_end = part.get("pk_end")
                rows_estimated = part.get("rows_estimated")
                task_bulk_mode = _resolve_bulk_mode(_rl, rows_estimated, src_connector_type)
                task = {
                    "type": "initial_load",
                    "task_id": f"il-{connection.connection_id}-{stream.stream_id}-{seq}",
                    "connection_id": str(connection.connection_id),
                    "stream_id": str(stream.stream_id),
                    # v1.2.26: composite checkpoint key (connection_id,
                    # stream_id, chunk_seq) — each of the K ranges checkpoints
                    # under its own chunk_seq so concurrent pods do not stomp
                    # the same row.
                    "chunk_seq": seq,
                    "pk_start": pk_start,
                    "pk_end": pk_end,
                    # v1.2.26: total number of ranges for this stream — the
                    # control-plane uses this to decide when ALL ranges are
                    # completed and the connection's initial load is done.
                    "total_chunks": len(parts),
                    # v1.2.30 Defect C fix: density-based per-partition row
                    # estimate (table_rows * span / total_span), stamped at
                    # ENQUEUE time so the worker can stamp it on the first
                    # checkpoint and compute a real progress_pct. None when
                    # the partitioner fell back to K=1 (unknown estimate).
                    "rows_estimated": rows_estimated,
                    # v1.2.17: PK-bounded chunk size (rows per chunk). The
                    # worker loops internally within [pk_start, pk_end] and
                    # resumes from last_pk on restart.
                    "chunk_size": chunk_size,
                    "bulk_mode": task_bulk_mode,
                    "committer_mode": task_committer_mode,
                    "transform_steps": steps,
                    "destination": dest_block,
                    "source": source_block,
                    "source_schema": stream.source_schema_name or "",
                    "source_table": stream.source_table_name,
                    "dest_schema": stream.destination_schema_name or "dw",
                    "dest_table": stream.destination_table_name or stream.source_table_name,
                    "primary_key": pk_str,
                }
                r.lpush(high_queue, _json.dumps(task))
                pushed += 1

        log.info(
            "initial_load producer: enqueued %d task(s) for connection=%s "
            "(snapshot_mode=transform_worker, queue=%s, parallelism=%d, chunk_size=%d)",
            pushed, connection.connection_id, high_queue, k, chunk_size,
        )
        return pushed
    except Exception as exc:
        log.warning(
            "initial_load producer: failed to enqueue tasks for connection=%s: %s",
            connection.connection_id, exc, exc_info=True,
        )
        return 0


# ===========================
# CRUD Endpoints
# ===========================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=ConnectionResponse)
async def create_connection(
    connection_data: ConnectionCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:create")),
):
    """
    Create a new connection between source and destination
    
    Requires: connections:create permission
    """
    # Validate source exists and is accessible
    # Superusers bypass the tenant filter so they can wire up sources/destinations
    # created under any tenant (the seeded admin has sub_tenant_id=NULL).
    source_conditions = [
        Source.source_id == connection_data.source_id,
        Source.is_deleted == False,
    ]
    if not getattr(current_user, "is_superuser", False):
        source_conditions.append(Source.sub_tenant_id == current_user.sub_tenant_id)
    source = db.query(Source).filter(*source_conditions).first()

    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {connection_data.source_id} not found",
        )

    # Validate destination exists and is accessible
    dest_conditions = [
        Destination.destination_id == connection_data.destination_id,
        Destination.is_deleted == False,
    ]
    if not getattr(current_user, "is_superuser", False):
        dest_conditions.append(Destination.sub_tenant_id == current_user.sub_tenant_id)
    destination = db.query(Destination).filter(*dest_conditions).first()

    if not destination:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Destination {connection_data.destination_id} not found",
        )
    
    # Validate compatibility
    is_valid, message, issues = _validate_connection_compatibility(
        db, source, destination, connection_data.sync_mode
    )
    
    if not is_valid and connection_data.status == "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot create active connection: {'; '.join(issues)}",
        )
    
    # Check for duplicate name
    existing = db.query(Connection).filter(
        Connection.sub_tenant_id == current_user.sub_tenant_id,
        Connection.connection_name == connection_data.connection_name,
        Connection.is_deleted == False,
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection with name '{connection_data.connection_name}' already exists",
        )
    
    # Calculate next sync time
    next_sync_at = None
    if connection_data.sync_frequency and connection_data.status == "active":
        next_sync_at = _calculate_next_sync_time(connection_data.sync_frequency)
    
    # Create connection
    # Inherit bank_id / sub_tenant_id from the source when the current user
    # doesn't have them set (super-admin scenario).
    src_obj = db.query(Source).filter(Source.source_id == connection_data.source_id).first()
    resolved_bank_id      = current_user.bank_id      or (src_obj.bank_id      if src_obj else None)
    resolved_sub_tenant_id = current_user.sub_tenant_id or (src_obj.sub_tenant_id if src_obj else None)

    connection = Connection(
        connection_name=connection_data.connection_name,
        source_id=connection_data.source_id,
        destination_id=connection_data.destination_id,
        sync_mode=connection_data.sync_mode,
        sync_type=getattr(connection_data, 'sync_type', None) or "BATCH",
        schedule_cron=connection_data.sync_frequency,
        resource_limits=connection_data.resource_limits,
        status=connection_data.status,
        sub_tenant_id=resolved_sub_tenant_id,
        bank_id=resolved_bank_id,
    )
    
    db.add(connection)
    db.flush()  # Get connection ID for streams
    
    # Resolve default destination schema once — used as fallback for any stream
    # that doesn't explicitly specify destination_schema_name.
    dest_obj = db.query(Destination).filter(Destination.destination_id == connection_data.destination_id).first()
    default_dest_schema = (dest_obj.schema_name if dest_obj else None) or "public"

    # Create streams if provided
    if connection_data.streams:
        for stream_data in connection_data.streams:
            stream = Stream(
                connection_id=connection.connection_id,
                stream_name=stream_data.stream_name,
                stream_namespace=stream_data.stream_namespace,
                source_table_name=stream_data.source_table_name or stream_data.stream_name,
                source_schema_name=stream_data.source_schema_name or stream_data.stream_namespace or "",
                destination_table_name=stream_data.destination_table_name or stream_data.source_table_name or stream_data.stream_name,
                # Auto-fill destination schema from destination config when not provided in request
                destination_schema_name=stream_data.destination_schema_name or default_dest_schema,
                sync_mode=stream_data.sync_mode,
                cursor_field=stream_data.cursor_field,
                primary_keys=stream_data.primary_keys or [],
                is_enabled=stream_data.is_enabled if stream_data.is_enabled is not None else True,
                column_mapping=stream_data.column_mapping or {},
                transform_overrides=stream_data.transform_steps or {},
            )
            db.add(stream)
    
    db.commit()
    db.refresh(connection)

    record_audit(
        db,
        "connection.create",
        user=current_user,
        resource_type="connection",
        resource_id=str(connection.connection_id),
        details={"connection_name": connection.connection_name, "sync_mode": connection.sync_mode},
    )

    # Trigger initial sync automatically if connection is created as active
    if connection.status == "active":
        connection.initial_load_started_at = datetime.utcnow()
        await _trigger_dag_or_worker(connection, db)
        db.commit()
    
    # Load relationships
    db.refresh(connection, ["source", "destination", "streams"])
    
    return ConnectionResponse.model_validate(connection)


@router.get("", response_model=ConnectionListResponse)
async def list_connections(
    status_filter: Optional[str] = Query(None, alias="status"),
    sync_mode: Optional[str] = Query(None),
    source_id: Optional[UUID] = Query(None),
    destination_id: Optional[UUID] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    List all connections for the current tenant
    
    Supports filtering by:
    - status: draft, active, paused, inactive
    - sync_mode: cdc, full_refresh, incremental
    - source_id: specific source
    - destination_id: specific destination
    - search: search in connection name
    
    Results are paginated.
    """
    # Build base query with tenant filtering
    query = (
        db.query(Connection)
        .options(
            joinedload(Connection.source).joinedload(Source.connector_definition),
            joinedload(Connection.destination).joinedload(Destination.connector_definition),
        )
        .filter(
            Connection.sub_tenant_id == current_user.sub_tenant_id,
            Connection.is_deleted == False,
        )
    )
    
    # Apply filters
    if status_filter:
        query = query.filter(Connection.status == status_filter)
    
    if sync_mode:
        query = query.filter(Connection.sync_mode == sync_mode)
    
    if source_id:
        query = query.filter(Connection.source_id == source_id)
    
    if destination_id:
        query = query.filter(Connection.destination_id == destination_id)
    
    if search:
        query = query.filter(Connection.connection_name.ilike(f"%{search}%"))
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    offset = (page - 1) * page_size
    connections = query.order_by(Connection.created_at.desc()).offset(offset).limit(page_size).all()
    
    # Convert to response models
    connection_responses = [ConnectionResponse.model_validate(conn) for conn in connections]
    
    total_pages = (total + page_size - 1) // page_size
    
    return ConnectionListResponse(
        connections=connection_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get connection details by ID"""
    connection = _get_connection_by_id(db, connection_id, current_user)
    return ConnectionResponse.model_validate(connection)


@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: UUID,
    connection_data: ConnectionUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Update connection configuration
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Check for duplicate name if name is being changed
    if connection_data.connection_name and connection_data.connection_name != connection.connection_name:
        existing = db.query(Connection).filter(
            Connection.sub_tenant_id == current_user.sub_tenant_id,
            Connection.connection_name == connection_data.connection_name,
            Connection.is_deleted == False,
            Connection.connection_id != connection_id,
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection with name '{connection_data.connection_name}' already exists",
            )
    
    # Prevent status change to active if connection is running
    if connection_data.status and connection.status == "active" and connection_data.status != "active":
        # TODO: Check if sync is currently running
        pass
    
    # Update fields
    update_dict = connection_data.model_dump(exclude_unset=True)
    
    # v1.2.25 Bug 2.2: public Pydantic field is sync_frequency but the ORM
    # column is schedule_cron. Remap so PATCH persists the schedule
    # (previously setattr(connection, "sync_frequency", ...) set a
    # non-mapped attribute and silently no-op'd for this field).
    if "sync_frequency" in update_dict:
        update_dict["schedule_cron"] = update_dict.pop("sync_frequency")
    
    for field, value in update_dict.items():
        setattr(connection, field, value)
    
    # Update next sync time if frequency changed
    if connection_data.sync_frequency and connection.status == "active":
        connection.next_sync_at = _calculate_next_sync_time(connection_data.sync_frequency)
    
    db.commit()
    db.refresh(connection)

    record_audit(
        db,
        "connection.update",
        user=current_user,
        resource_type="connection",
        resource_id=str(connection.connection_id),
        details={"fields": list(update_dict.keys())},
    )

    # Load relationships
    db.refresh(connection, ["source", "destination", "streams"])

    return ConnectionResponse.model_validate(connection)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    force: bool = Query(False, description="Force delete even if active"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:delete")),
):
    """
    Delete connection (soft delete)
    
    Requires: connections:delete permission
    
    Will fail if connection is active unless force=true.
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Check if connection is active
    if connection.status == "active" and not force:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete active connection. Pause or deactivate first, or use force=true",
        )
    
    # Soft delete
    connection.is_deleted = True
    connection.deleted_at = datetime.utcnow()
    connection.status = "inactive"

    # Stop the CDC worker from streaming this connection and clean Redis keys
    _stop_worker_streaming(connection)
    _delete_redis_cdc_keys(str(connection.source_id))

    # Disable all streams for this connection, tearing down each stream's
    # dynamically-provisioned Iceberg committer (see
    # app/services/committer_provisioner.py / _enqueue_initial_load_tasks's
    # ensure_committer call at creation/activation time). Never blocks the
    # delete on a teardown failure — an orphaned committer Deployment is a
    # cleanup nuisance, not a reason to fail the connection delete.
    dest_connector_type = ""
    if connection.destination and connection.destination.connector_definition:
        dest_connector_type = (connection.destination.connector_definition.connector_type or "").lower()
    for stream in db.query(Stream).filter(Stream.connection_id == connection_id).all():
        stream.is_enabled = False
        if dest_connector_type == "iceberg":
            try:
                from app.services.committer_provisioner import teardown_committer
                teardown_committer(
                    connection_id=str(connection_id),
                    table=stream.destination_table_name or stream.source_table_name,
                )
            except Exception:
                import logging as _logging
                _logging.getLogger(__name__).exception(
                    "delete_connection: committer teardown failed for connection=%s table=%s",
                    connection_id, stream.destination_table_name,
                )

    # Cancel any running jobs
    from app.models.monitoring import ConnectionRun
    db.query(ConnectionRun).filter(
        ConnectionRun.connection_id == connection_id,
        ConnectionRun.status.in_(["running", "pending"]),
    ).update({"status": "cancelled", "updated_at": datetime.utcnow()}, synchronize_session=False)

    db.commit()


# ===========================
# Connection Validation
# ===========================

@router.post("/validate", response_model=ConnectionValidationResponse)
async def validate_connection(
    validation_request: ConnectionValidationRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Validate connection compatibility before creation
    
    Checks:
    - Source and destination accessibility
    - Connector compatibility
    - Sync mode support
    - Connection test results
    """
    # Get source
    source = db.query(Source).options(
        joinedload(Source.connector_definition)
    ).filter(
        Source.source_id == validation_request.source_id,
        Source.sub_tenant_id == current_user.sub_tenant_id,
        Source.is_deleted == False,
    ).first()
    
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {validation_request.source_id} not found",
        )
    
    # Get destination
    destination = db.query(Destination).options(
        joinedload(Destination.connector_definition)
    ).filter(
        Destination.destination_id == validation_request.destination_id,
        Destination.sub_tenant_id == current_user.sub_tenant_id,
        Destination.is_deleted == False,
    ).first()
    
    if not destination:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Destination {validation_request.destination_id} not found",
        )
    
    # Validate compatibility
    is_valid, message, issues = _validate_connection_compatibility(
        db, source, destination, validation_request.sync_mode
    )
    
    return ConnectionValidationResponse(
        is_valid=is_valid,
        message=message,
        issues=issues,
        source_compatible=source.status in ["active", "draft"],
        destination_compatible=destination.status in ["active", "draft"],
        sync_mode_supported=len([i for i in issues if "does not support" in i]) == 0,
        validated_at=datetime.utcnow(),
    )


# ===========================
# Schedule Configuration
# ===========================

@router.post("/{connection_id}/schedule", response_model=ScheduleConfigResponse)
async def configure_schedule(
    connection_id: UUID,
    schedule_config: ScheduleConfig,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Configure connection sync schedule
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Update schedule
    # v1.2.25 Bug 2.2: ORM column is schedule_cron (not sync_frequency).
    connection.schedule_cron = schedule_config.sync_frequency
    connection.sync_enabled = schedule_config.sync_enabled
    
    # Store timezone in config
    if "schedule" not in connection.config:
        connection.config["schedule"] = {}
    connection.config["schedule"]["timezone"] = schedule_config.timezone
    
    # Update next sync time
    if connection.status == "active" and schedule_config.sync_enabled:
        connection.next_sync_at = _calculate_next_sync_time(schedule_config.sync_frequency)
    else:
        connection.next_sync_at = None
    
    from sqlalchemy import flag_modified
    flag_modified(connection, "config")
    
    db.commit()
    db.refresh(connection)
    
    return ScheduleConfigResponse(
        connection_id=connection_id,
        schedule_config=schedule_config,
        next_sync_at=connection.next_sync_at,
        updated_at=connection.updated_at,
    )


@router.get("/{connection_id}/schedule", response_model=ScheduleConfigResponse)
async def get_schedule(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get current schedule configuration"""
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    timezone = connection.config.get("schedule", {}).get("timezone", "UTC")
    
    schedule_config = ScheduleConfig(
        sync_frequency=connection.schedule_cron or "manual",  # v1.2.25 Bug 2.2: ORM col is schedule_cron
        sync_enabled=connection.sync_enabled,
        timezone=timezone,
    )
    
    return ScheduleConfigResponse(
        connection_id=connection_id,
        schedule_config=schedule_config,
        next_sync_at=connection.next_sync_at,
        updated_at=connection.updated_at,
    )


# ===========================
# Connection Actions
# ===========================

@router.post("/{connection_id}/activate", response_model=ConnectionActivateResponse)
async def activate_connection(
    connection_id: UUID,
    activate_request: ConnectionActivateRequest = ConnectionActivateRequest(),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Activate connection and start syncing
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user)
    
    if connection.status == "active":
        return ConnectionActivateResponse(
            connection_id=connection_id,
            status="active",
            message="Connection is already active",
            activated_at=datetime.utcnow(),
        )
    
    validation_result = None
    
    # Validate if requested
    if activate_request.validate_first:
        is_valid, message, issues = _validate_connection_compatibility(
            db, connection.source, connection.destination, connection.sync_mode
        )
        
        validation_result = ConnectionValidationResponse(
            is_valid=is_valid,
            message=message,
            issues=issues,
            source_compatible=connection.source.status in ["active", "draft"],
            destination_compatible=connection.destination.status in ["active", "draft"],
            sync_mode_supported=len([i for i in issues if "does not support" in i]) == 0,
            validated_at=datetime.utcnow(),
        )
        
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection validation failed: {'; '.join(issues)}",
            )
    
    # Activate connection
    connection.status = "active"
    
    # TODO: Trigger initial sync if not skip_initial_sync
    if not activate_request.skip_initial_sync:
        # Mark that initial load is in progress; the Airflow DAG / Spark consumer
        # will call /internal/connections/{id}/run-complete when done.
        connection.initial_load_started_at = datetime.utcnow()
        await _trigger_dag_or_worker(connection, db)
    
    db.commit()
    
    return ConnectionActivateResponse(
        connection_id=connection_id,
        status="active",
        message="Connection activated successfully",
        validation_result=validation_result,
        activated_at=datetime.utcnow(),
    )


@router.post("/{connection_id}/pause", response_model=dict)
async def pause_connection(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Pause connection (stop scheduled syncs)
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    if connection.status != "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot pause connection with status '{connection.status}'",
        )
    
    connection.status = "paused"
    connection.sync_enabled = False
    connection.next_sync_at = None

    # For CDC/REALTIME connections, notify the worker to stop streaming
    _stop_worker_streaming(connection)
    
    db.commit()
    
    return {
        "connection_id": connection_id,
        "status": "paused",
        "message": "Connection paused successfully",
        "paused_at": datetime.utcnow(),
    }


@router.post("/{connection_id}/resume", response_model=dict)
async def resume_connection(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Resume paused connection
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    if connection.status != "paused":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot resume connection with status '{connection.status}'",
        )
    
    connection.status = "active"
    connection.sync_enabled = True
    
    # v1.2.25 Bug 2.2: ORM column is schedule_cron (not sync_frequency).
    if connection.schedule_cron:
        connection.next_sync_at = _calculate_next_sync_time(connection.schedule_cron)

    # For CDC/REALTIME connections, notify the worker to resume streaming
    await _trigger_dag_or_worker(connection, db)
    
    db.commit()
    
    return {
        "connection_id": connection_id,
        "status": "active",
        "message": "Connection resumed successfully",
        "resumed_at": datetime.utcnow(),
    }


@router.post("/{connection_id}/trigger-sync", response_model=SyncTriggerResponse)
async def trigger_manual_sync(
    connection_id: UUID,
    sync_request: SyncTriggerRequest = SyncTriggerRequest(),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Trigger manual sync for connection
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user)
    
    if connection.status not in ["active", "paused"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot trigger sync for connection with status '{connection.status}'",
        )
    
    sync_type = (connection.sync_type or "").upper()

    # Determine next run_number
    last_run = (
        db.query(ConnectionRun)
        .filter(ConnectionRun.connection_id == connection_id)
        .order_by(ConnectionRun.run_number.desc())
        .first()
    )
    next_run_number = (last_run.run_number + 1) if last_run else 1

    # Mark any stale "running" batch runs as failed before creating the new one.
    # CDC/streaming runs run indefinitely — never mark them stale here.
    if sync_type in ("BATCH", "SCHEDULED"):
        stale_cutoff = datetime.utcnow() - timedelta(minutes=10)
        stale_runs = (
            db.query(ConnectionRun)
            .filter(
                ConnectionRun.connection_id == connection_id,
                ConnectionRun.status == "running",
                ConnectionRun.started_at < stale_cutoff,
            )
            .all()
        )
        for stale in stale_runs:
            stale.status = "failed"
            stale.completed_at = datetime.utcnow()
            stale.error_message = "Run timed out — no worker response within 10 minutes"

    # Create a new ConnectionRun record
    is_first_sync = not connection.initial_load_completed
    run = ConnectionRun(
        connection_id=connection_id,
        run_number=next_run_number,
        trigger_type="manual",
        triggered_by=current_user.user_id,
        status="running",
        started_at=datetime.utcnow(),
        run_config={
            "sync_mode": connection.sync_mode,
            "sync_type": sync_type,
            "is_first_sync": is_first_sync,
            "orchestration": "airflow" if sync_type in ("BATCH", "SCHEDULED") else "streaming",
        },
    )
    db.add(run)

    record_audit(
        db,
        "connection.sync",
        user=current_user,
        resource_type="connection",
        resource_id=str(connection_id),
        details={"sync_type": sync_type, "run_number": next_run_number},
    )

    # Mark initial load started if first sync
    if is_first_sync:
        connection.initial_load_started_at = datetime.utcnow()

    await _trigger_dag_or_worker(connection, db)

    record_audit(
        db,
        "connection_run.start",
        user=current_user,
        resource_type="connection_run",
        resource_id=str(run.run_id),
        details={"connection_id": str(connection_id), "run_number": next_run_number, "sync_type": sync_type},
    )

    # For CDC/streaming connections: check worker health immediately.
    # A CDC "trigger" is fire-and-confirm — the run completes once the worker
    # acknowledges the source is being streamed.  The actual streaming lives in
    # the worker process indefinitely; the run record is just an audit entry.
    worker_reachable = _check_worker_reachable(db)
    if sync_type not in ("BATCH", "SCHEDULED"):
        if not worker_reachable:
            run.status = "failed"
            run.completed_at = datetime.utcnow()
            run.error_message = "CDC worker is not running. Start the cdc-worker service and retry."
        else:
            import httpx as _httpx, os as _os
            _worker_url = _os.environ.get("CDC_WORKER_URL", "http://localhost:8081")
            _source_id = str(connection.source_id)
            _health_data = {}
            try:
                _health_data = _httpx.get(f"{_worker_url}/health", timeout=6.0).json()
                _active = _health_data.get("active_sources", [])
                if _source_id in _active:
                    # Source already streaming — mark initial load complete.
                    if not connection.initial_load_completed:
                        connection.initial_load_completed = True
                        connection.initial_load_completed_at = datetime.utcnow()
            except Exception:
                _active = []  # Worker reachable but health endpoint failed — stream starting up

            # Capture a snapshot of the current CDC state for the run log
            _snapshot: dict = {
                "worker_id": _health_data.get("worker_id"),
                "worker_status": "healthy",
                "is_source_active": _source_id in _active,
                "tables": [],
                "binlog_position": None,
                "checkpoint_at": None,
                "redis_event_counts": {},
                "total_events": 0,
            }
            try:
                # Tables being monitored for this connection
                _streams_q = db.query(Stream).filter(
                    Stream.connection_id == connection_id,
                    Stream.is_enabled == True,
                ).all()
                _snapshot["tables"] = [f"{s.source_schema_name}.{s.source_table_name}" for s in _streams_q]

                # Latest checkpoint
                _ckpt = (
                    db.query(CheckpointState)
                    .filter(CheckpointState.source_id == connection.source_id)
                    .order_by(CheckpointState.checkpoint_at.desc())
                    .first()
                )
                if _ckpt:
                    _snapshot["binlog_position"] = _ckpt.lsn
                    _snapshot["checkpoint_at"] = (
                        _ckpt.checkpoint_at.isoformat() if _ckpt.checkpoint_at else None
                    )

                # Redis event counts per table
                import redis as _redis_lib
                _redis_url = _os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))
                _source_obj = db.query(Source).filter(Source.source_id == connection.source_id).first()
                if _source_obj:
                    _bank_id = str(_source_obj.bank_id)
                    _tenant_id = str(_source_obj.sub_tenant_id)
                    _r = _redis_lib.from_url(_redis_url, decode_responses=True)
                    _counts = {}
                    for _s in _streams_q:
                        _key = f"cdc:{_bank_id}:{_tenant_id}:{_source_id}:{_s.source_schema_name}:{_s.source_table_name}"
                        try:
                            _counts[f"{_s.source_schema_name}.{_s.source_table_name}"] = _r.xlen(_key)
                        except Exception:
                            _counts[f"{_s.source_schema_name}.{_s.source_table_name}"] = 0
                    _snapshot["redis_event_counts"] = _counts
                    _snapshot["total_events"] = sum(_counts.values())
            except Exception:
                pass  # snapshot enrichment is best-effort

            # Mark the run completed — streaming is handled by the worker process.
            # Each trigger is a point-in-time "start/verify streaming" action.
            run.status = "completed"
            run.completed_at = datetime.utcnow()
            run.error_message = None
            run.run_config = {**run.run_config, "snapshot": _snapshot}

            record_audit(
                db,
                "connection_run.complete",
                user=current_user,
                resource_type="connection_run",
                resource_id=str(run.run_id),
                details={"connection_id": str(connection_id), "sync_type": sync_type, "snapshot": _snapshot},
            )

    db.commit()

    return SyncTriggerResponse(
        connection_id=connection_id,
        sync_triggered=True,
        message=f"Manual sync triggered successfully via {'Airflow' if sync_type in ('BATCH', 'SCHEDULED') else 'streaming worker'}",
        triggered_at=datetime.utcnow(),
        estimated_duration_seconds=None,
    )


# ===========================
# Retry Initial Load
# ===========================

@router.post("/{connection_id}/retry-initial-load", response_model=dict,
             status_code=status.HTTP_202_ACCEPTED)
async def retry_initial_load(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """Re-enqueue the initial-load snapshot tasks for a connection.

    v1.2.18: ``_enqueue_initial_load_tasks`` previously only fired once at
    connection creation. If it failed (e.g. the transform-worker wasn't
    ready, Redis was down, or the destination's ``snapshot_mode`` was
    misconfigured), users had to delete + recreate the connection to retry.
    This endpoint lets them retry without that destructive workaround.

    v1.2.27 P0 fix: returns ``202 Accepted`` immediately and runs the
    partitioning + enqueue in a background ``asyncio.create_task``. The
    partitioning step (``MIN/MAX`` + ``information_schema`` count, with a 30s
    timeout + KILL fallback) is offloaded to a threadpool so the uvicorn event
    loop stays responsive. The UI polls
    ``GET /connections/{id}/initial-load/status`` for progress. Only valid
    for CDC/REALTIME connections (BATCH/SCHEDULED connections use Airflow,
    not the transform-worker snapshot path).
    """
    import asyncio
    import uuid as _uuid

    connection = _get_connection_by_id(db, connection_id, current_user)

    sync_type = (getattr(connection, "sync_type", "") or "").upper()
    if sync_type in ("BATCH", "SCHEDULED"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "retry-initial-load is only supported for CDC/REALTIME "
                f"connections (this connection is sync_type={sync_type or 'BATCH'})."
            ),
        )

    # Reset initial-load state so the UI/run list reflects the retry.
    connection.initial_load_completed = False
    connection.initial_load_completed_at = None
    connection.initial_load_started_at = datetime.utcnow()
    db.commit()

    # Mark phase=partitioning and spawn the background task. The background
    # task opens its OWN DB session (the request session is closed once the
    # response is returned) and updates the state dict as it progresses.
    conn_id_str = str(connection_id)
    task_id = f"il-{conn_id_str}-{_uuid.uuid4().hex[:8]}"
    _set_initial_load_phase(
        conn_id_str, "partitioning",
        task_id=task_id, partitions=0, rows_estimated=None, error=None,
    )

    asyncio.create_task(_run_initial_load_background(
        conn_id_str, connection.connection_id, current_user.user_id,
    ))

    record_audit(
        db,
        "connection.retry_initial_load",
        user=current_user,
        resource_type="connection",
        resource_id=conn_id_str,
        details={"task_id": task_id, "phase": "partitioning"},
    )
    db.commit()

    return {
        "ok": True,
        "connection_id": conn_id_str,
        "status": "partitioning",
        "task_id": task_id,
        "message": (
            "Initial-load partitioning started in the background. Poll "
            "GET /connections/{id}/initial-load/status for progress."
        ),
        "retried_at": datetime.utcnow().isoformat(),
    }


async def _run_initial_load_background(conn_id_str: str,
                                       connection_id, user_id) -> None:
    """Background task: partition + enqueue the initial load off the request
    path. Opens its own DB session (the request session is closed by the time
    this runs) and updates ``_initial_load_state`` as it progresses.

    Failures are recorded in the state dict (phase=failed, error=...) so the
    UI can surface them via the status endpoint — the task itself never
    raises (it's a fire-and-forget ``asyncio.create_task``).
    """
    import logging as _logging
    log = _logging.getLogger(__name__)
    try:
        from app.database import SessionLocal
        from app.models.auth import User
        from app.models.connection import Connection
        from sqlalchemy.orm import joinedload
        with SessionLocal() as bg_db:
            connection = (
                bg_db.query(Connection)
                .options(joinedload(Connection.streams))
                .filter(Connection.connection_id == connection_id)
                .first()
            )
            if not connection:
                _set_initial_load_phase(conn_id_str, "failed",
                                        error="connection not found")
                return
            current_user = bg_db.query(User).filter(User.user_id == user_id).first()
            # Phase=partitioning -> _enqueue_initial_load_tasks does the
            # partitioning (offloaded to threadpool) and the enqueue.
            tasks_enqueued = await _enqueue_initial_load_tasks(connection, bg_db)
            _set_initial_load_phase(
                conn_id_str, "enqueued",
                partitions=tasks_enqueued,
            )
            # Record audit (best-effort).
            try:
                if current_user is not None:
                    record_audit(
                        bg_db,
                        "connection.retry_initial_load.enqueued",
                        user=current_user,
                        resource_type="connection",
                        resource_id=conn_id_str,
                        details={"tasks_enqueued": tasks_enqueued},
                    )
                bg_db.commit()
            except Exception as exc:
                log.warning("initial_load background audit failed: %s", exc)
                bg_db.rollback()
    except Exception as exc:
        log.warning("initial_load background task failed for %s: %s",
                    conn_id_str, exc, exc_info=True)
        _set_initial_load_phase(conn_id_str, "failed", error=str(exc))


@router.get("/{connection_id}/initial-load/status", response_model=dict)
async def get_initial_load_status(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """v1.2.27: return the current partitioning/enqueue state for a
    connection's initial load. The UI polls this after
    ``POST /connections/{id}/retry-initial-load`` (which returns 202
    immediately) to show progress.

    Returns ``{"phase": "idle"|"partitioning"|"enqueued"|"running"|"completed"|"failed",
              "task_id": ..., "partitions": K, "rows_estimated": N, "error": ...}``.
    """
    # Verify the connection exists + tenant filter (raises 404 if not).
    _get_connection_by_id(db, connection_id, current_user)
    conn_id_str = str(connection_id)
    state = _get_initial_load_state(conn_id_str)
    # Reflect the connection's overall initial_load_completed flag too so
    # the UI can show "completed" once the worker reports done.
    conn = db.query(Connection).filter(
        Connection.connection_id == connection_id,
    ).first()
    completed = bool(getattr(conn, "initial_load_completed", False)) if conn else False
    return {
        "connection_id": conn_id_str,
        "phase": state.get("phase", "idle"),
        "task_id": state.get("task_id"),
        "partitions": state.get("partitions", 0),
        "rows_estimated": state.get("rows_estimated"),
        "error": state.get("error"),
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "initial_load_completed": completed,
    }


@router.get("/{connection_id}/initial-load/progress", response_model=dict)
async def get_initial_load_progress(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """v1.2.29 Task 3: real-time initial-load progress + ETA. Aggregates the
    per-partition ``initial_load_checkpoints`` rows for the connection and
    returns:

      - ``phase``: idle | partitioning | enqueued | running | completed | failed
      - ``rows_written``: cumulative rows written across all partitions
      - ``rows_estimated``: sum of per-partition estimates (NULL when unknown)
      - ``progress_pct``: 0..100 (NULL when estimate unknown)
      - ``eta_seconds``: estimated seconds to completion (NULL when unknown)
      - ``throughput_rows_per_sec``: recent throughput
      - ``partitions``: [{chunk_seq, status, rows_written, rows_estimated,
                          pk_start, pk_end, last_pk, last_updated_at, progress_pct}]

    The frontend polls this every 5s while a load is in flight.
    """
    from datetime import datetime, timezone
    _get_connection_by_id(db, connection_id, current_user)
    conn_id_str = str(connection_id)
    state = _get_initial_load_state(conn_id_str)

    rows = (
        db.query(InitialLoadCheckpoint)
        .filter(InitialLoadCheckpoint.connection_id == connection_id)
        .order_by(InitialLoadCheckpoint.chunk_seq)
        .all()
    )

    now = datetime.now(timezone.utc)
    total_written = sum((r.rows_written or 0) for r in rows)
    total_estimated = sum((r.rows_estimated or 0) for r in rows if r.rows_estimated)
    has_estimate = any(r.rows_estimated for r in rows)

    # Throughput: rows written since the oldest partition's started_at.
    throughput = None
    if rows:
        started = min((r.started_at for r in rows if r.started_at), default=None)
        if started is not None:
            # Compute elapsed treating started_at as tz-aware.
            s = started if started.tzinfo else started.replace(tzinfo=timezone.utc)
            elapsed = (now - s).total_seconds()
            if elapsed > 0:
                throughput = total_written / elapsed

    progress_pct = None
    eta_seconds = None
    if has_estimate and total_estimated > 0:
        progress_pct = round(min(100.0, (total_written / total_estimated) * 100.0), 2)
        if throughput and throughput > 0 and total_written < total_estimated:
            eta_seconds = int((total_estimated - total_written) / throughput)

    # Overall phase: prefer the in-memory partitioning state; otherwise infer
    # from checkpoint statuses.
    phase = state.get("phase", "idle")
    if phase in ("idle", None):
        statuses = {r.status for r in rows}
        if statuses and all(s == "completed" for s in statuses):
            phase = "completed"
        elif statuses and any(s == "failed" for s in statuses):
            phase = "failed"
        elif statuses:
            phase = "running"

    partitions = []
    for r in rows:
        est = r.rows_estimated
        written = r.rows_written or 0
        p_pct = round(min(100.0, (written / est) * 100.0), 2) if est else None
        partitions.append({
            "chunk_seq": r.chunk_seq,
            "status": r.status,
            "rows_written": written,
            "rows_estimated": est,
            "pk_start": r.pk_start,
            "pk_end": r.pk_end,
            "last_pk": r.last_pk,
            "last_updated_at": r.last_updated_at.isoformat() if r.last_updated_at else None,
            "progress_pct": p_pct,
        })

    return {
        "connection_id": conn_id_str,
        "phase": phase,
        "rows_written": total_written,
        "rows_estimated": total_estimated if has_estimate else None,
        "progress_pct": progress_pct,
        "eta_seconds": eta_seconds,
        "throughput_rows_per_sec": round(throughput, 2) if throughput else None,
        "partitions": partitions,
        "updated_at": state.get("updated_at"),
    }


# ===========================
# Statistics
# ===========================

@router.get("/{connection_id}/stats", response_model=ConnectionStats)
async def get_connection_stats(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get connection usage statistics
    
    Returns sync count, data volume, and performance metrics.
    """
    connection = _get_connection_by_id(db, connection_id, current_user)

    # Count streams
    total_streams = len(connection.streams)
    active_streams = sum(1 for s in connection.streams if s.is_enabled)

    # Derive sync statistics from AuditLog entries (spec §5 P5-8)
    conn_id_str = str(connection_id)
    run_logs = (
        db.query(AuditLog)
        .filter(
            AuditLog.resource_type == "connection",
            AuditLog.resource_id == conn_id_str,
            AuditLog.action.like("connection.batch_run.%"),
        )
        .order_by(AuditLog.created_at.desc())
        .limit(1000)
        .all()
    )

    total_syncs = len(run_logs)
    successful_syncs = sum(1 for r in run_logs if r.action == "connection.batch_run.success")
    failed_syncs = total_syncs - successful_syncs
    total_rows_synced = sum(
        int((r.details or {}).get("rows_synced", 0) or 0) for r in run_logs
        if r.action == "connection.batch_run.success"
    )
    last_sync_at = run_logs[0].created_at if run_logs else None

    # Build human-readable last_sync string
    last_sync_str = None
    if last_sync_at:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        if last_sync_at.tzinfo is None:
            from datetime import timezone as tz
            last_sync_at_aware = last_sync_at.replace(tzinfo=tz.utc)
        else:
            last_sync_at_aware = last_sync_at
        delta = now - last_sync_at_aware
        if delta.total_seconds() < 60:
            last_sync_str = "Just now"
        elif delta.total_seconds() < 3600:
            last_sync_str = f"{int(delta.total_seconds() // 60)}m ago"
        elif delta.total_seconds() < 86400:
            last_sync_str = f"{int(delta.total_seconds() // 3600)}h ago"
        else:
            last_sync_str = f"{int(delta.days)}d ago"

    return ConnectionStats(
        connection_id=connection_id,
        connection_name=connection.connection_name,
        status=connection.status,
        total_syncs=total_syncs,
        successful_syncs=successful_syncs,
        failed_syncs=failed_syncs,
        last_sync_at=last_sync_at,
        last_sync_duration_seconds=None,
        total_rows_synced=total_rows_synced,
        total_bytes_synced=0,
        avg_sync_duration_seconds=None,
        avg_throughput_rows_per_sec=None,
        consecutive_failures=0,
        last_error=None,
        total_streams=total_streams,
        active_streams=active_streams,
        events_per_hour=total_rows_synced,
        lag_seconds=None,
        uptime_percent=100.0 if connection.status == "active" else 0.0,
        last_sync=last_sync_str,
    )


@router.get("/{connection_id}/runs")
async def get_connection_runs(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get sync run history for a connection, including initial load checkpoints."""
    connection = _get_connection_by_id(db, connection_id, current_user)

    runs_db = (
        db.query(ConnectionRun)
        .filter(ConnectionRun.connection_id == connection_id)
        .order_by(ConnectionRun.run_number.desc())
        .limit(100)
        .all()
    )

    stale_cutoff = datetime.utcnow() - timedelta(minutes=10)
    updated = False

    result = []
    for run in runs_db:
        effective_status = run.status
        error_msg = run.error_message

        _is_streaming = (run.run_config or {}).get("orchestration") == "streaming"
        if run.status == "running" and run.started_at and not _is_streaming:
            started_naive = run.started_at.replace(tzinfo=None) if run.started_at.tzinfo else run.started_at
            if started_naive < stale_cutoff:
                run.status = "failed"
                run.completed_at = datetime.utcnow()
                run.error_message = run.error_message or "Run timed out — no worker response within 10 minutes"
                effective_status = "failed"
                error_msg = run.error_message
                updated = True

        duration_sec = None
        if run.completed_at and run.started_at:
            started = run.started_at.replace(tzinfo=None) if run.started_at.tzinfo else run.started_at
            ended = run.completed_at.replace(tzinfo=None) if run.completed_at.tzinfo else run.completed_at
            duration_sec = int((ended - started).total_seconds())

        is_first = run.run_config.get("is_first_sync", False) if run.run_config else False

        result.append({
            "id": str(run.run_id),
            "run_number": run.run_number,
            "run_type": "initial_load" if is_first else "cdc",
            "trigger_type": run.trigger_type,
            "status": effective_status,
            "records_inserted": int(run.records_written or 0),
            "records_updated": 0,
            "records_deleted": 0,
            "records_synced": int(run.records_written or 0),
            "records_read": int(run.records_read or 0),
            "duration": duration_sec,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "error_message": error_msg,
            "is_first_sync": is_first,
            "tables": [],
            "run_config": run.run_config,
        })

    if updated:
        db.commit()

    # Always also include initial load checkpoints as synthetic "Run #0 – Initial Load" entries
    # even when connection_runs is empty (worker may not write a ConnectionRun record)
    checkpoints = (
        db.query(InitialLoadCheckpoint)
        .filter(InitialLoadCheckpoint.connection_id == connection_id)
        .order_by(InitialLoadCheckpoint.started_at)
        .all()
    )

    if checkpoints:
        # Group by start time (same second = same bulk run)
        total_rows = sum(int(c.rows_written or 0) for c in checkpoints)
        earliest_start = min((c.started_at for c in checkpoints if c.started_at), default=None)
        latest_end = max((c.completed_at for c in checkpoints if c.completed_at), default=None)
        all_done = all(c.status in ("done", "completed") for c in checkpoints)
        any_error = any(c.status == "error" for c in checkpoints)
        duration_sec_il = None
        if earliest_start and latest_end:
            s = earliest_start.replace(tzinfo=None) if earliest_start.tzinfo else earliest_start
            e = latest_end.replace(tzinfo=None) if latest_end.tzinfo else latest_end
            duration_sec_il = int((e - s).total_seconds())

        # Only add synthetic entry if there's no existing initial_load run in connection_runs
        has_il_run = any(r.get("is_first_sync") for r in result)
        if not has_il_run:
            table_details = [
                {
                    "table_name": c.source_table,
                    "rows_inserted": int(c.rows_written or 0),
                    "rows_updated": 0,
                    "rows_deleted": 0,
                    "status": "completed" if c.status in ("done", "completed") else c.status,
                    "started_at": c.started_at.isoformat() if c.started_at else None,
                    "completed_at": c.completed_at.isoformat() if c.completed_at else None,
                    "error": c.error,
                }
                for c in checkpoints
            ]
            result.append({
                "id": f"il-{connection_id}",
                "run_number": 0,
                "run_type": "initial_load",
                "trigger_type": "initial_load",
                "status": "completed" if all_done else ("failed" if any_error else "running"),
                "records_inserted": total_rows,
                "records_updated": 0,
                "records_deleted": 0,
                "records_synced": total_rows,
                "records_read": total_rows,
                "duration": duration_sec_il,
                "started_at": earliest_start.isoformat() if earliest_start else None,
                "completed_at": latest_end.isoformat() if latest_end else None,
                "error_message": None,
                "is_first_sync": True,
                "tables": table_details,
                "run_config": {},
            })

    # Sort: most recent first
    result.sort(key=lambda r: r.get("started_at") or "", reverse=True)
    return result


@router.get("/{connection_id}/initial-load")
async def get_initial_load_status(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get initial load checkpoint details per table for a connection."""
    connection = _get_connection_by_id(db, connection_id, current_user)
    checkpoints = (
        db.query(InitialLoadCheckpoint)
        .filter(InitialLoadCheckpoint.connection_id == connection_id)
        .order_by(InitialLoadCheckpoint.started_at)
        .all()
    )
    tables = []
    for c in checkpoints:
        duration_sec = None
        if c.started_at and c.completed_at:
            s = c.started_at.replace(tzinfo=None) if c.started_at.tzinfo else c.started_at
            e = c.completed_at.replace(tzinfo=None) if c.completed_at.tzinfo else c.completed_at
            duration_sec = int((e - s).total_seconds())
        tables.append({
            "checkpoint_id": str(c.checkpoint_id),
            "source_table": c.source_table,
            "rows_written": int(c.rows_written or 0),
            "status": "completed" if c.status in ("done", "completed") else c.status,
            "started_at": c.started_at.isoformat() if c.started_at else None,
            "completed_at": c.completed_at.isoformat() if c.completed_at else None,
            # v1.2.25 Bug 2.3: expose last_updated_at so the UI can show
            # "last progress N seconds ago" and detect stuck loads.
            "last_updated_at": c.last_updated_at.isoformat() if c.last_updated_at else None,
            "duration_seconds": duration_sec,
            "error": c.error,
            # v1.2.25: expose chunk-resume fields so the UI can show
            # "chunk 7 of ? — last_pk=12345" for a running load.
            "chunk_seq": int(c.chunk_seq or 0),
            "last_pk": c.last_pk,
            "current_chunk": int(c.current_chunk or 0),
            "total_chunks": int(c.total_chunks) if c.total_chunks is not None else None,
        })
    total_rows = sum(t["rows_written"] for t in tables)
    completed = sum(1 for t in tables if t["status"] == "completed")
    # v1.2.25 Bug 2.3: connection-level last_updated_at = the most recent
    # per-table last_updated_at, so the UI can surface "last progress" for
    # the whole load (not just per table).
    per_table_updates = [
        t["last_updated_at"] for t in tables if t["last_updated_at"]
    ]
    last_updated_at = max(per_table_updates) if per_table_updates else None
    return {
        "connection_id": str(connection_id),
        "initial_load_completed": connection.initial_load_completed,
        "initial_load_started_at": connection.initial_load_started_at.isoformat() if connection.initial_load_started_at else None,
        "initial_load_completed_at": connection.initial_load_completed_at.isoformat() if connection.initial_load_completed_at else None,
        "total_rows_written": total_rows,
        "tables_total": len(tables),
        "tables_completed": completed,
        "tables": tables,
        "last_updated_at": last_updated_at,
    }


@router.get("/{connection_id}/health")
async def get_connection_health(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get connection health status with lag and throughput history."""
    connection = _get_connection_by_id(db, connection_id, current_user)

    worker_status = "no_worker"
    last_heartbeat = None

    # Try to get heartbeat data if model exists
    try:
        from app.models.monitoring import WorkerHeartbeat, CDCLagMetrics
        latest_hb = (
            db.query(WorkerHeartbeat)
            .filter(WorkerHeartbeat.connection_id == connection_id)
            .order_by(WorkerHeartbeat.last_heartbeat_at.desc())
            .first()
        )
        if latest_hb:
            worker_status = latest_hb.status
            last_heartbeat = str(latest_hb.last_heartbeat_at)
    except Exception:
        pass

    return {
        "connection_id": str(connection_id),
        "status": worker_status if connection.status != "active" else (worker_status if worker_status != "no_worker" else "healthy"),
        "last_heartbeat_at": last_heartbeat,
        "lag_seconds": None,
        "lag_events": None,
        "lag_history": [],
        "throughput_history": [],
    }


# ===========================
# Stream Management
# ===========================

@router.post("/{connection_id}/streams", status_code=status.HTTP_201_CREATED, response_model=StreamResponse)
async def add_stream(
    connection_id: UUID,
    stream_data: StreamCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Add a new stream to connection
    
    Requires: connections:update permission
    """
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)

    # Resolve default destination schema from the connection's destination config
    dest_obj = db.query(Destination).filter(Destination.destination_id == connection.destination_id).first()
    default_dest_schema = (dest_obj.schema_name if dest_obj else None) or "public"

    # Create stream
    stream = Stream(
        connection_id=connection_id,
        stream_name=stream_data.stream_name,
        stream_namespace=stream_data.stream_namespace,
        source_table_name=stream_data.source_table_name or stream_data.stream_name,
        source_schema_name=stream_data.source_schema_name or stream_data.stream_namespace or "",
        destination_table_name=stream_data.destination_table_name or stream_data.source_table_name or stream_data.stream_name,
        # Auto-fill destination schema from destination config when not provided in request
        destination_schema_name=stream_data.destination_schema_name or default_dest_schema,
        sync_mode=stream_data.sync_mode,
        cursor_field=stream_data.cursor_field,
        primary_keys=stream_data.primary_keys or [],
        is_enabled=stream_data.is_enabled if stream_data.is_enabled is not None else True,
        column_mapping=stream_data.column_mapping or {},
        transform_overrides=stream_data.transform_steps or {},
    )
    
    db.add(stream)
    db.commit()
    db.refresh(stream)
    
    return StreamResponse.model_validate(stream)


@router.get("/{connection_id}/streams", response_model=List[StreamResponse])
async def list_streams(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all streams for a connection"""
    connection = _get_connection_by_id(db, connection_id, current_user)
    
    return [StreamResponse.model_validate(stream) for stream in connection.streams]


@router.patch("/{connection_id}/streams/{stream_id}", response_model=StreamResponse)
async def update_stream(
    connection_id: UUID,
    stream_id: UUID,
    stream_data: StreamUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Update stream configuration
    
    Requires: connections:update permission
    """
    # Verify connection exists and user has access
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Get stream
    stream = db.query(Stream).filter(
        Stream.stream_id == stream_id,
        Stream.connection_id == connection_id,
    ).first()
    
    if not stream:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stream {stream_id} not found in connection {connection_id}",
        )
    
    # Update fields — map transform_steps → transform_overrides
    update_dict = stream_data.model_dump(exclude_unset=True)
    if 'transform_steps' in update_dict:
        update_dict['transform_overrides'] = update_dict.pop('transform_steps') or {}
    
    for field, value in update_dict.items():
        setattr(stream, field, value)
    
    db.commit()
    db.refresh(stream)
    
    return StreamResponse.model_validate(stream)


@router.delete("/{connection_id}/streams/{stream_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_stream(
    connection_id: UUID,
    stream_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:update")),
):
    """
    Delete stream from connection
    
    Requires: connections:update permission
    """
    # Verify connection exists and user has access
    connection = _get_connection_by_id(db, connection_id, current_user, include_relations=False)
    
    # Get stream
    stream = db.query(Stream).filter(
        Stream.stream_id == stream_id,
        Stream.connection_id == connection_id,
    ).first()
    
    if not stream:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stream {stream_id} not found in connection {connection_id}",
        )
    
    db.delete(stream)
    db.commit()


# ---------------------------------------------------------------------------
# v1.2.25 Task 6 — Dead-letter task inspection
# ---------------------------------------------------------------------------

@router.get("/{connection_id}/tasks/dead-letter")
async def list_dead_letter_tasks(
    connection_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:read")),
):
    """List dead-lettered transform tasks for a connection.

    Tasks that exhausted their retry budget (MAX_TASK_RETRIES) are moved by the
    transform-worker to the Redis list ``fusion:transforms:dead-letter``. This
    endpoint reads that list, filters by ``connection_id``, and returns the
    entries so operators can inspect the failure reason and requeue the task
    once the root cause is fixed.

    Each entry has the shape::

        {
          "task_id": "...",
          "connection_id": "...",
          "type": "cdc_transform" | "initial_load",
          "reason": "<truncated exception>",
          "dead_lettered_at": "<iso8601>",
          "payload": "<raw task json>"
        }
    """
    from app.config import settings
    import redis as redis_lib
    import json as _json

    try:
        r = redis_lib.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        raw_entries = r.lrange("fusion:transforms:dead-letter", 0, -1)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not read dead-letter list from Redis: {exc}",
        )

    items = []
    for raw in raw_entries:
        try:
            entry = _json.loads(raw)
        except Exception:
            continue
        # Filter by connection_id (string compare — task payloads store UUIDs
        # as strings).
        if str(entry.get("connection_id")) != str(connection_id):
            continue
        items.append(entry)

    return {"connection_id": str(connection_id), "count": len(items), "tasks": items}


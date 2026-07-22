"""
Internal Worker API — /api/v1/internal/*

Endpoints used exclusively by CDC workers (authenticated via X-Worker-Token header,
NOT the standard JWT used by the UI).

Endpoints:
  POST /api/v1/internal/workers/heartbeat
  GET  /api/v1/internal/workers/{worker_id}/sources
  GET  /api/v1/internal/workers/{worker_id}/routing
  POST /api/v1/internal/checkpoints/batch
  GET  /api/v1/internal/checkpoints/{source_id}
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel as PydanticModel
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models.auth import AuditLog
from app.models.monitoring import CheckpointState, WorkerHeartbeat
from app.models.schema_evolution import SchemaChangeEvent
from app.models.source_destination import Source
from app.models.connector import ConnectorDefinition
from app.models.connection import Connection, Stream
from app.api.sources import _decrypt_password

log = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Auth helper — X-Worker-Token
# ---------------------------------------------------------------------------

def _verify_worker_token(x_worker_token: str = Header(...)) -> str:
    """
    Validate the X-Worker-Token header.
    Returns the token on success; raises 401 otherwise.
    """
    if not settings.WORKER_SHARED_SECRET:
        # If no secret configured, token auth is disabled (dev/test mode)
        return x_worker_token
    if x_worker_token != settings.WORKER_SHARED_SECRET:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid worker token",
        )
    return x_worker_token


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class HeartbeatRequest(PydanticModel):
    worker_id: str
    worker_type: Optional[str] = None
    status: str
    ts_ms: Optional[int] = None
    events_processed: int = 0
    errors_count: int = 0
    last_error: Optional[str] = None
    cpu_usage_percent: Optional[float] = None
    memory_usage_mb: Optional[int] = None
    hostname: Optional[str] = None
    pod_name: Optional[str] = None
    worker_metadata: Dict[str, Any] = {}


class HeartbeatResponse(PydanticModel):
    ok: bool


class AssignedTable(PydanticModel):
    schema_name: str
    table_name: str
    sync_mode: Optional[str] = None
    primary_key: Optional[str] = None
    cursor_field: Optional[str] = None


class SourceAssignment(PydanticModel):
    source_id: str
    source_name: str
    connector_type: str
    host: str
    port: int
    database_name: str
    username: str
    password: str  # decrypted plaintext for worker use
    ssl_enabled: bool
    ssl_config: Dict[str, Any]
    ssh_config: Dict[str, Any]
    config: Dict[str, Any]
    bank_id: str
    tenant_id: str
    assigned_tables: List[AssignedTable] = []


class CheckpointRecord(PydanticModel):
    schema_name: str
    table_name: str
    lsn: str


class CheckpointBatchRequest(PydanticModel):
    worker_id: str
    source_id: str
    checkpoints: List[CheckpointRecord]


class CheckpointBatchResponse(PydanticModel):
    upserted: int


class RoutingEntry(PydanticModel):
    schema_name: str
    table_name: str
    bank_id: str
    tenant_id: str
    source_id: str


# ---------------------------------------------------------------------------
# Heartbeat endpoint
# ---------------------------------------------------------------------------

@router.post(
    "/workers/heartbeat",
    response_model=HeartbeatResponse,
    summary="Receive worker heartbeat",
)
def worker_heartbeat(
    payload: HeartbeatRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> HeartbeatResponse:
    """
    Upsert a worker heartbeat record.  Workers call this every HEARTBEAT_INTERVAL
    seconds to signal liveness and report metrics.
    """
    existing = (
        db.query(WorkerHeartbeat)
        .filter(WorkerHeartbeat.worker_id == payload.worker_id)
        .first()
    )

    if existing:
        existing.status = payload.status
        existing.events_processed = payload.events_processed
        existing.errors_count = payload.errors_count
        existing.last_error = payload.last_error
        existing.cpu_usage_percent = payload.cpu_usage_percent
        existing.memory_usage_mb = payload.memory_usage_mb
        existing.hostname = payload.hostname
        existing.pod_name = payload.pod_name
        existing.worker_metadata = payload.worker_metadata
        existing.last_heartbeat_at = datetime.now(timezone.utc)
        if payload.last_error:
            existing.last_error_at = datetime.now(timezone.utc)
    else:
        heartbeat = WorkerHeartbeat(
            worker_id=payload.worker_id,
            worker_type=payload.worker_type or "cdc-worker",
            connection_id=None,  # not required for heartbeat
            status=payload.status,
            events_processed=payload.events_processed,
            errors_count=payload.errors_count,
            last_error=payload.last_error,
            cpu_usage_percent=payload.cpu_usage_percent,
            memory_usage_mb=payload.memory_usage_mb,
            hostname=payload.hostname,
            pod_name=payload.pod_name,
            worker_metadata=payload.worker_metadata,
            last_heartbeat_at=datetime.now(timezone.utc),
        )
        db.add(heartbeat)

    db.commit()
    # Spec §2 (P2-4): if the worker reports idle (no active sources) for consecutive
    # heartbeats, scale down the K8s deployment replica to 0.
    if payload.status in ("idle", "stopped") and payload.pod_name:
        _maybe_scale_down_worker(pod_name=payload.pod_name, worker_metadata=payload.worker_metadata or {})
    return HeartbeatResponse(ok=True)


def _maybe_scale_down_worker(pod_name: str, worker_metadata: dict) -> None:
    """
    Spec §2 (P2-4): attempt to scale the CDC worker Deployment to 0 replicas
    via the Kubernetes API when the worker is idle.

    Only runs when KUBERNETES_SCALE_DOWN_ENABLED=true is set and the
    kubernetes Python client is available.  Errors are logged and silently
    swallowed so the heartbeat endpoint is never disrupted.
    """
    import os
    if os.environ.get("KUBERNETES_SCALE_DOWN_ENABLED", "false").lower() != "true":
        return
    deployment_name = worker_metadata.get("deployment_name") or os.environ.get("WORKER_DEPLOYMENT_NAME", "cdc-workers")
    namespace = worker_metadata.get("namespace") or os.environ.get("WORKER_NAMESPACE", "fusion")
    try:
        from kubernetes import client as k8s_client, config as k8s_config  # type: ignore
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()
        apps_v1 = k8s_client.AppsV1Api()
        apps_v1.patch_namespaced_deployment_scale(
            name=deployment_name,
            namespace=namespace,
            body={"spec": {"replicas": 0}},
        )
        import logging
        logging.getLogger(__name__).info(
            "P2-4: scaled down deployment %s/%s to 0 replicas (pod=%s idle)",
            namespace, deployment_name, pod_name,
        )
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("P2-4: scale-down failed for %s: %s", deployment_name, exc)


# ---------------------------------------------------------------------------
# Get assigned sources
# ---------------------------------------------------------------------------

@router.get(
    "/workers/{worker_id}/sources",
    response_model=List[SourceAssignment],
    summary="Get sources assigned to a worker",
)
def get_worker_sources(
    worker_id: str,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> List[SourceAssignment]:
    """
    Return all active sources whose config field contains
    {"assigned_worker_id": worker_id}.

    If no sources are assigned to this worker, returns all active sources
    (single-worker / dev mode).
    """
    # Query all non-deleted, active sources joined with their connector definition
    from sqlalchemy.orm import joinedload

    sources = (
        db.query(Source)
        .options(joinedload(Source.connector_definition))
        .filter(
            Source.is_deleted == False,
            Source.status.in_(["active", "draft"]),
        )
        .all()
    )

    # Filter to sources assigned to this worker (or return all if none are assigned)
    assigned = [
        s for s in sources
        if s.config.get("assigned_worker_id") == worker_id
    ]
    if not assigned:
        assigned = sources  # dev / single-worker mode

    # Build a mapping: source_id -> list of enabled streams
    source_ids = [s.source_id for s in assigned]
    enabled_streams = (
        db.query(Stream)
        .join(Connection, Connection.connection_id == Stream.connection_id)
        .filter(
            Connection.source_id.in_(source_ids),
            Connection.is_deleted == False,
            Connection.status == "active",
            Stream.is_enabled == True,
        )
        .with_entities(
            Connection.source_id,
            Stream.source_schema_name,
            Stream.source_table_name,
            Stream.sync_mode,
            Stream.primary_keys,
            Stream.cursor_field,
        )
        .all()
    )
    tables_by_source: Dict[str, List[AssignedTable]] = {}
    for row in enabled_streams:
        sid = str(row.source_id)
        pk = row.primary_keys
        if isinstance(pk, list):
            pk = ",".join(str(k) for k in pk)
        elif isinstance(pk, dict):
            pk = ",".join(str(k) for k in pk.keys())
        tables_by_source.setdefault(sid, []).append(
            AssignedTable(
                schema_name=row.source_schema_name or "",
                table_name=row.source_table_name or "",
                sync_mode=row.sync_mode,
                primary_key=pk,
                cursor_field=row.cursor_field,
            )
        )

    result: List[SourceAssignment] = []
    for s in assigned:
        connector_type = (
            s.connector_definition.connector_type
            if s.connector_definition else "unknown"
        )
        result.append(
            SourceAssignment(
                source_id=str(s.source_id),
                source_name=s.source_name,
                connector_type=connector_type,
                host=s.host,
                port=s.port,
                database_name=s.database_name,
                username=s.username,
                password=_decrypt_password(s.password_encrypted or ""),
                ssl_enabled=s.ssl_enabled,
                ssl_config=s.ssl_config or {},
                ssh_config=s.ssh_config or {},
                config=s.config or {},
                bank_id=str(s.bank_id),
                tenant_id=str(s.sub_tenant_id),
                assigned_tables=tables_by_source.get(str(s.source_id), []),
            )
        )
    return result


# ---------------------------------------------------------------------------
# Get routing table for worker
# ---------------------------------------------------------------------------

@router.get(
    "/workers/{worker_id}/routing",
    response_model=List[RoutingEntry],
    summary="Get routing table for a worker",
)
def get_worker_routing(
    worker_id: str,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> List[RoutingEntry]:
    """
    Return the routing table: list of (schema, table, bank_id, tenant_id, source_id)
    tuples that tell the worker which Redis stream key to use for each DB table.

    Currently returns one entry per active source with schema wildcard ("*").
    A future enhancement would store per-table routing in the DB.
    """
    from sqlalchemy.orm import joinedload

    sources = (
        db.query(Source)
        .filter(
            Source.is_deleted == False,
            Source.status.in_(["active", "draft"]),
        )
        .all()
    )

    routing: List[RoutingEntry] = []
    for s in sources:
        # Per-table routing stored in config.routing_tables (optional)
        for entry in s.config.get("routing_tables", []):
            routing.append(
                RoutingEntry(
                    schema_name=entry.get("schema_name", "*"),
                    table_name=entry.get("table_name", "*"),
                    bank_id=str(s.bank_id),
                    tenant_id=str(s.sub_tenant_id),
                    source_id=str(s.source_id),
                )
            )
        if not s.config.get("routing_tables"):
            # Default: wildcard — matches any table in this source
            routing.append(
                RoutingEntry(
                    schema_name="*",
                    table_name="*",
                    bank_id=str(s.bank_id),
                    tenant_id=str(s.sub_tenant_id),
                    source_id=str(s.source_id),
                )
            )
    return routing


# ---------------------------------------------------------------------------
# Checkpoint batch upsert
# ---------------------------------------------------------------------------

@router.post(
    "/checkpoints/batch",
    response_model=CheckpointBatchResponse,
    summary="Batch-upsert checkpoint state from worker",
)
def upsert_checkpoints(
    payload: CheckpointBatchRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> CheckpointBatchResponse:
    """
    Accept a batch of checkpoint records from a CDC worker and upsert them
    into the `checkpoint_state` table.
    """
    count = 0
    for ckpt in payload.checkpoints:
        key_filter = (
            db.query(CheckpointState)
            .filter(
                CheckpointState.worker_id == payload.worker_id,
                CheckpointState.source_id == UUID(payload.source_id),
                CheckpointState.checkpoint_data["schema_name"].as_string() == ckpt.schema_name,
                CheckpointState.checkpoint_data["table_name"].as_string() == ckpt.table_name,
            )
            .first()
        )
        if key_filter:
            key_filter.lsn = ckpt.lsn
            key_filter.checkpoint_data = {
                **key_filter.checkpoint_data,
                "schema_name": ckpt.schema_name,
                "table_name": ckpt.table_name,
                "lsn": ckpt.lsn,
            }
            key_filter.checkpoint_at = datetime.now(timezone.utc)
        else:
            db.add(
                CheckpointState(
                    source_id=UUID(payload.source_id),
                    worker_id=payload.worker_id,
                    lsn=ckpt.lsn,
                    checkpoint_data={
                        "schema_name": ckpt.schema_name,
                        "table_name": ckpt.table_name,
                        "lsn": ckpt.lsn,
                    },
                    checkpoint_at=datetime.now(timezone.utc),
                )
            )
        count += 1

    db.commit()
    # Audit hook: checkpoint.update (worker → control-plane)
    db.add(AuditLog(
        action="checkpoint.update",
        resource_type="checkpoint",
        resource_id=UUID(payload.source_id) if payload.source_id else None,
        status="success",
        details={"worker_id": payload.worker_id, "upserted": count},
    ))
    db.commit()
    return CheckpointBatchResponse(upserted=count)


# ---------------------------------------------------------------------------
# Get checkpoints for a source
# ---------------------------------------------------------------------------

@router.get(
    "/checkpoints/{source_id}",
    summary="Get latest checkpoints for a source",
)
def get_checkpoints(
    source_id: str,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> Dict[str, str]:
    """
    Return a dict of {"{schema_name}.{table_name}": lsn} for the given source_id.
    Workers call this on startup to resume from the last committed position.
    """
    try:
        source_uuid = UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source_id")

    records = (
        db.query(CheckpointState)
        .filter(CheckpointState.source_id == source_uuid)
        .order_by(CheckpointState.checkpoint_at.desc())
        .all()
    )

    result: Dict[str, str] = {}
    for r in records:
        if r.lsn:
            schema_name = r.checkpoint_data.get("schema_name", "")
            table_name = r.checkpoint_data.get("table_name", "")
            key = f"{schema_name}.{table_name}" if schema_name else str(r.checkpoint_id)
            if key not in result:
                result[key] = r.lsn
    return result


# ---------------------------------------------------------------------------
# Resync request — MongoDB ChangeStreamHistoryLost / resume-token expired
# ---------------------------------------------------------------------------

class ResyncRequest(PydanticModel):
    source_id: str
    schema_name: str
    table_name: str


class ResyncResponse(PydanticModel):
    ok: bool
    message: str


@router.post(
    "/resync-request",
    response_model=ResyncResponse,
    summary="Worker signals that a collection needs a full resync (e.g. ChangeStreamHistoryLost)",
)
def request_resync(
    payload: ResyncRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
):
    """
    Called by connectors (e.g. MongoDB) when the change stream history is lost
    and a full resync is required.

    Records a SchemaChangeEvent of type 'resync_required' in MANUAL_APPROVAL
    status so an operator can review and trigger the resync.
    Spec §1 (MongoDB): 'Handles errors like ChangeStreamHistoryLost by emitting an alert
    and requiring a resync'.
    """
    from datetime import datetime, timezone
    from uuid import UUID

    try:
        source_uuid = UUID(payload.source_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="source_id must be a valid UUID")

    ev = SchemaChangeEvent(
        source_id=source_uuid,
        table_name=payload.table_name,
        schema_name=payload.schema_name,
        change_type="resync_required",
        old_schema=None,
        new_schema={},
        schema_diff={"reason": "ChangeStreamHistoryLost — resume token no longer valid"},
        detected_at=datetime.now(timezone.utc),
        detected_by="worker",
        is_breaking=True,
        impact_assessment={"action_required": "trigger_full_resync"},
        status="pending",
    )
    db.add(ev)
    db.commit()
    log.warning(
        "Resync required for source=%s schema=%s table=%s (event_id=%s)",
        payload.source_id, payload.schema_name, payload.table_name, ev.event_id,
    )
    return ResyncResponse(ok=True, message=f"Resync event created: {ev.event_id}")


# ---------------------------------------------------------------------------
# DDL change report — MySQL / Postgres DDL events detected by workers
# ---------------------------------------------------------------------------

class DDLChangeRequest(PydanticModel):
    source_id: str
    schema_name: str
    table_name: str
    ddl_query: str  # e.g. "ALTER TABLE orders ADD COLUMN currency VARCHAR(3)"
    change_type: str = "ddl_detected"  # column_added | column_removed | type_changed | ddl_detected


class DDLChangeResponse(PydanticModel):
    ok: bool
    event_id: Optional[str] = None


@router.post(
    "/report-ddl-change",
    response_model=DDLChangeResponse,
    summary="Worker reports a DDL change detected in the database log",
)
def report_ddl_change(
    payload: DDLChangeRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
):
    """
    Called by CDC workers (MySQL/Postgres) when a DDL event is detected
    in the binary log or WAL.

    Creates a pending SchemaChangeEvent, which operators can approve/reject.
    For connections with AUTO_APPLY policy the normal report endpoint handles
    actual application; this endpoint is the low-level ingress from workers.

    Spec §1 (MySQL): 'Handles DDL events...by invalidating the schema cache
    and sending schema change notifications to the control plane.'
    Spec §1 (Postgres): 'Detects DDL messages...and notifies the control plane
    to trigger schema re-introspection.'
    """
    from datetime import datetime, timezone
    from uuid import UUID

    try:
        source_uuid = UUID(payload.source_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="source_id must be a valid UUID")

    ev = SchemaChangeEvent(
        source_id=source_uuid,
        table_name=payload.table_name,
        schema_name=payload.schema_name,
        change_type=payload.change_type,
        old_schema=None,
        new_schema={},
        schema_diff={"ddl_query": payload.ddl_query},
        detected_at=datetime.now(timezone.utc),
        detected_by="worker-ddl-detector",
        is_breaking=False,
        impact_assessment={"ddl_query": payload.ddl_query},
        status="pending",
    )
    db.add(ev)
    db.commit()
    log.info(
        "DDL change recorded for source=%s table=%s.%s event_id=%s",
        payload.source_id, payload.schema_name, payload.table_name, ev.event_id,
    )
    return DDLChangeResponse(ok=True, event_id=str(ev.event_id))


# ---------------------------------------------------------------------------
# DQ Violations ingestion — called by Spark consumer (spec §5 P5-5)
# ---------------------------------------------------------------------------

class DQViolationIngestRequest(PydanticModel):
    tenant: str
    connection_id: str
    violations: List[Dict[str, Any]]
    recorded_at: Optional[str] = None


@router.post(
    "/dq-violations",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Spark consumer reports DQ violations for audit logging",
)
def ingest_dq_violations(
    payload: DQViolationIngestRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
):
    """
    Persists DQ violations to audit_logs so they are queryable alongside other audit events.
    Spec §5 (P5-5): 'DQ violations must be written to the audit log, not only emitted to Prometheus.'
    """
    from datetime import datetime, timezone

    recorded_at = datetime.now(timezone.utc)
    for v in payload.violations:
        entry = AuditLog(
            username=f"spark-consumer/{payload.tenant}",
            action="dq.violation",
            resource_type="connection",
            resource_id=payload.connection_id,
            status="success",
            details={
                "tenant": payload.tenant,
                "rule_type": v.get("rule_type"),
                "column": v.get("column"),
                "message": v.get("message"),
                "recorded_at": (payload.recorded_at or recorded_at.isoformat()),
                **{k: v2 for k, v2 in v.items() if k not in ("rule_type", "column", "message")},
            },
        )
        db.add(entry)

    db.commit()
    log.info(
        "Persisted %d DQ violations for connection=%s tenant=%s",
        len(payload.violations), payload.connection_id, payload.tenant,
    )
    return {"ok": True, "persisted": len(payload.violations)}


# ---------------------------------------------------------------------------
# Run-complete callback — called by Airflow after Spark batch job (spec §5 P5-7)
# ---------------------------------------------------------------------------

class RunCompleteRequest(PydanticModel):
    connection_id: str
    status: str = "success"    # "success" | "failed"
    rows_synced: int = 0
    dag_run_id: Optional[str] = None
    error_message: Optional[str] = None


@router.post(
    "/connections/{connection_id}/run-complete",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Airflow DAG reports that a batch CDC job has completed",
)
def run_complete(
    connection_id: str,
    payload: RunCompleteRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
):
    """
    Called by the Airflow `notify_run_complete` task after the Spark batch job finishes.
    Updates the Connection status and records a run-history entry.

    Spec §5 (P5-7): 'After Spark job completes, call back to control plane to update
    connection run status and last-successful-sync timestamp.'
    """
    from datetime import datetime, timezone

    try:
        conn_uuid = UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="connection_id must be a valid UUID")

    from app.models.connection import Connection as Conn
    conn = db.query(Conn).filter(Conn.connection_id == conn_uuid,
                                  Conn.is_deleted.is_(False)).first()
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    now = datetime.now(timezone.utc)
    job_status = payload.status

    # Mark initial load complete on first successful run
    if job_status == "success" and not conn.initial_load_completed:
        conn.initial_load_completed = True
        conn.initial_load_completed_at = now

    # Write audit entry
    audit = AuditLog(
        username="airflow",
        action=f"connection.batch_run.{job_status}",
        resource_type="connection",
        resource_id=str(conn_uuid),
        status="success" if job_status == "success" else "error",
        details={
            "rows_synced": payload.rows_synced,
            "dag_run_id": payload.dag_run_id,
            "error_message": payload.error_message,
        },
    )
    db.add(audit)
    db.commit()

    log.info(
        "run-complete for connection=%s status=%s rows=%d",
        connection_id, job_status, payload.rows_synced,
    )
    return {"ok": True, "status": job_status}


# ---------------------------------------------------------------------------
# Transform-route resolver — used by the cdc-worker to bridge CDC events into
# the transform-worker's Redis list queue (fusion:transforms:normal).
# See cdc_worker/transform_bridge.py and transform-worker/worker.py.
# ---------------------------------------------------------------------------

class TransformRoute(PydanticModel):
    connection_id: str
    destination: Dict[str, Any]
    dest_schema: str
    dest_table: str
    primary_key: str
    transform_steps: List[Dict[str, Any]] = []


@router.get(
    "/workers/{worker_id}/transform-route/{source_id}/{schema_name}/{table_name}",
    response_model=List[TransformRoute],
    summary="Resolve CDC event → transform-worker task route",
)
def get_transform_route(
    worker_id: str,
    source_id: str,
    schema_name: str,
    table_name: str,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> List[TransformRoute]:
    """
    For a given (source_id, schema, table), return the list of active
    Connection→Destination→Stream routes that the cdc-worker should bridge
    into ``fusion:transforms:normal`` as ``cdc_transform`` tasks.

    The transform-worker's ``CDCTransformTask`` consumes these and writes to
    Postgres or Iceberg. This closes the architectural gap where the
    cdc-worker published to Redis Streams (``cdc:*``) but the transform-worker
    only reads from Redis lists (``fusion:transforms:*``).
    """
    from sqlalchemy.orm import joinedload
    from app.models.source_destination import Destination
    from app.models.connection import Connection, Stream

    try:
        src_uuid = UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source_id")

    # Active connections for this source, with their enabled streams matching
    # the requested (schema, table). A wildcard schema ("") matches any.
    rows = (
        db.query(Connection, Stream, Destination)
        .join(Stream, Stream.connection_id == Connection.connection_id)
        .join(Destination, Destination.destination_id == Connection.destination_id)
        .filter(
            Connection.source_id == src_uuid,
            Connection.is_deleted == False,
            Connection.status == "active",
            Stream.is_enabled == True,
            Stream.source_table_name == table_name,
        )
        .all()
    )
    # Filter by schema (allow "" / None as wildcard)
    routes: List[TransformRoute] = []
    for conn, stream, dest in rows:
        s_schema = (stream.source_schema_name or "")
        if schema_name and s_schema and s_schema != schema_name:
            continue
        pk = stream.primary_keys
        if isinstance(pk, list):
            pk_str = ",".join(str(k) for k in pk) if pk else "id"
        elif isinstance(pk, dict):
            pk_str = ",".join(str(k) for k in pk.keys()) if pk else "id"
        else:
            pk_str = str(pk) if pk else "id"
        # transform_overrides shape: {"transforms": [{...}, ...]}
        to = stream.transform_overrides or {}
        steps = to.get("transforms", []) if isinstance(to, dict) else []
        # Build a connection_config copy with the decrypted plaintext password
        # so the transform-worker can derive a usable Postgres DSN without
        # needing the Fernet key. The stored row keeps password_encrypted only;
        # the plaintext never leaves this response (sent over the internal
        # worker API, authenticated via X-Worker-Token).
        dest_config = dict(dest.connection_config or {})
        enc = dest_config.get("password_encrypted")
        if enc and not dest_config.get("password"):
            try:
                dest_config["password"] = _decrypt_password(enc)
            except Exception:
                # If decryption fails, leave password empty — the
                # transform-worker will surface a connection error rather
                # than crash here.
                pass
        routes.append(TransformRoute(
            connection_id=str(conn.connection_id),
            destination={
                "connector_type": (dest.connector_definition.connector_type
                                   if dest.connector_definition else "postgres"),
                "connection_config": dest_config,
            },
            dest_schema=stream.destination_schema_name or "dw",
            dest_table=stream.destination_table_name or table_name,
            primary_key=pk_str,
            transform_steps=steps,
        ))
    return routes


# ---------------------------------------------------------------------------
# Initial-load checkpoint upsert — called by the transform-worker after each
# InitialLoadTask chunk completes (loader.py InitialLoadTask._mark_chunk_done).
# The previous worker implementation called a non-existent /internal/load-checkpoints
# endpoint and 404'd silently; this minimal implementation upserts into the
# initial_load_checkpoints table keyed by (connection_id, stream_id).
# See Gap 3 in the v1.2.16 release notes.
# ---------------------------------------------------------------------------

class LoadCheckpointRequest(PydanticModel):
    connection_id: str
    stream_id: Optional[str] = None
    source_table: Optional[str] = None
    chunk_seq: int = 0
    rows_written: int = 0
    last_pk: Optional[Any] = None
    state: str = "done"   # "done" | "running" | "failed"


class LoadCheckpointResponse(PydanticModel):
    ok: bool
    upserted: int


@router.post(
    "/load-checkpoints",
    response_model=LoadCheckpointResponse,
    summary="Transform-worker reports initial-load chunk progress",
)
def upsert_load_checkpoint(
    payload: LoadCheckpointRequest,
    db: Session = Depends(get_db),
    _token: str = Depends(_verify_worker_token),
) -> LoadCheckpointResponse:
    """Upsert a row into ``initial_load_checkpoints`` for the given
    (connection_id, stream_id) pair.

    If a row already exists for the stream it is updated (rows_written is
    incremented, status is set to the reported state); otherwise a new row is
    inserted. This lets the transform-worker's InitialLoadTask report chunk
    progress without needing a separate data-proxy round-trip.
    """
    from datetime import datetime, timezone
    from uuid import UUID
    from app.models.monitoring import InitialLoadCheckpoint

    try:
        conn_uuid = UUID(payload.connection_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail="connection_id must be a valid UUID")

    stream_uuid = None
    if payload.stream_id:
        try:
            stream_uuid = UUID(payload.stream_id)
        except (ValueError, TypeError):
            stream_uuid = None

    table = (payload.source_table or "").strip() or "unknown"

    existing = None
    if stream_uuid is not None:
        existing = (
            db.query(InitialLoadCheckpoint)
            .filter(
                InitialLoadCheckpoint.connection_id == conn_uuid,
                InitialLoadCheckpoint.stream_id == stream_uuid,
            )
            .first()
        )

    status = "completed" if payload.state == "done" else payload.state
    now = datetime.now(timezone.utc)

    if existing is not None:
        existing.rows_written = (existing.rows_written or 0) + payload.rows_written
        existing.status = status
        if status == "completed":
            existing.completed_at = now
        db.commit()
        return LoadCheckpointResponse(ok=True, upserted=1)

    db.add(InitialLoadCheckpoint(
        connection_id=conn_uuid,
        stream_id=stream_uuid or UUID("00000000-0000-0000-0000-000000000000"),
        source_table=table,
        rows_written=payload.rows_written,
        status=status,
        started_at=now,
        completed_at=now if status == "completed" else None,
    ))
    db.commit()
    return LoadCheckpointResponse(ok=True, upserted=1)

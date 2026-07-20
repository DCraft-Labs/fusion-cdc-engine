"""Monitoring API endpoints"""

import json
import math
import ssl
import urllib.request
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.config import settings
from app.database import get_db
from app.models.monitoring import (
    CDCLagMetrics,
    CheckpointState,
    WorkerHeartbeat,
)

_K8S_API = "https://kubernetes.default.svc.cluster.local"
_SA_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_SA_CA = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
_K8S_NAMESPACE = "fusion"


def _k8s_get(path: str) -> dict:
    """Call the Kubernetes API using the pod's service account credentials."""
    try:
        with open(_SA_TOKEN) as f:
            token = f.read().strip()
        ctx = ssl.create_default_context(cafile=_SA_CA)
        req = urllib.request.Request(
            f"{_K8S_API}{path}",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, context=ctx, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"error": str(exc)}

router = APIRouter()


# ============================================================================
# Helpers
# ============================================================================

def _check_redis() -> bool:
    """Try to ping Redis. Returns True if reachable."""
    try:
        import redis as redis_lib  # imported lazily to avoid hard dep at startup
        r = redis_lib.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        r.ping()
        return True
    except Exception:
        return False


def _check_db(db: Session) -> bool:
    """Try a trivial DB query. Returns True if reachable."""
    try:
        db.execute(
            __import__("sqlalchemy").text("SELECT 1")
        )
        return True
    except Exception:
        return False


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/health")
async def system_health(
    db: Session = Depends(get_db),
):
    """System health check — pings DB and Redis."""
    db_healthy = _check_db(db)
    redis_healthy = _check_redis()
    overall = "healthy" if (db_healthy and redis_healthy) else "degraded"

    return {
        "status": overall,
        "services": {
            "database": "healthy" if db_healthy else "unhealthy",
            "redis": "healthy" if redis_healthy else "unhealthy",
        },
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/connections/{connection_id}/health")
async def connection_health(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Health summary for a specific connection — latest heartbeat + lag."""
    latest_heartbeat = (
        db.query(WorkerHeartbeat)
        .filter(WorkerHeartbeat.connection_id == connection_id)
        .order_by(WorkerHeartbeat.last_heartbeat_at.desc())
        .first()
    )
    latest_lag = (
        db.query(CDCLagMetrics)
        .filter(CDCLagMetrics.connection_id == connection_id)
        .order_by(CDCLagMetrics.measured_at.desc())
        .first()
    )

    return {
        "connection_id": str(connection_id),
        "status": latest_heartbeat.status if latest_heartbeat else "no_worker",
        "last_heartbeat_at": (
            latest_heartbeat.last_heartbeat_at.isoformat() if latest_heartbeat else None
        ),
        "lag_seconds": latest_lag.lag_seconds if latest_lag else None,
        "lag_events": latest_lag.lag_events if latest_lag else None,
    }


@router.get("/connections/{connection_id}/lag")
async def connection_lag(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Latest CDC lag metrics for a connection."""
    latest = (
        db.query(CDCLagMetrics)
        .filter(CDCLagMetrics.connection_id == connection_id)
        .order_by(CDCLagMetrics.measured_at.desc())
        .first()
    )
    if not latest:
        return {
            "connection_id": str(connection_id),
            "lag_seconds": None,
            "lag_events": None,
            "lag_bytes": None,
            "events_per_second": None,
            "measured_at": None,
        }

    return {
        "connection_id": str(connection_id),
        "lag_seconds": latest.lag_seconds,
        "lag_events": latest.lag_events,
        "lag_bytes": float(latest.lag_bytes),
        "events_per_second": float(latest.events_per_second),
        "measured_at": latest.measured_at.isoformat(),
    }


@router.get("/connections/{connection_id}/throughput")
async def connection_throughput(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Event throughput for a connection (latest sample)."""
    latest = (
        db.query(CDCLagMetrics)
        .filter(CDCLagMetrics.connection_id == connection_id)
        .order_by(CDCLagMetrics.measured_at.desc())
        .first()
    )
    return {
        "connection_id": str(connection_id),
        "events_per_second": float(latest.events_per_second) if latest else 0.0,
        "bytes_per_second": float(latest.bytes_per_second) if latest else 0.0,
        "measured_at": latest.measured_at.isoformat() if latest else None,
    }


@router.get("/connections/{connection_id}/checkpoints")
async def list_checkpoints(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List checkpoint states for a connection's source."""
    checkpoints = (
        db.query(CheckpointState)
        .filter(CheckpointState.source_id == connection_id)
        .order_by(CheckpointState.checkpoint_at.desc())
        .all()
    )
    return {
        "checkpoints": [
            {
                "checkpoint_id": str(c.checkpoint_id),
                "source_id": str(c.source_id),
                "stream_id": str(c.stream_id) if c.stream_id else None,
                "worker_id": c.worker_id,
                "binlog_file": c.binlog_file,
                "binlog_position": c.binlog_position,
                "lsn": c.lsn,
                "resume_token": c.resume_token,
                "checkpoint_data": c.checkpoint_data,
                "checkpoint_at": c.checkpoint_at.isoformat(),
                "records_processed": c.records_processed,
            }
            for c in checkpoints
        ]
    }


@router.get("/workers")
async def list_workers(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List all active worker heartbeats (tenant-scoped via connection_id FK not yet enforced)."""
    workers = (
        db.query(WorkerHeartbeat)
        .order_by(WorkerHeartbeat.last_heartbeat_at.desc())
        .all()
    )
    return {
        "workers": [
            {
                "heartbeat_id": str(w.heartbeat_id),
                "worker_id": w.worker_id,
                "worker_type": w.worker_type,
                "connection_id": str(w.connection_id),
                "status": w.status,
                "last_heartbeat_at": w.last_heartbeat_at.isoformat(),
                "hostname": w.hostname,
                "pod_name": w.pod_name,
                "events_processed": w.events_processed,
                "errors_count": w.errors_count,
                "last_error": w.last_error,
            }
            for w in workers
        ]
    }


@router.get("/resource-usage")
async def resource_usage(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Aggregate resource usage across workers."""
    result = db.query(
        func.sum(WorkerHeartbeat.cpu_usage_percent).label("total_cpu"),
        func.sum(WorkerHeartbeat.memory_usage_mb).label("total_memory_mb"),
        func.count(WorkerHeartbeat.worker_id).label("worker_count"),
    ).first()

    return {
        "worker_count": result.worker_count or 0,
        "total_cpu_percent": float(result.total_cpu or 0),
        "total_memory_mb": int(result.total_memory_mb or 0),
    }



@router.get("/pods")
async def list_pods(
    user=Depends(get_current_user),
):
    """List all pods in the fusion namespace via the k8s API."""
    data = _k8s_get(f"/api/v1/namespaces/{_K8S_NAMESPACE}/pods")
    if "error" in data:
        # Return empty list — k8s API not reachable (e.g. running outside k8s)
        return {"pods": [], "error": data["error"]}
    pods = []
    for item in data.get("items", []):
        meta = item.get("metadata", {})
        status_obj = item.get("status", {})
        containers = status_obj.get("containerStatuses", [])
        ready = all(c.get("ready", False) for c in containers) if containers else False
        phase = status_obj.get("phase", "Unknown")
        start_time = meta.get("creationTimestamp")
        pods.append({
            "name": meta.get("name"),
            "phase": phase,
            "ready": ready,
            "start_time": start_time,
            "node": item.get("spec", {}).get("nodeName"),
            "restart_count": sum(c.get("restartCount", 0) for c in containers),
        })
    return {"pods": pods}


@router.get("/logs/{pod_name}")
async def get_pod_logs(
    pod_name: str,
    lines: int = Query(100, ge=1, le=1000),
    user=Depends(get_current_user),
):
    """Fetch the last N log lines from a pod in the fusion namespace."""
    # k8s log endpoint returns plain text, NOT JSON — use raw fetch directly
    try:
        with open(_SA_TOKEN) as f:
            token = f.read().strip()
        ctx = ssl.create_default_context(cafile=_SA_CA)
        url = (
            f"{_K8S_API}/api/v1/namespaces/{_K8S_NAMESPACE}/pods/{pod_name}/log"
            f"?tailLines={lines}&timestamps=true"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        log_lines = [line for line in raw.splitlines() if line.strip()]
        return {"pod": pod_name, "lines": log_lines}
    except FileNotFoundError:
        return {"pod": pod_name, "lines": [], "error": "Service account token not found — not running inside a Kubernetes pod"}
    except Exception as exc:
        return {"pod": pod_name, "lines": [], "error": str(exc)}

"""Resource admission-control API.

Two router objects (both included by ``main.py``, mounted at different
prefixes, following this codebase's multiple-routers-per-prefix pattern —
see how ``dlq`` and ``internal`` sit alongside ``connections`` in main.py):

  ``router``            -> mounted at ``/api/v1/resource-config``
      GET    /                        current bank's pool config (or the
                                       "unconfigured" placeholder)
      PUT    /                        create-or-update (admin) — the
                                       one-time setup step
      DELETE /                        reset back to unconfigured (admin)

  ``admission_router``   -> mounted at ``/api/v1/connections`` (alongside
                             ``connections.router``, NOT inside it — keeps
                             this diff fully separated from the other
                             agents' work in connections.py)
      POST   /{connection_id}/admission-preview
      POST   /{connection_id}/admission-confirm

See ``app/schemas/resource_config.py`` for the exact request/response
field names and types.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.auth.dependencies import get_current_user, require_permission
from app.models.auth import User
from app.models.connection import Connection, Stream
from app.models.source_destination import Source, Destination
from app.models.resource_config import ResourceConfig
from app.schemas.resource_config import (
    ResourceConfigUpsert,
    ResourceConfigResponse,
    AdmissionPreviewRequest,
    AdmissionPreviewResponse,
    AdmissionModeOption,
    AdmissionConfirmRequest,
    AdmissionConfirmResponse,
)
from app.services import resource_admission
from app.services import resource_ledger

log = logging.getLogger(__name__)

router = APIRouter()
admission_router = APIRouter()


# ===========================
# Resource Config CRUD
# ===========================

def _get_resource_config(db: Session, user: User) -> Optional[ResourceConfig]:
    return (
        db.query(ResourceConfig)
        .filter(ResourceConfig.bank_id == user.bank_id)
        .first()
    )


@router.get("", response_model=ResourceConfigResponse)
async def get_resource_config(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Any authenticated user can read this — the frontend's first-login
    gate needs it on every screen, not just an admin's."""
    cfg = _get_resource_config(db, current_user)
    if not cfg:
        return ResourceConfigResponse(resource_configured=False)
    return ResourceConfigResponse.model_validate(cfg)


@router.put("", response_model=ResourceConfigResponse)
async def upsert_resource_config(
    payload: ResourceConfigUpsert,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("admin")),
):
    """Create-or-update the calling admin's bank-scoped resource pool
    config. Always sets ``resource_configured=True`` — this endpoint IS the
    "admin configures, once, at first login" step; calling it again later
    is an intentional resize, not a re-onboarding."""
    cfg = _get_resource_config(db, current_user)
    if cfg is None:
        cfg = ResourceConfig(bank_id=current_user.bank_id, created_by=current_user.user_id)
        db.add(cfg)

    cfg.total_cpu_min_millis = payload.total_cpu_min_millis
    cfg.total_cpu_max_millis = payload.total_cpu_max_millis
    cfg.total_memory_min_mi = payload.total_memory_min_mi
    cfg.total_memory_max_mi = payload.total_memory_max_mi
    cfg.instance_type = payload.instance_type
    cfg.instance_count = payload.instance_count
    cfg.pool_scope = payload.pool_scope
    cfg.resource_configured = True

    db.commit()
    db.refresh(cfg)
    return ResourceConfigResponse.model_validate(cfg)


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def reset_resource_config(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("admin")),
):
    """Resets the bank's pool config back to unconfigured (deletes the row
    entirely — the frontend gate then re-blocks every screen until the
    admin re-runs the one-time setup)."""
    cfg = _get_resource_config(db, current_user)
    if cfg:
        db.delete(cfg)
        db.commit()


# ===========================
# Admission preview / confirm
# ===========================

def _require_configured_pool(db: Session, current_user: User) -> ResourceConfig:
    cfg = _get_resource_config(db, current_user)
    if not cfg or not cfg.resource_configured:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Resource pool is not configured yet — set it up under Resource Config first.",
        )
    return cfg


def _get_connection_for_admission(db: Session, connection_id: UUID, user: User) -> Connection:
    conn = (
        db.query(Connection)
        .filter(
            Connection.connection_id == connection_id,
            Connection.sub_tenant_id == user.sub_tenant_id,
            Connection.is_deleted == False,  # noqa: E712
        )
        .first()
    )
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection {connection_id} not found")
    return conn


def _rows_estimated_total(db: Session, connection: Connection, stream_ids: Optional[list]) -> Optional[int]:
    """Sums a density-based rows_estimated across the connection's selected
    (enabled) streams, reusing ``partition_with_estimates`` (control-plane's
    existing row-estimation logic — see ``connections._enqueue_initial_load_tasks``
    for the precedent this mirrors) rather than reinventing estimation.

    Uses a fixed k=2 purely to reach the estimating code path in
    ``partition_with_estimates`` (k<=1 short-circuits with no estimate at
    all) — this k is NOT the mode's eventual parallelism, just a probe.
    """
    from app.api.sources import _decrypt_password
    from app.services.partitioning import partition_with_estimates

    source = db.query(Source).filter(Source.source_id == connection.source_id).first()
    if not source:
        return None

    query = db.query(Stream).filter(
        Stream.connection_id == connection.connection_id,
        Stream.is_enabled == True,  # noqa: E712
    )
    if stream_ids:
        query = query.filter(Stream.stream_id.in_(stream_ids))
    streams = query.all()
    if not streams:
        return None

    src_connector_type = "postgres"
    if source.connector_definition:
        src_connector_type = source.connector_definition.connector_type

    src_pw = ""
    if source.password_encrypted:
        try:
            src_pw = _decrypt_password(source.password_encrypted)
        except Exception:
            pass
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

    total = 0
    any_estimate = False
    for stream in streams:
        pk = stream.primary_keys
        if isinstance(pk, list):
            pk_str = ",".join(str(kc) for kc in pk) if pk else "id"
        elif isinstance(pk, dict):
            pk_str = ",".join(str(kc) for kc in pk.keys()) if pk else "id"
        else:
            pk_str = str(pk) if pk else "id"
        pk_col = str(pk_str).split(",")[0].strip() or "id"
        if src_connector_type == "mongodb":
            pk_col = "_id"
        try:
            parts = partition_with_estimates(
                source_block, stream.source_schema_name or "",
                stream.source_table_name, pk_col, src_connector_type, 2,
            )
            stream_rows = sum((p.get("rows_estimated") or 0) for p in parts)
            if stream_rows:
                any_estimate = True
            total += stream_rows
        except Exception:
            log.warning(
                "admission: rows-estimate failed for connection=%s stream=%s — continuing without it",
                connection.connection_id, stream.stream_id, exc_info=True,
            )
    return total if any_estimate else None


def _build_mode_options(
    tier: str,
    rows_estimated_total: Optional[int],
    available_cpu_millis: int,
    available_memory_mi: int,
) -> list[AdmissionModeOption]:
    options = []
    for mode in resource_admission.MODES:
        req = resource_admission.mode_resource_requirement(tier, mode, rows_estimated_total)
        eta_seconds, bulk_mode = resource_admission.estimate_eta_seconds(rows_estimated_total, req["parallelism"])
        fits = req["cpu_millis"] <= available_cpu_millis and req["memory_mi"] <= available_memory_mi
        reason = None
        if not fits:
            if req["cpu_millis"] > available_cpu_millis and req["memory_mi"] > available_memory_mi:
                reason = "insufficient_capacity"
            elif req["cpu_millis"] > available_cpu_millis:
                reason = "insufficient_cpu"
            else:
                reason = "insufficient_memory"
        options.append(AdmissionModeOption(
            mode=mode,
            fits=fits,
            reason=reason,
            eta_seconds=eta_seconds,
            parallelism=req["parallelism"],
            cpu_millis=req["cpu_millis"],
            memory_mi=req["memory_mi"],
            bulk_mode=bulk_mode,
        ))
    return options


@admission_router.post("/{connection_id}/admission-preview", response_model=AdmissionPreviewResponse)
async def admission_preview(
    connection_id: UUID,
    payload: AdmissionPreviewRequest = AdmissionPreviewRequest(),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:read")),
):
    """Given the connection's selected tables (after the table-selection
    step in the create wizard), returns the tier, an ETA per speed mode,
    and which modes actually fit currently-available ledger capacity.

    Only ``available_modes`` should be selectable in the UI; ``modes``
    includes the non-fitting ones too (with a ``reason``) so the UI can
    show them disabled with an explanation rather than just hiding them.
    """
    cfg = _require_configured_pool(db, current_user)
    connection = _get_connection_for_admission(db, connection_id, current_user)

    rows_estimated_total = _rows_estimated_total(db, connection, payload.stream_ids)
    tier = resource_admission.resolve_tier(rows_estimated_total)

    capacity = resource_ledger.get_available_capacity(cfg.total_cpu_max_millis, cfg.total_memory_max_mi)
    avail_cpu = capacity["available_cpu_millis"]
    avail_mem = capacity["available_memory_mi"]

    modes = _build_mode_options(tier, rows_estimated_total, avail_cpu, avail_mem)
    available_modes = [m.mode for m in modes if m.fits]

    return AdmissionPreviewResponse(
        connection_id=connection_id,
        rows_estimated_total=rows_estimated_total,
        tier=tier,
        modes=modes,
        available_modes=available_modes,
        available_cpu_millis=avail_cpu,
        available_memory_mi=avail_mem,
    )


@admission_router.post("/{connection_id}/admission-confirm", response_model=AdmissionConfirmResponse)
async def admission_confirm(
    connection_id: UUID,
    payload: AdmissionConfirmRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("connections:create")),
):
    """Atomically reserves the transient initial-load capacity for the
    chosen mode (Redis Lua EVAL — check-then-reserve in one step, so two
    concurrent confirms racing for the same last sliver of capacity can't
    both win). On success, also stamps ``resource_limits.parallelism``
    (and an ``admission`` bookkeeping block) onto the connection so the
    EXISTING ``_connection_parallelism()`` mechanism in connections.py picks
    up saver mode's K=1 (or normal/aggressive's K) with no separate/
    competing mechanism.
    """
    cfg = _require_configured_pool(db, current_user)
    connection = _get_connection_for_admission(db, connection_id, current_user)

    rows_estimated_total = _rows_estimated_total(db, connection, payload.stream_ids)
    tier = resource_admission.resolve_tier(rows_estimated_total)
    req = resource_admission.mode_resource_requirement(tier, payload.mode, rows_estimated_total)
    eta_seconds, bulk_mode = resource_admission.estimate_eta_seconds(rows_estimated_total, req["parallelism"])

    try:
        result = resource_ledger.reserve_transient(
            str(connection_id),
            req["cpu_millis"],
            req["memory_mi"],
            cfg.total_cpu_max_millis,
            cfg.total_memory_max_mi,
        )
    except resource_ledger.LedgerUnavailableError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Resource ledger unavailable — cannot safely confirm a reservation right now",
        )

    if not result["reserved"]:
        return AdmissionConfirmResponse(
            connection_id=connection_id,
            reserved=False,
            reason="insufficient_capacity",
            available_cpu_millis=result["available_cpu_millis"],
            available_memory_mi=result["available_memory_mi"],
        )

    # Integrate with the EXISTING parallelism mechanism (connections.py
    # _connection_parallelism reads resource_limits["parallelism"]) instead
    # of adding a second, competing K knob.
    rl = dict(connection.resource_limits or {})
    rl["parallelism"] = req["parallelism"]
    rl["admission"] = {
        "mode": payload.mode,
        "tier": tier,
        "rows_estimated_total": rows_estimated_total,
        "cpu_reserved_millis": req["cpu_millis"],
        "memory_reserved_mi": req["memory_mi"],
        "eta_seconds": eta_seconds,
        "bulk_mode": bulk_mode,
    }
    connection.resource_limits = rl
    db.commit()

    return AdmissionConfirmResponse(
        connection_id=connection_id,
        reserved=True,
        mode=payload.mode,
        tier=tier,
        parallelism=req["parallelism"],
        cpu_reserved_millis=req["cpu_millis"],
        memory_reserved_mi=req["memory_mi"],
        eta_seconds=eta_seconds,
        available_cpu_millis=result["available_cpu_millis"],
        available_memory_mi=result["available_memory_mi"],
    )

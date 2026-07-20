"""Schema Evolution API endpoints"""

import logging
import math
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.config import settings
from app.database import get_db
from app.models.auth import AuditLog
from app.models.connection import Connection
from app.models.schema_evolution import JSONFlattenRule, JSONSchemaCache, SchemaChangeEvent
from app.models.transformation import TransformPipeline

router = APIRouter()
log = logging.getLogger(__name__)


# ============================================================================
# Pydantic schemas (inline — schema evolution is small)
# ============================================================================

class SchemaChangeResponse(BaseModel):
    event_id: UUID
    source_id: UUID
    stream_id: Optional[UUID]
    table_name: str
    schema_name: Optional[str]
    change_type: str
    old_schema: Optional[dict]
    new_schema: dict
    schema_diff: dict
    detected_at: datetime
    detected_by: str
    status: str
    reviewed_at: Optional[datetime]
    reviewed_by: Optional[UUID]
    review_notes: Optional[str]
    is_breaking: bool
    applied_at: Optional[datetime]

    class Config:
        from_attributes = True


class ReviewRequest(BaseModel):
    review_notes: Optional[str] = None


class SchemaChangeCreate(BaseModel):
    """Payload sent by CDC workers or introspection jobs when a schema change is detected."""
    table_name: str
    schema_name: Optional[str] = None
    change_type: str   # column_added | column_removed | type_changed | table_added | table_removed
    old_schema: Optional[dict] = None
    new_schema: dict
    schema_diff: dict
    detected_by: str = "worker"
    is_breaking: bool = False
    stream_id: Optional[UUID] = None


class JSONFlattenRuleCreate(BaseModel):
    source_column: str
    flatten_strategy: str  # full | selective | prefix
    target_columns: list = []
    json_path_expressions: dict = {}
    column_prefix: Optional[str] = None
    separator: str = "_"


class JSONFlattenRuleResponse(BaseModel):
    rule_id: UUID
    stream_id: UUID
    source_column: str
    flatten_strategy: str
    target_columns: list
    json_path_expressions: dict
    column_prefix: Optional[str]
    separator: str
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# Helpers
# ============================================================================

def _get_change_event(db: Session, connection_id: UUID, change_id: UUID) -> SchemaChangeEvent:
    """Fetch change event; use source_id == connection_id as join key."""
    ev = (
        db.query(SchemaChangeEvent)
        .filter(
            SchemaChangeEvent.event_id == change_id,
            SchemaChangeEvent.source_id == connection_id,
        )
        .first()
    )
    if not ev:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schema change event {change_id} not found for connection {connection_id}",
        )
    return ev


def _audit(
    db: Session,
    action: str,
    resource_type: str,
    resource_id: str,
    user=None,
    details: Optional[dict] = None,
) -> None:
    """Write a record to audit_logs (spec §4 — immutable audit trail)."""
    try:
        entry = AuditLog(
            user_id=str(user.user_id) if user else None,
            username=getattr(user, "username", None),
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            status="success",
            details=details or {},
        )
        db.add(entry)
        db.flush()   # don't call commit here — caller owns the transaction
    except Exception as exc:
        log.warning("Failed to write audit log for action=%s: %s", action, exc)


def _is_compatible_type_change(old_type: str, new_type: str) -> bool:
    """
    Spec §4 (P4-4): Determine if a column type change is backward-compatible.
    Compatible changes can be AUTO_APPLied; breaking changes require MANUAL_APPROVAL.

    Compatible (widening) changes:
      int → bigint, float, double, numeric, varchar, text
      float → double, numeric, varchar, text
      varchar(N) → varchar(M>N), text
      date → timestamp, varchar, text
      any → text / varchar (coercion)

    Breaking (narrowing / incompatible) changes:
      varchar → int, float, bool (data loss risk)
      float → int (truncation)
      timestamp → date (precision loss)
      text → int/float/bool
    """
    if not old_type or not new_type:
        return True  # Unknown — let AUTO_APPLY proceed; Spark will surface errors

    def _norm(t: str) -> str:
        return t.lower().split("(")[0].strip()

    old = _norm(old_type)
    new = _norm(new_type)

    if old == new:
        return True

    # Always compatible — widening to text/varchar
    if new in ("text", "varchar", "character varying", "clob"):
        return True

    # Numeric widening
    NUMERIC_WIDTH = {"tinyint": 1, "smallint": 2, "int": 3, "integer": 3,
                     "bigint": 4, "float": 5, "double": 6, "real": 5,
                     "numeric": 7, "decimal": 7}
    if old in NUMERIC_WIDTH and new in NUMERIC_WIDTH:
        return NUMERIC_WIDTH[new] >= NUMERIC_WIDTH[old]

    # Date/time widening
    if old == "date" and new in ("timestamp", "timestamptz", "datetime"):
        return True

    # Everything else is potentially breaking
    return False


def _notify_spark_schema_reload(connection_id: str, change_id: str) -> None:
    """
    Send a best-effort POST to the Spark consumer's schema-reload webhook.

    Spec §3 — Schema Change Notification Flow:
      For AUTO_APPLY or after MANUAL_APPROVAL: send a webhook to the Spark
      consumer to reload its schema and update transformation logic.

    Failures are logged but do not fail the API request — the consumer
    polls the control plane periodically as a fallback.
    """
    spark_url = settings.SPARK_CONSUMER_URL or None
    if not spark_url:
        log.debug("SPARK_CONSUMER_URL not configured — skipping schema reload notification")
        return

    url = f"{spark_url}/internal/schema-reload"
    payload = {"connection_id": connection_id, "change_id": change_id}
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.post(url, json=payload)
            if resp.status_code < 300:
                log.info(
                    "Schema reload notification sent to Spark consumer: connection=%s change=%s",
                    connection_id,
                    change_id,
                )
            else:
                log.warning(
                    "Spark consumer schema reload returned %s for connection=%s",
                    resp.status_code,
                    connection_id,
                )
    except Exception as exc:
        log.warning(
            "Could not notify Spark consumer of schema change (connection=%s): %s",
            connection_id,
            exc,
        )


def _apply_schema_change_to_pipeline(db: Session, connection_id: UUID, ev: SchemaChangeEvent) -> None:
    """
    Spec §4 (PDF3/PDF4): When a schema change is approved or auto-applied, update the
    connection's TransformPipeline spec so the new column / type change is reflected.

    Handles:
      - column_added   → appends a cast (identity) step to transformation_code JSON
      - column_removed → removes steps for that column; marks deprecated in input_streams
      - type_changed   → updates the cast step's to_type
    """
    import json as _json

    connection = db.query(Connection).filter(Connection.connection_id == connection_id).first()
    if not connection or not connection.transform_pipeline_id:
        return

    pipeline = db.query(TransformPipeline).filter(
        TransformPipeline.pipeline_id == connection.transform_pipeline_id,
        TransformPipeline.is_deleted.is_(False),
    ).first()
    if not pipeline:
        return

    try:
        spec = _json.loads(pipeline.transformation_code) if pipeline.transformation_code else {}
    except (ValueError, TypeError):
        return  # Non-JSON pipeline (SQL/Python text) — cannot auto-patch

    transforms = spec.get("transforms", [])
    change_type = ev.change_type or ""
    col_name = (ev.schema_diff or {}).get("column_name") or ""
    if not col_name:
        return

    if change_type == "column_added":
        new_col_def = ev.new_schema or {}
        data_type = new_col_def.get("data_type", "string") if isinstance(new_col_def, dict) else "string"
        step_id = f"auto_cast_{col_name}"
        if not any(s.get("id") == step_id for s in transforms):
            transforms.append({
                "id": step_id,
                "type": "cast",
                "column": col_name,
                "to_type": data_type,
                "output_column": col_name,
                "_auto_generated": True,
            })
            log.info("AUTO: added cast step for new column '%s' in pipeline %s",
                     col_name, pipeline.pipeline_id)

    elif change_type == "column_removed":
        old_len = len(transforms)
        transforms = [s for s in transforms
                      if s.get("column") != col_name and s.get("output_column") != col_name]
        if len(transforms) < old_len:
            log.info("AUTO: removed steps for deprecated column '%s' from pipeline %s",
                     col_name, pipeline.pipeline_id)
        # Mark column as deprecated in input_streams metadata
        input_streams = list(pipeline.input_streams or [])
        dep_entry = next((s for s in input_streams if "deprecated_columns" in s), None)
        if dep_entry:
            if col_name not in dep_entry["deprecated_columns"]:
                dep_entry["deprecated_columns"].append(col_name)
        else:
            input_streams.append({"deprecated_columns": [col_name]})
        pipeline.input_streams = input_streams

    elif change_type == "type_changed":
        new_schema = ev.new_schema or {}
        new_type = new_schema.get("data_type", "string") if isinstance(new_schema, dict) else "string"
        for step in transforms:
            if step.get("type") == "cast" and step.get("column") == col_name:
                step["to_type"] = new_type
                log.info("AUTO: updated cast type for column '%s' → '%s' in pipeline %s",
                         col_name, new_type, pipeline.pipeline_id)
                break

    spec["transforms"] = transforms
    pipeline.transformation_code = _json.dumps(spec)
    db.commit()


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/changes")
@router.get("/events")
async def list_all_schema_events(
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List all schema-change events across every connection (for the Schema Evolution dashboard)."""
    query = db.query(SchemaChangeEvent)
    if status_filter:
        query = query.filter(SchemaChangeEvent.status == status_filter)
    total = query.count()
    events = (
        query.order_by(SchemaChangeEvent.detected_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    # Enrich with connection name
    connection_ids = {e.source_id for e in events}
    connections = {
        c.connection_id: c.connection_name
        for c in db.query(Connection).filter(Connection.connection_id.in_(connection_ids)).all()
    } if connection_ids else {}

    def _build(e: SchemaChangeEvent) -> dict:
        data = SchemaChangeResponse.model_validate(e).model_dump()
        data["id"] = str(e.event_id)
        data["connection_id"] = str(e.source_id)
        data["connection_name"] = connections.get(e.source_id, str(e.source_id))
        data["description"] = f"{e.change_type.replace('_', ' ').title()} on {e.table_name}"
        return data

    return {
        "schema_changes": [_build(e) for e in events],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": math.ceil(total / page_size) if total > 0 else 1,
    }


@router.post("/events/{change_id}/approve")
async def approve_schema_event(
    change_id: UUID,
    payload: ReviewRequest = ReviewRequest(),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Approve a schema-change event by its ID (without knowing the connection)."""
    ev = db.query(SchemaChangeEvent).filter(SchemaChangeEvent.event_id == change_id).first()
    if not ev:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema change event not found")
    if ev.status not in ("pending",):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Schema change is already '{ev.status}', cannot approve")
    ev.status = "approved"
    ev.reviewed_at = datetime.now(timezone.utc)
    ev.reviewed_by = user.user_id
    ev.review_notes = payload.review_notes
    db.commit()
    db.refresh(ev)
    _notify_spark_schema_reload(connection_id=str(ev.source_id), change_id=str(change_id))
    _apply_schema_change_to_pipeline(db, ev.source_id, ev)
    ev.applied_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(ev)
    return SchemaChangeResponse.model_validate(ev)


@router.post("/events/{change_id}/reject")
async def reject_schema_event(
    change_id: UUID,
    payload: ReviewRequest = ReviewRequest(),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Reject a schema-change event by its ID (without knowing the connection)."""
    ev = db.query(SchemaChangeEvent).filter(SchemaChangeEvent.event_id == change_id).first()
    if not ev:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema change event not found")
    if ev.status not in ("pending",):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Schema change is already '{ev.status}', cannot reject")
    ev.status = "rejected"
    ev.reviewed_at = datetime.now(timezone.utc)
    ev.reviewed_by = user.user_id
    ev.review_notes = payload.review_notes
    db.commit()
    db.refresh(ev)
    return SchemaChangeResponse.model_validate(ev)


@router.get("/connections/{connection_id}/schema-changes")
async def list_schema_changes(
    connection_id: UUID,
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List detected schema changes for a connection, optionally filtered by status."""
    query = db.query(SchemaChangeEvent).filter(
        SchemaChangeEvent.source_id == connection_id
    )
    if status_filter:
        query = query.filter(SchemaChangeEvent.status == status_filter)

    total = query.count()
    events = (
        query.order_by(SchemaChangeEvent.detected_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "schema_changes": [SchemaChangeResponse.model_validate(e) for e in events],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": math.ceil(total / page_size) if total > 0 else 1,
    }


@router.get("/connections/{connection_id}/schema-changes/{change_id}")
async def get_schema_change(
    connection_id: UUID,
    change_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get a single schema change event."""
    ev = _get_change_event(db, connection_id, change_id)
    return SchemaChangeResponse.model_validate(ev)


@router.post(
    "/connections/{connection_id}/schema-changes",
    status_code=status.HTTP_201_CREATED,
    response_model=SchemaChangeResponse,
)
async def report_schema_change(
    connection_id: UUID,
    payload: SchemaChangeCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """
    Record a newly detected schema change event (spec §3 §3.1).

    Called by:
    - CDC workers when they detect column/table changes during CDC streaming.
    - Introspection background jobs after periodic re-introspection.

    AUTO_APPLY logic:
      If the parent connection's schema_evolution_policy is AUTO_APPLY, the event
      is immediately auto-approved and the Spark consumer is notified to reload.
      For MANUAL_APPROVAL, the event stays in 'pending' status for human review.
    """
    now = datetime.now(timezone.utc)
    ev = SchemaChangeEvent(
        source_id=connection_id,
        stream_id=payload.stream_id,
        table_name=payload.table_name,
        schema_name=payload.schema_name,
        change_type=payload.change_type,
        old_schema=payload.old_schema,
        new_schema=payload.new_schema,
        schema_diff=payload.schema_diff,
        detected_at=now,
        detected_by=payload.detected_by,
        is_breaking=payload.is_breaking,
        impact_assessment={},
        status="pending",
    )

    # Check connection's schema_evolution_policy for AUTO_APPLY
    connection = db.query(Connection).filter(Connection.connection_id == connection_id).first()
    auto_apply = connection and getattr(connection, "schema_evolution_policy", "MANUAL_APPROVAL") == "AUTO_APPLY"

    # Spec §4 (P4-4): for type_changed, check compatibility before AUTO_APPLY.
    # Breaking changes (e.g. varchar→int) are always held for manual review.
    if auto_apply and payload.change_type == "type_changed":
        old_type = (payload.old_schema or {}).get("data_type", "") if isinstance(payload.old_schema, dict) else ""
        new_type = (payload.new_schema or {}).get("data_type", "") if isinstance(payload.new_schema, dict) else ""
        if not _is_compatible_type_change(old_type, new_type):
            auto_apply = False
            log.info(
                "AUTO_APPLY disabled for connection %s: type change %s→%s is breaking",
                connection_id, old_type, new_type,
            )

    if auto_apply:
        ev.status = "auto_approved"
        ev.reviewed_at = now
        ev.review_notes = "Automatically approved by AUTO_APPLY policy"

    db.add(ev)
    db.commit()
    db.refresh(ev)

    # Notify Spark consumer immediately for AUTO_APPLY
    if auto_apply:
        _audit(db, "schema_change.auto_approved", "schema_change_event", str(ev.event_id),
               user=user, details={"connection_id": str(connection_id), "policy": "AUTO_APPLY"})
        db.commit()
        _notify_spark_schema_reload(connection_id=str(connection_id), change_id=str(ev.event_id))
        # Spec §4 (P4-1): update TransformPipeline spec with new/changed/removed column
        _apply_schema_change_to_pipeline(db, connection_id, ev)
        # Spec §3 §4: mark applied_at after notification
        ev.applied_at = datetime.now(timezone.utc)
        _audit(db, "schema_change.applied", "schema_change_event", str(ev.event_id),
               user=user, details={"connection_id": str(connection_id), "policy": "AUTO_APPLY"})
        db.commit()
        db.refresh(ev)
        log.info(
            "AUTO_APPLY: schema change %s applied for connection %s",
            ev.event_id, connection_id,
        )

    return SchemaChangeResponse.model_validate(ev)


@router.post("/connections/{connection_id}/schema-changes/{change_id}/approve")
async def approve_schema_change(
    connection_id: UUID,
    change_id: UUID,
    payload: ReviewRequest = ReviewRequest(),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Approve a pending schema change and notify the Spark consumer to reload its schema."""
    ev = _get_change_event(db, connection_id, change_id)
    if ev.status not in ("pending",):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Schema change is already '{ev.status}', cannot approve",
        )
    ev.status = "approved"
    ev.reviewed_at = datetime.now(timezone.utc)
    ev.reviewed_by = user.user_id
    ev.review_notes = payload.review_notes
    _audit(db, "schema_change.approved", "schema_change_event", str(change_id),
           user=user, details={"connection_id": str(connection_id), "review_notes": payload.review_notes})
    db.commit()
    db.refresh(ev)

    # ----------------------------------------------------------------
    # Spec §3 — Schema Evolution notification flow:
    # After approval, send a webhook to the Spark consumer so it can
    # reload its schema and transformation logic mid-stream.
    # ----------------------------------------------------------------
    _notify_spark_schema_reload(connection_id=str(connection_id), change_id=str(change_id))
    # Spec §4 (P4-2): update TransformPipeline spec with new/changed/removed column
    _apply_schema_change_to_pipeline(db, connection_id, ev)

    # Spec §3 §4: "After the change is applied, log a SchemaChangeApplied event"
    ev.applied_at = datetime.now(timezone.utc)
    _audit(db, "schema_change.applied", "schema_change_event", str(change_id),
           user=user, details={"connection_id": str(connection_id)})
    db.commit()
    db.refresh(ev)

    return SchemaChangeResponse.model_validate(ev)


@router.post("/connections/{connection_id}/schema-changes/{change_id}/reject")
async def reject_schema_change(
    connection_id: UUID,
    change_id: UUID,
    payload: ReviewRequest = ReviewRequest(),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Reject a pending schema change."""
    ev = _get_change_event(db, connection_id, change_id)
    if ev.status not in ("pending",):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Schema change is already '{ev.status}', cannot reject",
        )
    ev.status = "rejected"
    ev.reviewed_at = datetime.now(timezone.utc)
    ev.reviewed_by = user.user_id
    ev.review_notes = payload.review_notes
    _audit(db, "schema_change.rejected", "schema_change_event", str(change_id),
           user=user, details={"connection_id": str(connection_id), "review_notes": payload.review_notes})
    db.commit()
    db.refresh(ev)
    return SchemaChangeResponse.model_validate(ev)


@router.get("/connections/{connection_id}/json-schemas")
async def list_json_schemas(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List cached JSON schemas for a connection's source."""
    schemas = (
        db.query(JSONSchemaCache)
        .filter(JSONSchemaCache.source_id == connection_id)
        .order_by(JSONSchemaCache.last_seen_at.desc())
        .all()
    )
    return {
        "json_schemas": [
            {
                "cache_id": str(s.cache_id),
                "source_id": str(s.source_id),
                "table_name": s.table_name,
                "column_name": s.column_name,
                "json_schema": s.json_schema,
                "sample_count": s.sample_count,
                "first_seen_at": s.first_seen_at.isoformat(),
                "last_seen_at": s.last_seen_at.isoformat(),
                "occurrence_count": s.occurrence_count,
            }
            for s in schemas
        ]
    }


@router.post(
    "/connections/{connection_id}/json-flatten-rules",
    status_code=status.HTTP_201_CREATED,
)
async def create_json_flatten_rule(
    connection_id: UUID,
    payload: JSONFlattenRuleCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Create a JSON flattening rule for a stream."""
    rule = JSONFlattenRule(
        stream_id=connection_id,
        source_column=payload.source_column,
        flatten_strategy=payload.flatten_strategy,
        target_columns=payload.target_columns,
        json_path_expressions=payload.json_path_expressions,
        column_prefix=payload.column_prefix,
        separator=payload.separator,
        is_active=True,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return JSONFlattenRuleResponse.model_validate(rule)


@router.get("/connections/{connection_id}/json-flatten-rules")
async def list_json_flatten_rules(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List JSON flattening rules for a connection."""
    rules = (
        db.query(JSONFlattenRule)
        .filter(
            JSONFlattenRule.stream_id == connection_id,
            JSONFlattenRule.is_active == True,
        )
        .order_by(JSONFlattenRule.created_at.desc())
        .all()
    )
    return {
        "flatten_rules": [JSONFlattenRuleResponse.model_validate(r) for r in rules]
    }


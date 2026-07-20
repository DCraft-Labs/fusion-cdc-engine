"""Transformation Pipelines API endpoints"""

import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.transformation import TransformPipeline
from app.schemas.transformation import (
    TransformPipelineCreate,
    TransformPipelineListResponse,
    TransformPipelineResponse,
    TransformPipelineUpdate,
    TransformValidateResponse,
)

router = APIRouter()


# ============================================================================
# Helpers
# ============================================================================

def _sample_live_cdc_events(db: Session, connection_id: str, count: int = 10) -> List[Dict[str, Any]]:
    """
    Spec §3 (P3-1): Read the last `count` CDC events from the Redis Stream
    associated with this connection.  Returns parsed row dicts, or [] on error.

    Redis stream key pattern: cdc:<bank_id>:<tenant_id>:<source_id>:<schema>:<table>
    We query the connection's stream metadata to build the key.
    """
    import os
    import json as _json
    try:
        from app.models.connection import Connection
        from app.models.monitoring import RedisStreamTracking
        conn = db.query(Connection).filter(
            Connection.connection_id == connection_id,
            Connection.is_deleted == False,
        ).first()
        if not conn:
            return []

        # Prefer stream tracking metadata for the exact key; fall back to constructed key
        stream_track = db.query(RedisStreamTracking).filter(
            RedisStreamTracking.connection_id == conn.connection_id
        ).first()
        if stream_track and getattr(stream_track, "stream_key", None):
            stream_key = stream_track.stream_key
        else:
            # Best-effort key construction
            bank_id = str(conn.source.bank_id) if hasattr(conn, "source") and conn.source else "*"
            tenant_id = "*"
            source_id = str(conn.source_id) if conn.source_id else "*"
            stream_key = f"cdc:{bank_id}:{tenant_id}:{source_id}:*"

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        import redis as _redis_lib
        r = _redis_lib.from_url(redis_url, decode_responses=True, socket_timeout=2)

        # If key contains wildcard, resolve it
        if "*" in stream_key:
            keys = r.keys(stream_key)
            if not keys:
                return []
            stream_key = keys[0]

        # Read last `count` entries from the stream
        entries = r.xrevrange(stream_key, count=count)
        rows = []
        for _msg_id, fields in entries:
            try:
                payload = _json.loads(fields.get("payload", fields.get("data", "{}")))
                rows.append(payload)
            except Exception:
                rows.append(dict(fields))
        return rows[::-1]  # restore chronological order
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("_sample_live_cdc_events failed: %s", exc)
        return []


def _get_pipeline(db: Session, pipeline_id: UUID, user) -> TransformPipeline:
    """Fetch pipeline scoped to the current tenant, raise 404 if missing."""
    pipeline = (
        db.query(TransformPipeline)
        .filter(
            TransformPipeline.pipeline_id == pipeline_id,
            TransformPipeline.sub_tenant_id == user.sub_tenant_id,
            TransformPipeline.is_deleted == False,
        )
        .first()
    )
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transformation pipeline {pipeline_id} not found",
        )
    return pipeline


def _validate_code(pipeline_type: str, language: str, code: str) -> list[str]:
    """Basic syntax validation for transformation code. Returns list of error strings."""
    errors: list[str] = []
    if not code or not code.strip():
        errors.append("transformation_code must not be empty")
        return errors

    if language == "python":
        try:
            compile(code, "<transformation>", "exec")
        except SyntaxError as exc:
            errors.append(f"Python SyntaxError at line {exc.lineno}: {exc.msg}")

    elif language == "sql":
        # Minimal SQL check: must contain SELECT or INSERT or UPDATE or CREATE
        keywords = {"select", "insert", "update", "create", "merge"}
        if not any(kw in code.lower() for kw in keywords):
            errors.append("SQL transformation must contain a valid DML or DDL statement")

    # scala / spark: no compile-time check available without JVM
    return errors


# ============================================================================
# Endpoints
# ============================================================================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=TransformPipelineResponse)
async def create_transformation(
    payload: TransformPipelineCreate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Create a new transformation pipeline (version 1)."""
    # Reject duplicate name within the same tenant
    existing = (
        db.query(TransformPipeline)
        .filter(
            TransformPipeline.sub_tenant_id == user.sub_tenant_id,
            TransformPipeline.pipeline_name == payload.pipeline_name,
            TransformPipeline.is_deleted == False,
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"A transformation pipeline named '{payload.pipeline_name}' already exists in this tenant",
        )

    pipeline = TransformPipeline(
        pipeline_name=payload.pipeline_name,
        description=payload.description,
        pipeline_type=payload.pipeline_type,
        transformation_code=payload.transformation_code,
        language=payload.language,
        input_streams=payload.input_streams,
        output_stream=payload.output_stream,
        execution_mode=payload.execution_mode,
        spark_config=payload.spark_config,
        version=1,
        is_published=False,
        is_validated=False,
        is_active=True,
        is_deleted=False,
        sub_tenant_id=user.sub_tenant_id,
        bank_id=user.bank_id,
        created_by=user.user_id,
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline


@router.get("", response_model=TransformPipelineListResponse)
async def list_transformations(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    pipeline_type: Optional[str] = Query(None),
    language: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """List transformation pipelines for the current tenant with optional filters."""
    query = db.query(TransformPipeline).filter(
        TransformPipeline.sub_tenant_id == user.sub_tenant_id,
        TransformPipeline.is_deleted == False,
    )

    if pipeline_type:
        query = query.filter(TransformPipeline.pipeline_type == pipeline_type)
    if language:
        query = query.filter(TransformPipeline.language == language)
    if is_active is not None:
        query = query.filter(TransformPipeline.is_active == is_active)
    if search:
        query = query.filter(TransformPipeline.pipeline_name.ilike(f"%{search}%"))

    total = query.count()
    pipelines = (
        query.order_by(TransformPipeline.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return TransformPipelineListResponse(
        pipelines=pipelines,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=math.ceil(total / page_size) if total > 0 else 1,
    )


@router.get("/{pipeline_id}", response_model=TransformPipelineResponse)
async def get_transformation(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get a single transformation pipeline by ID."""
    return _get_pipeline(db, pipeline_id, user)


@router.put("/{pipeline_id}", response_model=TransformPipelineResponse)
async def update_transformation(
    pipeline_id: UUID,
    payload: TransformPipelineUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update transformation pipeline fields. Increments version on code change."""
    pipeline = _get_pipeline(db, pipeline_id, user)

    code_changed = (
        payload.transformation_code is not None
        and payload.transformation_code != pipeline.transformation_code
    )

    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(pipeline, field, value)

    if code_changed:
        pipeline.version += 1
        pipeline.is_validated = False
        pipeline.validation_errors = None
        pipeline.validated_at = None

    pipeline.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(pipeline)
    return pipeline


@router.delete("/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_transformation(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Soft-delete a transformation pipeline."""
    pipeline = _get_pipeline(db, pipeline_id, user)
    pipeline.is_deleted = True
    pipeline.deleted_at = datetime.now(timezone.utc)
    pipeline.is_active = False
    db.commit()


@router.post("/{pipeline_id}/validate", response_model=TransformValidateResponse)
async def validate_transformation(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Validate the transformation code syntax and update validation status."""
    pipeline = _get_pipeline(db, pipeline_id, user)

    errors = _validate_code(pipeline.pipeline_type, pipeline.language, pipeline.transformation_code)
    now = datetime.now(timezone.utc)

    pipeline.is_validated = len(errors) == 0
    pipeline.validation_errors = errors if errors else None
    pipeline.validated_at = now
    db.commit()
    db.refresh(pipeline)

    return TransformValidateResponse(
        pipeline_id=pipeline.pipeline_id,
        valid=len(errors) == 0,
        errors=errors,
        validated_at=now,
    )


# ============================================================================
# Preview endpoint — spec §2: "The UI provides a Preview feature"
# ============================================================================

class PreviewRequest(BaseModel):
    """Sample rows to apply transform steps to."""
    sample_rows: List[Dict[str, Any]] = []
    transform_spec: Optional[Dict[str, Any]] = None  # override pipeline spec if provided
    # Spec §3 (P3-1): if sample_rows is empty, fetch live CDC events from Redis
    # Caller may optionally specify which stream to sample from.
    connection_id: Optional[str] = None   # if set, sample from this connection's Redis stream
    live_sample_count: int = 10           # how many events to pull from Redis


class PreviewResponse(BaseModel):
    transformed_rows: List[Dict[str, Any]]
    step_count: int
    errors: List[str]


def _apply_transform_steps_python(rows: List[dict], transform_spec: dict) -> tuple[List[dict], List[str]]:
    """
    Apply a subset of transform steps natively in Python for preview.

    Supports: cast, string_op, mask, json_extract.
    Other step types are noted but skipped (require Spark).
    """
    import re

    errors: List[str] = []
    result = [dict(r) for r in rows]

    for step in transform_spec.get("transforms", []):
        step_type = step.get("type", "")
        out_col = step.get("output_column")

        try:
            if step_type == "cast":
                col = step["column"]
                to_type = step.get("to_type", "string")
                for row in result:
                    v = row.get(col)
                    if v is not None:
                        if to_type in ("int", "integer"):
                            row[out_col] = int(v)
                        elif to_type in ("float", "double"):
                            row[out_col] = float(v)
                        elif to_type == "string":
                            row[out_col] = str(v)
                        elif to_type == "boolean":
                            row[out_col] = bool(v)
                        else:
                            row[out_col] = str(v)

            elif step_type == "string_op":
                col = step["column"]
                op = step["op"]
                for row in result:
                    v = str(row.get(col, ""))
                    if op == "upper":
                        row[out_col] = v.upper()
                    elif op == "lower":
                        row[out_col] = v.lower()
                    elif op == "trim":
                        row[out_col] = v.strip()
                    elif op == "substring":
                        params = step.get("params", {})
                        start = int(params.get("start", 0))
                        length = int(params.get("length", len(v)))
                        row[out_col] = v[start: start + length]

            elif step_type == "mask":
                col = step["column"]
                strategy = step.get("strategy", "hash")
                for row in result:
                    v = str(row.get(col, ""))
                    if strategy == "last4":
                        row[out_col] = "****" + v[-4:] if len(v) >= 4 else "****"
                    elif strategy == "hash":
                        import hashlib
                        row[out_col] = hashlib.sha256(v.encode()).hexdigest()[:16]
                    else:
                        row[out_col] = "***MASKED***"

            elif step_type == "json_extract":
                col = step["column"]
                json_path = step.get("json_path", "$")
                for row in result:
                    raw = row.get(col)
                    if isinstance(raw, str):
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            parsed = {}
                    elif isinstance(raw, dict):
                        parsed = raw
                    else:
                        parsed = {}
                    # Simple $.key extraction
                    key = json_path.lstrip("$.")
                    row[out_col] = parsed.get(key)

            else:
                errors.append(
                    f"Step '{step.get('id', step_type)}' of type '{step_type}' "
                    "requires Spark — skipped in preview"
                )

        except Exception as exc:
            errors.append(f"Step '{step.get('id', step_type)}' error: {exc}")

    return result, errors


@router.post("/{pipeline_id}/preview", response_model=PreviewResponse)
async def preview_transformation(
    pipeline_id: UUID,
    payload: PreviewRequest,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """
    Preview transformation output on sample rows.

    Applies transform steps natively in Python for fast feedback.
    Steps requiring Spark (math_op, expression, udf, json_flatten_*)
    are noted in the errors list but skipped — full execution requires Spark.

    UI should display both transformed_rows and any step-skip notices.
    """
    pipeline = _get_pipeline(db, pipeline_id, user)

    # Spec §3 (P3-1): sample live CDC events from Redis when caller doesn't supply rows
    sample_rows = payload.sample_rows
    if not sample_rows and payload.connection_id:
        sample_rows = _sample_live_cdc_events(
            db,
            connection_id=payload.connection_id,
            count=payload.live_sample_count,
        )

    # Use override spec if provided, else parse pipeline's transformation_code as JSON spec
    if payload.transform_spec:
        spec = payload.transform_spec
    else:
        try:
            spec = json.loads(pipeline.transformation_code or "{}")
        except (json.JSONDecodeError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Pipeline transformation_code is not valid JSON. Use the transform_spec body field to supply a spec directly.",
            )

    transformed_rows, errors = _apply_transform_steps_python(sample_rows, spec)
    step_count = len(spec.get("transforms", []))

    return PreviewResponse(
        transformed_rows=transformed_rows,
        step_count=step_count,
        errors=errors,
    )


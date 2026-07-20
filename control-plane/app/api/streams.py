"""Streams API endpoints — per-table stream configuration for a Connection.

Spec §1 (PDF1/PDF2): Each connection has one or more Stream records that control
which tables are replicated, at what sync mode, and with what cursor/PK overrides.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.connection import Connection, Stream

log = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic schemas (no separate schemas/ file yet for streams)
# ---------------------------------------------------------------------------

class StreamResponse(BaseModel):
    stream_id: UUID
    connection_id: UUID
    # Names match Stream ORM columns exactly so model_validate(orm_obj) works
    source_schema_name: Optional[str] = None
    source_table_name: str
    destination_schema_name: Optional[str] = None
    destination_table_name: Optional[str] = None
    is_enabled: bool = True
    sync_mode: Optional[str] = None
    cursor_field: Optional[str] = None
    primary_keys: Optional[Any] = None  # JSONB list
    column_mapping: Optional[Any] = None
    selected_columns: Optional[Any] = None
    transform_overrides: Optional[Any] = None  # full transform spec from UI
    sync_status: Optional[str] = None
    last_synced_at: Optional[Any] = None
    records_synced: int = 0

    model_config = {"from_attributes": True}


class StreamListResponse(BaseModel):
    streams: List[StreamResponse]
    total: int


class StreamUpdate(BaseModel):
    sync_mode: Optional[str] = None
    cursor_field: Optional[str] = None
    primary_keys: Optional[Any] = None       # matches Stream.primary_keys column
    column_mapping: Optional[Any] = None     # matches Stream.column_mapping column
    selected_columns: Optional[Any] = None  # matches Stream.selected_columns column
    transform_steps: Optional[Any] = None   # saved as Stream.transform_overrides
    is_enabled: Optional[bool] = None       # matches Stream.is_enabled column


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_stream(db: Session, connection_id: UUID, stream_id: UUID) -> Stream:
    stream = (
        db.query(Stream)
        .join(Connection, Connection.connection_id == Stream.connection_id)
        .filter(
            Stream.stream_id == stream_id,
            Stream.connection_id == connection_id,
            Connection.is_deleted.is_(False),
        )
        .first()
    )
    if not stream:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stream {stream_id} not found for connection {connection_id}",
        )
    return stream


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/connections/{connection_id}/streams",
    response_model=StreamListResponse,
    summary="List all streams for a connection",
)
async def list_streams(
    connection_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Return all table-level stream configs for a connection."""
    conn = (
        db.query(Connection)
        .filter(Connection.connection_id == connection_id, Connection.is_deleted.is_(False))
        .first()
    )
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    streams = db.query(Stream).filter(Stream.connection_id == connection_id).all()
    return StreamListResponse(
        streams=[StreamResponse.model_validate(s) for s in streams],
        total=len(streams),
    )


@router.put(
    "/connections/{connection_id}/streams/{stream_id}",
    response_model=StreamResponse,
    summary="Update stream configuration (cursor, PK, transforms, sync mode)",
)
async def update_stream(
    connection_id: UUID,
    stream_id: UUID,
    payload: StreamUpdate,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update sync mode, cursor field, primary key overrides, or transform overrides."""
    stream = _get_stream(db, connection_id, stream_id)

    update_data = payload.model_dump(exclude_none=True)
    # Map transform_steps → transform_overrides
    if 'transform_steps' in update_data:
        update_data['transform_overrides'] = update_data.pop('transform_steps') or {}
    for field, value in update_data.items():
        setattr(stream, field, value)

    db.commit()
    db.refresh(stream)
    log.info("Stream %s updated by user %s", stream_id, getattr(user, "user_id", "?"))
    return StreamResponse.model_validate(stream)


@router.post(
    "/connections/{connection_id}/streams/{stream_id}/enable",
    response_model=StreamResponse,
    summary="Enable a stream (resume CDC for this table)",
)
async def enable_stream(
    connection_id: UUID,
    stream_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Mark the stream as enabled so the CDC worker starts (or resumes) capturing events."""
    stream = _get_stream(db, connection_id, stream_id)
    stream.is_enabled = True
    db.commit()
    db.refresh(stream)
    log.info("Stream %s enabled by user %s", stream_id, getattr(user, "user_id", "?"))
    return StreamResponse.model_validate(stream)


@router.post(
    "/connections/{connection_id}/streams/{stream_id}/disable",
    response_model=StreamResponse,
    summary="Disable a stream (pause CDC for this table)",
)
async def disable_stream(
    connection_id: UUID,
    stream_id: UUID,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Mark the stream as disabled so the CDC worker skips this table."""
    stream = _get_stream(db, connection_id, stream_id)
    stream.is_enabled = False
    db.commit()
    db.refresh(stream)
    log.info("Stream %s disabled by user %s", stream_id, getattr(user, "user_id", "?"))
    return StreamResponse.model_validate(stream)

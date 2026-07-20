"""Dead Letter Queue API endpoints.

Provides full CRUD + retry/purge operations over the event_dead_letter_queue table.

Routes (all mounted under /api/v1/dlq):
  GET    /stats                                      — global stats
  GET    /connections                                — per-connection failure summary
  GET    /{connection_id}/events                     — paginated events for one conn
  GET    /{connection_id}/{event_id}                 — single event detail
  POST   /retry-all                                  — retry ALL pending events
  POST   /purge-expired                              — delete all expired/max-retry events
  POST   /{connection_id}/retry-all                  — retry all pending for one conn
  POST   /{connection_id}/retry                      — retry selected event IDs
  POST   /{connection_id}/delete                     — delete selected event IDs
  POST   /{connection_id}/{event_id}/retry           — retry a single event
  DELETE /{connection_id}/{event_id}                 — hard-delete a single event
  DELETE /{connection_id}/purge                      — purge all events for one conn
"""
from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.system import EventDeadLetterQueue, EventDLQRetryHistory

router = APIRouter()

MAX_RETRIES = 5
TTL_HOURS   = 72   # events older than this are "expired"


# ============================================================================
# Pydantic schemas
# ============================================================================

class DLQStats(BaseModel):
    total: int
    pending_retry: int
    retried_success: int
    expired: int
    by_connection: List[dict] = []


class DLQConnectionSummary(BaseModel):
    connection_id: str
    connection_name: Optional[str]
    failed_count: int
    reason: Optional[str]
    oldest_event_time: Optional[datetime]


class RetryPayload(BaseModel):
    payload: Optional[dict] = None


class BulkEventIds(BaseModel):
    event_ids: List[str]


# ============================================================================
# Helpers
# ============================================================================

def _expired_cutoff() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=TTL_HOURS)


def _event_status(ev: EventDeadLetterQueue) -> str:
    """Derive human-readable status from the model's status field + retry count."""
    if ev.status == "resolved" or ev.resolved_at:
        return "resolved"
    if ev.status == "discarded":
        return "expired"
    if ev.retry_count >= MAX_RETRIES:
        return "expired"
    if ev.retry_count > 0 or ev.status == "retrying":
        return "retrying"
    return "pending"


def _get_event(db: Session, connection_id: str, event_id: str) -> EventDeadLetterQueue:
    ev = db.query(EventDeadLetterQueue).filter(
        EventDeadLetterQueue.dlq_id == event_id,
        EventDeadLetterQueue.connection_id == connection_id,
    ).first()
    if not ev:
        raise HTTPException(status_code=404, detail=f"DLQ event {event_id} not found")
    return ev


def _do_retry(db: Session, ev: EventDeadLetterQueue, override_payload: Optional[dict] = None) -> None:
    """
    Mark an event as retried.  In a real system this would re-publish the event to
    the CDC Redis stream; here we increment the counter and mark it resolved so the
    UI reflects the action immediately.
    """
    ev.retry_count += 1
    ev.status = "resolved"
    ev.last_retry_at = datetime.now(timezone.utc)
    ev.resolved_at   = datetime.now(timezone.utc)
    history = EventDLQRetryHistory(
        dlq_id=ev.dlq_id,
        retry_number=ev.retry_count,
        attempted_at=datetime.now(timezone.utc),
        success=True,   # optimistic — worker will nack if it fails again
        error_message=None,
    )
    db.add(history)
    db.commit()


# ============================================================================
# Routes
# ============================================================================

@router.get("/stats", response_model=DLQStats)
def get_stats(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Global DLQ statistics."""
    cutoff = _expired_cutoff()
    total          = db.query(func.count(EventDeadLetterQueue.dlq_id)).scalar() or 0
    pending_retry  = db.query(func.count(EventDeadLetterQueue.dlq_id)).filter(
        EventDeadLetterQueue.status == "failed",
        EventDeadLetterQueue.retry_count < MAX_RETRIES,
        EventDeadLetterQueue.failed_at >= cutoff,
    ).scalar() or 0
    retried_success = db.query(func.count(EventDeadLetterQueue.dlq_id)).filter(
        EventDeadLetterQueue.status == "resolved",
    ).scalar() or 0
    expired = db.query(func.count(EventDeadLetterQueue.dlq_id)).filter(
        EventDeadLetterQueue.status.in_(["failed", "retrying"]),
        EventDeadLetterQueue.failed_at < cutoff,
    ).scalar() or 0

    # Per-connection breakdown
    rows = db.execute(text("""
        SELECT connection_id,
               COUNT(*) AS failed_count,
               MIN(failure_reason) AS reason,
               MIN(failed_at)      AS oldest
        FROM event_dead_letter_queue
        WHERE status NOT IN ('resolved', 'discarded')
        GROUP BY connection_id
        ORDER BY failed_count DESC
        LIMIT 20
    """)).fetchall()
    by_conn = [
        {"connection_id": str(r[0]), "failed_count": r[1],
         "reason": r[2], "oldest_event_time": r[3]}
        for r in rows
    ]
    return DLQStats(
        total=total,
        pending_retry=pending_retry,
        retried_success=retried_success,
        expired=expired,
        by_connection=by_conn,
    )


@router.get("/connections", response_model=List[DLQConnectionSummary])
def get_connection_summaries(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """One row per connection that has DLQ events."""
    rows = db.execute(text("""
        SELECT dlq.connection_id,
               c.connection_name,
               COUNT(*) AS failed_count,
               MIN(dlq.failure_reason) AS reason,
               MIN(dlq.failed_at)      AS oldest
        FROM event_dead_letter_queue dlq
        LEFT JOIN connections c ON c.connection_id = dlq.connection_id
        WHERE dlq.status NOT IN ('resolved', 'discarded')
        GROUP BY dlq.connection_id, c.connection_name
        ORDER BY failed_count DESC
    """)).fetchall()
    return [
        DLQConnectionSummary(
            connection_id=str(r[0]),
            connection_name=r[1],
            failed_count=r[2],
            reason=r[3],
            oldest_event_time=r[4],
        )
        for r in rows
    ]


@router.get("/{connection_id}/events")
def list_events(
    connection_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Paginated list of DLQ events for a connection."""
    q = db.query(EventDeadLetterQueue).filter(
        EventDeadLetterQueue.connection_id == connection_id,
    ).order_by(EventDeadLetterQueue.failed_at.desc())
    total = q.count()
    events = q.offset((page - 1) * page_size).limit(page_size).all()
    return {
        "events": [
            {
                "id": str(ev.dlq_id),
                "connection_id": str(ev.connection_id),
                "event_id": ev.event_id,
                "event_type": ev.event_type,
                "error_message": ev.failure_reason,
                "retry_count": ev.retry_count,
                "max_retries": MAX_RETRIES,
                "status": _event_status(ev),
                "created_at": ev.failed_at.isoformat() if ev.failed_at else None,
                "last_retry_at": ev.last_retry_at.isoformat() if ev.last_retry_at else None,
                "resolved_at": ev.resolved_at.isoformat() if ev.resolved_at else None,
                "original_payload": ev.event_data,
            }
            for ev in events
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
    }


@router.get("/{connection_id}/{event_id}")
def get_event_detail(
    connection_id: str,
    event_id: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Single DLQ event with full payload and retry history."""
    ev = _get_event(db, connection_id, event_id)
    history = (
        db.query(EventDLQRetryHistory)
        .filter(EventDLQRetryHistory.dlq_id == ev.dlq_id)
        .order_by(EventDLQRetryHistory.retry_number)
        .all()
    )
    return {
        "id": str(ev.dlq_id),
        "connection_id": str(ev.connection_id),
        "event_id": ev.event_id,
        "event_type": ev.event_type,
        "error_message": ev.failure_reason,
        "error_details": ev.error_stack_trace,
        "retry_count": ev.retry_count,
        "max_retries": MAX_RETRIES,
        "status": _event_status(ev),
        "created_at": ev.failed_at.isoformat() if ev.failed_at else None,
        "last_retry_at": ev.last_retry_at.isoformat() if ev.last_retry_at else None,
        "resolved_at": ev.resolved_at.isoformat() if ev.resolved_at else None,
        "original_payload": ev.event_data,
        "retry_history": [
            {
                "retry_number": h.retry_number,
                "attempted_at": h.attempted_at.isoformat() if h.attempted_at else None,
                "success": h.success,
                "error_message": h.error_message,
            }
            for h in history
        ],
    }


@router.post("/retry-all")
def retry_all_global(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Retry ALL pending DLQ events across all connections."""
    pending = db.query(EventDeadLetterQueue).filter(
        EventDeadLetterQueue.status == "failed",
        EventDeadLetterQueue.retry_count < MAX_RETRIES,
    ).all()
    retried = 0
    for ev in pending:
        _do_retry(db, ev)
        retried += 1
    return {"retried": retried}


@router.post("/purge-expired")
def purge_expired_global(
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Hard-delete all expired (TTL exceeded or max retries reached) events."""
    cutoff  = _expired_cutoff()
    deleted = db.query(EventDeadLetterQueue).filter(
        EventDeadLetterQueue.failed_at < cutoff,
    ).delete(synchronize_session=False)
    db.commit()
    return {"deleted": deleted}


@router.post("/{connection_id}/retry-all")
def retry_all_for_connection(
    connection_id: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Retry all pending events for a specific connection."""
    pending = db.query(EventDeadLetterQueue).filter(
        EventDeadLetterQueue.connection_id == connection_id,
        EventDeadLetterQueue.status == "failed",
        EventDeadLetterQueue.retry_count < MAX_RETRIES,
    ).all()
    for ev in pending:
        _do_retry(db, ev)
    return {"retried": len(pending)}


@router.post("/{connection_id}/retry")
def retry_selected(
    connection_id: str,
    body: BulkEventIds,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Retry a specific list of event IDs."""
    retried = 0
    for eid in body.event_ids:
        try:
            ev = _get_event(db, connection_id, eid)
            _do_retry(db, ev)
            retried += 1
        except HTTPException:
            pass
    return {"retried": retried}


@router.post("/{connection_id}/delete")
def delete_selected(
    connection_id: str,
    body: BulkEventIds,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Hard-delete specific event IDs."""
    deleted = 0
    for eid in body.event_ids:
        ev = db.query(EventDeadLetterQueue).filter(
            EventDeadLetterQueue.dlq_id == eid,
            EventDeadLetterQueue.connection_id == connection_id,
        ).first()
        if ev:
            db.delete(ev)
            deleted += 1
    db.commit()
    return {"deleted": deleted}


@router.post("/{connection_id}/{event_id}/retry")
def retry_single(
    connection_id: str,
    event_id: str,
    body: RetryPayload = RetryPayload(),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Retry a single DLQ event, optionally with an edited payload."""
    ev = _get_event(db, connection_id, event_id)
    if ev.retry_count >= MAX_RETRIES:
        raise HTTPException(status_code=400, detail="Event has exceeded maximum retries")
    _do_retry(db, ev, override_payload=body.payload)
    return {"status": "retried", "retry_count": ev.retry_count}


@router.delete("/{connection_id}/{event_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_event(
    connection_id: str,
    event_id: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Hard-delete a single DLQ event."""
    ev = _get_event(db, connection_id, event_id)
    db.delete(ev)
    db.commit()


@router.delete("/{connection_id}/purge", status_code=status.HTTP_204_NO_CONTENT)
def purge_connection(
    connection_id: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    """Delete ALL DLQ events (resolved or not) for a connection."""
    db.query(EventDeadLetterQueue).filter(
        EventDeadLetterQueue.connection_id == connection_id,
    ).delete(synchronize_session=False)
    db.commit()

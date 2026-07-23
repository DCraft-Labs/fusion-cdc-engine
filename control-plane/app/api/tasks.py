"""Tasks API endpoints.

v1.2.25 Task 6: dead-letter task requeue. Tasks that exhausted their retry
budget are moved by the transform-worker to the Redis list
``fusion:transforms:dead-letter``. After an operator fixes the root cause, they
can requeue a dead-lettered task via

    POST /api/v1/tasks/dead-letter/{task_id}/requeue

which removes the entry from the dead-letter list and re-enqueues the original
payload to ``fusion:transforms:high`` so the transform-worker retries it.
"""
import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.dependencies import require_permission
from app.models.auth import User

router = APIRouter()


class RequeueResponse(BaseModel):
    task_id: str
    requeued: bool
    message: str
    queue: Optional[str] = None


@router.post(
    "/dead-letter/{task_id}/requeue",
    response_model=RequeueResponse,
    status_code=status.HTTP_200_OK,
)
async def requeue_dead_letter_task(
    task_id: str,
    current_user: User = Depends(require_permission("connections:update")),
):
    """Requeue a dead-lettered task after fixing the root cause.

    Reads the ``fusion:transforms:dead-letter`` Redis list, finds the entry
    whose ``task_id`` matches, removes it, and re-enqueues the original
    payload to ``fusion:transforms:high`` so the transform-worker retries it
    with a fresh retry counter.

    Returns 404 if no dead-lettered task with that ``task_id`` is found.
    """
    from app.config import settings
    import redis as redis_lib

    try:
        r = redis_lib.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        entries = r.lrange("fusion:transforms:dead-letter", 0, -1)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not read dead-letter list from Redis: {exc}",
        )

    target_raw = None
    for raw in entries:
        try:
            entry = json.loads(raw)
        except Exception:
            continue
        if str(entry.get("task_id")) == str(task_id):
            target_raw = raw
            break

    if target_raw is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dead-lettered task {task_id} not found",
        )

    entry = json.loads(target_raw)
    payload = entry.get("payload")
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dead-letter entry for {task_id} has no payload to requeue",
        )

    # Strip retry bookkeeping so the worker starts fresh.
    try:
        task = json.loads(payload)
        task.pop("_retry_count", None)
        task.pop("_last_error", None)
        task.pop("_last_failed_at", None)
        payload = json.dumps(task)
    except Exception:
        # Payload isn't JSON — requeue as-is.
        pass

    HIGH_QUEUE = "fusion:transforms:high"
    pipe = r.pipeline()
    pipe.lrem("fusion:transforms:dead-letter", 1, target_raw)
    pipe.lpush(HIGH_QUEUE, payload)
    pipe.execute()

    return RequeueResponse(
        task_id=task_id,
        requeued=True,
        message=f"Requeued task {task_id} to {HIGH_QUEUE}",
        queue=HIGH_QUEUE,
    )

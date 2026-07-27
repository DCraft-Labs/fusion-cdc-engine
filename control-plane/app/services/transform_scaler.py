"""
Phase 3b: control-plane direct authority over transform-worker's replica
count, both directions.

Replaces the transform-worker's stale KEDA ``ScaledObject`` (see
``kubernetes/base/transform-worker.yaml`` before this change): one of its
two triggers correctly watched ``fusion:transforms:high`` (initial-load
queue depth), but the OTHER trigger watched ``fusion:transforms:normal`` — a
Redis List retired since v1.3.9 (transform-worker/worker.py:47-51, when CDC
traffic moved to per-connection Redis Streams). That trigger's listName
never had anything pushed to it, so it never fired — CDC-driven scaling for
transform-worker has been silently dead since v1.3.9. This module gives
control-plane a real CDC-backlog signal (aggregate Redis Streams pending
count) plus the existing initial-load signal, and patches
``fusion-transform-worker``'s ``spec.replicas`` directly via the Kubernetes
API — scaling UP **and DOWN**, not just up.

Signals read every tick:
  - ``LLEN fusion:transforms:high`` — the initial-load queue. Already read
    elsewhere in control-plane (``app/api/connections.py``'s
    ``_enqueue_initial_load_tasks`` writes to it; ``app/api/tasks.py``'s
    dead-letter requeue also targets it) via the same
    ``HIGH_PRIORITY_QUEUE`` env var convention, matched here.
  - Aggregate CDC backlog: for every connection_id in the
    ``fusion:transforms:active_connections`` Redis set (maintained by
    transform-worker/cdc_stream_consumer.py), XPENDING-summary each
    ``fusion:transforms:stream:{connection_id}`` stream against the
    ``transform-workers`` consumer group and sum the pending counts. These
    names are duplicated here as literal constants rather than imported
    from ``cdc_stream_consumer.py`` — control-plane and transform-worker
    are separate deployables with no shared PYTHONPATH, the same reason
    ``committer_provisioner.py``'s ``lag_check`` inlines the
    ``fusion:iceberg-pending-files:...`` key format instead of importing
    ``iceberg_committer.py``.

Runs as an asyncio background task from ``app/main.py``'s lifespan, gated
by the SAME Redis leader-election pattern ``app/services/scheduler.py``
already uses (``RedisLeaderElection``), under its own key so it elects
independently of the worker-assignment scheduler.

Hysteresis / cooldown: KEDA's own ``cooldownPeriod`` (120s in the retired
ScaledObject) only debounces the scale-to-ZERO transition. A reconcile
loop computing desired replicas from two independent, noisy signals every
tick needs a SYMMETRIC guard — see ``decide_next_replicas`` — otherwise a
signal oscillating around a threshold would flap replicas up and down on
every tick. Scale-up requires fewer confirming ticks (queue pressure should
get relief quickly); scale-down requires more (a momentary lull should not
immediately give back capacity that will likely be needed again soon).
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Signal sources — see module docstring for why these are literal constants
# rather than cross-service imports.
# ---------------------------------------------------------------------------
HIGH_QUEUE_KEY = os.environ.get("HIGH_PRIORITY_QUEUE", "fusion:transforms:high")
CDC_ACTIVE_CONNECTIONS_SET = "fusion:transforms:active_connections"
CDC_STREAM_KEY_PREFIX = "fusion:transforms:stream:"
CDC_CONSUMER_GROUP = "transform-workers"

# ---------------------------------------------------------------------------
# Scaling model — HIGH_TASKS_PER_REPLICA/CDC_BACKLOG_PER_REPLICA mirror the
# retired ScaledObject's own trigger ratios (listLength: "1" for the
# high-priority trigger, "5" for the — never-firing — CDC trigger) so
# replica counts don't shift wildly on the day this ships relative to the
# (partially) working prior KEDA behavior. MIN/MAX_REPLICAS mirror the old
# ScaledObject's minReplicaCount/maxReplicaCount (0/500) — true
# scale-to-zero is preserved.
# ---------------------------------------------------------------------------
MIN_REPLICAS = int(os.environ.get("TRANSFORM_SCALER_MIN_REPLICAS", "0"))
MAX_REPLICAS = int(os.environ.get("TRANSFORM_SCALER_MAX_REPLICAS", "500"))
HIGH_TASKS_PER_REPLICA = int(os.environ.get("TRANSFORM_SCALER_HIGH_TASKS_PER_REPLICA", "1"))
CDC_BACKLOG_PER_REPLICA = int(os.environ.get("TRANSFORM_SCALER_CDC_BACKLOG_PER_REPLICA", "5"))

TICK_INTERVAL_S = int(os.environ.get("TRANSFORM_SCALER_TICK_INTERVAL_S", "15"))

# Hysteresis / cooldown (see module docstring). Scale-up reacts on the
# very next tick by default; scale-down needs several confirming ticks.
# Independent of these tick counts, no scale action of EITHER direction may
# fire more often than SCALE_COOLDOWN_S after the previous one — inspired
# by (not identical to) KEDA's own cooldownPeriod concept.
SCALE_COOLDOWN_S = int(os.environ.get("TRANSFORM_SCALER_COOLDOWN_S", "120"))
SCALE_UP_CONSECUTIVE_TICKS = int(os.environ.get("TRANSFORM_SCALER_SCALE_UP_TICKS", "1"))
SCALE_DOWN_CONSECUTIVE_TICKS = int(os.environ.get("TRANSFORM_SCALER_SCALE_DOWN_TICKS", "3"))

DEPLOYMENT_NAME = os.environ.get("TRANSFORM_WORKER_DEPLOYMENT_NAME", "fusion-transform-worker")

STATE_KEY = "fusion:transform-scaler:state"
LEADER_KEY = "fusion:transform-scaler:leader"


# ---------------------------------------------------------------------------
# Pure decision logic (zero I/O — unit-testable without Redis/K8s/DB, same
# separation resource_admission.py already uses relative to
# resource_ledger.py).
# ---------------------------------------------------------------------------

def compute_desired_replicas(high_queue_depth: int, cdc_backlog_total: int) -> int:
    """Signal depths -> desired replica count, clamped to
    ``[MIN_REPLICAS, MAX_REPLICAS]``.

    Mirrors KEDA's own semantics for a ScaledObject with multiple
    triggers: each trigger independently "votes" for however many
    replicas its own ratio implies, and the desired count is the MAX
    across triggers — not a sum. That's the same shape the retired
    ScaledObject already had (two independent redis-list triggers), so
    this doesn't change the scaling MODEL, only which signal is real.
    """
    high_queue_depth = max(0, int(high_queue_depth or 0))
    cdc_backlog_total = max(0, int(cdc_backlog_total or 0))
    from_high = math.ceil(high_queue_depth / HIGH_TASKS_PER_REPLICA) if high_queue_depth else 0
    from_cdc = math.ceil(cdc_backlog_total / CDC_BACKLOG_PER_REPLICA) if cdc_backlog_total else 0
    desired = max(from_high, from_cdc)
    return max(MIN_REPLICAS, min(MAX_REPLICAS, desired))


def decide_next_replicas(current_replicas: int, desired_replicas: int,
                          state: dict, now: float) -> tuple[int, dict]:
    """Hysteresis/cooldown gate around ``compute_desired_replicas``'s
    output.

    ``state`` is a plain dict with keys ``direction`` ("up"/"down"/None),
    ``consecutive`` (int), ``last_scale_ts`` (float) — round-tripped to/
    from Redis by the caller (a hash) so this function has zero I/O.

    Returns ``(next_replicas, new_state)``. ``next_replicas`` equals
    ``current_replicas`` unchanged unless the gate opens (enough
    consecutive same-direction ticks AND the cooldown has elapsed since
    the last actual scale action).
    """
    current_replicas = max(MIN_REPLICAS, min(MAX_REPLICAS, int(current_replicas or 0)))
    desired_replicas = max(MIN_REPLICAS, min(MAX_REPLICAS, int(desired_replicas or 0)))
    last_scale_ts = float(state.get("last_scale_ts") or 0)

    if desired_replicas == current_replicas:
        # Signal agrees with reality — reset any pending direction so a
        # brief agreement doesn't get "credited" toward a later opposite
        # move once the signal shifts again.
        return current_replicas, {"direction": None, "consecutive": 0,
                                   "last_scale_ts": last_scale_ts}

    direction = "up" if desired_replicas > current_replicas else "down"
    prior_direction = state.get("direction")
    consecutive = (int(state.get("consecutive") or 0) + 1) if prior_direction == direction else 1

    required = SCALE_UP_CONSECUTIVE_TICKS if direction == "up" else SCALE_DOWN_CONSECUTIVE_TICKS
    cooldown_ok = (now - last_scale_ts) >= SCALE_COOLDOWN_S

    if consecutive >= required and cooldown_ok:
        return desired_replicas, {"direction": None, "consecutive": 0, "last_scale_ts": now}

    return current_replicas, {"direction": direction, "consecutive": consecutive,
                               "last_scale_ts": last_scale_ts}


# ---------------------------------------------------------------------------
# I/O helpers (Redis signal reads, K8s replica patch). Kept thin and
# separate from the pure functions above.
# ---------------------------------------------------------------------------

def _read_high_queue_depth(r) -> int:
    try:
        return int(r.llen(HIGH_QUEUE_KEY) or 0)
    except Exception:
        log.warning("transform_scaler: LLEN %s failed — treating as 0", HIGH_QUEUE_KEY, exc_info=True)
        return 0


def _read_cdc_backlog_total(r) -> int:
    """Sum XPENDING-summary counts across every connection's CDC stream.

    Best-effort per-connection: one stream/connection failing (stream
    doesn't exist yet, group not created yet, transient Redis error) is
    skipped rather than aborting the whole aggregate.
    """
    total = 0
    try:
        connection_ids = r.smembers(CDC_ACTIVE_CONNECTIONS_SET) or set()
    except Exception:
        log.warning("transform_scaler: SMEMBERS %s failed — treating CDC backlog as 0",
                    CDC_ACTIVE_CONNECTIONS_SET, exc_info=True)
        return 0
    for cid in connection_ids:
        cid = cid.decode() if isinstance(cid, bytes) else cid
        stream_key = f"{CDC_STREAM_KEY_PREFIX}{cid}"
        try:
            summary = r.xpending(stream_key, CDC_CONSUMER_GROUP)
        except Exception:
            continue
        if not summary:
            continue
        # redis-py's xpending (summary form) returns a dict with a
        # "pending" key when decode_responses is set; be defensive about
        # both dict-like and tuple-like shapes across redis-py versions.
        try:
            pending = summary.get("pending", 0) if hasattr(summary, "get") else summary[0]
        except Exception:
            pending = 0
        total += int(pending or 0)
    return total


def _current_replicas(apps_v1, namespace: str, name: str) -> "int | None":
    try:
        dep = apps_v1.read_namespaced_deployment(name, namespace)
        return int(dep.spec.replicas or 0)
    except Exception:
        log.warning("transform_scaler: could not read current replicas for %s/%s",
                    namespace, name, exc_info=True)
        return None


def _patch_replicas(apps_v1, namespace: str, name: str, replicas: int) -> bool:
    try:
        apps_v1.patch_namespaced_deployment(name, namespace, {"spec": {"replicas": replicas}})
        return True
    except Exception:
        log.exception("transform_scaler: failed to patch replicas=%d for %s/%s",
                      replicas, namespace, name)
        return False


def _load_state(r) -> dict:
    try:
        raw = r.hgetall(STATE_KEY) or {}
    except Exception:
        return {}
    out: dict = {}
    for k, v in raw.items():
        k = k.decode() if isinstance(k, bytes) else k
        v = v.decode() if isinstance(v, bytes) else v
        out[k] = v
    return out


def _save_state(r, state: dict) -> None:
    try:
        payload = {
            "direction": state.get("direction") or "",
            "consecutive": str(state.get("consecutive") or 0),
            "last_scale_ts": str(state.get("last_scale_ts") or 0),
        }
        r.hset(STATE_KEY, mapping=payload)
    except Exception:
        log.warning("transform_scaler: failed to persist scaler state", exc_info=True)


class TransformScalerService:
    """Background reconcile loop giving control-plane direct authority over
    ``fusion-transform-worker``'s replica count (both directions).

    Usage (mirrors ``app/services/scheduler.py::SchedulerService`` exactly):

        scaler = TransformScalerService(redis_client=_redis)
        task = asyncio.create_task(scaler.run(), name="transform-scaler")
        ...
        task.cancel()
    """

    def __init__(self, redis_client=None) -> None:
        """``redis_client`` is an optional ``redis.asyncio.Redis`` used
        ONLY for leader election (mirrors SchedulerService). The tick
        itself uses a separate SYNC redis client (``resource_ledger.
        get_redis_client()``) run in a thread-pool executor, since the
        signal reads (LLEN, SMEMBERS, XPENDING) and the K8s patch are all
        blocking I/O — matches ``committer_provisioner.py``'s own
        "blocking I/O -> offload to a thread" comment in
        ``app/api/connections.py``.
        """
        self._redis = redis_client
        self._leader_election = None
        self._running = False

    async def run(self) -> None:
        self._running = True
        if self._redis is not None:
            from app.utils.leader_election import RedisLeaderElection
            self._leader_election = RedisLeaderElection(self._redis, key=LEADER_KEY)

        log.info("TransformScalerService starting (tick=%ds, min=%d, max=%d)",
                  TICK_INTERVAL_S, MIN_REPLICAS, MAX_REPLICAS)
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("TransformScalerService tick error")
            await asyncio.sleep(TICK_INTERVAL_S)
        log.info("TransformScalerService stopped")

    def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        if self._leader_election is not None:
            if not await self._leader_election.is_leader():
                log.debug("TransformScalerService: not leader — skipping tick")
                return
        await asyncio.get_event_loop().run_in_executor(None, self._reconcile_once)

    def _reconcile_once(self) -> None:
        """Synchronous reconcile tick — runs in a thread-pool executor."""
        from app.services.committer_provisioner import _load_k8s, _get_shared_api_client, _current_namespace
        from app.services.resource_ledger import get_redis_client

        k8s, _ = _load_k8s()
        if k8s is None:
            log.debug("TransformScalerService: kubernetes client unavailable — skipping tick")
            return

        r = get_redis_client()
        high_depth = _read_high_queue_depth(r)
        cdc_backlog = _read_cdc_backlog_total(r)
        desired = compute_desired_replicas(high_depth, cdc_backlog)

        namespace = _current_namespace()
        api_client = _get_shared_api_client(k8s)
        apps_v1 = k8s.AppsV1Api(api_client)

        current = _current_replicas(apps_v1, namespace, DEPLOYMENT_NAME)
        if current is None:
            log.debug("TransformScalerService: deployment %s/%s not found yet — skipping tick",
                      namespace, DEPLOYMENT_NAME)
            return

        state = _load_state(r)
        now = time.time()
        next_replicas, new_state = decide_next_replicas(current, desired, state, now)
        _save_state(r, new_state)

        if next_replicas != current:
            ok = _patch_replicas(apps_v1, namespace, DEPLOYMENT_NAME, next_replicas)
            log.info(
                "TransformScalerService: replicas %d -> %d (patched=%s) "
                "[high_queue=%d cdc_backlog=%d desired=%d]",
                current, next_replicas, ok, high_depth, cdc_backlog, desired,
            )
        else:
            log.debug(
                "TransformScalerService: no change (replicas=%d desired=%d) "
                "[high_queue=%d cdc_backlog=%d]",
                current, desired, high_depth, cdc_backlog,
            )

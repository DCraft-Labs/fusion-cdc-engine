"""
Phase 3b: control-plane ongoing/continuous resizing of the per-connection
Iceberg committer (Phase 1's consolidated ``IcebergCommitter``), via TWO
independent levers:

  1. Cheap, frequent, no-restart: the committer's own ``add_files()``
     concurrency (``IcebergCommitter.add_files_max_workers`` —
     see ``transform-worker/iceberg_committer.py``'s
     ``_refresh_add_files_concurrency``/``concurrency_key``). Written to a
     per-connection Redis key EVERY tick; the running committer process
     polls it once per drain cycle and updates itself in place — no pod
     restart. This is the ONLY real per-cycle concurrency knob the
     consolidated committer has: ``run_cycle()``/``run_loop()`` drain a
     connection's tables strictly SEQUENTIALLY (a plain
     ``for t in self.table_names`` loop), so "internal concurrency" does
     NOT mean cross-table parallelism (there isn't any) — it means how many
     files ``_add_files_fast``'s ``ThreadPoolExecutor`` builds
     concurrently per commit batch.

  2. Expensive, infrequent, restart-causing: the committer Deployment's
     ``resources.requests``/``limits`` (CPU/memory), patched via the
     Kubernetes API on a much coarser cadence, gated by hysteresis so a
     single noisy tick can't trigger a rolling restart (the committer
     Deployment uses ``strategy.type: Recreate`` —
     see ``committer_provisioner.py`` — so ANY resource patch briefly stops
     the committer entirely, not just a rolling replace).

Both levers are driven by the SAME signal ``committer_provisioner.py``'s
own liveness/readiness exec probes already use: the summed
``LLEN fusion:iceberg-pending-files:{connection_id}:{table}`` drain-lag
across every table a connection's committer owns (see that module's
``lag_check`` local-variable construction, ~lines 368-380) — reused here
rather than inventing a new signal, per the task's explicit instruction.

Phase 3a's admission-control system (``resource_admission.py``) already
computes the STARTING tier/footprint once at connection-creation/
activation time. This module is the ONGOING reassessment after that: it
reads whatever CPU/memory the committer Deployment is ACTUALLY running
right now (not the original admission decision) and adjusts from there,
so it stays correct even if an operator hand-edited the Deployment or the
admission math changes independently.

Runs as an asyncio background task from ``app/main.py``'s lifespan, gated
by Redis leader election (mirrors ``app/services/scheduler.py``).
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time

from app.services.resource_admission import TIER_BASE_CPU_MILLIS, TIER_BASE_MEM_MI

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Drain-lag thresholds — anchored on committer_provisioner.py's own
# liveness/readiness lag thresholds (imported, not re-guessed) so "over-
# provisioned"/"under-provisioned" here means the same thing as "about to
# fail its own readiness/liveness probe" there.
# ---------------------------------------------------------------------------
def _readiness_liveness_thresholds() -> tuple[int, int]:
    try:
        from app.services.committer_provisioner import (
            _READINESS_LAG_THRESHOLD, _LIVENESS_LAG_THRESHOLD,
        )
        return _READINESS_LAG_THRESHOLD, _LIVENESS_LAG_THRESHOLD
    except Exception:
        return 1000, 5000


_READY_THRESHOLD, _LIVE_THRESHOLD = _readiness_liveness_thresholds()

OVER_LAG_THRESHOLD = int(os.environ.get("COMMITTER_RESIZE_OVER_LAG", str(_READY_THRESHOLD)))
UNDER_LAG_THRESHOLD = int(os.environ.get("COMMITTER_RESIZE_UNDER_LAG", "50"))

# ---------------------------------------------------------------------------
# Lever 1: add_files() concurrency — cheap, frequent, no restart. Bounds
# mirror iceberg_committer.py's own defensive read-side clamps
# (_CONCURRENCY_MIN/_CONCURRENCY_MAX) so control-plane never writes a value
# the committer would just re-clamp anyway.
# ---------------------------------------------------------------------------
CONCURRENCY_MIN = int(os.environ.get("COMMITTER_RESIZE_CONCURRENCY_MIN", "2"))
CONCURRENCY_MAX = int(os.environ.get("COMMITTER_RESIZE_CONCURRENCY_MAX", "32"))
CONCURRENCY_LAG_CEILING = int(os.environ.get("COMMITTER_RESIZE_CONCURRENCY_LAG_CEILING", str(_LIVE_THRESHOLD)))

# ---------------------------------------------------------------------------
# Lever 2: CPU/memory — expensive, infrequent, restart-causing (Recreate
# strategy). RESIZE_CONSECUTIVE_CYCLES + RESIZE_COOLDOWN_S together gate
# this so a resource patch (and the restart it causes) only fires after
# SUSTAINED over/under-provisioning, never on a single noisy tick.
# ---------------------------------------------------------------------------
RESIZE_STEP_FACTOR = float(os.environ.get("COMMITTER_RESIZE_STEP_FACTOR", "1.5"))
RESIZE_CONSECUTIVE_CYCLES = int(os.environ.get("COMMITTER_RESIZE_CONSECUTIVE_CYCLES", "6"))
RESIZE_COOLDOWN_S = int(os.environ.get("COMMITTER_RESIZE_COOLDOWN_S", "600"))
# Limits ride along with requests at this multiplier (mirrors the original
# committer_provisioner.py defaults' own request:limit ratio of roughly
# 250m:2000m CPU (8x) / 512Mi:2048Mi memory (4x) — kept as one shared,
# simpler multiplier rather than two separate ones).
LIMIT_MULTIPLIER = float(os.environ.get("COMMITTER_RESIZE_LIMIT_MULTIPLIER", "4"))

TICK_INTERVAL_S = int(os.environ.get("COMMITTER_RESIZER_TICK_INTERVAL_S", "20"))
LEADER_KEY = "fusion:committer-resizer:leader"


def _state_key(connection_id: str) -> str:
    return f"fusion:committer-resizer:state:{connection_id}"


def _concurrency_key(connection_id: str) -> str:
    """Must match transform-worker/iceberg_committer.py's
    ``concurrency_key()`` exactly (duplicated as a literal for the same
    cross-service reason documented in transform_scaler.py)."""
    return f"fusion:iceberg-committer-concurrency:{connection_id}"


# ---------------------------------------------------------------------------
# Pure decision logic (zero I/O — unit-testable).
# ---------------------------------------------------------------------------

def compute_desired_concurrency(drain_lag: int) -> int:
    """Drain lag -> desired add_files() concurrency, linearly interpolated
    between CONCURRENCY_MIN (lag=0) and CONCURRENCY_MAX (lag >=
    CONCURRENCY_LAG_CEILING), clamped. Applied every tick with no
    hysteresis — this lever is explicitly the "cheap, frequent" one, and a
    hot-reloaded in-process setting doesn't cause a restart, so there is no
    thrashing cost to guard against the way there is for replicas/resources.
    """
    drain_lag = max(0, int(drain_lag or 0))
    if CONCURRENCY_LAG_CEILING <= 0:
        return CONCURRENCY_MIN
    frac = min(1.0, drain_lag / CONCURRENCY_LAG_CEILING)
    desired = CONCURRENCY_MIN + frac * (CONCURRENCY_MAX - CONCURRENCY_MIN)
    return max(CONCURRENCY_MIN, min(CONCURRENCY_MAX, int(round(desired))))


def classify_drain_lag(drain_lag: int) -> str:
    """"over" (under-provisioned, needs more resources), "under"
    (over-provisioned, can give resources back), or "ok" (leave alone)."""
    drain_lag = max(0, int(drain_lag or 0))
    if drain_lag >= OVER_LAG_THRESHOLD:
        return "over"
    if drain_lag <= UNDER_LAG_THRESHOLD:
        return "under"
    return "ok"


def decide_next_resources(current_cpu_millis: int, current_mem_mi: int,
                           signal: str, state: dict, now: float,
                           ) -> tuple[int, int, dict]:
    """Hysteresis/cooldown gate around a one-notch CPU/mem resize.

    ``state`` is a plain dict with keys ``signal`` (last non-"ok" signal),
    ``consecutive`` (int), ``last_resize_ts`` (float) — round-tripped
    to/from Redis by the caller. Returns
    ``(next_cpu_millis, next_mem_mi, new_state)``; the CPU/mem values are
    unchanged from the inputs unless the gate opens.
    """
    current_cpu_millis = max(1, int(current_cpu_millis or 1))
    current_mem_mi = max(1, int(current_mem_mi or 1))
    last_resize_ts = float(state.get("last_resize_ts") or 0)

    if signal not in ("over", "under"):
        return current_cpu_millis, current_mem_mi, {
            "signal": None, "consecutive": 0, "last_resize_ts": last_resize_ts,
        }

    prior_signal = state.get("signal")
    consecutive = (int(state.get("consecutive") or 0) + 1) if prior_signal == signal else 1
    cooldown_ok = (now - last_resize_ts) >= RESIZE_COOLDOWN_S

    if consecutive >= RESIZE_CONSECUTIVE_CYCLES and cooldown_ok:
        floor_cpu, floor_mem = TIER_BASE_CPU_MILLIS["S"], TIER_BASE_MEM_MI["S"]
        ceil_cpu, ceil_mem = TIER_BASE_CPU_MILLIS["XL"], TIER_BASE_MEM_MI["XL"]
        if signal == "over":
            next_cpu = min(ceil_cpu, round(current_cpu_millis * RESIZE_STEP_FACTOR))
            next_mem = min(ceil_mem, round(current_mem_mi * RESIZE_STEP_FACTOR))
        else:
            next_cpu = max(floor_cpu, round(current_cpu_millis / RESIZE_STEP_FACTOR))
            next_mem = max(floor_mem, round(current_mem_mi / RESIZE_STEP_FACTOR))
        return int(next_cpu), int(next_mem), {
            "signal": None, "consecutive": 0, "last_resize_ts": now,
        }

    return current_cpu_millis, current_mem_mi, {
        "signal": signal, "consecutive": consecutive, "last_resize_ts": last_resize_ts,
    }


# ---------------------------------------------------------------------------
# Quantity parsing helpers (Kubernetes resource strings <-> plain numbers).
# ---------------------------------------------------------------------------

def parse_cpu_millis(value: "str | None") -> "int | None":
    if not value:
        return None
    value = str(value).strip()
    if value.endswith("m"):
        try:
            return int(float(value[:-1]))
        except ValueError:
            return None
    try:
        return int(round(float(value) * 1000))
    except ValueError:
        return None


_MEM_UNIT_MULTIPLIERS_TO_MI = {
    "Ei": 1024 ** 5 / (1024 ** 2), "Pi": 1024 ** 4 / (1024 ** 2),
    "Ti": 1024 ** 3 / (1024 ** 2), "Gi": 1024, "Mi": 1, "Ki": 1 / 1024,
}


def parse_mem_mi(value: "str | None") -> "int | None":
    if not value:
        return None
    value = str(value).strip()
    m = re.match(r"^([0-9.]+)([A-Za-z]*)$", value)
    if not m:
        return None
    num, unit = m.groups()
    try:
        num = float(num)
    except ValueError:
        return None
    if not unit:
        # Bytes.
        return int(round(num / (1024 ** 2)))
    if unit in _MEM_UNIT_MULTIPLIERS_TO_MI:
        return int(round(num * _MEM_UNIT_MULTIPLIERS_TO_MI[unit]))
    return None


# ---------------------------------------------------------------------------
# I/O helpers.
# ---------------------------------------------------------------------------

def _drain_lag_for_tables(r, connection_id: str, table_names: list) -> int:
    """Sum LLEN across every one of the connection's pending-files keys —
    the EXACT same key format (and same signal) as
    committer_provisioner.py's own liveness/readiness lag_check."""
    total = 0
    for t in table_names:
        key = f"fusion:iceberg-pending-files:{connection_id}:{t}"
        try:
            total += int(r.llen(key) or 0)
        except Exception:
            continue
    return total


def _load_state(r, connection_id: str) -> dict:
    try:
        raw = r.hgetall(_state_key(connection_id)) or {}
    except Exception:
        return {}
    out: dict = {}
    for k, v in raw.items():
        k = k.decode() if isinstance(k, bytes) else k
        v = v.decode() if isinstance(v, bytes) else v
        out[k] = v
    return out


def _save_state(r, connection_id: str, state: dict) -> None:
    try:
        r.hset(_state_key(connection_id), mapping={
            "signal": state.get("signal") or "",
            "consecutive": str(state.get("consecutive") or 0),
            "last_resize_ts": str(state.get("last_resize_ts") or 0),
        })
    except Exception:
        log.warning("committer_resizer: failed to persist resize state for connection=%s",
                    connection_id, exc_info=True)


def _active_iceberg_connections(session):
    """Active, non-deleted connections whose destination is Iceberg — the
    population this resizer manages. Mirrors the same
    ``destination.connector_definition.connector_type`` path
    ``app/api/connections.py`` already uses to decide whether to call
    ``ensure_committer`` in the first place."""
    from app.models.connection import Connection

    connections = (
        session.query(Connection)
        .filter(Connection.status == "active", Connection.is_deleted == False)  # noqa: E712
        .all()
    )
    out = []
    for c in connections:
        dest = getattr(c, "destination", None)
        connector = getattr(dest, "connector_definition", None) if dest else None
        connector_type = (getattr(connector, "connector_type", "") or "").lower()
        if connector_type == "iceberg":
            out.append(c)
    return out


def _table_names_for_connection(connection) -> list:
    tables = []
    seen = set()
    for stream in getattr(connection, "streams", None) or []:
        if not getattr(stream, "is_enabled", True):
            continue
        name = stream.destination_table_name or stream.source_table_name
        if name and name not in seen:
            seen.add(name)
            tables.append(name)
    return tables


def _current_committer_resources(apps_v1, namespace: str, committer_name: str):
    """Returns (cpu_millis, mem_mi) actually running on the committer
    Deployment right now, or (None, None) if it can't be read. Reading the
    LIVE Deployment (not the original admission-control decision) is the
    deliberate "ongoing reassessment starts from reality" design — see
    module docstring."""
    try:
        dep = apps_v1.read_namespaced_deployment(committer_name, namespace)
        containers = dep.spec.template.spec.containers
        for c in containers:
            if c.name == "iceberg-committer":
                reqs = (c.resources.requests or {}) if c.resources else {}
                cpu = parse_cpu_millis(reqs.get("cpu"))
                mem = parse_mem_mi(reqs.get("memory"))
                if cpu and mem:
                    return cpu, mem
    except Exception:
        log.debug("committer_resizer: could not read current resources for %s/%s",
                  namespace, committer_name, exc_info=True)
    return None, None


def _patch_committer_resources(apps_v1, namespace: str, committer_name: str,
                                cpu_millis: int, mem_mi: int) -> bool:
    cpu_req = f"{cpu_millis}m"
    mem_req = f"{mem_mi}Mi"
    cpu_lim = f"{max(cpu_millis, round(cpu_millis * LIMIT_MULTIPLIER))}m"
    mem_lim = f"{max(mem_mi, round(mem_mi * LIMIT_MULTIPLIER))}Mi"
    body = {
        "spec": {"template": {"spec": {"containers": [{
            "name": "iceberg-committer",
            "resources": {
                "requests": {"cpu": cpu_req, "memory": mem_req},
                "limits": {"cpu": cpu_lim, "memory": mem_lim},
            },
        }]}}}
    }
    try:
        apps_v1.patch_namespaced_deployment(committer_name, namespace, body)
        return True
    except Exception:
        log.exception("committer_resizer: failed to patch resources for %s/%s",
                      namespace, committer_name)
        return False


class CommitterResizerService:
    """Background reconcile loop giving control-plane ongoing authority
    over every active Iceberg connection's committer: per-cycle
    add_files() concurrency (hot-reload, no restart) and coarser CPU/
    memory resizing (hysteresis-gated, causes a restart).

    Usage mirrors ``SchedulerService``/``TransformScalerService`` exactly.
    """

    def __init__(self, session_factory, redis_client=None) -> None:
        self._session_factory = session_factory
        self._redis = redis_client
        self._leader_election = None
        self._running = False

    async def run(self) -> None:
        self._running = True
        if self._redis is not None:
            from app.utils.leader_election import RedisLeaderElection
            self._leader_election = RedisLeaderElection(self._redis, key=LEADER_KEY)

        log.info("CommitterResizerService starting (tick=%ds)", TICK_INTERVAL_S)
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("CommitterResizerService tick error")
            await asyncio.sleep(TICK_INTERVAL_S)
        log.info("CommitterResizerService stopped")

    def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        if self._leader_election is not None:
            if not await self._leader_election.is_leader():
                log.debug("CommitterResizerService: not leader — skipping tick")
                return
        await asyncio.get_event_loop().run_in_executor(None, self._reconcile_once)

    def _reconcile_once(self) -> None:
        """Synchronous reconcile tick over every active Iceberg
        connection — runs in a thread-pool executor (DB + Redis + K8s are
        all blocking I/O here, matching the rest of this codebase's
        pattern for offloading blocking work off the event loop)."""
        from app.services.committer_provisioner import (
            _load_k8s, _get_shared_api_client, _current_namespace, _committer_name,
        )
        from app.services.resource_ledger import get_redis_client

        k8s, _ = _load_k8s()
        if k8s is None:
            log.debug("CommitterResizerService: kubernetes client unavailable — skipping tick")
            return
        release_name = os.environ.get("RELEASE_NAME")
        if not release_name:
            log.debug("CommitterResizerService: RELEASE_NAME not set — skipping tick")
            return

        r = get_redis_client()
        namespace = _current_namespace()
        api_client = _get_shared_api_client(k8s)
        apps_v1 = k8s.AppsV1Api(api_client)

        session = self._session_factory()
        try:
            connections = _active_iceberg_connections(session)
        except Exception:
            log.exception("CommitterResizerService: failed to load active iceberg connections")
            return
        finally:
            session.close()

        now = time.time()
        for connection in connections:
            connection_id = str(connection.connection_id)
            table_names = _table_names_for_connection(connection)
            if not table_names:
                continue
            committer_name = _committer_name(connection_id, release_name)

            drain_lag = _drain_lag_for_tables(r, connection_id, table_names)

            # Lever 1: concurrency — every tick, no hysteresis.
            desired_concurrency = compute_desired_concurrency(drain_lag)
            try:
                r.set(_concurrency_key(connection_id), str(desired_concurrency))
            except Exception:
                log.warning("CommitterResizerService: failed to write concurrency for connection=%s",
                            connection_id, exc_info=True)

            # Lever 2: CPU/memory — hysteresis-gated, restart-causing.
            current_cpu, current_mem = _current_committer_resources(apps_v1, namespace, committer_name)
            if current_cpu is None or current_mem is None:
                continue  # committer not provisioned yet / unreadable this tick
            signal = classify_drain_lag(drain_lag)
            state = _load_state(r, connection_id)
            next_cpu, next_mem, new_state = decide_next_resources(
                current_cpu, current_mem, signal, state, now)
            _save_state(r, connection_id, new_state)

            if next_cpu != current_cpu or next_mem != current_mem:
                ok = _patch_committer_resources(apps_v1, namespace, committer_name, next_cpu, next_mem)
                log.info(
                    "CommitterResizerService: connection=%s resources cpu=%dm->%dm mem=%dMi->%dMi "
                    "(patched=%s) [drain_lag=%d concurrency=%d]",
                    connection_id, current_cpu, next_cpu, current_mem, next_mem,
                    ok, drain_lag, desired_concurrency,
                )
            else:
                log.debug(
                    "CommitterResizerService: connection=%s no resource change "
                    "[drain_lag=%d concurrency=%d cpu=%dm mem=%dMi]",
                    connection_id, drain_lag, desired_concurrency, current_cpu, current_mem,
                )

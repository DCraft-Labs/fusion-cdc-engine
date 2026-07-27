"""
cdc-worker StatefulSet direct-scaling reconcile loop (source-count/tier
sized), both directions.

****************************************************************************
* NOT SAFE TO ENABLE IN PRODUCTION YET.                                    *
*                                                                          *
* Phase 2 (app/services/source_assignment.py) replaced the old always-    *
* empty assigned_worker_id filter with a REAL consistent-hash source-to-  *
* pod assignment. That fix has been code-reviewed and merged, but it has  *
* NOT been verified against a live Kubernetes cluster with a cdc-worker   *
* StatefulSet actually running at 2+ replicas -- nobody in the session    *
* that wrote this module had real cluster access to do that verification. *
*                                                                          *
* If this module's actual scale-up path fires against a real StatefulSet  *
* before that verification happens, and Phase 2's sharding has any bug    *
* that only surfaces under real load, the result is MySQL server_id       *
* collisions across pods (see source_assignment.py's module docstring)    *
* -- i.e. exactly the outage Phase 2 was written to prevent, except now   *
* self-inflicted by this autoscaler instead of a human running `kubectl   *
* scale`. That is why the real scaling action below is gated behind       *
* CDC_WORKER_DIRECT_SCALING_ENABLED, which MUST default to "false".       *
* Do not flip that default. Only flip the env var itself, in a real       *
* cluster, after Phase 2 has been watched running at 2+ ready replicas    *
* without server_id churn / duplicate-stream symptoms.                    *
****************************************************************************

While the flag is off (the default), ``reconcile_cdc_worker_replicas()``
still runs its full computation and LOGS what it would have done
(dry-run/observability mode) every reconcile cycle -- so whoever eventually
flips the flag on real assigned-source data has a log trail of desired
replica counts to sanity-check against first, rather than flying blind on
day one of enabling it.

Sizing model (judgment call -- see task write-up for the alternatives
considered):

  There is currently no real backlog/lag signal control-plane can read for
  cdc-worker sizing (cdc_stream_lag_seconds is emitted per (tenant, source,
  stream) by cdc-workers/cdc_worker/metrics.py, but nothing aggregates it
  into a control-plane-readable signal, and this codebase has ZERO existing
  Prometheus-QUERYING code anywhere -- only Prometheus metric emission via
  prometheus_client). Standing up a Prometheus HTTP-query client from
  scratch, for a first version of a not-yet-verified autoscaler, is more
  moving parts than this needs. So: desired replicas is sized from
  assigned-source COUNT, weighted by each source's heaviest associated
  connection's admission TIER (S/M/L/XL, from
  app.services.resource_admission -- reused, not reinvented) as a proxy for
  how much load that source's CDC capture is likely to place on its pod.
  This can be swapped for a real lag-based signal later without changing
  this module's public shape (compute_desired_replicas() takes a plain
  total_weight float either way).

Mirrors app/services/committer_provisioner.py's defensive K8s-import style
and REUSES its _load_k8s()/_get_shared_api_client() helpers directly (one
shared ApiClient per process, for the same OOM reason documented there)
rather than duplicating that plumbing a third time.
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from app.services import resource_admission
from app.services.committer_provisioner import (
    _current_namespace,
    _get_shared_api_client,
    _load_k8s,
)
from app.services.source_assignment import (
    rebalance_source_type_at_pod_count,
    statefulset_name_for_source_type,
    statefulset_source_type,
)

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import cost
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# THE feature flag. Default is "false" and MUST stay "false" by default --
# see the module docstring's warning banner for exactly why. Only an
# explicit CDC_WORKER_DIRECT_SCALING_ENABLED=true in the environment turns
# on real StatefulSet patching; anything else (unset, "false", typos) keeps
# this in dry-run/observability mode.
# ---------------------------------------------------------------------------
CDC_WORKER_DIRECT_SCALING_ENABLED = (
    os.environ.get("CDC_WORKER_DIRECT_SCALING_ENABLED", "false").lower() == "true"
)

# The three source-type StatefulSets this reconciles (matches
# helm/fusion-cdc/templates/cdc-workers.yaml's $sourceType loop values and
# source_assignment.py's _KNOWN_SOURCE_TYPES).
_SOURCE_TYPES: Tuple[str, ...] = ("mysql", "postgres", "mongodb")

# Availability floor / load ceiling for desired replica count. Tunable via
# env, same ad-hoc os.environ.get convention as committer_provisioner.py /
# source_assignment.py (this codebase doesn't route every flag through the
# central app/config.py Settings class -- see e.g. COMMITTER_* there).
CDC_WORKER_MIN_REPLICAS = int(os.environ.get("CDC_WORKER_MIN_REPLICAS", "1"))
CDC_WORKER_MAX_REPLICAS = int(os.environ.get("CDC_WORKER_MAX_REPLICAS", "10"))

# How much "weight" (see _tier_weight below) one pod is sized to carry
# before the reconciler asks for another one. 8.0 == "roughly 8 M-tier
# sources' worth of load per pod" as a first-version default; tune once
# real data exists (see the dry-run log trail this module leaves behind).
CDC_WORKER_TARGET_WEIGHT_PER_POD = float(
    os.environ.get("CDC_WORKER_TARGET_WEIGHT_PER_POD", "8.0")
)

# Hysteresis: a changed desired-replica value must be seen on this many
# CONSECUTIVE reconcile cycles before it's acted on (debounces one-off
# blips -- e.g. a source transitioning draft->active mid-cycle), AND at
# least this many seconds must have elapsed since the last actual scale
# action on that StatefulSet (prevents flapping: scale down, then
# immediately back up on transient noise, and vice versa). Both apply to
# scale up AND scale down.
CDC_WORKER_SCALE_DEBOUNCE_CYCLES = int(os.environ.get("CDC_WORKER_SCALE_DEBOUNCE_CYCLES", "2"))
CDC_WORKER_SCALE_COOLDOWN_SECONDS = int(os.environ.get("CDC_WORKER_SCALE_COOLDOWN_SECONDS", "600"))

# How often the background task (app/main.py) runs a full reconcile pass.
CDC_WORKER_RECONCILE_INTERVAL_SECONDS = int(
    os.environ.get("CDC_WORKER_RECONCILE_INTERVAL_SECONDS", "120")
)


# ---------------------------------------------------------------------------
# Tier weighting -- reuses app.services.resource_admission's existing S/M/L/
# XL tier constants (TIER_BASE_CPU_MILLIS) rather than inventing a second,
# competing set of tier boundaries. A source's weight is its HEAVIEST
# associated connection's tier weight (max, not sum): a source's own CDC
# capture cost (binlog/WAL/oplog read + routing) is driven by the
# underlying table(s) it captures from, not by how many downstream
# connections happen to consume that capture -- three connections reading
# the same big table don't triple the capture cost the way three DIFFERENT
# equally-big tables would. The heaviest connection's estimated row count
# is the best available proxy this codebase has today for "how big/busy is
# this source's data," so it anchors the source's weight.
# ---------------------------------------------------------------------------

def _tier_weight(tier: str) -> float:
    """Relative pod-load weight for an admission tier, derived from
    resource_admission.TIER_BASE_CPU_MILLIS (normalized to M == 1.0) so this
    stays in lockstep with that module's tuning rather than drifting as a
    separately hand-picked constant. With the defaults there (S=125,
    M=250, L=1000, XL=2000 CPU millis) this yields S=0.5, M=1.0, L=4.0,
    XL=8.0."""
    base = resource_admission.TIER_BASE_CPU_MILLIS.get(tier, resource_admission.TIER_BASE_CPU_MILLIS["M"])
    m = resource_admission.TIER_BASE_CPU_MILLIS["M"]
    return (base / m) if m else 1.0


def _connection_tier(connection) -> str:
    """Best-effort tier for one Connection. Duck-typed on `.resource_limits`
    (a dict, possibly None) so this works against real ORM rows and
    SimpleNamespace test doubles alike. Falls back to resolve_tier(None)
    ("M") when the connection never went through admission-confirm (e.g.
    legacy connections created before Phase 3a, or one that skipped the
    admission flow) -- same safe-middle-ground default resource_admission
    itself uses for an unknown estimate."""
    rl = getattr(connection, "resource_limits", None) or {}
    admission = rl.get("admission") or {}
    tier = admission.get("tier")
    if tier in resource_admission.TIERS:
        return tier
    return resource_admission.resolve_tier(None)


def source_weight(source) -> float:
    """Pod-load weight for one Source: the MAX tier weight across its
    non-deleted connections, or the M-equivalent baseline (1.0) if it has
    none yet (a freshly created source with no connections still needs to
    be captured once activated, so it isn't free)."""
    connections = [
        c for c in (getattr(source, "connections", None) or [])
        if not getattr(c, "is_deleted", False)
    ]
    if not connections:
        return _tier_weight(resource_admission.resolve_tier(None))
    return max(_tier_weight(_connection_tier(c)) for c in connections)


# ---------------------------------------------------------------------------
# Desired-replica-count computation -- pure, no I/O, unit-testable directly.
# ---------------------------------------------------------------------------

def compute_desired_replicas(
    total_weight: float,
    *,
    min_replicas: Optional[int] = None,
    max_replicas: Optional[int] = None,
    target_weight_per_pod: Optional[float] = None,
) -> int:
    """desired = ceil(total_weight / target_weight_per_pod), clamped to
    [min_replicas, max_replicas]. total_weight <= 0 (no assigned sources of
    this type yet) still returns min_replicas -- the StatefulSet stays up
    at its availability floor so it's ready to pick up the first source
    without a cold scale-up delay."""
    min_r = CDC_WORKER_MIN_REPLICAS if min_replicas is None else min_replicas
    max_r = CDC_WORKER_MAX_REPLICAS if max_replicas is None else max_replicas
    target = CDC_WORKER_TARGET_WEIGHT_PER_POD if target_weight_per_pod is None else target_weight_per_pod

    min_r = max(0, int(min_r))
    max_r = max(min_r, int(max_r))

    if total_weight <= 0:
        return min_r
    if target <= 0:
        # Misconfigured (target_weight_per_pod <= 0) — prefer over- to
        # under-provisioning, since scaling DOWN too far is the direction
        # that risks stranding sources; scaling UP too far just wastes
        # resources.
        return max_r
    raw = math.ceil(total_weight / target)
    return max(min_r, min(max_r, raw))


# ---------------------------------------------------------------------------
# Hysteresis / cooldown state (per StatefulSet name, per-process -- same
# "per-process is fine, rebalance/scale decisions are idempotent so
# redundant triggers across control-plane replicas are harmless" reasoning
# source_assignment.py's _last_known_ready_replicas already uses).
# ---------------------------------------------------------------------------
_scale_state: Dict[str, dict] = {}


def _should_apply_scale(statefulset_name: str, desired: int, current: int, now: float) -> Tuple[bool, str]:
    state = _scale_state.setdefault(
        statefulset_name, {"last_scale_ts": 0.0, "last_desired": None, "streak": 0}
    )

    if desired == current:
        state["last_desired"] = desired
        state["streak"] = 0
        return False, "already at desired replica count"

    if state["last_desired"] != desired:
        # Desired value changed since we last looked -- reset the debounce
        # streak and wait for it to be confirmed on subsequent cycles
        # before acting, rather than reacting to a single noisy sample.
        state["last_desired"] = desired
        state["streak"] = 1
        return False, (
            f"desired changed to {desired} (from {current}) — debouncing, "
            f"needs {CDC_WORKER_SCALE_DEBOUNCE_CYCLES} consecutive cycles"
        )

    state["streak"] += 1
    if state["streak"] < CDC_WORKER_SCALE_DEBOUNCE_CYCLES:
        return False, (
            f"desired={desired} confirmed {state['streak']}/"
            f"{CDC_WORKER_SCALE_DEBOUNCE_CYCLES} cycles — debouncing"
        )

    elapsed = now - state["last_scale_ts"]
    if elapsed < CDC_WORKER_SCALE_COOLDOWN_SECONDS:
        remaining = CDC_WORKER_SCALE_COOLDOWN_SECONDS - elapsed
        return False, f"cooldown active ({remaining:.0f}s remaining since last scale action)"

    return True, "ok"


def _record_scale_applied(statefulset_name: str, now: float) -> None:
    state = _scale_state.setdefault(
        statefulset_name, {"last_scale_ts": 0.0, "last_desired": None, "streak": 0}
    )
    state["last_scale_ts"] = now
    state["streak"] = 0


# ---------------------------------------------------------------------------
# DB query: total weight + count of active/draft sources for a source type.
# ---------------------------------------------------------------------------

def _weighted_source_count(db: "Session", source_type: str) -> Tuple[float, int]:
    """Sum of source_weight() over every active/draft, non-deleted source
    whose connector_type maps to `source_type`. Mirrors
    source_assignment.py's _rebalance()'s own query shape/filter exactly,
    so "which sources this reconciler counts" and "which sources Phase 2
    actually assigns to a pod" never disagree."""
    from sqlalchemy.orm import joinedload
    from app.models.source_destination import Source

    sources = (
        db.query(Source)
        .options(
            joinedload(Source.connector_definition),
            joinedload(Source.connections),
        )
        .filter(
            Source.is_deleted == False,  # noqa: E712
            Source.status.in_(["active", "draft"]),
        )
        .all()
    )

    total_weight = 0.0
    count = 0
    for s in sources:
        ctype = s.connector_definition.connector_type if s.connector_definition else None
        if statefulset_source_type(ctype) != source_type:
            continue
        total_weight += source_weight(s)
        count += 1
    return total_weight, count


# ---------------------------------------------------------------------------
# Per-source-type reconcile.
# ---------------------------------------------------------------------------

def _reconcile_source_type(
    db: "Session", k8s, apps_v1, source_type: str, release_name: str, namespace: str,
) -> dict:
    statefulset_name = statefulset_name_for_source_type(source_type, release_name)

    try:
        sts = apps_v1.read_namespaced_stateful_set(statefulset_name, namespace)
        current_replicas = int(sts.spec.replicas or 1)
    except Exception as exc:
        log.warning(
            "cdc_worker_autoscaler: could not read StatefulSet %s/%s — skipping this cycle: %s",
            namespace, statefulset_name, exc,
        )
        return {
            "source_type": source_type, "statefulset_name": statefulset_name,
            "skipped": True, "reason": f"StatefulSet unreadable: {exc}",
        }

    total_weight, source_count = _weighted_source_count(db, source_type)
    desired = compute_desired_replicas(total_weight)
    now = time.monotonic()
    should_apply, reason = _should_apply_scale(statefulset_name, desired, current_replicas, now)

    result = {
        "source_type": source_type,
        "statefulset_name": statefulset_name,
        "current_replicas": current_replicas,
        "desired_replicas": desired,
        "total_weight": round(total_weight, 3),
        "source_count": source_count,
        "would_apply": should_apply,
        "reason": reason,
        "enabled": CDC_WORKER_DIRECT_SCALING_ENABLED,
        "applied": False,
    }

    if not should_apply:
        log.info(
            "cdc_worker_autoscaler: [dry_run=%s] %s current=%d desired=%d weight=%.2f "
            "sources=%d — %s",
            not CDC_WORKER_DIRECT_SCALING_ENABLED, statefulset_name, current_replicas,
            desired, total_weight, source_count, reason,
        )
        return result

    if not CDC_WORKER_DIRECT_SCALING_ENABLED:
        log.info(
            "cdc_worker_autoscaler: DRY-RUN would scale %s from %d to %d replicas "
            "(weight=%.2f sources=%d) — CDC_WORKER_DIRECT_SCALING_ENABLED=false, not "
            "applying. See app/services/cdc_worker_autoscaler.py's module docstring "
            "for why this stays off until Phase 2's source-sharding fix is verified "
            "against a live multi-replica cluster.",
            statefulset_name, current_replicas, desired, total_weight, source_count,
        )
        return result

    # Real scale-DOWN: coordinate with Phase 2's rebalance mechanism FIRST.
    # StatefulSets always remove the HIGHEST ordinal pod(s) first, and
    # source_assignment.py's own rebalance-on-heartbeat trigger only fires
    # when a SURVIVING pod's heartbeat later observes the changed
    # ready_replicas count — that's up to one heartbeat interval (default
    # 30s) of sources sitting stranded on an assigned_worker_id that no
    # longer exists. Rebalancing at the FUTURE (smaller) pod_count BEFORE
    # patching replicas closes that gap: every active/draft source of this
    # type gets re-hashed onto ordinals [0, desired) right now, so by the
    # time the doomed high-ordinal pod(s) actually terminate, nothing was
    # ever relying on them post-scale-down.
    if desired < current_replicas:
        try:
            rebalance_result = rebalance_source_type_at_pod_count(db, source_type, desired)
            log.info(
                "cdc_worker_autoscaler: pre-scale-down rebalance for %s to pod_count=%d: %s",
                statefulset_name, desired, rebalance_result,
            )
            if not rebalance_result.get("rebalanced"):
                log.warning(
                    "cdc_worker_autoscaler: pre-scale-down rebalance did not complete "
                    "for %s (%s) — skipping scale-down this cycle rather than risk "
                    "stranding sources",
                    statefulset_name, rebalance_result.get("reason"),
                )
                result["reason"] = f"pre-scale-down rebalance incomplete: {rebalance_result.get('reason')}"
                return result
        except Exception:
            log.exception(
                "cdc_worker_autoscaler: pre-scale-down rebalance raised for %s — "
                "skipping scale-down this cycle",
                statefulset_name,
            )
            result["reason"] = "pre-scale-down rebalance raised an exception (see logs)"
            return result

    try:
        apps_v1.patch_namespaced_stateful_set(
            statefulset_name, namespace, {"spec": {"replicas": desired}}
        )
        _record_scale_applied(statefulset_name, now)
        result["applied"] = True
        log.info(
            "cdc_worker_autoscaler: scaled %s from %d to %d replicas (weight=%.2f sources=%d)",
            statefulset_name, current_replicas, desired, total_weight, source_count,
        )
    except Exception:
        log.exception(
            "cdc_worker_autoscaler: failed to patch replicas for %s", statefulset_name,
        )
        result["reason"] = "kubernetes patch failed (see logs)"

    return result


# ---------------------------------------------------------------------------
# Top-level entry point, called from the periodic background task
# (app/main.py).
# ---------------------------------------------------------------------------

def reconcile_cdc_worker_replicas(db: "Session") -> List[dict]:
    """Reconcile all three cdc-worker StatefulSets (mysql/postgres/mongodb).

    Runs its full computation and logs the result REGARDLESS of
    CDC_WORKER_DIRECT_SCALING_ENABLED — only the actual `patch_namespaced_
    stateful_set` call is gated. Never raises: a K8s/DB problem for one
    source type must not block the others or the caller's periodic loop.
    Returns [] (a no-op) in dev/local/test environments where RELEASE_NAME
    or the kubernetes client/credentials aren't available, mirroring
    source_assignment.py's / committer_provisioner.py's own degrade-to-
    no-op convention.
    """
    release_name = os.environ.get("RELEASE_NAME")
    if not release_name:
        log.debug("cdc_worker_autoscaler: RELEASE_NAME not set — skipping (dev/local)")
        return []

    k8s, _ = _load_k8s()
    if k8s is None:
        log.debug("cdc_worker_autoscaler: kubernetes client/credentials unavailable — skipping")
        return []

    namespace = _current_namespace()
    api_client = _get_shared_api_client(k8s)
    apps_v1 = k8s.AppsV1Api(api_client)

    results = []
    for source_type in _SOURCE_TYPES:
        try:
            results.append(
                _reconcile_source_type(db, k8s, apps_v1, source_type, release_name, namespace)
            )
        except Exception:
            log.exception(
                "cdc_worker_autoscaler: reconcile failed for source_type=%s", source_type,
            )
    return results

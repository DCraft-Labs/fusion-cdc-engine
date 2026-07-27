"""
Source-to-worker-pod assignment (Phase 2 — real cdc-worker sharding).

Each cdc-worker source-type StatefulSet (mysql/postgres/mongodb) can run N
replica pods, each with a stable ordinal pod name (e.g.
``<release>-cdc-worker-mysql-0``, ``...-mysql-1``, ...). ``WORKER_ID`` is
already set to that pod name via the Downward API in
``helm/fusion-cdc/templates/cdc-workers.yaml``
(``fieldRef: fieldPath: metadata.name``), and every worker already sends it
both in the URL path and the ``X-Worker-ID`` header of
``GET /api/v1/internal/workers/{worker_id}/sources``
(see ``cdc-workers/cdc_worker/worker.py::_fetch_sources``).

Previously ``assigned_worker_id`` was READ in
``app/api/internal.py::get_worker_sources`` but never WRITTEN anywhere, so
that filter always produced an empty list and the endpoint silently fell
back to returning ALL active sources to EVERY pod — harmless at the
default of 1 replica per StatefulSet, but the instant anyone scales to 2+
every pod starts a coroutine for every source (for MySQL specifically,
``connectors/mysql.py``'s deterministic ``server_id`` then collides across
pods and MySQL kills the older binlog connection when the newer one
registers).

This module is the real assignment mechanism: for every active/draft
source, it decides which pod ordinal of that source-type's StatefulSet
owns it — via consistent hashing over the StatefulSet's CURRENTLY READY
pod count (queried live from the Kubernetes API) — and persists the
decision onto ``Source.config["assigned_worker_id"]``.

Mirrors ``app/services/committer_provisioner.py``'s defensive K8s-import
style (``_load_k8s()``): the ``kubernetes`` package/credentials may not be
available (dev/test/local docker-compose), in which case this degrades to
a no-op and ``get_worker_sources()`` keeps its original single-worker
fallback behavior untouched.
"""
from __future__ import annotations

import bisect
import hashlib
import logging
import os
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import cost
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

# Number of virtual nodes placed on the hash ring per pod ordinal. A higher
# value spreads sources more evenly across pods (lower variance) at the
# cost of a larger ring to build/search; 100 is a conventional default for
# consistent hashing and is cheap at the pod counts these StatefulSets run
# (single digits to low tens).
_VIRTUAL_NODES_PER_POD = 100

# The three source-type StatefulSets this control-plane provisions (see
# helm/fusion-cdc/templates/cdc-workers.yaml + values.yaml's cdcWorkers.*).
_KNOWN_SOURCE_TYPES: Tuple[str, ...] = ("mysql", "postgres", "mongodb")

# Connector-type (as stored on ConnectorDefinition.connector_type, e.g.
# "mysql" / "postgresql" / "mongodb") -> StatefulSet source-type key (as
# used in the StatefulSet name "<release>-cdc-worker-<sourceType>"). The DB
# value and the Helm value disagree for Postgres ("postgresql" vs
# "postgres"), so this mapping exists to reconcile the two.
_CONNECTOR_TYPE_TO_SOURCE_TYPE: Dict[str, str] = {
    "mysql": "mysql",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mongodb": "mongodb",
    "mongo": "mongodb",
}


def statefulset_source_type(connector_type: Optional[str]) -> Optional[str]:
    """Map a Source's connector_type to its StatefulSet source-type key
    ("mysql" / "postgres" / "mongodb"), or None if unrecognized."""
    return _CONNECTOR_TYPE_TO_SOURCE_TYPE.get((connector_type or "").lower())


def statefulset_name_for_source_type(source_type: str, release_name: str) -> str:
    """Matches helm/fusion-cdc/templates/cdc-workers.yaml's StatefulSet
    metadata.name EXACTLY: ``{{ $root.Release.Name }}-cdc-worker-{{ $sourceType }}``.
    """
    return f"{release_name}-cdc-worker-{source_type}"


def worker_id_for_ordinal(statefulset_name: str, ordinal: int) -> str:
    """The pod name Kubernetes gives ordinal N of a StatefulSet — same value
    the pod's own WORKER_ID env var resolves to via the Downward API."""
    return f"{statefulset_name}-{ordinal}"


def parse_worker_id(worker_id: str) -> Optional[Tuple[str, int]]:
    """Split a StatefulSet pod name ("<statefulset-name>-<ordinal>") into
    (statefulset_name, ordinal). Returns None if it doesn't look like one
    (e.g. a hand-set WORKER_ID like "worker-1" in dev/local mode)."""
    if not worker_id:
        return None
    idx = worker_id.rfind("-")
    if idx == -1:
        return None
    suffix = worker_id[idx + 1:]
    if not suffix.isdigit():
        return None
    return worker_id[:idx], int(suffix)


def source_type_for_worker_id(worker_id: str) -> Optional[str]:
    """Best-effort: figure out which source-type StatefulSet a worker_id
    belongs to, purely from its own name (no K8s call). Returns None when
    worker_id doesn't look like one of our StatefulSet pod names (dev/local
    WORKER_ID values, e.g. the "worker-1" default in
    cdc-workers/cdc_worker/config.py)."""
    parsed = parse_worker_id(worker_id)
    if parsed is None:
        return None
    statefulset_name, _ordinal = parsed
    for source_type in _KNOWN_SOURCE_TYPES:
        if statefulset_name.endswith(f"-cdc-worker-{source_type}"):
            return source_type
    return None


# ---------------------------------------------------------------------------
# Consistent hashing (pure, no I/O — deliberately free of heavy imports so
# it can be unit tested directly without kubernetes/sqlalchemy installed).
# ---------------------------------------------------------------------------

def _hash_int(key: str) -> int:
    """Stable, process-independent hash. Python's built-in hash() is
    randomized per-process for strings (PYTHONHASHSEED) unless disabled,
    which would make the assignment control-plane computes disagree from
    one process/run to the next. md5 is used purely as a stable digest,
    not for any security property."""
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


_ring_cache: Dict[int, Tuple[List[int], Dict[int, int]]] = {}


def _build_ring(pod_count: int) -> Tuple[List[int], Dict[int, int]]:
    """Build (sorted virtual-node hash positions, hash -> pod ordinal) for a
    ring sized to `pod_count` pods. Cached per pod_count: it's pure and
    StatefulSet replica counts change far less often than sources get
    (re)assigned."""
    cached = _ring_cache.get(pod_count)
    if cached is not None:
        return cached
    owner_by_hash: Dict[int, int] = {}
    for ordinal in range(pod_count):
        for v in range(_VIRTUAL_NODES_PER_POD):
            h = _hash_int(f"pod-{ordinal}-vnode-{v}")
            owner_by_hash[h] = ordinal
    sorted_hashes = sorted(owner_by_hash.keys())
    result = (sorted_hashes, owner_by_hash)
    _ring_cache[pod_count] = result
    return result


def select_assigned_sources(sources: List[object], worker_id: str) -> List[object]:
    """Pure filtering decision used by
    ``app/api/internal.py::get_worker_sources``: given ALL active/draft
    sources and the calling worker's own pod name, return just the ones
    this worker owns.

    Duck-typed on `sources`: each element needs a `.config` dict (may be
    None/missing "assigned_worker_id") and a `.connector_definition` with a
    `.connector_type` (may be None) — real ``Source`` ORM rows and plain
    ``SimpleNamespace`` test doubles both satisfy this, which is what lets
    this be unit tested without a database.

    Real per-pod filtering only kicks in once assignment has actually run
    for this worker's source-type (at least one of ITS sources carries an
    assigned_worker_id). Until then — dev/local/docker-compose without a
    real Kubernetes API, or a WORKER_ID that isn't a StatefulSet ordinal pod
    name — this returns ALL sources unfiltered (single-worker mode), exactly
    matching the endpoint's original fallback behavior.
    """
    def _source_type(s) -> Optional[str]:
        connector_definition = getattr(s, "connector_definition", None)
        ctype = getattr(connector_definition, "connector_type", None) if connector_definition else None
        return statefulset_source_type(ctype)

    def _assigned_worker_id(s) -> Optional[str]:
        cfg = getattr(s, "config", None) or {}
        return cfg.get("assigned_worker_id")

    my_source_type = source_type_for_worker_id(worker_id)
    relevant = [s for s in sources if my_source_type and _source_type(s) == my_source_type]
    assignment_active = bool(relevant) and any(_assigned_worker_id(s) for s in relevant)

    if assignment_active:
        # Real assignment data exists for this worker's source-type — trust
        # it fully, including an empty result (this pod may legitimately own
        # zero sources, e.g. more pods than sources).
        return [s for s in relevant if _assigned_worker_id(s) == worker_id]
    return list(sources)  # dev / single-worker mode (unfiltered, as before)


def assign_pod_ordinal(source_id: str, pod_count: int) -> int:
    """Return the pod ordinal (0..pod_count-1) that owns `source_id`, via
    consistent hashing over a ring sized to `pod_count`. Deterministic and
    stable: the same (source_id, pod_count) always maps to the same
    ordinal, and when pod_count changes only ~1/pod_count of sources remap
    (the whole point of consistent hashing over plain modulo hashing)."""
    pod_count = max(1, int(pod_count))
    if pod_count == 1:
        return 0
    sorted_hashes, owner_by_hash = _build_ring(pod_count)
    h = _hash_int(str(source_id))
    idx = bisect.bisect_right(sorted_hashes, h)
    if idx == len(sorted_hashes):
        idx = 0
    return owner_by_hash[sorted_hashes[idx]]


# ---------------------------------------------------------------------------
# Kubernetes I/O (defensive import, mirrors
# app/services/committer_provisioner.py's _load_k8s() pattern exactly).
# ---------------------------------------------------------------------------

def _load_k8s():
    try:
        from kubernetes import client as k8s, config as k8s_config  # type: ignore
    except ImportError:
        return None, None
    try:
        k8s_config.load_incluster_config()
    except Exception:
        try:
            k8s_config.load_kube_config()
        except Exception:
            log.warning("source_assignment: no in-cluster or kubeconfig credentials available")
            return None, None
    return k8s, k8s_config


def _current_namespace() -> str:
    ns_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    try:
        with open(ns_file) as f:
            return f.read().strip()
    except OSError:
        return os.environ.get("WORKER_NAMESPACE", os.environ.get("NAMESPACE", "fusion"))


def get_ready_replica_count(statefulset_name: str, namespace: Optional[str] = None) -> Optional[int]:
    """Live-query the StatefulSet's current ready replica count via
    ``AppsV1Api.read_namespaced_stateful_set(name, namespace).status.ready_replicas``.

    Returns None if the kubernetes client/credentials aren't available
    (dev/test/local — caller should preserve single-worker behavior) or the
    StatefulSet lookup otherwise fails."""
    k8s, _ = _load_k8s()
    if k8s is None:
        return None
    ns = namespace or _current_namespace()
    try:
        apps_v1 = k8s.AppsV1Api()
        sts = apps_v1.read_namespaced_stateful_set(statefulset_name, ns)
        ready = sts.status.ready_replicas
        if not ready or ready < 1:
            # Not-yet-ready StatefulSet (e.g. mid-rollout, just scaled up from
            # 0) — treat as 1 so we never divide assignment across zero pods.
            return 1
        return int(ready)
    except Exception as exc:
        log.warning(
            "source_assignment: could not read StatefulSet %s/%s ready_replicas: %s",
            ns, statefulset_name, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Rebalance
# ---------------------------------------------------------------------------

def _rebalance(db: "Session", source_type: str, statefulset_name: str, pod_count: int) -> dict:
    """Recompute assigned_worker_id for every active/draft source whose
    connector_type maps to `source_type`, sharding across `pod_count` pods.
    Persists changes onto Source.config (JSONB) using the same
    mutate-then-flag_modified convention app/api/connections.py already
    uses for JSONB column updates (e.g. configure_schedule())."""
    from sqlalchemy import flag_modified
    from sqlalchemy.orm import joinedload
    from app.models.source_destination import Source

    sources = (
        db.query(Source)
        .options(joinedload(Source.connector_definition))
        .filter(
            Source.is_deleted == False,  # noqa: E712
            Source.status.in_(["active", "draft"]),
        )
        .all()
    )

    changed = 0
    assignments: Dict[str, str] = {}
    for s in sources:
        ctype = s.connector_definition.connector_type if s.connector_definition else None
        if statefulset_source_type(ctype) != source_type:
            continue
        ordinal = assign_pod_ordinal(str(s.source_id), pod_count)
        worker_id = worker_id_for_ordinal(statefulset_name, ordinal)
        assignments[str(s.source_id)] = worker_id

        cfg = s.config
        if cfg is None:
            cfg = {}
            s.config = cfg
        if cfg.get("assigned_worker_id") != worker_id:
            cfg["assigned_worker_id"] = worker_id
            flag_modified(s, "config")
            changed += 1

    db.flush()
    log.info(
        "source_assignment: rebalanced source_type=%s statefulset=%s pod_count=%d "
        "sources=%d changed=%d",
        source_type, statefulset_name, pod_count, len(assignments), changed,
    )
    return {
        "rebalanced": True,
        "source_type": source_type,
        "statefulset_name": statefulset_name,
        "pod_count": pod_count,
        "assignments": assignments,
        "changed": changed,
    }


def rebalance_source_type(db: "Session", connector_type: Optional[str]) -> dict:
    """Recompute assigned_worker_id for every active/draft source of the
    StatefulSet type that `connector_type` belongs to, sharding across that
    StatefulSet's CURRENTLY READY pod count via consistent hashing.

    Never raises: called from request-handling code paths (source
    activation), and a K8s/config problem here must not block the caller.
    If the kubernetes client/credentials aren't available, RELEASE_NAME
    isn't set, or the StatefulSet can't be read yet, this is a no-op and
    get_worker_sources()'s existing single-worker fallback keeps working
    exactly as before.
    """
    try:
        source_type = statefulset_source_type(connector_type)
        if source_type is None:
            return {"rebalanced": False, "reason": f"unknown connector_type {connector_type!r}"}

        release_name = os.environ.get("RELEASE_NAME")
        if not release_name:
            return {"rebalanced": False, "reason": "RELEASE_NAME not set"}

        statefulset_name = statefulset_name_for_source_type(source_type, release_name)
        pod_count = get_ready_replica_count(statefulset_name)
        if pod_count is None:
            return {"rebalanced": False, "reason": "kubernetes client/StatefulSet unavailable"}

        return _rebalance(db, source_type, statefulset_name, pod_count)
    except Exception:
        log.exception(
            "source_assignment: rebalance_source_type failed for connector_type=%r",
            connector_type,
        )
        return {"rebalanced": False, "reason": "unexpected error (see logs)"}


def rebalance_source_type_at_pod_count(db: "Session", connector_type: Optional[str], pod_count: int) -> dict:
    """Like ``rebalance_source_type()``, but the caller supplies `pod_count`
    explicitly instead of it being derived from the StatefulSet's CURRENT
    live ``ready_replicas``.

    Added for ``app/services/cdc_worker_autoscaler.py`` (the direct-scaling
    reconcile loop): before scaling a StatefulSet DOWN, sources need to be
    re-hashed onto the ring sized to the POST-scale-down pod count ahead of
    time, so no source is left pointing at a pod ordinal (StatefulSets
    always remove the highest ordinal(s) first) that's about to be
    terminated. Calling ``rebalance_source_type()`` itself would not achieve
    this since it always reads the CURRENT (pre-scale-down) ready_replicas —
    this function exists purely so a caller that already knows the intended
    FUTURE pod count (because it's the one about to set it) can rebalance
    against that count instead. Never raises, same convention as
    ``rebalance_source_type()``.
    """
    try:
        source_type = statefulset_source_type(connector_type)
        if source_type is None:
            return {"rebalanced": False, "reason": f"unknown connector_type {connector_type!r}"}

        release_name = os.environ.get("RELEASE_NAME")
        if not release_name:
            return {"rebalanced": False, "reason": "RELEASE_NAME not set"}

        statefulset_name = statefulset_name_for_source_type(source_type, release_name)
        return _rebalance(db, source_type, statefulset_name, max(1, int(pod_count)))
    except Exception:
        log.exception(
            "source_assignment: rebalance_source_type_at_pod_count failed for "
            "connector_type=%r pod_count=%r",
            connector_type, pod_count,
        )
        return {"rebalanced": False, "reason": "unexpected error (see logs)"}


# In-memory cache of the last-seen ready replica count per StatefulSet, used
# by maybe_rebalance_on_heartbeat() to detect scale up/down events without a
# full K8s watch. This is a deliberate simplification (see the docstring
# below) — it is per-process, so with multiple control-plane API replicas
# each process detects the change independently the first time ITS
# heartbeat handler observes the new count; a rebalance is idempotent (same
# ring, same inputs -> same assignment) so redundant triggers across
# replicas are harmless, just slightly duplicated work.
_last_known_ready_replicas: Dict[str, int] = {}


def maybe_rebalance_on_heartbeat(db: "Session", worker_id: str) -> Optional[dict]:
    """Judgment call for the rebalance trigger (see task notes): a full
    Kubernetes watch on StatefulSet status would be the "proper" event-driven
    way to notice a replica-count change, but every cdc-worker pod already
    heartbeats to this endpoint every HEARTBEAT_INTERVAL seconds (default
    30s) — piggybacking the check there is by far the simplest mechanism
    that actually works, at the cost of up to one heartbeat interval of
    staleness after a scale event, which is fine for this use case.

    Looks up the calling worker's StatefulSet's current ready replica count;
    if it differs from the last-seen value for that StatefulSet, triggers a
    full rebalance for that source-type and updates the cache. Returns None
    when no rebalance was triggered (unrecognized worker_id, count
    unchanged, or K8s unavailable).
    """
    try:
        source_type = source_type_for_worker_id(worker_id)
        if source_type is None:
            return None  # not one of our StatefulSet pods (dev/local WORKER_ID)

        parsed = parse_worker_id(worker_id)
        if parsed is None:
            return None
        statefulset_name, _ordinal = parsed

        pod_count = get_ready_replica_count(statefulset_name)
        if pod_count is None:
            return None

        if _last_known_ready_replicas.get(statefulset_name) == pod_count:
            return None  # no change since last observation

        _last_known_ready_replicas[statefulset_name] = pod_count
        result = _rebalance(db, source_type, statefulset_name, pod_count)
        db.commit()
        return result
    except Exception:
        log.exception(
            "source_assignment: maybe_rebalance_on_heartbeat failed for worker_id=%r",
            worker_id,
        )
        return None

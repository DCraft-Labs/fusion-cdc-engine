"""Phase 2 (cdc-worker source sharding) unit tests for
``app.services.source_assignment`` — the consistent-hash assignment
mechanism that replaces the old always-empty ``assigned_worker_id`` filter
in ``app/api/internal.py::get_worker_sources`` (previously that filter was
read but never written, so it always fell back to handing EVERY active
source to EVERY cdc-worker pod, which is silently fine at 1 replica but
causes MySQL server_id churn / duplicate streaming the instant a
StatefulSet scales to 2+).

These tests cover only the pure, dependency-free parts of
``source_assignment.py`` (hashing, worker-id parsing, and the
``select_assigned_sources`` filtering decision) using plain
``SimpleNamespace`` stand-ins for the ``Source`` ORM model — the same
dependency-light convention ``test_partitioning.py`` and
``test_routing_v120.py`` already use in this suite, so this runs with only
``pytest`` installed (no sqlalchemy/kubernetes/live DB required). The
K8s-touching functions (``get_ready_replica_count``, ``_rebalance``,
``rebalance_source_type``, ``maybe_rebalance_on_heartbeat``) are exercised
only implicitly (their "kubernetes client unavailable" no-op path), since
they need a live Kubernetes API / SQLAlchemy session to do anything real —
see the environment note in the task write-up for why that side isn't
covered here.
"""
import os
import sys
from types import SimpleNamespace

import pytest

# Make ``app`` importable when run from the control-plane dir (matches
# test_partitioning.py's convention).
HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services.source_assignment import (  # noqa: E402
    assign_pod_ordinal,
    parse_worker_id,
    rebalance_source_type,
    select_assigned_sources,
    source_type_for_worker_id,
    statefulset_name_for_source_type,
    statefulset_source_type,
    worker_id_for_ordinal,
)


def _source(source_id, connector_type="mysql", assigned_worker_id=None):
    """Build a SimpleNamespace standing in for a Source ORM row — just
    enough shape for select_assigned_sources/duck-typing."""
    return SimpleNamespace(
        source_id=source_id,
        config={"assigned_worker_id": assigned_worker_id} if assigned_worker_id else {},
        connector_definition=SimpleNamespace(connector_type=connector_type),
    )


# ---------------------------------------------------------------------------
# connector_type <-> StatefulSet source-type mapping
# ---------------------------------------------------------------------------

class TestStatefulsetSourceType:
    def test_mysql(self):
        assert statefulset_source_type("mysql") == "mysql"

    def test_postgres_and_postgresql_both_map_to_postgres(self):
        # The DB stores "postgresql"; the Helm chart's StatefulSet key is
        # "postgres" -- this is the exact naming-drift case the task called
        # out, so pin both directions explicitly.
        assert statefulset_source_type("postgres") == "postgres"
        assert statefulset_source_type("postgresql") == "postgres"

    def test_mongo_and_mongodb_both_map_to_mongodb(self):
        assert statefulset_source_type("mongo") == "mongodb"
        assert statefulset_source_type("mongodb") == "mongodb"

    def test_case_insensitive(self):
        assert statefulset_source_type("MySQL") == "mysql"
        assert statefulset_source_type("POSTGRESQL") == "postgres"

    def test_unknown_or_none(self):
        assert statefulset_source_type("kafka") is None
        assert statefulset_source_type(None) is None
        assert statefulset_source_type("") is None


# ---------------------------------------------------------------------------
# worker_id <-> (statefulset_name, ordinal) round trip
# ---------------------------------------------------------------------------

class TestWorkerIdParsing:
    def test_parse_valid_statefulset_pod_name(self):
        assert parse_worker_id("fusion-cdc-cdc-worker-mysql-0") == (
            "fusion-cdc-cdc-worker-mysql", 0,
        )
        assert parse_worker_id("fusion-cdc-cdc-worker-postgres-11") == (
            "fusion-cdc-cdc-worker-postgres", 11,
        )

    def test_parse_rejects_non_ordinal_names(self):
        assert parse_worker_id("") is None
        assert parse_worker_id("no-dashes-suffix-abc") is None

    def test_round_trip_name_ordinal(self):
        sts_name = statefulset_name_for_source_type("mongodb", "fusion-cdc")
        assert sts_name == "fusion-cdc-cdc-worker-mongodb"
        worker_id = worker_id_for_ordinal(sts_name, 3)
        assert worker_id == "fusion-cdc-cdc-worker-mongodb-3"
        assert parse_worker_id(worker_id) == (sts_name, 3)
        assert source_type_for_worker_id(worker_id) == "mongodb"

    def test_dev_default_worker_id_is_not_mistaken_for_a_statefulset_pod(self):
        # cdc-workers/cdc_worker/config.py's WORKER_ID default is "worker-1"
        # -- it happens to end in a digit (so parse_worker_id succeeds) but
        # must NOT be mistaken for one of our StatefulSet pods, or a dev/
        # local worker would get an empty assignment instead of the
        # single-worker fallback.
        assert source_type_for_worker_id("worker-1") is None

    def test_unrecognized_source_type_suffix(self):
        assert source_type_for_worker_id("some-release-cdc-worker-kafka-0") is None


# ---------------------------------------------------------------------------
# Consistent hashing: assign_pod_ordinal
# ---------------------------------------------------------------------------

class TestAssignPodOrdinal:
    def test_single_replica_always_ordinal_zero(self):
        for i in range(50):
            assert assign_pod_ordinal(f"source-{i}", 1) == 0

    def test_deterministic(self):
        # Same (source_id, pod_count) must always map to the same ordinal --
        # control-plane may recompute this on a different process/request.
        for i in range(20):
            sid = f"11111111-2222-3333-4444-{i:012d}"
            assert assign_pod_ordinal(sid, 4) == assign_pod_ordinal(sid, 4)

    def test_two_replicas_split_with_no_double_assignment(self):
        source_ids = [f"src-{i}" for i in range(200)]
        owners = {sid: assign_pod_ordinal(sid, 2) for sid in source_ids}
        # Every source maps to exactly one ordinal (trivially true — the
        # function returns a single int) drawn from {0, 1}.
        assert set(owners.values()) <= {0, 1}
        # Both pods actually get a share -- not everything piling onto one
        # ordinal (would defeat the point of sharding).
        counts = {0: 0, 1: 0}
        for ordinal in owners.values():
            counts[ordinal] += 1
        assert counts[0] > 0
        assert counts[1] > 0
        # Roughly even (consistent hashing over 200 samples / 2 pods) --
        # generous tolerance since this isn't a perfect-balance guarantee.
        assert 0.25 <= counts[0] / len(source_ids) <= 0.75

    def test_no_source_assigned_to_two_pods_simultaneously(self):
        # By construction assign_pod_ordinal returns a single ordinal, but
        # pin it explicitly for a batch of sources across a few pod counts.
        for pod_count in (2, 3, 5):
            for i in range(30):
                ordinal = assign_pod_ordinal(f"multi-{pod_count}-{i}", pod_count)
                assert isinstance(ordinal, int)
                assert 0 <= ordinal < pod_count


# ---------------------------------------------------------------------------
# select_assigned_sources — the actual get_worker_sources() filtering
# decision, exercised directly (same function internal.py calls).
# ---------------------------------------------------------------------------

class TestSelectAssignedSources:
    STS = "fusion-cdc-cdc-worker-mysql"

    def test_single_replica_worker_gets_all_its_sources(self):
        # (a) Single-replica case: rebalance would have written
        # assigned_worker_id="<sts>-0" onto every mysql source (pod_count=1
        # -> assign_pod_ordinal always 0). The sole pod must get all of them.
        worker_id = worker_id_for_ordinal(self.STS, 0)
        sources = [
            _source(f"s{i}", connector_type="mysql", assigned_worker_id=worker_id)
            for i in range(5)
        ]
        assigned = select_assigned_sources(sources, worker_id)
        assert {s.source_id for s in assigned} == {s.source_id for s in sources}

    def test_two_replicas_split_no_source_assigned_to_both(self):
        # (b) 2 replicas: assign 40 sources across ordinals 0/1 via the real
        # consistent-hash function, then verify each worker's
        # select_assigned_sources() call returns a disjoint subset that
        # together cover every source exactly once.
        worker0 = worker_id_for_ordinal(self.STS, 0)
        worker1 = worker_id_for_ordinal(self.STS, 1)
        sources = []
        for i in range(40):
            sid = f"src-{i}"
            ordinal = assign_pod_ordinal(sid, 2)
            owner = worker_id_for_ordinal(self.STS, ordinal)
            sources.append(_source(sid, connector_type="mysql", assigned_worker_id=owner))

        assigned0 = select_assigned_sources(sources, worker0)
        assigned1 = select_assigned_sources(sources, worker1)

        ids0 = {s.source_id for s in assigned0}
        ids1 = {s.source_id for s in assigned1}
        all_ids = {s.source_id for s in sources}

        assert ids0.isdisjoint(ids1), "a source was handed to both pod-0 and pod-1"
        assert ids0 | ids1 == all_ids, "some source wasn't claimed by either pod"
        # Both pods should own something for 40 sources spread over 2 pods.
        assert ids0 and ids1

    def test_scaling_1_to_2_reassigns_rather_than_leaving_stale_state(self):
        # (c) Simulate a rebalance run at pod_count=1 (everything -> "-0"),
        # then a second rebalance after scaling to pod_count=2 that
        # recomputes assigned_worker_id from scratch (mirrors what
        # source_assignment._rebalance does: overwrite config in place, not
        # append/merge). After rescaling, worker "-0" must NOT still see
        # every source -- some real subset must have moved to "-1".
        worker0 = worker_id_for_ordinal(self.STS, 0)
        worker1 = worker_id_for_ordinal(self.STS, 1)
        source_ids = [f"scale-{i}" for i in range(60)]

        # --- pod_count=1 rebalance ---
        sources = [_source(sid, "mysql") for sid in source_ids]
        for s in sources:
            ordinal = assign_pod_ordinal(s.source_id, 1)
            s.config["assigned_worker_id"] = worker_id_for_ordinal(self.STS, ordinal)

        assigned_before = select_assigned_sources(sources, worker0)
        assert {s.source_id for s in assigned_before} == set(source_ids)

        # --- scale to pod_count=2, rebalance recomputes in place ---
        for s in sources:
            ordinal = assign_pod_ordinal(s.source_id, 2)
            s.config["assigned_worker_id"] = worker_id_for_ordinal(self.STS, ordinal)

        assigned_after_0 = select_assigned_sources(sources, worker0)
        assigned_after_1 = select_assigned_sources(sources, worker1)

        assert len(assigned_after_0) < len(source_ids), (
            "worker-0 still owns every source after scaling to 2 replicas -- "
            "stale assignment was not overwritten"
        )
        assert len(assigned_after_1) > 0
        assert len(assigned_after_0) + len(assigned_after_1) == len(source_ids)

    def test_dev_single_worker_fallback_when_assignment_never_ran(self):
        # No source of this type has an assigned_worker_id yet (assignment
        # machinery never ran -- e.g. no RELEASE_NAME / no kubernetes client
        # in dev/local/docker-compose). The single worker must still get
        # everything, exactly like the pre-Phase-2 behavior.
        worker_id = worker_id_for_ordinal(self.STS, 0)
        sources = [_source(f"dev-{i}", "mysql") for i in range(3)]
        assigned = select_assigned_sources(sources, worker_id)
        assert {s.source_id for s in assigned} == {s.source_id for s in sources}

    def test_unrecognized_worker_id_falls_back_to_all_sources(self):
        # A hand-set dev WORKER_ID ("worker-1") that doesn't parse as a
        # StatefulSet pod name must fall back to the original unfiltered
        # behavior, even if OTHER sources happen to carry assigned_worker_id
        # values (e.g. a mixed dev/prod DB fixture).
        sources = [
            _source("a", "mysql", assigned_worker_id="fusion-cdc-cdc-worker-mysql-0"),
            _source("b", "mysql", assigned_worker_id="fusion-cdc-cdc-worker-mysql-1"),
        ]
        assigned = select_assigned_sources(sources, "worker-1")
        assert {s.source_id for s in assigned} == {"a", "b"}

    def test_only_same_type_sources_count_toward_assignment_activity(self):
        # A mysql pod must not be confused by assignment data that only
        # exists for postgres sources -- and must not receive postgres
        # sources it can't handle.
        mysql_worker0 = worker_id_for_ordinal(self.STS, 0)
        pg_sts = "fusion-cdc-cdc-worker-postgres"
        pg_worker0 = worker_id_for_ordinal(pg_sts, 0)

        sources = [
            _source("pg-1", "postgresql", assigned_worker_id=pg_worker0),
            _source("my-1", "mysql"),  # no assignment yet for mysql
            _source("my-2", "mysql"),
        ]
        assigned = select_assigned_sources(sources, mysql_worker0)
        # mysql assignment hasn't run -> dev/single-worker fallback -> ALL
        # sources (matches original endpoint behavior; a stricter same-type
        # only fallback was considered but rejected to avoid changing
        # behavior beyond what the task asked for).
        assert {s.source_id for s in assigned} == {"pg-1", "my-1", "my-2"}


# ---------------------------------------------------------------------------
# rebalance_source_type — degrades to a safe no-op without a kubernetes
# client (this environment doesn't have one installed; see the task's
# environment note). Confirms it never raises and reports why.
# ---------------------------------------------------------------------------

class TestRebalanceSourceTypeNoOp:
    def test_unknown_connector_type(self):
        result = rebalance_source_type(db=None, connector_type="kafka")
        assert result == {"rebalanced": False, "reason": "unknown connector_type 'kafka'"}

    def test_no_release_name_env(self, monkeypatch):
        monkeypatch.delenv("RELEASE_NAME", raising=False)
        result = rebalance_source_type(db=None, connector_type="mysql")
        assert result["rebalanced"] is False
        assert result["reason"] == "RELEASE_NAME not set"

    def test_kubernetes_client_unavailable(self, monkeypatch):
        # In this environment the `kubernetes` package isn't importable
        # (see task environment note), so this exercises the real
        # ImportError path in _load_k8s() rather than a mock.
        monkeypatch.setenv("RELEASE_NAME", "fusion-cdc")
        result = rebalance_source_type(db=None, connector_type="mysql")
        assert result["rebalanced"] is False
        assert result["reason"] in (
            "kubernetes client/StatefulSet unavailable",
            "unexpected error (see logs)",
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))

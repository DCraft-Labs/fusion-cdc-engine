"""Unit tests for app.services.resource_ledger — the Redis-backed admission
ledger (baseline + transient reservation classes, atomic reserve/release).

ENVIRONMENT NOTE: this sandbox's Python 3.14 has a broken ``pyexpat``,
which breaks ``pip install`` here, so the real ``redis`` package could not
be installed/exercised against a live Redis server. These tests instead use
a hand-rolled ``FakeRedis`` test double (below) whose ``register_script``
executes a Python re-implementation of the exact algorithm in
``resource_ledger._RESERVE_TRANSIENT_SCRIPT`` (sum baseline+transient
hashes, compare against total-minus-overhead, HSET if it fits) — this
verifies ``resource_ledger``'s calling contract and the algorithm's
atomicity property (relying on Redis's real guarantee that a single EVAL
runs to completion without interleaving from another EVAL), but does NOT
execute the actual Lua text against a real Lua interpreter. The Lua script
itself was hand-reviewed instead of executed — see the task summary.

Run with: pytest control-plane/tests/test_resource_ledger.py
(requires ``pytest`` + the rest of this repo's normal test deps, which this
sandbox could not install — see the note above).
"""
import os
import sys
import threading

import pytest

HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services import resource_ledger  # noqa: E402


class FakeScript:
    """Mimics redis-py's ``Script`` callable object (returned by
    ``client.register_script(...)``) by re-implementing the exact
    check-then-reserve algorithm the real Lua script encodes, against the
    FakeRedis instance's in-memory hashes. A single ``threading.Lock`` makes
    the whole "read four hashes, compare, maybe write two" sequence
    indivisible from another thread's call — mirroring the atomicity a real
    Redis server's single-threaded EVAL execution provides for free.
    """

    def __init__(self, client):
        self._client = client

    def __call__(self, keys, args):
        baseline_cpu_key, baseline_mem_key, transient_cpu_key, transient_mem_key = keys
        conn_id, req_cpu, req_mem, total_cpu, total_mem, cp_cpu, cp_mem = args
        req_cpu, req_mem = int(req_cpu), int(req_mem)
        total_cpu, total_mem = int(total_cpu), int(total_mem)
        cp_cpu, cp_mem = int(cp_cpu), int(cp_mem)

        with self._client.eval_lock:
            baseline_cpu = self._client._sum(baseline_cpu_key)
            baseline_mem = self._client._sum(baseline_mem_key)
            transient_cpu = self._client._sum(transient_cpu_key)
            transient_mem = self._client._sum(transient_mem_key)

            avail_cpu = total_cpu - cp_cpu - baseline_cpu - transient_cpu
            avail_mem = total_mem - cp_mem - baseline_mem - transient_mem

            if req_cpu > avail_cpu or req_mem > avail_mem:
                return [0, avail_cpu, avail_mem]

            self._client.hset(transient_cpu_key, str(conn_id), req_cpu)
            self._client.hset(transient_mem_key, str(conn_id), req_mem)
            return [1, avail_cpu - req_cpu, avail_mem - req_mem]


class FakeRedis:
    """Minimal in-memory stand-in for the subset of the redis-py sync API
    ``resource_ledger.py`` uses: hvals, hset, hsetnx, hdel, hget,
    register_script."""

    def __init__(self):
        self._hashes: dict = {}
        self.eval_lock = threading.Lock()

    def _sum(self, key: str) -> int:
        h = self._hashes.get(key, {})
        return sum(int(v) for v in h.values())

    def hvals(self, key):
        return list(self._hashes.get(key, {}).values())

    def hset(self, key, field, value):
        self._hashes.setdefault(key, {})[str(field)] = value
        return 1

    def hsetnx(self, key, field, value):
        h = self._hashes.setdefault(key, {})
        field = str(field)
        if field in h:
            return 0
        h[field] = value
        return 1

    def hdel(self, key, field):
        h = self._hashes.get(key, {})
        return 1 if h.pop(str(field), None) is not None else 0

    def hget(self, key, field):
        return self._hashes.get(key, {}).get(str(field))

    def register_script(self, script_text):
        return FakeScript(self)


@pytest.fixture
def fake_redis():
    return FakeRedis()


class TestReserveTransient:
    def test_reserve_fits_within_capacity(self, fake_redis):
        result = resource_ledger.reserve_transient(
            "conn-1", cpu_millis=500, mem_mi=512,
            total_cpu_millis=2000, total_mem_mi=2048,
            client=fake_redis,
        )
        assert result["reserved"] is True
        # 2000 - 500(control plane default) - 500(request) = 1000
        assert result["available_cpu_millis"] == 2000 - resource_ledger.CONTROL_PLANE_CPU_MILLIS - 500

    def test_reserve_rejected_when_insufficient_capacity(self, fake_redis):
        # Exhaust nearly everything with a first reservation.
        first = resource_ledger.reserve_transient(
            "conn-1", cpu_millis=1400, mem_mi=1400,
            total_cpu_millis=2000, total_mem_mi=2048,
            client=fake_redis,
        )
        assert first["reserved"] is True

        second = resource_ledger.reserve_transient(
            "conn-2", cpu_millis=1000, mem_mi=1000,
            total_cpu_millis=2000, total_mem_mi=2048,
            client=fake_redis,
        )
        assert second["reserved"] is False
        # Rejected reservation must NOT have touched the hash — conn-2 should
        # not appear as a transient consumer.
        assert fake_redis.hget(resource_ledger.TRANSIENT_CPU_KEY, "conn-2") is None

    def test_release_frees_capacity_for_a_subsequent_reserve(self, fake_redis):
        first = resource_ledger.reserve_transient(
            "conn-1", cpu_millis=1400, mem_mi=1400,
            total_cpu_millis=2000, total_mem_mi=2048,
            client=fake_redis,
        )
        assert first["reserved"] is True

        blocked = resource_ledger.reserve_transient(
            "conn-2", cpu_millis=1000, mem_mi=1000,
            total_cpu_millis=2000, total_mem_mi=2048,
            client=fake_redis,
        )
        assert blocked["reserved"] is False

        resource_ledger.release_transient("conn-1", client=fake_redis)

        now_fits = resource_ledger.reserve_transient(
            "conn-2", cpu_millis=1000, mem_mi=1000,
            total_cpu_millis=2000, total_mem_mi=2048,
            client=fake_redis,
        )
        assert now_fits["reserved"] is True


class TestConcurrentReserveAtomicity:
    """Simulates two simultaneous connection-creation requests racing for
    the same last sliver of capacity — only one may win."""

    def test_only_one_of_two_concurrent_reservations_succeeds(self, fake_redis):
        # Total pool: 2000 CPU millis. Available = 2000 - control-plane
        # overhead (default 500) = 1500. Two requests each ask for 1000
        # (more than half the available capacity) — only one can fit.
        total_cpu, total_mem = 2000, 4096
        results = [None, None]

        def worker(idx, conn_id):
            results[idx] = resource_ledger.reserve_transient(
                conn_id, cpu_millis=1000, mem_mi=100,
                total_cpu_millis=total_cpu, total_mem_mi=total_mem,
                client=fake_redis,
            )

        t1 = threading.Thread(target=worker, args=(0, "conn-a"))
        t2 = threading.Thread(target=worker, args=(1, "conn-b"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        reserved_flags = [r["reserved"] for r in results]
        assert reserved_flags.count(True) == 1, (
            "exactly one of the two concurrent reservations should have won "
            f"the last sliver of capacity, got {reserved_flags}"
        )
        assert reserved_flags.count(False) == 1

        # The ledger's recorded transient CPU total must equal exactly one
        # request's worth (1000), never both (2000) and never zero.
        assert fake_redis._sum(resource_ledger.TRANSIENT_CPU_KEY) == 1000

    def test_many_concurrent_reservations_never_oversell(self, fake_redis):
        """Ten concurrent requests for 300 millis each against a pool that
        can only fit 3 of them (900 available) — exactly 3 should win."""
        total_cpu = 900 + resource_ledger.CONTROL_PLANE_CPU_MILLIS
        total_mem = 100000
        results = [None] * 10

        def worker(idx):
            results[idx] = resource_ledger.reserve_transient(
                f"conn-{idx}", cpu_millis=300, mem_mi=10,
                total_cpu_millis=total_cpu, total_mem_mi=total_mem,
                client=fake_redis,
            )

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        won = sum(1 for r in results if r["reserved"])
        assert won == 3
        assert fake_redis._sum(resource_ledger.TRANSIENT_CPU_KEY) == 900


class TestBaselineReservation:
    def test_ensure_baseline_creates_once(self, fake_redis):
        created_first = resource_ledger.ensure_baseline_reservation("conn-1", client=fake_redis)
        assert created_first is True
        assert fake_redis.hget(resource_ledger.BASELINE_CPU_KEY, "conn-1") == resource_ledger.CDC_BASELINE_CPU_MILLIS

        # Re-triggering (e.g. resume / trigger-sync) must NOT double-reserve.
        created_second = resource_ledger.ensure_baseline_reservation("conn-1", client=fake_redis)
        assert created_second is False
        assert fake_redis._sum(resource_ledger.BASELINE_CPU_KEY) == resource_ledger.CDC_BASELINE_CPU_MILLIS

    def test_release_baseline_frees_it(self, fake_redis):
        resource_ledger.ensure_baseline_reservation("conn-1", client=fake_redis)
        assert fake_redis._sum(resource_ledger.BASELINE_CPU_KEY) > 0
        resource_ledger.release_baseline("conn-1", client=fake_redis)
        assert fake_redis._sum(resource_ledger.BASELINE_CPU_KEY) == 0

    def test_release_all_releases_both_classes(self, fake_redis):
        resource_ledger.ensure_baseline_reservation("conn-1", client=fake_redis)
        resource_ledger.reserve_transient(
            "conn-1", cpu_millis=100, mem_mi=100,
            total_cpu_millis=5000, total_mem_mi=5000,
            client=fake_redis,
        )
        assert fake_redis._sum(resource_ledger.BASELINE_CPU_KEY) > 0
        assert fake_redis._sum(resource_ledger.TRANSIENT_CPU_KEY) > 0

        resource_ledger.release_all("conn-1", client=fake_redis)

        assert fake_redis._sum(resource_ledger.BASELINE_CPU_KEY) == 0
        assert fake_redis._sum(resource_ledger.TRANSIENT_CPU_KEY) == 0


class TestGetAvailableCapacity:
    def test_subtracts_control_plane_and_reservations(self, fake_redis):
        resource_ledger.ensure_baseline_reservation("conn-1", client=fake_redis, cpu_millis=100, mem_mi=128)
        resource_ledger.reserve_transient(
            "conn-2", cpu_millis=200, mem_mi=256,
            total_cpu_millis=10000, total_mem_mi=10000,
            client=fake_redis,
        )
        capacity = resource_ledger.get_available_capacity(10000, 10000, client=fake_redis)
        expected_cpu = 10000 - resource_ledger.CONTROL_PLANE_CPU_MILLIS - 100 - 200
        expected_mem = 10000 - resource_ledger.CONTROL_PLANE_MEM_MI - 128 - 256
        assert capacity["available_cpu_millis"] == expected_cpu
        assert capacity["available_memory_mi"] == expected_mem

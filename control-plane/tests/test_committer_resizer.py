"""Unit tests for app.services.committer_resizer — the pure decision logic
behind control-plane's ongoing Iceberg committer resizing (Phase 3b): the
add_files() concurrency mapping (cheap/frequent lever) and the CPU/memory
resize hysteresis (expensive/infrequent lever), plus the K8s resource
quantity parsers.

Pure-Python only (no Redis/K8s/DB) — same "hand-verified, not
pytest-executed" caveat as test_resource_admission.py (this sandbox's
Python 3.14 has a broken pyexpat blocking pip install).
"""
import os
import sys

import pytest

HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services import committer_resizer as cr  # noqa: E402


class TestComputeDesiredConcurrency:
    def test_zero_lag_gives_minimum(self):
        assert cr.compute_desired_concurrency(0) == cr.CONCURRENCY_MIN

    def test_lag_at_ceiling_gives_maximum(self):
        assert cr.compute_desired_concurrency(cr.CONCURRENCY_LAG_CEILING) == cr.CONCURRENCY_MAX

    def test_lag_beyond_ceiling_still_clamped_to_maximum(self):
        assert cr.compute_desired_concurrency(cr.CONCURRENCY_LAG_CEILING * 10) == cr.CONCURRENCY_MAX

    def test_monotonically_increases_with_lag(self):
        prev = cr.compute_desired_concurrency(0)
        for lag in range(0, cr.CONCURRENCY_LAG_CEILING + 1, max(1, cr.CONCURRENCY_LAG_CEILING // 10)):
            cur = cr.compute_desired_concurrency(lag)
            assert cur >= prev
            prev = cur

    def test_negative_lag_treated_as_zero(self):
        assert cr.compute_desired_concurrency(-100) == cr.CONCURRENCY_MIN


class TestClassifyDrainLag:
    def test_over_threshold(self):
        assert cr.classify_drain_lag(cr.OVER_LAG_THRESHOLD) == "over"
        assert cr.classify_drain_lag(cr.OVER_LAG_THRESHOLD + 1) == "over"

    def test_under_threshold(self):
        assert cr.classify_drain_lag(cr.UNDER_LAG_THRESHOLD) == "under"
        assert cr.classify_drain_lag(0) == "under"

    def test_ok_in_between(self):
        mid = (cr.OVER_LAG_THRESHOLD + cr.UNDER_LAG_THRESHOLD) // 2
        # Guard against a config where the band is empty/inverted.
        if mid > cr.UNDER_LAG_THRESHOLD and mid < cr.OVER_LAG_THRESHOLD:
            assert cr.classify_drain_lag(mid) == "ok"


class TestDecideNextResources:
    def _state(self, **overrides):
        state = {"signal": None, "consecutive": 0, "last_resize_ts": 0}
        state.update(overrides)
        return state

    def test_ok_signal_never_resizes(self):
        cpu, mem, state = cr.decide_next_resources(500, 1024, "ok", self._state(), now=0)
        assert (cpu, mem) == (500, 1024)
        assert state["signal"] is None

    def test_over_signal_requires_consecutive_cycles_before_resizing(self):
        state = self._state(last_resize_ts=-100_000)  # cooldown never binds in this test
        cpu, mem = 500, 1024
        for i in range(cr.RESIZE_CONSECUTIVE_CYCLES - 1):
            new_cpu, new_mem, state = cr.decide_next_resources(cpu, mem, "over", state, now=i)
            assert (new_cpu, new_mem) == (cpu, mem), "must not resize before enough consecutive cycles"
        new_cpu, new_mem, state = cr.decide_next_resources(
            cpu, mem, "over", state, now=cr.RESIZE_CONSECUTIVE_CYCLES)
        assert new_cpu > cpu
        assert new_mem > mem

    def test_under_signal_shrinks_after_enough_cycles(self):
        state = self._state(last_resize_ts=-100_000)
        cpu, mem = 2000, 2048
        for _ in range(cr.RESIZE_CONSECUTIVE_CYCLES):
            new_cpu, new_mem, state = cr.decide_next_resources(cpu, mem, "under", state, now=0)
        assert new_cpu < cpu
        assert new_mem < mem

    def test_cooldown_blocks_resize_even_with_enough_cycles(self):
        state = self._state(signal="over", consecutive=cr.RESIZE_CONSECUTIVE_CYCLES - 1,
                             last_resize_ts=1000)
        now = 1000 + cr.RESIZE_COOLDOWN_S - 1  # still inside cooldown
        cpu, mem, state = cr.decide_next_resources(500, 1024, "over", state, now=now)
        assert (cpu, mem) == (500, 1024)

    def test_resize_never_exceeds_xl_ceiling(self):
        state = self._state(last_resize_ts=-100_000)
        cpu, mem = cr.TIER_BASE_CPU_MILLIS["XL"], cr.TIER_BASE_MEM_MI["XL"]
        for _ in range(cr.RESIZE_CONSECUTIVE_CYCLES):
            cpu, mem, state = cr.decide_next_resources(cpu, mem, "over", state, now=0)
        assert cpu == cr.TIER_BASE_CPU_MILLIS["XL"]
        assert mem == cr.TIER_BASE_MEM_MI["XL"]

    def test_resize_never_goes_below_s_floor(self):
        state = self._state(last_resize_ts=-100_000)
        cpu, mem = cr.TIER_BASE_CPU_MILLIS["S"], cr.TIER_BASE_MEM_MI["S"]
        for _ in range(cr.RESIZE_CONSECUTIVE_CYCLES):
            cpu, mem, state = cr.decide_next_resources(cpu, mem, "under", state, now=0)
        assert cpu == cr.TIER_BASE_CPU_MILLIS["S"]
        assert mem == cr.TIER_BASE_MEM_MI["S"]

    def test_signal_flip_resets_consecutive_count(self):
        state = self._state(signal="under", consecutive=5, last_resize_ts=-100_000)
        cpu, mem, state = cr.decide_next_resources(500, 1024, "over", state, now=0)
        assert state["signal"] == "over"
        assert state["consecutive"] == 1


class TestQuantityParsers:
    def test_parse_cpu_millis(self):
        assert cr.parse_cpu_millis("250m") == 250
        assert cr.parse_cpu_millis("2") == 2000
        assert cr.parse_cpu_millis("0.5") == 500
        assert cr.parse_cpu_millis(None) is None
        assert cr.parse_cpu_millis("") is None

    def test_parse_mem_mi(self):
        assert cr.parse_mem_mi("512Mi") == 512
        assert cr.parse_mem_mi("2Gi") == 2048
        assert cr.parse_mem_mi("1Ki") == 0  # rounds down under 1 Mi
        assert cr.parse_mem_mi(None) is None
        assert cr.parse_mem_mi("garbage") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

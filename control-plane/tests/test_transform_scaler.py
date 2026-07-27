"""Unit tests for app.services.transform_scaler — the pure decision logic
behind control-plane's direct transform-worker replica scaling (Phase 3b,
replacing the stale KEDA ScaledObject).

Pure-Python only (no Redis/K8s/DB) so these run even in this sandbox's
broken-pip environment (see the task summary for what could NOT be
executed — kubernetes/redis themselves aren't importable here, but nothing
in this module needs them at import time).
"""
import os
import sys

import pytest

HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services import transform_scaler as ts  # noqa: E402


class TestComputeDesiredReplicas:
    def test_zero_signals_zero_replicas(self):
        assert ts.compute_desired_replicas(0, 0) == 0

    def test_high_queue_drives_one_pod_per_task(self):
        # HIGH_TASKS_PER_REPLICA default is 1 -> 1 task = 1 pod immediately,
        # matching the retired ScaledObject's own listLength: "1".
        assert ts.compute_desired_replicas(1, 0) == 1
        assert ts.compute_desired_replicas(7, 0) == 7

    def test_cdc_backlog_batches_five_per_pod(self):
        # CDC_BACKLOG_PER_REPLICA default is 5, matching the retired
        # (never-firing) trigger's own listLength: "5".
        assert ts.compute_desired_replicas(0, 5) == 1
        assert ts.compute_desired_replicas(0, 6) == 2
        assert ts.compute_desired_replicas(0, 50) == 10

    def test_desired_is_max_across_signals_not_sum(self):
        # Mirrors KEDA multi-trigger semantics: each trigger votes
        # independently, the highest vote wins.
        assert ts.compute_desired_replicas(3, 50) == max(3, 10)

    def test_clamped_to_max_replicas(self):
        assert ts.compute_desired_replicas(10_000, 0) <= ts.MAX_REPLICAS

    def test_clamped_to_min_replicas(self):
        assert ts.compute_desired_replicas(0, 0) >= ts.MIN_REPLICAS


class TestDecideNextReplicas:
    def _state(self, **overrides):
        state = {"direction": None, "consecutive": 0, "last_scale_ts": 0}
        state.update(overrides)
        return state

    def test_no_change_when_desired_equals_current(self):
        next_r, state = ts.decide_next_replicas(3, 3, self._state(), now=1000)
        assert next_r == 3
        assert state["direction"] is None
        assert state["consecutive"] == 0

    def test_scale_up_fires_on_first_confirming_tick_by_default(self):
        assert ts.SCALE_UP_CONSECUTIVE_TICKS == 1
        next_r, state = ts.decide_next_replicas(0, 3, self._state(), now=1000)
        assert next_r == 3
        assert state["last_scale_ts"] == 1000

    def test_scale_down_requires_multiple_consecutive_ticks(self):
        state = self._state()
        now = 0.0
        # Cooldown window must also be satisfied — start last_scale_ts far
        # enough in the past that only the consecutive-tick gate is being
        # exercised in this test.
        state["last_scale_ts"] = -10_000
        for i in range(ts.SCALE_DOWN_CONSECUTIVE_TICKS - 1):
            next_r, state = ts.decide_next_replicas(5, 2, state, now=now + i)
            assert next_r == 5, "must not scale down before enough consecutive ticks"
        next_r, state = ts.decide_next_replicas(
            5, 2, state, now=now + ts.SCALE_DOWN_CONSECUTIVE_TICKS)
        assert next_r == 2

    def test_cooldown_blocks_a_scale_even_with_enough_consecutive_ticks(self):
        state = self._state(last_scale_ts=1000)
        now = 1000 + ts.SCALE_COOLDOWN_S - 1  # still inside cooldown
        for _ in range(ts.SCALE_DOWN_CONSECUTIVE_TICKS + 2):
            next_r, state = ts.decide_next_replicas(5, 1, state, now=now)
        assert next_r == 5, "cooldown must block the scale-down even after enough ticks"

    def test_cooldown_expires_and_scale_proceeds(self):
        state = self._state(last_scale_ts=1000)
        now = 1000 + ts.SCALE_COOLDOWN_S + 1
        state["direction"] = "down"
        state["consecutive"] = ts.SCALE_DOWN_CONSECUTIVE_TICKS - 1
        next_r, state = ts.decide_next_replicas(5, 1, state, now=now)
        assert next_r == 1

    def test_direction_flip_resets_consecutive_count(self):
        # Stay inside cooldown so the scale-up cannot fire yet — with
        # SCALE_UP_CONSECUTIVE_TICKS=1 a cool-down-expired flip would
        # apply immediately and reset direction to None, hiding the
        # consecutive-restart bookkeeping this test is asserting.
        state = self._state(direction="down", consecutive=5, last_scale_ts=0)
        next_r, state = ts.decide_next_replicas(2, 5, state, now=0)
        assert next_r == 2, "cooldown must block the scale-up"
        assert state["direction"] == "up"
        assert state["consecutive"] == 1

    def test_result_always_clamped(self):
        next_r, _ = ts.decide_next_replicas(-5, 10_000, {}, now=0)
        assert ts.MIN_REPLICAS <= next_r <= ts.MAX_REPLICAS

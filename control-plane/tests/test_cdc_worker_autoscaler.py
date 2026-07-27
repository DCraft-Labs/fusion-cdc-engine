"""Unit tests for app.services.cdc_worker_autoscaler — the cdc-worker
StatefulSet direct-scaling reconcile loop (source-count/tier sized).

NOT SAFE TO ENABLE IN PRODUCTION YET: see that module's docstring — the
real scaling action is gated behind CDC_WORKER_DIRECT_SCALING_ENABLED
(default "false") until Phase 2's source-sharding fix
(app/services/source_assignment.py) is verified against a live
multi-replica cdc-worker StatefulSet. These tests only exercise the pure,
dependency-free computation (tier weighting, desired-replica math,
hysteresis/debounce state machine) plus the flag's default-off behavior —
same dependency-light convention
tests/test_connections/test_source_assignment_p2.py and
tests/test_resource_admission.py already use in this suite (plain
SimpleNamespace stand-ins, no sqlalchemy/kubernetes/live DB/Redis
required).

ENVIRONMENT NOTE: this sandbox's Python 3.14 has a broken pyexpat, which
blocks `pip install`, so neither `pytest` nor `sqlalchemy`/`kubernetes` are
installed here. These tests were hand-verified logically (traced through
by hand against the implementation) but could NOT actually be executed via
`pytest` in this environment — same caveat as
tests/test_resource_admission.py. `python3 -m py_compile` was used instead
to confirm the module and this test file are at least syntactically valid.
"""
import importlib
import os
import sys
from types import SimpleNamespace

import pytest

HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services import cdc_worker_autoscaler as autoscaler  # noqa: E402
from app.services import resource_admission  # noqa: E402


def _connection(tier=None, is_deleted=False):
    resource_limits = {"admission": {"tier": tier}} if tier else {}
    return SimpleNamespace(resource_limits=resource_limits, is_deleted=is_deleted)


def _source(connections=None):
    return SimpleNamespace(connections=connections if connections is not None else [])


# ---------------------------------------------------------------------------
# Feature flag default
# ---------------------------------------------------------------------------

class TestFeatureFlagDefault:
    def test_default_is_disabled_when_env_var_unset(self, monkeypatch):
        monkeypatch.delenv("CDC_WORKER_DIRECT_SCALING_ENABLED", raising=False)
        reloaded = importlib.reload(autoscaler)
        try:
            assert reloaded.CDC_WORKER_DIRECT_SCALING_ENABLED is False
        finally:
            importlib.reload(autoscaler)  # restore module state for later tests

    def test_stays_disabled_on_any_non_true_value(self, monkeypatch):
        for value in ("False", "0", "no", "garbage", ""):
            monkeypatch.setenv("CDC_WORKER_DIRECT_SCALING_ENABLED", value)
            reloaded = importlib.reload(autoscaler)
            assert reloaded.CDC_WORKER_DIRECT_SCALING_ENABLED is False, value
        monkeypatch.delenv("CDC_WORKER_DIRECT_SCALING_ENABLED", raising=False)
        importlib.reload(autoscaler)

    def test_only_enabled_on_explicit_true(self, monkeypatch):
        monkeypatch.setenv("CDC_WORKER_DIRECT_SCALING_ENABLED", "true")
        reloaded = importlib.reload(autoscaler)
        try:
            assert reloaded.CDC_WORKER_DIRECT_SCALING_ENABLED is True
        finally:
            monkeypatch.delenv("CDC_WORKER_DIRECT_SCALING_ENABLED", raising=False)
            importlib.reload(autoscaler)


# ---------------------------------------------------------------------------
# Tier weighting
# ---------------------------------------------------------------------------

class TestTierWeight:
    def test_m_is_baseline_one(self):
        assert autoscaler._tier_weight("M") == pytest.approx(1.0)

    def test_s_is_half_of_m(self):
        assert autoscaler._tier_weight("S") == pytest.approx(0.5)

    def test_l_is_four_x_m(self):
        assert autoscaler._tier_weight("L") == pytest.approx(4.0)

    def test_xl_is_eight_x_m(self):
        assert autoscaler._tier_weight("XL") == pytest.approx(8.0)

    def test_unknown_tier_falls_back_to_m_weight(self):
        assert autoscaler._tier_weight("bogus") == pytest.approx(1.0)


class TestConnectionTier:
    def test_reads_stamped_admission_tier(self):
        assert autoscaler._connection_tier(_connection(tier="XL")) == "XL"

    def test_defaults_to_m_when_no_admission_block(self):
        assert autoscaler._connection_tier(_connection(tier=None)) == "M"

    def test_defaults_to_m_when_resource_limits_none(self):
        conn = SimpleNamespace(resource_limits=None, is_deleted=False)
        assert autoscaler._connection_tier(conn) == "M"


class TestSourceWeight:
    def test_source_with_no_connections_gets_m_baseline_weight(self):
        assert autoscaler.source_weight(_source([])) == pytest.approx(1.0)

    def test_source_weight_is_max_not_sum_across_connections(self):
        # Judgment call: a source's capture cost is driven by its own
        # tables, not multiplied by how many connections read them — so
        # three S-tier connections plus one XL-tier connection on the same
        # source should weight like ONE XL source, not 0.5+0.5+0.5+8=9.5.
        src = _source([
            _connection(tier="S"), _connection(tier="S"),
            _connection(tier="S"), _connection(tier="XL"),
        ])
        assert autoscaler.source_weight(src) == pytest.approx(8.0)

    def test_deleted_connections_are_excluded(self):
        src = _source([
            _connection(tier="XL", is_deleted=True),
            _connection(tier="S", is_deleted=False),
        ])
        assert autoscaler.source_weight(src) == pytest.approx(0.5)

    def test_deleted_only_falls_back_to_m_baseline(self):
        src = _source([_connection(tier="XL", is_deleted=True)])
        assert autoscaler.source_weight(src) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# compute_desired_replicas
# ---------------------------------------------------------------------------

class TestComputeDesiredReplicas:
    def test_zero_weight_returns_floor(self):
        assert autoscaler.compute_desired_replicas(0.0, min_replicas=1, max_replicas=10) == 1

    def test_light_load_stays_at_floor(self):
        # 3 M-tier sources (weight 3.0) under target_weight_per_pod=8 -> ceil(3/8)=1
        assert autoscaler.compute_desired_replicas(
            3.0, min_replicas=1, max_replicas=10, target_weight_per_pod=8.0
        ) == 1

    def test_scales_up_with_heavier_load(self):
        # 20 M-tier-equivalent weight / 8 per pod -> ceil(2.5) = 3
        assert autoscaler.compute_desired_replicas(
            20.0, min_replicas=1, max_replicas=10, target_weight_per_pod=8.0
        ) == 3

    def test_clamped_to_max_replicas_ceiling(self):
        assert autoscaler.compute_desired_replicas(
            10_000.0, min_replicas=1, max_replicas=10, target_weight_per_pod=8.0
        ) == 10

    def test_multi_tier_mix_scales_correctly(self):
        # 5 S (0.5 each=2.5) + 2 M (1.0 each=2.0) + 1 L (4.0) + 1 XL (8.0)
        # = 16.5 total weight; target=8 -> ceil(16.5/8) = 3
        total = 5 * 0.5 + 2 * 1.0 + 1 * 4.0 + 1 * 8.0
        assert total == pytest.approx(16.5)
        assert autoscaler.compute_desired_replicas(
            total, min_replicas=1, max_replicas=10, target_weight_per_pod=8.0
        ) == 3

    def test_never_goes_below_min_replicas_floor(self):
        assert autoscaler.compute_desired_replicas(
            0.1, min_replicas=2, max_replicas=10, target_weight_per_pod=8.0
        ) == 2

    def test_min_replicas_can_be_zero_when_no_sources(self):
        assert autoscaler.compute_desired_replicas(
            0.0, min_replicas=0, max_replicas=10, target_weight_per_pod=8.0
        ) == 0


# ---------------------------------------------------------------------------
# Hysteresis / debounce / cooldown state machine
# ---------------------------------------------------------------------------

class TestShouldApplyScale:
    # Fresh state's last_scale_ts starts at 0.0 (see
    # cdc_worker_autoscaler._scale_state's default), and real
    # time.monotonic() values are large, arbitrary-origin numbers that are
    # already far past any cooldown window relative to 0.0. BASE_NOW
    # mimics that (100000s in) so "elapsed since last real scale" behaves
    # like a long-running process's first-ever scale, not like a
    # process that booted 100 seconds ago — the distinction matters
    # because CDC_WORKER_SCALE_COOLDOWN_SECONDS defaults to 600.
    BASE_NOW = 100_000.0

    def setup_method(self):
        autoscaler._scale_state.clear()

    def test_no_change_needed_when_desired_equals_current(self):
        should, reason = autoscaler._should_apply_scale(
            "sts-a", desired=2, current=2, now=self.BASE_NOW
        )
        assert should is False
        assert "already at desired" in reason

    def test_first_observation_of_a_change_is_debounced(self):
        should, reason = autoscaler._should_apply_scale(
            "sts-a", desired=3, current=2, now=self.BASE_NOW
        )
        assert should is False
        assert "debounc" in reason

    def test_confirmed_on_second_consecutive_cycle_applies_when_no_prior_scale(self):
        # With last_scale_ts still at its 0.0 default (no scale action has
        # ever been taken on this StatefulSet), the cooldown window is
        # trivially satisfied — a fresh StatefulSet's very first scale
        # isn't wrongly blocked forever waiting on a cooldown that never
        # started.
        autoscaler._should_apply_scale("sts-a", desired=3, current=2, now=self.BASE_NOW)
        should, reason = autoscaler._should_apply_scale(
            "sts-a", desired=3, current=2, now=self.BASE_NOW + 1
        )
        assert should is True
        assert reason == "ok"

    def test_flapping_desired_value_resets_the_debounce_streak(self):
        autoscaler._should_apply_scale("sts-a", desired=3, current=2, now=self.BASE_NOW)
        # Signal flips back before confirmation completes -> streak resets.
        should, reason = autoscaler._should_apply_scale(
            "sts-a", desired=2, current=2, now=self.BASE_NOW + 1
        )
        assert should is False
        assert "already at desired" in reason

    def test_cooldown_blocks_a_second_scale_too_soon_after_the_first(self):
        autoscaler._should_apply_scale("sts-a", desired=3, current=2, now=self.BASE_NOW)
        autoscaler._should_apply_scale("sts-a", desired=3, current=2, now=self.BASE_NOW + 1)
        autoscaler._record_scale_applied("sts-a", now=self.BASE_NOW + 1)

        # Desired flips down immediately after scaling up -- exactly the
        # flap scenario hysteresis exists to prevent. Only ~2s have
        # elapsed since the recorded scale, well under the 600s default
        # cooldown.
        autoscaler._should_apply_scale("sts-a", desired=2, current=3, now=self.BASE_NOW + 2)
        should, reason = autoscaler._should_apply_scale(
            "sts-a", desired=2, current=3, now=self.BASE_NOW + 3
        )
        assert should is False
        assert "cooldown" in reason

    def test_cooldown_clears_after_the_configured_window(self):
        autoscaler._should_apply_scale("sts-a", desired=3, current=2, now=self.BASE_NOW)
        autoscaler._should_apply_scale("sts-a", desired=3, current=2, now=self.BASE_NOW + 1)
        autoscaler._record_scale_applied("sts-a", now=self.BASE_NOW + 1)

        far_future = self.BASE_NOW + 1 + autoscaler.CDC_WORKER_SCALE_COOLDOWN_SECONDS + 10
        autoscaler._should_apply_scale("sts-a", desired=2, current=3, now=far_future)
        should, reason = autoscaler._should_apply_scale(
            "sts-a", desired=2, current=3, now=far_future + 1
        )
        assert should is True
        assert reason == "ok"

    def test_different_statefulsets_have_independent_state(self):
        autoscaler._should_apply_scale("sts-mysql", desired=3, current=2, now=self.BASE_NOW)
        should, reason = autoscaler._should_apply_scale(
            "sts-postgres", desired=3, current=2, now=self.BASE_NOW
        )
        assert should is False
        assert "debounc" in reason


# ---------------------------------------------------------------------------
# Dry-run behavior of _reconcile_source_type when the flag is disabled
# ---------------------------------------------------------------------------

class _FakeStatefulSetSpec:
    def __init__(self, replicas):
        self.replicas = replicas


class _FakeStatefulSet:
    def __init__(self, replicas):
        self.spec = _FakeStatefulSetSpec(replicas)


class _FakeAppsV1:
    """Records whether patch_namespaced_stateful_set was ever called, so
    dry-run tests can assert it never fires while the flag is off."""

    def __init__(self, current_replicas):
        self._current_replicas = current_replicas
        self.patch_calls = []

    def read_namespaced_stateful_set(self, name, namespace):
        return _FakeStatefulSet(self._current_replicas)

    def patch_namespaced_stateful_set(self, name, namespace, body):
        self.patch_calls.append((name, namespace, body))


class TestReconcileSourceTypeDryRun:
    def setup_method(self):
        autoscaler._scale_state.clear()

    def test_dry_run_never_patches_even_when_desired_differs(self, monkeypatch):
        monkeypatch.setattr(autoscaler, "CDC_WORKER_DIRECT_SCALING_ENABLED", False)
        monkeypatch.setattr(
            autoscaler, "_weighted_source_count", lambda db, source_type: (40.0, 5)
        )
        fake_apps_v1 = _FakeAppsV1(current_replicas=1)

        result = autoscaler._reconcile_source_type(
            db=None, k8s=None, apps_v1=fake_apps_v1,
            source_type="mysql", release_name="fusion", namespace="fusion-ns",
        )

        assert fake_apps_v1.patch_calls == []
        assert result["applied"] is False
        assert result["enabled"] is False
        assert result["desired_replicas"] > result["current_replicas"]

    def test_dry_run_still_computes_and_reports_desired_state(self, monkeypatch):
        monkeypatch.setattr(autoscaler, "CDC_WORKER_DIRECT_SCALING_ENABLED", False)
        monkeypatch.setattr(
            autoscaler, "_weighted_source_count", lambda db, source_type: (2.0, 2)
        )
        fake_apps_v1 = _FakeAppsV1(current_replicas=1)

        result = autoscaler._reconcile_source_type(
            db=None, k8s=None, apps_v1=fake_apps_v1,
            source_type="postgres", release_name="fusion", namespace="fusion-ns",
        )

        assert result["source_count"] == 2
        assert result["total_weight"] == pytest.approx(2.0)
        assert result["current_replicas"] == 1
        assert result["desired_replicas"] == 1  # stays at floor, no patch needed either way


# ---------------------------------------------------------------------------
# Scale-down coordination with Phase 2's rebalance mechanism
# ---------------------------------------------------------------------------

class TestScaleDownRebalanceCoordination:
    def setup_method(self):
        autoscaler._scale_state.clear()

    def test_scale_down_rebalances_at_future_pod_count_before_patching(self, monkeypatch):
        monkeypatch.setattr(autoscaler, "CDC_WORKER_DIRECT_SCALING_ENABLED", True)
        monkeypatch.setattr(
            autoscaler, "_weighted_source_count", lambda db, source_type: (1.0, 1)
        )
        # Force the debounce/cooldown gate open immediately for this test.
        monkeypatch.setattr(
            autoscaler, "_should_apply_scale",
            lambda name, desired, current, now: (True, "ok"),
        )

        calls = []

        def _fake_rebalance(db, source_type, pod_count):
            calls.append(("rebalance", source_type, pod_count))
            return {"rebalanced": True, "pod_count": pod_count}

        monkeypatch.setattr(autoscaler, "rebalance_source_type_at_pod_count", _fake_rebalance)

        fake_apps_v1 = _FakeAppsV1(current_replicas=5)
        original_patch = fake_apps_v1.patch_namespaced_stateful_set

        def _tracking_patch(name, namespace, body):
            calls.append(("patch", name, body))
            return original_patch(name, namespace, body)

        fake_apps_v1.patch_namespaced_stateful_set = _tracking_patch

        result = autoscaler._reconcile_source_type(
            db=None, k8s=None, apps_v1=fake_apps_v1,
            source_type="mysql", release_name="fusion", namespace="fusion-ns",
        )

        assert result["applied"] is True
        assert result["desired_replicas"] == 1
        # Rebalance must happen BEFORE the patch, and at the FUTURE
        # (post-scale-down) pod count, not the current one.
        assert calls[0] == ("rebalance", "mysql", 1)
        assert calls[1][0] == "patch"
        assert fake_apps_v1.patch_calls[0][2]["spec"]["replicas"] == 1

    def test_scale_down_is_skipped_if_rebalance_does_not_complete(self, monkeypatch):
        monkeypatch.setattr(autoscaler, "CDC_WORKER_DIRECT_SCALING_ENABLED", True)
        monkeypatch.setattr(
            autoscaler, "_weighted_source_count", lambda db, source_type: (1.0, 1)
        )
        monkeypatch.setattr(
            autoscaler, "_should_apply_scale",
            lambda name, desired, current, now: (True, "ok"),
        )
        monkeypatch.setattr(
            autoscaler, "rebalance_source_type_at_pod_count",
            lambda db, source_type, pod_count: {"rebalanced": False, "reason": "k8s unavailable"},
        )

        fake_apps_v1 = _FakeAppsV1(current_replicas=5)

        result = autoscaler._reconcile_source_type(
            db=None, k8s=None, apps_v1=fake_apps_v1,
            source_type="mysql", release_name="fusion", namespace="fusion-ns",
        )

        assert fake_apps_v1.patch_calls == []
        assert result["applied"] is False
        assert "rebalance" in result["reason"]

    def test_scale_up_does_not_require_a_rebalance_call(self, monkeypatch):
        # Scaling UP doesn't strand anything (no pod is being removed), so
        # rebalance_source_type_at_pod_count should NOT be called — the
        # existing heartbeat-triggered maybe_rebalance_on_heartbeat handles
        # spreading sources onto the new pod once it's ready.
        monkeypatch.setattr(autoscaler, "CDC_WORKER_DIRECT_SCALING_ENABLED", True)
        monkeypatch.setattr(
            autoscaler, "_weighted_source_count", lambda db, source_type: (40.0, 5)
        )
        monkeypatch.setattr(
            autoscaler, "_should_apply_scale",
            lambda name, desired, current, now: (True, "ok"),
        )

        rebalance_called = []
        monkeypatch.setattr(
            autoscaler, "rebalance_source_type_at_pod_count",
            lambda db, source_type, pod_count: rebalance_called.append(pod_count) or {"rebalanced": True},
        )

        fake_apps_v1 = _FakeAppsV1(current_replicas=1)

        result = autoscaler._reconcile_source_type(
            db=None, k8s=None, apps_v1=fake_apps_v1,
            source_type="mysql", release_name="fusion", namespace="fusion-ns",
        )

        assert rebalance_called == []
        assert result["applied"] is True
        assert fake_apps_v1.patch_calls[0][2]["spec"]["replicas"] == result["desired_replicas"]

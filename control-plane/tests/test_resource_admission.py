"""Unit tests for app.services.resource_admission — tier resolution, mode
resource requirements, and ETA estimation for the admission-control system.

These are pure-Python (no DB/Redis) so they run even in this sandbox's
broken-pip environment; see the note in test_resource_ledger.py for what
could NOT be executed here (this file's tests were hand-verified logically
but could not be run through ``pytest`` itself, since pytest isn't
installed — see the task summary).
"""
import os
import sys

import pytest

HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services import resource_admission as ra  # noqa: E402


class TestResolveTier:
    def test_below_one_million_is_s(self):
        assert ra.resolve_tier(0) == "S"
        assert ra.resolve_tier(999_999) == "S"

    def test_one_million_boundary_is_m(self):
        # S/M boundary intentionally matches AUTO_BULK_MODE_ROW_THRESHOLD
        # (1,000,000) in connections.py.
        assert ra.resolve_tier(1_000_000) == "M"
        assert ra.resolve_tier(19_999_999) == "M"

    def test_twenty_million_boundary_is_l(self):
        assert ra.resolve_tier(20_000_000) == "L"
        assert ra.resolve_tier(199_999_999) == "L"

    def test_two_hundred_million_boundary_is_xl(self):
        assert ra.resolve_tier(200_000_000) == "XL"
        assert ra.resolve_tier(10_000_000_000) == "XL"

    def test_none_defaults_to_m(self):
        assert ra.resolve_tier(None) == "M"

    def test_non_numeric_defaults_to_m(self):
        assert ra.resolve_tier("not-a-number") == "M"


class TestModeResourceRequirement:
    def test_saver_is_always_k1(self):
        for tier in ra.TIERS:
            req = ra.mode_resource_requirement(tier, "saver", 5_000_000)
            assert req["parallelism"] == 1

    def test_saver_uses_less_than_one_tier_worker_worth(self):
        """Saver's overhead multiplier (<1.0) means it reserves LESS than a
        single tier-worker's base footprint — genuinely the smallest viable
        footprint, not just 'K=1 at full size'."""
        req = ra.mode_resource_requirement("M", "saver", 5_000_000)
        assert req["cpu_millis"] < ra.TIER_BASE_CPU_MILLIS["M"]
        assert req["memory_mi"] < ra.TIER_BASE_MEM_MI["M"]

    def test_normal_uses_default_parallelism(self):
        # Large enough table that MIN_ROWS_PER_PARTITION doesn't cap K.
        req = ra.mode_resource_requirement("XL", "normal", 500_000_000)
        default_k, _ = ra._connection_parallelism_bounds()
        assert req["parallelism"] == default_k

    def test_aggressive_uses_more_parallelism_than_normal(self):
        rows = 500_000_000
        normal = ra.mode_resource_requirement("XL", "normal", rows)
        aggressive = ra.mode_resource_requirement("XL", "aggressive", rows)
        assert aggressive["parallelism"] > normal["parallelism"]
        assert aggressive["cpu_millis"] > normal["cpu_millis"]

    def test_aggressive_parallelism_capped_for_small_tables(self):
        """A tiny table shouldn't get MAX_PARALLELISM workers even in
        aggressive mode — capped by MIN_ROWS_PER_PARTITION."""
        _, max_k = ra._connection_parallelism_bounds()
        req = ra.mode_resource_requirement("S", "aggressive", 10_000)
        assert req["parallelism"] < max_k
        assert req["parallelism"] >= 1

    def test_tier_ordering_cpu_increases_s_to_xl(self):
        rows_by_tier = {"S": 500_000, "M": 5_000_000, "L": 50_000_000, "XL": 500_000_000}
        reqs = {t: ra.mode_resource_requirement(t, "normal", rows_by_tier[t]) for t in ra.TIERS}
        assert reqs["S"]["cpu_millis"] <= reqs["M"]["cpu_millis"]
        assert reqs["M"]["cpu_millis"] <= reqs["L"]["cpu_millis"]
        assert reqs["L"]["cpu_millis"] <= reqs["XL"]["cpu_millis"]

    def test_m_tier_normal_anchors_to_committer_default_request(self):
        """M tier's base footprint is anchored on committer_provisioner.py's
        _CPU_REQUEST/_MEM_REQUEST ('250m'/'512Mi')."""
        assert ra.TIER_BASE_CPU_MILLIS["M"] == 250
        assert ra.TIER_BASE_MEM_MI["M"] == 512

    def test_xl_tier_anchors_to_committer_limit(self):
        assert ra.TIER_BASE_CPU_MILLIS["XL"] == 2000
        assert ra.TIER_BASE_MEM_MI["XL"] == 2048


class TestEstimateEtaSeconds:
    def test_more_parallelism_yields_shorter_eta(self):
        rows = 10_000_000
        eta_k1, _ = ra.estimate_eta_seconds(rows, 1)
        eta_k4, _ = ra.estimate_eta_seconds(rows, 4)
        assert eta_k4 < eta_k1

    def test_zero_rows_is_zero_eta(self):
        eta, _ = ra.estimate_eta_seconds(0, 4)
        assert eta == 0
        eta_none, _ = ra.estimate_eta_seconds(None, 4)
        assert eta_none == 0

    def test_large_table_resolves_duckdb_bulk_mode(self):
        _, bulk_mode = ra.estimate_eta_seconds(5_000_000, 4)
        assert bulk_mode == "duckdb"

    def test_small_table_resolves_python_bulk_mode(self):
        _, bulk_mode = ra.estimate_eta_seconds(10_000, 4)
        assert bulk_mode == "python"


class TestObservedThroughputCorrection:
    class _FakeClient:
        """Minimal stand-in for the two hget/hset calls
        record_observed_throughput / get_observed_or_baseline_rate need —
        see test_resource_ledger.py's FakeRedis for the fuller version used
        against resource_ledger directly."""

        def __init__(self):
            self._store = {}

        def hget(self, key, field):
            return self._store.get((key, field))

        def hset(self, key, field, value):
            self._store[(key, field)] = value

    def test_falls_back_to_baseline_when_nothing_observed(self):
        client = self._FakeClient()
        rate = ra.get_observed_or_baseline_rate("duckdb", client=client)
        assert rate == ra.BASELINE_ROWS_PER_SEC_DUCKDB

    def test_first_observation_initializes_the_rate_directly(self):
        """With no prior stored rate, the first observation becomes the
        rate outright (nothing to blend with yet)."""
        client = self._FakeClient()
        ra.record_observed_throughput("duckdb", rows_written=200_000, elapsed_seconds=10.0, parallelism=1, client=client)
        new_rate = ra.get_observed_or_baseline_rate("duckdb", client=client)
        assert new_rate == pytest.approx(20_000.0)

    def test_second_observation_blends_via_ema_not_overwrite(self):
        """A second, different observation should move the rate toward it
        via EMA rather than jumping straight to the new value or staying
        at the old one — the whole point of the doubling/halving-style
        'adapt from observation' pattern this mirrors (loader.py's
        ADAPTIVE_MIN_CHUNK/MAX_CHUNK) is gradual correction, not
        overwrite-on-every-sample."""
        client = self._FakeClient()
        ra.record_observed_throughput("duckdb", rows_written=200_000, elapsed_seconds=10.0, parallelism=1, client=client)
        first_rate = ra.get_observed_or_baseline_rate("duckdb", client=client)  # == 20,000

        # Second run observed much slower: 5,000 rows/sec/worker.
        ra.record_observed_throughput("duckdb", rows_written=50_000, elapsed_seconds=10.0, parallelism=1, client=client)
        second_rate = ra.get_observed_or_baseline_rate("duckdb", client=client)

        assert second_rate < first_rate  # moved down toward the slow sample
        assert second_rate > 5_000  # but didn't jump all the way to it (EMA)

    def test_zero_elapsed_is_ignored(self):
        client = self._FakeClient()
        ra.record_observed_throughput("duckdb", rows_written=100, elapsed_seconds=0, parallelism=1, client=client)
        assert client.hget(ra.OBSERVED_RATE_KEY, "duckdb") is None

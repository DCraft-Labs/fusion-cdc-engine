"""
Tests for dq/executor.py — 10 tests

One test per rule type (6) + on_fail behaviour tests (3) + violation record test (1).
"""
from datetime import datetime

import pytest

from dq.executor import DQExecutor


# ---------------------------------------------------------------------------
# Rule type tests
# ---------------------------------------------------------------------------

class TestNullRatioCheck:
    def test_null_ratio_check_fails_when_above_threshold(self, spark):
        # 3 out of 5 rows are null in 'value' → ratio 0.6 > max 0.2
        data = [("a", 1), ("b", None), ("c", None), ("d", 4), ("e", None)]
        df = spark.createDataFrame(data, ["name", "value"])

        policy = {"rules": [{"type": "null_ratio_check", "column": "value",
                              "max_null_ratio": 0.2}], "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        assert violations[0]["rule_type"] == "null_ratio_check"
        assert violations[0]["null_count"] == 3
        # alert mode: all rows written
        assert passed.count() == 5


class TestRangeCheck:
    def test_range_check_detects_out_of_range_values(self, spark):
        data = [(1, 0.5), (2, -0.1), (3, 1.5), (4, 0.8)]
        df = spark.createDataFrame(data, ["id", "score"])

        policy = {"rules": [{"type": "range_check", "column": "score",
                              "min_value": 0.0, "max_value": 1.0}], "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        assert violations[0]["rule_type"] == "range_check"
        assert violations[0]["failing_rows"] == 2


class TestFreshnessCheck:
    def test_freshness_check_detects_stale_data(self, spark):
        old_time = datetime(2020, 1, 1, 0, 0, 0)
        data = [(1, old_time), (2, old_time)]
        df = spark.createDataFrame(data, ["id", "created_at"])

        policy = {"rules": [{"type": "freshness_check", "column": "created_at",
                              "max_age_seconds": 3600}], "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        assert violations[0]["rule_type"] == "freshness_check"
        # alert: all rows passed to destination
        assert passed.count() == 2


class TestRegexCheck:
    def test_regex_check_rejects_non_matching_values(self, spark):
        data = [("12345678",), ("ABC",), ("9876543210",)]
        df = spark.createDataFrame(data, ["account_number"])

        policy = {"rules": [{"type": "regex_check", "column": "account_number",
                              "pattern": "^[0-9]{8,20}$"}], "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        assert violations[0]["failing_rows"] == 1  # "ABC" fails


class TestRowCountMatch:
    def test_row_count_match_warns_on_deviation(self, spark):
        # 100 rows actual vs 200 expected = 50% deviation > 10% threshold
        data = [(i,) for i in range(100)]
        df = spark.createDataFrame(data, ["id"])

        policy = {"rules": [{"type": "row_count_match", "expected_count": 200,
                              "threshold": 0.1}], "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        v = violations[0]
        assert v["rule_type"] == "row_count_match"
        assert v["actual_count"] == 100
        assert v["deviation"] > 0.1


class TestEnumCheck:
    def test_enum_check_rejects_invalid_values(self, spark):
        data = [("active",), ("inactive",), ("unknown",), ("pending",), ("INVALID",)]
        df = spark.createDataFrame(data, ["status"])

        policy = {"rules": [{"type": "enum_check", "column": "status",
                              "allowed_values": ["active", "inactive", "pending"]}],
                  "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        assert violations[0]["failing_rows"] == 2  # "unknown" and "INVALID"


# ---------------------------------------------------------------------------
# on_fail behaviour tests
# ---------------------------------------------------------------------------

class TestOnFailBlock:
    def test_on_fail_block_separates_failed_rows_into_dlq(self, spark):
        # range_check with block: out-of-range rows go to failed_df
        data = [(1, 100), (2, -5), (3, 50), (4, -1)]
        df = spark.createDataFrame(data, ["id", "amount"])

        policy = {"rules": [{"type": "range_check", "column": "amount",
                              "min_value": 0}], "on_fail": "block"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert passed.count() == 2   # id=1,3 (amount >= 0)
        assert failed.count() == 2   # id=2,4 (negative amounts)
        assert len(violations) == 1


class TestOnFailAlert:
    def test_on_fail_alert_writes_all_rows_to_destination(self, spark):
        data = [(1, None), (2, "Alice"), (3, None)]
        df = spark.createDataFrame(data, ["id", "name"])

        policy = {"rules": [{"type": "null_ratio_check", "column": "name",
                              "max_null_ratio": 0.0}], "on_fail": "alert"}
        passed, failed, violations = DQExecutor(policy).check(df)

        # All rows written to destination
        assert passed.count() == 3
        # Violations are reported
        assert len(violations) == 1


class TestOnFailContinue:
    def test_on_fail_continue_returns_all_rows_no_violations(self, spark):
        data = [("a", -1), ("b", 200)]
        df = spark.createDataFrame(data, ["name", "score"])

        policy = {"rules": [{"type": "range_check", "column": "score",
                              "min_value": 0, "max_value": 100}], "on_fail": "continue"}
        passed, failed, violations = DQExecutor(policy).check(df)

        assert passed.count() == 2
        assert failed.count() == 0
        # continue mode suppresses violations
        assert violations == []


# ---------------------------------------------------------------------------
# Violation record detail test
# ---------------------------------------------------------------------------

class TestViolationRecord:
    def test_violation_record_contains_rule_details(self, spark):
        data = [(1, 200), (2, -10)]
        df = spark.createDataFrame(data, ["id", "val"])

        policy = {"rules": [{"type": "range_check", "column": "val",
                              "min_value": 0, "max_value": 100}], "on_fail": "alert"}
        _, _, violations = DQExecutor(policy).check(df)

        assert len(violations) == 1
        v = violations[0]
        assert v["rule_type"] == "range_check"
        assert v["column"] == "val"
        assert "failing_rows" in v
        assert "min_value" in v
        assert "max_value" in v


# ---------------------------------------------------------------------------
# Prometheus metrics tests (spec §3)
# ---------------------------------------------------------------------------

class TestDQPrometheusMetrics:
    """Verify that DQ executor emits the three spec-mandated Prometheus metrics."""

    def _get_metric_value(self, metric, labels: dict):
        """Helper: extract current metric value via prometheus_client API."""
        for m in metric.collect():
            for sample in m.samples:
                if all(sample.labels.get(k) == v for k, v in labels.items()):
                    return sample.value
        return None

    def test_dq_rule_violation_total_incremented_on_failure(self, spark):
        """dq_rule_violation_total counter increments when a rule fails."""
        try:
            from dq.executor import _dq_rule_violation_total
        except ImportError:
            pytest.skip("prometheus_client not installed")

        data = [(1, None), (2, None), (3, 1)]
        df = spark.createDataFrame(data, ["id", "val"])
        policy = {"rules": [{"type": "null_ratio_check", "column": "val", "max_null_ratio": 0.1}],
                  "on_fail": "alert"}
        executor = DQExecutor(policy, tenant="test_t", connection="test_c")

        before = self._get_metric_value(
            _dq_rule_violation_total,
            {"tenant": "test_t", "connection": "test_c", "rule_type": "null_ratio_check"},
        ) or 0

        executor.check(df)

        after = self._get_metric_value(
            _dq_rule_violation_total,
            {"tenant": "test_t", "connection": "test_c", "rule_type": "null_ratio_check"},
        ) or 0

        assert after > before, f"Expected counter to increment, got before={before} after={after}"

    def test_dq_rule_status_pass_emits_1(self, spark):
        """dq_rule_status gauge is set to 1 (PASS) when rule succeeds."""
        try:
            from dq.executor import _dq_rule_status
        except ImportError:
            pytest.skip("prometheus_client not installed")

        data = [(1, 5), (2, 10), (3, 8)]
        df = spark.createDataFrame(data, ["id", "val"])
        policy = {"rules": [{"type": "range_check", "column": "val",
                              "min_value": 0, "max_value": 100}], "on_fail": "alert"}
        executor = DQExecutor(policy, tenant="test_t", connection="test_c_pass")
        executor.check(df)

        value = self._get_metric_value(
            _dq_rule_status,
            {"tenant": "test_t", "connection": "test_c_pass", "rule_type": "range_check"},
        )
        assert value == 1.0, f"Expected status=1.0 (PASS), got {value}"

    def test_dq_rule_status_fail_emits_0(self, spark):
        """dq_rule_status gauge is set to 0 (FAIL) when rule fails."""
        try:
            from dq.executor import _dq_rule_status
        except ImportError:
            pytest.skip("prometheus_client not installed")

        data = [(1, 200), (2, -10)]
        df = spark.createDataFrame(data, ["id", "val"])
        policy = {"rules": [{"type": "range_check", "column": "val",
                              "min_value": 0, "max_value": 100}], "on_fail": "alert"}
        executor = DQExecutor(policy, tenant="test_t", connection="test_c_fail")
        executor.check(df)

        value = self._get_metric_value(
            _dq_rule_status,
            {"tenant": "test_t", "connection": "test_c_fail", "rule_type": "range_check"},
        )
        assert value == 0.0, f"Expected status=0.0 (FAIL), got {value}"

    def test_dq_freshness_seconds_emitted(self, spark):
        """dq_freshness_seconds gauge is set when freshness_check fires."""
        try:
            from dq.executor import _dq_freshness_seconds
        except ImportError:
            pytest.skip("prometheus_client not installed")

        from datetime import datetime
        old_time = datetime(2020, 1, 1)
        data = [(1, old_time)]
        df = spark.createDataFrame(data, ["id", "ts"])
        policy = {"rules": [{"type": "freshness_check", "column": "ts",
                              "max_age_seconds": 60}], "on_fail": "alert"}
        executor = DQExecutor(policy, tenant="test_t", connection="test_c_freshness")
        executor.check(df)

        value = self._get_metric_value(
            _dq_freshness_seconds,
            {"tenant": "test_t", "connection": "test_c_freshness"},
        )
        assert value is not None, "dq_freshness_seconds was not emitted"
        assert value > 0, f"Expected freshness_seconds > 0, got {value}"


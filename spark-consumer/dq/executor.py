"""
DQExecutor — Data Quality rules executor for CDC streaming batches.

Usage:
    executor = DQExecutor(policy)
    passed_df, failed_df, violations = executor.check(df)

    passed_df  → write to destination
    failed_df  → write to DLQ  (non-empty only when on_fail='block')
    violations → list[dict] describing each rule failure

Policy schema:
    {
        "rules": [
            {"type": "null_ratio_check", "column": "amount", "max_null_ratio": 0.01},
            {"type": "range_check",      "column": "score",  "min_value": 0.0, "max_value": 1.0},
            {"type": "freshness_check",  "column": "ts",     "max_age_seconds": 3600},
            {"type": "regex_check",      "column": "acct",   "pattern": "^[0-9]{8,20}$"},
            {"type": "row_count_match",  "expected_count": 1000, "threshold": 0.02},
            {"type": "enum_check",       "column": "status", "allowed_values": ["active","inactive"]}
        ],
        "on_fail": "alert"   # or "block" | "continue"
    }

Prometheus metrics emitted (spec §3 — Data Quality & Sync Quality):
    dq_rule_status          Gauge     (tenant, connection, rule_type)  1=PASS 0=FAIL
    dq_rule_violation_total Counter   (tenant, connection, rule_type)
    dq_freshness_seconds    Gauge     (tenant, connection)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Type alias
ViolationList = List[dict]
CheckResult = Tuple[DataFrame, DataFrame, ViolationList]

# ---------------------------------------------------------------------------
# Prometheus metrics — graceful fallback when prometheus_client is absent
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge

    _dq_rule_status = Gauge(
        "dq_rule_status",
        "DQ rule status: 1=PASS, 0=FAIL",
        ["tenant", "connection", "rule_type"],
    )
    _dq_rule_violation_total = Counter(
        "dq_rule_violation_total",
        "Total DQ rule violations",
        ["tenant", "connection", "rule_type"],
    )
    _dq_freshness_seconds = Gauge(
        "dq_freshness_seconds",
        "Max timestamp age in seconds (freshness_check)",
        ["tenant", "connection"],
    )
    _PROM_AVAILABLE = True
except Exception:
    _PROM_AVAILABLE = False

    class _Noop:
        def labels(self, **_):
            return self
        def inc(self, amount=1):
            pass
        def set(self, value):
            pass

    _dq_rule_status = _Noop()
    _dq_rule_violation_total = _Noop()
    _dq_freshness_seconds = _Noop()


class DQExecutor:
    """Executes a DQ policy against a PySpark DataFrame batch."""

    _RULE_HANDLERS = {}

    def __init__(self, policy: dict, tenant: str = "unknown", connection: str = "unknown") -> None:
        self.rules: List[dict] = policy.get("rules", [])
        self.on_fail: str = policy.get("on_fail", "continue")
        self._tenant = tenant
        self._connection = connection

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def check(self, df: DataFrame) -> CheckResult:
        """
        Evaluate all rules against *df* (a static batch DataFrame).

        Returns:
            passed_df   — rows to write to destination
            failed_df   — rows to write to DLQ (only populated when on_fail='block')
            violations  — list of violation dicts
        """
        if self.on_fail == "continue":
            # Log only — run checks to collect violations but never block
            _, violations = self._evaluate(df)
            self._emit_metrics(violations)
            empty = df.filter(F.lit(False))
            logger.info("DQ continue mode: %d violations (ignored)", len(violations))
            return df, empty, []

        fail_flag, violations = self._evaluate(df)
        self._emit_metrics(violations)

        if not violations:
            # All rules PASS — emit PASS status for every rule type present
            for rule in self.rules:
                _dq_rule_status.labels(
                    tenant=self._tenant,
                    connection=self._connection,
                    rule_type=rule.get("type", "unknown"),
                ).set(1)
            empty = df.filter(F.lit(False))
            return df, empty, violations

        if self.on_fail == "alert":
            # Write all rows; report violations
            failed_for_report = df.filter(fail_flag) if fail_flag is not None else df.filter(F.lit(False))
            return df, failed_for_report, violations

        if self.on_fail == "block":
            if fail_flag is None:
                # Aggregate failure → block entire batch
                empty = df.filter(F.lit(False))
                return empty, df, violations
            passed_df = df.filter(~fail_flag)
            failed_df = df.filter(fail_flag)
            return passed_df, failed_df, violations

        # Unknown on_fail — treat as continue
        empty = df.filter(F.lit(False))
        return df, empty, violations

    # ------------------------------------------------------------------
    # Prometheus metric emission
    # ------------------------------------------------------------------

    def _emit_metrics(self, violations: ViolationList) -> None:
        """Emit dq_rule_status, dq_rule_violation_total, dq_freshness_seconds.

        Spec §5 (PDF5/PDF3): DQ violations must also be persisted to the control-plane
        audit_logs table via the internal DQ-violations API, not only emitted to Prometheus.
        """
        violated_types = {v.get("rule_type") for v in violations}

        for rule in self.rules:
            rt = rule.get("type", "unknown")
            is_failed = rt in violated_types
            status_val = 0 if is_failed else 1

            _dq_rule_status.labels(
                tenant=self._tenant,
                connection=self._connection,
                rule_type=rt,
            ).set(status_val)

            if is_failed:
                _dq_rule_violation_total.labels(
                    tenant=self._tenant,
                    connection=self._connection,
                    rule_type=rt,
                ).inc()

        # Emit dq_freshness_seconds for freshness_check violations
        for v in violations:
            if v.get("rule_type") == "freshness_check":
                age = v.get("age_seconds")
                if age is not None:
                    _dq_freshness_seconds.labels(
                        tenant=self._tenant,
                        connection=self._connection,
                    ).set(age)

        # Persist violations to control-plane audit_logs (spec §5 P5-5)
        if violations:
            self._persist_violations(violations)

    def _persist_violations(self, violations: ViolationList) -> None:
        """
        POST DQ violations to the control-plane internal API so they are recorded
        in audit_logs alongside Prometheus metrics.
        Failures are best-effort; logged but never raise.
        """
        import os
        import httpx

        control_plane_url = os.environ.get("CONTROL_PLANE_URL", "").rstrip("/")
        worker_token = os.environ.get("INTERNAL_API_TOKEN", "")
        if not control_plane_url:
            logger.debug("CONTROL_PLANE_URL not set — skipping DQ violation persistence")
            return

        payload = {
            "tenant": self._tenant,
            "connection_id": self._connection,
            "violations": violations,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        url = f"{control_plane_url}/api/v1/internal/dq-violations"
        headers = {"Authorization": f"Bearer {worker_token}", "Content-Type": "application/json"}
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.post(url, json=payload, headers=headers)
                if resp.status_code >= 300:
                    logger.warning("DQ violation persistence returned HTTP %s", resp.status_code)
        except Exception as exc:
            logger.warning("Could not persist DQ violations to control plane: %s", exc)

    # ------------------------------------------------------------------
    # Internal: evaluate all rules
    # ------------------------------------------------------------------

    def _evaluate(self, df: DataFrame):
        """
        Returns (combined_fail_flag: Column | None, violations: list).
        combined_fail_flag is None when only aggregate failures were found.
        """
        combined_flag = None  # Column (boolean) OR None
        violations: ViolationList = []
        has_aggregate_failure = False

        for rule in self.rules:
            rule_type = rule.get("type", "")
            has_violation, fail_cond, violation = self._check_rule(df, rule)

            if has_violation:
                violations.append(violation)
                if fail_cond is not None:
                    # Row-level failure
                    combined_flag = fail_cond if combined_flag is None else (combined_flag | fail_cond)
                else:
                    # Aggregate failure (no individual rows to flag)
                    has_aggregate_failure = True

        # If only aggregate failures, return None so caller blocks entire batch
        if has_aggregate_failure and combined_flag is None:
            return None, violations

        return combined_flag, violations

    def _check_rule(self, df: DataFrame, rule: dict):
        """Dispatch to the appropriate rule checker."""
        rule_type = rule.get("type", "")
        handler = {
            "null_ratio_check": self._null_ratio_check,
            "range_check": self._range_check,
            "freshness_check": self._freshness_check,
            "regex_check": self._regex_check,
            "row_count_match": self._row_count_match,
            "enum_check": self._enum_check,
            "referential_integrity": self._referential_integrity_check,
        }.get(rule_type)

        if handler is None:
            logger.warning("Unknown DQ rule type: %s", rule_type)
            return False, None, {}

        return handler(df, rule)

    # ------------------------------------------------------------------
    # Rule implementations
    # ------------------------------------------------------------------

    def _null_ratio_check(self, df: DataFrame, rule: dict):
        col_name = rule["column"]
        max_null_ratio: float = rule.get("max_null_ratio", 0.0)

        stats = df.agg(
            F.count("*").alias("total"),
            F.sum(F.col(col_name).isNull().cast("int")).alias("null_count"),
        ).collect()[0]

        total = stats["total"] or 0
        null_count = stats["null_count"] or 0

        if total == 0:
            return False, None, {}

        actual_ratio = null_count / total
        if actual_ratio > max_null_ratio:
            violation = {
                "rule_type": "null_ratio_check",
                "column": col_name,
                "actual_null_ratio": actual_ratio,
                "max_null_ratio": max_null_ratio,
                "null_count": null_count,
                "total": total,
            }
            fail_cond = F.col(col_name).isNull()
            return True, fail_cond, violation

        return False, None, {}

    def _range_check(self, df: DataFrame, rule: dict):
        col_name = rule["column"]
        min_val = rule.get("min_value")
        max_val = rule.get("max_value")

        fail_cond = F.lit(False)
        if min_val is not None:
            fail_cond = fail_cond | (F.col(col_name) < min_val)
        if max_val is not None:
            fail_cond = fail_cond | (F.col(col_name) > max_val)

        failing_count = df.filter(fail_cond).count()
        if failing_count > 0:
            violation = {
                "rule_type": "range_check",
                "column": col_name,
                "failing_rows": failing_count,
                "min_value": min_val,
                "max_value": max_val,
            }
            return True, fail_cond, violation

        return False, None, {}

    def _freshness_check(self, df: DataFrame, rule: dict):
        col_name = rule["column"]
        max_age_seconds: float = rule.get("max_age_seconds", 3600)

        result = df.agg(
            F.max(F.col(col_name).cast("timestamp")).alias("max_ts")
        ).collect()

        if not result or result[0]["max_ts"] is None:
            return False, None, {}

        max_ts: datetime = result[0]["max_ts"]
        # Ensure offset-naive comparison
        if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is not None:
            now = datetime.now(timezone.utc)
        else:
            now = datetime.utcnow()
            max_ts = max_ts.replace(tzinfo=None) if hasattr(max_ts, "replace") else max_ts

        try:
            age_seconds = (now.replace(tzinfo=None) - max_ts.replace(tzinfo=None)).total_seconds()
        except Exception:
            age_seconds = float("inf")

        if age_seconds > max_age_seconds:
            violation = {
                "rule_type": "freshness_check",
                "column": col_name,
                "age_seconds": age_seconds,
                "max_age_seconds": max_age_seconds,
            }
            # Aggregate failure — no individual row condition
            return True, None, violation

        return False, None, {}

    def _regex_check(self, df: DataFrame, rule: dict):
        col_name = rule["column"]
        pattern: str = rule["pattern"]

        fail_cond = ~F.col(col_name).rlike(pattern)
        failing_count = df.filter(fail_cond).count()

        if failing_count > 0:
            violation = {
                "rule_type": "regex_check",
                "column": col_name,
                "pattern": pattern,
                "failing_rows": failing_count,
            }
            return True, fail_cond, violation

        return False, None, {}

    def _row_count_match(self, df: DataFrame, rule: dict):
        threshold: float = rule.get("threshold", 0.02)
        expected_count = rule.get("expected_count")

        if expected_count is None or expected_count == 0:
            return False, None, {}

        actual_count = df.count()
        deviation = abs(actual_count - expected_count) / expected_count

        if deviation > threshold:
            violation = {
                "rule_type": "row_count_match",
                "actual_count": actual_count,
                "expected_count": expected_count,
                "deviation": deviation,
                "threshold": threshold,
            }
            # Aggregate failure
            return True, None, violation

        return False, None, {}

    def _enum_check(self, df: DataFrame, rule: dict):
        col_name = rule["column"]
        allowed_values: list = rule.get("allowed_values", [])

        if not allowed_values:
            return False, None, {}

        fail_cond = ~F.col(col_name).isin(allowed_values)
        failing_count = df.filter(fail_cond).count()

        if failing_count > 0:
            violation = {
                "rule_type": "enum_check",
                "column": col_name,
                "failing_rows": failing_count,
                "allowed_values": allowed_values,
            }
            return True, fail_cond, violation

        return False, None, {}

    def _referential_integrity_check(self, df: DataFrame, rule: dict):
        """
        referential_integrity — verifies that all values in *fk_column* appear in a
        reference DataFrame loaded from a JDBC source.

        Rule spec::
            {
                "type": "referential_integrity",
                "fk_column": "customer_id",
                "ref_jdbc_url": "jdbc:postgresql://host:5433/dw",
                "ref_jdbc_driver": "org.postgresql.Driver",
                "ref_table": "public.customers",
                "ref_pk_column": "id",
                "jdbc_user": "...",
                "jdbc_password": "..."
            }

        For in-memory unit tests *ref_values* (list) can be supplied instead of
        JDBC params to bypass the DB round-trip.
        """
        fk_col = rule["fk_column"]

        # Allow tests to inject reference values directly
        ref_values: Optional[list] = rule.get("ref_values")

        if ref_values is not None:
            allowed = ref_values
        else:
            jdbc_url: str = rule["ref_jdbc_url"]
            ref_table: str = rule["ref_table"]
            ref_pk: str = rule.get("ref_pk_column", "id")
            properties = {
                "user": rule.get("jdbc_user", ""),
                "password": rule.get("jdbc_password", ""),
            }
            if "ref_jdbc_driver" in rule:
                properties["driver"] = rule["ref_jdbc_driver"]

            ref_df = df.sparkSession.read.jdbc(
                url=jdbc_url, table=ref_table, properties=properties
            )
            allowed = [row[ref_pk] for row in ref_df.select(ref_pk).collect()]

        if not allowed:
            violation = {
                "rule_type": "referential_integrity",
                "fk_column": fk_col,
                "ref_table": rule.get("ref_table", "inline"),
                "failing_rows": df.count(),
                "message": "Reference set is empty — all rows fail",
            }
            return True, None, violation

        fail_cond = ~F.col(fk_col).isin(allowed)
        failing_count = df.filter(fail_cond).count()

        if failing_count > 0:
            violation = {
                "rule_type": "referential_integrity",
                "fk_column": fk_col,
                "ref_table": rule.get("ref_table", "inline"),
                "failing_rows": failing_count,
            }
            return True, fail_cond, violation

        return False, None, {}

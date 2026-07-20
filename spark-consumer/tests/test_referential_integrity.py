"""
Tests for the referential_integrity DQ rule (7th rule type — PDF gap).
"""
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestReferentialIntegrityRule:
    def test_all_fk_values_valid_passes(self, spark):
        from dq.executor import DQExecutor

        policy = {
            "rules": [
                {
                    "type": "referential_integrity",
                    "fk_column": "customer_id",
                    "ref_table": "customers",
                    "ref_values": [1, 2, 3],   # inline for unit tests
                }
            ],
            "on_fail": "block",
        }
        schema = StructType([
            StructField("op", StringType()),
            StructField("customer_id", IntegerType()),
        ])
        df = spark.createDataFrame([("c", 1), ("c", 2)], schema=schema)

        executor = DQExecutor(policy)
        passed, failed, violations = executor.check(df)

        assert passed.count() == 2
        assert failed.count() == 0
        assert violations == []

    def test_invalid_fk_value_blocked(self, spark):
        from dq.executor import DQExecutor

        policy = {
            "rules": [
                {
                    "type": "referential_integrity",
                    "fk_column": "customer_id",
                    "ref_table": "customers",
                    "ref_values": [1, 2],
                }
            ],
            "on_fail": "block",
        }
        schema = StructType([
            StructField("op", StringType()),
            StructField("customer_id", IntegerType()),
        ])
        df = spark.createDataFrame([("c", 1), ("c", 99)], schema=schema)  # 99 is invalid

        executor = DQExecutor(policy)
        passed, failed, violations = executor.check(df)

        assert passed.count() == 1
        assert failed.count() == 1
        assert len(violations) == 1
        assert violations[0]["rule_type"] == "referential_integrity"
        assert violations[0]["failing_rows"] == 1

    def test_empty_ref_set_fails_all(self, spark):
        from dq.executor import DQExecutor

        policy = {
            "rules": [
                {
                    "type": "referential_integrity",
                    "fk_column": "customer_id",
                    "ref_table": "customers",
                    "ref_values": [],
                }
            ],
            "on_fail": "block",
        }
        schema = StructType([
            StructField("op", StringType()),
            StructField("customer_id", IntegerType()),
        ])
        df = spark.createDataFrame([("c", 1)], schema=schema)

        executor = DQExecutor(policy)
        passed, failed, violations = executor.check(df)

        # Empty ref set = aggregate failure → entire batch blocked
        assert passed.count() == 0
        assert len(violations) == 1
        assert "empty" in violations[0]["message"].lower()

    def test_alert_mode_all_rows_pass_through(self, spark):
        from dq.executor import DQExecutor

        policy = {
            "rules": [
                {
                    "type": "referential_integrity",
                    "fk_column": "customer_id",
                    "ref_table": "customers",
                    "ref_values": [1],
                }
            ],
            "on_fail": "alert",
        }
        schema = StructType([
            StructField("op", StringType()),
            StructField("customer_id", IntegerType()),
        ])
        df = spark.createDataFrame([("c", 1), ("c", 999)], schema=schema)

        executor = DQExecutor(policy)
        passed, failed, violations = executor.check(df)

        # Alert mode: all rows pass through to destination
        assert passed.count() == 2
        assert len(violations) == 1

"""
Tests for transform/executor.py — 12 tests

One test per step type (10) + test_steps_applied_in_order + test_unknown_type_raises.
Uses PySpark local mode; no cluster needed.
"""
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import Row

from transform.executor import TransformPipelineExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _exec(spark, pipeline_spec, data, schema):
    df = spark.createDataFrame(data, schema)
    executor = TransformPipelineExecutor(pipeline_spec, spark=spark)
    return executor.apply(df), executor


# ---------------------------------------------------------------------------
# Tests — one per step type
# ---------------------------------------------------------------------------

class TestCastStep:
    def test_cast_converts_string_to_integer(self, spark):
        df = spark.createDataFrame([("42",), ("7",)], ["amount_str"])
        spec = {"transforms": [{"type": "cast", "column": "amount_str",
                                "to_type": "integer", "output_column": "amount"}]}
        result = TransformPipelineExecutor(spec).apply(df)

        amounts = [r["amount"] for r in result.collect()]
        assert amounts == [42, 7]
        assert dict(result.dtypes)["amount"] == "int"


class TestStringOpStep:
    def test_string_op_upper(self, spark):
        df = spark.createDataFrame([("alice",), ("bob",)], ["name"])
        spec = {"transforms": [{"type": "string_op", "column": "name",
                                "op": "upper", "output_column": "name_upper"}]}
        result = TransformPipelineExecutor(spec).apply(df)

        names = [r["name_upper"] for r in result.collect()]
        assert names == ["ALICE", "BOB"]

    def test_string_op_substring(self, spark):
        df = spark.createDataFrame([("1234567890",)], ["acct"])
        spec = {"transforms": [{"type": "string_op", "column": "acct",
                                "op": "substring",
                                "params": {"start": 1, "length": 4},
                                "output_column": "acct_prefix"}]}
        result = TransformPipelineExecutor(spec).apply(df)
        assert result.collect()[0]["acct_prefix"] == "1234"


class TestMathOpStep:
    def test_math_op_multiplies_columns(self, spark):
        df = spark.createDataFrame([(100.0, 1.25)], ["amount", "fx_rate"])
        spec = {"transforms": [{"type": "math_op",
                                "expression": "amount * fx_rate",
                                "output_column": "amount_usd"}]}
        result = TransformPipelineExecutor(spec).apply(df)
        assert result.collect()[0]["amount_usd"] == pytest.approx(125.0)


class TestDateOpStep:
    def test_date_op_extracts_year(self, spark):
        df = spark.createDataFrame([("2024-06-15",)], ["created_date"])
        # Cast to date first so year() works
        from pyspark.sql import functions as F
        df = df.withColumn("created_date", F.col("created_date").cast("date"))
        spec = {"transforms": [{"type": "date_op", "column": "created_date",
                                "op": "year", "output_column": "year"}]}
        result = TransformPipelineExecutor(spec).apply(df)
        assert result.collect()[0]["year"] == 2024


class TestJsonExtractStep:
    def test_json_extract_from_string_column(self, spark):
        df = spark.createDataFrame([('{"amount": 99.5, "currency": "USD"}',)], ["payload"])
        spec = {"transforms": [{"type": "json_extract",
                                "column": "payload",
                                "json_path": "$.amount",
                                "to_type": "double",
                                "output_column": "amount"}]}
        result = TransformPipelineExecutor(spec).apply(df)
        assert result.collect()[0]["amount"] == pytest.approx(99.5)


class TestJsonFlattenInlineStep:
    def test_json_flatten_inline_expands_keys_into_columns(self, spark):
        df = spark.createDataFrame([('{"currency": "USD", "merchant": "acme"}',)], ["meta"])
        spec = {"transforms": [{
            "type": "json_flatten_inline",
            "column": "meta",
            "json_schema": {"currency": "string", "merchant": "string"},
            "output_columns": {"currency": "currency", "merchant": "merchant"},
            "keep_original": False,
        }]}
        result = TransformPipelineExecutor(spec).apply(df)
        row = result.collect()[0]
        assert row["currency"] == "USD"
        assert row["merchant"] == "acme"
        assert "meta" not in result.columns


class TestJsonFlattenChildStep:
    def test_json_flatten_child_creates_child_table(self, spark):
        data = [("tx1", '[{"product_id": "p1", "qty": "2"}, {"product_id": "p2", "qty": "1"}]')]
        df = spark.createDataFrame(data, ["transaction_id", "line_items_json"])

        spec = {"transforms": [{
            "type": "json_flatten_child",
            "column": "line_items_json",
            "child_table": "transaction_line_items",
            "parent_keys": ["transaction_id"],
            "output_columns": {"product_id": "product_id", "qty": "quantity"},
            "keep_original": False,
        }]}
        executor = TransformPipelineExecutor(spec, spark=spark)
        result = executor.apply(df)

        # Parent df no longer has the array column
        assert "line_items_json" not in result.columns
        # Child table was produced
        assert "transaction_line_items" in executor.child_tables
        child = executor.child_tables["transaction_line_items"]
        assert child.count() == 2
        child_rows = {r["product_id"] for r in child.collect()}
        assert child_rows == {"p1", "p2"}


class TestMaskStep:
    def test_mask_last4_replaces_prefix_with_asterisks(self, spark):
        df = spark.createDataFrame([("1234567890",)], ["card"])
        spec = {"transforms": [{"type": "mask", "column": "card",
                                "strategy": "last4", "output_column": "card"}]}
        result = TransformPipelineExecutor(spec).apply(df)
        masked = result.collect()[0]["card"]
        # Last 4 chars must be preserved
        assert masked.endswith("7890")
        # Everything before must be masked
        assert "*" in masked
        assert len(masked) == 10

    def test_mask_hash_produces_sha256_hex(self, spark):
        df = spark.createDataFrame([("secret",)], ["password"])
        spec = {"transforms": [{"type": "mask", "column": "password",
                                "strategy": "hash", "output_column": "password_hash"}]}
        result = TransformPipelineExecutor(spec).apply(df)
        hashed = result.collect()[0]["password_hash"]
        # SHA-256 hex is always 64 chars
        assert len(hashed) == 64
        assert hashed != "secret"


class TestExpressionStep:
    def test_expression_spark_sql_case_when(self, spark):
        df = spark.createDataFrame([(1200.0,), (500.0,)], ["amount"])
        spec = {"transforms": [{
            "type": "expression",
            "expression": "CASE WHEN amount > 1000 THEN 'high' ELSE 'normal' END",
            "output_column": "risk_label",
        }]}
        result = TransformPipelineExecutor(spec).apply(df)
        labels = [r["risk_label"] for r in result.collect()]
        assert labels == ["high", "normal"]


class TestUDFStep:
    def test_udf_step_fetches_registers_and_applies(self, spark):
        """UDF code is fetched from registry URL, registered, and applied."""
        df = spark.createDataFrame([(10.0,), (20.0,)], ["amount"])
        spec = {"transforms": [{
            "type": "udf",
            "function": "double_amount",
            "args": ["amount"],
            "output_column": "doubled",
        }]}

        udf_code = "def double_amount(x):\n    return float(x) * 2 if x else None\n"
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"code": udf_code, "return_type": "double"}

        with patch("requests.get", return_value=mock_resp):
            executor = TransformPipelineExecutor(
                spec, spark=spark, udf_registry_url="http://control-plane:8000"
            )
            result = executor.apply(df)

        doubled = [r["doubled"] for r in result.collect()]
        assert doubled == [20.0, 40.0]


# ---------------------------------------------------------------------------
# Cross-cutting behaviour tests
# ---------------------------------------------------------------------------

class TestPipelineOrdering:
    def test_steps_applied_in_order(self, spark):
        """Cast to int, then multiply — order matters."""
        df = spark.createDataFrame([("3",)], ["x_str"])
        spec = {"transforms": [
            {"type": "cast", "column": "x_str", "to_type": "double", "output_column": "x"},
            {"type": "math_op", "expression": "x * 10", "output_column": "x_times_10"},
        ]}
        result = TransformPipelineExecutor(spec).apply(df)
        assert result.collect()[0]["x_times_10"] == pytest.approx(30.0)


class TestUnknownStepType:
    def test_unknown_type_raises_value_error(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        spec = {"transforms": [{"type": "nonexistent_step", "column": "id"}]}
        with pytest.raises(ValueError, match="nonexistent_step"):
            TransformPipelineExecutor(spec).apply(df)

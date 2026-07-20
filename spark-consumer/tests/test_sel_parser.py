"""
Tests for transform/sel_parser.py — SEL → Spark SQL translation (spec §2).

These tests run WITHOUT PySpark — they validate the `sel_to_sql()` string
transformation only.  Full Spark integration is covered in test_transform_executor.py.
"""
import pytest
from transform.sel_parser import sel_to_sql


class TestIfTranslation:
    def test_simple_if(self):
        result = sel_to_sql("IF(currency = 'USD', amount, amount * fx_rate)")
        assert result.upper().startswith("CASE WHEN")
        assert "THEN" in result.upper()
        assert "ELSE" in result.upper()
        assert "END" in result.upper()

    def test_nested_if(self):
        expr = "IF(a > 0, IF(b > 0, 'positive', 'zero'), 'negative')"
        result = sel_to_sql(expr)
        assert result.upper().count("CASE WHEN") == 2
        assert result.upper().count("END") == 2

    def test_if_with_string_literals(self):
        result = sel_to_sql("IF(status = 'active', 1, 0)")
        assert "CASE WHEN status = 'active' THEN 1 ELSE 0 END" in result


class TestInTranslation:
    def test_in_list(self):
        result = sel_to_sql("IN(status, 'active', 'pending', 'closed')")
        # Should contain "status IN (..." or "status IN ('active'..."
        assert "status" in result.lower()
        assert " IN " in result.upper()
        assert "'active'" in result

    def test_not_in_list(self):
        result = sel_to_sql("NOT_IN(code, 'X', 'Y')")
        assert "NOT IN" in result.upper()
        assert "code" in result.lower()


class TestIsNullTranslation:
    def test_is_null(self):
        result = sel_to_sql("IS_NULL(column_a)")
        assert "IS NULL" in result.upper()
        assert "column_a" in result

    def test_is_not_null(self):
        result = sel_to_sql("IS_NOT_NULL(column_a)")
        assert "IS NOT NULL" in result.upper()
        assert "column_a" in result


class TestPassThroughFunctions:
    """Functions that are already valid Spark SQL should pass through unchanged."""

    def test_upper_passthrough(self):
        assert sel_to_sql("UPPER(name)") == "UPPER(name)"

    def test_lower_passthrough(self):
        assert sel_to_sql("LOWER(email)") == "LOWER(email)"

    def test_trim_passthrough(self):
        assert sel_to_sql("TRIM(field)") == "TRIM(field)"

    def test_coalesce_passthrough(self):
        assert sel_to_sql("COALESCE(a, b, 0)") == "COALESCE(a, b, 0)"

    def test_substring_passthrough(self):
        assert sel_to_sql("SUBSTRING(card, 1, 4)") == "SUBSTRING(card, 1, 4)"

    def test_arithmetic_passthrough(self):
        assert sel_to_sql("amount * fx_rate") == "amount * fx_rate"

    def test_plain_column_passthrough(self):
        assert sel_to_sql("my_column") == "my_column"


class TestComplexExpressions:
    def test_if_with_arithmetic(self):
        result = sel_to_sql("IF(currency = 'USD', amount, amount * 1.2)")
        assert "CASE WHEN" in result.upper()
        assert "amount * 1.2" in result

    def test_if_inside_arithmetic(self):
        """IF used as part of a larger expression."""
        result = sel_to_sql("IF(flag = 1, base_amount, 0) + fee")
        assert "CASE WHEN flag = 1 THEN base_amount ELSE 0 END + fee" in result

    def test_whitespace_normalisation(self):
        """Extra whitespace should not break the parser."""
        result = sel_to_sql("  IF( a = 1 ,  10 ,  20 )  ")
        assert "CASE WHEN" in result.upper()
        assert "10" in result
        assert "20" in result

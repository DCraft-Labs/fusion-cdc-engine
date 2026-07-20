"""
SEL — Simple Expression Language Parser (spec §2)

Translates a minimal, non-technical DSL into a PySpark Column expression.

Supported constructs
--------------------
  Arithmetic:     +, -, *, /
  Comparison:     =, !=, <>, <, <=, >, >=
  Boolean:        AND, OR, NOT
  Functions:      IF(cond, trueVal, falseVal)
                  IN(value, item1, item2, ...)   → value IN (item1, item2, ...)
                  NOT_IN(value, item1, ...)
                  IS_NULL(col)
                  IS_NOT_NULL(col)
                  SUBSTRING(col, start, length)
                  UPPER(col)
                  LOWER(col)
                  TRIM(col)
                  COALESCE(col1, col2, ...)
                  CONCAT(col1, col2, ...)
                  CAST(col AS type)
  Literals:       integers, floats, single- or double-quoted strings, NULL, TRUE, FALSE

Translation strategy
--------------------
SEL is a strict subset of Spark SQL with slightly different keyword casing.
The parser normalises keywords and then calls PySpark's ``F.expr()`` /
``selectExpr()`` on the resulting SQL string, which Spark evaluates natively.

Usage
-----
    from transform.sel_parser import sel_to_spark_expr

    col_expr = sel_to_spark_expr("IF(currency = 'USD', amount, amount * fx_rate)")
    df = df.withColumn("amount_usd", col_expr)
"""

from __future__ import annotations

import re

from pyspark.sql import functions as F
from pyspark.sql.column import Column


# ---------------------------------------------------------------------------
# Keyword / function normalisation map: SEL → Spark SQL
# ---------------------------------------------------------------------------

_KEYWORD_MAP = {
    # Functions
    "IF":           "CASE WHEN {args[0]} THEN {args[1]} ELSE {args[2]} END",  # special
    "NOT_IN":       "NOT IN",       # postfix, handled separately
    # Operators & literals (case normalisation only)
    "AND":          "AND",
    "OR":           "OR",
    "NOT":          "NOT",
    "NULL":         "NULL",
    "TRUE":         "TRUE",
    "FALSE":        "FALSE",
    "IS_NULL":      "{args[0]} IS NULL",       # special
    "IS_NOT_NULL":  "{args[0]} IS NOT NULL",   # special
    # These map directly to Spark SQL built-ins
    "IN":           "IN",
    "SUBSTRING":    "SUBSTRING",
    "UPPER":        "UPPER",
    "LOWER":        "LOWER",
    "TRIM":         "TRIM",
    "COALESCE":     "COALESCE",
    "CONCAT":       "CONCAT",
    "CAST":         "CAST",
}


def sel_to_sql(sel_expr: str) -> str:
    """
    Convert a SEL expression string into an equivalent Spark SQL string.

    The conversion is achieved through a series of regex substitutions that
    rewrite SEL-specific syntax into standard Spark SQL.  The resulting
    string is safe to pass to ``F.expr()``.

    Parameters
    ----------
    sel_expr : str
        A SEL expression, e.g. ``"IF(currency = 'USD', amount, amount * fx_rate)"``

    Returns
    -------
    str
        A Spark SQL string, e.g.
        ``"CASE WHEN currency = 'USD' THEN amount ELSE amount * fx_rate END"``
    """
    sql = sel_expr.strip()

    # 1. IF(cond, trueVal, falseVal) → CASE WHEN cond THEN trueVal ELSE falseVal END
    sql = _transform_if(sql)

    # 2. IS_NULL(col) → col IS NULL
    sql = re.sub(
        r"\bIS_NULL\s*\(\s*([^)]+?)\s*\)",
        lambda m: f"({m.group(1).strip()} IS NULL)",
        sql, flags=re.IGNORECASE,
    )

    # 3. IS_NOT_NULL(col) → col IS NOT NULL
    sql = re.sub(
        r"\bIS_NOT_NULL\s*\(\s*([^)]+?)\s*\)",
        lambda m: f"({m.group(1).strip()} IS NOT NULL)",
        sql, flags=re.IGNORECASE,
    )

    # 4. IN(value, a, b, c) → value IN (a, b, c)
    sql = re.sub(
        r"\bIN\s*\(\s*([^,]+?)\s*,\s*(.+?)\s*\)(?!\s*IN\s*\()",
        lambda m: f"({m.group(1).strip()} IN ({m.group(2).strip()}))",
        sql, flags=re.IGNORECASE,
    )

    # 5. NOT_IN(value, a, b, c) → value NOT IN (a, b, c)
    sql = re.sub(
        r"\bNOT_IN\s*\(\s*([^,]+?)\s*,\s*(.+?)\s*\)",
        lambda m: f"({m.group(1).strip()} NOT IN ({m.group(2).strip()}))",
        sql, flags=re.IGNORECASE,
    )

    # 6. Normalise SUBSTRING(col, start, length) → already valid Spark SQL; pass through
    # 7. Other built-in functions (UPPER, LOWER, TRIM, COALESCE, CONCAT, CAST) are already
    #    valid Spark SQL; no transformation needed.

    return sql


def _transform_if(sql: str) -> str:
    """
    Recursively transform IF(cond, trueVal, falseVal) into
    CASE WHEN cond THEN trueVal ELSE falseVal END.

    Handles nested IFs via repeated passes.
    """
    pattern = re.compile(r"\bIF\s*\(", re.IGNORECASE)
    max_passes = 20  # guard against infinite loops
    for _ in range(max_passes):
        match = pattern.search(sql)
        if not match:
            break
        start = match.end()  # position after the opening (
        args = _split_args(sql, start)
        if len(args) != 3:
            break  # malformed; leave as-is
        cond, true_val, false_val = [a.strip() for a in args]
        end_pos = _find_closing_paren(sql, start - 1) + 1
        replacement = f"CASE WHEN {cond} THEN {true_val} ELSE {false_val} END"
        sql = sql[: match.start()] + replacement + sql[end_pos:]
    return sql


def _find_closing_paren(s: str, open_pos: int) -> int:
    """Return index of the closing ) matching the ( at open_pos."""
    depth = 0
    for i in range(open_pos, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return len(s) - 1


def _split_args(s: str, start: int) -> list[str]:
    """
    Split arguments inside a parenthesised list starting at ``start``
    (the position AFTER the opening paren) into a list of strings.
    Respects nested parentheses and quoted strings.
    """
    depth = 0
    current: list[str] = []
    args: list[str] = []
    in_string: str | None = None  # None or the quote character
    end = _find_closing_paren(s, start - 1)

    for ch in s[start:end]:
        if in_string:
            current.append(ch)
            if ch == in_string:
                in_string = None
        elif ch in ('"', "'"):
            in_string = ch
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            args.append("".join(current))
            current = []
        else:
            current.append(ch)

    if current:
        args.append("".join(current))

    return args


def sel_to_spark_expr(sel_expr: str) -> Column:
    """
    Parse a SEL expression and return a PySpark Column.

    Parameters
    ----------
    sel_expr : str
        SEL expression string.

    Returns
    -------
    pyspark.sql.column.Column
        A PySpark Column that can be passed to ``df.withColumn()``.

    Examples
    --------
    >>> col = sel_to_spark_expr("IF(currency = 'USD', amount, amount * fx_rate)")
    >>> df = df.withColumn("amount_usd", col)
    """
    spark_sql = sel_to_sql(sel_expr)
    return F.expr(spark_sql)

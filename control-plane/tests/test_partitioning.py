"""v1.2.26 Task 1 tests: PK-range partitioning for multi-pod intra-table
parallelism. Covers the pure helpers in ``app.services.partitioning`` —
``naive_numeric_ranges`` and ``ranges_from_splits`` — which the producer
(``connections._enqueue_initial_load_tasks``) uses to split a table's PK
space into K disjoint sub-ranges before enqueuing K independent tasks.

The DB-touching ``partition_pk_ranges`` / ``_partition_mysql`` / ``_partition_pg``
paths are exercised by the integration suite; here we cover the math that
decides correctness (no gaps, no overlaps, full coverage, K ranges).
"""
import os
import sys

import pytest

# Make ``app`` importable when run from the control-plane dir.
HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services.partitioning import (  # noqa: E402
    DEFAULT_PARALLELISM,
    MAX_PARALLELISM,
    clamp_parallelism,
    naive_numeric_ranges,
    ranges_from_splits,
)


class TestClampParallelism:
    def test_none_falls_back_to_default(self):
        assert clamp_parallelism(None) == DEFAULT_PARALLELISM

    def test_non_numeric_falls_back_to_default(self):
        assert clamp_parallelism("abc") == DEFAULT_PARALLELISM

    def test_clamps_to_max(self):
        assert clamp_parallelism(1000) == MAX_PARALLELISM

    def test_clamps_to_min(self):
        assert clamp_parallelism(0) == 1
        assert clamp_parallelism(-3) == 1

    def test_passes_through_valid(self):
        assert clamp_parallelism(4) == 4
        assert clamp_parallelism("8") == 8
        assert clamp_parallelism(MAX_PARALLELISM) == MAX_PARALLELISM


class TestNaiveNumericRanges:
    def test_k_one_returns_single_unbounded(self):
        assert naive_numeric_ranges(0, 1000, 1) == [(None, None)]

    def test_none_bounds_return_single_unbounded(self):
        assert naive_numeric_ranges(None, 100, 4) == [(None, None)]
        assert naive_numeric_ranges(0, None, 4) == [(None, None)]

    def test_k_ranges_returned(self):
        ranges = naive_numeric_ranges(0, 1000, 4)
        assert len(ranges) == 4

    def test_first_start_open_last_end_open(self):
        ranges = naive_numeric_ranges(0, 1000, 4)
        assert ranges[0][0] is None
        assert ranges[-1][1] is None

    def test_interior_bounds_form_disjoint_cover(self):
        """The K ranges must be disjoint and cover [mn, mx] with no gaps or
        overlaps — this is the correctness invariant for multi-pod intra-table
        parallelism (no row is fetched by two pods, no row is skipped)."""
        mn, mx, k = 0, 1000, 4
        ranges = naive_numeric_ranges(mn, mx, k)
        # Interior end of range i == interior start of range i+1 (the worker's
        # ``pk > last_pk`` advances past the shared boundary, so adjacent
        # ranges are disjoint).
        for i in range(len(ranges) - 1):
            assert ranges[i][1] == ranges[i + 1][0], (
                f"gap/overlap between range {i} and {i+1}: {ranges}"
            )

    def test_monotonic_bounds(self):
        ranges = naive_numeric_ranges(0, 1000, 5)
        bounds = [r[1] for r in ranges[:-1]]  # interior ends
        assert bounds == sorted(b), f"interior bounds not monotonic: {bounds}"

    def test_equal_min_max_returns_single(self):
        # degenerate: single-row table
        assert naive_numeric_ranges(5, 5, 4) == [(None, None)]


class TestRangesFromSplits:
    def test_no_splits_returns_single_unbounded(self):
        assert ranges_from_splits(0, 1000, []) == [(None, None)]

    def test_none_bounds_return_single_unbounded(self):
        assert ranges_from_splits(None, 1000, [500]) == [(None, None)]
        assert ranges_from_splits(0, None, [500]) == [(None, None)]

    def test_k_minus_one_splits_yield_k_ranges(self):
        splits = [250, 500, 750]
        ranges = ranges_from_splits(0, 1000, splits)
        assert len(ranges) == 4  # K = len(splits) + 1

    def test_first_open_last_open(self):
        ranges = ranges_from_splits(0, 1000, [250, 500, 750])
        assert ranges[0][0] is None
        assert ranges[-1][1] is None

    def test_disjoint_cover(self):
        """Adjacent ranges share a boundary PK; the worker's ``pk > last_pk``
        advances past it, so the union covers the full space with no gaps or
        overlaps."""
        splits = [250, 500, 750]
        ranges = ranges_from_splits(0, 1000, splits)
        for i in range(len(ranges) - 1):
            assert ranges[i][1] == ranges[i + 1][0], (
                f"gap/overlap between range {i} and {i+1}: {ranges}"
            )

    def test_splits_used_as_boundaries(self):
        splits = [250, 500, 750]
        ranges = ranges_from_splits(0, 1000, splits)
        # range 0 ends at splits[0], range 1 starts at splits[0] ends at splits[1], ...
        assert ranges[0] == (None, 250)
        assert ranges[1] == (250, 500)
        assert ranges[2] == (500, 750)
        assert ranges[3] == (750, None)

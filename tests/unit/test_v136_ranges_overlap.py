"""v1.3.6 Bugs #5 / #6 — half-open-correct _ranges_overlap matrix."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

COMMITTER = (
    Path(__file__).resolve().parents[2]
    / "transform-worker" / "iceberg_committer.py"
)


@pytest.fixture(scope="module")
def ranges_overlap():
    tw = str(COMMITTER.parent)
    if tw not in sys.path:
        sys.path.insert(0, tw)
    spec = importlib.util.spec_from_file_location("iceberg_committer_ut", COMMITTER)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod._ranges_overlap


def test_disjoint_partitions(ranges_overlap):
    assert ranges_overlap(1, 10, 20, 30) is False
    assert ranges_overlap(20, 30, 1, 10) is False


def test_adjacent_chunks_touching_boundary_not_overlapping(ranges_overlap):
    # [1,10] vs [10,20] — half-open cursor semantics → NOT overlapping
    assert ranges_overlap(1, 10, 10, 20) is False
    assert ranges_overlap(10, 20, 1, 10) is False
    # Live failure signature: consecutive chunks
    assert ranges_overlap(2279837, 2299838, 2299838, 2319838) is False


def test_real_overlap(ranges_overlap):
    assert ranges_overlap(1, 15, 10, 20) is True
    assert ranges_overlap(10, 20, 1, 15) is True
    # Identical non-empty ranges overlap
    assert ranges_overlap(4, 5, 4, 5) is True
    # Half-open empty interval (min==max) does not overlap itself
    assert ranges_overlap(5, 5, 5, 5) is False


def test_unbounded_sides(ranges_overlap):
    assert ranges_overlap(None, 10, 20, 30) is False
    assert ranges_overlap(None, 25, 20, 30) is True
    assert ranges_overlap(1, None, 20, 30) is True
    assert ranges_overlap(1, 10, None, None) is True

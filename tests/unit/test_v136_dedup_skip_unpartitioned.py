"""v1.3.6 Bug #7 — skip expensive dedup only when table is unpartitioned."""
from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

COMMITTER = (
    Path(__file__).resolve().parents[2]
    / "transform-worker" / "iceberg_committer.py"
)


@pytest.fixture(scope="module")
def committer_mod():
    tw = str(COMMITTER.parent)
    if tw not in sys.path:
        sys.path.insert(0, tw)
    spec = importlib.util.spec_from_file_location("iceberg_committer_dedup_ut", COMMITTER)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _make_committer(mod):
    inst = object.__new__(mod.IcebergCommitter)
    inst.namespace = "ns"
    inst.table_name = "t"
    inst.connection_id = "c"
    inst.catalog = MagicMock()
    inst.redis = None
    return inst


def test_skip_dedup_when_unpartitioned(committer_mod, caplog):
    inst = _make_committer(committer_mod)
    table = MagicMock()
    spec = MagicMock()
    spec.fields = ()
    table.spec.return_value = spec
    entry = {"file_path": "s3://bucket/data/x.parquet"}
    with caplog.at_level(logging.WARNING):
        committer_mod.IcebergCommitter._dedup_one_range(
            inst, table, "pkey", 1, 100, entry,
        )
    assert "skipping expensive delete-dedup" in caplog.text
    table.delete.assert_not_called()


def test_keep_dedup_path_when_partitioned(committer_mod):
    inst = _make_committer(committer_mod)
    table = MagicMock()
    spec = MagicMock()
    spec.fields = (MagicMock(),)  # non-empty → partitioned
    table.spec.return_value = spec
    entry = {"file_path": "s3://bucket/data/x.parquet"}
    # Force path 2 (range delete) by returning no keys from staged file.
    inst._extract_pk_values_from_staged_file = MagicMock(return_value=[])
    committer_mod.IcebergCommitter._dedup_one_range(
        inst, table, "pkey", 1, 100, entry,
    )
    table.delete.assert_called_once()

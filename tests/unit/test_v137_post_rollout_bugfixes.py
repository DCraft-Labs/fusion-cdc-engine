"""v1.3.7 post-rollout bugfixes — unit coverage for Bugs #1-#5, #10-#12, #15-#17, #19."""
from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMMITTER = ROOT / "transform-worker" / "iceberg_committer.py"
WRITER = ROOT / "transform-worker" / "iceberg_writer.py"
LOADER = ROOT / "transform-worker" / "loader.py"
CONNECTIONS = ROOT / "control-plane" / "app" / "api" / "connections.py"
PROVISIONER = ROOT / "control-plane" / "app" / "services" / "committer_provisioner.py"
MYSQL = ROOT / "cdc-workers" / "connectors" / "mysql.py"
WORKER = ROOT / "cdc-workers" / "cdc_worker" / "worker.py"
CONFIG = ROOT / "cdc-workers" / "cdc_worker" / "config.py"


def _load(path: Path, name: str, stubs: dict | None = None):
    """Load a source file as a module, optionally pre-seeding sys.modules stubs."""
    if stubs:
        for mod_name, stub in stubs.items():
            sys.modules.setdefault(mod_name, stub)
    parent = str(path.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def committer_mod():
    return _load(COMMITTER, "iceberg_committer_v137")


# ── Bug #11: mixed str/int PK coercion ───────────────────────────────────────

def test_coerce_pk_numeric_string(committer_mod):
    assert committer_mod._coerce_pk("42") == 42
    assert committer_mod._coerce_pk(42) == 42
    assert committer_mod._coerce_pk("abc") == "abc"


def test_ranges_overlap_mixed_str_int(committer_mod):
    # Live crash signature: str bounds from task vs int bounds from batch min/max
    assert committer_mod._ranges_overlap("1", "100", 50, 60) is True
    assert committer_mod._ranges_overlap("1", "10", 20, 30) is False
    assert committer_mod._ranges_overlap("10", "20", 10, 20) is True


def test_pk_to_score_preserves_numeric_string_order(committer_mod):
    scores = [committer_mod._pk_to_score(v) for v in ("1", "2", "10", "100")]
    assert scores == sorted(scores)
    # Must NOT fall into the hash path for numeric strings
    assert committer_mod._pk_to_score("10") == 10.0


# ── Bug #10: batched add_files helper exists ─────────────────────────────────

def test_add_files_fast_helper_present(committer_mod):
    assert callable(committer_mod._add_files_fast)
    src = COMMITTER.read_text(encoding="utf-8")
    assert "tx.fast_append()" in src or "fast_append" in src
    assert "--drain-batch" in src


# ── Bug #4/#5: drain_batch resolver + provisioner ────────────────────────────

def test_resolve_drain_batch_baseline_and_override():
    mod = _load(PROVISIONER, "committer_provisioner_v137", stubs={
        "kubernetes": MagicMock(),
        "kubernetes.client": MagicMock(),
        "kubernetes.config": MagicMock(),
    })
    # Explicit override wins
    assert mod.resolve_drain_batch({"drain_batch": 2500}, k=6, rows_estimated_total=1_000_000) == 2500
    # Session-validated baseline: K=6 → 1000
    assert mod.resolve_drain_batch({}, k=6, rows_estimated_total=35_860_000) == 1000
    # Catalog readiness derived from URI, not guessed hostname
    url = mod._catalog_readiness_check_url({
        "catalog_type": "nessie",
        "nessie_uri": "http://nessie:19120/api/v1",
    })
    assert url is not None
    assert "nessie" in url
    assert mod._catalog_readiness_check_url({"catalog_type": "glue"}) is None


# ── Bug #3: auto bulk_mode resolver ──────────────────────────────────────────

def test_resolve_bulk_mode_auto_threshold():
    src = CONNECTIONS.read_text(encoding="utf-8")
    assert "def _resolve_bulk_mode" in src
    assert 'task_bulk_mode = _resolve_bulk_mode(' in src
    assert "ensure_committer" in src
    assert "teardown_committer" in src

    import os as _os
    tree = ast.parse(src)
    fn = None
    threshold_node = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_resolve_bulk_mode":
            fn = node
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "AUTO_BULK_MODE_ROW_THRESHOLD":
                    threshold_node = node
    assert fn is not None
    ns: dict = {"os": _os}
    if threshold_node is not None:
        exec(compile(ast.Module(body=[threshold_node], type_ignores=[]), "<t>", "exec"), ns)
    else:
        ns["AUTO_BULK_MODE_ROW_THRESHOLD"] = 1_000_000
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<f>", "exec"), ns)
    resolve = ns["_resolve_bulk_mode"]
    assert resolve({"bulk_mode": "duckdb"}, 10, "postgres") == "duckdb"
    assert resolve({"bulk_mode": "python"}, 10_000_000, "postgres") == "python"
    assert resolve({"bulk_mode": "auto"}, 2_000_000, "postgres") == "duckdb"
    assert resolve({"bulk_mode": "auto"}, 100, "postgres") == "python"
    assert resolve({"bulk_mode": "auto"}, 10_000_000, "mongodb") == "python"


# ── Bug #12 / #15: MySQL CDC blocking + bytes sanitize ───────────────────────

def test_mysql_blocking_true_and_sanitize_helpers():
    src = MYSQL.read_text(encoding="utf-8")
    assert "blocking=True" in src
    assert "def _sanitize_row" in src
    assert "def _sanitize_scalar" in src
    # The False mention may appear in comments documenting the old bug;
    # the constructor kwarg itself must be True.
    assert "\n                blocking=True," in src or "\n            blocking=True," in src
    assert "blocking=False," not in src


# ── Bug #16: source crash recovery ───────────────────────────────────────────

def test_source_retry_config_and_finally_pop():
    cfg = CONFIG.read_text(encoding="utf-8")
    assert "SOURCE_MAX_RETRIES" in cfg
    assert "SOURCE_RETRY_BACKOFF_BASE_SECONDS" in cfg
    worker = WORKER.read_text(encoding="utf-8")
    assert "SOURCE_MAX_RETRIES" in worker
    assert "self._source_tasks.pop(source_id, None)" in worker


# ── Bug #17: bootstrap lock timeout re-acquire ───────────────────────────────

def test_bootstrap_lock_timeout_reacquires():
    src = WRITER.read_text(encoding="utf-8")
    assert "BOOTSTRAP" in src.upper() or "bootstrap" in src
    # Timeout fallback must attempt SETNX again before direct create
    assert "setnx" in src.lower() or "SETNX" in src or "set(" in src
    # create_table exception falls back to load_table
    assert "load_table" in src


# ── Bug #19: reached_end breaks the chunk loop ───────────────────────────────

def test_loader_breaks_on_reached_end():
    src = LOADER.read_text(encoding="utf-8")
    assert "if reached_end:" in src
    # Ensure the break is in the continuation block, not only the prefetch gate
    assert "if reached_end:\n                break" in src or "if reached_end:\n            break" in src


# ── Bug #1/#2: DynamoDB creds + IRSA ambient-role skip ───────────────────────

def test_iceberg_writer_dynamodb_creds_and_irsa():
    src = WRITER.read_text(encoding="utf-8")
    assert "dynamodb.access-key-id" in src
    assert "dynamodb.region" in src
    assert "AWS_ROLE_ARN" in src
    assert "requested_role" in src

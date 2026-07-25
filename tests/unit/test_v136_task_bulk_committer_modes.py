"""v1.3.6 Bug #3 / v1.3.7 Bug #3 — control-plane forwards bulk_mode / committer_mode on tasks.

v1.3.7 replaced the passthrough ``_rl.get("bulk_mode")`` with
``_resolve_bulk_mode(...)`` (auto / duckdb / python / MongoDB force-python).
"""
from __future__ import annotations

import ast
from pathlib import Path


CONNECTIONS = (
    Path(__file__).resolve().parents[2]
    / "control-plane" / "app" / "api" / "connections.py"
)


def test_enqueue_reads_resource_limits_modes():
    src = CONNECTIONS.read_text(encoding="utf-8")
    assert "def _resolve_bulk_mode" in src
    assert 'task_bulk_mode = _resolve_bulk_mode(' in src
    assert 'task_committer_mode = _rl.get("committer_mode")' in src
    assert '"bulk_mode": task_bulk_mode' in src
    assert '"committer_mode": task_committer_mode' in src


def test_enqueue_task_literal_includes_mode_keys():
    """AST check: the task dict literal under _enqueue_initial_load_tasks
    includes bulk_mode and committer_mode keys."""
    tree = ast.parse(CONNECTIONS.read_text(encoding="utf-8"))
    found = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys = []
        for k in node.keys:
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.append(k.value)
        if "type" in keys and "chunk_size" in keys and "bulk_mode" in keys:
            assert "committer_mode" in keys
            found = True
            break
    assert found, "task dict with bulk_mode/committer_mode not found"

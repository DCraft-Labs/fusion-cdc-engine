"""v1.2.20: unit tests for the CDC routing decision.

The routing decision (``_dest_needs_transform_worker``) is the single
source of truth that determines which consumer owns a connection's
snapshot + CDC streaming. It MUST agree across three call sites:

  * ``cdc_consumer._dest_needs_transform_worker`` (consumer-side skip)
  * ``connections._dest_needs_transform_worker`` (initial-load producer)
  * ``internal._dest_needs_transform_worker`` (transform-route resolver)

These tests pin the rule so a refactor in one file cannot silently
desync the others and re-introduce the double-write bug.
"""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Import the three copies of _dest_needs_transform_worker without importing
# the whole FastAPI / psycopg2 stack (the modules have heavy import-time
# side effects). We load each module file directly via importlib from its
# absolute path so the test does not depend on a particular package layout.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_function(module_path: Path, func_name: str):
    """Load a single function from a .py file without importing the package.

    We exec the module source in a throwaway namespace and return the
    function. The modules we target only define the helper at module top
    level (no heavy I/O at import time for the helper itself), so this is
    safe and fast.
    """
    import types
    mod = types.ModuleType(module_path.stem)
    mod.__file__ = str(module_path)
    src = module_path.read_text(encoding="utf-8")
    # Strip the module to just the helper function definition to avoid
    # executing heavy imports. We find the `def <func_name>` line and
    # the next top-level `def `/class after it, then exec only that slice.
    lines = src.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith(f"def {func_name}("):
            start = i
            break
    if start is None:
        raise AssertionError(f"{func_name} not found in {module_path}")
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("def ") or lines[j].startswith("class "):
            end = j
            break
    snippet = "\n".join(lines[start:end])
    ns: dict = {}
    exec(compile(snippet, str(module_path), "exec"), ns)
    return ns[func_name]


CONSUMER_HELPER = _load_function(
    REPO_ROOT / "cdc-workers" / "cdc_consumer.py",
    "_dest_needs_transform_worker",
)
CONNECTIONS_HELPER = _load_function(
    REPO_ROOT / "control-plane" / "app" / "api" / "connections.py",
    "_dest_needs_transform_worker",
)
INTERNAL_HELPER = _load_function(
    REPO_ROOT / "control-plane" / "app" / "api" / "internal.py",
    "_dest_needs_transform_worker",
)


ALL_THREE = [
    pytest.param(CONSUMER_HELPER, id="cdc_consumer"),
    pytest.param(CONNECTIONS_HELPER, id="connections"),
    pytest.param(INTERNAL_HELPER, id="internal"),
]


# ---------------------------------------------------------------------------
# Routing rule tests — the 6 source × destination combinations.
# Only the destination type + snapshot_mode matters for routing (the source
# type does not affect which consumer owns the stream), so we test the
# destination matrix exhaustively.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("helper", ALL_THREE)
class TestRoutingRule:
    """Pin the routing rule for every destination type + snapshot_mode."""

    def test_iceberg_always_transform_worker(self, helper):
        # Iceberg destinations ALWAYS go to the transform-worker regardless
        # of snapshot_mode (cdc_consumer.py cannot write to Iceberg).
        assert helper("iceberg", "inline") is True
        assert helper("iceberg", "transform_worker") is True
        assert helper("iceberg", "") is True
        assert helper("ICEBERG", "Inline") is True

    def test_mysql_destination_always_transform_worker(self, helper):
        # MySQL destinations always go to the transform-worker (cdc_consumer.py
        # can only write to Postgres).
        assert helper("mysql", "inline") is True
        assert helper("mysql", "transform_worker") is True

    def test_mongodb_destination_always_transform_worker(self, helper):
        assert helper("mongodb", "inline") is True
        assert helper("mongo", "transform_worker") is True

    def test_postgres_inline_owned_by_cdc_consumer(self, helper):
        # Postgres destination with default inline mode → cdc_consumer.py owns it.
        # This is the ONLY case where the transform-worker is NOT used.
        assert helper("postgres", "inline") is False
        assert helper("postgresql", "inline") is False
        assert helper("postgres", "") is False  # default = inline
        assert helper("postgres", None) is False

    def test_postgres_transform_worker_owned_by_transform_worker(self, helper):
        # Postgres destination with snapshot_mode=transform_worker → transform-worker.
        assert helper("postgres", "transform_worker") is True
        assert helper("postgresql", "TRANSFORM_WORKER") is True

    def test_unknown_destination_defaults_to_transform_worker(self, helper):
        # Unknown / unsupported destination types default to the
        # transform-worker (safer — cdc_consumer.py cannot handle them).
        assert helper("kafka", "inline") is True
        assert helper("", "inline") is True
        assert helper(None, "inline") is True


# ---------------------------------------------------------------------------
# Cross-module consistency — all three copies MUST agree on every input.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ctype, snap",
    [
        ("iceberg", "inline"),
        ("iceberg", "transform_worker"),
        ("mysql", "inline"),
        ("mongodb", "transform_worker"),
        ("postgres", "inline"),
        ("postgresql", "inline"),
        ("postgres", "transform_worker"),
        ("", "inline"),
        (None, None),
    ],
)
def test_all_three_copies_agree(ctype, snap):
    """The consumer, producer, and resolver must all make the same decision."""
    results = {
        "consumer": CONSUMER_HELPER(ctype, snap),
        "connections": CONNECTIONS_HELPER(ctype, snap),
        "internal": INTERNAL_HELPER(ctype, snap),
    }
    assert len(set(results.values())) == 1, (
        f"Routing decision disagrees across modules for ({ctype!r}, {snap!r}): {results}"
    )


# ---------------------------------------------------------------------------
# Bulletproof matrix — for every source × destination combo, assert the
# routing picks a consumer that can actually handle the destination type.
# This is the contract test: no combination returns "inline" for a
# destination cdc_consumer.py cannot write to.
# ---------------------------------------------------------------------------

CDC_CONSUMER_CAN_WRITE = {"postgres", "postgresql"}


@pytest.mark.parametrize(
    "src_type, dest_type, snap",
    [
        # The 6 combinations from the audit matrix.
        ("mysql", "postgres", "inline"),
        ("mysql", "postgres", "transform_worker"),
        ("mysql", "iceberg", "inline"),
        ("mysql", "iceberg", "transform_worker"),
        ("mongodb", "postgres", "inline"),
        ("mongodb", "iceberg", "inline"),
        ("postgresql", "postgres", "inline"),
        ("postgresql", "postgres", "transform_worker"),
        ("postgresql", "iceberg", "inline"),
        ("postgresql", "iceberg", "transform_worker"),
    ],
)
def test_routing_picks_a_capable_consumer(src_type, dest_type, snap):
    """For every source × destination combo, the routing decision must
    hand the connection to a consumer that can actually write to the
    destination type.

    cdc_consumer.py can only write to Postgres. So if the routing says
    "inline" (cdc_consumer.py owns it), the destination MUST be Postgres.
    """
    uses_transform_worker = CONSUMER_HELPER(dest_type, snap)
    if not uses_transform_worker:
        # cdc_consumer.py owns it — destination must be Postgres.
        assert (dest_type or "").lower() in CDC_CONSUMER_CAN_WRITE, (
            f"Routing handed {dest_type!r} (snap={snap!r}) to cdc_consumer.py, "
            f"which can only write to Postgres. This is the double-write / "
            f"silent-no-op bug."
        )

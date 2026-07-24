"""v1.3.6 Bug #4 — migration exists, revises current head, idempotent upgrade."""
from __future__ import annotations

from pathlib import Path

import pytest

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations" / "versions"
TARGET = MIGRATIONS / "g1a2b3c4d5e6_fix_ilc_unique_include_chunk_seq.py"


def test_migration_file_exists_with_expected_revision():
    assert TARGET.exists()
    src = TARGET.read_text(encoding="utf-8")
    assert 'revision = "g1a2b3c4d5e6"' in src or "revision = 'g1a2b3c4d5e6'" in src
    assert "d6e7f8a9b0c1" in src
    assert "uq_ilc_connection_stream_chunk" in src
    assert "chunk_seq" in src
    # Must NOT reuse the already-taken revision id
    assert "c5d6e7f8a9b0" not in src or "Revises" in src  # only as historical note ok
    assert 'revision = "c5d6e7f8a9b0"' not in src
    assert "revision = 'c5d6e7f8a9b0'" not in src


def test_migration_upgrade_is_idempotent_when_constraint_present():
    """Call upgrade() against a fake bind that already has the new constraint."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("mig_g1", TARGET)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None

    class FakeInsp:
        def get_unique_constraints(self, table_name):
            return [{"name": "uq_ilc_connection_stream_chunk"}]

    class FakeBind:
        pass

    import alembic.op as alembic_op
    from unittest.mock import patch

    calls = []

    def _drop(*a, **k):
        calls.append(("drop", a, k))

    def _create(*a, **k):
        calls.append(("create", a, k))

    with patch.object(alembic_op, "get_bind", return_value=FakeBind()):
        with patch("sqlalchemy.inspect", return_value=FakeInsp()):
            with patch.object(alembic_op, "drop_constraint", side_effect=_drop):
                with patch.object(alembic_op, "create_unique_constraint", side_effect=_create):
                    spec.loader.exec_module(mod)
                    mod.upgrade()
    assert calls == [], f"expected no-op when new constraint exists, got {calls}"

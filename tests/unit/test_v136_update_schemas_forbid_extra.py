"""v1.3.6 Bug #11 — StreamUpdate / ConnectionUpdate forbid unknown fields."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

SCHEMAS = (
    Path(__file__).resolve().parents[2]
    / "control-plane" / "app" / "schemas" / "connection.py"
)


@pytest.fixture(scope="module")
def schemas():
    # Import schemas module without pulling full control-plane app package.
    import importlib.util
    cp = str(SCHEMAS.parents[2])  # control-plane/
    if cp not in sys.path:
        sys.path.insert(0, cp)
    # Ensure app package resolves
    app_dir = str(SCHEMAS.parents[1])  # control-plane/app parent is control-plane
    spec = importlib.util.spec_from_file_location(
        "connection_schemas_ut", SCHEMAS,
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_stream_update_rejects_source_table_name(schemas):
    with pytest.raises(ValidationError) as exc:
        schemas.StreamUpdate(source_table_name="other_table")
    assert "extra" in str(exc.value).lower() or "forbidden" in str(exc.value).lower() or "source_table_name" in str(exc.value)


def test_connection_update_rejects_sync_type(schemas):
    with pytest.raises(ValidationError) as exc:
        schemas.ConnectionUpdate(sync_type="REALTIME")
    assert "sync_type" in str(exc.value)


def test_stream_update_accepts_known_fields(schemas):
    m = schemas.StreamUpdate(stream_name="ok", is_enabled=False)
    assert m.stream_name == "ok"
    assert m.is_enabled is False


def test_connection_update_accepts_resource_limits(schemas):
    m = schemas.ConnectionUpdate(resource_limits={"bulk_mode": "none"})
    assert m.resource_limits["bulk_mode"] == "none"

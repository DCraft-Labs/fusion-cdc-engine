"""v1.2.25 Task 5 — manifest compaction + Task 7 delete-after-commit defaults.

Verifies:
- ``IcebergWriter.compact_manifests`` does NOT crash when PyIceberg 0.7.1
  lacks ``rewrite_manifests`` / ``expire_snapshots`` (it falls back to the
  ``commit.manifest.min-count-to-merge`` table property and logs).
- ``_build_table_properties`` defaults ``commit.manifest.min-count-to-merge=1``
  and ``write.metadata.delete-after-commit.enabled=true`` for initial-load
  destinations (Task 5 + Task 7), so manifests auto-merge on every commit
  and old metadata files are removed after each commit.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_compact_manifests_does_not_crash_without_rewrite_manifests():
    """PyIceberg 0.7.1 has no table.rewrite_manifests() / expire_snapshots().
    compact_manifests must degrade gracefully (log + return), not raise.
    """
    from iceberg_writer import IcebergWriter

    fake_table = MagicMock(spec=[])  # no rewrite_manifests / expire_snapshots attrs
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    with patch.object(IcebergWriter, "_ensure_namespace", return_value=None):
        writer = IcebergWriter.__new__(IcebergWriter)
        writer.catalog = fake_catalog
        writer.namespace = "default"
        writer.warehouse = ""
        writer.dest_config = {}

    result = writer.compact_manifests("users", keep_snapshots=5)
    assert result["table"] == "users"
    # Neither API is available in 0.7.1 → both must be False, no crash.
    assert result["rewrote_manifests"] is False
    assert result["expired_snapshots"] is False
    assert "0.7.1" in result["note"]


def test_compact_manifests_uses_new_api_when_available():
    """Forward-compat: if a future PyIceberg exposes rewrite_manifests() and
    expire_snapshots(), compact_manifests calls them and reports True.
    """
    from iceberg_writer import IcebergWriter

    fake_table = MagicMock()
    fake_table.rewrite_manifests = MagicMock()
    fake_table.expire_snapshots = MagicMock()
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    writer = IcebergWriter.__new__(IcebergWriter)
    writer.catalog = fake_catalog
    writer.namespace = "default"
    writer.warehouse = ""
    writer.dest_config = {}

    result = writer.compact_manifests("users")
    assert result["rewrote_manifests"] is True
    assert result["expired_snapshots"] is True
    fake_table.rewrite_manifests.assert_called_once()
    fake_table.expire_snapshots.assert_called_once()


def test_build_table_properties_defaults_for_initial_load():
    """Task 5 + Task 7: initial-load destinations get manifest auto-merge +
    delete-after-commit by default so a long load does not degrade.
    """
    from iceberg_writer import _build_table_properties

    props = _build_table_properties({"initial_load_destination": True})
    assert props["commit.manifest.min-count-to-merge"] == "1", (
        f"Task 5: expected commit.manifest.min-count-to-merge=1, got {props}"
    )
    assert props["write.metadata.delete-after-commit.enabled"] == "true", (
        f"Task 7: expected delete-after-commit=true, got {props}"
    )


def test_build_table_properties_operator_can_opt_out():
    """Task 7: an operator can disable delete-after-commit by setting
    write_metadata_delete_after_commit=false explicitly.
    """
    from iceberg_writer import _build_table_properties

    props = _build_table_properties({
        "initial_load_destination": True,
        "write_metadata_delete_after_commit": False,
    })
    # The explicit False wins (setdefault does not overwrite the existing
    # write.metadata.delete-after-commit.enabled key because the legacy
    # branch only adds it when write_metadata_delete_after_commit is truthy).
    assert "write.metadata.delete-after-commit.enabled" not in props or \
        props.get("write.metadata.delete-after-commit.enabled") != "true"

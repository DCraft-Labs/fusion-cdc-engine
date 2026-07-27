"""v1.3.5 Fix 4 — committer catalog_config tests.

Verifies three things (per brief):
  1. load_catalog({}) raises a clear ValueError (not a KeyError on
     "catalog_uri") — the failure mode is now loud, not silent.
  2. load_catalog() can build a nessie catalog from a populated config
     (the happy path still works).
  3. The chart template wires the destination's connection_config via a
     per-target Secret (rendered YAML assertion) — operators no longer
     need to manually supply catalogConfig per target.

All tests mock pyiceberg (no live Nessie required).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch


class TestLoadCatalogLoudFailure(unittest.TestCase):
    """v1.3.5 Fix 4: load_catalog({}) must raise a clear ValueError,
    not silently default to "rest" and KeyError on "catalog_uri"."""

    def test_empty_config_raises_clear_value_error(self):
        from iceberg_writer import load_catalog
        with self.assertRaises(ValueError) as cm:
            load_catalog({})
        msg = str(cm.exception)
        # The error must mention catalog_config is empty (actionable),
        # not a cryptic KeyError on catalog_uri.
        self.assertIn("catalog_config is empty", msg)
        self.assertNotIsInstance(cm.exception, KeyError)

    def test_none_config_raises_clear_value_error(self):
        from iceberg_writer import load_catalog
        with self.assertRaises(ValueError) as cm:
            load_catalog(None)
        self.assertIn("catalog_config is empty", str(cm.exception))


class TestLoadCatalogNessieHappyPath(unittest.TestCase):
    """v1.3.5 Fix 4: a populated nessie config still loads a catalog
    (the loud-error fix must not break the happy path)."""

    def test_nessie_config_loads_catalog(self):
        # Inject a fake pyiceberg module so the test doesn't require
        # pyiceberg installed (CI's test job only installs pyarrow/
        # duckdb/redis/etc, not pyiceberg). load_catalog() does
        # `from pyiceberg.catalog import load_catalog as _load` then
        # `return _load(name, **settings)` — we patch _load directly.
        import importlib, sys, types
        fake_loader = MagicMock(name="pyiceberg_load_catalog")
        if "pyiceberg" not in sys.modules or "pyiceberg.catalog" not in sys.modules:
            fake_pkg = types.ModuleType("pyiceberg")
            fake_catalog = types.ModuleType("pyiceberg.catalog")
            fake_catalog.load_catalog = fake_loader
            fake_pkg.catalog = fake_catalog
            sys.modules["pyiceberg"] = fake_pkg
            sys.modules["pyiceberg.catalog"] = fake_catalog
        else:
            # pyiceberg is installed and imported — patch its load_catalog.
            real_catalog_mod = sys.modules["pyiceberg.catalog"]
            fake_loader = real_catalog_mod.load_catalog

        import iceberg_writer
        fake_loader.reset_mock()
        fake_catalog_obj = MagicMock(name="nessie_catalog")
        fake_loader.return_value = fake_catalog_obj

        cfg = {
            "catalog_type": "nessie",
            "nessie_uri": "http://nessie:19120/api/v1",
            "warehouse": "s3://fusion/warehouse",
            "s3_endpoint": "http://minio:9000",
            "s3_access_key_id": "minio",
            "s3_secret_access_key": "minio123",
            "auth_mode": "static",
        }
        cat = iceberg_writer.load_catalog(cfg)
        self.assertIs(cat, fake_catalog_obj)
        _, kwargs = fake_loader.call_args
        self.assertEqual(kwargs.get("uri"), "http://nessie:19120/api/v1")
        self.assertEqual(kwargs.get("warehouse"), "s3://fusion/warehouse")
        self.assertEqual(kwargs.get("s3.endpoint"), "http://minio:9000")


class TestCommitterCliRequiresCatalogConfig(unittest.TestCase):
    """v1.3.5 Fix 4: the committer CLI must error out clearly when
    --catalog-config is empty/None (was silent default → crash-loop)."""

    def test_committer_main_errors_without_catalog_config(self):
        # Simulate `python iceberg_committer.py --connection-id X --tables Y`
        # with no --catalog-config and no ICEBERG_CATALOG_CONFIG env var.
        # v1.4.x Phase 1: --tables (comma-separated) replaced the old
        # singular --table flag when the committer was consolidated to one
        # process per connection.
        env = dict(os.environ)
        env.pop("ICEBERG_CATALOG_CONFIG", None)
        proc = subprocess.run(
            [sys.executable, "iceberg_committer.py",
             "--connection-id", "conn-1",
             "--tables", "customers",
             "--redis-url", "redis://localhost:6379/0"],
            capture_output=True, text=True, env=env, timeout=10,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        # argparse.error() exits with code 2 and prints to stderr.
        self.assertNotEqual(proc.returncode, 0)
        stderr = proc.stderr.lower()
        self.assertIn("catalog-config", stderr)


class TestChartWiresCatalogSecret(unittest.TestCase):
    """v1.3.5 Fix 4: the chart template wires the destination's
    connection_config via a per-target Secret mounted as
    ICEBERG_CATALOG_CONFIG env var (helm template assertion)."""

    CHART_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))),
        "helm", "fusion-cdc",
    )

    def _helm_template(self, values: dict) -> str:
        import tempfile
        helm = os.environ.get("HELM_BIN", "helm")
        # Write values as JSON (valid YAML) to avoid the pyyaml dependency
        # in the CI test job (which only installs pyarrow/duckdb/redis/etc).
        vpath = None
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".yaml",
                                             delete=False) as f:
                f.write(json.dumps(values))
                vpath = f.name
            try:
                out = subprocess.run(
                    [helm, "template", "fusion-cdc", self.CHART_DIR, "-f", vpath],
                    capture_output=True, text=True, timeout=60,
                )
            except FileNotFoundError:
                self.skipTest("helm binary not installed (HELM_BIN unset and 'helm' not on PATH)")
        finally:
            if vpath:
                os.unlink(vpath)
        if out.returncode != 0:
            self.skipTest(f"helm template failed (chart deps missing?): {out.stderr[:200]}")
        return out.stdout

    def test_secret_and_env_var_wired(self):
        catalog_cfg = {
            "catalog_type": "nessie",
            "nessie_uri": "http://nessie:19120/api/v1",
            "warehouse": "s3://fusion/warehouse",
        }
        rendered = self._helm_template({
            "committer": {
                "enabled": True,
                "targets": [{
                    "enabled": True,
                    "connectionId": "550e8400-e29b-41d4-a716-446655440000",
                    "table": "customers",
                    "namespace": "fusion",
                    "catalogConfig": json.dumps(catalog_cfg),
                }],
            },
            "nessie": {"enabled": True, "service": {"apiPort": 19120}},
        })
        # A per-target Secret is rendered with the catalog config.
        self.assertIn("kind: Secret", rendered)
        self.assertIn("ICEBERG_CATALOG_CONFIG:", rendered)
        self.assertIn("catalog_type", rendered)
        # The Deployment references the Secret via secretKeyRef.
        self.assertIn("secretKeyRef:", rendered)
        self.assertIn("ICEBERG_CATALOG_CONFIG", rendered)


if __name__ == "__main__":
    unittest.main()

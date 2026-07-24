"""v1.3.0 Fix 4 — UDF cache key tests.

Verifies the in-process UDF cache is keyed by ``(udf_name, version)``
(not ``(udf_name, udf_registry_url)`` as in v1.2.40, which never
invalidated when the registry served new code for the same name). The
version is taken from ``step["version"]`` if the pipeline config pins
one, else from the registry response's ``version`` field, else a sha256
hash of the source code.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch


class TestUDFCacheKey(unittest.TestCase):
    def setUp(self):
        import engine
        engine.clear_udf_cache()

    def tearDown(self):
        import engine
        engine.clear_udf_cache()

    def _mock_resp(self, code, version=None):
        resp = MagicMock()
        d = {"code": code}
        if version is not None:
            d["version"] = version
        resp.json.return_value = d
        resp.raise_for_status = MagicMock()
        return resp

    def test_repeat_call_hits_cache_no_http(self):
        """Two _apply_udf calls for the same UDF (same version) result in
        exactly ONE HTTP fetch (the second call hits the cache)."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_get.return_value = self._mock_resp(
                "def myudf(x):\n    return x.upper()", version="1")
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string",
                    "version": "1"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        self.assertEqual(mock_get.call_count, 1)

    def test_version_bump_causes_cache_miss(self):
        """After the registry bumps the UDF version (and the step pins the
        new version), the next fetch is a cache miss -> new code loaded."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_get.side_effect = [
                self._mock_resp("def myudf(x):\n    return x", version="1"),
                self._mock_resp("def myudf(x):\n    return x.upper()",
                                version="2"),
            ]
            conn = MagicMock()
            step_v1 = {"function": "myudf", "args": ["c"],
                       "output_column": "r", "return_type": "string",
                       "version": "1"}
            step_v2 = {"function": "myudf", "args": ["c"],
                       "output_column": "r", "return_type": "string",
                       "version": "2"}
            engine._apply_udf(conn, step_v1, udf_registry_url="http://cp")
            engine._apply_udf(conn, step_v2, udf_registry_url="http://cp")
        # Two HTTP fetches (cache miss on the new version).
        self.assertEqual(mock_get.call_count, 2)

    def test_cache_key_contains_version(self):
        """Inspect the cache dict and confirm keys are (udf_name, version)."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_get.return_value = self._mock_resp(
                "def myudf(x):\n    return x", version="7")
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string",
                    "version": "7"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        # The cache key is (udf_name, version), NOT (udf_name, url).
        self.assertIn(("myudf", "7"), engine._UDF_CACHE)
        self.assertNotIn(("myudf", "http://cp"), engine._UDF_CACHE)

    def test_no_step_version_uses_registry_version(self):
        """When the step has no ``version`` field, the cache key uses the
        registry response's ``version`` field."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_get.return_value = self._mock_resp(
                "def myudf(x):\n    return x", version="3")
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        self.assertIn(("myudf", "3"), engine._UDF_CACHE)

    def test_no_step_version_no_registry_version_uses_source_hash(self):
        """When neither the step nor the registry response has a version,
        the cache key uses a sha256 hash of the source code (prefix
        ``sha:``)."""
        import engine
        code = "def myudf(x):\n    return x.upper()\n"
        with patch("engine.requests.get") as mock_get:
            mock_get.return_value = self._mock_resp(code)
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        keys = list(engine._UDF_CACHE.keys())
        self.assertEqual(len(keys), 1)
        name, ver = keys[0]
        self.assertEqual(name, "myudf")
        self.assertTrue(ver.startswith("sha:"))

    def test_source_change_without_version_invalidates_cache(self):
        """When there's no explicit version, a source change (which
        changes the sha256 fallback version) is a cache miss on the next
        fetch -> new code loaded. The per-name index is updated so a
        repeat of the SAME source still hits the cache."""
        import engine
        code1 = "def myudf(x):\n    return x\n"
        code2 = "def myudf(x):\n    return x.upper()\n"
        with patch("engine.requests.get") as mock_get:
            mock_get.side_effect = [
                self._mock_resp(code1),
                self._mock_resp(code2),
                self._mock_resp(code2),
            ]
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string"}
            # First fetch: code1, cache miss.
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
            old_keys = set(engine._UDF_CACHE.keys())
            # Invalidate the per-name index so the next fetch re-discovers
            # the version (simulates the registry serving new code; in
            # practice a worker would call invalidate_udf or restart).
            engine.invalidate_udf("myudf")
            # Second fetch: code2 (different source -> different sha ->
            # cache miss -> new code loaded).
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
            new_keys = set(engine._UDF_CACHE.keys())
            self.assertNotEqual(old_keys, new_keys)
            # Third fetch: code2 again -> cache hit (no HTTP).
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        self.assertEqual(mock_get.call_count, 2)

    def test_invalidate_udf_drops_index(self):
        """invalidate_udf removes the per-name index entry so the next
        fetch re-discovers the current version."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_get.return_value = self._mock_resp(
                "def myudf(x):\n    return x", version="1")
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
            self.assertEqual(mock_get.call_count, 1)
            engine.invalidate_udf("myudf")
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        self.assertEqual(mock_get.call_count, 2)

    def test_clear_udf_cache_wipes_everything(self):
        """clear_udf_cache empties both the cache and the per-name index."""
        import engine
        with patch("engine.requests.get") as mock_get:
            mock_get.return_value = self._mock_resp(
                "def myudf(x):\n    return x", version="1")
            conn = MagicMock()
            step = {"function": "myudf", "args": ["c"],
                    "output_column": "r", "return_type": "string",
                    "version": "1"}
            engine._apply_udf(conn, step, udf_registry_url="http://cp")
        self.assertEqual(len(engine._UDF_CACHE), 1)
        self.assertEqual(len(engine._UDF_NAME_INDEX), 1)
        engine.clear_udf_cache()
        self.assertEqual(len(engine._UDF_CACHE), 0)
        self.assertEqual(len(engine._UDF_NAME_INDEX), 0)


if __name__ == "__main__":
    unittest.main()

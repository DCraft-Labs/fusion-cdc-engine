"""
Unit tests for the Airflow DAG generator.
No Airflow installation required — mocks the DAG/PythonOperator classes.
"""
import json
import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch


class TestDagGenerator:
    def _make_generator(self):
        from orchestration.dag_generator import DagGenerator
        return DagGenerator("http://localhost:8000", "test-token")

    def test_dag_id_format(self):
        from orchestration.dag_generator import DagGenerator

        source = {"bank_id": "bank-001", "tenant_id": "tenant-001", "source_id": "src-abc"}
        result = DagGenerator._dag_id(source)
        assert result == "fusion_cdc_bank_001_tenant_001_src_abc"

    def test_fetch_scheduled_sources_returns_empty_on_error(self):
        import requests as _requests
        gen = self._make_generator()
        with patch("requests.get", side_effect=_requests.exceptions.ConnectionError("refused")):
            sources = gen._fetch_scheduled_sources()
        assert sources == []

    def test_fetch_scheduled_sources_returns_list(self):
        gen = self._make_generator()
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"bank_id": "b1", "tenant_id": "t1", "source_id": "s1", "schedule_interval": "@hourly"}
        ]
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            sources = gen._fetch_scheduled_sources()
        assert len(sources) == 1
        assert sources[0]["source_id"] == "s1"

    def test_generate_dag_files_creates_py_file(self):
        gen = self._make_generator()
        source = {
            "bank_id": "bank-x", "tenant_id": "tenant-x", "source_id": "src-x",
            "schedule_interval": "@daily", "batch_max_count": 5000,
        }
        with patch.object(gen, "_fetch_scheduled_sources", return_value=[source]):
            with tempfile.TemporaryDirectory() as tmpdir:
                files = gen.generate_dag_files(tmpdir)
                assert len(files) == 1
                assert files[0].endswith("fusion_cdc_bank_x_tenant_x_src_x.py")
                assert os.path.exists(files[0])
                content = open(files[0]).read()
                assert "BatchConsumer" in content
                assert "@daily" in content

    def test_rendered_dag_file_contains_source_config(self):
        from orchestration.dag_generator import DagGenerator

        source = {
            "bank_id": "b", "tenant_id": "t", "source_id": "s1",
            "schedule_interval": "@hourly",
        }
        dag_id = DagGenerator._dag_id(source)
        content = DagGenerator._render_dag_file(source, dag_id)
        # Source config should be embedded
        assert '"source_id": "s1"' in content
        assert "fusion_cdc_b_t_s1" in content

    def test_build_all_requires_airflow(self):
        gen = self._make_generator()
        with patch("builtins.__import__", side_effect=ImportError("no airflow")):
            with pytest.raises(ImportError):
                gen.build_all()

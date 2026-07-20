"""
Airflow DAG Generator — Fusion CDC Engine (Phase 4).

Generates Apache Airflow DAG files for scheduled CDC batch jobs.
Each source configured for "scheduled" mode in the control-plane
gets its own Airflow DAG that triggers the BatchConsumer.

Usage (one-off generation):
    python -m orchestration.dag_generator \
        --control-plane-url http://localhost:8000 \
        --token "$WORKER_TOKEN" \
        --output-dir /opt/airflow/dags

Usage (from Airflow DagFactory plugin):
    from orchestration.dag_generator import DagGenerator
    generator = DagGenerator(control_plane_url, token)
    dags = generator.build_all()          # returns {dag_id: DAG}

DAG naming convention:
    fusion_cdc_{bank_id}_{tenant_id}_{source_id}

Each DAG has a single PythonOperator task that calls:
    BatchConsumer(config).run(max_count=config.get("batch_max_count", 100_000))
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DAG-level settings (can be overridden per source via source config)
# ---------------------------------------------------------------------------
_DEFAULT_SCHEDULE_INTERVAL = "@hourly"
_DEFAULT_RETRIES = 2
_DEFAULT_RETRY_DELAY_MINUTES = 5
_DEFAULT_CATCHUP = False


class DagGenerator:
    """
    Fetches source configurations from the control-plane and produces
    Airflow DAG objects (or serialised Python files).

    Parameters
    ----------
    control_plane_url : str
        Base URL of the Fusion control-plane (e.g. http://control-plane:8000).
    worker_token : str
        X-Worker-Token header value for the internal worker API.
    """

    def __init__(self, control_plane_url: str, worker_token: str) -> None:
        self._base_url = control_plane_url.rstrip("/")
        self._token = worker_token
        self._headers = {"X-Worker-Token": worker_token}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_all(self) -> Dict[str, Any]:
        """
        Fetch all scheduled sources and build Airflow DAGs.

        Returns a dict of {dag_id: airflow.models.dag.DAG}.
        Requires the ``apache-airflow`` package at import time.
        """
        try:
            from airflow import DAG
            from airflow.operators.python import PythonOperator
        except ImportError as exc:
            raise ImportError(
                "apache-airflow must be installed to use DagGenerator.build_all(). "
                "Install it with: pip install apache-airflow"
            ) from exc

        sources = self._fetch_scheduled_sources()
        dags: Dict[str, Any] = {}

        for source in sources:
            dag_id = self._dag_id(source)
            dag = self._build_dag(source, dag_id, DAG, PythonOperator)
            dags[dag_id] = dag

        log.info("DagGenerator built %d DAG(s)", len(dags))
        return dags

    def generate_dag_files(self, output_dir: str) -> List[str]:
        """
        Write one Python DAG file per scheduled source into *output_dir*.
        These files are importable by the Airflow scheduler without a
        live control-plane connection.

        Returns list of written file paths.
        """
        os.makedirs(output_dir, exist_ok=True)
        sources = self._fetch_scheduled_sources()
        written: List[str] = []

        for source in sources:
            dag_id = self._dag_id(source)
            content = self._render_dag_file(source, dag_id)
            path = os.path.join(output_dir, f"{dag_id}.py")
            with open(path, "w") as fh:
                fh.write(content)
            written.append(path)
            log.info("Wrote DAG file: %s", path)

        return written

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_scheduled_sources(self) -> List[dict]:
        """Fetch sources configured for scheduled (batch) mode."""
        url = f"{self._base_url}/api/v1/internal/sources/scheduled"
        try:
            resp = requests.get(url, headers=self._headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error("Failed to fetch scheduled sources from control-plane: %s", exc)
            return []

    @staticmethod
    def _dag_id(source: dict) -> str:
        bank = source.get("bank_id", "unknown").replace("-", "_")
        tenant = source.get("tenant_id", "unknown").replace("-", "_")
        src = source.get("source_id", "unknown").replace("-", "_")
        return f"fusion_cdc_{bank}_{tenant}_{src}"

    def _build_dag(self, source: dict, dag_id: str, DAG, PythonOperator) -> Any:  # type: ignore[return]
        """Build and return an Airflow DAG object in-process."""
        schedule = source.get("schedule_interval", _DEFAULT_SCHEDULE_INTERVAL)
        start_date = datetime(2024, 1, 1)

        default_args = {
            "owner": "fusion",
            "retries": _DEFAULT_RETRIES,
            "retry_delay": timedelta(minutes=_DEFAULT_RETRY_DELAY_MINUTES),
        }

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval=schedule,
            start_date=start_date,
            catchup=_DEFAULT_CATCHUP,
            tags=["fusion", "cdc", source.get("bank_id", ""), source.get("tenant_id", "")],
        )

        task_config = json.dumps(source)
        control_plane_url = self._control_plane_url
        worker_token = self._worker_token
        connection_id = source.get("connection_id", "")

        def _run_batch(**context):
            import json as _json
            from consumer.batch_consumer import BatchConsumer

            cfg = _json.loads(task_config)
            consumer = BatchConsumer(cfg)
            result = consumer.run(max_count=cfg.get("batch_max_count", 100_000))
            log.info("Batch result: %s", result)
            # Store result in XCom for the callback task
            return result

        def _notify_run_complete(**context):
            """
            Spec §5 (P5-7): After the Spark batch job completes, POST to the control-plane
            /internal/connections/{id}/run-complete so it can update connection status,
            advance last-successful-sync timestamp, and record run metrics.
            """
            import requests  # available in Airflow workers

            ti = context.get("ti")
            batch_result = ti.xcom_pull(task_ids="run_batch_consumer") if ti else {}
            payload = {
                "connection_id": connection_id,
                "status": "success",
                "rows_synced": (batch_result or {}).get("rows_written", 0),
                "dag_run_id": context.get("run_id", ""),
            }
            url = f"{control_plane_url}/api/v1/internal/connections/{connection_id}/run-complete"
            headers = {"X-Worker-Token": worker_token, "Content-Type": "application/json"}
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=10)
                log.info("run-complete callback HTTP %s for connection %s", resp.status_code, connection_id)
            except Exception as exc:
                log.warning("Could not call run-complete callback: %s", exc)

        with dag:
            batch_task = PythonOperator(
                task_id="run_batch_consumer",
                python_callable=_run_batch,
            )
            callback_task = PythonOperator(
                task_id="notify_run_complete",
                python_callable=_notify_run_complete,
                trigger_rule="all_done",  # run even if batch fails (report failure)
            )
            batch_task >> callback_task

        return dag

    @staticmethod
    def _render_dag_file(source: dict, dag_id: str) -> str:
        """Render a standalone Python DAG file from a source config."""
        schedule = source.get("schedule_interval", _DEFAULT_SCHEDULE_INTERVAL)
        source_json = json.dumps(source, indent=4)
        connection_id = source.get("connection_id", "")
        return f'''\
# AUTO-GENERATED by DagGenerator — do not edit manually.
# Regenerate with: python -m orchestration.dag_generator

from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_SOURCE_CONFIG = {source_json}
_CONTROL_PLANE_URL = _SOURCE_CONFIG.get("control_plane_url", "")
_WORKER_TOKEN = _SOURCE_CONFIG.get("worker_token", "")
_CONNECTION_ID = "{connection_id}"

_default_args = {{
    "owner": "fusion",
    "retries": {_DEFAULT_RETRIES},
    "retry_delay": timedelta(minutes={_DEFAULT_RETRY_DELAY_MINUTES}),
}}


def _run_batch(**context):
    from consumer.batch_consumer import BatchConsumer
    result = BatchConsumer(_SOURCE_CONFIG).run(
        max_count=_SOURCE_CONFIG.get("batch_max_count", 100_000)
    )
    log.info("Batch result: %s", result)
    return result


def _notify_run_complete(**context):
    """Spec §5 (P5-7): notify control-plane after batch job completes."""
    import requests
    ti = context.get("ti")
    batch_result = ti.xcom_pull(task_ids="run_batch_consumer") if ti else {{}}
    payload = {{
        "connection_id": _CONNECTION_ID,
        "status": "success",
        "rows_synced": (batch_result or {{}}).get("rows_written", 0),
        "dag_run_id": context.get("run_id", ""),
    }}
    url = f"{{_CONTROL_PLANE_URL}}/api/v1/internal/connections/{{_CONNECTION_ID}}/run-complete"
    headers = {{"X-Worker-Token": _WORKER_TOKEN, "Content-Type": "application/json"}}
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        log.info("run-complete callback HTTP %s", resp.status_code)
    except Exception as exc:
        log.warning("Could not call run-complete callback: %s", exc)


with DAG(
    dag_id="{dag_id}",
    default_args=_default_args,
    schedule_interval="{schedule}",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fusion", "cdc"],
) as dag:
    _batch = PythonOperator(task_id="run_batch_consumer", python_callable=_run_batch)
    _callback = PythonOperator(
        task_id="notify_run_complete",
        python_callable=_notify_run_complete,
        trigger_rule="all_done",
    )
    _batch >> _callback
'''


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Generate Airflow DAG files for Fusion CDC sources")
    parser.add_argument("--control-plane-url", required=True, help="Control-plane base URL")
    parser.add_argument("--token", required=True, help="Worker token for X-Worker-Token header")
    parser.add_argument("--output-dir", default="/opt/airflow/dags", help="Directory to write DAG files")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    gen = DagGenerator(args.control_plane_url, args.token)
    files = gen.generate_dag_files(args.output_dir)
    print(f"Generated {len(files)} DAG file(s) in {args.output_dir}")
    for f in files:
        print(f"  {f}")

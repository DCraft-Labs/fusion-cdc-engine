"""
StreamingConsumer — REALTIME mode CDC consumer using micro-batch polling.

Pipeline:
    RedisStreamSource → TransformPipelineExecutor → DQExecutor → Writer (Postgres|Iceberg)
                                                               → DLQWriter (on DQ block)

Config keys:
    redis_url           str    Redis URL
    stream_keys         list   Stream keys to consume
    consumer_group      str    Consumer group name (default: fusion-spark)
    consumer_name       str    This consumer's name
    trigger_seconds     int    Polling interval (default: 5)
    checkpoint_dir      str    Spark checkpoint directory
    writer_type         str    "postgres" | "iceberg"
    writer_config       dict   Writer-specific config
    transform_spec      dict   Pipeline spec (optional)
    dq_policy           dict   DQ policy (optional)
    dlq_config          dict   DLQ writer config (optional)
    spark_app_name      str    Spark app name (default: fusion-spark-consumer)
    connection_id       str    Connection ID (used for schema-reload tracking)
    schema_reload_port  int    Port for schema-reload webhook server (default: 8080)
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)


class StreamingConsumer:
    """Micro-batch streaming consumer wiring Redis → transforms → DQ → destination."""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.spark = None
        self.redis_source = None
        self.transform_executor = None
        self.dq_executor = None
        self.writer = None
        self.dlq_writer = None
        self._running = False
        self._schema_reload_server = None

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def setup(self, spark=None) -> None:
        """Initialise Spark session and all pipeline components."""
        from pyspark.sql import SparkSession

        from consumer.redis_source import RedisStreamSource
        from consumer.schema_reload_server import SchemaReloadServer
        from dq.executor import DQExecutor
        from transform.executor import TransformPipelineExecutor
        from writers.dlq_writer import DLQWriter
        from writers.iceberg_writer import IcebergWriter
        from writers.postgres_writer import PostgresWriter

        if spark is not None:
            self.spark = spark
        else:
            app_name = self.config.get("spark_app_name", "fusion-spark-consumer")
            checkpoint_dir = self.config.get(
                "checkpoint_dir",
                os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints"),
            )
            self.spark = (
                SparkSession.builder.appName(app_name)
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate()
            )

        # Redis source
        self.redis_source = RedisStreamSource(
            redis_url=self.config["redis_url"],
            stream_keys=self.config["stream_keys"],
            group=self.config.get("consumer_group", RedisStreamSource.DEFAULT_GROUP),
            consumer_name=self.config.get("consumer_name", "spark-consumer-1"),
        )

        # Transform pipeline (optional)
        if self.config.get("transform_spec"):
            self.transform_executor = TransformPipelineExecutor(
                self.config["transform_spec"],
                spark=self.spark,
                udf_registry_url=self.config.get("udf_registry_url"),
            )

        # DQ executor (optional)
        if self.config.get("dq_policy"):
            self.dq_executor = DQExecutor(self.config["dq_policy"])

        # Writer
        writer_type = self.config.get("writer_type", "postgres")
        wc = self.config.get("writer_config", {})
        if writer_type == "postgres":
            self.writer = PostgresWriter(
                pg_dsn=wc["pg_dsn"],
                table=wc["table"],
                pk_columns=wc["pk_columns"],
                schema=wc.get("schema", "public"),
                scd2_mode=wc.get("scd2_mode", False),
            )
        elif writer_type == "iceberg":
            self.writer = IcebergWriter(
                spark=self.spark,
                catalog=wc["catalog"],
                namespace=wc["namespace"],
                table=wc["table"],
                pk_columns=wc["pk_columns"],
            )
        else:
            raise ValueError(f"Unknown writer_type: {writer_type!r}")

        # DLQ writer (optional)
        if self.config.get("dlq_config"):
            dc = self.config["dlq_config"]
            self.dlq_writer = DLQWriter(
                redis_url=dc["redis_url"],
                bank_id=dc["bank_id"],
                tenant_id=dc["tenant_id"],
                source_id=dc["source_id"],
                schema_name=dc["schema_name"],
                table_name=dc["table_name"],
            )

        # Schema reload webhook server (spec §3) — started here so it's live before streaming
        reload_port = int(self.config.get("schema_reload_port", os.environ.get("SCHEMA_RELOAD_PORT", 8080)))
        self._schema_reload_server = SchemaReloadServer(port=reload_port)
        self._schema_reload_server.start()

    # ------------------------------------------------------------------
    # Core batch processing
    # ------------------------------------------------------------------

    def process_batch(self, batch_df, batch_id: int) -> None:
        """Process one micro-batch: transform → DQ → write."""
        if self.transform_executor:
            batch_df = self.transform_executor.apply(batch_df)

        if self.dq_executor:
            passed_df, failed_df, violations = self.dq_executor.check(batch_df)
            if violations:
                logger.warning("Batch %d: %d DQ violations", batch_id, len(violations))
            if self.dlq_writer and failed_df is not None and not failed_df.rdd.isEmpty():
                self.dlq_writer.write(failed_df, batch_id)
            batch_df = passed_df

        self.writer.upsert_batch(batch_df, batch_id)

    # ------------------------------------------------------------------
    # Streaming loop
    # ------------------------------------------------------------------

    def start(self, max_batches: Optional[int] = None) -> None:
        """
        Start the polling loop.  Runs until ``stop()`` is called or
        ``max_batches`` is reached (useful for testing).
        """
        self.redis_source.ensure_groups()
        self._running = True
        trigger_seconds = self.config.get("trigger_seconds", 5)
        connection_id = self.config.get("connection_id", "")
        batch_id = 0

        logger.info("StreamingConsumer started (trigger=%ds)", trigger_seconds)
        while self._running:
            # Check for spec §3 schema-reload notifications
            if connection_id and self._schema_reload_server and \
                    self._schema_reload_server.has_pending_reload(connection_id):
                logger.info("Schema reload requested for connection_id=%s — refreshing transform spec", connection_id)
                self._reload_transform_spec(connection_id)
                self._schema_reload_server.mark_reloaded(connection_id)

            batch_df = self.redis_source.read_batch(
                self.spark,
                count=self.config.get("read_count", 100),
            )
            count = batch_df.count()
            if count > 0:
                self.process_batch(batch_df, batch_id)
                self.redis_source.ack()
                batch_id += 1
                logger.debug("Processed batch %d (%d rows)", batch_id - 1, count)

            if max_batches is not None and batch_id >= max_batches:
                break

            if count == 0:
                time.sleep(trigger_seconds)

        logger.info("StreamingConsumer stopped after %d batches", batch_id)

    def _reload_transform_spec(self, connection_id: str) -> None:
        """
        Fetch the latest transformation spec from the control plane and
        hot-swap the executor.  Uses the CONTROL_PLANE_URL env var.
        """
        import requests
        from transform.executor import TransformPipelineExecutor

        cp_url = os.environ.get("CONTROL_PLANE_URL", "")
        if not cp_url:
            logger.warning("CONTROL_PLANE_URL not set — cannot reload transform spec")
            return
        try:
            resp = requests.get(
                f"{cp_url}/api/v1/connections/{connection_id}/transform-spec",
                timeout=10,
            )
            if resp.ok:
                spec = resp.json()
                self.transform_executor = TransformPipelineExecutor(
                    spec, spark=self.spark,
                    udf_registry_url=self.config.get("udf_registry_url"),
                )
                logger.info("Transform spec reloaded for connection_id=%s", connection_id)
            else:
                logger.warning(
                    "Failed to fetch transform spec for connection_id=%s: HTTP %s",
                    connection_id, resp.status_code,
                )
        except Exception as exc:
            logger.error("Error reloading transform spec for connection_id=%s: %s", connection_id, exc)

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# CLI entry point — `python -m consumer.streaming_consumer`
# ---------------------------------------------------------------------------

def _build_config_from_env() -> dict:
    """
    Build a StreamingConsumer config dict from environment variables.
    This is used when running as a standalone container or via spark-submit.
    """
    import requests
    from consumer.redis_source import RedisStreamSource

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    cp_url = os.environ.get("CONTROL_PLANE_URL", "")
    connection_id = os.environ.get("CONNECTION_ID", "")
    consumer_group = os.environ.get("CONSUMER_GROUP", RedisStreamSource.DEFAULT_GROUP)
    consumer_name = os.environ.get("CONSUMER_NAME", "spark-consumer-1")
    checkpoint_dir = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    trigger_seconds = int(os.environ.get("TRIGGER_SECONDS", "5"))
    worker_token = os.environ.get("WORKER_TOKEN", "")

    # Destination Postgres config
    dest_pg_dsn = (
        f"postgresql://{os.environ.get('DEST_PG_USER', 'dw_user')}"
        f":{os.environ.get('DEST_PG_PASSWORD', 'dw_password')}"
        f"@{os.environ.get('DEST_PG_HOST', 'localhost')}"
        f":{os.environ.get('DEST_PG_PORT', '5433')}"
        f"/{os.environ.get('DEST_PG_DATABASE', 'fusion_dw')}"
    )

    # Discover stream keys from Redis: scan for cdc:* keys
    stream_keys = []
    if connection_id and cp_url:
        # Fetch connection details from control plane to get stream keys
        try:
            headers = {"X-Worker-Token": worker_token} if worker_token else {}
            resp = requests.get(
                f"{cp_url}/api/v1/connections/{connection_id}",
                headers=headers,
                timeout=10,
            )
            if resp.ok:
                conn = resp.json()
                source_id = conn.get("source_id", "")
                for stream in conn.get("streams", []):
                    schema = stream.get("schema_name", "*")
                    table = stream.get("table_name", "*")
                    key = f"cdc:*:*:{source_id}:{schema}:{table}"
                    stream_keys.append(key)
        except Exception as exc:
            logger.warning("Could not fetch connection details: %s", exc)

    if not stream_keys:
        # Fallback: discover cdc:* keys from Redis
        try:
            import redis as _redis
            r = _redis.from_url(redis_url, decode_responses=True)
            discovered = list(r.scan_iter("cdc:*", count=1000))
            stream_keys = discovered or ["cdc:*"]
            logger.info("Discovered %d stream key(s) from Redis", len(stream_keys))
        except Exception as exc:
            logger.warning("Could not scan Redis for stream keys: %s", exc)
            stream_keys = ["cdc:*"]

    config = {
        "redis_url": redis_url,
        "stream_keys": stream_keys,
        "consumer_group": consumer_group,
        "consumer_name": consumer_name,
        "checkpoint_dir": checkpoint_dir,
        "trigger_seconds": trigger_seconds,
        "connection_id": connection_id,
        "writer_type": os.environ.get("WRITER_TYPE", "postgres"),
        "writer_config": {
            "pg_dsn": dest_pg_dsn,
            "table": os.environ.get("DEST_TABLE", "cdc_events"),
            "pk_columns": os.environ.get("DEST_PK_COLUMNS", "event_id").split(","),
            "schema": os.environ.get("DEST_SCHEMA", "public"),
        },
    }

    # Transform spec and DQ policy — fetched from control plane if available
    if connection_id and cp_url:
        try:
            headers = {"X-Worker-Token": worker_token} if worker_token else {}
            resp = requests.get(
                f"{cp_url}/api/v1/connections/{connection_id}/transform-spec",
                headers=headers,
                timeout=10,
            )
            if resp.ok:
                config["transform_spec"] = resp.json()
        except Exception:
            pass

    return config


if __name__ == "__main__":
    import argparse
    import signal

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Fusion Spark CDC Consumer")
    parser.add_argument("--connection-id", default=os.environ.get("CONNECTION_ID", ""))
    parser.add_argument("--checkpoint-dir", default=os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints"))
    args = parser.parse_args()

    if args.connection_id:
        os.environ["CONNECTION_ID"] = args.connection_id
    if args.checkpoint_dir:
        os.environ["SPARK_CHECKPOINT_DIR"] = args.checkpoint_dir

    config = _build_config_from_env()
    consumer = StreamingConsumer(config)
    consumer.setup()

    def _shutdown(sig, frame):
        logger.info("Received signal %s — shutting down", sig)
        consumer.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer.start()

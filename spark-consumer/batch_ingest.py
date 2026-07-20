"""
batch_ingest.py — CLI entry point for fusion-cdc-engine batch source connectors.

Reads a JSON job config and runs the appropriate source connector
(MySQL, PostgreSQL, or MongoDB) against the Iceberg or Postgres destination.

Usage:
    python batch_ingest.py --config job.json

    # Or pass config as a JSON string:
    python batch_ingest.py --config-json '{"source_type": "mysql", ...}'

    # Dev mode (local Spark, no K8s):
    SPARK_ENV=dev ICEBERG_WAREHOUSE=file:///tmp/warehouse python batch_ingest.py --config job.json

Job config schema (job.json):
{
    "source_type": "mysql" | "postgres" | "mongodb",

    // MySQL / Postgres
    "host":         "rds-host.amazonaws.com",
    "port":         3306,
    "username":     "dwhusr",
    "password":     "secret",
    "database":     "mydb",
    "source_schema": "public",     // Postgres only
    "source_db":    "mydb",        // MongoDB only (db containing the collection)

    // Table / collection
    "source_table":      "orders",  // MySQL / Postgres: table name
    "source_collection": "orders",  // MongoDB: collection name
    "dest_table":        "raw_orders",

    // Incremental / PK
    "cursor_field":  "updated_at",  // null → full refresh every run
    "primary_key":   "id",

    // Destination
    "writer_type":   "iceberg",     // "iceberg" | "postgres"
    "catalog":       "vp_terra",
    "namespace":     "raw_bank",

    // Postgres destination (only when writer_type = "postgres")
    "pg_dsn":        "host=localhost port=5432 dbname=dw user=u password=p",
    "pg_schema":     "public",

    // Spark config — loaded from the UI Destination record (takes priority over env vars).
    // Either pass the full dict here or set "destination_id" and the job will
    // fetch the config from the control-plane API automatically.
    "destination_id":   "<uuid>",            // optional — fetches Spark config from API
    "spark_config": {                         // optional — inline override
        "spark_env":    "prod",
        "spark_master": "k8s://https://kubernetes.default.svc.cluster.local:443",
        "catalog_name": "vp_terra",
        "nessie_uri":   "http://nessie-svc:19120/api/v2",
        "nessie_ref":   "main",
        "warehouse":    "s3a://my-bucket/warehouse/",
        "s3_endpoint":  "https://s3.ap-south-1.amazonaws.com",
        "s3_region":    "ap-south-1",
        "aws_credentials_provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    },

    // Spark app name (optional)
    "app_name":      "fusion-batch-ingest"
}
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("batch_ingest")


def _load_config(args) -> dict:
    if args.config:
        with open(args.config) as f:
            return json.load(f)
    if args.config_json:
        return json.loads(args.config_json)
    raise ValueError("Provide --config <file> or --config-json '<json>'")


def _fetch_destination_config(destination_id: str) -> dict:
    """
    Fetch the Iceberg destination config from the fusion-cdc-engine control-plane API.
    Returns the `connection_config` dict (or empty dict on error).
    """
    import os
    import urllib.request

    control_plane_url = os.environ.get(
        "CONTROL_PLANE_URL", "http://localhost:8000"
    ).rstrip("/")
    worker_token = os.environ.get("WORKER_TOKEN", "")

    url = f"{control_plane_url}/api/v1/destinations/{destination_id}"
    req = urllib.request.Request(url)
    if worker_token:
        req.add_header("Authorization", f"Bearer {worker_token}")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            import json as _json
            data = _json.loads(resp.read())
            # The API returns the destination record; Spark config lives in
            # connection_config (or config for older records).
            return data.get("connection_config") or data.get("config") or {}
    except Exception as exc:
        logger.warning(
            "Could not fetch destination config for %s: %s — using env/defaults.",
            destination_id, exc,
        )
        return {}


def run(config: dict) -> None:
    from sources.spark_session import get_spark_session, get_spark_session_from_config

    app_name = config.get("app_name", "fusion-batch-ingest")

    # Build Spark session — priority:
    #   1. inline spark_config dict in job config
    #   2. destination_id → fetch from control-plane API
    #   3. environment variables / hard-coded defaults
    spark_cfg: dict = {}
    if config.get("spark_config"):
        spark_cfg = config["spark_config"]
        logger.info("Using inline spark_config from job config.")
    elif config.get("destination_id"):
        spark_cfg = _fetch_destination_config(config["destination_id"])
        logger.info(
            "Fetched Spark config from destination %s.", config["destination_id"]
        )

    if spark_cfg:
        spark = get_spark_session_from_config(spark_cfg, app_name)
    else:
        spark = get_spark_session(app_name)

    source_type = config["source_type"].lower()
    writer_type = config.get("writer_type", "iceberg")
    catalog = config.get("catalog", "vp_terra")
    namespace = config["namespace"]
    dest_table = config["dest_table"]
    cursor_field = config.get("cursor_field") or None
    primary_key = config.get("primary_key", "id")
    pg_dsn = config.get("pg_dsn")
    pg_schema = config.get("pg_schema", "public")

    if source_type == "mysql":
        from sources.mysql_source import MySQLSource
        src = MySQLSource(
            host=config["host"],
            port=config.get("port", 3306),
            username=config["username"],
            password=config["password"],
            database=config["database"],
        )
        src.ingest(
            spark=spark,
            source_table=config["source_table"],
            dest_table=dest_table,
            cursor_field=cursor_field,
            primary_key=primary_key,
            catalog=catalog,
            namespace=namespace,
            writer_type=writer_type,
            pg_dsn=pg_dsn,
            pg_schema=pg_schema,
        )

    elif source_type == "postgres":
        from sources.postgres_source import PostgresSource
        src = PostgresSource(
            host=config["host"],
            port=config.get("port", 5432),
            username=config["username"],
            password=config["password"],
            database=config["database"],
            source_schema=config.get("source_schema", "public"),
        )
        src.ingest(
            spark=spark,
            source_table=config["source_table"],
            dest_table=dest_table,
            cursor_field=cursor_field,
            primary_key=primary_key,
            catalog=catalog,
            namespace=namespace,
            writer_type=writer_type,
            pg_dsn=pg_dsn,
            pg_schema=pg_schema,
        )

    elif source_type in ("mongo", "mongodb"):
        from sources.mongodb_source import MongoDBSource
        src = MongoDBSource(
            host=config["host"],
            port=config.get("port", 27017),
            username=config.get("username", ""),
            password=config.get("password", ""),
            auth_source=config.get("auth_source", "admin"),
        )
        src.ingest(
            spark=spark,
            source_db=config.get("source_db", config.get("database")),
            source_collection=config.get(
                "source_collection", config.get("source_table")
            ),
            dest_table=dest_table,
            cursor_field=cursor_field,
            primary_key=primary_key,
            catalog=catalog,
            namespace=namespace,
            writer_type=writer_type,
            pg_dsn=pg_dsn,
            pg_schema=pg_schema,
        )

    else:
        raise ValueError(
            f"Unknown source_type={source_type!r}. "
            "Supported: mysql, postgres, mongodb."
        )

    spark.stop()
    logger.info("Job complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="fusion-cdc-engine batch source ingest runner"
    )
    parser.add_argument("--config", help="Path to JSON config file")
    parser.add_argument("--config-json", help="Inline JSON config string")
    args = parser.parse_args()

    config = _load_config(args)
    run(config)


if __name__ == "__main__":
    main()

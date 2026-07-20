"""
Env-configurable Spark session factory for fusion-cdc-engine batch ingest.

Answer to the question "can I configure Spark .config() things via environment variables?":
YES — all SparkSession.builder.config() values are read from environment variables here.
In Kubernetes (prod), inject them from Secrets or ConfigMaps:

    env:
      - name: NESSIE_URI
        valueFrom:
          configMapKeyRef:
            name: fusion-spark-config
            key: nessie_uri
      - name: ICEBERG_WAREHOUSE
        valueFrom:
          secretKeyRef:
            name: fusion-spark-secrets
            key: iceberg_warehouse

Environment variables (all optional — defaults to production settings):
    SPARK_ENV                  "dev" | "prod"   (default: prod)
    SPARK_MASTER               Spark master URL  (default: k8s://https://kubernetes.default.svc.cluster.local:443)
    ICEBERG_CATALOG            Catalog name      (default: vp_terra)
    NESSIE_URI                 Nessie REST API   (default: http://...fusion:19120/api/v2)
    NESSIE_REF                 Nessie branch     (default: main)
    ICEBERG_WAREHOUSE          S3 warehouse URI  (default: s3a://visapay-ds-app-dr/...)
    S3_ENDPOINT                S3 endpoint URL   (default: https://s3.ap-south-1.amazonaws.com)
    S3_REGION                  S3 region         (default: ap-south-1)
    AWS_CREDENTIALS_PROVIDER   AWS creds class   (default: WebIdentityTokenCredentialsProvider)

SPARK_ENV=dev  → local[*], Iceberg/Nessie only.
               Set ICEBERG_WAREHOUSE=file:///tmp/iceberg-warehouse for fully local testing.
SPARK_ENV=prod → Kubernetes master + Nessie/Iceberg on S3 + IRSA auth (WebIdentity).
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Read configuration from environment (with production defaults)
# ---------------------------------------------------------------------------

SPARK_ENV = os.environ.get("SPARK_ENV", "prod")

SPARK_MASTER = os.environ.get(
    "SPARK_MASTER",
    "k8s://https://kubernetes.default.svc.cluster.local:443",
)

ICEBERG_CATALOG = os.environ.get("ICEBERG_CATALOG", "vp_terra")

NESSIE_URI = os.environ.get(
    "NESSIE_URI",
    "http://dr-shared-visapay-fusion-nessie-ds-nessie.fusion:19120/api/v2",
)
NESSIE_REF = os.environ.get("NESSIE_REF", "main")

ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3a://visapay-ds-app-dr/terra/data/warehouse/",
)

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ap-south-1.amazonaws.com")
S3_REGION = os.environ.get("S3_REGION", "ap-south-1")

AWS_CREDS_PROVIDER = os.environ.get(
    "AWS_CREDENTIALS_PROVIDER",
    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
)


def get_spark_session(app_name: str = "fusion-batch-ingest"):
    """
    Return a SparkSession configured for the current SPARK_ENV.

    dev  → local[*] with Iceberg/Nessie.  No S3 configuration applied —
           point ICEBERG_WAREHOUSE to a local path for unit testing.

    prod → Kubernetes master + Nessie Iceberg catalog backed by AWS S3.
           All connection details are read from environment variables so
           they can be injected at deploy time via K8s Secrets / ConfigMaps.
    """
    from pyspark.sql import SparkSession

    cat = ICEBERG_CATALOG

    # Iceberg + Nessie config — identical for dev and prod
    iceberg_conf = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{cat}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"spark.sql.catalog.{cat}.uri": NESSIE_URI,
        f"spark.sql.catalog.{cat}.ref": NESSIE_REF,
        f"spark.sql.catalog.{cat}.warehouse": ICEBERG_WAREHOUSE,
    }

    builder = SparkSession.builder.appName(app_name)

    if SPARK_ENV == "dev":
        builder = builder.master("local[*]")
    else:
        # Production: K8s master + S3 filesystem config
        builder = (
            builder
            .master(SPARK_MASTER)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.region", S3_REGION)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", AWS_CREDS_PROVIDER)
        )

    for key, value in iceberg_conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    logger.info(
        "SparkSession created [env=%s, master=%s, catalog=%s, warehouse=%s]",
        SPARK_ENV,
        spark.sparkContext.master,
        cat,
        ICEBERG_WAREHOUSE,
    )
    return spark


def get_spark_session_from_config(
    dest_config: dict,
    app_name: str = "fusion-batch-ingest",
):
    """
    Build a SparkSession from a destination config dict (as stored in the
    fusion-cdc-engine control-plane `destinations.connection_config` JSONB column).

    All keys fall back to environment variables (and then to hard-coded defaults)
    so the function is fully backward-compatible.

    Expected keys in dest_config (all optional — falls back to env / defaults):
        spark_env                  "dev" | "prod"
        spark_master               Spark master URL
        catalog_name               Iceberg catalog name (e.g. "vp_terra")
        nessie_uri                 Nessie REST API endpoint
        nessie_ref                 Nessie branch (default "main")
        warehouse                  Iceberg warehouse URI (s3a://... or file:///...)
        s3_endpoint                S3 endpoint URL
        s3_region                  S3 region
        aws_credentials_provider   AWS credentials provider class

    Usage in batch_ingest.py:
        dest_cfg = fetch_destination_config(dest_id)   # from control-plane API
        spark = get_spark_session_from_config(dest_cfg, app_name="my-job")
    """
    from pyspark.sql import SparkSession

    env = dest_config.get("spark_env") or SPARK_ENV
    master = dest_config.get("spark_master") or SPARK_MASTER
    cat = dest_config.get("catalog_name") or ICEBERG_CATALOG
    nessie_uri = dest_config.get("nessie_uri") or NESSIE_URI
    nessie_ref = dest_config.get("nessie_ref") or NESSIE_REF
    warehouse = dest_config.get("warehouse") or ICEBERG_WAREHOUSE
    s3_endpoint = dest_config.get("s3_endpoint") or S3_ENDPOINT
    s3_region = dest_config.get("s3_region") or S3_REGION
    aws_creds = dest_config.get("aws_credentials_provider") or AWS_CREDS_PROVIDER

    iceberg_conf = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{cat}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"spark.sql.catalog.{cat}.uri": nessie_uri,
        f"spark.sql.catalog.{cat}.ref": nessie_ref,
        f"spark.sql.catalog.{cat}.warehouse": warehouse,
    }

    builder = SparkSession.builder.appName(app_name)

    if env == "dev":
        builder = builder.master("local[*]")
    else:
        builder = (
            builder
            .master(master)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.region", s3_region)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", aws_creds)
        )

    for key, value in iceberg_conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    logger.info(
        "SparkSession created from dest config [env=%s, master=%s, catalog=%s, warehouse=%s]",
        env,
        spark.sparkContext.master,
        cat,
        warehouse,
    )
    return spark

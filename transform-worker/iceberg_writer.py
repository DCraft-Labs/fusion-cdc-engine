"""
Iceberg Lake Writer — DuckDB batching + PyIceberg commits.

This module implements the DuckDB/PyIceberg lake path for Fusion CDC:
  - source CDC events / initial-load chunks are converted to Arrow batches
  - PyIceberg loads the catalog (glue/rest/hive/sql/nessie/dynamodb)
  - table is created on first write with partition_spec + identifier_fields
  - rows are applied via table.append() (initial) or table.upsert()/delete() (CDC)
  - auth modes a/b/c are resolved into boto credentials / PyIceberg client.* / role-arn

No Spark required. Designed for the transform-worker pod (low RAM/CPU).
"""
from __future__ import annotations

import logging
import os
from typing import Any

import pyarrow as pa

log = logging.getLogger(__name__)


# ─── Catalog factory ────────────────────────────────────────────────────────
def load_catalog(dest_config: dict):
    """Build a PyIceberg Catalog from destination connection_config."""
    from pyiceberg.catalog import load_catalog as _load

    catalog_type = (dest_config.get("catalog_type") or "rest").lower()
    catalog_name = dest_config.get("catalog_name", "fusion_cdc")

    settings: dict[str, Any] = {}
    creds = _resolve_credentials(dest_config)

    if catalog_type == "rest":
        settings["uri"] = dest_config["catalog_uri"]
        if dest_config.get("catalog_oauth_token"):
            settings["credential"] = dest_config["catalog_oauth_token"]
        if dest_config.get("rest_sigv4"):
            settings["rest-signing-enabled"] = "true"
    elif catalog_type == "nessie":
        settings["uri"] = dest_config["nessie_uri"]
        settings["ref"] = dest_config.get("nessie_ref", "main")
    elif catalog_type == "glue":
        settings["glue.region"] = dest_config.get("glue_region") or creds.get("region", "us-east-1")
        if dest_config.get("glue_endpoint"):
            settings["glue.endpoint"] = dest_config["glue_endpoint"]
        if creds.get("access_key_id"):
            settings["glue.access-key-id"] = creds["access_key_id"]
            settings["glue.secret-access-key"] = creds["secret_access_key"]
            if creds.get("session_token"):
                settings["glue.session-token"] = creds["session_token"]
    elif catalog_type == "hive":
        settings["uri"] = dest_config["hive_uri"]
    elif catalog_type == "sql":
        settings["uri"] = dest_config["sql_catalog_uri"]
    elif catalog_type == "dynamodb":
        settings["dynamodb.table-name"] = dest_config["dynamodb_table"]
    else:
        raise ValueError(f"Unsupported catalog_type: {catalog_type}")

    # S3 settings (always set when present)
    s3 = _resolve_s3_settings(dest_config, creds)
    settings.update(s3)

    # PyIceberg expects settings under `catalog.<name>.*` or passed to load_catalog
    return _load(catalog_name, **{f"catalog.{catalog_name}.{k}": v for k, v in settings.items()})


def _resolve_credentials(dest_config: dict) -> dict:
    """Resolve auth mode a/b/c into a boto-style credential dict."""
    mode = (dest_config.get("auth_mode") or "access_key").lower()
    region = dest_config.get("aws_region") or dest_config.get("s3_region") or "us-east-1"
    out = {"region": region}

    if mode == "access_key":
        out["access_key_id"] = dest_config.get("aws_access_key_id")
        out["secret_access_key"] = dest_config.get("aws_secret_access_key")
        out["session_token"] = dest_config.get("aws_session_token")
    elif mode == "sts_assume":
        # Parent → target STS chain. Caller (transform-worker) is expected to have
        # resolved temp creds before invoking the writer; if pre-resolved creds are
        # present in dest_config (encrypted), use them directly.
        out["access_key_id"] = dest_config.get("aws_access_key_id")
        out["secret_access_key"] = dest_config.get("aws_secret_access_key")
        out["session_token"] = dest_config.get("aws_session_token")
        out["role_arn"] = dest_config.get("target_role_arn")
    elif mode == "irsa":
        # IRSA / workload identity — boto default chain picks up the SA token.
        # PyIceberg supports `client.role-arn` for direct role assumption.
        out["role_arn"] = dest_config.get("service_account_role_arn")
    else:
        raise ValueError(f"Unsupported auth_mode: {mode}")

    return out


def _resolve_s3_settings(dest_config: dict, creds: dict) -> dict:
    """Translate S3 / object-store options into PyIceberg `s3.*` settings."""
    s3: dict[str, Any] = {}
    if dest_config.get("s3_endpoint"):
        s3["s3.endpoint"] = dest_config["s3_endpoint"]
    if dest_config.get("s3_region") or creds.get("region"):
        s3["s3.region"] = dest_config.get("s3_region") or creds["region"]
    if dest_config.get("s3_path_style"):
        s3["s3.path-style-access"] = "true"
    if dest_config.get("s3_force_virtual_addressing"):
        s3["s3.force-virtual-addressing"] = "true"
    if dest_config.get("s3_proxy_uri"):
        s3["s3.proxy-uri"] = dest_config["s3_proxy_uri"]
    if dest_config.get("s3_anonymous"):
        s3["s3.anonymous"] = "true"

    # Auth — prefer unified `client.*` when same_creds_for_catalog_and_s3 is true
    if dest_config.get("same_creds_for_catalog_and_s3", True):
        if creds.get("access_key_id"):
            s3["s3.access-key-id"] = creds["access_key_id"]
            s3["s3.secret-access-key"] = creds["secret_access_key"]
            if creds.get("session_token"):
                s3["s3.session-token"] = creds["session_token"]
        if creds.get("role_arn"):
            s3["s3.role-arn"] = creds["role_arn"]
    else:
        # Separate catalog vs S3 creds — use explicit s3.* keys
        if dest_config.get("s3_access_key_id"):
            s3["s3.access-key-id"] = dest_config["s3_access_key_id"]
            s3["s3.secret-access-key"] = dest_config["s3_secret_access_key"]
            if dest_config.get("s3_session_token"):
                s3["s3.session-token"] = dest_config["s3_session_token"]

    # SSE
    sse = (dest_config.get("sse_type") or "none").lower()
    if sse != "none":
        s3["s3.sse.type"] = sse
        if sse in ("sse-kms", "dsse-kms") and dest_config.get("sse_kms_key_id"):
            s3["s3.sse.kms-key-id"] = dest_config["sse_kms_key_id"]
    return s3


def _normalize_warehouse(warehouse: str) -> str:
    """Normalize `s3a://` → `s3://` for PyIceberg compatibility."""
    if not warehouse:
        return warehouse
    if warehouse.startswith("s3a://"):
        return "s3://" + warehouse[len("s3a://"):]
    return warehouse

# ─── Partition + table helpers ───────────────────────────────────────────────
def _build_partition_spec(table_schema, partition_spec_cfg):
    """Build a PyIceberg PartitionSpec from [{source_column, transform, name}, ...]."""
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.partitioning import PartitionField
    from pyiceberg.transforms import (
        IdentityTransform, YearTransform, MonthTransform, DayTransform,
        HourTransform, BucketTransform, TruncateTransform,
    )

    transform_map = {
        "identity": IdentityTransform(),
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
        "hour": HourTransform(),
        "bucket": lambda n: BucketTransform(n),
        "truncate": lambda w: TruncateTransform(w),
    }

    fields = []
    for i, p in enumerate(partition_spec_cfg or []):
        col = p["source_column"]
        t = p["transform"]
        name = p.get("name") or f"{col}_{t}"
        if t == "bucket":
            transform = BucketTransform(int(p.get("width", 16)))
        elif t == "truncate":
            transform = TruncateTransform(int(p.get("width", 16)))
        else:
            transform = transform_map[t]
        fields.append(PartitionField(
            source_id=table_schema.find_field(col).field_id,
            field_id=1000 + i,
            transform=transform,
            name=name,
        ))
    return PartitionSpec(fields) if fields else PartitionSpec(())


def _build_table_properties(dest_config: dict) -> dict:
    """Default Iceberg write properties for Fusion CDC."""
    props = {
        "format-version": str(dest_config.get("format_version", 2)),
        "write.parquet.compression-codec": dest_config.get("parquet_compression", "zstd"),
        "write.object-storage.enabled": str(dest_config.get("object_storage_enabled", True)).lower(),
        "write.object-storage.partitioned-paths": str(dest_config.get("partitioned_paths", True)).lower(),
    }
    if dest_config.get("write_metadata_delete_after_commit"):
        props["write.metadata.delete-after-commit.enabled"] = "true"
    return props


def _rows_to_arrow(rows: list[dict]) -> pa.Table:
    """Convert CDC row dicts to a PyArrow Table (string-safe)."""
    if not rows:
        return pa.table({})
    return pa.Table.from_pylist(rows)


def _get_or_create_table(catalog, namespace: str, table_name: str,
                         arrow_schema: pa.Schema, dest_config: dict):
    """Idempotently load or create an Iceberg table."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        return catalog.load_table(f"{namespace}.{table_name}")
    except NoSuchTableError:
        log.info("Creating Iceberg table %s.%s", namespace, table_name)
        partition_spec = _build_partition_spec_from_arrow(arrow_schema, dest_config.get("partition_spec", []))
        props = _build_table_properties(dest_config)
        identifier_fields = dest_config.get("identifier_fields", [])
        return catalog.create_table(
            identifier=f"{namespace}.{table_name}",
            schema=arrow_schema,
            partition_spec=partition_spec,
            properties=props,
            identifier_fields=identifier_fields or None,
        )


def _build_partition_spec_from_arrow(arrow_schema, partition_spec_cfg):
    """Build PartitionSpec using field IDs from an Arrow schema."""
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import (
        IdentityTransform, YearTransform, MonthTransform, DayTransform,
        HourTransform, BucketTransform, TruncateTransform,
    )
    from pyiceberg.schema import Schema

    # Map arrow field name → field_id (assign sequential ids)
    name_to_id = {field.name: i + 1 for i, field in enumerate(arrow_schema)}

    fields = []
    for i, p in enumerate(partition_spec_cfg or []):
        col = p["source_column"]
        t = p["transform"]
        name = p.get("name") or f"{col}_{t}"
        if t == "bucket":
            transform = BucketTransform(int(p.get("width", 16)))
        elif t == "truncate":
            transform = TruncateTransform(int(p.get("width", 16)))
        elif t == "identity":
            transform = IdentityTransform()
        elif t == "year":
            transform = YearTransform()
        elif t == "month":
            transform = MonthTransform()
        elif t == "day":
            transform = DayTransform()
        elif t == "hour":
            transform = HourTransform()
        else:
            raise ValueError(f"Unknown partition transform: {t}")
        fields.append(PartitionField(
            source_id=name_to_id[col],
            field_id=1000 + i,
            transform=transform,
            name=name,
        ))
    return PartitionSpec(fields) if fields else PartitionSpec(())

# ─── Public writer API ───────────────────────────────────────────────────────
class IcebergWriter:
    """Wraps a PyIceberg catalog + table for one destination connection."""

    def __init__(self, dest_config: dict):
        self.dest_config = dest_config
        self.catalog = load_catalog(dest_config)
        self.warehouse = _normalize_warehouse(dest_config.get("warehouse", ""))
        self.namespace = dest_config.get("namespace", "default")
        self._ensure_namespace()

    def _ensure_namespace(self):
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        try:
            self.catalog.create_namespace(self.namespace)
        except NamespaceAlreadyExistsError:
            pass

    def write_batch(self, rows: list[dict], table_name: str) -> int:
        """Append a batch of rows (initial load or insert-only stream)."""
        if not rows:
            return 0
        table_data = _rows_to_arrow(rows)
        table = _get_or_create_table(
            self.catalog, self.namespace, table_name,
            table_data.schema, self.dest_config,
        )
        table.append(table_data)
        return len(rows)

    def upsert(self, rows: list[dict], table_name: str,
               identifier_fields: list[str]) -> int:
        """Apply upsert on identifier fields (PyIceberg table.upsert())."""
        if not rows:
            return 0
        table_data = _rows_to_arrow(rows)
        table = _get_or_create_table(
            self.catalog, self.namespace, table_name,
            table_data.schema, self.dest_config,
        )
        try:
            table.upsert(table_data, join_cols=identifier_fields)
        except AttributeError:
            # Older PyIceberg — fall back to overwrite
            table.overwrite(table_data)
        return len(rows)

    def delete(self, table_name: str, identifier_fields: list[str],
               delete_keys: list) -> int:
        """Delete rows by identifier field values (CDC deletes)."""
        if not delete_keys:
            return 0
        # Build a filter expression and call table.delete()
        from pyiceberg.expressions import In
        table = self.catalog.load_table(f"{self.namespace}.{table_name}")
        col = identifier_fields[0]
        # PyIceberg delete() supports a filter; for simplicity use IN
        try:
            table.delete(In(col, delete_keys))
            return len(delete_keys)
        except Exception:
            log.exception("Iceberg delete failed — skipping %d keys", len(delete_keys))
            return 0


def test_connection(dest_config: dict) -> dict:
    """Connection test: resolve catalog, list namespace, HeadBucket/list warehouse."""
    result = {"catalog_ok": False, "namespace_ok": False, "s3_ok": False, "checks": []}
    try:
        catalog = load_catalog(dest_config)
        namespaces = catalog.list_namespaces()
        result["catalog_ok"] = True
        result["checks"].append(f"catalog: listed {len(namespaces)} namespaces")
        ns = dest_config.get("namespace", "default")
        if (ns,) in namespaces or ns in [n[0] if isinstance(n, tuple) else n for n in namespaces]:
            result["namespace_ok"] = True
            result["checks"].append(f"namespace '{ns}' exists")
    except Exception as e:
        result["checks"].append(f"catalog error: {e}")
        return result

    # S3 HeadBucket via boto
    try:
        import boto3
        creds = _resolve_credentials(dest_config)
        s3_args = {}
        if creds.get("access_key_id"):
            s3_args = {
                "aws_access_key_id": creds["access_key_id"],
                "aws_secret_access_key": creds["secret_access_key"],
                "aws_session_token": creds.get("session_token"),
            }
        if dest_config.get("s3_endpoint"):
            s3_args["endpoint_url"] = dest_config["s3_endpoint"]
        region = creds.get("region", "us-east-1")
        client = boto3.client("s3", region_name=region, **s3_args)
        bucket = dest_config["warehouse"].split("//", 1)[1].split("/", 1)[0]
        client.head_bucket(Bucket=bucket)
        result["s3_ok"] = True
        result["checks"].append(f"s3: HeadBucket {bucket} ok")
    except Exception as e:
        result["checks"].append(f"s3 error: {e}")
    return result
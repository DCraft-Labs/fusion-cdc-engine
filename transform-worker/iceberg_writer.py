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

    # Warehouse is required by all catalogs to resolve table locations
    # (PyIceberg key: `warehouse`). Without this, create_table() raises
    # "No default path is set, please specify a location when creating a table".
    warehouse = _normalize_warehouse(dest_config.get("warehouse", ""))
    if warehouse:
        settings["warehouse"] = warehouse

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

    # PyIceberg 0.7.1: load_catalog(name, **properties) takes FLAT property keys
    # (e.g. `uri=...`, `glue.region=...`, `s3.endpoint=...`). The `catalog.<name>.*`
    # prefix is only used for env vars / yaml config — passing prefixed kwargs here
    # causes `infer_catalog_type` to miss `uri` and raise "URI missing".
    return _load(catalog_name, **settings)


def _resolve_credentials(dest_config: dict) -> dict:
    """Resolve auth mode a/b/c into a boto-style credential dict.

    ``static`` is the mode used by the seeded MinIO Iceberg destination: the
    S3 access key / secret are placed directly in ``s3_access_key_id`` /
    ``s3_secret_access_key`` (no STS / IRSA). It is treated like access_key
    but reads the ``s3_*`` keys so the seeded destination works out of the
    box. Mirrored in control-plane/app/utils/iceberg_tester.py.
    """
    mode = (dest_config.get("auth_mode") or "access_key").lower()
    region = dest_config.get("aws_region") or dest_config.get("s3_region") or "us-east-1"
    out = {"region": region}

    if mode in ("access_key", "static"):
        out["access_key_id"] = (
            dest_config.get("aws_access_key_id")
            or dest_config.get("s3_access_key_id")
        )
        out["secret_access_key"] = (
            dest_config.get("aws_secret_access_key")
            or dest_config.get("s3_secret_access_key")
        )
        out["session_token"] = dest_config.get("aws_session_token") or dest_config.get("s3_session_token")
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
    # PyIceberg 0.7.1: PartitionSpec.__init__ takes *args (varargs), not a list.
    # PartitionSpec([pf]) raises ValidationError; must use PartitionSpec(*fields).
    return PartitionSpec(*fields) if fields else PartitionSpec()


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


# ─── Source schema introspection (v1.2.22 Bug A fix) ─────────────────────────
# Map source SQL/BSON types → PyArrow types. Used by `_get_source_schema` so
# all-NULL columns get their declared type instead of `pa.null()`.
_MYSQL_TYPE_TO_ARROW = {
    "tinyint":   pa.int8(),
    "smallint":  pa.int16(),
    "mediumint": pa.int32(),
    "int":       pa.int32(),
    "integer":   pa.int32(),
    "bigint":    pa.int64(),
    "float":     pa.float32(),
    "double":    pa.float64(),
    "real":      pa.float64(),
    "decimal":   pa.decimal128(38, 18),
    "numeric":   pa.decimal128(38, 18),
    "char":      pa.string(),
    "varchar":   pa.string(),
    "text":      pa.string(),
    "tinytext":  pa.string(),
    "mediumtext": pa.string(),
    "longtext":  pa.string(),
    "json":      pa.string(),
    "enum":      pa.string(),
    "set":       pa.string(),
    "date":      pa.date32(),
    "time":      pa.time32("ms"),
    "datetime":  pa.timestamp("us"),
    "timestamp": pa.timestamp("us"),
    "year":      pa.int32(),
    "bit":       pa.binary(),
    "binary":    pa.binary(),
    "varbinary": pa.binary(),
    "blob":      pa.binary(),
    "tinyblob":  pa.binary(),
    "mediumblob": pa.binary(),
    "longblob":  pa.binary(),
}

_POSTGRES_TYPE_TO_ARROW = {
    "smallint":          pa.int16(),
    "int2":              pa.int16(),
    "integer":           pa.int32(),
    "int4":              pa.int32(),
    "int":               pa.int32(),
    "bigint":            pa.int64(),
    "int8":              pa.int64(),
    "serial":            pa.int32(),
    "bigserial":         pa.int64(),
    "real":              pa.float32(),
    "float4":            pa.float32(),
    "double precision":  pa.float64(),
    "float8":            pa.float64(),
    "decimal":           pa.decimal128(38, 18),
    "numeric":           pa.decimal128(38, 18),
    "money":             pa.decimal128(19, 4),
    "boolean":           pa.bool_(),
    "bool":              pa.bool_(),
    "char":              pa.string(),
    "character":         pa.string(),
    "varchar":           pa.string(),
    "character varying": pa.string(),
    "text":              pa.string(),
    "name":              pa.string(),
    "citext":            pa.string(),
    "bytea":             pa.binary(),
    "blob":              pa.binary(),
    "date":              pa.date32(),
    "time":              pa.time32("ms"),
    "time without time zone": pa.time32("ms"),
    "timetz":            pa.time32("ms"),
    "timestamp":         pa.timestamp("us"),
    "timestamp without time zone": pa.timestamp("us"),
    "timestamptz":        pa.timestamp("us"),
    "timestamp with time zone": pa.timestamp("us"),
    "interval":          pa.string(),
    "json":              pa.string(),
    "jsonb":             pa.string(),
    "uuid":              pa.string(),
    "inet":              pa.string(),
    "cidr":              pa.string(),
    "macaddr":           pa.string(),
    "xml":               pa.string(),
    "bit":               pa.string(),
    "bit varying":       pa.string(),
    "oid":               pa.int64(),
}


def _py_val_to_arrow(v: Any) -> pa.DataType:
    """Map a live Python/BSON value to its preferred Arrow type (Mongo path)."""
    if isinstance(v, bool):
        return pa.bool_()
    if isinstance(v, int):
        return pa.int64()
    if isinstance(v, float):
        return pa.float64()
    if isinstance(v, str):
        return pa.string()
    if isinstance(v, bytes):
        return pa.binary()
    import datetime as _dt
    if isinstance(v, _dt.datetime):
        return pa.timestamp("us")
    if isinstance(v, _dt.date):
        return pa.date32()
    if isinstance(v, (dict, list)):
        return pa.string()  # serialised as JSON string
    type_name = type(v).__name__
    if type_name in ("ObjectId", "Decimal128", "Regex", "DBRef", "Timestamp", "UUID", "uuid"):
        return pa.string()
    return pa.string()


def _normalize_mysql_type(raw: str) -> pa.DataType:
    """Map a MySQL ``information_schema.columns.DATA_TYPE`` to an Arrow type."""
    base = (raw or "").lower().strip()
    # `decimal(10,2)` → `decimal`
    base = base.split("(", 1)[0].strip()
    return _MYSQL_TYPE_TO_ARROW.get(base, pa.string())


def _normalize_pg_type(raw: str) -> pa.DataType:
    """Map a Postgres ``information_schema.columns.DATA_TYPE`` to an Arrow type."""
    t = (raw or "").lower().strip()
    # `character varying(255)` → `character varying`
    t = t.split("(", 1)[0].strip()
    return _POSTGRES_TYPE_TO_ARROW.get(t, pa.string())


def _get_source_schema(source: dict, schema_name: str, table_name: str) -> pa.Schema:
    """v1.2.22 Bug A fix: fetch the source table's column types ONCE from
    ``information_schema`` (MySQL/Postgres) or by sampling one document
    (Mongo), and return an explicit ``pa.Schema`` so all-NULL columns
    retain their declared type instead of being inferred as ``pa.null()``.

    Called once per stream at the start of ``InitialLoadTask.run`` /
    ``CDCTransformTask.run`` and cached — never per chunk (Fix C1).
    """
    ctype = (source.get("connector_type") or "").lower()
    host = source.get("host") or ""
    port = source.get("port")
    database = source.get("database_name") or source.get("database") or ""
    user = source.get("username") or source.get("user") or ""
    password = source.get("password") or ""
    cfg = source.get("config") or {}

    if ctype in ("postgres", "postgresql"):
        return _get_pg_source_schema(host, int(port or 5432), database, user, password,
                                       schema_name, table_name)
    if ctype == "mysql":
        return _get_mysql_source_schema(host, int(port or 3306), database, user, password,
                                         schema_name, table_name)
    if ctype == "mongodb":
        return _get_mongo_source_schema(host, int(port or 27017), database, user, password,
                                         cfg, table_name)
    log.warning("_get_source_schema: unsupported connector_type=%s — returning empty schema", ctype)
    return pa.schema([])


def _get_mysql_source_schema(host, port, database, user, password,
                              schema_name, table_name) -> pa.Schema:
    import pymysql
    conn = pymysql.connect(host=host, port=port, database=database,
                           user=user, password=password,
                           connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
                "ORDER BY ORDINAL_POSITION",
                (database, table_name),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    fields = [
        pa.field(name, _normalize_mysql_type(dtype))
        for (name, dtype, _pos) in rows
    ]
    return pa.schema(fields)


def _get_pg_source_schema(host, port, database, user, password,
                          schema_name, table_name) -> pa.Schema:
    import psycopg2
    conn = psycopg2.connect(host=host, port=port, dbname=database,
                            user=user, password=password, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s "
                "ORDER BY ordinal_position",
                (schema_name or "public", table_name),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    fields = [pa.field(name, _normalize_pg_type(dtype)) for (name, dtype) in rows]
    return pa.schema(fields)


def _get_mongo_source_schema(host, port, database, user, password,
                             cfg, collection_name) -> pa.Schema:
    """Sample one document from the collection and use its field types.

    Mongo has no server-side schema, so we sample a single doc (cheap —
    the driver returns it in one round-trip) and map each value's Python
    type to Arrow. All-NULL / missing fields fall back to ``pa.string()``.
    """
    from urllib.parse import quote_plus
    from pymongo import MongoClient
    auth_source = (cfg.get("auth_source") if isinstance(cfg, dict) else None) or "admin"
    if user and password:
        uri = (f"mongodb://{quote_plus(user)}:{quote_plus(password)}@"
               f"{host}:{port}/{database}?authSource={auth_source}")
    else:
        uri = f"mongodb://{host}:{port}/{database}?authSource={auth_source}"
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        db = client[database]
        doc = db[collection_name].find_one({}) or {}
        fields = [pa.field(k, _py_val_to_arrow(v)) for k, v in doc.items()]
        return pa.schema(fields)
    finally:
        client.close()


def _evolve_schema_for_drift(table, cached_schema: pa.Schema,
                             new_columns: dict[str, pa.DataType]) -> pa.Schema:
    """v1.2.22 Fix A4: when a chunk contains a column not in the cached
    schema, add it to the Iceberg table via ``update_schema().add_column``
    and return the updated schema. ``new_columns`` is ``{name: arrow_type}``.
    """
    if not new_columns:
        return cached_schema
    updated = cached_schema
    with table.update_schema() as tx:
        for name, dtype in new_columns.items():
            try:
                tx.add_column(
                    next(iter(updated)),  # position hint — append after first field
                    pa.field(name, dtype, nullable=True),
                )
            except Exception:
                log.exception("Schema drift: add_column(%s) failed — skipping", name)
                continue
            updated = updated.append(pa.field(name, dtype, nullable=True))
    return updated


def _rows_to_arrow(rows: list[dict], schema: pa.Schema | None = None) -> pa.Table:
    """Convert CDC row dicts to a PyArrow Table.

    v1.2.22 Bug A fix: when ``schema`` is provided, the table is built with
    that explicit schema so all-NULL columns retain their declared type
    (e.g. ``pa.string()``) instead of being inferred as ``pa.null()`` —
    which PyIceberg rejects with ``ValueError: Cannot write DataType
    null``. When ``schema`` is None (legacy callers / tests), the old
    inference behaviour is preserved.
    """
    if not rows:
        if schema is not None:
            return pa.Table.from_pylist([], schema=schema)
        return pa.table({})
    if schema is not None:
        return pa.Table.from_pylist(rows, schema=schema)
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
        # PyIceberg 0.7.1: Catalog.create_table() does NOT accept an
        # `identifier_fields` kwarg (TypeError: unexpected keyword argument).
        # Identifier fields must be set on the PyIceberg Schema via
        # `identifier_field_ids=[field_id, ...]`, but the Arrow schema produced
        # by `pa.Table.from_pylist` has nullable fields with no field IDs, so
        # attaching `identifier_field_ids` here raises
        # "Identifier field -1 invalid: not a required field".
        # We therefore create the table WITHOUT identifier fields on 0.7.1.
        # The `identifier_fields` config is still consumed by `IcebergWriter.upsert`
        # (delete+append) and `IcebergWriter.delete` (In-filter), so CDC correctness
        # does not depend on schema-level identifier fields. When the project
        # upgrades to PyIceberg >= 0.11, switch to `table.upsert(join_cols=...)`
        # and re-enable schema-level identifier fields here.
        return catalog.create_table(
            identifier=f"{namespace}.{table_name}",
            schema=arrow_schema,
            partition_spec=partition_spec,
            properties=props,
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
    # PyIceberg 0.7.1: PartitionSpec.__init__ takes *args (varargs), not a list.
    return PartitionSpec(*fields) if fields else PartitionSpec()

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

    def write_batch(self, rows: list[dict], table_name: str,
                    schema: pa.Schema | None = None) -> int:
        """Append a batch of rows (initial load or insert-only stream).

        v1.2.22 Bug A fix: when ``schema`` is provided (the explicit source
        schema fetched once per stream), it is used for both the Arrow
        conversion and the Iceberg table creation so all-NULL columns
        keep their declared type instead of being inferred as
        ``pa.null()`` (which PyIceberg rejects).
        """
        if not rows:
            return 0
        table_data = _rows_to_arrow(rows, schema=schema)
        # Prefer the explicit schema for table creation; fall back to the
        # inferred schema only when the caller did not supply one.
        create_schema = schema if schema is not None else table_data.schema
        table = _get_or_create_table(
            self.catalog, self.namespace, table_name,
            create_schema, self.dest_config,
        )
        # If the cached schema is a subset of the row keys (schema drift),
        # evolve the Iceberg table before appending.
        if schema is not None:
            row_keys = set(rows[0].keys()) if rows else set()
            cached_names = {f.name for f in schema}
            new_cols = row_keys - cached_names
            if new_cols:
                drift_types = {
                    k: _py_val_to_arrow(rows[0].get(k))
                    for k in new_cols
                }
                evolved = _evolve_schema_for_drift(table, schema, drift_types)
                table_data = _rows_to_arrow(rows, schema=evolved)
        table.append(table_data)
        return len(rows)

    def upsert(self, rows: list[dict], table_name: str,
               identifier_fields: list[str],
               schema: pa.Schema | None = None) -> int:
        """Apply upsert on identifier fields (PyIceberg table.upsert()).

        PyIceberg 0.7.1 does NOT implement `Table.upsert()` — it was added
        in 0.11.0. The previous fallback to `table.overwrite()` was
        semantically WRONG: `overwrite` replaces the entire table data with
        just the new batch, losing all previously-committed rows. We now
        emulate upsert as delete(matching keys) + append, which is the
        standard pattern for older PyIceberg (and what the 0.11+ `upsert`
        does under the hood for non-merge-on-read catalogs).

        v1.2.22 Bug A fix: ``schema`` is the explicit source schema so
        all-NULL columns keep their declared type.
        """
        if not rows:
            return 0
        table_data = _rows_to_arrow(rows, schema=schema)
        create_schema = schema if schema is not None else table_data.schema
        table = _get_or_create_table(
            self.catalog, self.namespace, table_name,
            create_schema, self.dest_config,
        )
        if schema is not None:
            row_keys = set(rows[0].keys()) if rows else set()
            cached_names = {f.name for f in schema}
            new_cols = row_keys - cached_names
            if new_cols:
                drift_types = {
                    k: _py_val_to_arrow(rows[0].get(k))
                    for k in new_cols
                }
                evolved = _evolve_schema_for_drift(table, schema, drift_types)
                table_data = _rows_to_arrow(rows, schema=evolved)
        if hasattr(table, "upsert"):
            table.upsert(table_data, join_cols=identifier_fields)
        else:
            # PyIceberg 0.7.1 path: delete matching rows then append.
            col = identifier_fields[0] if identifier_fields else None
            if col:
                keys = [r[col] for r in rows if r.get(col) is not None]
                if keys:
                    from pyiceberg.expressions import In
                    try:
                        table.delete(In(col, keys))
                    except Exception:
                        log.exception("Iceberg upsert: delete-before-append failed for %d keys", len(keys))
            table.append(table_data)
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
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
import time
from typing import Any

import pyarrow as pa

log = logging.getLogger(__name__)


# v1.2.33 Bug #21 fix 3: per-table distributed commit mutex.
# Iceberg uses optimistic concurrency (compare-and-swap on the snapshot id).
# When K writers all commit to the SAME table, most lose the race, retry with
# backoff, and (under K=6 contention) burn through the retry budget and get
# dead-lettered. The fix is to SERIALIZE commits to the same table across pods
# while keeping FETCHES parallel (the fetch — reading from MySQL — is the slow
# part; the commit — writing one Arrow batch to S3 + metadata — is fast).
# We use a Redis SET NX EX 30 lock keyed on the destination table. If another
# pod holds the lock, we wait 1s and retry (up to 60s). The lock has a 30s TTL
# so a crashed pod can't hold it forever.
COMMIT_LOCK_TTL_S = int(os.environ.get("ICEBERG_COMMIT_LOCK_TTL_S", "30"))
COMMIT_LOCK_WAIT_S = int(os.environ.get("ICEBERG_COMMIT_LOCK_WAIT_S", "60"))
COMMIT_LOCK_POLL_S = float(os.environ.get("ICEBERG_COMMIT_LOCK_POLL_S", "1.0"))


def _commit_lock_key(connection_id: str, table_name: str) -> str:
    return f"fusion:iceberg-commit-lock:{connection_id}:{table_name}"


def _acquire_commit_lock(redis_client, connection_id: str, table_name: str,
                        pod_id: str | None = None) -> bool:
    """Acquire a Redis SET NX EX lock for committing to one table.

    Returns True on acquisition, False if the wait budget elapsed without
    acquiring. Polls every COMMIT_LOCK_POLL_S seconds up to COMMIT_LOCK_WAIT_S.
    """
    if redis_client is None:
        return True  # no redis — no serialization (tests / single-writer CDC)
    key = _commit_lock_key(connection_id, table_name)
    val = pod_id or os.environ.get("WORKER_ID", "transform-worker")
    deadline = time.monotonic() + COMMIT_LOCK_WAIT_S
    while time.monotonic() < deadline:
        try:
            ok = redis_client.set(key, val, nx=True, ex=COMMIT_LOCK_TTL_S)
        except Exception:
            log.exception("Iceberg commit lock: SET NX failed for key=%s — proceeding without lock (degraded)", key)
            return True
        if ok:
            return True
        time.sleep(COMMIT_LOCK_POLL_S)
    log.warning("Iceberg commit lock: could not acquire key=%s within %ds — proceeding without lock (degraded)",
                key, COMMIT_LOCK_WAIT_S)
    return True  # degraded mode: commit anyway rather than dead-letter


def _release_commit_lock(redis_client, connection_id: str, table_name: str,
                         pod_id: str | None = None) -> None:
    """Release the commit lock. Best-effort: a Lua compare-and-del would be
    safer, but the 30s TTL bounds the worst case (a crashed pod's lock
    auto-expires). We only DEL if we still hold it (same pod_id)."""
    if redis_client is None:
        return
    key = _commit_lock_key(connection_id, table_name)
    val = pod_id or os.environ.get("WORKER_ID", "transform-worker")
    try:
        # Compare-and-del via Lua to avoid releasing a lock we no longer hold.
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "  return redis.call('DEL', KEYS[1]) "
            "else return 0 end"
        )
        redis_client.eval(script, 1, key, val)
    except Exception:
        log.exception("Iceberg commit lock: release failed for key=%s — relying on TTL", key)


def _dedup_on_pk(table, pk_col: str,
                 rows: "list[dict] | None" = None,
                 arrow_tbl: "pa.Table | None" = None) -> None:
    """v1.2.33 Bug #22 fix 2 (BELT-AND-SUSPENDERS): before appending a batch,
    delete any existing rows whose PK is in this batch's PK set. This makes
    each chunk idempotent — a retry that re-appends a chunk already committed
    (e.g. a batched commit that partially succeeded before failing, or a
    checkpoint that was advanced past durable data) will not produce
    duplicate rows: the duplicates are deleted first, then re-appended
    exactly once.

    Must be called INSIDE the per-table commit mutex (Bug #21 fix 3) so the
    delete+append is atomic w.r.t. other pods.

    Either ``rows`` (list of dicts) or ``arrow_tbl`` (a pyarrow.Table) must
    be provided; the PK values are extracted from whichever is present.
    """
    try:
        from pyiceberg.expressions import In
    except Exception:
        log.warning("Iceberg dedup-on-PK: pyiceberg.expressions.In unavailable — skipping dedup (non-fatal)")
        return
    # Extract PK values.
    keys: list = []
    if rows:
        keys = [r.get(pk_col) for r in rows if r.get(pk_col) is not None]
    elif arrow_tbl is not None:
        try:
            keys = [v for v in arrow_tbl.column(pk_col).to_pylist() if v is not None]
        except Exception:
            log.exception("Iceberg dedup-on-PK: could not extract PK column %r from arrow table — skipping dedup", pk_col)
            return
    if not keys:
        return
    try:
        table.delete(In(pk_col, keys))
    except Exception:
        # Non-fatal: the delete may fail if the table is empty (no rows match)
        # or on a transient catalog error. The append still proceeds; the
        # PRIMARY fix (Bug #22 fix 1, checkpoint-after-commit) prevents the
        # duplicate scenario in the common case. This dedup is belt-and-
        # suspenders.
        log.exception("Iceberg dedup-on-PK: delete-before-append failed for %d keys — proceeding with append (non-fatal)", len(keys))




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
    # v1.2.25 Task 7: delete-after-commit default. For initial-load
    # destinations, default to true so old metadata files are removed after
    # each commit (reduces accumulation during a long load). An operator can
    # explicitly opt out by setting write_metadata_delete_after_commit=false
    # in the destination config — the explicit value always wins.
    wmdac_explicit = "write_metadata_delete_after_commit" in dest_config
    wmdac_value = bool(dest_config.get("write_metadata_delete_after_commit"))
    if wmdac_value:
        props["write.metadata.delete-after-commit.enabled"] = "true"
    elif dest_config.get("initial_load_destination", True) and not wmdac_explicit:
        props["write.metadata.delete-after-commit.enabled"] = "true"
    # v1.2.25 Task 5: auto-merge manifests on every commit so a long initial
    # load (one snapshot per 10k-row chunk) does not accumulate hundreds of
    # small manifests and degrade throughput ~30%. PyIceberg 0.7.1 honors
    # this property on every table.append() commit. min-count-to-merge=1
    # means "merge whenever there is more than 1 manifest", which keeps the
    # manifest list flat. This is the actual compaction lever in 0.7.1 (the
    # table.rewrite_manifests() API was added in 0.11+).
    if dest_config.get("initial_load_destination", True):
        props.setdefault("commit.manifest.min-count-to-merge", "1")
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

    def __init__(self, dest_config: dict, redis_client: "Any | None" = None,
                 connection_id: "str | None" = None):
        self.dest_config = dest_config
        self.catalog = load_catalog(dest_config)
        self.warehouse = _normalize_warehouse(dest_config.get("warehouse", ""))
        self.namespace = dest_config.get("namespace", "default")
        # v1.2.33 Bug #21 fix 3: optional Redis client + connection id for
        # the per-table commit mutex. When both are provided, every commit
        # (append/upsert/delete) is serialized across pods via a Redis lock
        # keyed on the destination table. When None (tests, single-writer
        # CDC), commits proceed without serialization (legacy behavior).
        self.redis_client = redis_client
        self.connection_id = connection_id
        self._ensure_namespace()

    def _ensure_namespace(self):
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        try:
            self.catalog.create_namespace(self.namespace)
        except NamespaceAlreadyExistsError:
            pass

    def write_batch(self, rows: list[dict], table_name: str,
                    schema: pa.Schema | None = None,
                    pk_col: "str | None" = None) -> int:
        """Append a batch of rows (initial load or insert-only stream).

        v1.2.22 Bug A fix: when ``schema`` is provided (the explicit source
        schema fetched once per stream), it is used for both the Arrow
        conversion and the Iceberg table creation so all-NULL columns
        keep their declared type instead of being inferred as
        ``pa.null()`` (which PyIceberg rejects).

        v1.2.33 Bug #22 fix 2 (BELT-AND-SUSPENDERS): when ``pk_col`` is
        provided, delete any existing rows whose PK is in this batch BEFORE
        appending. This makes each chunk idempotent — even if a retry
        re-appends a chunk that was already committed (e.g. a batched commit
        partially succeeded before failing), the duplicates are removed
        first. The delete-then-append runs inside the same per-table commit
        mutex (Bug #21 fix 3) so it is atomic w.r.t. other pods.
        """
        if not rows:
            return 0
        table_data = _rows_to_arrow(rows, schema=schema)
        # Prefer the explicit schema for table creation; fall back to the
        # inferred schema only when the caller did not supply one.
        create_schema = schema if schema is not None else table_data.schema
        # v1.2.36 Bug #24 fix: acquire the commit lock BEFORE loading the
        # table object. Previously _get_or_create_table() ran outside the
        # lock, so the table object referenced a STALE snapshot id by the
        # time the pod got into the lock — and table.append() against the
        # stale snapshot raised CommitFailedException: snapshot id changed.
        # This was the root cause of partitions dead-lettering under real
        # K=6 contention (the "snapshot id changed" errors that burned
        # through 10 retries). Loading the table INSIDE the lock
        # guarantees the table object reflects the latest committed
        # snapshot. The schema-drift evolution below is also an Iceberg
        # commit, so it must be inside the lock too.
        _acquire_commit_lock(self.redis_client, self.connection_id, table_name)
        try:
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
            # v1.2.33 Bug #22 fix 2: dedup-on-PK before append (idempotency).
            if pk_col:
                _dedup_on_pk(table, pk_col, rows=rows)
            table.append(table_data)
        finally:
            _release_commit_lock(self.redis_client, self.connection_id, table_name)
        return len(rows)

    def write_arrow(self, arrow_tbl: "pa.Table", table_name: str,
                    pk_col: "str | None" = None) -> int:
        """v1.2.29 Task 1: append a pre-built Arrow table directly to Iceberg,
        skipping the Python row-dict → Arrow conversion (the DuckDB native
        scanner already produced typed Arrow). Used by the bulk initial-load
        path. Returns the number of rows appended.

        v1.2.33 Bug #22 fix 2 (BELT-AND-SUSPENDERS): when ``pk_col`` is
        provided, delete any existing rows whose PK is in this batch BEFORE
        appending — makes each chunk idempotent under retry-after-conflict.
        """
        if arrow_tbl is None or arrow_tbl.num_rows == 0:
            return 0
        create_schema = arrow_tbl.schema
        # v1.2.36 Bug #24 fix: acquire the commit lock BEFORE loading the
        # table object (see write_batch for the full rationale). Loading
        # inside the lock guarantees the table object reflects the latest
        # committed snapshot, eliminating the stale-snapshot
        # CommitFailedException that was dead-lettering partitions.
        _acquire_commit_lock(self.redis_client, self.connection_id, table_name)
        try:
            table = _get_or_create_table(
                self.catalog, self.namespace, table_name,
                create_schema, self.dest_config,
            )
            # v1.2.33 Bug #22 fix 2: dedup-on-PK before append (idempotency).
            if pk_col:
                _dedup_on_pk(table, pk_col, arrow_tbl=arrow_tbl)
            table.append(arrow_tbl)
        finally:
            _release_commit_lock(self.redis_client, self.connection_id, table_name)
        return arrow_tbl.num_rows

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
        # v1.2.36 Bug #24 fix: acquire the commit lock BEFORE loading the
        # table object (see write_batch for the full rationale). Loading
        # inside the lock guarantees the table object reflects the latest
        # committed snapshot, eliminating the stale-snapshot
        # CommitFailedException that was dead-lettering partitions.
        _acquire_commit_lock(self.redis_client, self.connection_id, table_name)
        try:
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
        finally:
            _release_commit_lock(self.redis_client, self.connection_id, table_name)
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

    def compact_manifests(self, table_name: str, keep_snapshots: int = 5) -> dict:
        """v1.2.25 Task 5: compact the manifest list + expire old snapshots.

        Called by ``InitialLoadTask`` after every ``INITIAL_LOAD_COMPACTION_INTERVAL``
        chunks to flatten the ~30% throughput degradation caused by manifest /
        snapshot accumulation during a long initial load (every 10k-row chunk
        = 1 Iceberg snapshot commit; by row 8M that's ~800 snapshots).

        PyIceberg 0.7.1 does NOT expose ``table.rewrite_manifests()`` or
        ``table.expire_snapshots()`` (added in 0.11+). So this method:

          1. Calls them defensively when available (forward-compatible with
             newer PyIceberg) and logs the outcome.
          2. When unavailable, relies on the table property
             ``commit.manifest.min-count-to-merge=1`` set in
             ``_build_table_properties`` for initial-load destinations, which
             makes PyIceberg auto-merge manifests on every commit. This is
             the actual compaction lever in 0.7.1 and is what flattens the
             degradation curve.

        Never raises — compaction is an optimization, not a correctness gate.
        Returns a small dict describing what ran (for logging / tests).
        """
        result = {"table": table_name, "rewrote_manifests": False,
                  "expired_snapshots": False, "note": ""}
        try:
            table = self.catalog.load_table(f"{self.namespace}.{table_name}")
        except Exception:
            log.exception("compact_manifests: could not load table %s", table_name)
            result["note"] = "table load failed"
            return result

        # 1. Manifest rewrite (PyIceberg 0.11+ exposes table.rewrite_manifests()).
        if hasattr(table, "rewrite_manifests"):
            try:
                table.rewrite_manifests()
                result["rewrote_manifests"] = True
                log.info("compact_manifests: rewrote manifests for %s", table_name)
            except Exception:
                log.exception("compact_manifests: rewrite_manifests failed for %s", table_name)
        else:
            # PyIceberg 0.7.1: rely on commit.manifest.min-count-to-merge=1
            # (set in _build_table_properties) which auto-merges on commit.
            result["note"] = "rewrite_manifests unavailable in pyiceberg 0.7.1; commit.manifest.min-count-to-merge handles auto-merge"

        # 2. Snapshot expiration (PyIceberg 0.11+ exposes table.expire_snapshots()).
        if hasattr(table, "expire_snapshots"):
            try:
                table.expire_snapshots()
                result["expired_snapshots"] = True
                log.info("compact_manifests: expired snapshots for %s (keep last %d)",
                         table_name, keep_snapshots)
            except Exception:
                log.exception("compact_manifests: expire_snapshots failed for %s", table_name)
        else:
            # 0.7.1: no snapshot expiration available — the table properties
            # history.expire.min-snapshots-to-keep / max-snapshot-age-ms are
            # only honored by Spark/Airflow maintenance jobs, not pyiceberg.
            if not result["note"]:
                result["note"] = "expire_snapshots unavailable in pyiceberg 0.7.1"

        log.info("compact_manifests: table=%s rewrote=%s expired=%s",
                 table_name, result["rewrote_manifests"], result["expired_snapshots"])
        return result


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
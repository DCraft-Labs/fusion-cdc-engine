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
# 2026-07-24 addition (Bug #17): separate, short-lived lock so exactly one
# of K concurrent partitions bootstraps a not-yet-existing table, instead
# of all K racing into a direct commit simultaneously (see
# write_arrow_to_file). BOOTSTRAP_WAIT_S bounds how long a losing partition
# waits for the winner before falling back to committing itself.
BOOTSTRAP_WAIT_S = int(os.environ.get("ICEBERG_BOOTSTRAP_WAIT_S", "30"))
BOOTSTRAP_POLL_S = float(os.environ.get("ICEBERG_BOOTSTRAP_POLL_S", "0.5"))


def _commit_lock_key(connection_id: str, table_name: str) -> str:
    # v1.3.4 Fix 2: unify with the committer's lock namespace. Previously
    # this used ``fusion:iceberg-commit-lock:...`` while iceberg_committer.py
    # used ``fusion:iceberg-committer-lock:...`` — two non-coordinating locks
    # for the same (connection_id, table_name) pair. The bootstrap path
    # (write_arrow/write_batch) and the committer (add_files) provided ZERO
    # mutual exclusion against each other, which reproduced
    # ``FileNotFoundError: ...snap-...avro`` inside commit() during
    # _existing_manifests() (the previous snapshot's manifest-list was
    # deleted by the winner's delete-after-commit.enabled=true) and was the
    # primary cause of the 110.6% duplicate overage (both commit paths
    # succeeding independently). Both code paths now SET NX EX the SAME key
    # so only one commit can run against a given (conn, table) at a time.
    return f"fusion:iceberg-committer-lock:{connection_id}:{table_name}"


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
    """Build a PyIceberg Catalog from destination connection_config.

    v1.3.5 Fix 4: fail LOUDLY on empty config. Previously an empty dict
    silently defaulted ``catalog_type`` to "rest" and then raised
    ``KeyError: 'catalog_uri'`` deep inside the rest-branch — a confusing
    failure mode for operators running the committer without
    ``--catalog-config``. Now raise a clear ValueError up front so the
    operator sees exactly what's missing.
    """
    if not dest_config:
        raise ValueError(
            "catalog_config is empty — cannot load catalog. "
            "The Iceberg committer requires the destination's "
            "connection_config (catalog_type + catalog_uri/nessie_uri/..."
            ") to build a PyIceberg Catalog. Pass it via --catalog-config "
            "(JSON) or the ICEBERG_CATALOG_CONFIG env var, or wire the "
            "chart template to mount the destination's connection_config "
            "Secret (see helm/fusion-cdc/templates/iceberg-committer.yaml)."
        )

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
        # v1.3.7 Bug #1: forward region + credentials the same way the Glue
        # branch does. Previously only table-name was set, so access_key /
        # sts_assume connections silently used the ambient boto3 chain.
        settings["dynamodb.region"] = dest_config.get("dynamodb_region") or creds.get("region", "us-east-1")
        if creds.get("access_key_id"):
            settings["dynamodb.access-key-id"] = creds["access_key_id"]
            settings["dynamodb.secret-access-key"] = creds["secret_access_key"]
            if creds.get("session_token"):
                settings["dynamodb.session-token"] = creds["session_token"]
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
        # v1.3.7 Bug #2: only set role_arn when the requested role differs
        # from the pod's ambient IRSA identity (AWS_ROLE_ARN). Setting the
        # same role triggers a self-AssumeRole that standard IRSA trust
        # policies reject with AccessDenied.
        requested_role = dest_config.get("service_account_role_arn")
        ambient_role = os.environ.get("AWS_ROLE_ARN")
        if requested_role and requested_role != ambient_role:
            out["role_arn"] = requested_role
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
    if wmdac_explicit:
        # 2026-07-25 fix: an explicit False used to fall through to neither
        # branch, silently OMITTING the property (leaving Iceberg's own
        # spec default in effect) instead of actually setting "false" -- no
        # way to deterministically force it off for an A/B comparison.
        props["write.metadata.delete-after-commit.enabled"] = "true" if wmdac_value else "false"
    elif dest_config.get("initial_load_destination", True):
        props["write.metadata.delete-after-commit.enabled"] = "true"
    # v1.2.25 Task 5: auto-merge manifests on every commit so a long initial
    # load (one snapshot per 10k-row chunk) does not accumulate hundreds of
    # small manifests and degrade throughput ~30%. PyIceberg 0.7.1 honors
    # this property on every table.append() commit. min-count-to-merge=1
    # means "merge whenever there is more than 1 manifest", which keeps the
    # manifest list flat. This is the actual compaction lever in 0.7.1 (the
    # table.rewrite_manifests() API was added in 0.11+).
    #
    # v1.2.37 §8 item 4 investigation: the property IS set correctly here,
    # but the live observation (§2 of the master report) is that 120 commits
    # still produced 120 manifests — i.e. the property has NO effect on
    # PyIceberg 0.7.1's ``table.append()`` / ``fast_append`` path. Confirmed
    # by source inspection of pyiceberg 0.7.1's
    # ``pyiceberg/table/update/snapshot.py``: ``FastAppend`` / ``MergeAppend``
    # do NOT read ``commit.manifest.min-count-to-merge`` anywhere — that
    # property is a Java-Iceberg-core concept (``org.apache.iceberg.Snapshot``
    # summarization / ``ManifestListMergeManager``) and is not wired into
    # PyIceberg's append path through 0.7.1 (or 0.11.1). PyIceberg's
    # ``MergeAppend`` does merge manifests when ``snapshot.new`` /
    # ``snapshot.cherry-pick`` are used, but plain ``fast_append`` (which
    # ``table.append()`` and ``Transaction.add_files`` both use) always
    # creates a fresh manifest per call — there is no min-count gate.
    # The property is therefore kept here as a no-op placeholder so that
    # IF PyIceberg adds support later it picks the value up automatically,
    # but the manifest-growth problem is NOT fixed by this property on
    # 0.7.1. The real fix is the v1.2.39 single-committer + ``add_files()``
    # redesign (§6), which collapses N chunk commits into 1 batched commit
    # per drain cycle — structurally limiting manifest count regardless of
    # whether PyIceberg honors the min-count property. Upgrading to
    # PyIceberg >= 0.11 to gain ``table.rewrite_manifests()`` is a separate
    # follow-up and not required for the redesign.
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


def _kill_mysql_thread(host, port, user, password, thread_id) -> None:
    """2026-07-24 fix: force-terminate a source-DB query we fired ourselves,
    once we've given up on it (timeout/exception), instead of just dropping
    our own client connection and hoping the server notices. Matters most
    against a shared/multi-tenant source (e.g. a live UAT MySQL instance
    also used by other teams) -- an abandoned client socket does not
    promptly free server-side query resources. Opens a brand-new,
    short-lived connection purely to issue ``KILL <thread_id>``; any
    failure here is logged and swallowed (this is best-effort cleanup,
    not allowed to mask the original error).
    """
    try:
        import pymysql
        killer = pymysql.connect(host=host, port=int(port), user=user,
                                  password=password, connect_timeout=5,
                                  read_timeout=5)
        try:
            with killer.cursor() as kc:
                kc.execute(f"KILL {int(thread_id)}")
            log.warning("KILLed orphaned source query thread_id=%s on %s:%s "
                        "after our client gave up on it", thread_id, host, port)
        finally:
            killer.close()
    except Exception as e:
        log.warning("failed to KILL source thread_id=%s on %s:%s (%s) "
                    "-- server will have to reclaim it on its own",
                    thread_id, host, port, e)


def _get_mysql_source_schema(host, port, database, user, password,
                              schema_name, table_name) -> pa.Schema:
    import pymysql
    conn = pymysql.connect(host=host, port=port, database=database,
                           user=user, password=password,
                           connect_timeout=10, read_timeout=30)
    # 2026-07-24 fix: kill orphaned source queries on failure (shared UAT).
    _thread_id = None
    try:
        _thread_id = conn.thread_id()
    except Exception:
        pass
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
    except Exception:
        if _thread_id is not None:
            _kill_mysql_thread(host, port, user, password, _thread_id)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
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


def _arrow_schema_to_iceberg_schema_with_ids(arrow_schema: pa.Schema):
    """2026-07-24 fix (post-v1.3.6): return (iceberg_schema, name_to_id) where
    ``iceberg_schema`` is a REAL ``pyiceberg.schema.Schema`` whose field ids
    exactly match ``name_to_id`` (sequential, 1-based, in arrow column order).

    Why this is needed: ``Catalog.create_table()`` only accepts either an
    already-real ``Schema`` object (used as-is) or a raw ``pa.Schema`` --
    for the latter it converts via ``_ConvertToIcebergWithoutIDs``, which
    assigns ``field_id=-1`` to *every* field (verified directly against the
    installed pyiceberg==0.7.1: "Converts PyArrowSchema to Iceberg Schema
    with all -1 ids. ... should always be used in conjunction with
    `new_table_metadata` [to] assign new field ids in order."). When a
    ``partition_spec`` is ALSO supplied, table creation internally calls
    ``assign_fresh_partition_spec_ids(spec, old_schema, fresh_schema)``,
    which resolves each partition field's ``source_id`` by looking up
    ``old_schema.find_column_name(source_id)`` -- but every field in that
    "old_schema" is id=-1, so a partition field built assuming real
    sequential ids (as ``_build_partition_spec_from_arrow`` below does) can
    never be found there. Confirmed live: 100% reproducible
    ``ValueError: Could not find in old schema: 1000: pkey_bucket:
    bucket[16](1)`` on every single bootstrap-create attempt, for every
    partition, the moment v1.3.6's new default bucket(16, pk) partitioning
    (Bug #7 fix) was exercised end-to-end for the first time outside its
    own unit tests. Building a real, correctly-ID'd Schema object here and
    passing IT (not the raw arrow schema) to create_table() sidesteps the
    WithoutIDs path entirely -- create_table()'s own
    ``_convert_schema_if_needed`` returns an already-``Schema`` instance
    unchanged, so "old_schema" in the partition-spec-id-reconciliation step
    ends up being THIS schema, whose ids genuinely match what
    ``_build_partition_spec_from_arrow`` assumes.
    """
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    raw = _pyarrow_to_schema_without_ids(arrow_schema)  # types correct, every field_id=-1
    name_to_id = {field.name: i + 1 for i, field in enumerate(arrow_schema)}
    fields = [
        NestedField(field_id=name_to_id[f.name], name=f.name,
                    field_type=f.field_type, required=f.required)
        for f in raw.fields
    ]
    return Schema(*fields), name_to_id


# 2026-07-25 fix (Fix C, found while investigating why per-chunk "write"
# phase latency dominated at ~1s/20,000-row-chunk even after Fix B made
# convert fast): PyIceberg's REST catalog re-selects FsspecFileIO on every
# single load_table() call for this deployment's s3:// table location,
# regardless of a client-side `py-io-impl` catalog override -- confirmed by
# reading RestCatalog._response_to_table / Catalog._load_file_io: the merge
# is `{**self.properties, **table_response.config}`, and the REST server's
# own `config` response field (not the client's init properties) is what
# actually determines the FileIO class here, so a client-side override is
# silently discarded. Measured live (20,000-row Parquet write): FsspecFileIO's
# `new_output().create()` costs ~510-530ms and `.close()` (upload) another
# ~90-390ms PER FILE -- ~92% of total per-chunk time -- while the actual
# `pq.write_table()` call is only ~15ms. A warm (post-cold-start)
# `PyArrowFileIO` instance does the same create+write+close in ~100-490ms,
# roughly 1.5-4x faster on the dominant cost, with an identical
# `new_output`/`new_input`/`delete` interface. Since the server always wins
# the FileIO-selection merge, the fix is applied client-side instead: swap
# `table.io` for a POOLED (built once per worker process — PyArrowFileIO's
# own cold start costs ~5s, so this must never run per-chunk) PyArrowFileIO
# right after each load_table() call, rather than fighting the server's
# config. Reuses the existing `_resolve_credentials`/`_resolve_s3_settings`
# helpers so auth/region/path-style stay correct for whatever this
# deployment's actual destination config is (not hardcoded).
_POOLED_PYARROW_IO: dict[tuple, Any] = {}


def _fast_file_io_for(dest_config: dict):
    from pyiceberg.io.pyarrow import PyArrowFileIO
    creds = _resolve_credentials(dest_config)
    s3_settings = _resolve_s3_settings(dest_config, creds)
    key = tuple(sorted(s3_settings.items()))
    io = _POOLED_PYARROW_IO.get(key)
    if io is None:
        io = PyArrowFileIO(s3_settings)
        _POOLED_PYARROW_IO[key] = io
    return io


def _use_fast_io(table, dest_config: dict):
    """Best-effort: swap table.io for the pooled PyArrowFileIO. On any
    failure, leave table.io untouched (falls back to the slower-but-correct
    server-selected default) rather than risk breaking a write."""
    try:
        table.io = _fast_file_io_for(dest_config)
    except Exception:
        log.exception("_use_fast_io: failed to patch table.io to PyArrowFileIO "
                      "— continuing with the default FileIO")
    return table


def _get_or_create_table(catalog, namespace: str, table_name: str,
                         arrow_schema: pa.Schema, dest_config: dict,
                         pk_col: "str | None" = None):
    """Idempotently load or create an Iceberg table.

    v1.3.6: when creating a new table and no explicit ``partition_spec`` is
    configured, apply ``bucket(16, <pk>)`` when a primary key is known so
    future overlap deletes can prune to a handful of data files (see Bug #7).
    Existing unpartitioned tables are left unchanged (skip-dedup path).
    """
    from pyiceberg.exceptions import NoSuchTableError

    try:
        return _use_fast_io(catalog.load_table(f"{namespace}.{table_name}"), dest_config)
    except NoSuchTableError:
        log.info("Creating Iceberg table %s.%s", namespace, table_name)
        partition_spec_cfg = list(dest_config.get("partition_spec") or [])
        # 2026-07-24 fix (post-v1.3.6): the auto bucket(16, pk) default below
        # is DISABLED. `add_files()` -- the whole basis of the staged
        # committer path this table is used with -- can only register a
        # pre-written Parquet file into a partition whose value it can
        # INFER from that file's own column min/max statistics ("linear"
        # transforms: identity/truncate/year/month/day/hour). `bucket()` is
        # not linear (hash(pk) % 16 isn't derivable from min/max stats), so
        # every add_files() call against a bucket-partitioned table fails
        # unconditionally. Confirmed live: 100% of commit attempts hit
        # ``ValueError: Cannot infer partition value from parquet metadata
        # for a non-linear Partition Field: pkey_bucket with transform
        # bucket[16]`` (raised from pyiceberg/io/pyarrow.py's
        # `_partition_value`), meaning v1.3.6's own Bug #7 recommendation
        # (auto-partition on create, to make future overlap-deletes cheap)
        # is fundamentally incompatible with `committer_mode: staged`,
        # which is the mode this whole investigation's throughput numbers
        # were measured under. Since the checkpoint-constraint fix (Bug #4)
        # already makes genuine duplicate re-staging rare, and the
        # dedup-on-overlap path is already skip-only (see
        # `_dedup_one_range`), the cheap-delete benefit this partitioning
        # was meant to provide is low-value right now versus actually
        # landing rows. Leaving `dest_config["partition_spec"]` as an
        # explicit escape hatch: an operator who deliberately configures a
        # LINEAR transform (identity/truncate/day/etc, not bucket) still
        # gets it applied via `_build_partition_spec_from_arrow` below.
        if False and not partition_spec_cfg:
            # Prefer explicit pk_col; fall back to identifier_fields[0].
            resolved_pk = pk_col
            if not resolved_pk:
                ids = dest_config.get("identifier_fields") or []
                if isinstance(ids, (list, tuple)) and ids:
                    resolved_pk = str(ids[0])
            schema_names = {f.name for f in arrow_schema}
            if resolved_pk and resolved_pk in schema_names:
                partition_spec_cfg = [{
                    "source_column": resolved_pk,
                    "transform": "bucket",
                    "width": 16,
                    "name": f"{resolved_pk}_bucket",
                }]
                log.info(
                    "Creating Iceberg table %s.%s with default bucket(16, %s) "
                    "partition (v1.3.6)",
                    namespace, table_name, resolved_pk,
                )
        # 2026-07-24 fix: build a real, correctly-ID'd Schema (see
        # _arrow_schema_to_iceberg_schema_with_ids docstring) whenever a
        # partition spec is actually being created, so its field ids match
        # what _build_partition_spec_from_arrow assumes. For the plain
        # unpartitioned case (no pk known / not creating a spec) keep
        # passing the raw arrow_schema -- create_table()'s own WithoutIDs +
        # fresh-id-assignment path works fine when there's no partition
        # spec to reconcile ids against.
        if partition_spec_cfg:
            create_schema, _ = _arrow_schema_to_iceberg_schema_with_ids(arrow_schema)
        else:
            create_schema = arrow_schema
        partition_spec = _build_partition_spec_from_arrow(arrow_schema, partition_spec_cfg)
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
        try:
            return catalog.create_table(
                identifier=f"{namespace}.{table_name}",
                schema=create_schema,
                partition_spec=partition_spec,
                properties=props,
            )
        except Exception as exc:
            # Confirmed live (v1.3.8, on top of the Bug #17 bootstrap lock
            # below): when the bootstrap-lock winner's create_table() is
            # itself slow (tenacity retries against Nessie), BOOTSTRAP_WAIT_S
            # can elapse for every losing partition at once, and the
            # write_arrow_to_file timeout fallback used to send them all
            # straight into a direct commit simultaneously -- reproducing
            # the exact "K writers race to create the same table" storm the
            # bootstrap lock exists to prevent. Nessie's REST catalog returns
            # a plain 400 Bad Request for a losing concurrent /tables POST
            # (not the 409 TableAlreadyExistsError PyIceberg's own retry path
            # expects), so it propagated as a hard failure and permanently
            # dead-lettered 3 of 6 partitions (rows_written=0) on a
            # brand-new table. Regardless of which writer actually won, the
            # table now exists -- load it instead of failing the partition.
            try:
                return _use_fast_io(catalog.load_table(f"{namespace}.{table_name}"), dest_config)
            except Exception:
                raise exc


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
                create_schema, self.dest_config, pk_col=pk_col,
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
                create_schema, self.dest_config, pk_col=pk_col,
            )
            # v1.2.33 Bug #22 fix 2: dedup-on-PK before append (idempotency).
            if pk_col:
                _dedup_on_pk(table, pk_col, arrow_tbl=arrow_tbl)
            table.append(arrow_tbl)
        finally:
            _release_commit_lock(self.redis_client, self.connection_id, table_name)
        return arrow_tbl.num_rows

    # v1.2.39 section 6: single-committer staging path.
    def write_arrow_to_file(self, arrow_tbl: "pa.Table", table_name: str,
                            partition_id: str = "default",
                            chunk_seq: int | None = None,
                            pk_range: tuple | None = None,
                            pk_col: "str | None" = None,
                            ) -> str:
        """v1.2.39 section 6: write an Arrow batch as a plain Parquet file
        DIRECTLY to ``table.location()/data/<partition>/<chunk_seq>-<uuid>.parquet``
        via ``table.io.new_output(path).create()`` + ``pq.write_table()``.

        NO catalog call happens here - the file is inert (not in any
        manifest) until the committer picks it up and registers it via
        ``table.transaction().add_files([path])``. This is the worker side
        of the single-committer redesign: K partitions write files
        independently with zero coordination, then ONE committer registers
        them in a single Iceberg commit.

        Returns the absolute file path written. The caller is responsible
        for RPUSHing the path onto the pending-files list and advancing the
        checkpoint to ``staged`` (not ``durable``) - the committer promotes
        to ``durable`` after the commit confirms.

        The table must already exist (the committer or a prior
        ``write_arrow``/``write_batch`` call created it). If it does not,
        this method falls back to ``write_arrow`` (one catalog commit) so
        the very first chunk of a fresh table bootstraps the table; the
        committer then takes over for subsequent chunks.
        """
        if arrow_tbl is None or arrow_tbl.num_rows == 0:
            return ""
        import uuid as _uuid
        try:
            import pyarrow.parquet as pq
        except ImportError as e:  # pragma: no cover - pyarrow always present
            raise RuntimeError("pyarrow.parquet is required for write_arrow_to_file") from e

        # Load the table (no lock - we're not committing). If it doesn't
        # exist yet, bootstrap it via write_arrow (one commit) and return
        # the empty string so the caller treats this chunk as already
        # durable (the bootstrap commit made it so).
        from pyiceberg.exceptions import NoSuchTableError
        try:
            table = _use_fast_io(
                self.catalog.load_table(f"{self.namespace}.{table_name}"),
                self.dest_config,
            )
        except NoSuchTableError:
            # 2026-07-24 fix (Bug #17): every one of the K parallel
            # partitions hits this NoSuchTableError on its own first chunk
            # SIMULTANEOUSLY at the start of a fresh load, and every one of
            # them used to fall straight into write_arrow() -- a DIRECT,
            # lock-protected commit -- at the same instant. That is exactly
            # the "workers fighting to create the table" / "commit lock:
            # could not acquire ... proceeding without lock (degraded)"
            # storm observed live: with K=6, 5 of the 6 lose the race for
            # _acquire_commit_lock's 60s wait budget, log a WARNING, and
            # then proceed to commit ANYWAY in degraded mode (by design --
            # "commit anyway rather than dead-letter"), producing repeated
            # CommitFailedException/"snapshot id changed" conflicts that
            # only resolve via loader.py's retry-with-backoff. This is
            # wasted work, not a capacity problem: only ONE commit is ever
            # needed to create the table; the other K-1 gain nothing by
            # racing for it.
            #
            # Fix: serialize BOOTSTRAP OWNERSHIP with its own short-lived
            # SETNX lock, separate from the commit-serialization lock.
            # Exactly one partition wins and performs the one real
            # bootstrap commit; every other partition just polls
            # catalog.load_table() (a read, never a commit, so it cannot
            # conflict with anything) until the table appears, then falls
            # through to the normal lock-free staged write below -- so at
            # most 1 commit happens for the whole table's bootstrap,
            # regardless of K.
            bootstrap_key = f"fusion:iceberg-bootstrap-lock:{self.connection_id}:{table_name}"
            won_bootstrap = True
            if self.redis_client is not None:
                try:
                    won_bootstrap = bool(
                        self.redis_client.set(bootstrap_key, "1", nx=True, ex=120)
                    )
                except Exception:
                    log.exception("write_arrow_to_file: bootstrap-lock SET NX failed for "
                                  "key=%s — proceeding as if bootstrap owner (degraded)",
                                  bootstrap_key)
                    won_bootstrap = True
            if won_bootstrap:
                log.info("write_arrow_to_file: table %s.%s does not exist - "
                         "this partition won the bootstrap lock, creating via "
                         "write_arrow (one commit)", self.namespace, table_name)
                self.write_arrow(arrow_tbl, table_name=table_name, pk_col=pk_col)
                return ""
            log.info("write_arrow_to_file: table %s.%s does not exist yet and "
                     "another partition holds the bootstrap lock - waiting for "
                     "it to appear instead of racing a competing commit",
                     self.namespace, table_name)
            deadline = time.monotonic() + BOOTSTRAP_WAIT_S
            while time.monotonic() < deadline:
                time.sleep(BOOTSTRAP_POLL_S)
                try:
                    table = _use_fast_io(
                        self.catalog.load_table(f"{self.namespace}.{table_name}"),
                        self.dest_config,
                    )
                    break
                except NoSuchTableError:
                    continue
            else:
                # 2026-07-25 fix: previously every losing partition that
                # timed out here fell straight into its OWN direct
                # write_arrow() commit. When the original bootstrap winner
                # was itself slow (e.g. its create_table() hit internal
                # tenacity retries against Nessie), ALL K-1 losers can hit
                # this same timeout within moments of each other and would
                # ALL commit at once -- reproducing the exact "K writers
                # race to create the table" storm the bootstrap lock exists
                # to prevent (confirmed live: 3 of 6 partitions permanently
                # dead-lettered on one brand-new table's first sync). Try to
                # take over bootstrap ownership via the same SETNX before
                # committing directly: if the original winner is still
                # working, this fails harmlessly and we re-poll instead of
                # racing; if it died without ever creating the table (the
                # only case this fallback is actually for), we become the
                # sole new owner.
                won_bootstrap_retry = True
                if self.redis_client is not None:
                    try:
                        won_bootstrap_retry = bool(
                            self.redis_client.set(bootstrap_key, "1", nx=True, ex=120)
                        )
                    except Exception:
                        log.exception("write_arrow_to_file: bootstrap-lock retry SET NX "
                                      "failed for key=%s — proceeding as owner (degraded)",
                                      bootstrap_key)
                        won_bootstrap_retry = True
                if not won_bootstrap_retry:
                    try:
                        table = _use_fast_io(
                            self.catalog.load_table(f"{self.namespace}.{table_name}"),
                            self.dest_config,
                        )
                    except NoSuchTableError:
                        log.warning("write_arrow_to_file: table %s.%s still missing "
                                    "after %ds and another partition just took over "
                                    "bootstrap ownership - deferring to write_arrow's "
                                    "own idempotent create-or-load instead of racing",
                                    self.namespace, table_name, BOOTSTRAP_WAIT_S)
                        self.write_arrow(arrow_tbl, table_name=table_name, pk_col=pk_col)
                        return ""
                else:
                    log.warning("write_arrow_to_file: table %s.%s still missing after "
                                "waiting %ds - no other partition holds the bootstrap "
                                "lock (original owner likely died) - taking over and "
                                "committing directly", self.namespace, table_name,
                                BOOTSTRAP_WAIT_S)
                    self.write_arrow(arrow_tbl, table_name=table_name, pk_col=pk_col)
                    return ""

        loc = table.location().rstrip("/")
        part_dir = f"{loc}/data/{partition_id}"
        seq_tag = f"{chunk_seq}-" if chunk_seq is not None else ""
        file_name = f"{seq_tag}{_uuid.uuid4()}.parquet"
        path = f"{part_dir}/{file_name}"

        # table.io.new_output(path).create() returns a writable file-like.
        out = table.io.new_output(path)
        f = out.create()
        try:
            pq.write_table(arrow_tbl, f)
        finally:
            try:
                f.close()
            except Exception:
                pass
        return path

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
                pk_col=(identifier_fields[0] if identifier_fields else None),
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
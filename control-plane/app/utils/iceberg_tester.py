"""
Iceberg destination connectivity + write-permission tester.

Used by the control-plane destinations API to provide a REAL Test Connection
and a REAL write-permission check for Apache Iceberg destinations, instead of
the prior stub that fell through to a generic socket check.

The catalog-building helpers below are mirrored from
``transform-worker/iceberg_writer.py`` (load_catalog / _resolve_credentials /
_resolve_s3_settings / _normalize_warehouse). They are intentionally copied
rather than imported because the control-plane and transform-worker are
separately built Docker images with disjoint source trees. KEEP IN SYNC with
``transform-worker/iceberg_writer.py`` when the catalog config shape changes.

PyIceberg + pyarrow + boto3 are added to control-plane/requirements.txt so
this module can load the catalog, list namespaces, create a test table, and
HeadBucket the warehouse. All imports are lazy so the control-plane still
boots if an optional dep is absent (the test endpoints surface a clear error
instead of crashing the app).
"""
from __future__ import annotations

import logging
import time
from typing import Any

log = logging.getLogger(__name__)


# ─── Catalog factory (mirrored from transform-worker/iceberg_writer.py) ────────
def load_catalog(dest_config: dict):
    """Build a PyIceberg Catalog from destination connection_config."""
    from pyiceberg.catalog import load_catalog as _load

    catalog_type = (dest_config.get("catalog_type") or "rest").lower()
    catalog_name = dest_config.get("catalog_name", "fusion_cdc")

    settings: dict[str, Any] = {}
    creds = _resolve_credentials(dest_config)

    warehouse = _normalize_warehouse(dest_config.get("warehouse", ""))
    if warehouse:
        settings["warehouse"] = warehouse

    if catalog_type == "rest":
        settings["uri"] = dest_config["catalog_uri"]
        if dest_config.get("catalog_oauth_token"):
            settings["credential"] = dest_config["catalog_oauth_token"]
        if dest_config.get("rest_sigv5"):
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

    s3 = _resolve_s3_settings(dest_config, creds)
    settings.update(s3)

    return _load(catalog_name, **settings)


def _resolve_credentials(dest_config: dict) -> dict:
    """Resolve auth mode into a boto-style credential dict.

    Supports both the writer's auth_mode values (access_key / sts_assume /
    irsa) and the seeded ``static`` mode (s3_access_key_id / s3_secret_access_key
    placed directly in config). ``static`` is treated like access_key but reads
    the s3_* keys so the seeded MinIO destination works out of the box.
    """
    mode = (dest_config.get("auth_mode") or "access_key").lower()
    region = dest_config.get("aws_region") or dest_config.get("s3_region") or "us-east-1"
    out: dict[str, Any] = {"region": region}

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
        out["access_key_id"] = dest_config.get("aws_access_key_id")
        out["secret_access_key"] = dest_config.get("aws_secret_access_key")
        out["session_token"] = dest_config.get("aws_session_token")
        out["role_arn"] = dest_config.get("target_role_arn")
    elif mode == "irsa":
        out["role_arn"] = dest_config.get("service_account_role_arn")
    else:
        raise ValueError(f"Unsupported auth_mode: {mode}")

    return out


def _resolve_s3_settings(dest_config: dict, creds: dict) -> dict:
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

    same_creds = dest_config.get("same_creds_for_catalog_and_s3", True)
    if same_creds:
        if creds.get("access_key_id"):
            s3["s3.access-key-id"] = creds["access_key_id"]
            s3["s3.secret-access-key"] = creds["secret_access_key"]
            if creds.get("session_token"):
                s3["s3.session-token"] = creds["session_token"]
        if creds.get("role_arn"):
            s3["s3.role-arn"] = creds["role_arn"]
    else:
        if dest_config.get("s3_access_key_id"):
            s3["s3.access-key-id"] = dest_config["s3_access_key_id"]
            s3["s3.secret-access-key"] = dest_config["s3_secret_access_key"]
            if dest_config.get("s3_session_token"):
                s3["s3.session-token"] = dest_config["s3_session_token"]

    sse = (dest_config.get("sse_type") or "none").lower()
    if sse != "none":
        s3["s3.sse.type"] = sse
        if sse in ("sse-kms", "dsse-kms") and dest_config.get("sse_kms_key_id"):
            s3["s3.sse.kms-key-id"] = dest_config["sse_kms_key_id"]
    return s3


def _normalize_warehouse(warehouse: str) -> str:
    if not warehouse:
        return warehouse
    if warehouse.startswith("s3a://"):
        return "s3://" + warehouse[len("s3a://"):]
    return warehouse


def _bucket_from_warehouse(warehouse: str) -> str:
    """Extract the bucket name from an s3://bucket/... warehouse URI."""
    wh = _normalize_warehouse(warehouse)
    if "://" in wh:
        return wh.split("//", 1)[1].split("/", 1)[0]
    return wh


# ─── Public test API ──────────────────────────────────────────────────────────
def test_iceberg_connection(dest_config: dict) -> dict:
    """Real Iceberg connection test.

    Returns a structured dict::

        {
          "ok": bool,                  # overall
          "catalog_reachable": bool,
          "warehouse_reachable": bool,
          "auth_ok": bool,
          "error": str | None,
          "checks": [
            {"label": str, "ok": bool, "message": str | None},
            ...
          ],
        }

    The endpoint wraps this in a 200 response even on failure so the UI can
    render per-check status (the frontend reads ``checks``).
    """
    checks: list[dict] = []
    catalog_ok = False
    warehouse_ok = False
    auth_ok = False

    # Check 1: load catalog + list namespaces (validates catalog URI + creds)
    try:
        catalog = load_catalog(dest_config)
        namespaces = catalog.list_namespaces()
        catalog_ok = True
        auth_ok = True  # if list_namespaces succeeded, catalog auth worked
        ns_list = [n[0] if isinstance(n, tuple) else n for n in namespaces]
        checks.append({
            "label": "Resolve Iceberg catalog",
            "ok": True,
            "message": f"listed {len(ns_list)} namespace(s): {', '.join(map(str, ns_list[:5]))}",
        })
    except Exception as exc:
        checks.append({
            "label": "Resolve Iceberg catalog",
            "ok": False,
            "message": f"catalog error: {exc}",
        })
        return {
            "ok": False,
            "catalog_reachable": False,
            "warehouse_reachable": False,
            "auth_ok": False,
            "error": f"catalog error: {exc}",
            "checks": checks,
        }

    # Check 2: namespace exists (if configured)
    ns = dest_config.get("namespace", "default")
    ns_ok = False
    try:
        existing = [n[0] if isinstance(n, tuple) else n for n in namespaces]
        if ns in existing or not existing:
            ns_ok = True
            checks.append({"label": "List namespace", "ok": True, "message": f"namespace '{ns}' accessible"})
        else:
            checks.append({"label": "List namespace", "ok": True, "message": f"namespace '{ns}' not yet present (will be created on first write); available: {', '.join(map(str, existing[:5]))}"})
    except Exception as exc:
        checks.append({"label": "List namespace", "ok": False, "message": f"{exc}"})

    # Check 3: S3 HeadBucket on the warehouse bucket (validates S3 creds + endpoint)
    bucket = _bucket_from_warehouse(dest_config.get("warehouse", ""))
    try:
        import boto3
        creds = _resolve_credentials(dest_config)
        s3_args: dict[str, Any] = {}
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
        if bucket:
            client.head_bucket(Bucket=bucket)
            warehouse_ok = True
            auth_ok = auth_ok and True
            checks.append({"label": "S3 HeadBucket / warehouse prefix", "ok": True, "message": f"HeadBucket {bucket} ok"})
        else:
            checks.append({"label": "S3 HeadBucket / warehouse prefix", "ok": False, "message": "warehouse not configured"})
    except Exception as exc:
        checks.append({"label": "S3 HeadBucket / warehouse prefix", "ok": False, "message": f"s3 error: {exc}"})

    overall_ok = catalog_ok and warehouse_ok
    return {
        "ok": overall_ok,
        "catalog_reachable": catalog_ok,
        "warehouse_reachable": warehouse_ok,
        "auth_ok": auth_ok,
        "error": None if overall_ok else "one or more checks failed (see checks)",
        "checks": checks,
    }


def test_iceberg_write(dest_config: dict) -> dict:
    """Real Iceberg write-permission test.

    Creates a throwaway namespace + table, appends one row, deletes it, then
    drops the table + namespace. Returns::

        {
          "ok": bool,
          "can_write": bool,
          "can_create_table": bool,
          "can_insert": bool,
          "can_delete": bool,
          "error": str | None,
          "checks": [...],
        }
    """
    import pyarrow as pa

    checks: list[dict] = []
    can_create_table = False
    can_insert = False
    can_delete = False

    test_ns = "__dcraft_test"
    test_table = "__write_check"
    catalog = None
    table = None

    try:
        catalog = load_catalog(dest_config)
    except Exception as exc:
        checks.append({"label": "Load catalog", "ok": False, "message": f"{exc}"})
        return {
            "ok": False,
            "can_write": False,
            "can_create_table": False,
            "can_insert": False,
            "can_delete": False,
            "error": f"catalog error: {exc}",
            "checks": checks,
        }

    # Create test namespace
    try:
        from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError
        try:
            catalog.create_namespace(test_ns)
            checks.append({"label": "Create test namespace", "ok": True, "message": f"created {test_ns}"})
        except NamespaceAlreadyExistsError:
            checks.append({"label": "Create test namespace", "ok": True, "message": f"{test_ns} already exists"})
    except Exception as exc:
        checks.append({"label": "Create test namespace", "ok": False, "message": f"{exc}"})
        return _write_result(False, can_create_table, can_insert, can_delete, f"namespace create failed: {exc}", checks)

    # Create test table + append + delete + drop
    try:
        from pyiceberg.exceptions import NoSuchTableError, TableNotFound
        try:
            table = catalog.load_table(f"{test_ns}.{test_table}")
        except (NoSuchTableError, TableNotFound):
            schema = pa.schema([("id", pa.int64())])
            table = catalog.create_table(
                identifier=f"{test_ns}.{test_table}",
                schema=schema,
            )
            can_create_table = True
            checks.append({"label": "Create test table", "ok": True, "message": f"created {test_ns}.{test_table}"})
    except Exception as exc:
        checks.append({"label": "Create test table", "ok": False, "message": f"{exc}"})
        _cleanup_namespace(catalog, test_ns, checks)
        return _write_result(False, can_create_table, can_insert, can_delete, f"table create failed: {exc}", checks)

    # Insert one row
    try:
        row = pa.table({"id": pa.array([1], type=pa.int64())})
        table.append(row)
        can_insert = True
        checks.append({"label": "Insert test row", "ok": True, "message": "appended 1 row"})
    except Exception as exc:
        checks.append({"label": "Insert test row", "ok": False, "message": f"{exc}"})
        _cleanup_table_and_namespace(catalog, test_ns, test_table, checks)
        return _write_result(False, can_create_table, can_insert, can_delete, f"insert failed: {exc}", checks)

    # Delete the row
    try:
        from pyiceberg.expressions import In
        table.delete(In("id", [1]))
        can_delete = True
        checks.append({"label": "Delete test row", "ok": True, "message": "deleted 1 row"})
    except Exception as exc:
        checks.append({"label": "Delete test row", "ok": False, "message": f"{exc}"})

    # Drop table + namespace
    _cleanup_table_and_namespace(catalog, test_ns, test_table, checks)

    can_write = can_create_table and can_insert
    overall_ok = can_write
    return {
        "ok": overall_ok,
        "can_write": can_write,
        "can_create_table": can_create_table,
        "can_insert": can_insert,
        "can_delete": can_delete,
        "error": None if overall_ok else "write check failed (see checks)",
        "checks": checks,
    }


def _cleanup_table_and_namespace(catalog, ns: str, table_name: str, checks: list) -> None:
    _cleanup_table(catalog, ns, table_name, checks)
    _cleanup_namespace(catalog, ns, checks)


def _cleanup_table(catalog, ns: str, table_name: str, checks: list) -> None:
    try:
        catalog.drop_table(f"{ns}.{table_name}")
        checks.append({"label": "Drop test table", "ok": True, "message": f"dropped {ns}.{table_name}"})
    except Exception as exc:
        checks.append({"label": "Drop test table", "ok": False, "message": f"{exc}"})


def _cleanup_namespace(catalog, ns: str, checks: list) -> None:
    try:
        from pyiceberg.exceptions import NamespaceNotEmptyError
        catalog.drop_namespace(ns)
        checks.append({"label": "Drop test namespace", "ok": True, "message": f"dropped {ns}"})
    except NamespaceNotEmptyError:
        checks.append({"label": "Drop test namespace", "ok": True, "message": f"{ns} not empty (left in place)"})
    except Exception as exc:
        checks.append({"label": "Drop test namespace", "ok": False, "message": f"{exc}"})


def _write_result(ok, can_create_table, can_insert, can_delete, error, checks) -> dict:
    return {
        "ok": ok,
        "can_write": ok,
        "can_create_table": can_create_table,
        "can_insert": can_insert,
        "can_delete": can_delete,
        "error": error,
        "checks": checks,
    }


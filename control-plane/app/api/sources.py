"""
Sources API Endpoints
Manages source database configurations for data ingestion
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import select, func, and_, or_
from typing import Optional
from uuid import UUID
from datetime import datetime
import logging
import time
import math

from app.database import get_db
from app.models.source_destination import Source
from app.models.connector import ConnectorDefinition
from app.models.connection import Connection
from app.models.auth import AuditLog, User
from app.schemas.source import (
    SourceCreate,
    SourceUpdate,
    SourceResponse,
    SourceListResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    SchemaDiscoveryResponse,
    SchemaInfo,
    TableInfo,
    TableSchemaRequest,
    TableSchemaResponse,
    ColumnInfo,
    CDCConfigRequest,
    CDCConfigResponse,
    SourceStats,
)
from app.auth.dependencies import get_current_user, require_permission


router = APIRouter()
log = logging.getLogger(__name__)


# ============================================================================
# Helper Functions
# ============================================================================

def _encrypt_password(password: str) -> str:
    """Encrypt password using Fernet symmetric encryption.
    Always stores the raw (un-encoded) password so URI builders can encode correctly.
    """
    from urllib.parse import unquote
    from app.utils.crypto import encrypt_secret
    # Normalise: if the user pasted an already URL-encoded password (e.g. Devusr%40123)
    # store the decoded form so quote_plus() doesn't double-encode it later.
    return encrypt_secret(unquote(password or ""))


def _decrypt_password(encrypted: str) -> str:
    """Decrypt password using Fernet encryption from crypto utils"""
    if not encrypted:
        return ""
    from app.utils.crypto import decrypt_secret
    try:
        return decrypt_secret(encrypted)
    except Exception:
        # Fallback for legacy prefix format
        if encrypted.startswith("encrypted_"):
            return encrypted[10:]
        return encrypted


def _get_source_by_id(db: Session, source_id: UUID, user: User) -> Source:
    """Get source by ID with tenant filtering"""
    stmt = select(Source).where(
        and_(
            Source.source_id == source_id,
            Source.sub_tenant_id == user.sub_tenant_id,
            Source.is_deleted == False
        )
    ).options(joinedload(Source.connector_definition))
    
    source = db.execute(stmt).scalar_one_or_none()
    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {source_id} not found"
        )
    return source


def _maybe_activate_source(source: Source, db: Session) -> bool:
    """
    Auto-activate a source if all readiness criteria are met:
    1. Connection test has passed
    2. Schema discovery has run and found tables
    Returns True if status was changed to active.
    """
    if source.status != "draft":
        return False

    connection_ok = source.connection_test_status == "success"
    cache = source.discovery_cache or {}
    total_tables = sum(
        len(s.get("tables", []))
        for s in cache.get("schemas", [])
    )
    schemas_ok = source.last_discovery_at is not None and total_tables > 0

    if connection_ok and schemas_ok:
        source.status = "active"
        source.updated_at = datetime.utcnow()
        db.flush()
        log.info("Source %s auto-activated (connection OK, %d tables discovered)", source.source_id, total_tables)
        return True
    return False


def _test_database_connection(
    connector_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    ssl_enabled: bool,
    ssl_config: dict,
    ssh_config: Optional[dict] = None,
    extra_config: Optional[dict] = None,
) -> tuple[bool, str, Optional[int]]:
    """
    Test database connection using the real driver for the given connector_type.
    Returns: (success, message, latency_ms)
    """
    from app.utils.db_tester import test_connection
    return test_connection(
        connector_type=connector_type,
        host=host,
        port=port,
        database_name=database_name,
        username=username,
        password=password,
        ssl_enabled=ssl_enabled,
        ssl_config=ssl_config,
        ssh_config=ssh_config,
        extra_config=extra_config,
    )


def _discover_database_schemas(
    connector_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    ssl_enabled: bool,
    ssl_config: dict,
    ssh_config: dict = None,
    extra_config: dict = None,
) -> dict:
    """
    Introspect the source database and return a discovery cache dict.

    Spec §3: 'Upon source registration the engine introspects the database to retrieve:
    schemas/tables, column names, types, nullability, defaults, JSON column detection
    and primary keys.'
    """
    try:
        ct = (connector_type or "").lower()
        if ct in ("mysql", "mysql-binlog"):
            return _discover_mysql(host, port or 3306, database_name, username, password, ssh_config)
        elif ct in ("postgresql", "postgres", "postgres-wal"):
            return _discover_postgres(host, port or 5432, database_name, username, password, ssh_config)
        elif ct in ("mongodb", "mongo"):
            return _discover_mongodb(host, port or 27017, database_name, username, password, ssh_config, extra_config)
    except Exception as exc:
        log.exception("Schema discovery failed for %s@%s:%s — %s", connector_type, host, port, exc)

    # Fallback: empty result — will be populated on next re-introspection
    return {"schemas": []}


def _detect_json_column(data_type: str) -> bool:
    """Return True if column is known JSON type or text that may contain JSON."""
    return data_type.lower() in ("json", "jsonb", "text", "longtext", "mediumtext", "clob")


def _discover_mysql(host: str, port: int, database_name: str, username: str, password: str, ssh_config: dict = None) -> dict:
    """Query information_schema for MySQL schema structure."""
    from app.utils.db_tester import _ssh_tunnel
    import pymysql

    with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
        conn = pymysql.connect(
            host=bind_host, port=bind_port, user=username, password=password,
            database="information_schema", connect_timeout=10,
        )
        schemas: dict = {}
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                # Get tables
                cur.execute(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TABLE_ROWS "
                    "FROM TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')",
                    (database_name,)
                )
                for row in cur.fetchall():
                    s = row["TABLE_SCHEMA"]
                    t = row["TABLE_NAME"]
                    schemas.setdefault(s, {})[t] = {
                        "schema_name": s, "table_name": t,
                        "table_type": row["TABLE_TYPE"],
                        "row_count": int(row["TABLE_ROWS"] or 0),
                        "columns": [], "primary_keys": [],
                    }

                # Get columns
                cur.execute(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, "
                    "IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH "
                    "FROM COLUMNS WHERE TABLE_SCHEMA = %s ORDER BY ORDINAL_POSITION",
                    (database_name,)
                )
                for row in cur.fetchall():
                    key = (row["TABLE_SCHEMA"], row["TABLE_NAME"])
                    if key[0] in schemas and key[1] in schemas[key[0]]:
                        schemas[key[0]][key[1]]["columns"].append({
                            "column_name": row["COLUMN_NAME"],
                            "data_type": row["DATA_TYPE"],
                            "is_nullable": row["IS_NULLABLE"] == "YES",
                            "is_primary_key": row["COLUMN_KEY"] == "PRI",
                            "default_value": row["COLUMN_DEFAULT"],
                            "character_maximum_length": row["CHARACTER_MAXIMUM_LENGTH"],
                            "is_json_candidate": _detect_json_column(row["DATA_TYPE"]),
                        })
                        if row["COLUMN_KEY"] == "PRI":
                            schemas[key[0]][key[1]]["primary_keys"].append(row["COLUMN_NAME"])
        finally:
            conn.close()

    return {"schemas": [
        {"schema_name": sn, "tables": list(tables.values())}
        for sn, tables in schemas.items()
    ]}


def _discover_postgres(host: str, port: int, database_name: str, username: str, password: str, ssh_config: dict = None) -> dict:
    """Query information_schema for PostgreSQL schema structure."""
    from app.utils.db_tester import _ssh_tunnel
    import psycopg2
    import psycopg2.extras

    with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
        conn = psycopg2.connect(
            host=bind_host, port=bind_port, dbname=database_name, user=username,
            password=password, connect_timeout=10,
        )
        schemas: dict = {}
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Tables
                cur.execute(
                    "SELECT table_schema, table_name, table_type "
                    "FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY table_schema, table_name"
                )
                for row in cur.fetchall():
                    s, t = row["table_schema"], row["table_name"]
                    schemas.setdefault(s, {})[t] = {
                        "schema_name": s, "table_name": t,
                        "table_type": row["table_type"],
                        "row_count": 0, "columns": [], "primary_keys": [],
                    }

                # Collect PK info
                cur.execute(
                    "SELECT tc.table_schema, tc.table_name, kcu.column_name "
                    "FROM information_schema.table_constraints tc "
                    "JOIN information_schema.key_column_usage kcu "
                    "  ON tc.constraint_name = kcu.constraint_name "
                    "  AND tc.table_schema = kcu.table_schema "
                    "WHERE tc.constraint_type = 'PRIMARY KEY'"
                )
                pk_map: dict = {}
                for row in cur.fetchall():
                    pk_map.setdefault((row["table_schema"], row["table_name"]), set()).add(row["column_name"])

                # Columns
                cur.execute(
                    "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, "
                    "c.is_nullable, c.column_default, c.character_maximum_length "
                    "FROM information_schema.columns c "
                    "WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY c.table_schema, c.table_name, c.ordinal_position"
                )
                for row in cur.fetchall():
                    s, t = row["table_schema"], row["table_name"]
                    if s in schemas and t in schemas[s]:
                        is_pk = row["column_name"] in pk_map.get((s, t), set())
                        schemas[s][t]["columns"].append({
                            "column_name": row["column_name"],
                            "data_type": row["data_type"],
                            "is_nullable": row["is_nullable"] == "YES",
                            "is_primary_key": is_pk,
                            "default_value": row["column_default"],
                            "character_maximum_length": row["character_maximum_length"],
                            "is_json_candidate": _detect_json_column(row["data_type"]),
                        })
                        if is_pk:
                            schemas[s][t]["primary_keys"].append(row["column_name"])
        finally:
            conn.close()

    return {"schemas": [
        {"schema_name": sn, "tables": list(tables.values())}
        for sn, tables in schemas.items()
    ]}


def _discover_mongodb(host: str, port: int, database_name: str, username: str, password: str, ssh_config: dict = None, extra_config: dict = None) -> dict:
    """
    Parallel, timeout-safe MongoDB schema discovery.

    Sampling strategy (per collection):
      1. estimated_document_count()  — instant (metadata, no scan)
      2a. If count ≤ sample_size     → find({}) all docs, maxTimeMS=per_col_timeout
      2b. If count >  sample_size    → $sample aggregation (O(√n), very fast for large)
      2c. $sample timeout/error      → find({}).limit(sample_size) maxTimeMS=per_col_timeout
      2d. find timeout               → find_one() — at minimum get field names

    Type resolution (vote-based across N sampled docs):
      bool > int > float > str, with numeric-family widening.
      Cross-family conflicts → TEXT (safe catch-all).

    All collections sampled concurrently (ThreadPoolExecutor) so a database
    with 50 collections still finishes in seconds, not minutes.
    """
    from app.utils.db_tester import _ssh_tunnel
    from pymongo import MongoClient
    from urllib.parse import quote_plus, unquote
    from collections import defaultdict, Counter
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

    # ── BSON / Python type → Postgres type ───────────────────────────────────
    _PY_TO_PG = {
        "bool":        "BOOLEAN",
        "int":         "BIGINT",
        "float":       "DOUBLE PRECISION",
        "str":         "TEXT",
        "bytes":       "TEXT",
        "datetime":    "TIMESTAMPTZ",
        "Decimal128":  "NUMERIC",
        "ObjectId":    "TEXT",
        "Binary":      "TEXT",
        "Timestamp":   "BIGINT",
        "Int64":       "BIGINT",
        "Int32":       "BIGINT",
        "Regex":       "TEXT",
        "DBRef":       "TEXT",
        "UUID":        "UUID",
        "uuid":        "UUID",
        "dict":        "JSONB",
        "OrderedDict": "JSONB",
        "list":        "JSONB",
        "tuple":       "JSONB",
        "NoneType":    None,
    }
    _NUMERIC = frozenset({"BOOLEAN", "INTEGER", "BIGINT", "DOUBLE PRECISION", "NUMERIC"})
    _RANK    = {"BOOLEAN": 0, "INTEGER": 1, "BIGINT": 2,
                "DOUBLE PRECISION": 3, "NUMERIC": 4, "TIMESTAMPTZ": 10,
                "UUID": 11, "JSONB": 12, "TEXT": 99}

    def _py_type(v):
        if v is None:            return None
        if isinstance(v, bool):  return "BOOLEAN"
        if isinstance(v, int):   return "BIGINT"
        if isinstance(v, float): return "DOUBLE PRECISION"
        if isinstance(v, str):   return "TEXT"
        return _PY_TO_PG.get(type(v).__name__)

    def _resolve(votes: dict) -> str:
        clean = {t: c for t, c in votes.items() if t is not None}
        if not clean:
            return "TEXT"
        unique = set(clean.keys())
        if len(unique) == 1:
            return next(iter(unique))
        if unique.issubset(_NUMERIC):
            return max(unique, key=lambda t: _RANK.get(t, 0))
        return "TEXT"

    def _infer(docs):
        votes = defaultdict(Counter)
        for doc in docs:
            for k, v in doc.items():
                votes[k][_py_type(v)] += 1
        return {f: _resolve(dict(cnt)) for f, cnt in votes.items()}

    extra = extra_config or {}
    sample_size     = int(extra.get("sample_size") or 500)
    sample_size     = max(50, min(sample_size, 10000))
    # Per-collection timeout: generous but bounded so one slow collection can't block all
    per_col_timeout = int(extra.get("discovery_timeout_ms") or 5000)

    try:
        with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
            auth_source      = extra.get("auth_source")      or extra.get("authSource")      or "admin"
            replica_set      = extra.get("replica_set")      or extra.get("replicaSet")      or ""
            read_preference  = extra.get("read_preference")  or extra.get("readPreference")  or ""
            extra_hosts      = (extra.get("extra_hosts")     or "").strip()
            extra_uri_params = (extra.get("extra_uri_params") or "").strip().lstrip("?&")
            using_tunnel     = bool((ssh_config or {}).get("tunnel_host"))

            host_part = (
                f"{bind_host}:{bind_port}"
                if (using_tunnel or not extra_hosts)
                else f"{bind_host}:{bind_port},{extra_hosts}"
            )
            _user = quote_plus(unquote(username or ""))
            _pass = quote_plus(unquote(password or ""))
            if username and password:
                uri = (f"mongodb://{_user}:{_pass}@{host_part}/{database_name}"
                       f"?connectTimeoutMS=10000&authSource={auth_source}")
            else:
                uri = f"mongodb://{host_part}/{database_name}?connectTimeoutMS=10000"
            if using_tunnel:
                uri += "&directConnection=true"
            elif extra_hosts or replica_set:
                if replica_set:
                    uri += f"&replicaSet={replica_set}"
            else:
                uri += "&directConnection=true"
            if read_preference:
                uri += f"&readPreference={read_preference}"
            if extra_uri_params:
                uri += f"&{extra_uri_params}"

            client = MongoClient(uri, serverSelectionTimeoutMS=12000)
            db     = client[database_name]

            col_names = db.list_collection_names()

            def _sample_collection(col_name: str) -> dict:
                """Sample one collection and return its schema info."""
                try:
                    col      = db[col_name]
                    est      = col.estimated_document_count()
                    docs     = []

                    if est <= sample_size:
                        # Fetch all docs (small collection)
                        try:
                            docs = list(col.find({}).max_time_ms(per_col_timeout))
                        except Exception:
                            docs = []
                    else:
                        # Try $sample (O(√n) on large collections)
                        try:
                            docs = list(col.aggregate(
                                [{"$sample": {"size": sample_size}}],
                                maxTimeMS=per_col_timeout,
                                allowDiskUse=True,
                            ))
                        except Exception:
                            # Fallback 1: sequential limit scan
                            try:
                                docs = list(col.find({}).limit(sample_size).max_time_ms(per_col_timeout))
                            except Exception:
                                # Fallback 2: at minimum grab one doc for field names
                                try:
                                    one = col.find_one({})
                                    docs = [one] if one else []
                                except Exception:
                                    docs = []

                    typed_schema = _infer(docs)

                    columns = [
                        {
                            "column_name":            k,
                            "data_type":              v,           # Postgres type string
                            "python_type":            None,        # not needed downstream
                            "is_nullable":            True,
                            "is_primary_key":         k == "_id",
                            "default_value":          None,
                            "character_maximum_length": None,
                            "is_json_candidate":      v == "JSONB",
                            "sample_count":           len(docs),
                        }
                        for k, v in typed_schema.items()
                    ]
                    # Guarantee _id is present even if missing from all sample docs
                    if not any(c["column_name"] == "_id" for c in columns):
                        columns.insert(0, {
                            "column_name": "_id", "data_type": "TEXT",
                            "python_type": None, "is_nullable": False,
                            "is_primary_key": True, "default_value": None,
                            "character_maximum_length": None,
                            "is_json_candidate": False, "sample_count": len(docs),
                        })

                    return {
                        "schema_name": database_name,
                        "table_name":  col_name,
                        "table_type":  "COLLECTION",
                        "row_count":   est,
                        "sample_count": len(docs),
                        "columns":     columns,
                        "primary_keys": ["_id"],
                    }
                except Exception as col_exc:
                    log.warning("MongoDB discovery: collection %s skipped — %s", col_name, col_exc)
                    return {
                        "schema_name": database_name, "table_name": col_name,
                        "table_type": "COLLECTION", "row_count": 0,
                        "sample_count": 0, "columns": [], "primary_keys": ["_id"],
                        "error": str(col_exc),
                    }

            # ── Parallel collection sampling ─────────────────────────────────
            tables = []
            max_workers = min(8, max(1, len(col_names)))    # cap at 8 parallel threads
            total_timeout = max(30, len(col_names) * 4)     # generous total cap

            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mongo-discover") as pool:
                futures = {pool.submit(_sample_collection, n): n for n in col_names}
                for future in as_completed(futures, timeout=total_timeout):
                    try:
                        tables.append(future.result(timeout=per_col_timeout / 1000 + 2))
                    except Exception as fe:
                        col_name = futures[future]
                        log.warning("MongoDB discovery future failed for %s: %s", col_name, fe)
                        tables.append({
                            "schema_name": database_name, "table_name": col_name,
                            "table_type": "COLLECTION", "row_count": 0,
                            "sample_count": 0, "columns": [], "primary_keys": ["_id"],
                            "error": str(fe),
                        })

            client.close()
            tables.sort(key=lambda t: t["table_name"])
            return {"schemas": [{"schema_name": database_name, "tables": tables}]}

    except Exception as exc:
        log.warning("MongoDB discovery failed: %s", exc)
        return {"schemas": []}


def _get_table_schema(
    connector_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    schema_name: str,
    table_name: str,
    ssl_enabled: bool,
    ssl_config: dict,
    ssh_config: dict = None,
) -> dict:
    """
    Get detailed schema for a specific table (columns, types, PKs, indexes).
    Spec §3: real column introspection for schema evolution baseline.
    """
    try:
        ct = (connector_type or "").lower()
        if ct in ("mysql", "mysql-binlog"):
            return _get_table_schema_mysql(
                host, port or 3306, database_name, username, password, schema_name, table_name, ssh_config
            )
        elif ct in ("postgresql", "postgres", "postgres-wal"):
            return _get_table_schema_postgres(
                host, port or 5432, database_name, username, password, schema_name, table_name, ssh_config
            )
    except Exception as exc:
        log.exception("Table schema introspection failed for %s.%s@%s:%s — %s", schema_name, table_name, host, port, exc)

    return {"schema_name": schema_name, "table_name": table_name, "columns": [], "primary_keys": [], "indexes": []}


def _get_table_schema_mysql(
    host: str, port: int, database_name: str, username: str, password: str,
    schema_name: str, table_name: str, ssh_config: dict = None,
) -> dict:
    from app.utils.db_tester import _ssh_tunnel
    import pymysql

    with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
        conn = pymysql.connect(
            host=bind_host, port=bind_port, user=username, password=password,
            database="information_schema", connect_timeout=10,
        )
        columns, primary_keys, indexes = [], [], []
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
                    "COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH "
                    "FROM COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                    "ORDER BY ORDINAL_POSITION",
                    (schema_name, table_name),
                )
                for row in cur.fetchall():
                    is_pk = row["COLUMN_KEY"] == "PRI"
                    columns.append({
                        "column_name": row["COLUMN_NAME"],
                        "data_type": row["DATA_TYPE"],
                        "is_nullable": row["IS_NULLABLE"] == "YES",
                        "is_primary_key": is_pk,
                        "default_value": row["COLUMN_DEFAULT"],
                        "character_maximum_length": row["CHARACTER_MAXIMUM_LENGTH"],
                        "is_json_candidate": _detect_json_column(row["DATA_TYPE"]),
                    })
                    if is_pk:
                        primary_keys.append(row["COLUMN_NAME"])

                cur.execute(
                    "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE "
                    "FROM STATISTICS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                    "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                    (schema_name, table_name),
                )
                idx_map: dict = {}
                for row in cur.fetchall():
                    idx_map.setdefault(row["INDEX_NAME"], {"columns": [], "is_unique": not row["NON_UNIQUE"]})
                    idx_map[row["INDEX_NAME"]]["columns"].append(row["COLUMN_NAME"])
                indexes = [{"index_name": n, **v} for n, v in idx_map.items()]
        finally:
            conn.close()
    return {"schema_name": schema_name, "table_name": table_name,
            "columns": columns, "primary_keys": primary_keys, "indexes": indexes}


def _get_table_schema_postgres(
    host: str, port: int, database_name: str, username: str, password: str,
    schema_name: str, table_name: str, ssh_config: dict = None,
) -> dict:
    from app.utils.db_tester import _ssh_tunnel
    import psycopg2
    import psycopg2.extras

    with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
        conn = psycopg2.connect(
            host=bind_host, port=bind_port, dbname=database_name, user=username,
            password=password, connect_timeout=10,
        )
        columns, primary_keys, indexes = [], [], []
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, column_default, "
                    "character_maximum_length "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (schema_name, table_name),
                )
                col_rows = cur.fetchall()
                cur.execute(
                    "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                    "JOIN information_schema.key_column_usage kcu "
                    "  ON tc.constraint_name = kcu.constraint_name "
                    "  AND tc.table_schema = kcu.table_schema "
                    "WHERE tc.constraint_type = 'PRIMARY KEY' "
                    "AND tc.table_schema = %s AND tc.table_name = %s",
                    (schema_name, table_name),
                )
                pk_cols = {row["column_name"] for row in cur.fetchall()}
                for row in col_rows:
                    is_pk = row["column_name"] in pk_cols
                    columns.append({
                        "column_name": row["column_name"],
                        "data_type": row["data_type"],
                        "is_nullable": row["is_nullable"] == "YES",
                        "is_primary_key": is_pk,
                        "default_value": row["column_default"],
                        "character_maximum_length": row["character_maximum_length"],
                        "is_json_candidate": _detect_json_column(row["data_type"]),
                    })
                    if is_pk:
                        primary_keys.append(row["column_name"])
        finally:
            conn.close()
    return {"schema_name": schema_name, "table_name": table_name,
            "columns": columns, "primary_keys": primary_keys, "indexes": indexes}


# ============================================================================
# CRUD Endpoints
# ============================================================================

@router.post("", status_code=status.HTTP_201_CREATED, response_model=SourceResponse)
async def create_source(
    source: SourceCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("sources:create"))
):
    """
    Create a new source connection
    
    Requires: sources:create permission
    """
    # Verify connector definition exists
    stmt = select(ConnectorDefinition).where(
        and_(
            ConnectorDefinition.connector_id == source.connector_definition_id,
            ConnectorDefinition.category == "source",
            ConnectorDefinition.is_active == True
        )
    )
    connector_def = db.execute(stmt).scalar_one_or_none()
    if not connector_def:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector definition not found or not a source type"
        )
    
    # Check for duplicate name in tenant
    stmt = select(Source).where(
        and_(
            Source.source_name == source.source_name,
            Source.sub_tenant_id == current_user.sub_tenant_id,
            Source.is_deleted == False
        )
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Source with name '{source.source_name}' already exists"
        )
    
    # Create source
    db_source = Source(
        source_name=source.source_name,
        connector_definition_id=source.connector_definition_id,
        connector_version=source.connector_version,
        host=source.host,
        port=source.port,
        database_name=source.database_name,
        username=source.username,
        password_encrypted=_encrypt_password(source.password),
        ssl_enabled=source.ssl_enabled,
        ssl_config=source.ssl_config or {},
        ssh_config=source.ssh_config or {},
        config=source.config or {},
        status="draft",
        bank_id=current_user.bank_id,
        sub_tenant_id=current_user.sub_tenant_id,
        created_by=current_user.user_id,
    )
    
    db.add(db_source)
    db.commit()
    db.refresh(db_source)
    
    # Load connector definition for response
    db.refresh(db_source, attribute_names=['connector_definition'])
    
    # Build response
    response = SourceResponse.from_orm(db_source)
    response.connector_definition_name = connector_def.connector_name
    response.connector_definition_type = connector_def.connector_type
    
    return response


@router.get("", response_model=SourceListResponse)
async def list_sources(
    status: Optional[str] = Query(None, description="Filter by status"),
    connector_type: Optional[str] = Query(None, description="Filter by connector type"),
    connector_definition_id: Optional[UUID] = Query(None, description="Filter by connector definition"),
    search: Optional[str] = Query(None, description="Search in source name"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    List sources with filtering and pagination
    
    Automatically filtered by user's tenant
    """
    # Build base query with tenant filtering
    stmt = select(Source).where(
        and_(
            Source.sub_tenant_id == current_user.sub_tenant_id,
            Source.is_deleted == False
        )
    ).options(joinedload(Source.connector_definition))
    
    # Apply filters
    if status:
        stmt = stmt.where(Source.status == status)
    
    if connector_definition_id:
        stmt = stmt.where(Source.connector_definition_id == connector_definition_id)
    
    if connector_type:
        stmt = stmt.join(ConnectorDefinition).where(
            ConnectorDefinition.connector_type == connector_type
        )
    
    if search:
        stmt = stmt.where(Source.source_name.ilike(f"%{search}%"))
    
    # Get total count
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = db.execute(count_stmt).scalar()
    
    # Apply pagination
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    
    # Execute query
    sources = db.execute(stmt).scalars().all()
    
    # Build responses
    source_responses = []
    for source in sources:
        response = SourceResponse.from_orm(source)
        response.connector_definition_name = source.connector_definition.connector_name
        response.connector_definition_type = source.connector_definition.connector_type
        source_responses.append(response)
    
    total_pages = math.ceil(total / page_size) if total > 0 else 0
    
    return SourceListResponse(
        sources=source_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


@router.get("/{source_id}", response_model=SourceResponse)
async def get_source(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get source details by ID
    
    Automatically filtered by user's tenant
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    response = SourceResponse.from_orm(source)
    response.connector_definition_name = source.connector_definition.connector_name
    response.connector_definition_type = source.connector_definition.connector_type
    
    return response


@router.patch("/{source_id}", response_model=SourceResponse)
async def update_source(
    source_id: UUID,
    source_update: SourceUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("sources:update"))
):
    """
    Update source configuration
    
    Requires: sources:update permission
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Check for duplicate name if name is being updated
    if source_update.source_name and source_update.source_name != source.source_name:
        stmt = select(Source).where(
            and_(
                Source.source_name == source_update.source_name,
                Source.sub_tenant_id == current_user.sub_tenant_id,
                Source.source_id != source_id,
                Source.is_deleted == False
            )
        )
        existing = db.execute(stmt).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Source with name '{source_update.source_name}' already exists"
            )
    
    # Update fields
    update_data = source_update.dict(exclude_unset=True)
    
    # Handle password encryption
    if 'password' in update_data:
        update_data['password_encrypted'] = _encrypt_password(update_data.pop('password'))
    
    for field, value in update_data.items():
        setattr(source, field, value)
    
    source.updated_at = datetime.utcnow()
    
    db.commit()
    db.refresh(source)
    db.refresh(source, attribute_names=['connector_definition'])
    
    response = SourceResponse.from_orm(source)
    response.connector_definition_name = source.connector_definition.connector_name
    response.connector_definition_type = source.connector_definition.connector_type
    
    return response


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_source(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("sources:delete"))
):
    """
    Soft delete source
    
    Requires: sources:delete permission
    
    Cannot delete source if it has active connections
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Check for active connections
    stmt = select(func.count()).select_from(Connection).where(
        and_(
            Connection.source_id == source_id,
            Connection.is_deleted == False
        )
    )
    connection_count = db.execute(stmt).scalar()
    
    if connection_count > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete source with {connection_count} active connection(s)"
        )
    
    # Soft delete
    source.is_deleted = True
    source.deleted_at = datetime.utcnow()
    source.updated_at = datetime.utcnow()

    # Clean up Redis CDC stream keys for this source
    _delete_redis_cdc_keys(str(source_id))

    db.commit()


def _delete_redis_cdc_keys(source_id: str) -> None:
    """Delete all Redis CDC stream keys belonging to a source_id."""
    import os
    log = logging.getLogger(__name__)
    try:
        import redis as _redis
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        r = _redis.from_url(redis_url)
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"cdc:*:{source_id}:*", count=100)
            if keys:
                r.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        if deleted:
            log.info("Deleted %d Redis CDC key(s) for source_id=%s", deleted, source_id)
    except Exception as exc:
        log.warning("Could not clean Redis CDC keys for source_id=%s: %s", source_id, exc)


# ============================================================================
# Connection Testing Endpoints
# ============================================================================

@router.post("/test-tunnel", response_model=ConnectionTestResponse)
async def test_tunnel_adhoc(
    body: dict,
    current_user: User = Depends(get_current_user),
):
    """
    Test an SSH tunnel connection without needing a saved source.
    Accepts a JSON body with a ssh_config object (tunnel_host, tunnel_port,
    tunnel_username, tunnel_auth_method, tunnel_password / tunnel_private_key,
    tunnel_passphrase).  Also accepts the old format where ssl_config carried
    the tunnel fields.
    """
    from app.utils.db_tester import test_tunnel_connection
    ssh_config = body.get("ssh_config") or body.get("ssl_config") or body
    success, message, latency_ms = test_tunnel_connection(ssh_config)
    return ConnectionTestResponse(
        status="success" if success else "failure",
        message=message,
        error_details=None if success else message,
        connection_test_at=datetime.utcnow(),
        latency_ms=latency_ms,
    )


@router.post("/{source_id}/test-tunnel", response_model=ConnectionTestResponse)
async def test_source_tunnel(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Test the SSH tunnel configured on an existing source.
    """
    from app.utils.db_tester import test_tunnel_connection
    source = _get_source_by_id(db, source_id, current_user)
    # Try ssh_config first (new field), fall back to ssl_config (old behaviour)
    ssh_config = source.ssh_config or source.ssl_config or {}
    success, message, latency_ms = test_tunnel_connection(ssh_config)
    return ConnectionTestResponse(
        status="success" if success else "failure",
        message=message,
        error_details=None if success else message,
        connection_test_at=datetime.utcnow(),
        latency_ms=latency_ms,
    )


@router.post("/{source_id}/test-connection", response_model=ConnectionTestResponse)
async def test_source_connection(
    source_id: UUID,
    test_request: Optional[ConnectionTestRequest] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Test source database connectivity, routing through SSH tunnel if configured.

    Can optionally override connection parameters for testing without saving
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Use override parameters or source defaults
    host = test_request.host if test_request and test_request.host else source.host
    port = test_request.port if test_request and test_request.port else source.port
    database_name = test_request.database_name if test_request and test_request.database_name else source.database_name
    username = test_request.username if test_request and test_request.username else source.username
    password = test_request.password if test_request and test_request.password else _decrypt_password(source.password_encrypted)
    ssl_enabled = test_request.ssl_enabled if test_request and test_request.ssl_enabled is not None else source.ssl_enabled
    ssl_config = test_request.ssl_config if test_request and test_request.ssl_config else (source.ssl_config or {})
    ssh_config = getattr(test_request, "ssh_config", None) or source.ssh_config or {}
    extra_config = (getattr(test_request, "config", None) or source.config or {})

    # Test connection
    success, message, latency_ms = _test_database_connection(
        connector_type=source.connector_definition.connector_type,
        host=host,
        port=port,
        database_name=database_name,
        username=username,
        password=password,
        ssl_enabled=ssl_enabled,
        ssl_config=ssl_config,
        ssh_config=ssh_config,
        extra_config=extra_config,
    )
    
    # Update source with test results (only if testing current config)
    if not test_request:
        source.connection_test_status = "success" if success else "failure"
        source.connection_test_error = None if success else message
        source.connection_test_at = datetime.utcnow()
        _maybe_activate_source(source, db)
        db.commit()
    
    return ConnectionTestResponse(
        status="success" if success else "failure",
        message=message,
        error_details=None if success else message,
        connection_test_at=datetime.utcnow(),
        latency_ms=latency_ms
    )


# ============================================================================
# Schema Discovery Endpoints
# ============================================================================

@router.post("/{source_id}/discover", response_model=SchemaDiscoveryResponse)
@router.post("/{source_id}/discover-schemas", response_model=SchemaDiscoveryResponse)
async def discover_source_schemas(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Discover available schemas and tables from source database
    
    Results are cached in the source for faster subsequent queries
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Discover schemas
    discovery_cache = _discover_database_schemas(
        connector_type=source.connector_definition.connector_type,
        host=source.host,
        port=source.port,
        database_name=source.database_name,
        username=source.username,
        password=_decrypt_password(source.password_encrypted),
        ssl_enabled=source.ssl_enabled,
        ssl_config=source.ssl_config or {},
        ssh_config=source.ssh_config or {},
        extra_config=source.config or {},
    )
    
    # Update source with discovery cache
    source.discovery_cache = discovery_cache
    source.last_discovery_at = datetime.utcnow()
    _maybe_activate_source(source, db)
    db.commit()
    
    # Parse discovery cache into response model
    schemas = []
    total_tables = 0
    
    for schema_data in discovery_cache.get("schemas", []):
        tables = [TableInfo(**table) for table in schema_data.get("tables", [])]
        total_tables += len(tables)
        schemas.append(SchemaInfo(
            schema_name=schema_data["schema_name"],
            tables=tables
        ))
    
    return SchemaDiscoveryResponse(
        schemas=schemas,
        total_schemas=len(schemas),
        total_tables=total_tables,
        last_discovery_at=source.last_discovery_at
    )


@router.get("/{source_id}/schemas")
async def get_source_schemas(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get cached discovered schemas/tables for a source.
    Returns a flat list of tables for use in connection wizard.
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    cache = source.discovery_cache or {}
    tables = []
    for schema_data in cache.get("schemas", []):
        schema_name = schema_data.get("schema_name", "")
        for table in schema_data.get("tables", []):
            tables.append({
                "table_name": table.get("table_name", table.get("name", "")),
                "schema_name": schema_name,
                "name": table.get("table_name", table.get("name", "")),
                "primary_key": ",".join(table.get("primary_keys", [])) if table.get("primary_keys") else "",
                "cursor_field": table.get("cursor_field", ""),
                "columns": table.get("columns", []),
            })
    
    return {"tables": tables}


@router.post("/{source_id}/table-schema", response_model=TableSchemaResponse)
async def get_table_schema(
    source_id: UUID,
    table_request: TableSchemaRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed schema for a specific table including columns and indexes
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Get table schema
    table_schema = _get_table_schema(
        connector_type=source.connector_definition.connector_type,
        host=source.host,
        port=source.port,
        database_name=source.database_name,
        username=source.username,
        password=_decrypt_password(source.password_encrypted),
        schema_name=table_request.schema_name,
        table_name=table_request.table_name,
        ssl_enabled=source.ssl_enabled,
        ssl_config=source.ssl_config or {},
        ssh_config=source.ssh_config or {},
    )
    
    # Parse into response model
    columns = [ColumnInfo(**col) for col in table_schema["columns"]]
    
    return TableSchemaResponse(
        schema_name=table_schema["schema_name"],
        table_name=table_schema["table_name"],
        columns=columns,
        primary_keys=table_schema["primary_keys"],
        indexes=table_schema.get("indexes")
    )


# ============================================================================
# CDC Configuration Endpoints
# ============================================================================

@router.post("/{source_id}/cdc-config", response_model=CDCConfigResponse)
async def configure_cdc(
    source_id: UUID,
    cdc_config: CDCConfigRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("sources:update"))
):
    """
    Configure CDC (Change Data Capture) for a source
    
    Requires: sources:update permission
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Check if connector supports CDC
    if not source.connector_definition.supports_cdc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connector {source.connector_definition.connector_name} does not support CDC"
        )
    
    # Update source config with CDC settings
    source.config = source.config or {}
    source.config['cdc'] = {
        'enabled': cdc_config.enable_cdc,
        'replication_method': cdc_config.replication_method,
        'replication_config': cdc_config.replication_config
    }
    source.updated_at = datetime.utcnow()
    
    db.commit()
    db.refresh(source)
    
    return CDCConfigResponse(
        enable_cdc=cdc_config.enable_cdc,
        replication_method=cdc_config.replication_method,
        replication_config=cdc_config.replication_config,
        cdc_status="configured",
        last_updated_at=source.updated_at
    )


@router.get("/{source_id}/cdc-config", response_model=CDCConfigResponse)
async def get_cdc_config(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get current CDC configuration for a source
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    cdc_config = source.config.get('cdc', {}) if source.config else {}
    
    return CDCConfigResponse(
        enable_cdc=cdc_config.get('enabled', False),
        replication_method=cdc_config.get('replication_method'),
        replication_config=cdc_config.get('replication_config', {}),
        cdc_status="configured" if cdc_config.get('enabled') else "not_configured",
        last_updated_at=source.updated_at
    )


# ============================================================================
# Statistics Endpoints
# ============================================================================

@router.get("/{source_id}/stats", response_model=SourceStats)
async def get_source_stats(
    source_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get usage statistics for a source
    """
    source = _get_source_by_id(db, source_id, current_user)
    
    # Count total connections
    total_connections_stmt = select(func.count()).select_from(Connection).where(
        and_(
            Connection.source_id == source_id,
            Connection.is_deleted == False
        )
    )
    total_connections = db.execute(total_connections_stmt).scalar() or 0
    
    # Count active connections
    active_connections_stmt = select(func.count()).select_from(Connection).where(
        and_(
            Connection.source_id == source_id,
            Connection.status == "active",
            Connection.is_deleted == False
        )
    )
    active_connections = db.execute(active_connections_stmt).scalar() or 0
    
    # Derive sync stats from audit_logs (spec §5 P5-8)
    conn_ids = [
        str(c.connection_id)
        for c in db.execute(
            select(Connection.connection_id).where(
                and_(Connection.source_id == source_id, Connection.is_deleted == False)
            )
        ).all()
    ]
    total_syncs = 0
    successful_syncs = 0
    failed_syncs = 0
    last_sync_at = None
    total_rows_extracted = 0
    total_bytes_extracted = 0

    if conn_ids:
        run_logs = (
            db.query(AuditLog)
            .filter(
                AuditLog.resource_type == "connection",
                AuditLog.resource_id.in_(conn_ids),
                AuditLog.action.like("connection.batch_run.%"),
            )
            .order_by(AuditLog.created_at.desc())
            .limit(5000)
            .all()
        )
        total_syncs = len(run_logs)
        successful_syncs = sum(1 for r in run_logs if r.action == "connection.batch_run.success")
        failed_syncs = total_syncs - successful_syncs
        total_rows_extracted = sum(
            int((r.details or {}).get("rows_synced", 0) or 0)
            for r in run_logs if r.action == "connection.batch_run.success"
        )
        last_sync_at = run_logs[0].created_at if run_logs else None
    
    return SourceStats(
        source_id=source.source_id,
        source_name=source.source_name,
        total_connections=total_connections,
        active_connections=active_connections,
        total_syncs=total_syncs,
        successful_syncs=successful_syncs,
        failed_syncs=failed_syncs,
        last_sync_at=last_sync_at,
        total_rows_extracted=total_rows_extracted,
        total_bytes_extracted=total_bytes_extracted
    )

"""
CDC Consumer — reads CDC events from Redis Streams and applies them to the
configured Postgres destination.  Also performs the initial full-table load
for any CDC connection whose initial_load_completed flag is False.

How it works
────────────
1. On startup: connect to the metadata Postgres to discover all active CDC
   connections.  For each connection where initial_load_completed=false, read
   every row from the MySQL source tables and bulk-upsert them into the
   destination Postgres, then mark initial_load_completed=true.

2. Continuously: read new CDC events from the Redis streams that the cdc-worker
   publishes and apply upsert/delete to the destination Postgres tables.

3. Dynamic destination: DSN is built from the connection_config stored in the
   metadata DB so that this consumer works with any configured destination
   without any hardcoded values.

Run:
    python cdc_consumer.py

Environment overrides (all optional — defaults work for dev):
    METADATA_DB_DSN     DSN for the fusion_cdc_metadata postgres
    REDIS_URL           Redis URL
    ENCRYPTION_KEY      Fernet key used to decrypt credential blobs
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import paramiko
import psycopg2
import psycopg2.extras
import pymysql
import redis
import select as _select_module
import socket
import tempfile
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("cdc_consumer")

# ── Config ────────────────────────────────────────────────────────────────
METADATA_DSN   = os.environ.get(
    "METADATA_DB_DSN",
    "host=localhost port=5432 dbname=fusion_cdc_metadata user=fusion_user password=fusion_password",
)
REDIS_URL      = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "your-32-byte-encryption-key-for-aes256")

CONSUMER_GRP   = "fusion-spark"
CONSUMER_ID    = "dev-consumer-1"
SCAN_INTERVAL  = 2      # seconds between Redis key scans
BLOCK_MS       = 2000   # XREADGROUP block timeout ms

# When the consumer runs on the host (not inside Docker), Docker internal
# hostnames (e.g. "fusion-mysql") are not resolvable.  Set these env vars
# to override the source host/port for the initial full-table load.
MYSQL_HOST_OVERRIDE = os.environ.get("MYSQL_HOST_OVERRIDE")   # e.g. "localhost"
MYSQL_PORT_OVERRIDE = os.environ.get("MYSQL_PORT_OVERRIDE")   # e.g. "3307"

_running = True


def _stop(sig, frame):
    global _running
    log.info("Shutting down CDC consumer…")
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


# ── Crypto ────────────────────────────────────────────────────────────────

def _decrypt(ciphertext: str) -> str:
    from cryptography.fernet import Fernet
    if not ciphertext:
        return ""
    if ciphertext.startswith("encrypted_"):
        return ciphertext[len("encrypted_"):]
    key_bytes = hashlib.sha256(ENCRYPTION_KEY.encode()).digest()
    fernet = Fernet(base64.urlsafe_b64encode(key_bytes))
    return fernet.decrypt(ciphertext.encode()).decode()


# ── SSH Tunnel helper ──────────────────────────────────────────────────────

def _start_ssh_port_forward(ssh_cfg: dict, remote_host: str, remote_port: int):
    """Open an SSH tunnel via bastion and return (local_port, server_socket, ssh_client).
    Caller must keep server_socket and ssh_client alive for the tunnel to remain open.
    """
    key_text = ssh_cfg.get("tunnel_private_key", "")
    pkey_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
    pkey_file.write(key_text)
    pkey_file.close()
    os.chmod(pkey_file.name, 0o600)
    try:
        pkey = paramiko.RSAKey.from_private_key_file(pkey_file.name)
    finally:
        os.unlink(pkey_file.name)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=ssh_cfg["tunnel_host"],
        port=int(ssh_cfg.get("tunnel_port", 22)),
        username=ssh_cfg["tunnel_username"],
        pkey=pkey,
        timeout=15,
    )
    transport = client.get_transport()

    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0))
    lsock.listen(10)
    local_port = lsock.getsockname()[1]

    def _serve():
        while True:
            try:
                conn, _ = lsock.accept()
            except OSError:
                break

            def _handle(c=conn):
                try:
                    ch = transport.open_channel(
                        "direct-tcpip", (remote_host, remote_port), ("localhost", 0)
                    )
                    while True:
                        r, _, _ = _select_module.select([c, ch], [], [], 30)
                        if not r:
                            break
                        if c in r:
                            d = c.recv(65536)
                            if not d:
                                break
                            ch.sendall(d)
                        if ch in r:
                            d = ch.recv(65536)
                            if not d:
                                break
                            c.sendall(d)
                    ch.close()
                    c.close()
                except Exception:
                    pass

            threading.Thread(target=_handle, daemon=True).start()

    threading.Thread(target=_serve, daemon=True).start()
    return local_port, lsock, client


# ══════════════════════════════════════════════════════════════════════════
# §M  MongoDB / BSON — Type System & Schema Evolution Utilities
# ══════════════════════════════════════════════════════════════════════════

# Python / BSON type name  →  Postgres type string (None = defer to other votes)
_PY_TYPE_TO_PG: Dict[str, Optional[str]] = {
    "bool":         "BOOLEAN",
    "int":          "BIGINT",
    "float":        "DOUBLE PRECISION",
    "str":          "TEXT",
    "bytes":        "TEXT",            # stored base64-encoded
    "datetime":     "TIMESTAMPTZ",
    "Decimal128":   "NUMERIC",
    "ObjectId":     "TEXT",            # 24-hex string — not a UUID
    "Binary":       "TEXT",            # base64-encoded
    "Timestamp":    "BIGINT",          # MongoDB internal replication timestamp
    "Int64":        "BIGINT",
    "Int32":        "BIGINT",
    "Regex":        "TEXT",
    "DBRef":        "TEXT",
    "UUID":         "UUID",
    "uuid":         "UUID",
    "dict":         "JSONB",
    "OrderedDict":  "JSONB",
    "list":         "JSONB",
    "tuple":        "JSONB",
    "NoneType":     None,              # absent value — skip vote
}

# Permissiveness rank — higher = more permissive (wider) type
_PG_WIDEN_RANK: Dict[str, int] = {
    "BOOLEAN":          0,
    "INTEGER":          1,
    "BIGINT":           2,
    "DOUBLE PRECISION": 3,
    "NUMERIC":          4,
    "TIMESTAMPTZ":     10,
    "UUID":            11,
    "JSONB":           12,
    "TEXT":            99,   # universal catch-all
}
_NUMERIC_PG = frozenset({"BOOLEAN", "INTEGER", "BIGINT", "DOUBLE PRECISION", "NUMERIC"})

# Safe ALTER COLUMN TYPE … USING … templates  ({col} = double-quoted column reference)
_ALTER_TYPE_USING: Dict[tuple, str] = {
    ("BOOLEAN",          "INTEGER"):          "CASE WHEN {col} THEN 1 ELSE 0 END",
    ("BOOLEAN",          "BIGINT"):           "CASE WHEN {col} THEN 1 ELSE 0 END",
    ("BOOLEAN",          "DOUBLE PRECISION"): "CASE WHEN {col} THEN 1.0 ELSE 0.0 END",
    ("BOOLEAN",          "NUMERIC"):          "CASE WHEN {col} THEN 1 ELSE 0 END",
    ("INTEGER",          "BIGINT"):           "{col}::BIGINT",
    ("INTEGER",          "DOUBLE PRECISION"): "{col}::DOUBLE PRECISION",
    ("INTEGER",          "NUMERIC"):          "{col}::NUMERIC",
    ("BIGINT",           "DOUBLE PRECISION"): "{col}::DOUBLE PRECISION",
    ("BIGINT",           "NUMERIC"):          "{col}::NUMERIC",
    ("DOUBLE PRECISION", "NUMERIC"):          "{col}::NUMERIC",
    # Everything → TEXT is always safe and handled separately
}


def _py_val_pg_type(v: Any) -> Optional[str]:
    """Map a live Python/BSON value to its preferred Postgres type string."""
    if v is None:
        return None
    if isinstance(v, bool):          # MUST precede int (bool subclasses int)
        return "BOOLEAN"
    if isinstance(v, int):
        return "BIGINT"
    if isinstance(v, float):
        return "DOUBLE PRECISION"
    if isinstance(v, str):
        return "TEXT"
    if isinstance(v, bytes):
        return "TEXT"
    return _PY_TYPE_TO_PG.get(type(v).__name__)


def _resolve_pg_type(votes: Dict[Optional[str], int]) -> str:
    """
    Given a {pg_type: count} frequency map, return the single compatible PG type:
    - All within the numeric family → widest numeric member
    - Any cross-family mix (numeric + str, JSONB + numeric …) → TEXT
    - Only NoneType observed → TEXT
    """
    clean = {t: c for t, c in votes.items() if t is not None}
    if not clean:
        return "TEXT"
    unique = set(clean.keys())
    if len(unique) == 1:
        return next(iter(unique))
    if unique.issubset(_NUMERIC_PG):
        return max(unique, key=lambda t: _PG_WIDEN_RANK.get(t, 0))
    return "TEXT"    # heterogeneous types — safe fallback


def _normalize_pg_type(raw: str) -> str:
    """Normalise information_schema.columns.data_type to our canonical set."""
    t = (raw or "TEXT").upper().strip()
    if t in ("CHARACTER VARYING", "VARCHAR", "CHAR", "CHARACTER", "NAME"):
        return "TEXT"
    if t in ("INTEGER", "INT4", "INT"):
        return "INTEGER"
    if t in ("BIGINT", "INT8"):
        return "BIGINT"
    if t in ("SMALLINT", "INT2"):
        return "INTEGER"
    if t in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    if t in ("DOUBLE PRECISION", "FLOAT8", "FLOAT4", "REAL", "FLOAT"):
        return "DOUBLE PRECISION"
    if t in ("NUMERIC", "DECIMAL"):
        return "NUMERIC"
    if t in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"):
        return "TIMESTAMPTZ"
    if t in ("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP"):
        return "TIMESTAMPTZ"
    if t in ("JSONB", "JSON"):
        return "JSONB"
    if t == "UUID":
        return "UUID"
    return "TEXT"


def _widen_using(old_pg: str, new_pg: str, col_quoted: str) -> Optional[str]:
    """
    Return an SQL USING expression for ALTER COLUMN TYPE, or None if incompatible.
    Returns "" (empty) for no-op (same type).  Widening to TEXT is always safe.
    """
    if old_pg == new_pg:
        return ""
    if new_pg == "TEXT":
        return f"{col_quoted}::TEXT"
    tmpl = _ALTER_TYPE_USING.get((old_pg, new_pg))
    if tmpl:
        return tmpl.replace("{col}", col_quoted)
    return None    # no known safe widening path


def _bson_to_pg_copy_str(v: Any, pg_type: str) -> Optional[str]:
    """
    Convert a Python/BSON value to a string suitable for Postgres COPY CSV.
    Returns None for SQL NULL (caller writes r'\\N' to the CSV buffer).
    """
    import json as _j
    from datetime import datetime as _dt
    if v is None:
        return None
    # bool MUST precede int
    if isinstance(v, bool):
        if pg_type == "BOOLEAN":
            return "true" if v else "false"
        if pg_type in ("INTEGER", "BIGINT"):
            return "1" if v else "0"
        if pg_type in ("DOUBLE PRECISION", "NUMERIC"):
            return "1.0" if v else "0.0"
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if pg_type in ("INTEGER", "BIGINT"):
            return str(int(v))
        return repr(v)    # repr preserves more precision than str
    if isinstance(v, _dt):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return _j.dumps(v, default=str)
    if isinstance(v, bytes):
        import base64 as _b
        return _b.b64encode(v).decode()
    type_name = type(v).__name__
    if type_name == "ObjectId":
        return str(v)
    if type_name == "Decimal128":
        return str(v)
    if type_name == "Timestamp":
        try:
            return str(int(v.as_doc()["t"]))
        except Exception:
            return str(v)
    if type_name == "Regex":
        return str(v.pattern)
    if type_name == "DBRef":
        import json as _j2
        return _j2.dumps({"$ref": v.collection, "$id": str(v.id)})
    if type_name in ("UUID", "uuid"):
        return str(v)
    return str(v)


def _is_copy_compatible(val: str, pg_type: str) -> bool:
    """
    Quick check: would Postgres COPY accept this string value for the given type?
    False triggers pre-emptive column widening before the batch is even sent.
    """
    if val is None or pg_type == "TEXT":
        return True
    if pg_type in ("INTEGER", "BIGINT"):
        try:
            int(val); return True
        except Exception:
            return False
    if pg_type in ("DOUBLE PRECISION", "NUMERIC"):
        try:
            float(val); return True
        except Exception:
            return False
    if pg_type == "BOOLEAN":
        return val.lower() in ("true", "false", "t", "f", "1", "0", "yes", "no", "on", "off")
    if pg_type == "JSONB":
        try:
            json.loads(val); return True
        except Exception:
            return False
    if pg_type == "UUID":
        import re
        return bool(re.match(r"^[0-9a-f-]{36}$", val.lower()))
    return True    # TIMESTAMPTZ and others — let Postgres validate


def _infer_schema_from_docs(docs: List[Dict]) -> Dict[str, str]:
    """
    Vote-based schema inference: collect all Python types observed for each field
    across sample docs and pick the most permissive compatible PG type.
    Handles sparse fields (present in <1% of docs) without issue.
    """
    from collections import defaultdict, Counter
    votes: Dict[str, Counter] = defaultdict(Counter)
    for doc in docs:
        for k, v in doc.items():
            votes[k][_py_val_pg_type(v)] += 1
    return {field: _resolve_pg_type(dict(cnt)) for field, cnt in votes.items()}


def _row_val_for_pg(v: Any) -> Any:
    """
    Prepare a Python/BSON value for psycopg2 execute().
    - None        → None  (SQL NULL)
    - bool        → bool  (psycopg2 → BOOLEAN natively)
    - int / float → pass-through
    - dict / list → json.dumps() string (works for TEXT and JSONB columns)
    - bytes       → base64 string
    - ObjectId / Decimal128 / etc. → str()
    - Everything else → pass-through (str, datetime handled by psycopg2)
    """
    if v is None:
        return None
    if isinstance(v, bool):         # MUST precede int
        return v
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str)
    if isinstance(v, bytes):
        import base64 as _b
        return _b.b64encode(v).decode()
    type_name = type(v).__name__
    if type_name in ("ObjectId", "Decimal128", "Regex", "DBRef", "Timestamp"):
        return str(v)
    if type_name in ("UUID", "uuid"):
        return str(v)
    return v    # str, datetime → psycopg2 handles natively


def _log_schema_change(
    meta_conn,
    source_id: Optional[str],
    stream_id: Optional[str],
    table_name: str,
    schema_name: Optional[str],
    change_type: str,
    col_name: Optional[str],
    old_type: Optional[str],
    new_type: Optional[str],
    is_breaking: bool = False,
) -> None:
    """Persist a schema_change_events record. Silently swallows all errors."""
    if meta_conn is None or source_id is None:
        return
    try:
        old_s   = json.dumps({"column": col_name, "type": old_type}) if old_type else None
        new_s   = json.dumps({"column": col_name or "", "type": new_type or ""})
        diff    = json.dumps({"old": old_type, "new": new_type} if old_type else {"added": col_name})
        status  = "auto_approved" if not is_breaking else "pending"
        with meta_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO schema_change_events
                    (source_id, stream_id, table_name, schema_name, change_type,
                     old_schema, new_schema, schema_diff, detected_by,
                     is_breaking, status, impact_assessment)
                VALUES
                    (%s, %s::uuid, %s, %s, %s,
                     %s::jsonb, %s::jsonb, %s::jsonb, 'cdc_consumer',
                     %s, %s, '{}'::jsonb)
            """, (
                source_id, stream_id, table_name, schema_name, change_type,
                old_s, new_s, diff,
                is_breaking, status,
            ))
        meta_conn.commit()
    except Exception as exc:
        log.debug("[schema-log] schema_change_events write failed: %s", exc)
        try:
            meta_conn.rollback()
        except Exception:
            pass


# ── Metadata DB helpers ───────────────────────────────────────────────────

def get_meta_conn():
    return psycopg2.connect(METADATA_DSN)


def _get_active_cdc_connections(meta) -> List[Dict]:
    with meta.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                c.connection_id,
                c.connection_name,
                c.initial_load_completed,
                c.source_id,
                c.destination_id,
                s.host            AS src_host,
                s.port            AS src_port,
                s.database_name   AS src_db,
                s.username        AS src_user,
                s.password_encrypted AS src_pw_enc,
                s.ssh_config      AS src_ssh_config,
                s.config          AS src_config,
                cd.connector_type AS src_connector_type,
                CAST(s.bank_id AS TEXT)       AS bank_id,
                CAST(s.sub_tenant_id AS TEXT) AS tenant_id,
                d.connection_config AS dest_config
            FROM connections c
            JOIN sources s                ON s.source_id           = c.source_id
            JOIN connector_definitions cd ON cd.connector_id       = s.connector_definition_id
            JOIN destinations d           ON d.destination_id      = c.destination_id
            WHERE c.is_deleted = false
              AND UPPER(c.sync_type) IN ('CDC', 'REALTIME')
              AND s.is_deleted = false
              AND d.is_deleted = false
        """)
        return [dict(r) for r in cur.fetchall()]


def _get_streams_for_connection(meta, connection_id: str) -> List[Dict]:
    with meta.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT stream_id,
                   source_schema_name AS schema_name,
                   source_table_name  AS table_name,
                   destination_schema_name,
                   destination_table_name,
                   sync_mode,
                   primary_keys       AS primary_key,
                   column_mapping,
                   selected_columns,
                   transform_overrides
            FROM streams
            WHERE connection_id = %s AND is_enabled = true
        """, (connection_id,))
        return [dict(r) for r in cur.fetchall()]


def _get_pending_run(meta, connection_id: str) -> Optional[Dict]:
    with meta.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT run_id, run_number, status
            FROM connection_runs
            WHERE connection_id = %s AND status = 'running'
            ORDER BY run_number DESC LIMIT 1
        """, (connection_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def _complete_run(meta, connection_id: str, run_id: Optional[str],
                  records: int, error: Optional[str] = None) -> None:
    with meta.cursor() as cur:
        if run_id:
            if error:
                cur.execute("""
                    UPDATE connection_runs
                    SET status='failed', completed_at=now(), error_message=%s
                    WHERE run_id=%s
                """, (error, run_id))
            else:
                cur.execute("""
                    UPDATE connection_runs
                    SET status='completed', completed_at=now(),
                        records_written=%s, records_read=%s
                    WHERE run_id=%s
                """, (records, records, run_id))
        if error is None:  # only mark complete if no error (not error is falsy for empty strings)
            cur.execute("""
                UPDATE connections
                SET initial_load_completed=true, initial_load_completed_at=now()
                WHERE connection_id=%s
            """, (connection_id,))
    meta.commit()


# ── Destination DSN ───────────────────────────────────────────────────────

def _build_dest_dsn(dest_config: dict) -> str:
    host   = dest_config.get("host", "localhost")
    port   = int(dest_config.get("port", 5432))
    db     = dest_config.get("database_name", "")
    user   = dest_config.get("username", "")
    pw_enc = dest_config.get("password_encrypted", "")
    if pw_enc:
        try:
            pw = _decrypt(pw_enc)
        except Exception:
            pw = dest_config.get("password", "")
    else:
        pw = dest_config.get("password", "")
    return f"host={host} port={port} dbname={db} user={user} password={pw}"


# ── Destination Postgres helpers ──────────────────────────────────────────

def _ensure_dest_table(
    dest_conn,
    schema: str,
    table: str,
    columns: List[str],
    pk_cols: List[str],
    typed_schema: Optional[Dict[str, str]] = None,
    meta_conn=None,
    source_id: Optional[str] = None,
    stream_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    Ensure the destination table exists with the correct schema.

    typed_schema:  {col_name: pg_type} — when provided, columns are created with
                   their proper Postgres types (BOOLEAN, BIGINT, JSONB, etc.) rather
                   than defaulting everything to TEXT.  Existing columns are widened
                   when safe; irreconcilable conflicts fall back to TEXT and are logged.

    Returns the *effective* {col: pg_type} after any DDL.
    Backward-compatible: old callers that omit typed_schema get the old TEXT-only behaviour.
    """
    non_meta = [c for c in columns if c not in ("_cdc_op", "_cdc_ts", "_cdc_loaded_at")]

    def _col_pg_type(col: str) -> str:
        if typed_schema and col in typed_schema:
            return typed_schema[col] or "TEXT"
        return "TEXT"

    with dest_conn.cursor() as cur:
        # ── 1. Discover existing columns ──────────────────────────────────────
        cur.execute("""
            SELECT column_name,
                   CASE data_type
                       WHEN 'USER-DEFINED' THEN udt_name
                       ELSE data_type
                   END AS data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        existing: Dict[str, str] = {
            row[0]: _normalize_pg_type(row[1]) for row in cur.fetchall()
        }

        if not existing:
            # ── 2a. CREATE TABLE fresh with correct types ──────────────────────
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            col_defs = ", ".join(f'"{c}" {_col_pg_type(c)}' for c in non_meta)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                    {col_defs},
                    _cdc_op TEXT,
                    _cdc_ts BIGINT,
                    _cdc_loaded_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            effective: Dict[str, str] = {c: _col_pg_type(c) for c in non_meta}
        else:
            # ── 2b. ALTER TABLE: add missing cols / widen types ────────────────
            effective = dict(existing)
            for col in non_meta:
                want_type = _col_pg_type(col)
                if col not in existing:
                    # Add new column with the correct type
                    try:
                        cur.execute(
                            f'ALTER TABLE "{schema}"."{table}" '
                            f'ADD COLUMN IF NOT EXISTS "{col}" {want_type}'
                        )
                        effective[col] = want_type
                        _log_schema_change(
                            meta_conn, source_id, stream_id,
                            table, schema, "column_added",
                            col, None, want_type, is_breaking=False,
                        )
                    except Exception as add_exc:
                        dest_conn.rollback()
                        log.warning("[schema] Add col %s.%s.%s failed: %s", schema, table, col, add_exc)
                else:
                    # Widen type if needed and typed_schema was provided
                    cur_type = existing[col]
                    if typed_schema and cur_type != want_type and cur_type != "TEXT":
                        col_q  = f'"{col}"'
                        using  = _widen_using(cur_type, want_type, col_q)
                        if using is not None:
                            try:
                                alter = (
                                    f'ALTER TABLE "{schema}"."{table}" '
                                    f'ALTER COLUMN "{col}" TYPE {want_type}'
                                )
                                if using:
                                    alter += f" USING {using}"
                                cur.execute(alter)
                                effective[col] = want_type
                                _log_schema_change(
                                    meta_conn, source_id, stream_id,
                                    table, schema, "type_changed",
                                    col, cur_type, want_type, is_breaking=False,
                                )
                            except Exception as widen_exc:
                                dest_conn.rollback()
                                # Fallback: widen to TEXT (always safe)
                                try:
                                    cur.execute(
                                        f'ALTER TABLE "{schema}"."{table}" '
                                        f'ALTER COLUMN "{col}" TYPE TEXT USING "{col}"::TEXT'
                                    )
                                    effective[col] = "TEXT"
                                    _log_schema_change(
                                        meta_conn, source_id, stream_id,
                                        table, schema, "type_conflict_widened_text",
                                        col, cur_type, "TEXT", is_breaking=True,
                                    )
                                except Exception:
                                    dest_conn.rollback()
                                    log.error("[schema] Cannot widen %s.%s.%s %s→%s: %s",
                                              schema, table, col, cur_type, want_type, widen_exc)
                        else:
                            # No known safe widening path → TEXT
                            try:
                                cur.execute(
                                    f'ALTER TABLE "{schema}"."{table}" '
                                    f'ALTER COLUMN "{col}" TYPE TEXT USING "{col}"::TEXT'
                                )
                                effective[col] = "TEXT"
                                _log_schema_change(
                                    meta_conn, source_id, stream_id,
                                    table, schema, "type_conflict_widened_text",
                                    col, cur_type, "TEXT", is_breaking=True,
                                )
                            except Exception as te_exc:
                                dest_conn.rollback()
                                log.error("[schema] Cannot coerce %s.%s.%s→TEXT: %s",
                                          schema, table, col, te_exc)

        # ── 3. Ensure meta-columns exist ──────────────────────────────────────
        for meta_col, meta_type in [
            ("_cdc_op",        "TEXT"),
            ("_cdc_ts",        "BIGINT"),
            ("_cdc_loaded_at", "TIMESTAMPTZ DEFAULT now()"),
        ]:
            if meta_col not in existing:
                try:
                    cur.execute(
                        f'ALTER TABLE "{schema}"."{table}" '
                        f'ADD COLUMN IF NOT EXISTS {meta_col} {meta_type}'
                    )
                except Exception:
                    dest_conn.rollback()

        # ── 4. PK / Unique constraint ─────────────────────────────────────────
        if pk_cols:
            pk_str = ", ".join(f'"{c}"' for c in pk_cols)
            try:
                cur.execute(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conrelid = ('"{schema}"."{table}"')::regclass
                              AND contype IN ('p','u')
                        ) THEN
                            ALTER TABLE "{schema}"."{table}"
                            ADD CONSTRAINT "{table}_cdc_pk" UNIQUE ({pk_str});
                        END IF;
                    END $$
                """)
            except Exception:
                dest_conn.rollback()

    dest_conn.commit()
    # Always strip meta-columns from the returned schema so callers only see
    # data columns and don't accidentally duplicate _cdc_op/_cdc_ts in SQL.
    _META = frozenset({"_cdc_op", "_cdc_ts", "_cdc_loaded_at", "_mongo_raw"})
    return {k: v for k, v in effective.items() if k not in _META}


def _coerce_columns_to_text(
    dest_conn, schema: str, table: str, cols: List[str], pk_cols: List[str]
) -> None:
    """Widen all non-PK, non-TEXT columns to TEXT — conflict-resolution sledgehammer."""
    with dest_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        existing = {r[0]: _normalize_pg_type(r[1]) for r in cur.fetchall()}
    for col in cols:
        if col in existing and existing[col] != "TEXT" and col not in pk_cols:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(
                        f'ALTER TABLE "{schema}"."{table}" '
                        f'ALTER COLUMN "{col}" TYPE TEXT USING "{col}"::TEXT'
                    )
                dest_conn.commit()
                log.info("[schema] Coerced %s.%s.%s → TEXT", schema, table, col)
            except Exception as exc:
                dest_conn.rollback()
                log.warning("[schema] Cannot coerce %s.%s.%s to TEXT: %s", schema, table, col, exc)


def _apply_row(dest_conn, schema: str, table: str, op: str,
               row: dict, pk_cols: List[str], ts_ms: int = 0) -> None:
    if not row:
        return
    _ensure_dest_table(dest_conn, schema, table, list(row.keys()), pk_cols)

    for attempt in range(3):
        try:
            with dest_conn.cursor() as cur:
                if op in ("c", "u", "r"):
                    cols    = list(row.keys())
                    vals    = [_row_val_for_pg(v) for v in row.values()]
                    col_str = ", ".join(f'"{c}"' for c in cols)
                    val_str = ", ".join("%s" for _ in cols)
                    if pk_cols:
                        conflict = ", ".join(f'"{c}"' for c in pk_cols)
                        non_pk   = [c for c in cols if c not in pk_cols]
                        upd = (", ".join(f'"{c}"=EXCLUDED."{c}"' for c in non_pk) + ", " if non_pk else "")
                        upd += "_cdc_op=EXCLUDED._cdc_op, _cdc_ts=EXCLUDED._cdc_ts, _cdc_loaded_at=now()"
                        sql = f"""
                            INSERT INTO "{schema}"."{table}" ({col_str}, _cdc_op, _cdc_ts)
                            VALUES ({val_str}, %s, %s)
                            ON CONFLICT ({conflict}) DO UPDATE SET {upd}
                        """
                    else:
                        sql = (
                            f'INSERT INTO "{schema}"."{table}" ({col_str}, _cdc_op, _cdc_ts) '
                            f'VALUES ({val_str}, %s, %s)'
                        )
                    cur.execute(sql, vals + [op, ts_ms])
                elif op == "d" and pk_cols:
                    where = " AND ".join(f'"{c}"=%s' for c in pk_cols)
                    cur.execute(
                        f'DELETE FROM "{schema}"."{table}" WHERE {where}',
                        [_row_val_for_pg(row.get(c)) for c in pk_cols],
                    )
            dest_conn.commit()
            return    # success

        except psycopg2.errors.UndefinedColumn:
            dest_conn.rollback()
            log.warning("[apply-row] Undefined column in %s.%s — adding columns (attempt %d)",
                        schema, table, attempt + 1)
            _ensure_dest_table(dest_conn, schema, table, list(row.keys()), pk_cols)

        except (psycopg2.errors.InvalidTextRepresentation,
                psycopg2.errors.DatatypeMismatch,
                psycopg2.errors.NumericValueOutOfRange) as type_err:
            dest_conn.rollback()
            if attempt < 2:
                log.warning("[apply-row] Type mismatch in %s.%s: %s — widening to TEXT (attempt %d)",
                            schema, table, type_err, attempt + 1)
                _coerce_columns_to_text(dest_conn, schema, table, list(row.keys()), pk_cols)
            else:
                log.error("[apply-row] Persistent type error %s.%s after %d attempts: %s",
                          schema, table, attempt + 1, type_err)
                raise

        except Exception:
            dest_conn.rollback()
            raise


# ── Initial Load ──────────────────────────────────────────────────────────

def _copy_batch_to_pg(dest_conn, schema: str, table: str, cols: List[str],
                      batch: List[tuple], fallback_upsert_sql: str, batch_size: int) -> None:
    """
    Write a batch of rows to Postgres using COPY FROM STDIN (binary CSV).
    COPY is 10-50x faster than INSERT for bulk loads.
    Falls back to execute_values if COPY fails (e.g. type mismatch).

    Rows in `batch` are tuples: (col1, col2, ..., _cdc_op, _cdc_ts)
    already in the correct order.
    """
    import csv, io as _io

    all_cols = cols + ['_cdc_op', '_cdc_ts']
    quoted_cols = ", ".join(f'"{c}"' for c in all_cols)
    copy_sql = (
        f'COPY "{schema}"."{table}" ({quoted_cols}) '
        f'FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')'
    )

    # Build CSV in-memory buffer
    buf = _io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for row in batch:
        # None → r'\N'  (the NULL marker declared in the COPY command above)
        writer.writerow([r'\N' if v is None else v for v in row])
    buf.seek(0)

    try:
        with dest_conn.cursor() as cur:
            cur.copy_expert(copy_sql, buf)
    except Exception as copy_exc:
        log.warning("COPY failed (%s) — falling back to execute_values", copy_exc)
        dest_conn.rollback()
        psycopg2.extras.execute_values(
            dest_conn.cursor(), fallback_upsert_sql, batch, page_size=batch_size
        )


def _apply_transform_steps(row: dict, steps: list) -> dict:
    """
    Apply per-column transform steps from the UI wizard to a single row dict.

    Supported step types (mirrors the frontend TransformType):
      cast        — convert value to target Python type
      string_op   — upper / lower / trim / replace / lpad / rpad
      mask        — last4 / first4 / full / email
      json_extract— extract a sub-key from a JSON string value
      expression  — evaluate a simple Python expression (sandboxed)
      udf         — skip (handled by transform-worker, not here)
      math_op / json_flatten_* — skip (complex; pass through unchanged)

    Returns a new dict with transformed values.  Any step error is logged and
    the original value is kept so the load is never blocked by a bad transform.
    """
    import json as _j
    out = dict(row)
    for step in steps:
        stype = step.get("type")
        col   = step.get("column")
        out_col = step.get("output_column") or col
        if not col or col not in out:
            continue
        val = out[col]
        try:
            if stype == "cast":
                ttype = step.get("to_type", "string")
                if ttype in ("int", "integer", "bigint"):
                    val = int(val) if val is not None else None
                elif ttype in ("float", "double", "numeric", "decimal"):
                    val = float(val) if val is not None else None
                elif ttype == "boolean":
                    val = str(val).lower() in ("1", "true", "yes") if val is not None else None
                else:
                    val = str(val) if val is not None else None

            elif stype == "string_op" and val is not None:
                op = step.get("op", "")
                s = str(val)
                params = step.get("params") or {}
                if op == "upper":   val = s.upper()
                elif op == "lower": val = s.lower()
                elif op == "trim":  val = s.strip()
                elif op == "replace":
                    val = s.replace(params.get("from", ""), params.get("to", ""))
                elif op == "lpad":
                    val = s.ljust(int(params.get("length", len(s))), str(params.get("char", " ")))
                elif op == "rpad":
                    val = s.rjust(int(params.get("length", len(s))), str(params.get("char", " ")))

            elif stype == "mask" and val is not None:
                s = str(val)
                strategy = step.get("strategy", "full")
                if strategy == "last4":
                    val = "*" * max(0, len(s) - 4) + s[-4:] if len(s) > 4 else "****"
                elif strategy == "first4":
                    val = s[:4] + "*" * max(0, len(s) - 4)
                elif strategy == "full":
                    val = "*" * len(s)
                elif strategy == "email":
                    parts = s.split("@")
                    val = parts[0][0] + "***@" + (parts[1] if len(parts) > 1 else "")

            elif stype == "json_extract" and val is not None:
                obj = val if isinstance(val, dict) else _j.loads(str(val))
                path = step.get("json_path", "$")
                # Simple dot-path extraction: "$.key.subkey"
                keys = [k for k in path.lstrip("$.").split(".") if k]
                for k in keys:
                    if isinstance(obj, dict):
                        obj = obj.get(k)
                    else:
                        obj = None
                        break
                val = obj

            elif stype == "math_op" and val is not None:
                # Simple arithmetic expression — uses Python eval with only numeric val in scope
                expr = step.get("expression", str(col))
                try:
                    val = eval(expr, {"__builtins__": {}}, {col: float(val)})  # nosec B307 — expression is admin-configured
                except Exception:
                    pass  # keep original

            elif stype == "date_op" and val is not None:
                # Date operations — extract year/month/day etc. in pure Python
                from datetime import datetime as _dt
                op = step.get("operation", "year")
                try:
                    if isinstance(val, str):
                        dt = _dt.fromisoformat(val.replace("Z", "+00:00"))
                    else:
                        dt = val
                    if op == "year":    val = dt.year
                    elif op == "month": val = dt.month
                    elif op == "day":   val = dt.day
                    elif op == "hour":  val = dt.hour
                    elif op == "epoch": val = int(dt.timestamp())
                except Exception:
                    pass  # keep original

            # expression / udf / json_flatten_* — handled by transform-worker (need DuckDB/Spark)

        except Exception as ex:
            log.debug("[transform] step %s on col %s failed: %s — keeping original", stype, col, ex)

        if out_col != col:
            # write to new column name, keep original unless it was renamed
            out[out_col] = val
        else:
            out[col] = val
    return out


def _do_initial_load(conn_cfg: dict, dest_conn, meta, dest_schema_fallback: str = "public") -> int:
    """Route to MongoDB or MySQL initial load based on connector_type."""
    connector_type = (conn_cfg.get("src_connector_type") or "").lower()
    if "mongo" in connector_type:
        return _do_initial_load_mongodb(conn_cfg, dest_conn, meta, dest_schema_fallback)
    return _do_initial_load_mysql(conn_cfg, dest_conn, meta, dest_schema_fallback)


def _do_initial_load_mongodb(conn_cfg: dict, dest_conn, meta, dest_schema_fallback: str = "public") -> int:
    """
    Three-phase MongoDB → Postgres initial full load.

    Phase 1  — Schema inference: sample up to `sample_size` random documents per
               collection using $sample (O(√n) on large collections, instant on small
               ones).  Vote-based type resolution: for each field, pick the widest
               compatible Postgres type from all sampled values.

    Phase 2  — Table DDL: create/alter destination table with typed columns
               (BOOLEAN, BIGINT, DOUBLE PRECISION, TIMESTAMPTZ, JSONB, UUID, TEXT …).
               Schema changes are logged to schema_change_events.

    Phase 3  — Streaming: cursor over the entire collection with no_cursor_timeout=True
               (essential for large collections).  Before each batch is sent:
               • Pre-validate every value against the column's PG type.
               • If a conflict is detected: flush the batch, ALTER TABLE (widen), rebuild.
               • If a new field appears mid-stream: ALTER TABLE ADD COLUMN, rebuild.
               On COPY failure the batch falls back to execute_values row-by-row.
               Checkpoints every 10 000 docs so a restart resumes from the last table.
    """
    import json as _json
    from urllib.parse import quote_plus, unquote
    from collections import defaultdict, Counter

    cid    = str(conn_cfg["connection_id"])
    src_id = str(conn_cfg["source_id"])
    src_pw = _decrypt(conn_cfg.get("src_pw_enc") or "")
    streams = _get_streams_for_connection(meta, cid)
    if not streams:
        log.warning("[mongo-load] No streams for connection %s", cid)
        return 0

    with meta.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT stream_id::text, rows_written, status FROM initial_load_checkpoints "
            "WHERE connection_id = %s", (cid,)
        )
        checkpoints = {r["stream_id"]: dict(r) for r in cur.fetchall()}

    src_ssh_cfg = conn_cfg.get("src_ssh_config") or {}
    src_config  = conn_cfg.get("src_config") or {}
    if isinstance(src_config, str):
        try:
            src_config = _json.loads(src_config)
        except Exception:
            src_config = {}
    auth_source = (src_config.get("auth_source") if isinstance(src_config, dict) else None) or "admin"
    sample_size = int((src_config.get("sample_size") if isinstance(src_config, dict) else None) or 500)
    sample_size = max(50, min(sample_size, 10000))   # clamp: 50 – 10 000

    mongo_host  = conn_cfg["src_host"]
    mongo_port  = int(conn_cfg["src_port"] or 27017)
    _user = quote_plus(unquote(conn_cfg.get("src_user") or ""))
    _pass = quote_plus(unquote(src_pw))

    _tunnel_resources: list = []
    total = 0

    try:
        if src_ssh_cfg.get("tunnel_host"):
            local_port, lsock, ssh_cl = _start_ssh_port_forward(src_ssh_cfg, mongo_host, mongo_port)
            _tunnel_resources.extend([lsock, ssh_cl])
            import time as _t; _t.sleep(0.4)
            mongo_host, mongo_port = "127.0.0.1", local_port

        if _user and _pass:
            mongo_uri = (
                f"mongodb://{_user}:{_pass}@{mongo_host}:{mongo_port}"
                f"/{conn_cfg['src_db']}?authSource={auth_source}&directConnection=true"
            )
        else:
            mongo_uri = f"mongodb://{mongo_host}:{mongo_port}/{conn_cfg['src_db']}?directConnection=true"

        import pymongo
        mongo_client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=15000)
        mongo_db = mongo_client[conn_cfg["src_db"]]

        BATCH_SIZE = 1000

        for stream in streams:
            sid        = str(stream["stream_id"])
            src_schema = stream["schema_name"]
            src_table  = stream["table_name"]
            dst_schema = stream.get("destination_schema_name") or dest_schema_fallback
            dst_table  = stream.get("destination_table_name") or src_table
            pk_raw     = stream.get("primary_key") or ["_id"]
            if isinstance(pk_raw, str):
                try:
                    pk_raw = _json.loads(pk_raw)
                except Exception:
                    pk_raw = [pk_raw] if pk_raw else ["_id"]
            pk_cols = list(pk_raw)

            ckpt = checkpoints.get(sid)
            if ckpt and ckpt["status"] == "done":
                log.info("[mongo-load] ⏭  %s.%s already done (%d docs) — skipping",
                         src_schema, src_table, ckpt["rows_written"])
                total += ckpt["rows_written"]
                continue

            with meta.cursor() as cur:
                cur.execute("""
                    INSERT INTO initial_load_checkpoints
                        (connection_id, stream_id, source_table, rows_written, status)
                    VALUES (%s, %s, %s, 0, 'running')
                    ON CONFLICT (connection_id, stream_id)
                    DO UPDATE SET status='running', rows_written=0, started_at=now(), error=null
                """, (cid, sid, src_table))
            meta.commit()

            collection = mongo_db[src_table]
            estimated  = collection.estimated_document_count()
            log.info(
                "[mongo-load] %s.%s — ~%s docs → %s.%s  (sampling %d for schema, pk=%s)",
                src_schema, src_table, f"{estimated:,}",
                dst_schema, dst_table, sample_size, pk_cols,
            )

            # ── Phase 1: Schema inference via sampling ────────────────────────
            typed_schema: Dict[str, str] = {}
            sample_docs: List[Dict] = []
            try:
                if estimated <= sample_size:
                    sample_docs = list(
                        collection.find({}, max_time_ms=8000)
                    )
                else:
                    # $sample is O(√n) on large collections — very fast
                    try:
                        sample_docs = list(collection.aggregate(
                            [{"$sample": {"size": sample_size}}],
                            maxTimeMS=8000, allowDiskUse=True,
                        ))
                    except Exception as agg_exc:
                        log.warning("[mongo-load] $sample failed: %s — using find().limit()", agg_exc)
                        sample_docs = list(
                            collection.find({}).max_time_ms(8000).limit(sample_size)
                        )
                typed_schema = _infer_schema_from_docs(sample_docs)
                preview = dict(list(typed_schema.items())[:12])
                log.info("[mongo-load] %s.%s — inferred schema from %d docs: %s",
                         src_schema, src_table, len(sample_docs), preview)
            except Exception as sample_exc:
                log.warning("[mongo-load] Sampling failed for %s.%s: %s — will use TEXT fallback",
                            src_schema, src_table, sample_exc)
                typed_schema = {"_id": "TEXT"}

            # ── Phase 2: Create / alter destination table ─────────────────────
            pk_dest = [p for p in pk_cols if p in typed_schema or p == "_id"]
            if not pk_dest and pk_cols:
                pk_dest = pk_cols   # keep original even if not in sample

            effective_schema: Dict[str, str] = _ensure_dest_table(
                dest_conn, dst_schema, dst_table,
                list(typed_schema.keys()), pk_dest,
                typed_schema=typed_schema,
                meta_conn=meta, source_id=src_id, stream_id=sid,
            )

            # TRUNCATE — fresh load
            with dest_conn.cursor() as tc:
                tc.execute(f'TRUNCATE TABLE "{dst_schema}"."{dst_table}"')
            dest_conn.commit()

            def _build_upsert_sql(eff_cols: List[str]) -> str:
                col_str = ", ".join(f'"{c}"' for c in eff_cols)
                col_full = col_str + ", _cdc_op, _cdc_ts"
                if pk_dest:
                    conflict = ", ".join(f'"{c}"' for c in pk_dest)
                    non_pk   = [c for c in eff_cols if c not in pk_dest]
                    upd = (", ".join(f'"{c}"=EXCLUDED."{c}"' for c in non_pk) + ", " if non_pk else "")
                    upd += "_cdc_op=EXCLUDED._cdc_op, _cdc_ts=EXCLUDED._cdc_ts, _cdc_loaded_at=now()"
                    return (
                        f'INSERT INTO "{dst_schema}"."{dst_table}" ({col_full}) VALUES %s '
                        f'ON CONFLICT ({conflict}) DO UPDATE SET {upd}'
                    )
                return f'INSERT INTO "{dst_schema}"."{dst_table}" ({col_full}) VALUES %s'

            ts_ms       = int(datetime.utcnow().timestamp() * 1000)
            stream_count = 0
            batch: List[tuple] = []
            eff_cols  = list(effective_schema.keys())
            upsert_sql = _build_upsert_sql(eff_cols)

            # ── Phase 3: Stream all documents ─────────────────────────────────
            try:
                cursor = collection.find({}).batch_size(BATCH_SIZE)
                # no_cursor_timeout prevents the server-side cursor from expiring
                # during long loads (default Mongo cursor TTL = 10 min)
                cursor = cursor.allow_disk_use(True)

                for doc in cursor:
                    # ── Detect new fields (not yet in effective_schema) ───────
                    new_fields = {
                        k: (_py_val_pg_type(v) or "TEXT")
                        for k, v in doc.items()
                        if k not in effective_schema
                    }
                    if new_fields:
                        if batch:
                            _copy_batch_to_pg(dest_conn, dst_schema, dst_table, eff_cols,
                                              batch, upsert_sql, BATCH_SIZE)
                            dest_conn.commit()
                            stream_count += len(batch)
                            batch = []
                        merged = {**effective_schema, **new_fields}
                        effective_schema = _ensure_dest_table(
                            dest_conn, dst_schema, dst_table,
                            list(merged.keys()), pk_dest,
                            typed_schema=merged,
                            meta_conn=meta, source_id=src_id, stream_id=sid,
                        )
                        eff_cols   = list(effective_schema.keys())
                        upsert_sql = _build_upsert_sql(eff_cols)
                        log.info("[mongo-load] %s.%s schema evolved: +%s", src_schema, src_table, list(new_fields))

                    # ── Pre-validate values against column types ──────────────
                    type_conflicts: Dict[str, str] = {}
                    for k, v in doc.items():
                        if k not in effective_schema or v is None:
                            continue
                        pg_type = effective_schema[k]
                        if pg_type == "TEXT":
                            continue
                        copy_str = _bson_to_pg_copy_str(v, pg_type)
                        if copy_str is not None and not _is_copy_compatible(copy_str, pg_type):
                            # This value would break COPY — widen column
                            observed = _py_val_pg_type(v) or "TEXT"
                            new_pg   = (
                                max({pg_type, observed}, key=lambda t: _PG_WIDEN_RANK.get(t, 0))
                                if {pg_type, observed}.issubset(_NUMERIC_PG)
                                else "TEXT"
                            )
                            if new_pg != pg_type:
                                type_conflicts[k] = new_pg

                    if type_conflicts:
                        if batch:
                            _copy_batch_to_pg(dest_conn, dst_schema, dst_table, eff_cols,
                                              batch, upsert_sql, BATCH_SIZE)
                            dest_conn.commit()
                            stream_count += len(batch)
                            batch = []
                        merged = {**effective_schema, **type_conflicts}
                        effective_schema = _ensure_dest_table(
                            dest_conn, dst_schema, dst_table,
                            list(merged.keys()), pk_dest,
                            typed_schema=merged,
                            meta_conn=meta, source_id=src_id, stream_id=sid,
                        )
                        eff_cols   = list(effective_schema.keys())
                        upsert_sql = _build_upsert_sql(eff_cols)
                        log.warning("[mongo-load] %s.%s type conflict resolved: %s",
                                    src_schema, src_table, type_conflicts)

                    # ── Convert doc values to COPY-safe strings ───────────────
                    row_tuple = tuple(
                        _bson_to_pg_copy_str(doc.get(c), effective_schema.get(c, "TEXT"))
                        for c in eff_cols
                    ) + ("r", str(ts_ms))
                    batch.append(row_tuple)

                    if len(batch) >= BATCH_SIZE:
                        _copy_batch_to_pg(dest_conn, dst_schema, dst_table, eff_cols,
                                          batch, upsert_sql, BATCH_SIZE)
                        dest_conn.commit()
                        stream_count += len(batch)
                        if stream_count % 10000 == 0:
                            log.info("[mongo-load] %s.%s … %d docs written",
                                     src_schema, src_table, stream_count)
                            with meta.cursor() as mc:
                                mc.execute(
                                    "UPDATE initial_load_checkpoints SET rows_written=%s "
                                    "WHERE connection_id=%s AND stream_id=%s",
                                    (stream_count, cid, sid)
                                )
                            meta.commit()
                        batch = []

                if batch:
                    _copy_batch_to_pg(dest_conn, dst_schema, dst_table, eff_cols,
                                      batch, upsert_sql, BATCH_SIZE)
                    dest_conn.commit()
                    stream_count += len(batch)

            except Exception as exc:
                log.error("[mongo-load] Stream failed %s.%s: %s",
                          src_schema, src_table, exc, exc_info=True)
                with meta.cursor() as mc:
                    mc.execute(
                        "UPDATE initial_load_checkpoints SET status='failed', error=%s "
                        "WHERE connection_id=%s AND stream_id=%s",
                        (str(exc)[:2000], cid, sid)
                    )
                meta.commit()
                raise

            with meta.cursor() as mc:
                mc.execute(
                    "UPDATE initial_load_checkpoints "
                    "SET status='done', rows_written=%s, completed_at=now() "
                    "WHERE connection_id=%s AND stream_id=%s",
                    (stream_count, cid, sid)
                )
            meta.commit()
            total += stream_count
            log.info("[mongo-load] ✅ %s.%s → %d docs written to %s.%s  schema=%s",
                     src_schema, src_table, stream_count, dst_schema, dst_table,
                     {k: v for k, v in list(effective_schema.items())[:10]})

        mongo_client.close()

    finally:
        for res in _tunnel_resources:
            try:
                res.close()
            except Exception:
                pass

    return total


def _do_initial_load_mysql(conn_cfg: dict, dest_conn, meta, dest_schema_fallback: str = "public") -> int:
    """
    Perform the initial full-table load for every stream in the connection.

    Features:
    - Per-stream checkpointing: skips tables already marked 'done' so an OOM
      restart resumes from the next incomplete table rather than re-doing everything.
    - Column mapping: applies stream.column_mapping (rename / drop) before writing.
    - Pre-load size estimation: logs estimated row count so you can assess memory needs.

    dest_schema_fallback: schema to use when stream.destination_schema_name is NULL.
    """
    import json as _json

    cid     = str(conn_cfg["connection_id"])
    src_pw  = _decrypt(conn_cfg.get("src_pw_enc") or "")
    streams = _get_streams_for_connection(meta, cid)
    if not streams:
        log.warning("[initial-load] No streams for connection %s", cid)
        return 0

    BATCH_SIZE = 10000  # rows per fallback-upsert batch

    # ── Load existing checkpoints for this connection ─────────────────────────
    with meta.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT stream_id::text, rows_written, status FROM initial_load_checkpoints "
            "WHERE connection_id = %s", (cid,)
        )
        checkpoints = {r["stream_id"]: dict(r) for r in cur.fetchall()}

    total = 0
    for stream in streams:
        sid         = str(stream["stream_id"])
        src_schema  = stream["schema_name"]
        src_table   = stream["table_name"]
        dst_schema  = stream.get("destination_schema_name") or dest_schema_fallback
        dst_table   = stream.get("destination_table_name") or src_table
        pk_raw      = stream.get("primary_key") or []
        if isinstance(pk_raw, str):
            try:
                pk_raw = _json.loads(pk_raw)
            except Exception:
                pk_raw = [pk_raw] if pk_raw else []
        pk_cols = list(pk_raw)

        # ── Column mapping: {src_col: dest_col} or {src_col: null} to drop ──
        col_map: dict = {}
        raw_col_map = stream.get("column_mapping") or {}
        if isinstance(raw_col_map, str):
            try:
                raw_col_map = _json.loads(raw_col_map)
            except Exception:
                raw_col_map = {}
        col_map = raw_col_map  # e.g. {"old_name": "new_name", "secret_col": null}

        # ── Selected columns whitelist ────────────────────────────────────────
        selected: list = []
        raw_sel = stream.get("selected_columns") or []
        if isinstance(raw_sel, str):
            try:
                raw_sel = _json.loads(raw_sel)
            except Exception:
                raw_sel = []
        selected = list(raw_sel)  # empty = all columns

        # ── Transform overrides: {transforms: [{type, column, ...}]} ─────────
        transform_steps: list = []
        raw_to = stream.get("transform_overrides") or {}
        if isinstance(raw_to, str):
            try:
                raw_to = _json.loads(raw_to)
            except Exception:
                raw_to = {}
        transform_steps = raw_to.get("transforms", []) if isinstance(raw_to, dict) else []
        if transform_steps:
            log.info("[initial-load] %s.%s — %d transform step(s) active",
                     src_schema, src_table, len(transform_steps))

        # ── Checkpoint check: skip tables already completed ───────────────────
        ckpt = checkpoints.get(sid)
        if ckpt and ckpt["status"] == "done":
            log.info("[initial-load] ⏭  %s.%s already done (%d rows) — skipping",
                     src_schema, src_table, ckpt["rows_written"])
            total += ckpt["rows_written"]
            continue

        # ── Upsert / insert checkpoint row ────────────────────────────────────
        with meta.cursor() as cur:
            cur.execute("""
                INSERT INTO initial_load_checkpoints
                    (connection_id, stream_id, source_table, rows_written, status)
                VALUES (%s, %s, %s, 0, 'running')
                ON CONFLICT (connection_id, stream_id)
                DO UPDATE SET status='running', rows_written=0, started_at=now(), error=null
            """, (cid, sid, src_table))
        meta.commit()

        log.info("[initial-load] %s.%s — connecting to MySQL %s:%s/%s",
                 src_schema, src_table, conn_cfg["src_host"], conn_cfg["src_port"], src_schema)
        mysql_host = MYSQL_HOST_OVERRIDE or conn_cfg["src_host"]
        mysql_port = int(MYSQL_PORT_OVERRIDE or conn_cfg["src_port"] or 3306)
        src_ssh_cfg = conn_cfg.get("src_ssh_config") or {}
        _tunnel_resources: list = []
        stream_count = 0

        try:
            if src_ssh_cfg.get("tunnel_host"):
                local_port, lsock, ssh_cl = _start_ssh_port_forward(
                    src_ssh_cfg, mysql_host, mysql_port
                )
                _tunnel_resources.extend([lsock, ssh_cl])
                import time as _t; _t.sleep(0.2)
                mysql_host, mysql_port = "127.0.0.1", local_port

            src = pymysql.connect(
                host=mysql_host,
                port=mysql_port,
                user=conn_cfg["src_user"],
                password=src_pw,
                database=src_schema,
                cursorclass=pymysql.cursors.SSDictCursor,
            )

            # ── Pre-load size estimation ──────────────────────────────────────
            try:
                with src.cursor() as ec:
                    ec.execute(
                        "SELECT TABLE_ROWS, DATA_LENGTH "
                        "FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                        (src_schema, src_table)
                    )
                    info = ec.fetchone()
                    if info:
                        est_rows = info.get("TABLE_ROWS") or 0
                        est_mb   = (info.get("DATA_LENGTH") or 0) / 1024 / 1024
                        rec_mem  = max(512, int(est_mb * 3))  # 3x data size headroom
                        log.info(
                            "[initial-load] %s.%s — estimated %s rows, ~%.1f MB "
                            "(recommended memory: %d Mi)",
                            src_schema, src_table, f"{est_rows:,}", est_mb, rec_mem
                        )
            except Exception as size_exc:
                log.debug("[initial-load] Could not estimate table size: %s", size_exc)

            ts_ms = int(datetime.utcnow().timestamp() * 1000)
            cols_initialized = False
            cols: List[str] = []      # source columns (post-filter)
            dest_cols: List[str] = [] # destination column names (post-mapping)
            upsert_sql = ""

            try:
                with src.cursor() as cur:
                    cur.execute(f"SELECT * FROM `{src_table}`")
                    batch: List[tuple] = []

                    for raw_row in cur:
                        if not cols_initialized:
                            all_src_cols = list(raw_row.keys())

                            # Apply selected_columns whitelist
                            if selected:
                                all_src_cols = [c for c in all_src_cols if c in selected]

                            # Apply column_mapping: drop cols mapped to null, rename others
                            cols = []
                            dest_cols = []
                            for c in all_src_cols:
                                mapped = col_map.get(c, c)  # default: same name
                                if mapped is None:
                                    continue  # drop this column
                                cols.append(c)
                                dest_cols.append(mapped)

                            if col_map:
                                log.info(
                                    "[initial-load] %s.%s — column mapping active: "
                                    "%d src cols → %d dest cols",
                                    src_schema, src_table, len(all_src_cols), len(cols)
                                )

                            # Remap pk_cols to destination names
                            pk_dest = [col_map.get(p, p) for p in pk_cols if col_map.get(p, p) is not None]

                            _ensure_dest_table(dest_conn, dst_schema, dst_table, dest_cols, pk_dest)
                            with dest_conn.cursor() as tc:
                                tc.execute(f'TRUNCATE TABLE "{dst_schema}"."{dst_table}"')
                            dest_conn.commit()

                            col_str      = ", ".join(f'"{c}"' for c in dest_cols)
                            col_str_full = col_str + ', _cdc_op, _cdc_ts'
                            if pk_dest:
                                conflict = ", ".join(f'"{c}"' for c in pk_dest)
                                non_pk   = [c for c in dest_cols if c not in pk_dest]
                                upd = (
                                    ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in non_pk) + ", "
                                    if non_pk else ""
                                )
                                upd += "_cdc_op=EXCLUDED._cdc_op, _cdc_ts=EXCLUDED._cdc_ts, _cdc_loaded_at=now()"
                                conflict_clause = f"ON CONFLICT ({conflict}) DO UPDATE SET {upd}"
                            else:
                                conflict_clause = ""
                            upsert_sql = f"""
                                INSERT INTO "{dst_schema}"."{dst_table}" ({col_str_full})
                                VALUES %s
                                {conflict_clause}
                            """
                            cols_initialized = True

                        # Apply transform steps (cast, mask, string_op, json_extract, etc.)
                        working_row = dict(raw_row)
                        if transform_steps:
                            working_row = _apply_transform_steps(working_row, transform_steps)

                        # Extract only selected/mapped source columns
                        vals = tuple(
                            (str(v) if v is not None else None)
                            for k, v in working_row.items()
                            if k in cols
                        ) + ("r", ts_ms)
                        batch.append(vals)

                        if len(batch) >= BATCH_SIZE:
                            _copy_batch_to_pg(dest_conn, dst_schema, dst_table, dest_cols, batch, upsert_sql, BATCH_SIZE)
                            dest_conn.commit()
                            stream_count += len(batch)
                            if stream_count % 50000 == 0:
                                log.info("[initial-load] %s.%s … %d rows written",
                                         src_schema, src_table, stream_count)
                                # Update checkpoint progress
                                with meta.cursor() as mc:
                                    mc.execute(
                                        "UPDATE initial_load_checkpoints SET rows_written=%s "
                                        "WHERE connection_id=%s AND stream_id=%s",
                                        (stream_count, cid, sid)
                                    )
                                meta.commit()
                            batch = []

                    if batch:
                        _copy_batch_to_pg(dest_conn, dst_schema, dst_table, dest_cols, batch, upsert_sql, BATCH_SIZE)
                        dest_conn.commit()
                        stream_count += len(batch)

            finally:
                src.close()

        except Exception as exc:
            log.error("[initial-load] MySQL read failed for %s.%s: %s", src_schema, src_table, exc)
            # Mark checkpoint as failed
            with meta.cursor() as mc:
                mc.execute(
                    "UPDATE initial_load_checkpoints SET status='failed', error=%s "
                    "WHERE connection_id=%s AND stream_id=%s",
                    (str(exc), cid, sid)
                )
            meta.commit()
            raise
        finally:
            for res in _tunnel_resources:
                try:
                    res.close()
                except Exception:
                    pass

        # ── Mark this stream done ─────────────────────────────────────────────
        with meta.cursor() as mc:
            mc.execute(
                "UPDATE initial_load_checkpoints "
                "SET status='done', rows_written=%s, completed_at=now() "
                "WHERE connection_id=%s AND stream_id=%s",
                (stream_count, cid, sid)
            )
        meta.commit()

        total += stream_count
        log.info("[initial-load] ✅ %s.%s → %d rows written to %s.%s",
                 src_schema, src_table, stream_count, dst_schema, dst_table)

    return total


# ── Redis CDC streaming ───────────────────────────────────────────────────

def _ensure_group(r: redis.Redis, key: str) -> None:
    try:
        r.xgroup_create(key, CONSUMER_GRP, id="0", mkstream=False)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _get_cdc_keys(r: redis.Redis, allowed_source_ids: set = None) -> List[str]:
    """
    Return all CDC stream keys, optionally filtered to only those belonging
    to the given source UUIDs.

    Stream key format: cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}
    """
    keys: List[str] = []
    cursor = 0
    while True:
        cursor, batch = r.scan(cursor, match="cdc:*", count=100)
        for k in batch:
            key = k.decode() if isinstance(k, bytes) else k
            if allowed_source_ids:
                parts = key.split(":")
                # parts[3] is source_id in the standard key format
                if len(parts) >= 4 and parts[3] in allowed_source_ids:
                    keys.append(key)
            else:
                keys.append(key)
        if cursor == 0:
            break
    return keys


def _decode_fields(fields: dict) -> dict:
    return {
        (k.decode() if isinstance(k, bytes) else k):
        (v.decode() if isinstance(v, bytes) else v)
        for k, v in fields.items()
    }


def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


def _process_redis_events(r: redis.Redis,
                           source_to_dest: Dict[str, tuple],
                           known_keys: set,
                           pk_map: Dict[str, List[str]]) -> int:
    """
    Process pending + new events from Redis streams.

    source_to_dest: maps source_id (str) → (dest_conn, dest_schema).
    Stream key format: cdc:{bank_id}:{tenant_id}:{source_id}:{schema}:{table}
    Each event is routed to the correct destination/schema via source_id.
    """
    if not known_keys:
        return 0

    processed = 0
    key_list  = list(known_keys)

    def _route(stream_key: str):
        """Return (dest_conn, dest_schema, table_name) for a stream key."""
        parts = stream_key.split(":")
        source_id  = parts[3] if len(parts) >= 4 else ""
        table_name = parts[-1] if len(parts) >= 6 else stream_key.split(":")[-1]
        info = source_to_dest.get(source_id)
        if info is None:
            # Fallback: use first available destination
            info = next(iter(source_to_dest.values()), (None, "public"))
        dest_conn, dest_schema = info
        return dest_conn, dest_schema, table_name

    # Drain pending (unacknowledged) messages first
    for key in key_list:
        try:
            pending = r.xreadgroup(CONSUMER_GRP, CONSUMER_ID, {key: "0"}, count=100)
            if not pending:
                continue
            for stream_key, messages in pending:
                stream_key = stream_key.decode() if isinstance(stream_key, bytes) else stream_key
                dest_conn, dest_schema, table_name = _route(stream_key)
                if dest_conn is None:
                    log.warning("[pending] No destination for stream %s — skipping", stream_key)
                    continue
                pk_cols = pk_map.get(table_name, [])
                for msg_id, fields in messages:
                    msg_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    try:
                        f   = _decode_fields(fields)
                        op  = f.get("op", "c")
                        ts  = int(f.get("ts_ms", 0))
                        pk  = json.loads(f.get("pk_values", "{}"))
                        af  = json.loads(f.get("after",  "null"))
                        bf  = json.loads(f.get("before", "null"))
                        row = af if af is not None else bf
                        eff_pk = pk_cols or (list(pk.keys()) if isinstance(pk, dict) else [])
                        _apply_row(dest_conn, dest_schema, table_name, op, row or {}, eff_pk, ts)
                        r.xack(stream_key, CONSUMER_GRP, msg_id)
                        processed += 1
                        log.info("✓ [pending] %s %s pk=%s → %s.%s", op, table_name, pk, dest_schema, table_name)
                    except Exception as exc:
                        log.error("Pending msg %s error: %s", msg_id, exc)
                        _safe_rollback(dest_conn)
        except Exception as exc:
            log.warning("Pending drain error for %s: %s", key, exc)

    # Read new messages
    streams = {k: ">" for k in key_list}
    try:
        results = r.xreadgroup(CONSUMER_GRP, CONSUMER_ID, streams, count=100, block=BLOCK_MS)
    except redis.exceptions.ResponseError as e:
        log.warning("XREADGROUP error: %s", e)
        return processed

    if not results:
        return processed

    for stream_key, messages in results:
        stream_key = stream_key.decode() if isinstance(stream_key, bytes) else stream_key
        dest_conn, dest_schema, table_name = _route(stream_key)
        if dest_conn is None:
            log.warning("No destination for stream %s — skipping", stream_key)
            continue
        pk_cols = pk_map.get(table_name, [])

        for msg_id, fields in messages:
            msg_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            try:
                f   = _decode_fields(fields)
                op  = f.get("op", "c")
                ts  = int(f.get("ts_ms", 0))
                pk  = json.loads(f.get("pk_values", "{}"))
                af  = json.loads(f.get("after",  "null"))
                bf  = json.loads(f.get("before", "null"))
                row = af if af is not None else bf
                eff_pk = pk_cols or (list(pk.keys()) if isinstance(pk, dict) else [])
                _apply_row(dest_conn, dest_schema, table_name, op, row or {}, eff_pk, ts)
                r.xack(stream_key, CONSUMER_GRP, msg_id)
                processed += 1
                log.info("✓ %s %s pk=%s → %s.%s", op, table_name, pk, dest_schema, table_name)
            except Exception as exc:
                log.error("msg %s error: %s", msg_id, exc)
                _safe_rollback(dest_conn)

    return processed


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== CDC Consumer starting ===")
    log.info("Metadata DB: %s", METADATA_DSN.split("dbname")[0] + "…")
    log.info("Redis:       %s", REDIS_URL)

    r    = redis.from_url(REDIS_URL, decode_responses=False)
    meta = get_meta_conn()
    log.info("Connected to metadata DB ✓")

    connections = _get_active_cdc_connections(meta)
    log.info("Found %d active CDC connection(s)", len(connections))

    pk_map: Dict[str, List[str]]  = {}
    dest_conns: Dict[str, Any]    = {}  # cid  → psycopg2 connection
    dest_schemas: Dict[str, str]  = {}  # cid  → schema name (e.g. "dw")
    # source_to_dest routes each Redis stream (by source_id) to the right destination.
    # key: source_id (str UUID), value: (dest_conn, dest_schema)
    source_to_dest: Dict[str, tuple] = {}
    allowed_source_ids: set           = set()  # source UUIDs this consumer handles
    _ssh_tunnels: List[Any]           = []  # keep tunnel sockets/clients alive

    for conn_cfg in connections:
        cid  = str(conn_cfg["connection_id"])
        name = conn_cfg["connection_name"]

        dest_config = conn_cfg["dest_config"]
        if isinstance(dest_config, str):
            dest_config = json.loads(dest_config)

        dsn = _build_dest_dsn(dest_config)
        log.info("Connecting to destination '%s' at %s:%s/%s …",
                 name, dest_config.get("host"), dest_config.get("port"),
                 dest_config.get("database_name"))
        try:
            # Try direct connection first; if it times out or fails, try via SSH tunnel
            src_ssh_cfg = conn_cfg.get("src_ssh_config") or {}
            dest_host = dest_config.get("host", "")
            dest_port = int(dest_config.get("port", 5432))
            pg_conn_args = dict(
                dbname=dest_config.get("database_name", ""),
                user=dest_config.get("username", ""),
                password=(
                    _decrypt(dest_config["password_encrypted"])
                    if dest_config.get("password_encrypted")
                    else dest_config.get("password", "")
                ),
                connect_timeout=8,
            )
            dc = None
            if src_ssh_cfg.get("tunnel_host"):
                log.info("  Using SSH tunnel via %s …", src_ssh_cfg["tunnel_host"])
                try:
                    local_port, lsock, ssh_client = _start_ssh_port_forward(
                        src_ssh_cfg, dest_host, dest_port
                    )
                    _ssh_tunnels.extend([lsock, ssh_client])
                    import time as _time; _time.sleep(0.2)
                    dc = psycopg2.connect(host="127.0.0.1", port=local_port, **pg_conn_args)
                    log.info("  Connected via SSH tunnel (local port %d)", local_port)
                except Exception as tunnel_exc:
                    log.warning("  SSH tunnel failed: %s — falling back to direct", tunnel_exc)
            if dc is None:
                dc = psycopg2.connect(host=dest_host, port=dest_port, **pg_conn_args)
                log.info("  Connected directly")
            dest_conns[cid] = dc
            dest_schemas[cid] = dest_config.get("schema_name") or "public"
            log.info("Connected to destination ✓  (schema='%s')", dest_schemas[cid])
        except Exception as exc:
            log.error("Cannot connect to destination for '%s': %s", name, exc)
            continue

        # Register this source in the routing table
        sid = str(conn_cfg["source_id"])
        source_to_dest[sid] = (dest_conns[cid], dest_schemas[cid])
        allowed_source_ids.add(sid)
        log.info("  Routing: source_id=%s  bank_id=%s  tenant_id=%s  → schema=%s",
                 sid, conn_cfg.get("bank_id"), conn_cfg.get("tenant_id"), dest_schemas[cid])

        streams = _get_streams_for_connection(meta, cid)
        for st in streams:
            pk_raw = st.get("primary_key") or []
            if isinstance(pk_raw, str):
                try:
                    pk_raw = json.loads(pk_raw)
                except Exception:
                    pk_raw = [pk_raw] if pk_raw else []
            pk_map[st["table_name"]] = list(pk_raw)

        if not conn_cfg["initial_load_completed"]:
            pending_run = _get_pending_run(meta, cid)
            run_id = str(pending_run["run_id"]) if pending_run else None
            log.info("▶ Initial load required for '%s'", name)
            try:
                # Pass dest schema as fallback so initial load never defaults to 'public'
                n = _do_initial_load(conn_cfg, dest_conns[cid], meta, dest_schemas[cid])
                _complete_run(meta, cid, run_id, records=n)
                log.info("✅ Initial load DONE for '%s': %d rows loaded", name, n)
            except Exception as exc:
                log.error("Initial load FAILED for '%s': %s", name, exc, exc_info=True)
                _complete_run(meta, cid, run_id, records=0, error=str(exc))
        else:
            log.info("'%s': initial load already complete, starting CDC stream", name)

    if not source_to_dest and not connections:
        log.error("No active CDC connections found — will keep polling for new ones.")

    log.info("=== Entering real-time CDC streaming loop ===")
    if source_to_dest:
        log.info("Routing table: %s",
                 {sid: schema for sid, (_, schema) in source_to_dest.items()})
    known_keys: set = set()

    # Track which connection_ids we've already set up so the poller skips them
    known_cids: set = {str(c["connection_id"]) for c in connections}

    NEW_CONN_POLL_INTERVAL = 30  # seconds between checks for new connections

    def _poll_new_connections():
        """Background thread: detect new CDC connections and run initial loads."""
        import time as _t
        while _running:
            _t.sleep(NEW_CONN_POLL_INTERVAL)
            try:
                fresh_meta = get_meta_conn()
                all_conns = _get_active_cdc_connections(fresh_meta)
                new_conns = [c for c in all_conns if str(c["connection_id"]) not in known_cids]
                if new_conns:
                    log.info("[poller] Found %d new connection(s): %s",
                             len(new_conns), [c["connection_name"] for c in new_conns])
                for conn_cfg in new_conns:
                    cid  = str(conn_cfg["connection_id"])
                    name = conn_cfg["connection_name"]
                    known_cids.add(cid)  # register immediately to avoid double-processing

                    dest_config = conn_cfg["dest_config"]
                    if isinstance(dest_config, str):
                        dest_config = json.loads(dest_config)

                    pg_conn_args = dict(
                        dbname=dest_config.get("database_name", ""),
                        user=dest_config.get("username", ""),
                        password=(
                            _decrypt(dest_config["password_encrypted"])
                            if dest_config.get("password_encrypted")
                            else dest_config.get("password", "")
                        ),
                        connect_timeout=8,
                    )
                    dest_host = dest_config.get("host", "")
                    dest_port = int(dest_config.get("port", 5432))
                    src_ssh_cfg = conn_cfg.get("src_ssh_config") or {}
                    dc = None
                    try:
                        if src_ssh_cfg.get("tunnel_host"):
                            local_port, lsock, ssh_client = _start_ssh_port_forward(
                                src_ssh_cfg, dest_host, dest_port
                            )
                            _ssh_tunnels.extend([lsock, ssh_client])
                            _t.sleep(0.2)
                            dc = psycopg2.connect(host="127.0.0.1", port=local_port, **pg_conn_args)
                        else:
                            dc = psycopg2.connect(host=dest_host, port=dest_port, **pg_conn_args)
                        dest_conns[cid] = dc
                        dest_schemas[cid] = dest_config.get("schema_name") or "public"
                        log.info("[poller] Connected to destination for '%s' ✓", name)
                    except Exception as exc:
                        log.error("[poller] Cannot connect to destination for '%s': %s", name, exc)
                        continue

                    sid = str(conn_cfg["source_id"])
                    source_to_dest[sid] = (dest_conns[cid], dest_schemas[cid])
                    allowed_source_ids.add(sid)

                    streams = _get_streams_for_connection(fresh_meta, cid)
                    for st in streams:
                        pk_raw = st.get("primary_key") or []
                        if isinstance(pk_raw, str):
                            try:
                                pk_raw = json.loads(pk_raw)
                            except Exception:
                                pk_raw = [pk_raw] if pk_raw else []
                        pk_map[st["table_name"]] = list(pk_raw)

                    if not conn_cfg["initial_load_completed"]:
                        log.info("[poller] ▶ Starting initial load for '%s'", name)
                        try:
                            n = _do_initial_load(conn_cfg, dest_conns[cid], fresh_meta, dest_schemas[cid])
                            _complete_run(fresh_meta, cid, None, records=n)
                            log.info("[poller] ✅ Initial load DONE for '%s': %d rows", name, n)
                        except Exception as exc:
                            log.error("[poller] Initial load FAILED for '%s': %s", name, exc, exc_info=True)
                    else:
                        log.info("[poller] '%s': initial load already done, added to CDC routing", name)
                fresh_meta.close()
            except Exception as exc:
                log.error("[poller] Error polling for new connections: %s", exc)

    import threading as _threading
    _threading.Thread(target=_poll_new_connections, daemon=True, name="new-conn-poller").start()
    log.info("New-connection poller started (interval: %ds)", NEW_CONN_POLL_INTERVAL)

    while _running:
        try:
            # Only watch streams belonging to our configured sources
            current = set(_get_cdc_keys(r, allowed_source_ids))
            for key in current - known_keys:
                log.info("New CDC stream: %s", key)
                _ensure_group(r, key)
                known_keys.add(key)

            if known_keys:
                n = _process_redis_events(r, source_to_dest, known_keys, pk_map)
                if n:
                    log.info("Applied %d real-time CDC event(s)", n)
            else:
                time.sleep(SCAN_INTERVAL)

        except Exception as exc:
            log.error("Main loop error: %s", exc, exc_info=True)
            _safe_rollback(default_dest)
            time.sleep(SCAN_INTERVAL)

    for dc in dest_conns.values():
        try:
            dc.close()
        except Exception:
            pass
    meta.close()
    r.close()
    log.info("CDC Consumer stopped.")


if __name__ == "__main__":
    main()

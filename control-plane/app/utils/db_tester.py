"""
Real database connection tester.

Returns a 3-tuple (success: bool, message: str, latency_ms: int | None).

Supported connector types: "postgresql", "mysql", "mongodb"

SSL and SSH are SEPARATE concerns:
  ssl_config  → TLS/SSL settings  (ssl_mode, ssl_ca, ssl_cert, ssl_key)
  ssh_config  → SSH jump/bastion  (tunnel_host, tunnel_port, tunnel_username,
                                   tunnel_auth_method, tunnel_password /
                                   tunnel_private_key, tunnel_passphrase)

Both can be active simultaneously: the SSH tunnel is opened first, then the
TLS handshake happens over the tunnelled connection.

For Spark JDBC compatibility the same ssl_mode strings are used:
  disable | allow | prefer | require | verify-ca | verify-full
"""

import io
import time
from contextlib import contextmanager
from typing import Optional, Tuple


ConnectionResult = Tuple[bool, str, Optional[int]]


# ============================================================================
# SSH Tunnel helper
# ============================================================================

@contextmanager
def _ssh_tunnel(ssh_config: dict, remote_host: str, remote_port: int):
    """
    Open an SSH tunnel when ssh_config has a tunnel_host.
    Yields (bind_host, bind_port). Without a tunnel yields the originals.
    """
    tunnel_host = (ssh_config or {}).get("tunnel_host")
    if not tunnel_host:
        yield remote_host, remote_port
        return

    try:
        from sshtunnel import SSHTunnelForwarder
    except ImportError:
        raise RuntimeError("sshtunnel package not installed; cannot open SSH tunnel")

    tunnel_port = int((ssh_config or {}).get("tunnel_port") or 22)
    tunnel_username = (ssh_config or {}).get("tunnel_username")
    auth_method = (ssh_config or {}).get("tunnel_auth_method", "password")
    tunnel_password = (ssh_config or {}).get("tunnel_password")
    tunnel_private_key = (ssh_config or {}).get("tunnel_private_key")
    tunnel_passphrase = (ssh_config or {}).get("tunnel_passphrase") or None

    ssh_kwargs: dict = dict(
        ssh_address_or_host=(tunnel_host, tunnel_port),
        remote_bind_address=(remote_host, remote_port),
        ssh_username=tunnel_username,
        set_keepalive=5,
    )

    if auth_method == "key" and tunnel_private_key:
        try:
            import paramiko
        except ImportError:
            raise RuntimeError("paramiko not installed; cannot use key-based SSH auth")
        key_text = tunnel_private_key.strip()
        for key_class in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key, paramiko.DSSKey):
            try:
                pkey = key_class.from_private_key(io.StringIO(key_text), password=tunnel_passphrase)
                ssh_kwargs["ssh_pkey"] = pkey
                break
            except Exception:
                continue
        else:
            raise ValueError("Could not parse the SSH private key. Supported: RSA, ECDSA, Ed25519, DSA.")
    else:
        ssh_kwargs["ssh_password"] = tunnel_password

    server = SSHTunnelForwarder(**ssh_kwargs)
    try:
        server.start()
        yield "127.0.0.1", server.local_bind_port
    finally:
        try:
            server.stop()
        except Exception:
            pass


def test_tunnel_connection(ssh_config: dict) -> ConnectionResult:
    """
    Test ONLY the SSH tunnel — does NOT connect to the database.
    Accepts the ssh_config dict with tunnel_host, tunnel_port, tunnel_username, …
    """
    tunnel_host = (ssh_config or {}).get("tunnel_host")
    if not tunnel_host:
        return False, "No tunnel_host configured in ssh_config", None

    t0 = time.monotonic()
    try:
        tunnel_port = int((ssh_config or {}).get("tunnel_port") or 22)
        with _ssh_tunnel(ssh_config, tunnel_host, tunnel_port):
            return True, f"SSH tunnel to {tunnel_host} established successfully", int((time.monotonic() - t0) * 1000)
    except Exception as exc:
        return False, f"SSH tunnel failed: {exc}", None


# ============================================================================
# SSL helpers — build driver-specific TLS kwargs from ssl_config
# ============================================================================

def _pg_ssl_kwargs(ssl_config: dict) -> dict:
    """
    ssl_mode: disable|allow|prefer|require|verify-ca|verify-full (default require)
    ssl_ca, ssl_cert, ssl_key: PEM text written to temp files
    """
    import tempfile
    cfg = ssl_config or {}
    mode = cfg.get("ssl_mode") or "require"
    args: dict = {"sslmode": mode}
    for field, kwarg in (("ssl_ca", "sslrootcert"), ("ssl_cert", "sslcert"), ("ssl_key", "sslkey")):
        if cfg.get(field):
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="w")
            f.write(cfg[field]); f.close()
            args[kwarg] = f.name
    return args


def _mysql_ssl_kwargs(ssl_config: dict) -> dict:
    import tempfile
    cfg = ssl_config or {}
    ssl_dict: dict = {}
    for field, key in (("ssl_ca", "ca"), ("ssl_cert", "cert"), ("ssl_key", "key")):
        if cfg.get(field):
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="w")
            f.write(cfg[field]); f.close()
            ssl_dict[key] = f.name
    ssl_dict["check_hostname"] = cfg.get("ssl_mode") in ("verify-ca", "verify-full")
    return ssl_dict


# ============================================================================
# Per-database testers
# ============================================================================

def test_postgres_connection(
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    ssl_enabled: bool = False,
    ssl_config: Optional[dict] = None,
    ssh_config: Optional[dict] = None,
) -> ConnectionResult:
    """Test PostgreSQL via psycopg2, optionally through SSH tunnel and/or TLS."""
    try:
        import psycopg2
    except ImportError:
        return False, "psycopg2-binary driver not installed", None

    t0 = time.monotonic()
    try:
        with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
            args: dict = {
                "host": bind_host,
                "port": bind_port,
                "dbname": database_name,
                "user": username,
                "password": password,
                "connect_timeout": 10,
            }
            if ssl_enabled:
                args.update(_pg_ssl_kwargs(ssl_config or {}))
            conn = psycopg2.connect(**args)
            conn.close()
        return True, "Connection successful", int((time.monotonic() - t0) * 1000)
    except Exception as exc:
        return False, f"PostgreSQL connection failed: {exc}", None


def test_mysql_connection(
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    ssl_enabled: bool = False,
    ssl_config: Optional[dict] = None,
    ssh_config: Optional[dict] = None,
) -> ConnectionResult:
    """Test MySQL/MariaDB via PyMySQL, optionally through SSH tunnel and/or TLS."""
    try:
        import pymysql
    except ImportError:
        return False, "pymysql driver not installed", None

    t0 = time.monotonic()
    try:
        with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
            args: dict = {
                "host": bind_host,
                "port": bind_port,
                "database": database_name,
                "user": username,
                "password": password,
                "connect_timeout": 10,
            }
            if ssl_enabled:
                args["ssl"] = _mysql_ssl_kwargs(ssl_config or {})
            conn = pymysql.connect(**args)
            conn.close()
        return True, "Connection successful", int((time.monotonic() - t0) * 1000)
    except Exception as exc:
        return False, f"MySQL connection failed: {exc}", None


def test_mongodb_connection(
    host: str,
    port: int,
    database_name: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl_enabled: bool = False,
    ssl_config: Optional[dict] = None,
    ssh_config: Optional[dict] = None,
    extra_config: Optional[dict] = None,
) -> ConnectionResult:
    """Test MongoDB via PyMongo, optionally through SSH tunnel and/or TLS."""
    try:
        import pymongo
    except ImportError:
        return False, "pymongo driver not installed", None

    t0 = time.monotonic()
    try:
        from urllib.parse import quote_plus, unquote
        with _ssh_tunnel(ssh_config or {}, host, port) as (bind_host, bind_port):
            extra = extra_config or {}
            auth_source = extra.get("auth_source") or extra.get("authSource") or "admin"
            replica_set = extra.get("replica_set") or extra.get("replicaSet") or ""
            read_preference = extra.get("read_preference") or extra.get("readPreference") or ""
            extra_hosts = (extra.get("extra_hosts") or "").strip()
            extra_uri_params = (extra.get("extra_uri_params") or "").strip().lstrip("?&")
            # unquote first so already-encoded passwords (e.g. Devusr%40123) aren't double-encoded
            _user = quote_plus(unquote(username or ""))
            _pass = quote_plus(unquote(password or ""))
            # When using an SSH tunnel the bind address is localhost — pymongo must
            # NOT try to discover other replica set members (they aren't reachable
            # through the tunnel).  Force directConnection=true in that case.
            using_tunnel = bool((ssh_config or {}).get("tunnel_host"))
            # Build the host-list portion of the URI
            if using_tunnel or not extra_hosts:
                host_part = f"{bind_host}:{bind_port}"
            else:
                host_part = f"{bind_host}:{bind_port},{extra_hosts}"
            if username and password:
                uri = f"mongodb://{_user}:{_pass}@{host_part}/{database_name}?authSource={auth_source}"
            else:
                uri = f"mongodb://{host_part}/{database_name}?"
            # Topology hints
            if using_tunnel:
                uri += "&directConnection=true"
            elif extra_hosts or replica_set:
                # Multiple members or explicit replica set — let pymongo discover topology
                if replica_set:
                    uri += f"&replicaSet={replica_set}"
            else:
                uri += "&directConnection=true"
            if read_preference:
                uri += f"&readPreference={read_preference}"
            if extra_uri_params:
                uri += f"&{extra_uri_params}"
            if ssl_enabled:
                mode = (ssl_config or {}).get("ssl_mode", "require")
                uri += "&tls=true"
                uri += "&tlsAllowInvalidCertificates=" + ("false" if mode in ("verify-ca", "verify-full") else "true")
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
            client[database_name].command("ping")
            client.close()
        return True, "Connection successful", int((time.monotonic() - t0) * 1000)
    except Exception as exc:
        return False, f"MongoDB connection failed: {exc}", None


def test_connection(
    connector_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    ssl_enabled: bool = False,
    ssl_config: Optional[dict] = None,
    ssh_config: Optional[dict] = None,
    extra_config: Optional[dict] = None,
) -> ConnectionResult:
    """
    Route to the correct DB tester.

    ssl_config : TLS settings  (ssl_mode, ssl_ca, ssl_cert, ssl_key)
    ssh_config : SSH jump host (tunnel_host, tunnel_port, tunnel_username, …)

    Backwards-compat: if ssh_config is None but ssl_config has a tunnel_host,
    ssl_config is interpreted as ssh_config (old single-blob format).
    """
    # backwards-compat shim
    if ssh_config is None and (ssl_config or {}).get("tunnel_host"):
        ssh_config = ssl_config
        ssl_config = {}

    ct = (connector_type or "").lower()
    if ct in ("postgresql", "postgres", "postgres-wal"):
        return test_postgres_connection(host, port, database_name, username, password,
                                        ssl_enabled, ssl_config, ssh_config)
    elif ct in ("mysql", "mysql-binlog", "mariadb"):
        return test_mysql_connection(host, port, database_name, username, password,
                                     ssl_enabled, ssl_config, ssh_config)
    elif ct in ("mongodb", "mongo"):
        return test_mongodb_connection(host, port, database_name, username, password,
                                       ssl_enabled, ssl_config, ssh_config, extra_config)
    else:
        return False, f"Unsupported connector type: {connector_type!r}", None

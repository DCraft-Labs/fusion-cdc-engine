"""
Root-level conftest for control-plane tests.

SQLite compatibility patches
------------------------------
All models use PostgreSQL-specific column types (UUID, JSONB, ARRAY, etc.) and
server_defaults (uuid_generate_v4(), now()). SQLite supports neither.

We patch:
1. SQLiteTypeCompiler — render PG types as SQLite-compatible equivalents.
2. SQLAlchemy DDL event — strip PG function server_defaults before CREATE TABLE
   so SQLite doesn't choke on `DEFAULT uuid_generate_v4()`.
"""
import re
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler

# ── 1. Type compiler patches ────────────────────────────────────────────────

_type_patches = {
    "visit_UUID":     lambda self, t, **kw: "VARCHAR(36)",
    "visit_JSONB":    lambda self, t, **kw: "TEXT",
    "visit_JSON":     lambda self, t, **kw: "TEXT",
    "visit_ARRAY":    lambda self, t, **kw: "TEXT",
    "visit_INET":     lambda self, t, **kw: "VARCHAR(50)",
    "visit_CIDR":     lambda self, t, **kw: "VARCHAR(50)",
    "visit_TSVECTOR": lambda self, t, **kw: "TEXT",
    "visit_BYTEA":    lambda self, t, **kw: "BLOB",
}
for _name, _fn in _type_patches.items():
    if not hasattr(SQLiteTypeCompiler, _name):
        setattr(SQLiteTypeCompiler, _name, _fn)


# ── 2. Strip PG-function server_defaults on SQLite ─────────────────────────
# uuid_generate_v4(), gen_random_uuid(), now() etc. are Postgres-only.
# For SQLite tests we simply drop those server_defaults; SQLAlchemy will
# auto-fill UUID PKs from application side and timestamps will be NULL-able.

_PG_FUNCTIONS = re.compile(
    r"uuid_generate_v4\(\)|gen_random_uuid\(\)",
    re.IGNORECASE,
)
# now() → CURRENT_TIMESTAMP in SQLite
_PG_NOW = re.compile(r"\bnow\(\)", re.IGNORECASE)
# PostgreSQL cast operator — e.g. '{}'::jsonb, 'tenant'::character varying
_PG_CAST = re.compile(r"::[a-zA-Z_ ()]+", re.IGNORECASE)


def _sqlite_safe_default(text_val: str) -> str:
    """Translate PG server_default text to SQLite-compatible equivalent."""
    # Replace now() → CURRENT_TIMESTAMP
    text_val = _PG_NOW.sub("CURRENT_TIMESTAMP", text_val)
    # Strip ::type casts
    text_val = _PG_CAST.sub("", text_val)
    return text_val.strip()


@event.listens_for(Engine, "connect")
def _sqlite_strip_pg_defaults(dbapi_connection, connection_record):
    """Called on every new SQLite connection — nothing needed here, but we
    register the module-level table-event hooks once on first import."""


def _strip_pg_server_defaults(target, connection, **kw):
    """Before CREATE TABLE, remove server_defaults containing PG functions."""
    if "sqlite" not in connection.dialect.name:
        return
    from sqlalchemy.sql.schema import DefaultClause
    from sqlalchemy import text as sa_text
    for column in target.columns:
        if column.server_default is not None:
            arg = getattr(column.server_default, "arg", None)
            text_val = str(arg) if arg is not None else ""
            if _PG_FUNCTIONS.search(text_val):
                # UUID generators — handled by Python-side auto-fill in session event
                column.server_default = None
            elif _PG_NOW.search(text_val) or _PG_CAST.search(text_val):
                cleaned = _sqlite_safe_default(text_val)
                column.server_default = DefaultClause(sa_text(cleaned)) if cleaned else None


from sqlalchemy import Table
event.listen(Table, "before_create", _strip_pg_server_defaults)


# ── 3. Auto-generate UUIDs for UUID PKs on SQLite INSERT ───────────────────
# When we strip server_defaults (uuid_generate_v4()), inserting a model
# without an explicit PK would fail with NOT NULL constraint. This Core-level
# before_execute hook rewrites INSERT statements on SQLite to generate UUIDs.

from sqlalchemy import event as _sa_event
from sqlalchemy.dialects.postgresql import UUID as _PG_UUID


def _auto_uuid_before_flush(session, flush_context, instances):
    """Generate UUID PKs for new ORM objects that have UUID PK columns
    but no value set (relies on stripped server_default)."""
    import uuid as _uuid_lib
    from sqlalchemy import inspect as _sa_inspect
    for obj in list(session.new):
        try:
            mapper = _sa_inspect(type(obj))
        except Exception:
            continue
        for col in mapper.persist_selectable.primary_key:
            if isinstance(col.type, _PG_UUID) and getattr(obj, col.key, None) is None:
                setattr(obj, col.key, _uuid_lib.uuid4())


# Register on the Session class so it applies to all test sessions
from sqlalchemy.orm import Session as _OrmSession
_sa_event.listen(_OrmSession, "before_flush", _auto_uuid_before_flush)


# ── 4. Patch UUID bind/result processors for SQLite compatibility ───────────
# SQLAlchemy 2.x UUID.bind_processor() calls value.hex when as_uuid=True,
# which fails if the caller passes a plain string.  On SQLite (character-based
# storage) we simply want str(value) for bind and uuid.UUID(value) for result.

import uuid as _uuid_mod


def _patched_bind_processor(self, dialect):
    """Return a bind processor that accepts both uuid.UUID and plain strings."""
    def process(value):
        if value is None:
            return None
        if isinstance(value, _uuid_mod.UUID):
            return str(value)
        return str(value)  # already a string — pass through

    return process


def _patched_result_processor(self, dialect, coltype):
    """Return a result processor that converts stored strings to uuid.UUID."""
    if self.as_uuid:
        def process(value):
            if value is None:
                return None
            if isinstance(value, _uuid_mod.UUID):
                return value
            try:
                return _uuid_mod.UUID(str(value))
            except (TypeError, ValueError):
                return value
        return process
    return None


_PG_UUID.bind_processor = _patched_bind_processor
_PG_UUID.result_processor = _patched_result_processor

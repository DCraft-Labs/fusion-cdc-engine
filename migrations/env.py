from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url from DATABASE_URL environment variable if set,
# or construct it from individual POSTGRES_* env vars injected via Kubernetes secrets.
import sys
import os

_db_url = os.environ.get("DATABASE_URL", "")
if not _db_url or "localhost" in _db_url:
    _user = os.environ.get("POSTGRES_DB_USERNAME", "")
    _pass = os.environ.get("POSTGRES_DB_PASSWORD", "")
    _host = os.environ.get("POSTGRES_HOST", "localhost")
    _port = os.environ.get("POSTGRES_PORT", "5432")
    _db   = os.environ.get("POSTGRES_DB", "fusion_cdc_metadata")
    if _user and _pass:
        _db_url = f"postgresql://{_user}:{_pass}@{_host}:{_port}/{_db}"

if _db_url:
    config.set_main_option("sqlalchemy.url", _db_url)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'control-plane'))

from app.database import Base
from app.models import auth, connector, source_destination, connection, system, alerting

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

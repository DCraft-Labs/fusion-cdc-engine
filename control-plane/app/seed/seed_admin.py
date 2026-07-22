"""Self-healing CDC seed runner.

Executed on every control-plane startup (see `app.main.lifespan`) AFTER
Alembic migrations have applied. Checks `connector_definitions` and, if empty,
runs the baked-in `seed-admin.sql` to populate roles, admin user, connector
definitions, and sample source/destination/connection.

Design notes:
- The seed SQL is loaded from `seed-admin.sql` shipped next to this module
  (baked into the Docker image via `COPY control-plane/ ./`). This removes the
  dependency on `kubectl cp` / external file copying that caused the v1.2.1
  empty-seed regression.
- The seed SQL is idempotent (ON CONFLICT DO NOTHING / WHERE NOT EXISTS), so
  re-running on every boot is safe.
- Errors are logged loudly but DO NOT crash the app — the control-plane must
  still start so operators can debug. The seed will be retried on the next
  pod restart.
- `psql`-style `DO $$ ... $$;` blocks are executed via SQLAlchemy `text()`
  against the psycopg2 DBAPI; the whole DO block is one atomic transaction,
  so a partial failure rolls back cleanly.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

SEED_SQL_PATH = Path(__file__).parent / "seed-admin.sql"

# Module-level tracker for the last seed outcome, surfaced via the
# /api/v1/monitoring/health endpoint so operators can detect a failed
# auto-seed without scraping logs. Values:
#   "applied"      — seed ran and connector_definitions is now populated
#   "not_applied" — seed ran but failed / left connector_definitions empty
#   "skipped"      — seed skipped because DB was already populated
#   "not_run"      — run_seed() has not been called yet in this process
_SEED_STATUS: str = "not_run"


def get_seed_status() -> str:
    """Return the last auto-seed outcome for health-endpoint surfacing."""
    return _SEED_STATUS


def _load_seed_sql() -> str:
    """Read the baked-in seed SQL file. Called once per startup."""
    if not SEED_SQL_PATH.exists():
        raise FileNotFoundError(
            f"Seed SQL not found at {SEED_SQL_PATH}. "
            "The control-plane Docker image must ship control-plane/app/seed/seed-admin.sql."
        )
    return SEED_SQL_PATH.read_text(encoding="utf-8")


# Cache the SQL so we only read the file once per process.
_SEED_SQL_CACHE: Optional[str] = None


def get_seed_sql() -> str:
    global _SEED_SQL_CACHE
    if _SEED_SQL_CACHE is None:
        _SEED_SQL_CACHE = _load_seed_sql()
    return _SEED_SQL_CACHE


# Public alias used by app/seed/__init__.py
SEED_SQL: str = ""  # populated lazily via get_seed_sql()


def _connector_definitions_count(db: Session) -> int:
    """Return the number of rows in connector_definitions (0 = empty / needs seed)."""
    result = db.execute(text("SELECT COUNT(*) FROM connector_definitions"))
    count = result.scalar()
    return int(count) if count is not None else 0


def run_seed(db: Session, *, force: bool = False) -> bool:
    """Run the CDC seed against the metadata DB if connector_definitions is empty.

    Args:
        db: SQLAlchemy Session bound to the CDC metadata DB.
        force: If True, run the seed even if connector_definitions is non-empty
            (the seed SQL is idempotent, so this is safe — used for manual re-seed).

    Returns:
        True if the seed was executed (or attempted), False if it was skipped
        because the DB was already populated.

    Logs clearly on every path. Never raises — a seed failure must not crash
    the control-plane startup (operators need the API up to debug).
    """
    global _SEED_STATUS
    try:
        existing = _connector_definitions_count(db)
    except Exception as exc:
        # Most likely cause: the connector_definitions table doesn't exist yet
        # (Alembic migrations didn't run / failed). Log loudly and bail — the
        # app should still start so operators can inspect the migration state.
        logger.error(
            "Seed: could not read connector_definitions count — %s. "
            "Did Alembic migrations run? Skipping auto-seed.",
            exc,
            exc_info=True,
        )
        _SEED_STATUS = "not_applied"
        return False

    if not force and existing > 0:
        logger.info(
            "Seed: %d connector definition(s) already present, skipping auto-seed.",
            existing,
        )
        _SEED_STATUS = "skipped"
        return False

    logger.info(
        "Seed: connector_definitions empty%s, running seed...",
        " (force=True)" if force else "",
    )

    sql = get_seed_sql()
    try:
        # Execute the whole DO $$ ... $$; block as a single statement. The
        # block is atomic — if any INSERT fails, the entire block rolls back
        # and the DB stays empty (so the next startup will retry).
        db.execute(text(sql))
        db.commit()
    except Exception as exc:
        # Roll back the failed transaction so the session is reusable.
        try:
            db.rollback()
        except Exception:
            pass
        # Log loudly but DO NOT raise — the control-plane must still start.
        logger.error(
            "Seed: FAILED to apply seed SQL — %s. The CDC metadata DB was NOT seeded. "
            "The control-plane will start anyway so operators can debug. "
            "The seed will be retried on the next pod restart.",
            exc,
            exc_info=True,
        )
        _SEED_STATUS = "not_applied"
        return False

    # Verify the seed actually populated connector_definitions.
    try:
        after = _connector_definitions_count(db)
    except Exception:
        after = -1

    if after > 0:
        logger.info(
            "Seed: applied successfully — connector_definitions now has %d row(s).",
            after,
        )
        _SEED_STATUS = "applied"
    else:
        logger.error(
            "Seed: SQL executed without error but connector_definitions is still "
            "empty (%d). Inspect the seed SQL and the DB schema.",
            after,
        )
        _SEED_STATUS = "not_applied"
    return True


# ──────────────────────────────────────────────────────────────────────────
# Self-healing: ensure the seeded Iceberg destination has a non-empty
# connection_config. Prior releases (≤ v1.2.9) seeded this row with an
# empty config, leaving the destination a non-functional shell. This runs
# on EVERY startup (not just when the DB is empty) so existing clusters
# are repaired without a manual re-seed.
# ──────────────────────────────────────────────────────────────────────────
_ICEBERG_CONFIG_SQL = """
UPDATE destinations
SET connection_config = :cfg,
    connector_version = COALESCE(connector_version, '1.0.0'),
    status = COALESCE(status, 'active'),
    updated_at = NOW()
WHERE destination_name = 'Local Iceberg (MinIO + Nessie)'
  AND sub_tenant_id IS NULL
  AND (connection_config IS NULL OR connection_config = '{}'::jsonb);
"""

_ICEBERG_CONFIG_JSON = (
    '{'
    '"catalog_type": "rest",'
    '"catalog_name": "fusion_cdc",'
    '"namespace": "fusion",'
    '"catalog_uri": "http://nessie:19120",'
    '"nessie_ref": "main",'
    '"warehouse": "s3://iceberg-warehouse/",'
    '"s3_endpoint": "http://minio:9000",'
    '"s3_access_key_id": "minioadmin",'
    '"s3_secret_access_key": "minioadmin",'
    '"s3_region": "us-east-1",'
    '"s3_path_style": true,'
    '"auth_mode": "static",'
    '"format_version": 2,'
    '"parquet_compression": "zstd",'
    '"object_storage_enabled": true,'
    '"partitioned_paths": true,'
    '"cdc_apply_strategy": "upsert"'
    '}'
)


def ensure_iceberg_destination_config(db: Session) -> None:
    """Repair any seeded Iceberg destination row that has an empty config.

    Called on every control-plane startup (see ``app.main.lifespan``) so that
    clusters upgraded from v1.2.9 or earlier get the MinIO + Nessie config
    back-filled without requiring a manual re-seed.
    """
    try:
        result = db.execute(text(_ICEBERG_CONFIG_SQL), {"cfg": _ICEBERG_CONFIG_JSON})
        db.commit()
        if result.rowcount and result.rowcount > 0:
            logger.info(
                "Seed: repaired %d Iceberg destination row(s) with empty connection_config.",
                result.rowcount,
            )
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.warning(
            "Seed: could not repair Iceberg destination connection_config — %s",
            exc,
        )

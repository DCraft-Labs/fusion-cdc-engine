"""Self-healing CDC seed package.

Baked into the control-plane Docker image. On every startup, after Alembic
migrations, `app.main` calls `run_seed()` which checks whether
`connector_definitions` is empty and, if so, executes `seed-admin.sql` against
the metadata DB. This makes the deployment self-healing: no matter what
happens with deploy.ps1, kubectl cp, or postgres restarts, the control-plane
re-seeds itself whenever it starts and finds an empty DB.

See CHANGELOG v1.2.2 and `app/seed/seed_admin.py`.
"""

from app.seed.seed_admin import run_seed, SEED_SQL

__all__ = ["run_seed", "SEED_SQL"]

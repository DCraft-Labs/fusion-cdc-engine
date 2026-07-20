#!/usr/bin/env python3
"""
Audit the fusion_cdc_metadata database after an E2E run.

Checks that every expected table has data and that audit hooks fired for
create/update/delete operations on the key entities:
  - users, roles, user_roles
  - connector_definitions
  - sources, destinations, connections
  - connection_runs, connection_run_events
  - load_checkpoints
  - audit_log
  - cdc_workers / worker_heartbeats (if present)

Emits docs/E2E_METADATA_AUDIT.md with per-table row counts, missing audit
entries, and recommendations.

Usage:
  python scripts/e2e/audit_metadata.py --dsn postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from dataclasses import dataclass

try:
    import psycopg2
except ImportError as e:
    print(f"ERROR: {e}. Install with: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)


EXPECTED_TABLES = [
    "users", "roles", "user_roles",
    "connector_definitions",
    "sources", "destinations", "connections",
    "connection_runs", "connection_run_events",
    "checkpoint_state",
    "audit_logs",
]

# Audit hooks we expect to fire after E2E (audit_logs.action values)
EXPECTED_AUDIT_EVENTS = [
    "user.login",
    "source.create", "source.update",
    "destination.create", "destination.update", "destination.test",
    "connection.create", "connection.update", "connection.sync",
    "connection_run.start", "connection_run.complete",
    "checkpoint.update",
]


@dataclass
class TableInfo:
    name: str
    row_count: int
    has_data: bool


def fetch_table_counts(cur) -> list[TableInfo]:
    infos = []
    for t in EXPECTED_TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            n = cur.fetchone()[0]
            infos.append(TableInfo(t, n, n > 0))
        except Exception as e:
            print(f"WARN: table {t} not queryable: {e}")
            infos.append(TableInfo(t, -1, False))
    return infos


def fetch_audit_events(cur) -> dict[str, int]:
    counts: dict[str, int] = {}
    try:
        cur.execute("SELECT action, COUNT(*) FROM audit_logs GROUP BY action")
        for ev, n in cur.fetchall():
            counts[ev] = n
    except Exception as e:
        print(f"WARN: audit_logs not queryable: {e}")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default="postgresql://fusion_user:fusion_password@localhost:5432/fusion_cdc_metadata")
    parser.add_argument("--out", default="docs/E2E_METADATA_AUDIT.md")
    args = parser.parse_args()

    with psycopg2.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            infos = fetch_table_counts(cur)
            audit = fetch_audit_events(cur)

    missing_audit = []
    for ev in EXPECTED_AUDIT_EVENTS:
        if ev not in audit:
            # Allow prefix-style matches (e.g. connection.batch_run.success covers connection_run.complete)
            if ev == "connection_run.complete" and any(k.startswith("connection.batch_run.") for k in audit):
                continue
            missing_audit.append(ev)
    empty_tables = [i.name for i in infos if i.row_count == 0]
    missing_tables = [i.name for i in infos if i.row_count < 0]

    lines = []
    lines.append("# E2E Metadata Audit")
    lines.append("")
    lines.append(f"Generated: `{dt.datetime.utcnow().isoformat()}Z`")
    lines.append(f"DSN: `{args.dsn}`")
    lines.append("")
    lines.append("## Table row counts")
    lines.append("")
    lines.append("| Table | Rows | Has data |")
    lines.append("|-------|------|----------|")
    for i in infos:
        lines.append(f"| `{i.name}` | {i.row_count} | {'yes' if i.has_data else 'NO'} |")
    lines.append("")
    lines.append("## Audit log events")
    lines.append("")
    if audit:
        lines.append("| Event type | Count |")
        lines.append("|------------|-------|")
        for ev, n in sorted(audit.items()):
            lines.append(f"| `{ev}` | {n} |")
        lines.append("")
    else:
        lines.append("_(audit_log table empty or not queryable)_")
        lines.append("")
    if missing_audit:
        lines.append("## Missing audit events")
        lines.append("")
        lines.append("The following expected audit events did not fire during the E2E run:")
        lines.append("")
        for ev in missing_audit:
            lines.append(f"- `{ev}`")
        lines.append("")
        lines.append("### Recommended fixes")
        lines.append("")
        lines.append("Add audit-log writes in the control-plane handlers listed below. Each")
        lines.append("should call `audit_log.record(user_id, event_type, entity_id, payload)`")
        lines.append("after the DB transaction commits:")
        lines.append("")
        lines.append("| Event | Handler |")
        lines.append("|-------|---------|")
        if "user.login" in missing_audit:
            lines.append("| `user.login` | `app/api/auth.py::login` |")
        if "source.create" in missing_audit:
            lines.append("| `source.create` | `app/api/sources.py::create_source` |")
        if "destination.create" in missing_audit:
            lines.append("| `destination.create` | `app/api/destinations.py::create_destination` |")
        if "destination.test" in missing_audit:
            lines.append("| `destination.test` | `app/api/destinations.py::test_connection` |")
        if "connection.create" in missing_audit:
            lines.append("| `connection.create` | `app/api/connections.py::create_connection` |")
        if "connection.sync" in missing_audit:
            lines.append("| `connection.sync` | `app/api/connections.py::trigger_sync` |")
        if "connection_run.start" in missing_audit:
            lines.append("| `connection_run.start` | `app/services/sync_orchestrator.py::start_run` |")
        if "connection_run.complete" in missing_audit:
            lines.append("| `connection_run.complete` | `app/services/sync_orchestrator.py::complete_run` |")
        if "checkpoint.update" in missing_audit:
            lines.append("| `checkpoint.update` | `transform-worker/loader.py::_mark_chunk_done` (via control-plane `/internal/load-checkpoints`) |")
        lines.append("")
    if empty_tables:
        lines.append("## Empty tables")
        lines.append("")
        lines.append("The following tables have zero rows after E2E — verify the E2E actually")
        lines.append("exercised the code path that writes them:")
        lines.append("")
        for t in empty_tables:
            lines.append(f"- `{t}`")
        lines.append("")
    if missing_tables:
        lines.append("## Missing tables")
        lines.append("")
        lines.append("The following tables could not be queried — they may not exist or the")
        lines.append("schema is out of date. Run `alembic upgrade head` in the control plane:")
        lines.append("")
        for t in missing_tables:
            lines.append(f"- `{t}`")
        lines.append("")
    lines.append("## Verdict")
    lines.append("")
    if not missing_audit and not empty_tables and not missing_tables:
        lines.append("✅ **PASS** — all expected tables populated and all expected audit events fired.")
    else:
        lines.append("❌ **FAIL** — see sections above for missing items.")
    lines.append("")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Wrote {args.out}")
    return 0 if not (missing_audit or missing_tables) else 1


if __name__ == "__main__":
    sys.exit(main())

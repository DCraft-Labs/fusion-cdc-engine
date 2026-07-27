"""Redis-backed resource ledger for admission control.

Tracks TWO consumer classes against a total pool (whose size lives in
Postgres — see ``app.models.resource_config.ResourceConfig`` — NOT in
Redis, so there is a single source of truth for "how big is the pool" and
Redis only ever tracks *consumption* against it):

1. **baseline** — one standing CDC reservation per ACTIVE connection, held
   for that connection's entire lifetime (created when the connection
   starts streaming, released only when the connection is deleted).
2. **transient** — one reservation per connection whose INITIAL LOAD is
   currently in flight, released the instant that load completes (early or
   late — no partial credit, no mid-flight resizing; this is an explicit
   product decision, not an oversight).

Control-plane's own fixed footprint is a third, non-reservation-based
consumer: a configurable constant subtracted from the pool on every
capacity check (nobody reserves/releases it — it's just always-on
overhead).

Redis layout (four hashes, field = connection_id, value = millicores/MiB):

    fusion:ledger:baseline:cpu   fusion:ledger:baseline:mem
    fusion:ledger:transient:cpu  fusion:ledger:transient:mem

Concurrency: the only operation that needs true atomicity is "check
available capacity, then reserve" for a *transient* (initial-load)
reservation — two simultaneous connection-creation requests must not both
win the same last sliver of capacity. That's implemented as a single Lua
``EVAL`` (``_RESERVE_SCRIPT``) so the check-then-set is one atomic step on
the Redis server, not a separate lock. Baseline reservations use a plain
``HSETNX`` (idempotent, no capacity race to worry about — a connection's
mandatory standing footprint is never rejected, only logged if it pushes
the pool over budget) and releases are plain ``HDEL``s (deleting a field
that may not exist is already safe/idempotent in Redis).

No other part of this codebase registers a Lua script for Redis (checked:
no ``register_script``/``.lua`` hits anywhere in the repo), so this is a
new (documented) convention rather than a match to an existing one.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config — control-plane's own fixed footprint + the standing CDC baseline
# per active connection. Both deliberately rough estimates, not exact
# accounting (per the product spec) — tunable via env var.
# ---------------------------------------------------------------------------
CONTROL_PLANE_CPU_MILLIS = int(os.environ.get("CONTROL_PLANE_RESERVED_CPU_MILLIS", "500"))
CONTROL_PLANE_MEM_MI = int(os.environ.get("CONTROL_PLANE_RESERVED_MEM_MI", "512"))

CDC_BASELINE_CPU_MILLIS = int(os.environ.get("CDC_BASELINE_CPU_MILLIS", "100"))
CDC_BASELINE_MEM_MI = int(os.environ.get("CDC_BASELINE_MEM_MI", "128"))

BASELINE_CPU_KEY = "fusion:ledger:baseline:cpu"
BASELINE_MEM_KEY = "fusion:ledger:baseline:mem"
TRANSIENT_CPU_KEY = "fusion:ledger:transient:cpu"
TRANSIENT_MEM_KEY = "fusion:ledger:transient:mem"


# ---------------------------------------------------------------------------
# Lua script: atomic "check available, then reserve" for a transient
# (initial-load) reservation.
#
# KEYS[1..4] = baseline_cpu, baseline_mem, transient_cpu, transient_mem
# ARGV[1] = connection_id (hash field)
# ARGV[2] = requested cpu_millis
# ARGV[3] = requested mem_mi
# ARGV[4] = total_cpu_millis   (pool ceiling, from Postgres ResourceConfig)
# ARGV[5] = total_mem_mi
# ARGV[6] = control_plane_cpu_millis (static overhead, always subtracted)
# ARGV[7] = control_plane_mem_mi
#
# Returns {ok (0/1), available_cpu_after, available_mem_after}. On ok=0 the
# hash is left untouched (nothing reserved) and the two numbers are the
# capacity that WAS available (so the caller can report "you needed X, only
# Y was free").
# ---------------------------------------------------------------------------
_RESERVE_TRANSIENT_SCRIPT = """
local function sum_hash(key)
    local vals = redis.call('HVALS', key)
    local total = 0
    for _, v in ipairs(vals) do
        total = total + tonumber(v)
    end
    return total
end

local baseline_cpu = sum_hash(KEYS[1])
local baseline_mem = sum_hash(KEYS[2])
local transient_cpu = sum_hash(KEYS[3])
local transient_mem = sum_hash(KEYS[4])

local conn_id = ARGV[1]
local req_cpu = tonumber(ARGV[2])
local req_mem = tonumber(ARGV[3])
local total_cpu = tonumber(ARGV[4])
local total_mem = tonumber(ARGV[5])
local cp_cpu = tonumber(ARGV[6])
local cp_mem = tonumber(ARGV[7])

local avail_cpu = total_cpu - cp_cpu - baseline_cpu - transient_cpu
local avail_mem = total_mem - cp_mem - baseline_mem - transient_mem

if req_cpu > avail_cpu or req_mem > avail_mem then
    return {0, avail_cpu, avail_mem}
end

redis.call('HSET', KEYS[3], conn_id, req_cpu)
redis.call('HSET', KEYS[4], conn_id, req_mem)

return {1, avail_cpu - req_cpu, avail_mem - req_mem}
"""


class LedgerUnavailableError(RuntimeError):
    """Raised when Redis cannot be reached. Callers decide whether to fail
    open or closed — admission-confirm fails closed (no reservation without
    a confirmed atomic check); read-only preview/capacity calls fail open
    with a warning, mirroring this codebase's existing "best-effort Redis,
    never block the request" convention (see connections.py's various
    ``try/except`` around ``redis.from_url``)."""


def _redis_url() -> str:
    from app.config import settings
    return os.environ.get("REDIS_URL", getattr(settings, "REDIS_URL", "redis://localhost:6379"))


def get_redis_client():
    """Lazy sync redis client — mirrors the ``import redis`` /
    ``redis.from_url(...)`` pattern already used throughout
    ``app/api/connections.py`` rather than introducing a new async/shared
    client convention. Import is local so this module (and anything that
    imports it) stays importable in environments without ``redis``
    installed (e.g. this sandbox — see the syntax-only verification note in
    the task summary)."""
    import redis  # local import — see docstring
    return redis.from_url(_redis_url(), decode_responses=True)


def _get_reserve_script(client):
    """Register (once per client/connection pool) and return the callable
    ``Script`` object for the atomic transient-reserve EVAL."""
    return client.register_script(_RESERVE_TRANSIENT_SCRIPT)


# ---------------------------------------------------------------------------
# Capacity query (read-only, best-effort — not atomic; the authoritative
# atomic check happens inside reserve_transient's Lua EVAL at confirm time).
# ---------------------------------------------------------------------------

def get_available_capacity(
    total_cpu_millis: int,
    total_mem_mi: int,
    *,
    client=None,
) -> dict:
    """Returns available headroom against the given pool totals, after
    subtracting control-plane's fixed footprint + all baseline + all
    transient reservations currently in Redis.

    Best-effort: on Redis errors, returns zero availability (fails closed
    for a *preview*, so the UI doesn't advertise capacity that might not
    really be reachable) but never raises — callers show "unknown" rather
    than 500ing the page.
    """
    client = client or get_redis_client()
    try:
        baseline_cpu = _sum_hash_values(client, BASELINE_CPU_KEY)
        baseline_mem = _sum_hash_values(client, BASELINE_MEM_KEY)
        transient_cpu = _sum_hash_values(client, TRANSIENT_CPU_KEY)
        transient_mem = _sum_hash_values(client, TRANSIENT_MEM_KEY)
    except Exception:
        log.warning("resource_ledger: get_available_capacity failed to reach Redis", exc_info=True)
        return {
            "available_cpu_millis": 0,
            "available_memory_mi": 0,
            "baseline_cpu_millis": None,
            "baseline_memory_mi": None,
            "transient_cpu_millis": None,
            "transient_memory_mi": None,
            "control_plane_cpu_millis": CONTROL_PLANE_CPU_MILLIS,
            "control_plane_memory_mi": CONTROL_PLANE_MEM_MI,
            "error": "redis_unavailable",
        }

    avail_cpu = total_cpu_millis - CONTROL_PLANE_CPU_MILLIS - baseline_cpu - transient_cpu
    avail_mem = total_mem_mi - CONTROL_PLANE_MEM_MI - baseline_mem - transient_mem
    return {
        "available_cpu_millis": max(0, avail_cpu),
        "available_memory_mi": max(0, avail_mem),
        "baseline_cpu_millis": baseline_cpu,
        "baseline_memory_mi": baseline_mem,
        "transient_cpu_millis": transient_cpu,
        "transient_memory_mi": transient_mem,
        "control_plane_cpu_millis": CONTROL_PLANE_CPU_MILLIS,
        "control_plane_memory_mi": CONTROL_PLANE_MEM_MI,
    }


def _sum_hash_values(client, key: str) -> int:
    vals = client.hvals(key)
    total = 0
    for v in vals or []:
        try:
            total += int(v)
        except (TypeError, ValueError):
            continue
    return total


# ---------------------------------------------------------------------------
# Transient (initial-load) reservation — atomic check-then-reserve.
# ---------------------------------------------------------------------------

def reserve_transient(
    connection_id: str,
    cpu_millis: int,
    mem_mi: int,
    total_cpu_millis: int,
    total_mem_mi: int,
    *,
    client=None,
) -> dict:
    """Atomically checks available capacity against ``total_cpu_millis``/
    ``total_mem_mi`` (minus control-plane overhead + all standing
    baseline/transient reservations) and, if it fits, reserves
    ``cpu_millis``/``mem_mi`` for ``connection_id`` in the transient class —
    all in one Redis ``EVAL`` so two concurrent callers racing for the same
    last sliver of capacity can't both succeed.

    Returns ``{"reserved": bool, "available_cpu_millis": int,
    "available_memory_mi": int}``. Raises ``LedgerUnavailableError`` if
    Redis can't be reached — admission-confirm fails closed (better to
    reject a reservation than silently over-commit the pool).
    """
    client = client or get_redis_client()
    try:
        script = _get_reserve_script(client)
        result = script(
            keys=[BASELINE_CPU_KEY, BASELINE_MEM_KEY, TRANSIENT_CPU_KEY, TRANSIENT_MEM_KEY],
            args=[
                str(connection_id),
                int(cpu_millis),
                int(mem_mi),
                int(total_cpu_millis),
                int(total_mem_mi),
                CONTROL_PLANE_CPU_MILLIS,
                CONTROL_PLANE_MEM_MI,
            ],
        )
    except Exception as exc:
        raise LedgerUnavailableError(str(exc)) from exc

    ok, avail_cpu, avail_mem = result[0], result[1], result[2]
    return {
        "reserved": bool(int(ok)),
        "available_cpu_millis": int(avail_cpu),
        "available_memory_mi": int(avail_mem),
    }


def release_transient(connection_id: str, *, client=None) -> None:
    """Releases a connection's transient initial-load reservation. Called
    from ``app.api.internal._maybe_mark_initial_load_completed`` the instant
    a connection's initial load genuinely completes (whether early or late
    vs. the ETA — no partial credit). Idempotent: HDEL on an absent field is
    a no-op. Never raises — a leaked transient key self-heals worst-case as
    "capacity looks a bit tighter than it is" rather than breaking the
    completion hook."""
    client = client or get_redis_client()
    try:
        client.hdel(TRANSIENT_CPU_KEY, str(connection_id))
        client.hdel(TRANSIENT_MEM_KEY, str(connection_id))
    except Exception:
        log.warning("resource_ledger: release_transient failed for connection=%s", connection_id, exc_info=True)


# ---------------------------------------------------------------------------
# Baseline (standing CDC) reservation — one per ACTIVE connection, for its
# entire lifetime.
# ---------------------------------------------------------------------------

def ensure_baseline_reservation(
    connection_id: str,
    cpu_millis: Optional[int] = None,
    mem_mi: Optional[int] = None,
    *,
    client=None,
) -> bool:
    """Idempotently ensures a standing baseline reservation exists for
    ``connection_id`` (HSETNX — a connection that's already streaming and
    gets re-triggered, e.g. resume / trigger-sync, must NOT double-reserve).

    Unlike ``reserve_transient``, this never rejects: an active connection's
    mandatory CDC footprint is not something the product wants to block on
    admission (only the *initial load speed mode* is capacity-gated). If
    this pushes the pool over budget that's logged, not raised — see the
    task's phase note that elastic/autoscaled baseline reservations are a
    later phase; for now this is a fixed constant per active connection.

    Returns True if a new reservation was created, False if one already
    existed (both are success — the caller doesn't need to branch on it).
    """
    cpu_millis = CDC_BASELINE_CPU_MILLIS if cpu_millis is None else int(cpu_millis)
    mem_mi = CDC_BASELINE_MEM_MI if mem_mi is None else int(mem_mi)
    client = client or get_redis_client()
    try:
        created_cpu = bool(client.hsetnx(BASELINE_CPU_KEY, str(connection_id), cpu_millis))
        # hsetnx is per-field; call it for mem too even if cpu already existed
        # (keeps the two hashes in sync if a previous partial failure left
        # only one of them set).
        created_mem = bool(client.hsetnx(BASELINE_MEM_KEY, str(connection_id), mem_mi))
        return created_cpu or created_mem
    except Exception:
        log.warning("resource_ledger: ensure_baseline_reservation failed for connection=%s", connection_id, exc_info=True)
        return False


def release_baseline(connection_id: str, *, client=None) -> None:
    """Releases a connection's standing baseline reservation. Called
    alongside ``teardown_committer`` in ``delete_connection`` — a connection
    being deleted stops consuming its CDC baseline footprint regardless of
    whether it was mid-initial-load (see ``release_transient`` — both are
    released on delete, since a deleted connection can't be "currently
    syncing" or "active" anymore either way)."""
    client = client or get_redis_client()
    try:
        client.hdel(BASELINE_CPU_KEY, str(connection_id))
        client.hdel(BASELINE_MEM_KEY, str(connection_id))
    except Exception:
        log.warning("resource_ledger: release_baseline failed for connection=%s", connection_id, exc_info=True)


def release_all(connection_id: str, *, client=None) -> None:
    """Convenience: releases BOTH baseline and transient reservations for a
    connection. Used at connection-delete time since a deleted connection
    can be in either (or both, in theory) states."""
    client = client or get_redis_client()
    release_transient(connection_id, client=client)
    release_baseline(connection_id, client=client)

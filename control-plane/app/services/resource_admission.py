"""Admission-control business logic: row-count -> tier, tier -> resource
footprint, mode (aggressive/normal/saver) -> parallelism/footprint/ETA, and
the orchestration that ties ``partition_with_estimates`` (row estimation) +
``resource_ledger`` (the Redis reservation ledger) together for the
``admission-preview`` / ``admission-confirm`` endpoints in
``app.api.resource_config``.

Kept separate from ``resource_ledger.py`` (pure Redis plumbing) so the
tier/mode math is unit-testable with zero Redis/DB dependency.
"""
from __future__ import annotations

import logging
import math
import os
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tiers — thresholds are tunable constants (not magic numbers sprinkled
# through the code), intentionally consistent with the existing
# AUTO_BULK_MODE_ROW_THRESHOLD precedent in connections.py (1,000,000 rows
# as the "needs the fast path" boundary — the S/M boundary here is the same
# number for the same reason).
# ---------------------------------------------------------------------------
TIER_S_MAX_ROWS = int(os.environ.get("ADMISSION_TIER_S_MAX_ROWS", "1000000"))       # < 1M    -> S
TIER_M_MAX_ROWS = int(os.environ.get("ADMISSION_TIER_M_MAX_ROWS", "20000000"))      # 1M-20M  -> M
TIER_L_MAX_ROWS = int(os.environ.get("ADMISSION_TIER_L_MAX_ROWS", "200000000"))     # 20M-200M -> L
# > TIER_L_MAX_ROWS -> XL

TIERS = ("S", "M", "L", "XL")


def resolve_tier(rows_estimated_total: Optional[int]) -> str:
    """Rows -> tier. ``None``/unknown estimate defaults to "M" (today's
    existing default committer footprint — a safe middle ground rather than
    silently under- or over-provisioning when we simply don't know)."""
    if rows_estimated_total is None:
        return "M"
    try:
        rows = int(rows_estimated_total)
    except (TypeError, ValueError):
        return "M"
    if rows < TIER_S_MAX_ROWS:
        return "S"
    if rows < TIER_M_MAX_ROWS:
        return "M"
    if rows < TIER_L_MAX_ROWS:
        return "L"
    return "XL"


# ---------------------------------------------------------------------------
# Per-tier base resource footprint (one worker's worth, before mode/K
# scaling). Anchored on committer_provisioner.py's own
# _CPU_REQUEST/_MEM_REQUEST ("250m"/"512Mi") for M — "today's existing
# default committer footprint" — and _CPU_LIMIT/_MEM_LIMIT ("2000m"/"2048Mi")
# for XL (the ceiling that constant was already designed to allow up to).
# S and L interpolate between/around those. All tunable via env var.
# ---------------------------------------------------------------------------
TIER_BASE_CPU_MILLIS = {
    "S": int(os.environ.get("ADMISSION_TIER_S_CPU_MILLIS", "125")),
    "M": int(os.environ.get("ADMISSION_TIER_M_CPU_MILLIS", "250")),   # == committer _CPU_REQUEST
    "L": int(os.environ.get("ADMISSION_TIER_L_CPU_MILLIS", "1000")),
    "XL": int(os.environ.get("ADMISSION_TIER_XL_CPU_MILLIS", "2000")),  # == committer _CPU_LIMIT
}
TIER_BASE_MEM_MI = {
    "S": int(os.environ.get("ADMISSION_TIER_S_MEM_MI", "256")),
    "M": int(os.environ.get("ADMISSION_TIER_M_MEM_MI", "512")),   # == committer _MEM_REQUEST
    "L": int(os.environ.get("ADMISSION_TIER_L_MEM_MI", "1024")),
    "XL": int(os.environ.get("ADMISSION_TIER_XL_MEM_MI", "2048")),  # == committer _MEM_LIMIT
}

# A partition isn't worth its own worker below this many rows — caps how
# much parallelism aggressive/normal modes ask for on small tables even
# though MAX_PARALLELISM would otherwise allow more.
MIN_ROWS_PER_PARTITION = int(os.environ.get("ADMISSION_MIN_ROWS_PER_PARTITION", "50000"))

# Mirror connections.py's own K bounds (see _connection_parallelism /
# DEFAULT_PARALLELISM / MAX_PARALLELISM, ~lines 53-55, 537-557) rather than
# redefining independent ones — imported lazily inside functions below to
# avoid a module-load-time circular import between app.api.connections and
# app.services.resource_admission (connections.py will call into this
# module's confirm/release helpers).
_FALLBACK_DEFAULT_PARALLELISM = 4
_FALLBACK_MAX_PARALLELISM = 16


def _connection_parallelism_bounds() -> tuple[int, int]:
    try:
        from app.api.connections import DEFAULT_PARALLELISM, MAX_PARALLELISM
        return DEFAULT_PARALLELISM, MAX_PARALLELISM
    except Exception:
        # Only hit if connections.py fails to import for unrelated reasons;
        # keep admission math functional regardless.
        return _FALLBACK_DEFAULT_PARALLELISM, _FALLBACK_MAX_PARALLELISM


# ---------------------------------------------------------------------------
# Modes — aggressive/normal/saver are resource multipliers/allocations for
# the SAME connection, not different code paths. Saver is genuinely K=1 (no
# partition parallelism) with the smallest viable footprint and no time
# guarantee; aggressive asks for up to MAX_PARALLELISM (capped by how many
# MIN_ROWS_PER_PARTITION-sized chunks the table actually has); normal is the
# existing DEFAULT_PARALLELISM.
# ---------------------------------------------------------------------------
MODES = ("aggressive", "normal", "saver")

# overhead_multiplier: extra per-worker footprint headroom for that mode
# (aggressive workers run "hot" so get a little more than 1x each; saver
# deliberately asks for LESS than a full tier-worker's worth since it's a
# single, unhurried worker).
MODE_OVERHEAD_MULTIPLIER = {
    "aggressive": float(os.environ.get("ADMISSION_MODE_AGGRESSIVE_OVERHEAD", "1.0")),
    "normal": float(os.environ.get("ADMISSION_MODE_NORMAL_OVERHEAD", "1.0")),
    "saver": float(os.environ.get("ADMISSION_MODE_SAVER_OVERHEAD", "0.75")),
}


def _mode_parallelism(mode: str, rows_estimated_total: Optional[int]) -> int:
    """K for this mode, capped so tiny tables don't over-provision even in
    aggressive mode. ``saver`` is always exactly 1 — "no partition
    parallelism" is the defining property of saver mode, not just a small
    number (see ``_connection_parallelism`` in connections.py, which this
    integrates with by writing ``resource_limits.parallelism = 1`` at
    confirm time rather than adding a second, competing K mechanism)."""
    if mode == "saver":
        return 1

    default_k, max_k = _connection_parallelism_bounds()
    rows_cap = max(1, math.ceil((rows_estimated_total or 0) / MIN_ROWS_PER_PARTITION)) if rows_estimated_total else max_k

    if mode == "aggressive":
        return max(1, min(max_k, rows_cap))
    # normal
    return max(1, min(default_k, rows_cap))


def mode_resource_requirement(tier: str, mode: str, rows_estimated_total: Optional[int]) -> dict:
    """Returns ``{"cpu_millis", "memory_mi", "parallelism"}`` — the capacity
    this (tier, mode) combination would need to reserve for the initial
    load. Scales the tier's one-worker base footprint by K (roughly: K
    concurrent partition workers each need about a tier-worker's worth of
    resources — mirrors the committer's own drain_batch-scales-with-K
    heuristic in committer_provisioner.py) and the mode's overhead
    multiplier."""
    base_cpu = TIER_BASE_CPU_MILLIS.get(tier, TIER_BASE_CPU_MILLIS["M"])
    base_mem = TIER_BASE_MEM_MI.get(tier, TIER_BASE_MEM_MI["M"])
    k = _mode_parallelism(mode, rows_estimated_total)
    overhead = MODE_OVERHEAD_MULTIPLIER.get(mode, 1.0)
    return {
        "cpu_millis": max(1, round(base_cpu * k * overhead)),
        "memory_mi": max(1, round(base_mem * k * overhead)),
        "parallelism": k,
    }


# ---------------------------------------------------------------------------
# ETA — seeded from an admin-tunable rows/sec-per-resource-unit baseline.
# Anchors: committer_provisioner.py's own load-test note ("K=6 parallelism +
# drainBatch=1000 sustained ~74k-97k rows/sec end-to-end on a 35.86M-row
# table") and connections.py's bulk-mode comment ("duckdb ~97k rows/sec vs
# python ~55k rows/sec" on the same table) give a per-worker throughput of
# roughly 97000/6 ~= 14000 rows/sec (duckdb) and 55000/6 ~= 8000 rows/sec
# (python) at that K. Used as the DEFAULT per-worker baseline; env-
# overridable, and see ``record_observed_throughput`` below for the
# live-correction stretch goal.
# ---------------------------------------------------------------------------
BASELINE_ROWS_PER_SEC_DUCKDB = float(os.environ.get("INITIAL_LOAD_BASELINE_ROWS_PER_SEC_DUCKDB", "14000"))
BASELINE_ROWS_PER_SEC_PYTHON = float(os.environ.get("INITIAL_LOAD_BASELINE_ROWS_PER_SEC_PYTHON", "8000"))


def _resolve_bulk_mode_for_estimate(rows_estimated_total: Optional[int]) -> str:
    """Mirrors connections.py's ``_resolve_bulk_mode`` "auto" resolution
    (>= AUTO_BULK_MODE_ROW_THRESHOLD -> duckdb, else python) using that same
    module's threshold constant, imported lazily to avoid a load-time
    circular import."""
    try:
        from app.api.connections import AUTO_BULK_MODE_ROW_THRESHOLD
    except Exception:
        AUTO_BULK_MODE_ROW_THRESHOLD = 1_000_000
    if rows_estimated_total is None:
        return "python"
    try:
        return "duckdb" if int(rows_estimated_total) >= AUTO_BULK_MODE_ROW_THRESHOLD else "python"
    except (TypeError, ValueError):
        return "python"


def estimate_eta_seconds(rows_estimated_total: Optional[int], parallelism: int, *, client=None) -> tuple[int, str]:
    """Returns ``(eta_seconds, bulk_mode)``. ``eta_seconds`` = rows /
    (per-worker rows/sec * K). Per-worker rate is live-corrected against
    recently observed throughput when available (see
    ``record_observed_throughput``), else falls back to the static
    baseline constants above."""
    rows = int(rows_estimated_total) if rows_estimated_total else 0
    bulk_mode = _resolve_bulk_mode_for_estimate(rows_estimated_total)
    per_worker_rate = get_observed_or_baseline_rate(bulk_mode, client=client)
    k = max(1, int(parallelism or 1))
    if rows <= 0:
        return 0, bulk_mode
    eta = math.ceil(rows / max(1.0, per_worker_rate * k))
    return int(eta), bulk_mode


# ---------------------------------------------------------------------------
# Stretch goal: live ETA correction against actually-observed throughput.
#
# Mirrors the SHAPE of transform-worker/loader.py's ADAPTIVE_MIN_CHUNK /
# ADAPTIVE_MAX_CHUNK doubling/halving idea (adapt a starting guess from
# observation) but applied to rows/sec instead of chunk size, and using
# simple exponential smoothing rather than streak-gated doubling/halving —
# an ETA correction doesn't need the same "avoid thrashing on one noisy
# sample" guard a chunk-size knob does, since we're not mutating an
# in-flight load's behavior (per the product's explicit "no mid-flight
# resizing" decision), just recording a better prior for the NEXT preview.
#
# Stored in Redis as a small hash (``fusion:admission:observed_rate``,
# field = bulk_mode) so it's shared across control-plane replicas. Best-
# effort: falls back to the static baseline on any Redis error, and is
# never required for admission-preview/confirm to function.
# ---------------------------------------------------------------------------
OBSERVED_RATE_KEY = "fusion:admission:observed_rate"
OBSERVED_RATE_EMA_ALPHA = float(os.environ.get("ADMISSION_OBSERVED_RATE_EMA_ALPHA", "0.3"))


def get_observed_or_baseline_rate(bulk_mode: str, *, client=None) -> float:
    baseline = BASELINE_ROWS_PER_SEC_DUCKDB if bulk_mode == "duckdb" else BASELINE_ROWS_PER_SEC_PYTHON
    try:
        from app.services.resource_ledger import get_redis_client
        client = client or get_redis_client()
        raw = client.hget(OBSERVED_RATE_KEY, bulk_mode)
        if raw:
            return max(1.0, float(raw))
    except Exception:
        log.debug("resource_admission: no observed rate for bulk_mode=%s, using baseline", bulk_mode, exc_info=True)
    return baseline


def record_observed_throughput(bulk_mode: str, rows_written: int, elapsed_seconds: float, parallelism: int = 1, *, client=None) -> None:
    """Call after an initial load completes with the ACTUAL rows_written /
    elapsed_seconds / parallelism it ran with, to correct future ETA
    estimates for this bulk_mode. Per-worker observed rate =
    (rows_written / elapsed_seconds) / parallelism, blended into the stored
    rate via EMA (new = alpha*observed + (1-alpha)*old) so one anomalous
    run doesn't whiplash the estimate. Best-effort / never raises — this is
    explicitly a stretch goal, not on the critical path of release/billing
    correctness."""
    if elapsed_seconds <= 0 or rows_written <= 0:
        return
    observed_per_worker = (rows_written / elapsed_seconds) / max(1, parallelism)
    try:
        from app.services.resource_ledger import get_redis_client
        client = client or get_redis_client()
        current = client.hget(OBSERVED_RATE_KEY, bulk_mode)
        if current:
            prior = max(1.0, float(current))
            new_rate = OBSERVED_RATE_EMA_ALPHA * observed_per_worker + (1 - OBSERVED_RATE_EMA_ALPHA) * prior
        else:
            new_rate = observed_per_worker
        client.hset(OBSERVED_RATE_KEY, bulk_mode, new_rate)
    except Exception:
        log.warning("resource_admission: record_observed_throughput failed to persist for bulk_mode=%s", bulk_mode, exc_info=True)

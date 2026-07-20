"""
P2 Metrics — Prometheus metrics catalogue for CDC workers.

Implements all metrics from the spec (Section 15.1):
  cdc_events_total          Counter   (tenant, source, stream, op)
  cdc_stream_lag_seconds    Gauge     (tenant, source, stream)
  cdc_stream_throughput_per_second  Gauge (tenant, source, stream)
  cdc_errors_total          Counter   (tenant, source, error_type)
  cdc_checkpoint_age_seconds Gauge    (tenant, source)
  cdc_sqlite_checkpoint_size_bytes   Gauge (tenant, source)
  cdc_fallback_queue_length  Gauge    (tenant, source)

Also exposes a /metrics HTTP endpoint on METRICS_PORT (default 8080).

Usage:
    from cdc_worker.metrics import METRICS
    METRICS.events_total.labels(tenant="t1", source="src1", stream="public.orders", op="c").inc()
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Try to import prometheus_client; fall back to no-ops so tests don't require it
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server as _start_http
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    log.warning("prometheus_client not installed — metrics disabled")

    class _Noop:
        """Drop-in replacement when prometheus_client is absent."""
        def labels(self, **kwargs):
            return self
        def inc(self, amount=1):
            pass
        def dec(self, amount=1):
            pass
        def set(self, value):
            pass
        def observe(self, value):
            pass
        def time(self):
            import contextlib
            return contextlib.nullcontext()

    class _NoopMetricFactory:
        def __call__(self, *args, **kwargs):
            return _Noop()

    Counter = Gauge = Histogram = _NoopMetricFactory()

    def _start_http(port):
        pass


# ---------------------------------------------------------------------------
# Metric definitions
# ---------------------------------------------------------------------------

class CDCMetrics:
    """Container for all CDC Prometheus metrics."""

    def __init__(self) -> None:
        self.events_total = Counter(
            "cdc_events_total",
            "Total CDC events processed",
            ["tenant", "source", "stream", "op"],
        )

        self.stream_lag_seconds = Gauge(
            "cdc_stream_lag_seconds",
            "Lag between source event time and now (seconds)",
            ["tenant", "source", "stream"],
        )

        self.stream_throughput = Gauge(
            "cdc_stream_throughput_per_second",
            "Events per second for a CDC stream",
            ["tenant", "source", "stream"],
        )

        self.errors_total = Counter(
            "cdc_errors_total",
            "CDC errors by type",
            ["tenant", "source", "error_type"],
        )

        self.checkpoint_age_seconds = Gauge(
            "cdc_checkpoint_age_seconds",
            "Seconds since last successful checkpoint flush",
            ["tenant", "source"],
        )

        self.sqlite_checkpoint_size_bytes = Gauge(
            "cdc_sqlite_checkpoint_size_bytes",
            "Size of the local SQLite checkpoint file in bytes",
            ["tenant", "source"],
        )

        self.fallback_queue_length = Gauge(
            "cdc_fallback_queue_length",
            "Number of events waiting in the fallback disk queue",
            ["tenant", "source"],
        )

        self.fallback_queue_drained_total = Counter(
            "cdc_fallback_queue_drained_total",
            "Total events drained from the fallback queue back to Redis",
            ["tenant", "source"],
        )

        # Throughput tracking (internal, not exported)
        self._event_times: dict = {}

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def record_event(
        self,
        tenant: str,
        source: str,
        schema: str,
        table: str,
        op: str,
        ts_ms: int,
    ) -> None:
        """Record one processed event and update lag + throughput."""
        stream = f"{schema}.{table}"
        self.events_total.labels(tenant=tenant, source=source, stream=stream, op=op).inc()

        lag = (time.time() * 1000 - ts_ms) / 1000.0
        self.stream_lag_seconds.labels(tenant=tenant, source=source, stream=stream).set(lag)

        # Rolling throughput (events in last second)
        key = (tenant, source, stream)
        now = time.time()
        bucket = self._event_times.setdefault(key, [])
        bucket.append(now)
        # Prune events older than 1 second
        self._event_times[key] = [t for t in bucket if now - t < 1.0]
        self.stream_throughput.labels(tenant=tenant, source=source, stream=stream).set(
            len(self._event_times[key])
        )

    def record_error(self, tenant: str, source: str, error_type: str) -> None:
        self.errors_total.labels(tenant=tenant, source=source, error_type=error_type).inc()

    def update_checkpoint_age(self, tenant: str, source: str, last_flush_time: float) -> None:
        age = time.time() - last_flush_time
        self.checkpoint_age_seconds.labels(tenant=tenant, source=source).set(age)

    def update_sqlite_size(self, tenant: str, source: str, db_path: str) -> None:
        try:
            size = os.path.getsize(db_path)
            self.sqlite_checkpoint_size_bytes.labels(tenant=tenant, source=source).set(size)
        except OSError:
            pass

    def update_fallback_length(self, tenant: str, source: str, length: int) -> None:
        self.fallback_queue_length.labels(tenant=tenant, source=source).set(length)


# Singleton
METRICS = CDCMetrics()


# ---------------------------------------------------------------------------
# /metrics HTTP endpoint
# ---------------------------------------------------------------------------

def start_metrics_server(port: int = 8080) -> None:
    """
    Start the Prometheus /metrics HTTP server on a background thread.
    No-op if prometheus_client is not installed.
    """
    if not _PROMETHEUS_AVAILABLE:
        log.warning("prometheus_client not installed — /metrics endpoint not started")
        return
    _start_http(port)
    log.info("Prometheus /metrics endpoint listening on :%d", port)

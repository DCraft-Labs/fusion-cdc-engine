"""
P2.6 — Tests for fallback_queue.py.
"""

import pytest
from unittest.mock import MagicMock

from cdc_worker.event_envelope import build_event
from cdc_worker.fallback_queue import FallbackQueue


DEFAULTS = dict(
    op="c",
    source_id="src-001",
    bank_id="bank-001",
    tenant_id="tenant-001",
    schema_name="testdb",
    table_name="orders",
    lsn="bin:1234",
    ts_ms=1_714_000_000_000,
    pk_values={"id": 1},
    after={"id": 1, "name": "Alice"},
)

ROUTING = {"bank_id": "bank-001", "tenant_id": "tenant-001", "source_id": "src-001"}


class TestFallbackQueue:
    def setup_method(self):
        self.q = FallbackQueue(":memory:")

    def test_enqueue_increases_queue_length(self):
        event = build_event(**DEFAULTS)
        assert self.q.queue_length() == 0
        self.q.enqueue(event, ROUTING)
        assert self.q.queue_length() == 1

    def test_drain_flushes_to_redis(self):
        event = build_event(**DEFAULTS)
        self.q.enqueue(event, ROUTING)
        self.q.enqueue(build_event(**{**DEFAULTS, "lsn": "bin:9999"}), ROUTING)

        mock_pub = MagicMock()
        mock_pub.publish.return_value = True

        flushed = self.q.drain(mock_pub)

        assert flushed == 2
        assert self.q.queue_length() == 0
        assert mock_pub.publish.call_count == 2

    def test_drain_stops_on_first_failure(self):
        for i in range(5):
            self.q.enqueue(build_event(**{**DEFAULTS, "lsn": f"bin:{i}"}), ROUTING)

        mock_pub = MagicMock()
        mock_pub.publish.side_effect = [True, True, False, True, True]

        flushed = self.q.drain(mock_pub)

        assert flushed == 2
        assert self.q.queue_length() == 3

    def test_queue_length_counts_unflushed_only(self):
        event = build_event(**DEFAULTS)
        self.q.enqueue(event, ROUTING)
        mock_pub = MagicMock()
        mock_pub.publish.return_value = True
        self.q.drain(mock_pub)
        assert self.q.queue_length() == 0

    def test_data_survives_reopen(self, tmp_path):
        db_file = str(tmp_path / "fallback.db")
        q1 = FallbackQueue(db_file)
        q1.enqueue(build_event(**DEFAULTS), ROUTING)
        q1.close()
        q2 = FallbackQueue(db_file)
        assert q2.queue_length() == 1
        q2.close()

    def test_drain_returns_count_flushed(self):
        for i in range(3):
            self.q.enqueue(build_event(**{**DEFAULTS, "lsn": f"bin:{i}"}), ROUTING)
        mock_pub = MagicMock()
        mock_pub.publish.return_value = True
        assert self.q.drain(mock_pub) == 3

    def test_drain_emits_fallback_queue_drained_total_metric(self):
        """cdc_fallback_queue_drained_total increments for each drained event (spec §3)."""
        try:
            from cdc_worker.metrics import METRICS
        except ImportError:
            pytest.skip("metrics module unavailable")

        # Get baseline counter value
        def _get_counter_value(tenant, source):
            try:
                return METRICS.fallback_queue_drained_total.labels(
                    tenant=tenant, source=source
                )._value.get()
            except Exception:
                return 0.0

        tenant, source = "test_tenant", "test_source"
        before = _get_counter_value(tenant, source)

        for i in range(3):
            self.q.enqueue(build_event(**{**DEFAULTS, "lsn": f"bin:{i}"}), ROUTING)

        mock_pub = MagicMock()
        mock_pub.publish.return_value = True
        self.q.drain(mock_pub, tenant=tenant, source=source)

        after = _get_counter_value(tenant, source)
        assert after - before == 3, f"Expected counter +3, got {before} → {after}"

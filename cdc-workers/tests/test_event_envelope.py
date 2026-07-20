"""
P2.2 — Tests for event_envelope.py (10 tests).
"""

import json
import pytest
from cdc_worker.event_envelope import (
    CDCEvent,
    build_event,
    compute_event_id,
)


DEFAULTS = dict(
    source_id="src-001",
    bank_id="bank-001",
    tenant_id="tenant-001",
    schema_name="testdb",
    table_name="orders",
    lsn="mysql-bin.000001:1234",
    ts_ms=1_714_000_000_000,
)


class TestComputeEventId:
    def test_compute_event_id_is_deterministic(self):
        pk = {"id": 42}
        lsn = "bin:1000"
        assert compute_event_id(pk, lsn) == compute_event_id(pk, lsn)

    def test_different_lsn_different_hash(self):
        pk = {"id": 42}
        assert compute_event_id(pk, "bin:1000") != compute_event_id(pk, "bin:1001")

    def test_different_pk_different_hash(self):
        lsn = "bin:1000"
        assert compute_event_id({"id": 1}, lsn) != compute_event_id({"id": 2}, lsn)

    def test_event_id_is_64_char_hex(self):
        eid = compute_event_id({"id": 99}, "bin:999")
        assert len(eid) == 64
        assert all(c in "0123456789abcdef" for c in eid)


class TestBuildEvent:
    def test_build_insert_op_c_before_none(self):
        event = build_event(op="c", pk_values={"id": 1}, after={"id": 1, "name": "Alice"}, **DEFAULTS)
        assert event.op == "c"
        assert event.before is None
        assert event.after["name"] == "Alice"

    def test_build_update_op_u_both_sides(self):
        event = build_event(
            op="u", pk_values={"id": 1},
            before={"id": 1, "name": "Alice"},
            after={"id": 1, "name": "Bob"},
            **DEFAULTS,
        )
        assert event.op == "u"
        assert event.before["name"] == "Alice"
        assert event.after["name"] == "Bob"

    def test_build_delete_op_d_after_none(self):
        event = build_event(op="d", pk_values={"id": 1}, before={"id": 1, "name": "Alice"}, **DEFAULTS)
        assert event.op == "d"
        assert event.after is None


class TestRedisSerialization:
    def _make_insert(self):
        return build_event(op="c", pk_values={"id": 1}, after={"id": 1, "val": 99.5}, **DEFAULTS)

    def test_to_redis_dict_all_string_values(self):
        d = self._make_insert().to_redis_dict()
        for k, v in d.items():
            assert isinstance(v, str), f"key {k!r} has non-str value {v!r}"

    def test_from_redis_dict_roundtrip_insert(self):
        original = self._make_insert()
        restored = CDCEvent.from_redis_dict(original.to_redis_dict())
        assert restored.event_id == original.event_id
        assert restored.op == "c"
        assert restored.before is None
        assert restored.after["val"] == pytest.approx(99.5)

    def test_from_redis_dict_roundtrip_nested_json(self):
        event = build_event(
            op="c", pk_values={"id": 1},
            after={"id": 1, "meta": {"key": "value", "tags": [1, 2]}},
            **DEFAULTS,
        )
        restored = CDCEvent.from_redis_dict(event.to_redis_dict())
        assert restored.after["meta"]["key"] == "value"
        assert restored.after["meta"]["tags"] == [1, 2]

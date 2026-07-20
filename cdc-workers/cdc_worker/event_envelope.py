"""
P2.2 — CDC Event Envelope.

CDCEvent is the canonical event object that flows through every layer of
the worker (connector → publisher → Redis stream → Spark consumer).

event_id = SHA-256( json.dumps({"pk": pk_values, "lsn": lsn}, sort_keys=True) )
             → 64-character lowercase hex string

op values:
  "c" — INSERT   (before=None,  after=row values)
  "u" — UPDATE   (before=old,   after=new)
  "d" — DELETE   (before=row,   after=None)

All Redis XADD values must be str — to_redis_dict() handles this.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# event_id computation
# ---------------------------------------------------------------------------

def compute_event_id(pk_values: Dict[str, Any], lsn: str) -> str:
    """
    Deterministic SHA-256 event ID.

    The same (pk_values, lsn) pair always produces the same 64-hex-char string,
    which lets downstream consumers deduplicate retried events.
    """
    payload = json.dumps({"pk": pk_values, "lsn": lsn}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# CDCEvent dataclass
# ---------------------------------------------------------------------------

@dataclass
class CDCEvent:
    """
    Immutable envelope for a single CDC change record.

    Fields
    ------
    event_id    : 64-char hex SHA-256 (dedup key)
    op          : "c" | "u" | "d"
    source_id   : UUID string of the SourceConnector record
    bank_id     : UUID string
    tenant_id   : UUID string (sub_tenant_id in control plane)
    schema_name : database/schema name
    table_name  : table name
    lsn         : log position (binlog file+pos / WAL LSN / mongo resume token)
    ts_ms       : event timestamp in epoch milliseconds (source time)
    pk_values   : dict of primary-key column → value
    before      : full row state before change (None for inserts)
    after       : full row state after change  (None for deletes)
    processed_at: epoch milliseconds when the worker processed this event
    """

    event_id: str
    op: str
    source_id: str
    bank_id: str
    tenant_id: str
    schema_name: str
    table_name: str
    lsn: str
    ts_ms: int
    pk_values: Dict[str, Any]
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    processed_at: int = field(default_factory=lambda: int(time.time() * 1000))

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        if self.op not in ("c", "u", "d"):
            raise ValueError(f"Invalid op {self.op!r}. Must be 'c', 'u', or 'd'.")
        if self.op == "c" and self.before is not None:
            raise ValueError("Insert event (op='c') must have before=None.")
        if self.op == "d" and self.after is not None:
            raise ValueError("Delete event (op='d') must have after=None.")

    # ------------------------------------------------------------------
    # Redis serialization — all values must be str
    # ------------------------------------------------------------------

    def to_redis_dict(self) -> Dict[str, str]:
        """
        Flatten the event to a {str: str} dict suitable for XADD.
        Nested dicts (before, after, pk_values, metadata) are JSON-encoded.
        """
        return {
            "event_id": self.event_id,
            "op": self.op,
            "source_id": self.source_id,
            "bank_id": self.bank_id,
            "tenant_id": self.tenant_id,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "lsn": self.lsn,
            "ts_ms": str(self.ts_ms),
            "pk_values": json.dumps(self.pk_values, default=str),
            "before": json.dumps(self.before, default=str) if self.before is not None else "null",
            "after": json.dumps(self.after, default=str) if self.after is not None else "null",
            "metadata": json.dumps(self.metadata, default=str),
            "processed_at": str(self.processed_at),
        }

    @classmethod
    def from_redis_dict(cls, d: Dict[str, str]) -> "CDCEvent":
        """
        Reconstruct a CDCEvent from a Redis XREAD entry dict.
        Parses nested JSON in before/after/pk_values.
        """
        before_raw = d.get("before", "null")
        after_raw = d.get("after", "null")
        metadata_raw = d.get("metadata", "{}")
        return cls(
            event_id=d["event_id"],
            op=d["op"],
            source_id=d["source_id"],
            bank_id=d["bank_id"],
            tenant_id=d["tenant_id"],
            schema_name=d["schema_name"],
            table_name=d["table_name"],
            lsn=d["lsn"],
            ts_ms=int(d["ts_ms"]),
            pk_values=json.loads(d["pk_values"]),
            before=json.loads(before_raw),
            after=json.loads(after_raw),
            metadata=json.loads(metadata_raw),
            processed_at=int(d.get("processed_at", 0)),
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_event(
    op: str,
    source_id: str,
    bank_id: str,
    tenant_id: str,
    schema_name: str,
    table_name: str,
    lsn: str,
    ts_ms: int,
    pk_values: Dict[str, Any],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> CDCEvent:
    """
    Construct a CDCEvent with a computed event_id.
    This is the single canonical entry-point used by all connectors.
    """
    event_id = compute_event_id(pk_values, lsn)
    return CDCEvent(
        event_id=event_id,
        op=op,
        source_id=source_id,
        bank_id=bank_id,
        tenant_id=tenant_id,
        schema_name=schema_name,
        table_name=table_name,
        lsn=lsn,
        ts_ms=ts_ms,
        pk_values=pk_values,
        before=before,
        after=after,
        metadata=metadata or {},
    )

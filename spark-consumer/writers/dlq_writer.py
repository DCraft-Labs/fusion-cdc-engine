"""
DLQWriter — writes failed CDC rows to a Redis DLQ stream.

DLQ stream key format:
    dlq:{bank_id}:{tenant_id}:{source_id}:{schema_name}:{table_name}
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _safe_serialize(value: Any) -> str:
    """JSON-serialize a value, falling back to str() for non-serializable types."""
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


class DLQWriter:
    """Writes failed rows from a DQ block policy to a Redis DLQ stream."""

    def __init__(
        self,
        redis_url: str,
        bank_id: str,
        tenant_id: str,
        source_id: str,
        schema_name: str,
        table_name: str,
    ) -> None:
        self.redis_url = redis_url
        self.stream_key = (
            f"dlq:{bank_id}:{tenant_id}:{source_id}:{schema_name}:{table_name}"
        )
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import redis as _redis  # lazy

            self._client = _redis.from_url(self.redis_url, decode_responses=True)
        return self._client

    def write(self, df, batch_id: int) -> int:
        """
        Write each row of *df* to the DLQ stream.
        Returns the number of rows written.
        """
        rows = df.collect()
        if not rows:
            return 0

        pipe = self.client.pipeline(transaction=False)
        for row in rows:
            record: Dict[str, str] = {
                k: _safe_serialize(v) for k, v in row.asDict().items()
            }
            record["_batch_id"] = str(batch_id)
            pipe.xadd(self.stream_key, record)
        pipe.execute()
        logger.info("DLQWriter wrote %d rows to %s", len(rows), self.stream_key)
        return len(rows)

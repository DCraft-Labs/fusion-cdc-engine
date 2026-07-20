"""
RedisStreamSource — reads CDC events from Redis Streams using XREADGROUP.

Stream key format: cdc:{bank_id}:{tenant_id}:{source_id}:{schema_name}:{table_name}
Consumer group   : fusion-spark  (created by Phase 2 redis_publisher)
"""
from __future__ import annotations

import json
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

# Canonical CDC event fields present in every Redis message
CDC_EVENT_FIELDS = [
    "event_id",
    "tenant_id",
    "bank_id",
    "source_id",
    "schema_name",
    "table_name",
    "op",
    "before",
    "after",
    "ts_ms",
    "lsn",
    "metadata",
]


class RedisStreamSource:
    """
    Reads batches of CDC events from one or more Redis Stream keys and returns a
    PySpark DataFrame.  Uses XREADGROUP so the consumer group tracks offsets;
    callers must call ``ack()`` after a batch is successfully written.
    """

    DEFAULT_GROUP = "fusion-spark"

    def __init__(
        self,
        redis_url: str,
        stream_keys: List[str],
        group: str = DEFAULT_GROUP,
        consumer_name: str = "spark-consumer-1",
    ) -> None:
        self.redis_url = redis_url
        self.stream_keys = list(stream_keys)
        self.group = group
        self.consumer_name = consumer_name
        self._client = None
        # pending acks: {stream_key: [message_id, ...]}
        self._pending: dict = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def client(self):
        if self._client is None:
            import redis as _redis  # lazy import so unit tests can mock before import

            self._client = _redis.from_url(self.redis_url, decode_responses=True)
        return self._client

    def ensure_groups(self) -> None:
        """Create consumer groups on all stream keys if they don't already exist."""
        for key in self.stream_keys:
            try:
                self.client.xgroup_create(key, self.group, id="$", mkstream=True)
                logger.info("Created consumer group %s on %s", self.group, key)
            except Exception as exc:
                # BUSYGROUP means the group already exists — safe to ignore
                if "BUSYGROUP" not in str(exc):
                    logger.warning("xgroup_create %s: %s", key, exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_batch(self, spark, count: int = 100, block_ms: int = 2000):
        """
        Read up to ``count`` pending messages per stream key.

        Returns a PySpark DataFrame with one row per CDC event.  Returns an
        empty DataFrame (same schema) when there are no new messages.
        """
        from pyspark.sql import Row
        from pyspark.sql.types import (
            StringType,
            StructField,
            StructType,
        )

        schema = StructType([StructField(f, StringType(), True) for f in CDC_EVENT_FIELDS])

        streams = {key: ">" for key in self.stream_keys}
        try:
            raw = self.client.xreadgroup(
                self.group,
                self.consumer_name,
                streams,
                count=count,
                block=block_ms,
            )
        except Exception as exc:
            logger.error("xreadgroup failed: %s", exc)
            return spark.createDataFrame([], schema)

        if not raw:
            return spark.createDataFrame([], schema)

        rows = []
        self._pending = {}
        for stream_key, messages in raw:
            ids = []
            for msg_id, fields in messages:
                # fields is a dict of strings; ensure all CDC fields are present
                row_data = {}
                for field in CDC_EVENT_FIELDS:
                    row_data[field] = fields.get(field)
                try:
                    rows.append(Row(**row_data))
                    ids.append(msg_id)
                except Exception as exc:
                    logger.warning("Skipping malformed event %s: %s", msg_id, exc)
            if ids:
                self._pending[stream_key] = ids

        if not rows:
            return spark.createDataFrame([], schema)

        return spark.createDataFrame(rows, schema)

    def ack(self, stream_key: Optional[str] = None) -> None:
        """
        Acknowledge processed message IDs.

        If ``stream_key`` is None, ack all pending messages from the last
        ``read_batch`` call.
        """
        keys = [stream_key] if stream_key else list(self._pending.keys())
        for key in keys:
            ids = self._pending.get(key, [])
            if ids:
                try:
                    self.client.xack(key, self.group, *ids)
                    logger.debug("ACKed %d messages from %s", len(ids), key)
                except Exception as exc:
                    logger.error("xack failed for %s: %s", key, exc)
                self._pending.pop(key, None)

    def pending_count(self) -> int:
        """Return total number of pending (un-ACKed) messages."""
        return sum(len(ids) for ids in self._pending.values())

"""
BatchConsumer — SCHEDULED mode CDC consumer (designed for Airflow invocation).

Reads all pending messages from Redis Streams in one shot, applies the pipeline,
writes to the destination, and returns a summary dict.

Usage:
    consumer = BatchConsumer(config)
    consumer.setup()
    result = consumer.run()
    # result = {"processed": N, "failed": M, "violations": [...]}
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BatchConsumer:
    """One-shot batch consumer for SCHEDULED sync_type connections."""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.spark = None
        self.redis_source = None
        self.transform_executor = None
        self.dq_executor = None
        self.writer = None
        self.dlq_writer = None

    def setup(self, spark=None) -> None:
        """Initialise Spark session and pipeline components (reuses StreamingConsumer logic)."""
        from consumer.streaming_consumer import StreamingConsumer

        # Delegate setup to StreamingConsumer to avoid duplication
        _sc = StreamingConsumer(self.config)
        _sc.setup(spark=spark)

        self.spark = _sc.spark
        self.redis_source = _sc.redis_source
        self.transform_executor = _sc.transform_executor
        self.dq_executor = _sc.dq_executor
        self.writer = _sc.writer
        self.dlq_writer = _sc.dlq_writer

    def run(self, max_count: int = 10_000) -> dict:
        """
        Read all pending CDC events and process them.

        Returns a summary dict: {"processed": int, "failed": int, "violations": list}
        """
        self.redis_source.ensure_groups()

        batch_df = self.redis_source.read_batch(
            self.spark,
            count=max_count,
            block_ms=500,  # short block for batch mode
        )

        total = batch_df.count()
        if total == 0:
            logger.info("BatchConsumer: no pending events")
            return {"processed": 0, "failed": 0, "violations": []}

        # Apply transforms
        if self.transform_executor:
            batch_df = self.transform_executor.apply(batch_df)

        # DQ checks
        violations = []
        failed_count = 0
        if self.dq_executor:
            passed_df, failed_df, violations = self.dq_executor.check(batch_df)
            failed_count = failed_df.count()
            if self.dlq_writer and failed_count > 0:
                self.dlq_writer.write(failed_df, batch_id=0)
            batch_df = passed_df
        
        # Write to destination
        processed = batch_df.count()
        self.writer.upsert_batch(batch_df, batch_id=0)

        # ACK Redis
        self.redis_source.ack()

        logger.info(
            "BatchConsumer: processed=%d failed=%d violations=%d",
            processed, failed_count, len(violations),
        )
        return {"processed": processed, "failed": failed_count, "violations": violations}

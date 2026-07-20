"""
SparkCheckpointHandler — tracks Redis stream offsets and Spark batch IDs
so we know which messages are safe to ACK after a successful write.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class SparkCheckpointHandler:
    """
    Lightweight file-based checkpoint that persists the last successfully
    processed batch_id and the Redis message IDs that were ACKed in that batch.

    In production the Spark ``checkpointLocation`` option handles Spark-level
    recovery; this handler is an additional safeguard for the Redis ACK state.
    """

    def __init__(self, checkpoint_dir: str) -> None:
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
        self._state: Dict[str, object] = {}
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _state_file(self) -> str:
        return os.path.join(self.checkpoint_dir, "spark_consumer_checkpoint.json")

    def _load(self) -> None:
        path = self._state_file()
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    self._state = json.load(fh)
                logger.debug("Loaded checkpoint from %s", path)
            except Exception as exc:
                logger.warning("Could not load checkpoint: %s", exc)
                self._state = {}

    def _save(self) -> None:
        path = self._state_file()
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self._state, fh)
        os.replace(tmp, path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_last_batch_id(self) -> Optional[int]:
        return self._state.get("last_batch_id")

    def commit(self, batch_id: int, stream_offsets: Optional[Dict] = None) -> None:
        """Record a successfully completed batch."""
        self._state["last_batch_id"] = batch_id
        if stream_offsets:
            self._state["stream_offsets"] = stream_offsets
        self._save()
        logger.debug("Committed checkpoint for batch_id=%d", batch_id)

    def get_stream_offsets(self) -> Dict:
        return self._state.get("stream_offsets", {})

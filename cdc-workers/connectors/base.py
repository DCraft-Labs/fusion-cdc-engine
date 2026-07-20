"""
Abstract base connector.
All connectors must implement stream_events() as an async generator.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator

from cdc_worker.event_envelope import CDCEvent


class BaseConnector(ABC):
    """
    Abstract CDC connector.

    Subclasses receive a source config dict and a LocalCheckpointManager.
    They must implement stream_events() as an async generator of CDCEvent objects.
    On shutdown the worker cancels the coroutine, so connectors should clean up
    in a try/finally or via async context manager.
    """

    def __init__(self, source: dict, checkpoint_manager) -> None:
        self._source = source
        self._ckpt = checkpoint_manager

    @abstractmethod
    async def stream_events(self) -> AsyncIterator[CDCEvent]:
        """
        Yield CDCEvent objects indefinitely until cancelled.
        Must never swallow asyncio.CancelledError.
        """
        ...

    async def close(self) -> None:
        """Optional cleanup hook called on graceful shutdown."""
        pass

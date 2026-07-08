"""In-process event broadcaster used by workers and the SSE endpoint."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class MercuryEvent:
    """A small typed event payload for pub/sub consumers."""

    type: str
    payload: dict[str, Any]
    emitted_at: str

    @classmethod
    def create(cls, event_type: str, payload: dict[str, Any] | None = None) -> "MercuryEvent":
        return cls(
            type=event_type,
            payload=payload or {},
            emitted_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        )


class EventBus:
    """Asyncio pub/sub with independent queues per subscriber."""

    def __init__(self, queue_size: int = 200) -> None:
        self._queue_size = queue_size
        self._subscribers: set[asyncio.Queue[MercuryEvent]] = set()
        self._lock = asyncio.Lock()

    async def publish(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        event = MercuryEvent.create(event_type, payload)
        async with self._lock:
            subscribers = list(self._subscribers)
        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Slow browser tabs should not block ingestion.
                _ = queue.get_nowait()
                queue.put_nowait(event)

    async def subscribe(self) -> AsyncIterator[MercuryEvent]:
        queue: asyncio.Queue[MercuryEvent] = asyncio.Queue(maxsize=self._queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        try:
            yield MercuryEvent.create("connected", {})
            while True:
                yield await queue.get()
        finally:
            async with self._lock:
                self._subscribers.discard(queue)


events = EventBus()


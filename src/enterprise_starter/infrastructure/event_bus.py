from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from enterprise_starter.domain.events import WorkflowEvent


class EventBus:
    def __init__(self) -> None:
        self._events: list[WorkflowEvent] = []
        self._queue: asyncio.Queue[WorkflowEvent | None] = asyncio.Queue()

    @property
    def events(self) -> list[WorkflowEvent]:
        return list(self._events)

    async def publish(self, event: WorkflowEvent) -> None:
        self._events.append(event)
        await self._queue.put(event)

    async def close(self) -> None:
        await self._queue.put(None)

    async def stream(self) -> AsyncIterator[WorkflowEvent]:
        while True:
            event = await self._queue.get()
            if event is None:
                break
            yield event


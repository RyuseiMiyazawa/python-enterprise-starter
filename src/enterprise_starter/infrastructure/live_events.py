from __future__ import annotations

import json
import queue
import threading
from collections.abc import Iterator
from typing import Any


class LiveEventHub:
    def __init__(self) -> None:
        self._subscribers: set[queue.Queue[dict[str, Any] | None]] = set()
        self._lock = threading.Lock()

    def publish(self, event: dict[str, Any]) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.put(event)

    def stream(self) -> Iterator[str]:
        subscription: queue.Queue[dict[str, Any] | None] = queue.Queue()
        with self._lock:
            self._subscribers.add(subscription)
        try:
            yield ": connected\n\n"
            while True:
                event = subscription.get()
                if event is None:
                    break
                yield f"data: {json.dumps(event, sort_keys=True)}\n\n"
        finally:
            with self._lock:
                self._subscribers.discard(subscription)

    def close(self) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.put(None)


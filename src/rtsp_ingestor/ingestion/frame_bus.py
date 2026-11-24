"""Fan-out of decoded frames from the capture thread to independent consumers.

Each subscriber gets its own bounded queue and a back-pressure policy:

* ``block``       - lossless. The producer waits (with stop-awareness). Used by the
                    recorder so no frame is ever dropped from the video clips.
* ``drop_oldest`` - lossy, never blocks the producer; drops are counted.

Subscribers may also provide a ``accept`` predicate that is evaluated on the
producer thread (e.g. the 5 FPS gate), so only selected frames are queued.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

log = logging.getLogger(__name__)

SENTINEL: Any = object()

Policy = Literal["block", "drop_oldest"]


@dataclass
class Subscriber:
    name: str
    queue: queue.Queue
    policy: Policy
    accept: Callable[[Any], bool] | None = None
    delivered: int = 0
    dropped: int = 0
    blocked_s: float = 0.0
    max_depth: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def stats(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "delivered": self.delivered,
            "dropped": self.dropped,
            "queue_depth": self.queue.qsize(),
            "queue_max_depth": self.max_depth,
            "queue_capacity": self.queue.maxsize,
            "producer_blocked_s": round(self.blocked_s, 3),
        }


class FrameBus:
    def __init__(self, stop_event: threading.Event) -> None:
        self._subs: list[Subscriber] = []
        self._stop = stop_event
        self._closed = False

    def subscribe(
        self,
        name: str,
        maxsize: int,
        policy: Policy = "block",
        accept: Callable[[Any], bool] | None = None,
    ) -> Subscriber:
        sub = Subscriber(name=name, queue=queue.Queue(maxsize=maxsize), policy=policy, accept=accept)
        self._subs.append(sub)
        return sub

    @property
    def subscribers(self) -> list[Subscriber]:
        return list(self._subs)

    def publish(self, item: Any, control: bool = False) -> None:
        """Deliver ``item`` to every subscriber.

        ``control`` items (stream events) bypass ``accept`` filters and are always
        delivered, even to lossy subscribers (by evicting a frame if necessary).
        """
        if self._closed:
            return
        for sub in self._subs:
            if not control and sub.accept is not None and not sub.accept(item):
                continue
            if sub.policy == "block" or control:
                self._put_blocking(sub, item)
            else:
                self._put_drop_oldest(sub, item)
            depth = sub.queue.qsize()
            if depth > sub.max_depth:
                sub.max_depth = depth

    def _put_blocking(self, sub: Subscriber, item: Any) -> None:
        try:
            sub.queue.put_nowait(item)
            sub.delivered += 1
            return
        except queue.Full:
            pass
        if sub.policy == "drop_oldest":
            self._put_drop_oldest(sub, item)
            return
        t0 = time.perf_counter()
        warned = False
        while True:
            try:
                sub.queue.put(item, timeout=0.25)
                sub.delivered += 1
                break
            except queue.Full:
                waited = time.perf_counter() - t0
                if not warned and waited > 2.0:
                    log.warning("Back-pressure: subscriber %r queue full for %.1fs", sub.name, waited)
                    warned = True
                if self._stop.is_set() and waited > 10.0:
                    sub.dropped += 1
                    log.error("Dropping item for %r during shutdown (consumer stuck)", sub.name)
                    break
        sub.blocked_s += time.perf_counter() - t0

    @staticmethod
    def _put_drop_oldest(sub: Subscriber, item: Any) -> None:
        while True:
            try:
                sub.queue.put_nowait(item)
                sub.delivered += 1
                return
            except queue.Full:
                try:
                    sub.queue.get_nowait()
                    sub.dropped += 1
                except queue.Empty:
                    pass

    def close(self) -> None:
        """Send the end-of-stream sentinel to every consumer."""
        if self._closed:
            return
        self._closed = True
        for sub in self._subs:
            while True:
                try:
                    sub.queue.put(SENTINEL, timeout=0.5)
                    break
                except queue.Full:
                    if sub.policy == "drop_oldest":
                        try:
                            sub.queue.get_nowait()
                        except queue.Empty:
                            pass

    def stats(self) -> dict[str, dict[str, Any]]:
        return {s.name: s.stats() for s in self._subs}

"""Thread-safe counters/gauges and summary statistics helpers."""

from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

import numpy as np


def summarize(values: Iterable[float], ndigits: int = 2) -> dict[str, Any]:
    """count/mean/min/p50/p90/p95/p99/max for a sequence of numbers."""
    arr = np.asarray(list(values), dtype=float)
    if arr.size == 0:
        return {"count": 0, "mean": None, "min": None, "p50": None, "p90": None, "p95": None, "p99": None, "max": None}
    p50, p90, p95, p99 = np.percentile(arr, [50, 90, 95, 99])
    return {
        "count": int(arr.size),
        "mean": round(float(arr.mean()), ndigits),
        "min": round(float(arr.min()), ndigits),
        "p50": round(float(p50), ndigits),
        "p90": round(float(p90), ndigits),
        "p95": round(float(p95), ndigits),
        "p99": round(float(p99), ndigits),
        "max": round(float(arr.max()), ndigits),
    }


class PipelineMetrics:
    """Simple process-wide counters (frames in/out, errors, reconnects...)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, Any] = {}

    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + n

    def set(self, name: str, value: Any) -> None:
        with self._lock:
            self._gauges[name] = value

    def get(self, name: str, default: int = 0) -> int:
        with self._lock:
            return self._counters.get(name, default)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"counters": dict(sorted(self._counters.items())), "gauges": dict(sorted(self._gauges.items()))}

"""Ingestion latency measurement.

Methodology (see README "Latency methodology"):

1. **Source-clock jitter / delay variation** (``source_jitter_ms``)
   For every frame ``D_i = arrival_wall_i - pts_i``. The camera clock and our
   clock have an unknown constant offset, so absolute one-way delay is not
   observable without a shared reference. However the *minimum* ``D`` over a
   sliding window corresponds to the fastest-delivered frame (network floor).
   ``D_i - min(D)`` is therefore the extra delay each frame experienced on top of
   the floor (packet delay variation, cf. RFC 5481). It also gives a drift-free
   estimate of each frame's capture wall-clock time: ``capture_ts = pts + min(D)``.

2. **Pipeline stage latencies** on our side, measured with a monotonic clock:
   arrival->decoded, decoded->muxed into the clip, decoded->JPEG on disk, and the
   end-to-end arrival->persisted.

3. **Source-referenced end-to-end**: wall time when the JPEG is on disk minus
   the estimated capture time (= delay variation + processing).

4. **Network floor**: TCP handshake RTT to the camera at each (re)connect;
   RTT/2 approximates the one-way propagation floor that (1) cannot see.
"""

from __future__ import annotations

import random
import threading
from collections import deque
from typing import Any

from .metrics import summarize

# Metric names (milliseconds)
SOURCE_JITTER = "source_jitter_ms"
DECODE = "arrival_to_decode_ms"
RECORD = "decode_to_record_ms"
ARRIVAL_TO_RECORD = "arrival_to_record_ms"
FRAME_SAVE = "decode_to_frame_saved_ms"
ARRIVAL_TO_FRAME = "arrival_to_frame_saved_ms"
CAPTURE_TO_FRAME = "capture_to_frame_saved_ms"
CONNECT_RTT = "connect_rtt_ms"

METRIC_DESCRIPTIONS: dict[str, str] = {
    SOURCE_JITTER: "Per-frame arrival delay above the network floor, from source PTS vs local arrival clock (sliding-window min baseline)",
    DECODE: "Packet arrival on socket -> decoded frame available",
    RECORD: "Decoded frame -> encoded & muxed into the 5-minute clip",
    ARRIVAL_TO_RECORD: "Packet arrival -> frame persisted in clip (end-to-end ingestion, recording path)",
    FRAME_SAVE: "Decoded frame -> JPEG fully written to disk",
    ARRIVAL_TO_FRAME: "Packet arrival -> JPEG on disk (end-to-end ingestion, extraction path)",
    CAPTURE_TO_FRAME: "Estimated source capture time -> JPEG on disk (source-referenced end-to-end, excludes network floor)",
    CONNECT_RTT: "TCP handshake round-trip time to the RTSP server at each (re)connect",
}


class SourceClock:
    """Sliding-window minimum of ``arrival - pts`` (O(1) amortised, monotonic deque)."""

    def __init__(self, window_s: float = 60.0) -> None:
        self.window_s = window_s
        self._dq: deque[tuple[float, float]] = deque()

    def reset(self) -> None:
        self._dq.clear()

    def update(self, media_s: float, arrival_ts: float) -> tuple[float, float]:
        """Return ``(estimated_capture_ts, delay_above_floor_s)``."""
        d = arrival_ts - media_s
        dq = self._dq
        while dq and dq[-1][1] >= d:
            dq.pop()
        dq.append((media_s, d))
        horizon = media_s - self.window_s
        while dq[0][0] < horizon:
            dq.popleft()
        base = dq[0][1]
        return media_s + base, d - base


class _Reservoir:
    """Exact count/min/max/mean + bounded uniform sample for percentiles (Algorithm R)."""

    __slots__ = ("cap", "samples", "count", "total", "vmin", "vmax", "_rng")

    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.samples: list[float] = []
        self.count = 0
        self.total = 0.0
        self.vmin = float("inf")
        self.vmax = float("-inf")
        self._rng = random.Random(1234)

    def add(self, v: float) -> None:
        self.count += 1
        self.total += v
        self.vmin = min(self.vmin, v)
        self.vmax = max(self.vmax, v)
        if len(self.samples) < self.cap:
            self.samples.append(v)
        else:
            j = self._rng.randrange(self.count)
            if j < self.cap:
                self.samples[j] = v

    def summary(self) -> dict[str, Any]:
        s = summarize(self.samples)
        if self.count:
            s["count"] = self.count
            s["mean"] = round(self.total / self.count, 2)
            s["min"] = round(self.vmin, 2)
            s["max"] = round(self.vmax, 2)  # exact max, not sampled
        return s


class LatencyTracker:
    def __init__(self, max_samples: int = 200_000) -> None:
        self._lock = threading.Lock()
        self._max = max_samples
        self._all: dict[str, _Reservoir] = {}
        self._interval: dict[str, list[float]] = {}

    def record(self, metric: str, value_ms: float) -> None:
        with self._lock:
            res = self._all.get(metric)
            if res is None:
                res = self._all[metric] = _Reservoir(self._max)
            res.add(value_ms)
            self._interval.setdefault(metric, []).append(value_ms)

    def summary(self) -> dict[str, Any]:
        with self._lock:
            metrics = {name: res.summary() for name, res in sorted(self._all.items())}
        rtt = metrics.get(CONNECT_RTT, {})
        floor = round(rtt["p50"] / 2.0, 2) if rtt.get("p50") is not None else None
        cap = metrics.get(CAPTURE_TO_FRAME, {})
        est = None
        if floor is not None and cap.get("p50") is not None:
            est = {
                "p50": round(cap["p50"] + floor, 2),
                "p95": round(cap["p95"] + floor, 2),
                "max": round(cap["max"] + floor, 2),
                "note": "capture_to_frame_saved + RTT/2 network floor; excludes camera-internal encode delay",
            }
        return {
            "unit": "ms",
            "metrics": metrics,
            "descriptions": {k: v for k, v in METRIC_DESCRIPTIONS.items() if k in metrics},
            "network_floor_one_way_ms": floor,
            "estimated_source_to_disk_ms": est,
        }

    def drain_interval(self) -> dict[str, dict[str, Any]]:
        """Stats for samples since the previous call (for the time-series CSV)."""
        with self._lock:
            interval, self._interval = self._interval, {}
        return {k: summarize(v) for k, v in interval.items()}

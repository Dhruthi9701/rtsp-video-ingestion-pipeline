"""Shared data types passed between pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np


@dataclass(slots=True)
class StreamInfo:
    """Static properties of the incoming stream, captured at connect time."""

    url_masked: str
    container_format: str
    transport: str
    transport_verified: bool
    negotiated_transport: str | None
    codec: str
    codec_long_name: str
    profile: str | None
    width: int
    height: int
    pix_fmt: str | None
    nominal_fps: float | None
    time_base: str
    declared_bit_rate: int | None
    other_tracks: list[str] = field(default_factory=list)
    connect_rtt_ms: float | None = None
    open_duration_ms: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__slots__}  # type: ignore[attr-defined]


@dataclass(slots=True)
class FramePacket:
    """One decoded frame plus every timestamp needed for latency accounting.

    Time bases:
      * ``*_ts``   - wall clock, epoch seconds (``time.time()``)
      * ``*_mono`` - ``time.perf_counter()`` (monotonic, for durations)
      * ``media_s``- source media time (PTS seconds), continuous within a session
    """

    seq: int
    session: int
    image: np.ndarray  # BGR, output resolution
    source_image: np.ndarray | None  # BGR, native resolution (only if needed)
    media_s: float
    pts_valid: bool
    capture_ts: float  # estimated source capture time (wall clock)
    arrival_ts: float
    arrival_mono: float
    decoded_mono: float
    keyframe: bool
    warmup: bool  # within latency warm-up window after (re)connect


@dataclass(slots=True)
class StreamEvent:
    kind: Literal["connected", "disconnected"]
    session: int
    ts: float
    reason: str | None = None
    info: StreamInfo | None = None

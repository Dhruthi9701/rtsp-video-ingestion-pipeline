"""Rolling segmented recorder (default: 5-minute H.264 MP4 clips at 640x360).

No-drop guarantee across boundaries
-----------------------------------
* The recorder subscribes to the frame bus with the lossless ``block`` policy, so
  every decoded frame reaches it (``frames_in == frames_written`` is reported).
* Rollover is decided on *source media time*, on a fixed grid
  (``start, start+300, start+600, ...``) so clips do not drift.
* The frame that crosses a boundary is encoded as the **first frame of the next
  clip** - it is never discarded. Each clip starts with an IDR frame (new
  encoder), so every file is independently decodable.
* Clips are written as ``*.part`` and atomically renamed when finalised, so a
  downstream consumer never sees a half-written file.
* Frame PTS come from the source timestamps (90 kHz time base), so clip
  timing reflects real capture timing even if the camera's frame rate varies.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any

import av

from ..core.config import AppConfig
from ..core.types import FramePacket, StreamEvent
from ..core.utils import CsvAppender, file_stamp, iso
from ..ingestion.frame_bus import SENTINEL, FrameBus
from ..monitoring import latency as lat
from ..monitoring.latency import LatencyTracker
from ..monitoring.metrics import PipelineMetrics
from ..monitoring.profiler import StreamProfiler

log = logging.getLogger(__name__)

TIME_BASE = Fraction(1, 90000)
MANIFEST_FIELDS = [
    "index", "file", "session", "start_time", "end_time", "duration_s",
    "frames_written", "closed_reason", "size_bytes",
]


@dataclass
class _OpenSegment:
    index: int
    session: int
    path: Path
    tmp_path: Path
    container: Any
    stream: Any
    start_media: float
    end_media: float
    start_ts: float
    last_media: float
    last_pts: int = -1
    frames: int = 0


class SegmentWriter(threading.Thread):
    def __init__(
        self,
        cfg: AppConfig,
        bus: FrameBus,
        latency: LatencyTracker,
        profiler: StreamProfiler,
        metrics: PipelineMetrics,
    ) -> None:
        super().__init__(name="recorder", daemon=True)
        self.cfg = cfg
        self.rec = cfg.recording
        self.camera = cfg.camera.name
        self.tz = cfg.output.timezone
        self.out_dir = cfg.output.segments_dir
        self.latency = latency
        self.profiler = profiler
        self.metrics = metrics
        self.sub = bus.subscribe("recorder", maxsize=self.rec.queue_size, policy="block")
        self.fps = Fraction(25, 1)
        self.index = 0
        self.frames_in = 0
        self.frames_written = 0
        self.frames_dropped = 0
        self.closed: list[dict[str, Any]] = []
        self._cur: _OpenSegment | None = None
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._manifest_csv = CsvAppender(self.out_dir / "segments_manifest.csv", MANIFEST_FIELDS)
        self._manifest_jsonl = open(self.out_dir / "segments_manifest.jsonl", "a", encoding="utf-8")

    # ------------------------------------------------------------------ thread
    def run(self) -> None:
        try:
            while True:
                item = self.sub.queue.get()
                if item is SENTINEL:
                    break
                if isinstance(item, StreamEvent):
                    self._on_event(item)
                else:
                    self._on_frame(item)
        except Exception:  # noqa: BLE001
            log.exception("Recorder crashed")
        finally:
            self._close("shutdown")
            self._publish_stats()
            self._manifest_csv.close()
            self._manifest_jsonl.close()

    def _on_event(self, ev: StreamEvent) -> None:
        if ev.kind == "connected" and ev.info and ev.info.nominal_fps:
            self.fps = Fraction(ev.info.nominal_fps).limit_denominator(1001)
        elif ev.kind == "disconnected":
            self._close("stream_disconnected" if ev.reason != "shutdown" else "shutdown")

    # ------------------------------------------------------------------ frames
    def _on_frame(self, fp: FramePacket) -> None:
        self.frames_in += 1
        cur = self._cur
        next_end: float | None = None
        if cur is not None:
            if fp.session != cur.session:
                self._close("session_change")
                cur = None
            elif fp.media_s >= cur.end_media:
                # fixed grid: next boundary = previous boundary + segment length
                next_end = cur.end_media + self.rec.segment_seconds
                while next_end <= fp.media_s:
                    next_end += self.rec.segment_seconds
                self._close("rollover")
                cur = None
        if cur is None:
            try:
                cur = self._open(fp, next_end)
            except Exception:  # noqa: BLE001
                log.exception("Could not open new segment; frame %d dropped", fp.seq)
                self.frames_dropped += 1
                return
        self._encode(cur, fp)
        if self.frames_in % 50 == 0:
            self._publish_stats()

    def _open(self, fp: FramePacket, end_media: float | None) -> _OpenSegment:
        seg_len = self.rec.segment_seconds
        if end_media is None:
            if self.rec.align_to_wallclock:
                boundary = (math.floor(fp.capture_ts / seg_len) + 1) * seg_len
                end_media = fp.media_s + (boundary - fp.capture_ts)
            else:
                end_media = fp.media_s + seg_len
        self.index += 1
        ext = "mp4" if self.rec.container == "mp4" else "mkv"
        day_dir = self.out_dir / file_stamp(fp.capture_ts, self.tz)[:8]
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"{self.camera}_{file_stamp(fp.capture_ts, self.tz)}_seg{self.index:04d}.{ext}"
        tmp = path.with_name(path.name + ".part")
        fmt = "mp4" if ext == "mp4" else "matroska"
        options = {"movflags": "+faststart"} if fmt == "mp4" else {}
        container = av.open(str(tmp), mode="w", format=fmt, options=options)
        gop = max(1, int(round(float(self.fps) * self.rec.gop_seconds)))
        codec_opts = {"crf": str(self.rec.crf), "preset": self.rec.preset, "g": str(gop), "bf": "0"}
        if self.rec.tune:
            codec_opts["tune"] = self.rec.tune
        stream = container.add_stream(self.rec.codec, rate=self.fps, options=codec_opts)
        stream.width = self.rec.width
        stream.height = self.rec.height
        stream.pix_fmt = "yuv420p"
        stream.time_base = TIME_BASE
        stream.codec_context.time_base = TIME_BASE
        container.metadata["title"] = f"{self.camera} segment {self.index}"
        container.metadata["comment"] = f"start={iso(fp.capture_ts, self.tz)}"
        seg = _OpenSegment(
            index=self.index, session=fp.session, path=path, tmp_path=tmp, container=container,
            stream=stream, start_media=fp.media_s, end_media=end_media, start_ts=fp.capture_ts,
            last_media=fp.media_s,
        )
        self._cur = seg
        log.info("Recording segment #%d -> %s (until media t=%.2fs)", self.index, path.name, end_media)
        return seg

    def _encode(self, seg: _OpenSegment, fp: FramePacket) -> None:
        try:
            frame = av.VideoFrame.from_ndarray(fp.image, format="bgr24")
            pts = int(round((fp.media_s - seg.start_media) / TIME_BASE))
            if pts <= seg.last_pts:
                pts = seg.last_pts + 1
            frame.pts = pts
            frame.time_base = TIME_BASE
            for packet in seg.stream.encode(frame):
                seg.container.mux(packet)
            seg.last_pts = pts
            seg.last_media = fp.media_s
            seg.frames += 1
            self.frames_written += 1
            self.metrics.inc("frames_recorded")
            if not fp.warmup:
                now = time.perf_counter()
                self.latency.record(lat.RECORD, (now - fp.decoded_mono) * 1000.0)
                self.latency.record(lat.ARRIVAL_TO_RECORD, (now - fp.arrival_mono) * 1000.0)
        except Exception:  # noqa: BLE001
            self.frames_dropped += 1
            self.metrics.inc("record_errors")
            log.exception("Encoding failed for frame %d; finalising current segment", fp.seq)
            self._close("encoder_error")

    def _close(self, reason: str) -> None:
        seg = self._cur
        if seg is None:
            return
        self._cur = None
        try:
            for packet in seg.stream.encode(None):
                seg.container.mux(packet)
        except Exception:  # noqa: BLE001
            log.exception("Error flushing encoder for segment #%d", seg.index)
        try:
            seg.container.close()
        except Exception:  # noqa: BLE001
            log.exception("Error closing segment #%d", seg.index)
        if seg.frames == 0:
            seg.tmp_path.unlink(missing_ok=True)
            return
        os.replace(seg.tmp_path, seg.path)
        frame_dur = 1.0 / float(self.fps) if self.fps else 0.0
        duration = seg.last_media - seg.start_media + frame_dur
        info = {
            "index": seg.index,
            "file": str(seg.path.relative_to(self.out_dir)).replace("\\", "/"),
            "path": str(seg.path),
            "session": seg.session,
            "start_time": iso(seg.start_ts, self.tz),
            "end_time": iso(seg.start_ts + duration, self.tz),
            "duration_s": round(duration, 3),
            "frames_written": seg.frames,
            "closed_reason": reason,
            "size_bytes": seg.path.stat().st_size,
        }
        self.closed.append(info)
        self._manifest_csv.append(info)
        self._manifest_jsonl.write(json.dumps(info) + "\n")
        self._manifest_jsonl.flush()
        self.metrics.inc("segments_closed")
        log.info("Closed segment #%d %s: %.2fs, %d frames, %.1f MB (%s)", seg.index, seg.path.name,
                 duration, seg.frames, info["size_bytes"] / 1e6, reason)
        self.profiler.on_segment_closed(info)
        self._publish_stats()

    def _publish_stats(self) -> None:
        self.profiler.set_recorder_stats(self.stats())

    def stats(self) -> dict[str, Any]:
        return {
            "frames_in": self.frames_in,
            "frames_written": self.frames_written,
            "frames_dropped": self.frames_dropped,
            "segments_closed": len(self.closed),
            "current_segment": self._cur.path.name if self._cur else None,
        }

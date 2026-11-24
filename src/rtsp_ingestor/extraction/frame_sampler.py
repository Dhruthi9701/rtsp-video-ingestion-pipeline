"""Fixed-rate (default 5 FPS) frame extraction, independent of the source FPS.

Selection is done by :class:`FpsGate` on the **source media timeline**: a frame
is selected when its timestamp reaches the next slot of a ``1/target_fps``
grid. This yields exactly 5 FPS from 25/30/15 FPS sources, an even
spread from non-integer ratios (e.g. 7 FPS -> 5 FPS), and every frame when
the source is slower than the target. The gate runs on the capture thread as a
bus filter, so only selected frames are queued for JPEG encoding.

Naming: ``<camera>_YYYYMMDD_HHMMSS_mmm.jpg`` using the frame's estimated
capture time (see ``monitoring/latency.py``), optionally partitioned into
``frames/<YYYYMMDD>/<HH>/``. Files are written atomically (tmp + rename).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import cv2

from ..core.config import AppConfig
from ..core.types import FramePacket, StreamEvent
from ..core.utils import CsvAppender, frame_stamp, iso
from ..ingestion.frame_bus import SENTINEL, FrameBus
from ..monitoring import latency as lat
from ..monitoring.latency import LatencyTracker
from ..monitoring.metrics import PipelineMetrics
from ..monitoring.profiler import StreamProfiler

log = logging.getLogger(__name__)

INDEX_FIELDS = ["file", "capture_time", "media_s", "seq", "session", "keyframe", "save_latency_ms"]


class FpsGate:
    """Select frames on a fixed time grid of ``1/target_fps`` seconds."""

    EPS = 1e-6

    def __init__(self, target_fps: float) -> None:
        if target_fps <= 0:
            raise ValueError("target_fps must be > 0")
        self.interval = 1.0 / target_fps
        self._session: int | None = None
        self._next: float | None = None

    def reset(self) -> None:
        self._session = None
        self._next = None

    def offer(self, session: int, t: float) -> bool:
        if session != self._session:
            self._session = session
            self._next = None
        if self._next is None:
            self._next = t + self.interval
            return True
        if t + self.EPS >= self._next:
            if t - self._next > self.interval:
                self._next = t + self.interval  # gap in the stream: re-anchor the grid
            else:
                self._next += self.interval
            return True
        return False


def frame_filename(camera: str, capture_ts: float, tz: str = "local") -> str:
    return f"{camera}_{frame_stamp(capture_ts, tz)}.jpg"


class FrameExtractor(threading.Thread):
    def __init__(
        self,
        cfg: AppConfig,
        bus: FrameBus,
        latency: LatencyTracker,
        profiler: StreamProfiler,
        metrics: PipelineMetrics,
    ) -> None:
        super().__init__(name="extractor", daemon=True)
        self.cfg = cfg
        self.ex = cfg.extraction
        self.camera = cfg.camera.name
        self.tz = cfg.output.timezone
        self.out_dir = cfg.output.frames_dir
        self.latency = latency
        self.profiler = profiler
        self.metrics = metrics
        self.gate = FpsGate(self.ex.target_fps)
        self._gate_lock = threading.Lock()
        self.sub = bus.subscribe("extractor", maxsize=self.ex.queue_size, policy="drop_oldest", accept=self._accept)
        self.saved = 0
        self.errors = 0
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._index = CsvAppender(self.out_dir / "frames_index.csv", INDEX_FIELDS)
        self._params = [int(cv2.IMWRITE_JPEG_QUALITY), int(self.ex.jpeg_quality)]
        profiler.extraction_target_fps = self.ex.target_fps

    def _accept(self, fp: FramePacket) -> bool:
        """Runs on the capture thread: the 5 FPS gate."""
        with self._gate_lock:
            return self.gate.offer(fp.session, fp.media_s)

    def run(self) -> None:
        try:
            while True:
                item = self.sub.queue.get()
                if item is SENTINEL:
                    break
                if isinstance(item, StreamEvent):
                    continue
                self._save(item)
        except Exception:  # noqa: BLE001
            log.exception("Frame extractor crashed")
        finally:
            self._index.close()

    def _target_path(self, fp: FramePacket) -> Path:
        name = frame_filename(self.camera, fp.capture_ts, self.tz)
        d = self.out_dir
        if self.ex.partition_by_hour:
            stamp = name[len(self.camera) + 1:]
            d = d / stamp[:8] / stamp[9:11]
        path = d / name
        n = 1
        while path.exists():  # two frames in the same millisecond (should not happen at 5 FPS)
            path = d / f"{name[:-4]}_{n}.jpg"
            n += 1
        return path

    def _save(self, fp: FramePacket) -> None:
        img = fp.image if self.ex.use_downscaled or fp.source_image is None else fp.source_image
        try:
            ok, buf = cv2.imencode(".jpg", img, self._params)
            if not ok:
                raise RuntimeError("cv2.imencode failed")
            path = self._target_path(fp)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            with open(tmp, "wb") as fh:
                fh.write(buf.tobytes())
            os.replace(tmp, path)
        except Exception:  # noqa: BLE001
            self.errors += 1
            self.metrics.inc("frame_save_errors")
            log.exception("Failed to save frame %d", fp.seq)
            return

        now_mono = time.perf_counter()
        now_wall = time.time()
        save_ms = (now_mono - fp.decoded_mono) * 1000.0
        if not fp.warmup:
            self.latency.record(lat.FRAME_SAVE, save_ms)
            self.latency.record(lat.ARRIVAL_TO_FRAME, (now_mono - fp.arrival_mono) * 1000.0)
            self.latency.record(lat.CAPTURE_TO_FRAME, (now_wall - fp.capture_ts) * 1000.0)
        self.saved += 1
        self.metrics.inc("frames_saved")
        h, w = img.shape[:2]
        self.profiler.on_frame_saved(fp.capture_ts, len(buf), w, h, fp.session)
        self._index.append({
            "file": str(path.relative_to(self.out_dir)).replace("\\", "/"),
            "capture_time": iso(fp.capture_ts, self.tz),
            "media_s": round(fp.media_s, 4),
            "seq": fp.seq,
            "session": fp.session,
            "keyframe": int(fp.keyframe),
            "save_latency_ms": round(save_ms, 2),
        })

    def stats(self) -> dict[str, Any]:
        return {"saved": self.saved, "errors": self.errors, **self.sub.stats()}

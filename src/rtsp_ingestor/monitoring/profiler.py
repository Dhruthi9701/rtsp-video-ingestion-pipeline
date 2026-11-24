"""Stream profiling and telemetry for the input RTSP stream and all outputs.

Input metrics (live, from the demuxer/decoder):
  resolution, codec + profile + pixel format, nominal FPS (SDP/stream header),
  measured source FPS (from PTS), effective capture FPS (decoded frames / wall
  time connected), average + sliding-window + peak bitrate (from packet sizes),
  keyframe / GOP length (frames) and keyframe interval (seconds).

Output metrics:
  every closed clip is re-opened and probed (codec, resolution, FPS, duration,
  bitrate, GOP); extracted JPEGs are tracked (count, effective FPS, size).

Reports: ``reports/stream_profile.json`` (full, nested) and
``reports/stream_profile.csv`` (one row per stream: input / clip / frames).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import av

from ..core.types import FramePacket, StreamInfo
from ..core.utils import atomic_write_json, iso, write_csv
from .metrics import summarize

log = logging.getLogger(__name__)

PROFILE_CSV_FIELDS = [
    "role", "name", "codec", "profile", "width", "height", "pix_fmt",
    "fps_nominal", "fps_source_measured", "fps_effective", "bitrate_kbps_avg",
    "bitrate_kbps_peak", "gop_frames_mean", "gop_frames_min", "gop_frames_max",
    "keyframe_interval_s_mean", "duration_s", "frames", "size_bytes", "transport",
]


def _r(v: float | None, n: int = 3) -> float | None:
    return None if v is None else round(v, n)


def probe_media(path: str | Path, options: dict[str, str] | None = None, max_seconds: float | None = None) -> dict[str, Any]:
    """Profile a media file (or URL) by demuxing its video packets.

    Returns codec, resolution, nominal/measured FPS, duration, bitrate and GOP stats.
    """
    t0 = time.perf_counter()
    with av.open(str(path), options=options or {}) as c:
        vs = c.streams.video[0]
        cc = vs.codec_context
        tb = vs.time_base
        n = 0
        total = 0
        key_idx: list[int] = []
        key_times: list[float] = []
        first_pts = last_pts = None
        for pkt in c.demux(vs):
            if pkt.size == 0:
                continue
            if pkt.pts is not None:
                t = float(pkt.pts * tb)
                first_pts = t if first_pts is None else min(first_pts, t)
                last_pts = t if last_pts is None else max(last_pts, t)
            if pkt.is_keyframe:
                key_idx.append(n)
                if pkt.pts is not None:
                    key_times.append(float(pkt.pts * tb))
            n += 1
            total += pkt.size
            if max_seconds is not None and time.perf_counter() - t0 > max_seconds:
                break
        if vs.duration:
            duration = float(vs.duration * tb)
        elif c.duration:
            duration = c.duration / 1_000_000
        elif first_pts is not None and last_pts is not None:
            duration = last_pts - first_pts
        else:
            duration = 0.0
        if n > 1 and first_pts is not None and last_pts is not None and last_pts > first_pts:
            # pts span covers n-1 frame intervals
            measured_fps = (n - 1) / (last_pts - first_pts)
        else:
            measured_fps = None
        gops = [b - a for a, b in zip(key_idx, key_idx[1:])]
        kint = [b - a for a, b in zip(key_times, key_times[1:])]
        rate = vs.average_rate or vs.guessed_rate
        p = Path(str(path))
        return {
            "name": p.name,
            "path": str(path),
            "container": c.format.name,
            "codec": cc.name,
            "profile": cc.profile,
            "width": cc.width,
            "height": cc.height,
            "pix_fmt": cc.pix_fmt,
            "fps_nominal": _r(float(rate)) if rate else None,
            "fps_measured": _r(measured_fps),
            "duration_s": _r(duration),
            "frames": n,
            "size_bytes": p.stat().st_size if p.exists() else None,
            "bitrate_kbps": _r(total * 8 / duration / 1000, 1) if duration > 0 else None,
            "keyframes": len(key_idx),
            "gop_frames": summarize(gops) if gops else None,
            "keyframe_interval_s": summarize(kint, 3) if kint else None,
        }


class StreamProfiler:
    def __init__(self, reports_dir: Path, bitrate_window_s: float = 10.0, tz: str = "local") -> None:
        self.reports_dir = reports_dir
        self.window_s = bitrate_window_s
        self.tz = tz
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="probe")
        self.started_ts = time.time()

        self.info: StreamInfo | None = None
        self.sessions = 0
        self._connected_since: float | None = None
        self._connected_total = 0.0

        self.packets = 0
        self.bytes = 0
        self.key_packets = 0
        self.frames_decoded = 0
        self.decode_errors = 0
        self._win: deque[tuple[float, int]] = deque()
        self._win_bytes = 0
        self.peak_window_kbps = 0.0

        # per-session PTS tracking -> measured source fps
        self._sess_first_media: float | None = None
        self._sess_last_media: float | None = None
        self._sess_frames = 0
        self._media_span_total = 0.0
        self._media_frames_total = 0

        # GOP tracking
        self._frames_since_key: int | None = None
        self._last_key_media: float | None = None
        self.gop_frames: deque[int] = deque(maxlen=5000)
        self.key_intervals: deque[float] = deque(maxlen=5000)

        # outputs
        self.segments: list[dict[str, Any]] = []
        self.frames_saved = 0
        self.frames_bytes = 0
        self._frame_res: tuple[int, int] | None = None
        self._frame_first_ts: float | None = None
        self._frame_last_ts: float | None = None
        self._frame_intervals: deque[float] = deque(maxlen=50000)
        self._frames_active_span = 0.0
        self._frames_in_span = 0
        self._frame_session: int | None = None
        self.recorder: dict[str, Any] = {}
        self.extraction_target_fps: float | None = None

        # interval counters for time series
        self._iv_t = time.time()
        self._iv_frames = 0
        self._iv_bytes = 0
        self._iv_saved = 0

    # ------------------------------------------------------------------ input
    def on_connect(self, info: StreamInfo, session: int) -> None:
        with self._lock:
            self.info = info
            self.sessions = session
            self._connected_since = time.time()
            self._sess_first_media = self._sess_last_media = None
            self._sess_frames = 0
            self._frames_since_key = None
            self._last_key_media = None

    def on_disconnect(self) -> None:
        with self._lock:
            self._close_session_locked()

    def _close_session_locked(self) -> None:
        if self._connected_since is not None:
            self._connected_total += time.time() - self._connected_since
            self._connected_since = None
        if self._sess_first_media is not None and self._sess_last_media is not None and self._sess_frames > 1:
            self._media_span_total += self._sess_last_media - self._sess_first_media
            self._media_frames_total += self._sess_frames - 1
        self._sess_first_media = self._sess_last_media = None
        self._sess_frames = 0

    def on_packet(self, size: int, keyframe: bool, arrival_ts: float) -> None:
        with self._lock:
            self.packets += 1
            self.bytes += size
            self._iv_bytes += size
            if keyframe:
                self.key_packets += 1
            self._win.append((arrival_ts, size))
            self._win_bytes += size
            horizon = arrival_ts - self.window_s
            while self._win and self._win[0][0] < horizon:
                self._win_bytes -= self._win.popleft()[1]
            # only meaningful once the window is (nearly) full
            if self._connected_since is not None and arrival_ts - self._connected_since >= self.window_s:
                kbps = self._win_bytes * 8 / self.window_s / 1000
                self.peak_window_kbps = max(self.peak_window_kbps, kbps)

    def on_frame(self, fp: FramePacket) -> None:
        with self._lock:
            self.frames_decoded += 1
            self._iv_frames += 1
            if fp.pts_valid:
                if self._sess_first_media is None:
                    self._sess_first_media = fp.media_s
                    self._sess_frames = 0
                self._sess_last_media = fp.media_s
                self._sess_frames += 1
            if fp.keyframe:
                if self._frames_since_key is not None:
                    self.gop_frames.append(self._frames_since_key)
                if self._last_key_media is not None and fp.media_s > self._last_key_media:
                    self.key_intervals.append(fp.media_s - self._last_key_media)
                self._frames_since_key = 1
                self._last_key_media = fp.media_s
            elif self._frames_since_key is not None:
                self._frames_since_key += 1

    def on_decode_error(self) -> None:
        with self._lock:
            self.decode_errors += 1

    # ------------------------------------------------------------------ outputs
    def on_segment_closed(self, seg: dict[str, Any]) -> None:
        self._pool.submit(self._probe_segment, seg)

    def _probe_segment(self, seg: dict[str, Any]) -> None:
        try:
            probe = probe_media(seg["path"])
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not probe %s: %s", seg.get("path"), exc)
            probe = {"name": Path(seg["path"]).name, "path": seg["path"], "error": str(exc)}
        probe.update({k: seg[k] for k in ("index", "start_time", "end_time", "frames_written", "closed_reason") if k in seg})
        with self._lock:
            self.segments.append(probe)
        log.info(
            "Clip profile %s: %sx%s %s, %.1fs, %s frames, %.2f fps, %s kbps",
            probe.get("name"), probe.get("width"), probe.get("height"), probe.get("codec"),
            probe.get("duration_s") or 0, probe.get("frames"), probe.get("fps_measured") or 0,
            probe.get("bitrate_kbps"),
        )

    def on_frame_saved(self, capture_ts: float, nbytes: int, width: int, height: int, session: int) -> None:
        with self._lock:
            self.frames_saved += 1
            self._iv_saved += 1
            self.frames_bytes += nbytes
            self._frame_res = (width, height)
            if self._frame_first_ts is None:
                self._frame_first_ts = capture_ts
            if self._frame_last_ts is not None and self._frame_session == session:
                dt = capture_ts - self._frame_last_ts
                if dt > 0:
                    self._frame_intervals.append(dt)
                    self._frames_active_span += dt
                    self._frames_in_span += 1
            self._frame_last_ts = capture_ts
            self._frame_session = session

    def set_recorder_stats(self, stats: dict[str, Any]) -> None:
        with self._lock:
            self.recorder = dict(stats)

    # ------------------------------------------------------------------ reporting
    def _input_locked(self) -> dict[str, Any]:
        connected = self._connected_total + (time.time() - self._connected_since if self._connected_since else 0.0)
        span = self._media_span_total
        nframes = self._media_frames_total
        if self._sess_first_media is not None and self._sess_last_media is not None and self._sess_frames > 1:
            span += self._sess_last_media - self._sess_first_media
            nframes += self._sess_frames - 1
        info = self.info.as_dict() if self.info else {}
        win_kbps = self._win_bytes * 8 / self.window_s / 1000 if self._win else 0.0
        return {
            "stream": info,
            "resolution": f"{info.get('width')}x{info.get('height')}" if info else None,
            "codec": info.get("codec"),
            "profile": info.get("profile"),
            "transport": "tcp",
            "fps": {
                "nominal": _r(info.get("nominal_fps")),
                "source_measured": _r(nframes / span) if span > 0 else None,
                "effective_capture": _r(self.frames_decoded / connected) if connected > 0 else None,
            },
            "bitrate_kbps": {
                "average": _r(self.bytes * 8 / connected / 1000, 1) if connected > 0 else None,
                "current_window": _r(win_kbps, 1),
                "peak_window": _r(self.peak_window_kbps, 1),
                "window_s": self.window_s,
                "declared": _r(info["declared_bit_rate"] / 1000, 1) if info.get("declared_bit_rate") else None,
            },
            "gop": {
                "frames": summarize(self.gop_frames) if self.gop_frames else None,
                "keyframe_interval_s": summarize(self.key_intervals, 3) if self.key_intervals else None,
                "keyframes_seen": self.key_packets,
            },
            "totals": {
                "sessions": self.sessions,
                "connected_s": _r(connected, 1),
                "packets": self.packets,
                "bytes": self.bytes,
                "frames_decoded": self.frames_decoded,
                "decode_errors": self.decode_errors,
            },
        }

    def _frames_locked(self) -> dict[str, Any]:
        eff = self._frames_in_span / self._frames_active_span if self._frames_active_span > 0 else None
        return {
            "count": self.frames_saved,
            "target_fps": self.extraction_target_fps,
            "effective_fps": _r(eff),
            "interval_s": summarize(self._frame_intervals, 4) if self._frame_intervals else None,
            "resolution": f"{self._frame_res[0]}x{self._frame_res[1]}" if self._frame_res else None,
            "avg_size_bytes": int(self.frames_bytes / self.frames_saved) if self.frames_saved else None,
            "first_frame_time": iso(self._frame_first_ts, self.tz),
            "last_frame_time": iso(self._frame_last_ts, self.tz),
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            segs = list(self.segments)
            return {
                "generated_at": iso(time.time(), self.tz),
                "run_started_at": iso(self.started_ts, self.tz),
                "input": self._input_locked(),
                "output": {
                    "recording": {
                        "clips": len(segs),
                        "total_duration_s": _r(sum(s.get("duration_s") or 0 for s in segs), 1),
                        "frames_in": self.recorder.get("frames_in"),
                        "frames_written": self.recorder.get("frames_written"),
                        "frames_dropped": self.recorder.get("frames_dropped"),
                        "segments": segs,
                    },
                    "frames": self._frames_locked(),
                },
            }

    def interval_row(self) -> dict[str, Any]:
        """Per-interval figures for the telemetry time series."""
        with self._lock:
            now = time.time()
            dt = max(now - self._iv_t, 1e-6)
            row = {
                "time": iso(now, self.tz),
                "connected": self._connected_since is not None,
                "session": self.sessions,
                "decoded_fps": round(self._iv_frames / dt, 2),
                "saved_fps": round(self._iv_saved / dt, 2),
                "bitrate_kbps": round(self._iv_bytes * 8 / dt / 1000, 1),
                "gop_frames_last": self.gop_frames[-1] if self.gop_frames else None,
                "frames_decoded_total": self.frames_decoded,
                "frames_saved_total": self.frames_saved,
                "decode_errors_total": self.decode_errors,
            }
            self._iv_t = now
            self._iv_frames = self._iv_bytes = self._iv_saved = 0
            return row

    def csv_rows(self, snap: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        inp = snap["input"]
        st = inp["stream"] or {}
        gop = (inp["gop"]["frames"] or {})
        kint = (inp["gop"]["keyframe_interval_s"] or {})
        rows.append({
            "role": "input", "name": st.get("url_masked"), "codec": st.get("codec"), "profile": st.get("profile"),
            "width": st.get("width"), "height": st.get("height"), "pix_fmt": st.get("pix_fmt"),
            "fps_nominal": inp["fps"]["nominal"], "fps_source_measured": inp["fps"]["source_measured"],
            "fps_effective": inp["fps"]["effective_capture"], "bitrate_kbps_avg": inp["bitrate_kbps"]["average"],
            "bitrate_kbps_peak": inp["bitrate_kbps"]["peak_window"], "gop_frames_mean": gop.get("mean"),
            "gop_frames_min": gop.get("min"), "gop_frames_max": gop.get("max"),
            "keyframe_interval_s_mean": kint.get("mean"), "duration_s": inp["totals"]["connected_s"],
            "frames": inp["totals"]["frames_decoded"], "size_bytes": inp["totals"]["bytes"], "transport": "tcp",
        })
        for s in snap["output"]["recording"]["segments"]:
            g = s.get("gop_frames") or {}
            k = s.get("keyframe_interval_s") or {}
            rows.append({
                "role": "output_clip", "name": s.get("name"), "codec": s.get("codec"), "profile": s.get("profile"),
                "width": s.get("width"), "height": s.get("height"), "pix_fmt": s.get("pix_fmt"),
                "fps_nominal": s.get("fps_nominal"), "fps_source_measured": s.get("fps_measured"),
                "fps_effective": s.get("fps_measured"), "bitrate_kbps_avg": s.get("bitrate_kbps"),
                "gop_frames_mean": g.get("mean"), "gop_frames_min": g.get("min"), "gop_frames_max": g.get("max"),
                "keyframe_interval_s_mean": k.get("mean"), "duration_s": s.get("duration_s"),
                "frames": s.get("frames"), "size_bytes": s.get("size_bytes"),
            })
        fr = snap["output"]["frames"]
        w = h = None
        if fr.get("resolution"):
            w, h = (int(x) for x in fr["resolution"].split("x"))
        rows.append({
            "role": "output_frames", "name": "jpeg_frames", "codec": "mjpeg", "width": w, "height": h,
            "fps_nominal": fr.get("target_fps"), "fps_effective": fr.get("effective_fps"),
            "frames": fr.get("count"),
            "size_bytes": (fr.get("avg_size_bytes") or 0) * (fr.get("count") or 0),
        })
        return rows

    def write(self) -> dict[str, Any]:
        snap = self.snapshot()
        atomic_write_json(self.reports_dir / "stream_profile.json", snap)
        write_csv(self.reports_dir / "stream_profile.csv", self.csv_rows(snap), PROFILE_CSV_FIELDS)
        return snap

    def close(self) -> dict[str, Any]:
        self._pool.shutdown(wait=True)
        with self._lock:
            self._close_session_locked()
        return self.write()

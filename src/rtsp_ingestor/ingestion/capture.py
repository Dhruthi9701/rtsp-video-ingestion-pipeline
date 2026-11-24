"""Capture thread: RTSP session lifecycle, decoding, timestamping and fan-out.

The capture thread is the only producer in the pipeline. It owns the
:class:`RtspClient`, reconnects with exponential backoff, detects stalls, and
publishes :class:`FramePacket` objects (already downscaled once) to the
:class:`FrameBus`. Consumers never touch the network.
"""

from __future__ import annotations

import logging
import threading
import time

import av
import cv2
import numpy as np

from ..core.config import AppConfig
from ..core.exceptions import StreamError, StreamStallError, TransportError
from ..core.types import FramePacket, StreamEvent, StreamInfo
from ..monitoring import latency as lat
from ..monitoring.latency import LatencyTracker, SourceClock
from ..monitoring.metrics import PipelineMetrics
from ..monitoring.profiler import StreamProfiler
from ..recovery.downtime_logger import DowntimeLogger
from ..recovery.reconnect import BackoffPolicy
from .frame_bus import FrameBus
from .rtsp_client import RtspClient, classify_error

log = logging.getLogger(__name__)


class FrameScaler:
    """Convert a decoded frame to BGR at the output resolution (one resize per frame)."""

    def __init__(self, width: int, height: int, keep_aspect: bool = False) -> None:
        self.width = width
        self.height = height
        self.keep_aspect = keep_aspect

    def scale_array(self, src: np.ndarray) -> np.ndarray:
        h, w = src.shape[:2]
        if (w, h) == (self.width, self.height):
            return src
        if not self.keep_aspect:
            return cv2.resize(src, (self.width, self.height), interpolation=cv2.INTER_AREA)
        scale = min(self.width / w, self.height / h)
        nw, nh = max(2, int(round(w * scale)) // 2 * 2), max(2, int(round(h * scale)) // 2 * 2)
        resized = cv2.resize(src, (nw, nh), interpolation=cv2.INTER_AREA)
        top = (self.height - nh) // 2
        left = (self.width - nw) // 2
        return cv2.copyMakeBorder(resized, top, self.height - nh - top, left, self.width - nw - left,
                                  cv2.BORDER_CONSTANT, value=(0, 0, 0))

    def __call__(self, frame: av.VideoFrame, need_source: bool) -> tuple[np.ndarray, np.ndarray | None]:
        if need_source or self.keep_aspect:
            src = frame.to_ndarray(format="bgr24")
            return self.scale_array(src), (src if need_source else None)
        if (frame.width, frame.height) == (self.width, self.height):
            return frame.to_ndarray(format="bgr24"), None
        # swscale does colour conversion + area resize in one pass
        out = frame.reformat(width=self.width, height=self.height, format="bgr24", interpolation="AREA")
        return out.to_ndarray(), None


class CaptureWorker(threading.Thread):
    def __init__(
        self,
        cfg: AppConfig,
        bus: FrameBus,
        profiler: StreamProfiler,
        latency: LatencyTracker,
        downtime: DowntimeLogger,
        metrics: PipelineMetrics,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="capture", daemon=True)
        self.cfg = cfg
        self.bus = bus
        self.profiler = profiler
        self.latency = latency
        self.downtime = downtime
        self.metrics = metrics
        self.stop_event = stop_event
        self.need_source = cfg.extraction.enabled and not cfg.extraction.use_downscaled
        self.scaler = FrameScaler(cfg.recording.width, cfg.recording.height, cfg.recording.keep_aspect)
        self.clock = SourceClock(cfg.latency.clock_window_s)
        self.seq = 0
        self.session = 0
        self.fatal_error: BaseException | None = None
        self.connected = threading.Event()
        self.last_info: StreamInfo | None = None

    # ------------------------------------------------------------------ thread
    def run(self) -> None:
        try:
            self._run()
        except BaseException as exc:  # noqa: BLE001
            log.exception("Capture thread failed fatally")
            self.fatal_error = exc
            self.stop_event.set()
        finally:
            self.connected.clear()

    def _run(self) -> None:
        cfg = self.cfg
        policy = BackoffPolicy.from_config(cfg.reconnect)
        attempt = 0
        while not self.stop_event.is_set():
            if self.downtime.in_outage:
                attempt += 1
            client = RtspClient(cfg.camera.rtsp_url, cfg.stream)
            try:
                info = client.open()
            except TransportError:
                raise
            except StreamError as err:
                self.metrics.inc("connect_failures")
                if not self.downtime.in_outage:
                    reason = "initial_connect_failed" if self.session == 0 else err.reason
                    self.downtime.mark_disconnected(reason, str(err), session=self.session or None)
                    attempt = 1
                delay = policy.next_delay()
                self.downtime.record_attempt(attempt, False, err.reason, str(err), delay)
                if delay is None:
                    log.error("Giving up after %d reconnect attempts", attempt)
                    self.fatal_error = err
                    self.stop_event.set()
                    return
                log.warning("Connect attempt %d failed (%s: %s); retrying in %.2fs", attempt, err.reason, err, delay)
                self.stop_event.wait(delay)
                continue

            # ---- connected
            self.session += 1
            session = self.session
            self.last_info = info
            if self.downtime.in_outage:
                self.downtime.record_attempt(attempt, True)
                self.downtime.mark_recovered(session)
                self.metrics.inc("reconnects")
            attempt = 0
            policy.reset()
            if info.connect_rtt_ms is not None:
                self.latency.record(lat.CONNECT_RTT, info.connect_rtt_ms)
            self.profiler.on_connect(info, session)
            self.bus.publish(StreamEvent("connected", session, time.time(), info=info), control=True)
            self.connected.set()

            reason, detail = "shutdown", None
            try:
                self._consume(client, info, session)
            except StreamError as err:
                reason, detail = err.reason, str(err)
            except Exception as exc:  # noqa: BLE001 - never crash on stream errors
                err = classify_error(exc)
                reason, detail = err.reason, str(err)
                log.debug("Unclassified capture error", exc_info=True)
            finally:
                client.close()
                self.connected.clear()
                self.profiler.on_disconnect()

            if self.stop_event.is_set():
                self.bus.publish(StreamEvent("disconnected", session, time.time(), reason="shutdown"), control=True)
                break

            self.bus.publish(StreamEvent("disconnected", session, time.time(), reason=reason), control=True)
            self.downtime.mark_disconnected(reason, detail, session=session)
            self.metrics.inc("disconnects")
            delay = policy.next_delay() or 0.0
            log.warning("Stream lost (%s: %s); reconnecting in %.2fs", reason, detail, delay)
            self.stop_event.wait(delay)

    # ------------------------------------------------------------------ session
    def _consume(self, client: RtspClient, info: StreamInfo, session: int) -> None:
        cfg = self.cfg
        tb = client.time_base
        nominal_dt = 1.0 / info.nominal_fps if info.nominal_fps else 0.04
        stall_s = cfg.stream.stall_timeout_s
        warmup_s = cfg.latency.warmup_s
        self.clock.reset()

        start_mono = time.perf_counter()
        last_progress = start_mono
        last_media: float | None = None
        media_offset = 0.0
        frames_this_session = 0

        for packet, arrival_ts, arrival_mono in client.packets():
            if self.stop_event.is_set():
                return
            self.metrics.inc("packets")
            self.profiler.on_packet(packet.size, packet.is_keyframe, arrival_ts)
            try:
                frames = packet.decode()
            except av.error.FFmpegError as exc:
                self.metrics.inc("decode_errors")
                self.profiler.on_decode_error()
                log.debug("decode error (continuing): %s", exc)
                frames = []

            for frame in frames:
                decoded_mono = time.perf_counter()
                if frame.pts is not None:
                    media = float(frame.pts * tb) + media_offset
                    pts_valid = True
                else:
                    media = (last_media + nominal_dt) if last_media is not None else 0.0
                    pts_valid = False

                if last_media is not None:
                    if media < last_media - 1.0:
                        # timeline reset inside the session (camera restart / RTP wrap):
                        # keep media time continuous for downstream consumers
                        media_offset += last_media + nominal_dt - media
                        media = last_media + nominal_dt
                        self.clock.reset()
                        log.warning("PTS discontinuity detected; re-based media timeline")
                    elif media <= last_media:
                        media = last_media + 1e-4  # duplicate PTS: keep strictly increasing
                    else:
                        last_progress = decoded_mono
                else:
                    last_progress = decoded_mono
                last_media = media

                capture_ts, jitter_s = self.clock.update(media, arrival_ts)
                warmup = (decoded_mono - start_mono) < warmup_s
                image, source = self.scaler(frame, self.need_source)
                keyframe = bool(getattr(frame, "key_frame", packet.is_keyframe))

                self.seq += 1
                frames_this_session += 1
                fp = FramePacket(
                    seq=self.seq,
                    session=session,
                    image=image,
                    source_image=source,
                    media_s=media,
                    pts_valid=pts_valid,
                    capture_ts=capture_ts,
                    arrival_ts=arrival_ts,
                    arrival_mono=arrival_mono,
                    decoded_mono=decoded_mono,
                    keyframe=keyframe,
                    warmup=warmup,
                )
                if not warmup:
                    self.latency.record(lat.SOURCE_JITTER, jitter_s * 1000.0)
                    self.latency.record(lat.DECODE, (decoded_mono - arrival_mono) * 1000.0)
                self.profiler.on_frame(fp)
                self.metrics.inc("frames_decoded")
                self.bus.publish(fp)

            if time.perf_counter() - last_progress > stall_s:
                raise StreamStallError(
                    f"no new frames for {time.perf_counter() - last_progress:.1f}s "
                    f"(packets arriving but not decodable/advancing)"
                )

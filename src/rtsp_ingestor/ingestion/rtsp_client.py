"""RTSP client built on PyAV/FFmpeg with **enforced TCP transport**.

How TCP is enforced (defence in depth):

1. Config layer: ``stream.transport`` only accepts ``"tcp"`` (``TransportError`` otherwise).
2. Client layer: FFmpeg option ``rtsp_transport=tcp`` is always set and
   :func:`build_ffmpeg_options` asserts it; with this option FFmpeg's RTSP demuxer
   offers *only* ``RTP/AVP/TCP;interleaved`` in SETUP and never falls back to UDP.
3. Runtime verification: during open we capture FFmpeg's debug log and parse the
   server's SETUP reply ``Transport:`` header. If the server negotiated anything
   other than ``RTP/AVP/TCP`` the connection is rejected.
"""

from __future__ import annotations

import logging
import re
import socket
import time
from collections.abc import Iterator
from fractions import Fraction
from urllib.parse import urlsplit

import av
import av.logging

from ..core.config import StreamConfig
from ..core.exceptions import (
    StreamEndedError,
    StreamError,
    StreamOpenError,
    StreamReadTimeout,
    TransportError,
)
from ..core.types import StreamInfo
from ..core.utils import mask_url

log = logging.getLogger(__name__)

_TRANSPORT_LINE_RE = re.compile(r"Transport:\s*([^\r\n']+)", re.IGNORECASE)


def build_ffmpeg_options(cfg: StreamConfig) -> dict[str, str]:
    """FFmpeg demuxer options. ``rtsp_transport=tcp`` is non-negotiable."""
    opts: dict[str, str] = {
        "rtsp_transport": "tcp",  # interleaved RTP over the RTSP TCP connection
        "timeout": str(int(cfg.read_timeout_s * 1_000_000)),  # socket I/O timeout (us)
        "max_delay": str(int(cfg.max_delay_ms * 1000)),
        "buffer_size": str(cfg.socket_buffer_bytes),
        "probesize": "1000000",
        "analyzeduration": "2000000",
    }
    if cfg.video_only:
        opts["allowed_media_types"] = "video"
    if cfg.low_latency:
        opts["fflags"] = "nobuffer"
        opts["flags"] = "low_delay"
    assert_tcp_options(opts)
    return opts


def assert_tcp_options(opts: dict[str, str]) -> None:
    if opts.get("rtsp_transport") != "tcp":
        raise TransportError(f"rtsp_transport must be 'tcp', got {opts.get('rtsp_transport')!r}")
    if "udp" in opts.get("rtsp_flags", ""):
        raise TransportError("rtsp_flags must not enable UDP")


def parse_negotiated_transport(log_lines: list[str]) -> str | None:
    """Extract the server's SETUP-reply Transport header from FFmpeg debug logs."""
    found: str | None = None
    for line in log_lines:
        # Prefer the *response* line ("line='Transport: ...'") over our request.
        if "line='" in line or line.lstrip().lower().startswith("transport:"):
            m = _TRANSPORT_LINE_RE.search(line)
            if m:
                found = m.group(1).strip()
    if found is None:
        for line in log_lines:
            m = _TRANSPORT_LINE_RE.search(line)
            if m:
                found = m.group(1).strip()
    return found


def measure_tcp_rtt(url: str, timeout: float = 3.0) -> float | None:
    """TCP handshake time to the RTSP server (ms) - a proxy for network RTT."""
    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port or (322 if parts.scheme == "rtsps" else 554)
        if not host:
            return None
        t0 = time.perf_counter()
        with socket.create_connection((host, port), timeout=timeout):
            pass
        return (time.perf_counter() - t0) * 1000.0
    except OSError:
        return None


def classify_error(exc: BaseException) -> StreamError:
    """Map FFmpeg/OS exceptions onto recoverable :class:`StreamError` reason codes."""
    if isinstance(exc, StreamError):
        return exc
    name = type(exc).__name__
    msg = str(exc)
    low = msg.lower()
    if isinstance(exc, av.error.ExitError) or "immediate exit" in low or isinstance(exc, (TimeoutError, socket.timeout)) or "timed out" in low or "timeout" in name.lower():
        return StreamReadTimeout(f"read timeout: {msg}")
    if isinstance(exc, av.error.EOFError) or isinstance(exc, EOFError) or "end of file" in low:
        return StreamEndedError(f"end of stream: {msg}")
    if "401" in msg or "unauthorized" in low:
        return StreamError(f"authentication failed: {msg}", reason="auth_failed")
    if "404" in msg or "not found" in low:
        return StreamError(f"stream not found: {msg}", reason="not_found")
    if isinstance(exc, ConnectionRefusedError) or "refused" in low:
        return StreamError(f"connection refused: {msg}", reason="connection_refused")
    if isinstance(exc, ConnectionResetError) or "reset" in low:
        return StreamError(f"connection reset: {msg}", reason="connection_reset")
    if isinstance(exc, (OSError, av.error.FFmpegError)):
        return StreamError(f"{name}: {msg}", reason="network_error")
    return StreamError(f"{name}: {msg}", reason="unexpected_error")


class RtspClient:
    """Owns one RTSP session (one ``av.open``). Not thread-safe; used by the capture thread."""

    def __init__(self, url: str, cfg: StreamConfig, verify_transport: bool = True) -> None:
        self.url = url
        self.cfg = cfg
        self.verify_transport = verify_transport
        self.options = build_ffmpeg_options(cfg)
        self._container: av.container.InputContainer | None = None
        self._stream: av.video.stream.VideoStream | None = None
        self.info: StreamInfo | None = None

    # ------------------------------------------------------------------ lifecycle
    def open(self) -> StreamInfo:
        masked = mask_url(self.url)
        rtt = measure_tcp_rtt(self.url, timeout=min(self.cfg.open_timeout_s, 3.0))
        log.info("Opening %s (transport=tcp, open_timeout=%.1fs, read_timeout=%.1fs)",
                 masked, self.cfg.open_timeout_s, self.cfg.read_timeout_s)
        t0 = time.perf_counter()
        captured: list[str] = []
        try:
            if self.verify_transport:
                prev = av.logging.get_level()
                av.logging.set_level(av.logging.DEBUG)
                try:
                    with av.logging.Capture(local=False) as logs:
                        container = av.open(
                            self.url,
                            options=self.options,
                            timeout=(self.cfg.open_timeout_s, self.cfg.read_timeout_s),
                        )
                finally:
                    av.logging.set_level(prev)
                captured = [entry[2] for entry in logs if entry[1] in ("rtsp", None) or "Transport" in entry[2]]
            else:
                container = av.open(
                    self.url,
                    options=self.options,
                    timeout=(self.cfg.open_timeout_s, self.cfg.read_timeout_s),
                )
        except Exception as exc:  # noqa: BLE001 - classified below
            err = classify_error(exc)
            raise StreamOpenError(str(err), reason=err.reason if err.reason != "stream_error" else "open_failed") from exc

        open_ms = (time.perf_counter() - t0) * 1000.0
        if not container.streams.video:
            container.close()
            raise StreamOpenError("no video track in stream", reason="no_video_track")

        negotiated = parse_negotiated_transport(captured) if self.verify_transport else None
        verified = False
        if negotiated is not None:
            if "TCP" not in negotiated.upper():
                container.close()
                raise TransportError(f"server negotiated non-TCP transport: {negotiated}")
            verified = True
        elif self.verify_transport:
            log.warning("Could not read negotiated transport from FFmpeg log; relying on rtsp_transport=tcp")

        vs = container.streams.video[0]
        vs.thread_type = "AUTO"
        cc = vs.codec_context
        rate = vs.average_rate or vs.guessed_rate or vs.base_rate
        self._container = container
        self._stream = vs
        self.info = StreamInfo(
            url_masked=masked,
            container_format=container.format.name,
            transport="tcp",
            transport_verified=verified,
            negotiated_transport=negotiated,
            codec=cc.name,
            codec_long_name=cc.codec.long_name,
            profile=cc.profile,
            width=cc.width,
            height=cc.height,
            pix_fmt=cc.pix_fmt,
            nominal_fps=float(rate) if rate else None,
            time_base=str(vs.time_base),
            declared_bit_rate=cc.bit_rate or None,
            other_tracks=[f"{s.type}:{s.codec_context.name}" for s in container.streams if s is not vs],
            connect_rtt_ms=round(rtt, 2) if rtt is not None else None,
            open_duration_ms=round(open_ms, 1),
        )
        log.info(
            "Connected in %.0f ms: %s %s %dx%d @ %s fps, transport=%s%s",
            open_ms, cc.name, cc.profile or "", cc.width, cc.height,
            f"{self.info.nominal_fps:.2f}" if self.info.nominal_fps else "?",
            negotiated or "tcp", " (verified)" if verified else "",
        )
        return self.info

    def close(self) -> None:
        if self._container is not None:
            try:
                self._container.close()
            except Exception:  # noqa: BLE001
                log.debug("error while closing container", exc_info=True)
        self._container = None
        self._stream = None

    def __enter__(self) -> RtspClient:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ reading
    @property
    def time_base(self) -> Fraction:
        assert self._stream is not None
        return self._stream.time_base

    def packets(self) -> Iterator[tuple[av.Packet, float, float]]:
        """Yield ``(packet, arrival_wall_ts, arrival_mono)`` for the video track.

        Raises a classified :class:`StreamError` on timeout/EOF/network errors.
        """
        if self._container is None or self._stream is None:
            raise StreamOpenError("client not open")
        try:
            for packet in self._container.demux(self._stream):
                arrival_mono = time.perf_counter()
                arrival_ts = time.time()
                if packet.size == 0 and packet.pts is None:
                    continue  # flush packet emitted at EOF
                yield packet, arrival_ts, arrival_mono
        except StreamError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise classify_error(exc) from exc
        raise StreamEndedError("server closed the stream (EOF)")

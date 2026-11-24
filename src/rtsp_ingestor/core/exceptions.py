"""Exception hierarchy for the RTSP ingestor.

Every failure that the capture loop can recover from derives from
:class:`StreamError`; the reconnect logic catches that family, logs a reason
code (``reason``) and backs off. Anything else is a programming/config error.
"""

from __future__ import annotations


class IngestorError(Exception):
    """Base class for all ingestor errors."""


class ConfigError(IngestorError):
    """Invalid or missing configuration."""


class TransportError(ConfigError):
    """Attempt to use a transport other than RTSP-over-TCP."""


class StreamError(IngestorError):
    """Recoverable stream failure. ``reason`` is a short machine-readable code."""

    reason: str = "stream_error"

    def __init__(self, message: str = "", reason: str | None = None) -> None:
        super().__init__(message or self.__class__.__name__)
        if reason:
            self.reason = reason


class StreamOpenError(StreamError):
    reason = "open_failed"


class StreamReadTimeout(StreamError):
    reason = "read_timeout"


class StreamStallError(StreamError):
    reason = "stream_stall"


class StreamEndedError(StreamError):
    reason = "end_of_stream"


class RecordingError(IngestorError):
    """Segment encoding/muxing failure (handled inside the recorder)."""

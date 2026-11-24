"""Core building blocks: configuration, logging, exceptions, shared types."""

from .config import AppConfig, load_config, parse_set_overrides
from .exceptions import (
    ConfigError,
    IngestorError,
    RecordingError,
    StreamEndedError,
    StreamError,
    StreamOpenError,
    StreamReadTimeout,
    StreamStallError,
    TransportError,
)
from .logging_config import setup_logging

__all__ = [
    "AppConfig",
    "ConfigError",
    "IngestorError",
    "RecordingError",
    "StreamEndedError",
    "StreamError",
    "StreamOpenError",
    "StreamReadTimeout",
    "StreamStallError",
    "TransportError",
    "load_config",
    "parse_set_overrides",
    "setup_logging",
]

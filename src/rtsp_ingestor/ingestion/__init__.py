"""RTSP client, capture worker, and frame fan-out bus."""

from .capture import CaptureWorker, FrameScaler
from .frame_bus import SENTINEL, FrameBus, Subscriber
from .rtsp_client import RtspClient, assert_tcp_options, build_ffmpeg_options, parse_negotiated_transport

__all__ = [
    "CaptureWorker",
    "FrameBus",
    "FrameScaler",
    "RtspClient",
    "SENTINEL",
    "Subscriber",
    "assert_tcp_options",
    "build_ffmpeg_options",
    "parse_negotiated_transport",
]

"""Unit tests for RtspClient TCP enforcement, option building, and error classification."""

from __future__ import annotations

import socket

import pytest

from rtsp_ingestor.core.config import StreamConfig
from rtsp_ingestor.core.exceptions import StreamEndedError, StreamReadTimeout, TransportError
from rtsp_ingestor.ingestion.rtsp_client import (
    assert_tcp_options,
    build_ffmpeg_options,
    classify_error,
    parse_negotiated_transport,
)


def test_build_ffmpeg_options_sets_tcp() -> None:
    opts = build_ffmpeg_options(StreamConfig())
    assert opts["rtsp_transport"] == "tcp"
    assert opts["fflags"] == "nobuffer"
    assert opts["flags"] == "low_delay"


def test_assert_tcp_options_rejects_udp() -> None:
    with pytest.raises(TransportError):
        assert_tcp_options({"rtsp_transport": "udp"})
    with pytest.raises(TransportError):
        assert_tcp_options({"rtsp_transport": "tcp", "rtsp_flags": "prefer_tcp+udp"})


def test_parse_negotiated_transport_prefers_response_line() -> None:
    logs = [
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1",
        "line='CSeq: 3'",
        "line='Transport: RTP/AVP/TCP;unicast;interleaved=0-1;ssrc=1234ABCD'",
    ]
    parsed = parse_negotiated_transport(logs)
    assert parsed is not None
    assert "RTP/AVP/TCP" in parsed
    assert "ssrc=1234ABCD" in parsed


def test_classify_error_maps_timeout_and_eof() -> None:
    assert isinstance(classify_error(socket.timeout("timed out")), StreamReadTimeout)
    assert isinstance(classify_error(EOFError("end of file")), StreamEndedError)

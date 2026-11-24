"""Unit tests for FrameBus and SegmentWriter boundary rollover."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import numpy as np

from rtsp_ingestor.core.config import load_config
from rtsp_ingestor.core.types import FramePacket, StreamEvent, StreamInfo
from rtsp_ingestor.ingestion.frame_bus import FrameBus
from rtsp_ingestor.monitoring.latency import LatencyTracker
from rtsp_ingestor.monitoring.metrics import PipelineMetrics
from rtsp_ingestor.monitoring.profiler import StreamProfiler
from rtsp_ingestor.recording.segment_writer import SegmentWriter


def test_segment_writer_rolls_over_without_dropping_frames(tmp_path: Path) -> None:
    cfg = load_config(
        overrides={
            "camera": {"name": "testcam"},
            "recording": {"segment_seconds": 2.0, "width": 160, "height": 120, "gop_seconds": 1.0},
            "output": {"root_dir": str(tmp_path)},
        },
        use_env=False,
        dotenv_path=None,
    )
    cfg.ensure_dirs()
    stop = threading.Event()
    bus = FrameBus(stop)
    latency = LatencyTracker()
    profiler = StreamProfiler(cfg.output.reports_dir)
    metrics = PipelineMetrics()

    writer = SegmentWriter(cfg, bus, latency, profiler, metrics)
    writer.start()

    info = StreamInfo(
        url_masked="rtsp://test", container_format="rtsp", transport="tcp",
        transport_verified=True, negotiated_transport="RTP/AVP/TCP", codec="h264",
        codec_long_name="H.264", profile="Main", width=160, height=120,
        pix_fmt="yuv420p", nominal_fps=10.0, time_base="1/90000", declared_bit_rate=None,
    )
    bus.publish(StreamEvent("connected", 1, time.time(), info=info), control=True)

    # 4.5 seconds at 10 fps = 45 frames -> 3 segments (2.0s, 2.0s, 0.5s)
    base_ts = time.time()
    for i in range(45):
        img = np.full((120, 160, 3), (i * 5) % 255, dtype=np.uint8)
        now_m = time.perf_counter()
        bus.publish(
            FramePacket(
                seq=i + 1, session=1, image=img, source_image=None,
                media_s=i * 0.1, pts_valid=True, capture_ts=base_ts + i * 0.1,
                arrival_ts=base_ts + i * 0.1, arrival_mono=now_m, decoded_mono=now_m,
                keyframe=(i % 10 == 0), warmup=False,
            )
        )

    bus.close()
    writer.join(timeout=15.0)
    profiler.close()

    assert writer.frames_in == 45
    assert writer.frames_written == 45
    assert writer.frames_dropped == 0
    assert len(writer.closed) == 3
    assert sum(s["frames_written"] for s in writer.closed) == 45
    assert writer.closed[0]["closed_reason"] == "rollover"
    assert writer.closed[1]["closed_reason"] == "rollover"
    assert writer.closed[2]["closed_reason"] == "shutdown"

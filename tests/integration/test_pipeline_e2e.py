"""End-to-end integration test against a local MediaMTX + FFmpeg RTSP simulator."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from rtsp_ingestor.core.config import load_config
from rtsp_ingestor.main import Pipeline
from rtsp_ingestor.monitoring.profiler import probe_media

MEDIAMTX_BIN = Path("tools/mediamtx/mediamtx.exe") if os.name == "nt" else Path("tools/mediamtx/mediamtx")


def _have_simulator_bins() -> bool:
    return MEDIAMTX_BIN.is_file() and shutil.which("ffmpeg") is not None


@pytest.mark.integration
@pytest.mark.skipif(not _have_simulator_bins(), reason="MediaMTX or ffmpeg not available locally")
def test_pipeline_with_simulated_disconnect(tmp_path: Path) -> None:
    mtx_cfg = tmp_path / "mediamtx.yml"
    mtx_cfg.write_text("rtspAddress: :18554\npaths:\n  all:\n", encoding="utf-8")
    mtx = subprocess.Popen([str(MEDIAMTX_BIN), str(mtx_cfg)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def start_pub() -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            [
                "ffmpeg", "-re", "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=25",
                "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-g", "25", "-f", "rtsp", "-rtsp_transport", "tcp", "rtsp://127.0.0.1:18554/sim",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    pub = start_pub()
    time.sleep(2.5)
    try:
        cfg = load_config(
            overrides={
                "camera": {"name": "simtest", "rtsp_url": "rtsp://127.0.0.1:18554/sim"},
                "stream": {"open_timeout_s": 4, "read_timeout_s": 2, "stall_timeout_s": 3},
                "recording": {"segment_seconds": 4.0, "width": 640, "height": 360},
                "extraction": {"target_fps": 5.0},
                "reconnect": {"initial_delay_s": 0.5, "max_delay_s": 2.0},
                "latency": {"warmup_s": 1.0},
                "output": {"root_dir": str(tmp_path / "out")},
                "run_duration_s": 14.0,
            },
            use_env=False,
            dotenv_path=None,
        )
        cfg.ensure_dirs()
        pipe = Pipeline(cfg)
        pipe.start()

        # Let it record ~5s, then kill publisher for 2.5s to force disconnect + recovery
        time.sleep(5.5)
        pub.terminate()
        pub.wait(timeout=5)
        time.sleep(2.5)
        pub = start_pub()
        time.sleep(6.0)

        summary = pipe.shutdown()
        assert summary["recorder"]["frames_written"] > 100
        assert summary["recorder"]["frames_dropped"] == 0
        assert summary["extractor"]["saved"] >= 20
        assert summary["downtime"]["outages"] >= 1
        assert summary["downtime"]["total_downtime_s"] > 0

        # Verify every closed clip is valid 640x360 H.264
        clips = list((tmp_path / "out" / "segments").rglob("*.mp4"))
        assert len(clips) >= 2
        probed = probe_media(clips[0])
        assert probed["width"] == 640 and probed["height"] == 360
        assert probed["codec"] == "h264"
    finally:
        pub.terminate()
        mtx.terminate()

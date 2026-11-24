"""Local MediaMTX + FFmpeg simulator runner with an optional disconnect/recovery test.

Usage:
    python scripts/simulate_disconnect.py --duration 25 --disconnect-at 10 --outage 4
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from rtsp_ingestor.core.config import load_config  # noqa: E402
from rtsp_ingestor.core.logging_config import setup_logging  # noqa: E402
from rtsp_ingestor.main import Pipeline  # noqa: E402


def _mediamtx_path() -> Path:
    name = "mediamtx.exe" if os.name == "nt" else "mediamtx"
    p = ROOT / "tools" / "mediamtx" / name
    if p.is_file():
        return p
    found = shutil.which("mediamtx")
    if found:
        return Path(found)
    raise SystemExit("MediaMTX not found in tools/mediamtx or PATH.")


def _start_publisher(url: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            "ffmpeg", "-re", "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=25",
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-g", "25", "-f", "rtsp", "-rtsp_transport", "tcp", url,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Run RTSP ingestor against local MediaMTX with simulated disconnect")
    ap.add_argument("--duration", type=float, default=25.0)
    ap.add_argument("--disconnect-at", type=float, default=10.0)
    ap.add_argument("--outage", type=float, default=4.0)
    ap.add_argument("--segment-seconds", type=float, default=8.0)
    ap.add_argument("--output", default="output/disconnect_test")
    args = ap.parse_args()

    mtx_bin = _mediamtx_path()
    with tempfile.TemporaryDirectory() as td:
        mtx_cfg = Path(td) / "mediamtx.yml"
        mtx_cfg.write_text("rtspAddress: :18555\npaths:\n  all:\n", encoding="utf-8")
        mtx = subprocess.Popen([str(mtx_bin), str(mtx_cfg)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        url = "rtsp://127.0.0.1:18555/simcam"
        pub = _start_publisher(url)
        time.sleep(2.5)

        try:
            cfg = load_config(
                overrides={
                    "camera": {"name": "simcam", "rtsp_url": url},
                    "stream": {"open_timeout_s": 4, "read_timeout_s": 2, "stall_timeout_s": 4},
                    "recording": {"segment_seconds": args.segment_seconds, "width": 640, "height": 360},
                    "extraction": {"target_fps": 5.0},
                    "reconnect": {"initial_delay_s": 0.5, "max_delay_s": 2.0},
                    "latency": {"warmup_s": 1.0},
                    "output": {"root_dir": args.output},
                },
                use_env=False,
                dotenv_path=None,
            )
            cfg.ensure_dirs()
            setup_logging(level="INFO", log_file=cfg.output.root_dir / "logs" / "ingestor.log")
            pipe = Pipeline(cfg)
            pipe.start()

            time.sleep(args.disconnect_at)
            print(f"\n>>> Simulating RTSP disconnect for {args.outage}s...")
            pub.terminate()
            pub.wait(timeout=5)
            time.sleep(args.outage)
            print(">>> Restoring RTSP publisher...")
            pub = _start_publisher(url)
            remaining = max(2.0, args.duration - args.disconnect_at - args.outage)
            time.sleep(remaining)

            summary = pipe.shutdown()
            print(f"\nDisconnect test complete. Outages={summary['downtime']['outages']}, "
                  f"Downtime={summary['downtime']['total_downtime_s']}s, "
                  f"FramesRecorded={summary['recorder']['frames_written']}")
            return 0
        finally:
            pub.terminate()
            mtx.terminate()


if __name__ == "__main__":
    raise SystemExit(main())

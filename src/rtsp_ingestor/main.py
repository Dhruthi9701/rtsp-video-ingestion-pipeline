"""CLI and pipeline orchestration for the RTSP ingestion service.

Subcommands:
  * ``rtsp-ingestor run``   - run the full ingestion pipeline (capture, record, extract, profile, latency).
  * ``rtsp-ingestor probe`` - one-shot TCP-enforced RTSP stream or local media file profiler.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from .core.config import AppConfig, load_config, parse_set_overrides
from .core.exceptions import IngestorError
from .core.logging_config import setup_logging
from .core.utils import CsvAppender, atomic_write_json, iso
from .extraction.frame_sampler import FrameExtractor
from .ingestion.capture import CaptureWorker
from .ingestion.frame_bus import FrameBus
from .ingestion.rtsp_client import RtspClient
from .monitoring.latency import LatencyTracker
from .monitoring.metrics import PipelineMetrics
from .monitoring.profiler import StreamProfiler, probe_media
from .recovery.downtime_logger import DowntimeLogger
from .recording.segment_writer import SegmentWriter

log = logging.getLogger(__name__)

TELEMETRY_FIELDS = [
    "time", "connected", "session", "decoded_fps", "saved_fps",
    "bitrate_kbps", "gop_frames_last", "frames_decoded_total",
    "frames_saved_total", "decode_errors_total",
    "arrival_to_decode_p50_ms", "arrival_to_decode_p95_ms",
    "arrival_to_record_p50_ms", "arrival_to_record_p95_ms",
    "arrival_to_frame_p50_ms", "arrival_to_frame_p95_ms",
]


class Pipeline:
    """Wires together capture, recording, frame extraction, profiling, latency, and recovery."""

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.metrics = PipelineMetrics()
        self.latency = LatencyTracker(max_samples=cfg.latency.max_samples)
        self.profiler = StreamProfiler(
            reports_dir=cfg.output.reports_dir,
            bitrate_window_s=cfg.profiling.bitrate_window_s,
            tz=cfg.output.timezone,
        )
        self.downtime = DowntimeLogger(
            logs_dir=cfg.output.logs_dir,
            camera=cfg.camera.name,
            tz=cfg.output.timezone,
        )
        self.bus = FrameBus(self.stop_event)

        self.recorder: SegmentWriter | None = None
        if cfg.recording.enabled:
            self.recorder = SegmentWriter(cfg, self.bus, self.latency, self.profiler, self.metrics)

        self.extractor: FrameExtractor | None = None
        if cfg.extraction.enabled:
            self.extractor = FrameExtractor(cfg, self.bus, self.latency, self.profiler, self.metrics)

        self.capture = CaptureWorker(
            cfg=cfg,
            bus=self.bus,
            profiler=self.profiler,
            latency=self.latency,
            downtime=self.downtime,
            metrics=self.metrics,
            stop_event=self.stop_event,
        )
        self._telemetry_csv = CsvAppender(cfg.output.reports_dir / "telemetry_timeseries.csv", TELEMETRY_FIELDS)
        self._started_ts = time.time()

    def start(self) -> None:
        if self.recorder is not None:
            self.recorder.start()
        if self.extractor is not None:
            self.extractor.start()
        self.capture.start()

    def run(self) -> dict[str, Any]:
        """Start all workers, periodically flush telemetry/reports, and shut down cleanly."""
        self.start()
        deadline = (
            time.monotonic() + self.cfg.run_duration_s
            if self.cfg.run_duration_s is not None
            else None
        )
        snap_interval = self.cfg.profiling.snapshot_interval_s
        next_snap = time.monotonic() + snap_interval

        try:
            while not self.stop_event.is_set():
                now = time.monotonic()
                timeout = snap_interval if next_snap <= now else (next_snap - now)
                if deadline is not None:
                    rem = deadline - now
                    if rem <= 0:
                        log.info("Configured run duration (%.1fs) reached; stopping", self.cfg.run_duration_s)
                        self.stop_event.set()
                        break
                    timeout = min(timeout, rem)

                self.stop_event.wait(timeout=max(0.05, timeout))
                if self.stop_event.is_set():
                    break

                if time.monotonic() >= next_snap:
                    self._flush_periodic()
                    next_snap = time.monotonic() + snap_interval
        finally:
            summary = self.shutdown()
        return summary

    def _flush_periodic(self) -> None:
        try:
            row = self.profiler.interval_row()
            iv = self.latency.drain_interval()
            row["arrival_to_decode_p50_ms"] = (iv.get("arrival_to_decode_ms") or {}).get("p50")
            row["arrival_to_decode_p95_ms"] = (iv.get("arrival_to_decode_ms") or {}).get("p95")
            row["arrival_to_record_p50_ms"] = (iv.get("arrival_to_record_ms") or {}).get("p50")
            row["arrival_to_record_p95_ms"] = (iv.get("arrival_to_record_ms") or {}).get("p95")
            row["arrival_to_frame_p50_ms"] = (iv.get("arrival_to_frame_saved_ms") or {}).get("p50")
            row["arrival_to_frame_p95_ms"] = (iv.get("arrival_to_frame_saved_ms") or {}).get("p95")
            self._telemetry_csv.append(row)
            self.profiler.write()
            atomic_write_json(self.cfg.output.reports_dir / "latency_summary.json", self.latency.summary())
        except Exception:  # noqa: BLE001
            log.exception("Failed periodic telemetry flush")

    def shutdown(self) -> dict[str, Any]:
        log.info("Shutting down pipeline...")
        self.stop_event.set()
        self.capture.join(timeout=15.0)
        self.bus.close()
        if self.recorder is not None:
            self.recorder.join(timeout=30.0)
        if self.extractor is not None:
            self.extractor.join(timeout=15.0)

        profile = self.profiler.close()
        latency_summary = self.latency.summary()
        downtime_summary = self.downtime.close(self.cfg.output.reports_dir)
        self._telemetry_csv.close()

        atomic_write_json(self.cfg.output.reports_dir / "latency_summary.json", latency_summary)

        run_summary = {
            "camera": self.cfg.camera.name,
            "started_at": iso(self._started_ts, self.cfg.output.timezone),
            "ended_at": iso(time.time(), self.cfg.output.timezone),
            "duration_s": round(time.time() - self._started_ts, 3),
            "transport_enforced": "tcp",
            "metrics": self.metrics.snapshot(),
            "bus": self.bus.stats(),
            "recorder": self.recorder.stats() if self.recorder else None,
            "extractor": self.extractor.stats() if self.extractor else None,
            "downtime": downtime_summary,
            "latency": latency_summary,
            "profile_summary": {
                "input": profile.get("input"),
                "clips": profile.get("output", {}).get("recording", {}).get("clips"),
                "frames": profile.get("output", {}).get("frames"),
            },
            "fatal_error": str(self.capture.fatal_error) if self.capture.fatal_error else None,
        }
        atomic_write_json(self.cfg.output.reports_dir / "run_summary.json", run_summary)
        log.info(
            "Pipeline stopped. Decoded=%d, Recorded=%d (dropped=%d), SavedFrames=%d, Outages=%d",
            self.metrics.get("frames_decoded"),
            self.recorder.frames_written if self.recorder else 0,
            self.recorder.frames_dropped if self.recorder else 0,
            self.extractor.saved if self.extractor else 0,
            downtime_summary.get("outages", 0),
        )
        return run_summary


def _install_signal_handlers(stop_event: threading.Event) -> None:
    def _handle(signum: int, _frame: Any) -> None:
        log.info("Received signal %s; initiating graceful shutdown", signum)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle)
        except (OSError, ValueError):
            pass


def _cmd_run(args: argparse.Namespace) -> int:
    overrides = parse_set_overrides(args.set)
    if args.duration is not None:
        overrides["run_duration_s"] = args.duration
    if args.output is not None:
        overrides.setdefault("output", {})["root_dir"] = args.output

    cfg = load_config(args.config, overrides=overrides)
    cfg.require_source()
    cfg.ensure_dirs()

    log_file = (cfg.output.root_dir / cfg.logging.file) if cfg.logging.file else None
    setup_logging(level=cfg.logging.level, log_file=log_file, json_format=cfg.logging.json_format)

    pipeline = Pipeline(cfg)
    _install_signal_handlers(pipeline.stop_event)
    summary = pipeline.run()
    return 1 if summary.get("fatal_error") else 0


def _cmd_probe(args: argparse.Namespace) -> int:
    target = args.target
    if target.lower().startswith(("rtsp://", "rtsps://")):
        cfg = load_config(args.config)
        with RtspClient(target, cfg.stream) as client:
            info = client.info.as_dict() if client.info else {}
        print(json.dumps(info, indent=2))
        return 0
    report = probe_media(target, max_seconds=args.max_seconds)
    print(json.dumps(report, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rtsp-ingestor", description="Low-latency RTSP-over-TCP ingestion service")
    sub = p.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run the RTSP ingestion pipeline")
    run_p.add_argument("-c", "--config", default="config/config.yaml", help="Path to YAML config file")
    run_p.add_argument("-d", "--duration", type=float, default=None, help="Run duration in seconds (default: unlimited)")
    run_p.add_argument("-o", "--output", default=None, help="Override output.root_dir")
    run_p.add_argument("--set", action="append", default=[], help="Override config key, e.g. --set recording.segment_seconds=60")
    run_p.set_defaults(func=_cmd_run)

    probe_p = sub.add_parser("probe", help="Probe an RTSP URL (TCP) or local media file")
    probe_p.add_argument("target", help="RTSP URL or local video file path")
    probe_p.add_argument("-c", "--config", default=None, help="Optional YAML config for stream timeouts")
    probe_p.add_argument("--max-seconds", type=float, default=None, help="Max probe time for local file demux")
    probe_p.set_defaults(func=_cmd_probe)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except IngestorError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

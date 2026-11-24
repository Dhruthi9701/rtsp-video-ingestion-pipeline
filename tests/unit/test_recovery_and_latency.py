"""Unit tests for exponential backoff, downtime logger, and latency tracker."""

from __future__ import annotations

from pathlib import Path

from rtsp_ingestor.monitoring.latency import LatencyTracker, SourceClock
from rtsp_ingestor.recovery.downtime_logger import DowntimeLogger
from rtsp_ingestor.recovery.reconnect import BackoffPolicy


def test_backoff_exponential_and_exhaustion() -> None:
    p = BackoffPolicy(initial_delay_s=1.0, max_delay_s=4.0, multiplier=2.0, jitter=0.0, max_attempts=4)
    assert [p.next_delay() for _ in range(4)] == [1.0, 2.0, 4.0, 4.0]
    assert p.next_delay() is None
    p.reset()
    assert p.next_delay() == 1.0


def test_downtime_logger_records_outage_lifecycle(tmp_path: Path) -> None:
    dl = DowntimeLogger(tmp_path / "logs", camera="cam01")
    dl.mark_disconnected("read_timeout", "timed out", session=1, ts=1700000100.0)
    dl.record_attempt(1, False, "connection_refused", "refused", next_delay_s=1.0)
    dl.record_attempt(2, True)
    downtime = dl.mark_recovered(session=2, ts=1700000103.5)
    assert downtime == 3.5

    summary = dl.close(tmp_path / "reports")
    assert summary["outages"] == 1
    assert summary["total_downtime_s"] == 3.5
    assert (tmp_path / "logs" / "downtime_events.jsonl").is_file()
    assert (tmp_path / "logs" / "downtime_outages.csv").is_file()
    assert (tmp_path / "reports" / "downtime_summary.json").is_file()


def test_source_clock_and_latency_summary() -> None:
    clk = SourceClock(window_s=10.0)
    # constant offset 1000.0 + jitter (0ms, 5ms, 2ms)
    _, j0 = clk.update(0.0, 1000.000)
    _, j1 = clk.update(0.1, 1000.105)
    _, j2 = clk.update(0.2, 1000.202)
    assert abs(j0 - 0.0) < 1e-6
    assert abs(j1 - 0.005) < 1e-6
    assert abs(j2 - 0.002) < 1e-6

    tracker = LatencyTracker()
    for v in (10.0, 20.0, 30.0, 40.0, 100.0):
        tracker.record("arrival_to_decode_ms", v)
    s = tracker.summary()["metrics"]["arrival_to_decode_ms"]
    assert s["count"] == 5
    assert s["p50"] == 30.0
    assert s["max"] == 100.0

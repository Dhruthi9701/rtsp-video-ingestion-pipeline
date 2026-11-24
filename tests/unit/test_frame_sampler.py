"""Unit tests for 5 FPS gate and filename formatting."""

from __future__ import annotations

import re

from rtsp_ingestor.extraction.frame_sampler import FpsGate, frame_filename


def _simulate_fps(source_fps: float, duration_s: float, target_fps: float = 5.0) -> list[float]:
    gate = FpsGate(target_fps)
    dt = 1.0 / source_fps
    selected: list[float] = []
    n = int(round(duration_s * source_fps))
    for i in range(n):
        t = i * dt
        if gate.offer(1, t):
            selected.append(t)
    return selected


def test_gate_produces_5fps_across_source_rates() -> None:
    for src_fps in (15.0, 25.0, 30.0, 60.0):
        sel = _simulate_fps(src_fps, 10.0, target_fps=5.0)
        assert len(sel) == 50, f"expected 50 frames at {src_fps} fps, got {len(sel)}"


def test_gate_handles_non_integer_ratio_and_slow_source() -> None:
    sel_7 = _simulate_fps(7.0, 10.0, target_fps=5.0)
    assert 48 <= len(sel_7) <= 51
    sel_3 = _simulate_fps(3.0, 10.0, target_fps=5.0)
    assert len(sel_3) == 30  # never drops below source rate


def test_frame_filename_matches_spec() -> None:
    name = frame_filename("cam01", 1700000000.123, tz="utc")
    assert re.match(r"^cam01_\d{8}_\d{6}_\d{3}\.jpg$", name), name

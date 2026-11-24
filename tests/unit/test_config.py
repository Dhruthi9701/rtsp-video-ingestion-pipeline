"""Unit tests for configuration loading, validation, and TCP enforcement."""

from __future__ import annotations

from pathlib import Path

import pytest

from rtsp_ingestor.core.config import load_config, parse_set_overrides
from rtsp_ingestor.core.exceptions import ConfigError, TransportError


def test_default_config_enforces_tcp() -> None:
    cfg = load_config(use_env=False, dotenv_path=None)
    assert cfg.stream.transport == "tcp"
    assert cfg.recording.segment_seconds == 300.0
    assert cfg.extraction.target_fps == 5.0


def test_non_tcp_transport_is_rejected(tmp_path: Path) -> None:
    cfg_file = tmp_path / "bad.yaml"
    cfg_file.write_text("stream:\n  transport: udp\n", encoding="utf-8")
    with pytest.raises(TransportError):
        load_config(cfg_file, use_env=False, dotenv_path=None)


def test_odd_output_dimensions_rejected(tmp_path: Path) -> None:
    cfg_file = tmp_path / "odd.yaml"
    cfg_file.write_text("recording:\n  width: 641\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(cfg_file, use_env=False, dotenv_path=None)


def test_precedence_yaml_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text("camera:\n  name: yamlcam\nrecording:\n  segment_seconds: 120\n", encoding="utf-8")
    monkeypatch.setenv("CAMERA_NAME", "envcam")
    monkeypatch.setenv("RTSP_INGESTOR__RECORDING__WIDTH", "320")
    overrides = parse_set_overrides(["recording.segment_seconds=60"])
    cfg = load_config(cfg_file, overrides=overrides, use_env=True, dotenv_path=None)
    assert cfg.camera.name == "envcam"
    assert cfg.recording.width == 320
    assert cfg.recording.segment_seconds == 60

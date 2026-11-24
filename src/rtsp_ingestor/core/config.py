"""Typed, validated configuration.

Sources, lowest to highest precedence:

1. model defaults
2. YAML file (``config/config.yaml``)
3. environment variables / ``.env`` (friendly names such as ``RTSP_URL`` or the
   generic ``RTSP_INGESTOR__SECTION__KEY`` form)
4. explicit overrides (CLI ``--set section.key=value``)
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .exceptions import ConfigError, TransportError

ENV_PREFIX = "RTSP_INGESTOR__"

#: Friendly environment variable names -> dotted config path.
ENV_ALIASES: dict[str, str] = {
    "RTSP_URL": "camera.rtsp_url",
    "CAMERA_NAME": "camera.name",
    "OUTPUT_WIDTH": "recording.width",
    "OUTPUT_HEIGHT": "recording.height",
    "SEGMENT_SECONDS": "recording.segment_seconds",
    "TARGET_FPS": "extraction.target_fps",
    "OUTPUT_DIR": "output.root_dir",
    "LOG_LEVEL": "logging.level",
}

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class CameraConfig(_Section):
    name: str = "cam01"
    rtsp_url: str = ""

    @field_validator("name")
    @classmethod
    def _valid_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError("camera.name must be 1-64 chars of [A-Za-z0-9_-] (it is used in filenames)")
        return v

    @field_validator("rtsp_url")
    @classmethod
    def _valid_url(cls, v: str) -> str:
        v = v.strip()
        if v and not v.lower().startswith(("rtsp://", "rtsps://")):
            raise ValueError("camera.rtsp_url must start with rtsp:// or rtsps://")
        return v


class StreamConfig(_Section):
    transport: str = "tcp"
    open_timeout_s: float = Field(10.0, gt=0)
    read_timeout_s: float = Field(5.0, gt=0)
    stall_timeout_s: float = Field(8.0, gt=0)
    low_latency: bool = True
    max_delay_ms: int = Field(500, ge=0)
    socket_buffer_bytes: int = Field(1_048_576, gt=0)
    video_only: bool = True

    @field_validator("transport")
    @classmethod
    def _tcp_only(cls, v: str) -> str:
        if v.strip().lower() != "tcp":
            raise TransportError(
                f"stream.transport={v!r} rejected: RTSP over TCP is mandatory "
                "(UDP causes packet-loss artifacts)"
            )
        return "tcp"


class RecordingConfig(_Section):
    enabled: bool = True
    segment_seconds: float = Field(300.0, gt=0)
    width: int = Field(640, gt=0)
    height: int = Field(360, gt=0)
    keep_aspect: bool = False
    codec: str = "libx264"
    crf: int = Field(23, ge=0, le=51)
    preset: str = "veryfast"
    tune: str | None = "zerolatency"
    gop_seconds: float = Field(2.0, gt=0)
    container: Literal["mp4", "mkv"] = "mp4"
    queue_size: int = Field(1500, gt=0)
    align_to_wallclock: bool = False

    @field_validator("width", "height")
    @classmethod
    def _even(cls, v: int) -> int:
        if v % 2:
            raise ValueError("output width/height must be even (yuv420p chroma subsampling)")
        return v


class ExtractionConfig(_Section):
    enabled: bool = True
    target_fps: float = Field(5.0, gt=0, le=120)
    jpeg_quality: int = Field(90, ge=1, le=100)
    use_downscaled: bool = True
    partition_by_hour: bool = True
    queue_size: int = Field(256, gt=0)


class ReconnectConfig(_Section):
    initial_delay_s: float = Field(1.0, gt=0)
    max_delay_s: float = Field(30.0, gt=0)
    multiplier: float = Field(2.0, ge=1.0)
    jitter: float = Field(0.2, ge=0.0, le=1.0)
    max_attempts: int = Field(0, ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> ReconnectConfig:
        if self.max_delay_s < self.initial_delay_s:
            raise ValueError("reconnect.max_delay_s must be >= reconnect.initial_delay_s")
        return self


class ProfilingConfig(_Section):
    snapshot_interval_s: float = Field(30.0, gt=0)
    bitrate_window_s: float = Field(10.0, gt=0)


class LatencyConfig(_Section):
    warmup_s: float = Field(3.0, ge=0)
    clock_window_s: float = Field(60.0, gt=0)
    max_samples: int = Field(200_000, gt=100)
    summary_interval_s: float = Field(30.0, gt=0)


class OutputConfig(_Section):
    root_dir: Path = Path("output")
    timezone: Literal["local", "utc"] = "local"

    @property
    def segments_dir(self) -> Path:
        return self.root_dir / "segments"

    @property
    def frames_dir(self) -> Path:
        return self.root_dir / "frames"

    @property
    def reports_dir(self) -> Path:
        return self.root_dir / "reports"

    @property
    def logs_dir(self) -> Path:
        return self.root_dir / "logs"


class LoggingConfig(_Section):
    level: str = "INFO"
    file: str | None = "logs/ingestor.log"
    json_format: bool = Field(False, alias="json")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @field_validator("level")
    @classmethod
    def _level(cls, v: str) -> str:
        v = v.upper()
        if v not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"invalid log level {v!r}")
        return v


class AppConfig(_Section):
    camera: CameraConfig = Field(default_factory=CameraConfig)
    stream: StreamConfig = Field(default_factory=StreamConfig)
    recording: RecordingConfig = Field(default_factory=RecordingConfig)
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)
    reconnect: ReconnectConfig = Field(default_factory=ReconnectConfig)
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    latency: LatencyConfig = Field(default_factory=LatencyConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    run_duration_s: float | None = Field(None, gt=0)

    def require_source(self) -> None:
        if not self.camera.rtsp_url:
            raise ConfigError(
                "camera.rtsp_url is empty - set RTSP_URL in your environment/.env "
                "or camera.rtsp_url in the YAML config"
            )

    def ensure_dirs(self) -> None:
        for d in (
            self.output.root_dir,
            self.output.segments_dir,
            self.output.frames_dir,
            self.output.reports_dir,
            self.output.logs_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- loading


def _set_dotted(data: dict[str, Any], dotted: str, value: Any) -> None:
    parts = [p for p in dotted.split(".") if p]
    if not parts:
        raise ConfigError(f"invalid config key {dotted!r}")
    node = data
    for part in parts[:-1]:
        nxt = node.setdefault(part, {})
        if not isinstance(nxt, dict):
            raise ConfigError(f"cannot set {dotted!r}: {part!r} is not a section")
        node = nxt
    node[parts[-1]] = value


def _parse_scalar(raw: str) -> Any:
    """Parse an env/CLI string with YAML semantics (``"5"`` -> 5, ``"true"`` -> True)."""
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError:
        return raw


def env_overrides(environ: dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ if environ is None else environ
    out: dict[str, Any] = {}
    for name, path in ENV_ALIASES.items():
        if env.get(name):
            value = env[name] if path == "camera.rtsp_url" else _parse_scalar(env[name])
            _set_dotted(out, path, value)
    for name, raw in env.items():
        if name.upper().startswith(ENV_PREFIX):
            path = name[len(ENV_PREFIX):].lower().replace("__", ".")
            _set_dotted(out, path, _parse_scalar(raw))
    return out


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def parse_set_overrides(items: list[str] | None) -> dict[str, Any]:
    """Turn ``["recording.segment_seconds=60", ...]`` into a nested dict."""
    out: dict[str, Any] = {}
    for item in items or []:
        if "=" not in item:
            raise ConfigError(f"--set expects key=value, got {item!r}")
        key, raw = item.split("=", 1)
        _set_dotted(out, key.strip(), _parse_scalar(raw.strip()))
    return out


def load_config(
    path: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
    *,
    use_env: bool = True,
    dotenv_path: str | Path | None = ".env",
) -> AppConfig:
    data: dict[str, Any] = {}
    if path is not None:
        p = Path(path)
        if not p.is_file():
            raise ConfigError(f"config file not found: {p}")
        try:
            loaded = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"invalid YAML in {p}: {exc}") from exc
        if not isinstance(loaded, dict):
            raise ConfigError(f"{p} must contain a mapping at the top level")
        data = loaded

    if use_env:
        if dotenv_path:
            try:
                from dotenv import load_dotenv

                load_dotenv(dotenv_path, override=False)
            except ImportError:  # pragma: no cover - optional dependency
                pass
        data = _deep_merge(data, env_overrides())

    if overrides:
        data = _deep_merge(data, overrides)

    try:
        return AppConfig.model_validate(data)
    except TransportError:
        raise
    except ValidationError as exc:
        # Surface TransportError raised inside a validator as-is.
        for err in exc.errors():
            ctx_err = (err.get("ctx") or {}).get("error")
            if isinstance(ctx_err, TransportError):
                raise ctx_err from exc
        raise ConfigError(f"invalid configuration:\n{exc}") from exc

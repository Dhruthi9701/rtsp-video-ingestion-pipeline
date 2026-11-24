"""Time formatting, URL masking and small atomic-file helpers."""

from __future__ import annotations

import csv
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

_CRED_RE = re.compile(r"(?P<scheme>[a-z]+://)(?P<user>[^:@/]+):(?P<pw>[^@/]*)@", re.IGNORECASE)


def to_datetime(ts: float, tz: str = "local") -> datetime:
    if tz == "utc":
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    return datetime.fromtimestamp(ts).astimezone()


def frame_stamp(ts: float, tz: str = "local") -> str:
    """``YYYYMMDD_HHMMSS_mmm`` - the timestamp format used in frame filenames."""
    dt = to_datetime(ts, tz)
    return dt.strftime("%Y%m%d_%H%M%S_") + f"{dt.microsecond // 1000:03d}"


def file_stamp(ts: float, tz: str = "local") -> str:
    return to_datetime(ts, tz).strftime("%Y%m%d_%H%M%S")


def iso(ts: float | None, tz: str = "local") -> str | None:
    if ts is None:
        return None
    return to_datetime(ts, tz).isoformat(timespec="milliseconds")


def mask_url(url: str) -> str:
    """Hide the password in an RTSP URL so it never reaches logs or reports."""
    try:
        parts = urlsplit(url)
        if parts.password is not None:
            host = parts.hostname or ""
            if parts.port:
                host = f"{host}:{parts.port}"
            netloc = f"{parts.username}:***@{host}"
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except ValueError:
        pass
    return _CRED_RE.sub(lambda m: f"{m['scheme']}{m['user']}:***@", url)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, indent=2, default=str, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        fieldnames = []
        for r in rows:
            for k in r:
                if k not in fieldnames:
                    fieldnames.append(k)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


class CsvAppender:
    """Append rows to a CSV, writing the header only when the file is new."""

    def __init__(self, path: Path, fieldnames: list[str]) -> None:
        self.path = path
        self.fieldnames = fieldnames
        path.parent.mkdir(parents=True, exist_ok=True)
        new = not path.exists() or path.stat().st_size == 0
        self._fh = open(path, "a", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._fh, fieldnames=fieldnames, extrasaction="ignore")
        if new:
            self._writer.writeheader()
            self._fh.flush()

    def append(self, row: dict[str, Any]) -> None:
        self._writer.writerow(row)
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


def flatten(d: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        elif isinstance(v, (list, tuple)):
            out[key] = json.dumps(v, default=str)
        else:
            out[key] = v
    return out

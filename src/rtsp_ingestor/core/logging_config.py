"""Logging setup: human-readable console + rotating file (optionally JSON lines)."""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
import time
from pathlib import Path

_CONSOLE_FMT = "%(asctime)s.%(msecs)03d %(levelname)-7s [%(threadName)s] %(name)s: %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created))
            + f".{int(record.msecs):03d}",
            "level": record.levelname,
            "logger": record.name,
            "thread": record.threadName,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO", log_file: str | Path | None = None, json_format: bool = False) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_CONSOLE_FMT, _DATE_FMT))
    root.addHandler(console)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(path, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.setFormatter(JsonFormatter() if json_format else logging.Formatter(_CONSOLE_FMT, _DATE_FMT))
        root.addHandler(fh)

    # FFmpeg's own logging is noisy; keep PyAV quiet unless debugging.
    logging.getLogger("libav").setLevel(logging.ERROR if level != "DEBUG" else logging.WARNING)

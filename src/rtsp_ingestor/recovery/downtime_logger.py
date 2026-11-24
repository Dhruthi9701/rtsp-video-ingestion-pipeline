"""Persistent downtime / reconnection log.

Two files are written under ``<output>/logs`` (append-only, flushed + fsynced
per event so they survive crashes):

* ``downtime_events.jsonl`` - one JSON object per event: ``disconnect``,
  ``reconnect_attempt`` (success/failure + reason + next backoff delay),
  ``reconnected`` (with total downtime), ``shutdown_during_outage``.
* ``downtime_outages.csv``  - one row per outage: start, end, reason,
  attempts, downtime_s.

A summary (outage count, total/longest downtime, availability) is exported to
``reports/downtime_summary.json`` at shutdown.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..core.utils import CsvAppender, atomic_write_json, iso

log = logging.getLogger(__name__)

OUTAGE_FIELDS = [
    "outage_id", "camera", "disconnect_time", "reconnect_time", "reason",
    "last_error", "attempts", "downtime_s", "session_before", "session_after",
]


@dataclass
class Outage:
    outage_id: int
    start_ts: float
    reason: str
    session_before: int | None
    attempts: int = 0
    last_error: str | None = None
    end_ts: float | None = None
    session_after: int | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def downtime_s(self) -> float:
        return (self.end_ts or time.time()) - self.start_ts


class DowntimeLogger:
    def __init__(self, logs_dir: Path, camera: str, tz: str = "local") -> None:
        self.camera = camera
        self.tz = tz
        logs_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = logs_dir / "downtime_events.jsonl"
        self._events = open(self.events_path, "a", encoding="utf-8")
        self._csv = CsvAppender(logs_dir / "downtime_outages.csv", OUTAGE_FIELDS)
        self._lock = threading.Lock()
        self._current: Outage | None = None
        self._history: list[Outage] = []
        self._next_id = 1
        self.started_ts = time.time()

    # ------------------------------------------------------------------ events
    def _emit(self, event: str, ts: float, **fields: Any) -> None:
        rec = {"event": event, "camera": self.camera, "time": iso(ts, self.tz), "ts": round(ts, 3), **fields}
        self._events.write(json.dumps(rec, default=str) + "\n")
        self._events.flush()
        try:
            os.fsync(self._events.fileno())
        except OSError:
            pass

    @property
    def in_outage(self) -> bool:
        with self._lock:
            return self._current is not None

    def mark_disconnected(self, reason: str, error: str | None = None, session: int | None = None,
                          ts: float | None = None) -> None:
        ts = ts or time.time()
        with self._lock:
            if self._current is not None:
                return
            outage = Outage(self._next_id, ts, reason, session, last_error=error)
            self._current = outage
            self._next_id += 1
            self._emit("disconnect", ts, outage_id=outage.outage_id, reason=reason,
                       error=error, session=session)
        log.warning("DISCONNECT outage#%d reason=%s error=%s", outage.outage_id, reason, error)

    def record_attempt(self, attempt: int, success: bool, reason: str | None = None,
                       error: str | None = None, next_delay_s: float | None = None) -> None:
        ts = time.time()
        with self._lock:
            if self._current is None:
                return
            self._current.attempts = attempt
            if not success:
                self._current.last_error = error or reason
                if len(self._current.errors) < 50:
                    self._current.errors.append(reason or "unknown")
            self._emit(
                "reconnect_attempt", ts, outage_id=self._current.outage_id, attempt=attempt,
                success=success, reason=reason, error=error,
                next_delay_s=round(next_delay_s, 3) if next_delay_s is not None else None,
                elapsed_s=round(ts - self._current.start_ts, 3),
            )

    def mark_recovered(self, session: int, ts: float | None = None) -> float | None:
        ts = ts or time.time()
        with self._lock:
            cur = self._current
            if cur is None:
                return None
            cur.end_ts = ts
            cur.session_after = session
            self._emit("reconnected", ts, outage_id=cur.outage_id, attempts=cur.attempts,
                       downtime_s=round(cur.downtime_s, 3), session=session,
                       disconnect_time=iso(cur.start_ts, self.tz), reason=cur.reason)
            self._write_row(cur)
            self._history.append(cur)
            self._current = None
        log.warning("RECOVERED outage#%d after %.2fs (%d attempt(s))", cur.outage_id, cur.downtime_s, cur.attempts)
        return cur.downtime_s

    def _write_row(self, o: Outage) -> None:
        self._csv.append({
            "outage_id": o.outage_id,
            "camera": self.camera,
            "disconnect_time": iso(o.start_ts, self.tz),
            "reconnect_time": iso(o.end_ts, self.tz) if o.end_ts else "",
            "reason": o.reason,
            "last_error": o.last_error or "",
            "attempts": o.attempts,
            "downtime_s": round(o.downtime_s, 3),
            "session_before": o.session_before if o.session_before is not None else "",
            "session_after": o.session_after if o.session_after is not None else "",
        })

    # ------------------------------------------------------------------ summary
    def summary(self) -> dict[str, Any]:
        with self._lock:
            outages = list(self._history) + ([self._current] if self._current else [])
            now = time.time()
            total = sum(o.downtime_s for o in outages)
            span = max(now - self.started_ts, 1e-9)
            return {
                "camera": self.camera,
                "observation_start": iso(self.started_ts, self.tz),
                "observation_end": iso(now, self.tz),
                "observation_s": round(span, 3),
                "outages": len(outages),
                "ongoing_outage": self._current is not None,
                "total_downtime_s": round(total, 3),
                "longest_outage_s": round(max((o.downtime_s for o in outages), default=0.0), 3),
                "availability_pct": round(100.0 * (1 - total / span), 3),
                "total_reconnect_attempts": sum(o.attempts for o in outages),
                "details": [
                    {
                        "outage_id": o.outage_id,
                        "disconnect_time": iso(o.start_ts, self.tz),
                        "reconnect_time": iso(o.end_ts, self.tz),
                        "reason": o.reason,
                        "attempts": o.attempts,
                        "downtime_s": round(o.downtime_s, 3),
                        "attempt_errors": o.errors,
                    }
                    for o in outages
                ],
            }

    def close(self, reports_dir: Path | None = None) -> dict[str, Any]:
        with self._lock:
            cur = self._current
            if cur is not None:
                self._emit("shutdown_during_outage", time.time(), outage_id=cur.outage_id,
                           attempts=cur.attempts, downtime_s=round(cur.downtime_s, 3), reason=cur.reason)
                self._write_row(cur)
        summary = self.summary()
        if reports_dir is not None:
            atomic_write_json(reports_dir / "downtime_summary.json", summary)
        self._events.close()
        self._csv.close()
        return summary

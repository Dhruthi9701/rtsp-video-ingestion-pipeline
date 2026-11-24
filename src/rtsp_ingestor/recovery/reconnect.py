"""Exponential backoff with jitter for RTSP reconnection."""

from __future__ import annotations

import random
from collections.abc import Callable

from ..core.config import ReconnectConfig


class BackoffPolicy:
    """``delay_n = min(max_delay, initial * multiplier**n) * (1 +/- jitter)``.

    ``max_attempts == 0`` means retry forever. Call :meth:`reset` after a
    successful connection.
    """

    def __init__(
        self,
        initial_delay_s: float = 1.0,
        max_delay_s: float = 30.0,
        multiplier: float = 2.0,
        jitter: float = 0.2,
        max_attempts: int = 0,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.initial = initial_delay_s
        self.max_delay = max_delay_s
        self.multiplier = multiplier
        self.jitter = jitter
        self.max_attempts = max_attempts
        self._rng = rng
        self.attempt = 0

    @classmethod
    def from_config(cls, cfg: ReconnectConfig) -> BackoffPolicy:
        return cls(cfg.initial_delay_s, cfg.max_delay_s, cfg.multiplier, cfg.jitter, cfg.max_attempts)

    @property
    def exhausted(self) -> bool:
        return bool(self.max_attempts) and self.attempt >= self.max_attempts

    def base_delay(self, attempt: int) -> float:
        return min(self.max_delay, self.initial * (self.multiplier ** attempt))

    def next_delay(self) -> float | None:
        """Delay before the next attempt, or ``None`` if attempts are exhausted."""
        if self.exhausted:
            return None
        base = self.base_delay(self.attempt)
        self.attempt += 1
        if self.jitter:
            base *= 1.0 + self.jitter * (2.0 * self._rng() - 1.0)
        return max(0.0, min(base, self.max_delay * (1.0 + self.jitter)))

    def reset(self) -> None:
        self.attempt = 0

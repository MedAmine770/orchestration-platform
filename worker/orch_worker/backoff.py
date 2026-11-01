"""Reconnect delays: exponential backoff with full jitter."""

from __future__ import annotations

import random
from typing import Callable


class Backoff:
    """delay = uniform(0, min(cap, base * 2**attempt)).

    Full jitter spreads reconnects out, so when a coordinator restarts its
    workers don't all reconnect in the same instant (thundering herd).
    """

    def __init__(self, base: float = 0.5, cap: float = 30.0,
                 rng: Callable[[], float] = random.random):
        self.base = base
        self.cap = cap
        self._rng = rng
        self._attempt = 0

    def next(self) -> float:
        ceiling = min(self.cap, self.base * 2 ** min(self._attempt, 32))
        self._attempt += 1
        return self._rng() * ceiling

    def reset(self) -> None:
        """Called after a successful handshake."""
        self._attempt = 0

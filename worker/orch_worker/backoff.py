"""Reconnect delays: exponential backoff with full jitter."""

from __future__ import annotations

import random
from typing import Callable

# Below the coordinator's default recovery window (--recovery-window-ms, 10 s;
# DESIGN.md §8.3). Once a restarted coordinator is back, a worker's next attempt
# is at most this far away, so it can reclaim its jobs instead of having them
# rerun. A deployment that shortens the window must keep it above this.
DEFAULT_CAP_S = 8.0


class Backoff:
    """delay = uniform(0, min(cap, base * 2**attempt)).

    Full jitter spreads reconnects out, so when a coordinator restarts its
    workers don't all reconnect in the same instant (thundering herd).
    """

    def __init__(self, base: float = 0.5, cap: float = DEFAULT_CAP_S,
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

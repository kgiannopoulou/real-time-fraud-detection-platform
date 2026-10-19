"""Fault injection for the live stream: the imperfections real pipelines see.

* **late / out-of-order events** — a share of events is held back and
  published after newer ones (mobile terminals reconnecting, batching
  gateways). Their ``timestamp`` (event time) stays the original one;
* **very late events** — held back longer than the streaming job's allowed
  lateness, to exercise the late-event side output.
"""

from __future__ import annotations

import heapq
import itertools
import random
from dataclasses import dataclass


@dataclass
class ChaosConfig:
    late_rate: float = 0.0
    max_late_seconds: float = 60.0
    very_late_rate: float = 0.0
    very_late_seconds: tuple[float, float] = (180.0, 300.0)

    @property
    def enabled(self) -> bool:
        return self.late_rate > 0 or self.very_late_rate > 0


class ChaosInjector:
    """Holds back a share of events and releases them later (in wall-clock time)."""

    def __init__(self, cfg: ChaosConfig, seed: int | None = None):
        self.cfg = cfg
        self.rng = random.Random(seed)
        self._held: list[tuple[float, int, dict]] = []
        self._seq = itertools.count()
        self.stats = {"late": 0, "very_late": 0}

    def process(self, event: dict, now: float) -> list[dict]:
        """Events to publish now: released held-back events, then ``event`` unless delayed."""
        out = self.release(now)
        r = self.rng.random()
        if r < self.cfg.very_late_rate:
            delay = self.rng.uniform(*self.cfg.very_late_seconds)
            self.stats["very_late"] += 1
        elif r < self.cfg.very_late_rate + self.cfg.late_rate:
            delay = self.rng.uniform(2.0, self.cfg.max_late_seconds)
            self.stats["late"] += 1
        else:
            out.append(event)
            return out
        heapq.heappush(self._held, (now + delay, next(self._seq), event))
        return out

    def release(self, now: float) -> list[dict]:
        out = []
        while self._held and self._held[0][0] <= now:
            out.append(heapq.heappop(self._held)[2])
        return out

    def drain(self) -> list[dict]:
        """Everything still held (on shutdown)."""
        out = [item[2] for item in sorted(self._held)]
        self._held.clear()
        return out

"""Fault injection for the live stream: the imperfections real pipelines see.

* **late / out-of-order events** — a share of events is held back and
  published after newer ones (mobile terminals reconnecting, batching
  gateways). Their ``timestamp`` (event time) stays the original one;
* **very late events** — held back longer than the streaming job's allowed
  lateness, to exercise the late-event side output;
* **duplicates** — the same event published twice (an at-least-once producer
  retrying after a timeout);
* **invalid records** — malformed JSON, missing ids, negative or non-numeric
  amounts, unknown currencies/countries, timestamps in the future. They must
  end up in the dead-letter queue without disturbing the stream.
"""

from __future__ import annotations

import heapq
import itertools
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

CORRUPTIONS = (
    "malformed_json",
    "missing_user_id",
    "negative_amount",
    "non_numeric_amount",
    "unsupported_currency",
    "invalid_country",
    "future_timestamp",
)


def corrupt(event: dict, kind: str) -> dict | str:
    """A deliberately invalid version of ``event`` (dict or raw string)."""
    bad = dict(event)
    if kind == "malformed_json":
        return json.dumps(event)[: len(json.dumps(event)) // 2]
    if kind == "missing_user_id":
        bad.pop("user_id", None)
    elif kind == "negative_amount":
        bad["amount"] = -abs(bad["amount"])
    elif kind == "non_numeric_amount":
        bad["amount"] = f"{bad['amount']:.2f}".replace(".", ",")
    elif kind == "unsupported_currency":
        bad["currency"] = "XYZ"
    elif kind == "invalid_country":
        bad["country"] = "Greece"
    elif kind == "future_timestamp":
        bad["timestamp"] = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return bad


@dataclass
class ChaosConfig:
    late_rate: float = 0.0
    max_late_seconds: float = 60.0
    very_late_rate: float = 0.0
    very_late_seconds: tuple[float, float] = (180.0, 300.0)
    duplicate_rate: float = 0.0
    invalid_rate: float = 0.0


class ChaosInjector:
    """Holds back a share of events and releases them later (in wall-clock time)."""

    def __init__(self, cfg: ChaosConfig, seed: int | None = None):
        self.cfg = cfg
        self.rng = random.Random(seed)
        self._held: list[tuple[float, int, dict]] = []
        self._seq = itertools.count()
        self.stats = {"late": 0, "very_late": 0, "duplicate": 0, "invalid": 0}

    def process(self, event: dict, now: float) -> list[dict | str]:
        """Events to publish now: released held-back events, then ``event`` unless delayed."""
        out = self.release(now)
        if self.rng.random() < self.cfg.invalid_rate:
            self.stats["invalid"] += 1
            out.append(corrupt(event, self.rng.choice(CORRUPTIONS)))
            return out
        if self.rng.random() < self.cfg.duplicate_rate:  # re-sent a few seconds later
            self.stats["duplicate"] += 1
            heapq.heappush(self._held, (now + self.rng.uniform(1, 10), next(self._seq), dict(event)))
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

    def release(self, now: float) -> list[dict | str]:
        out = []
        while self._held and self._held[0][0] <= now:
            out.append(heapq.heappop(self._held)[2])
        return out

    def drain(self) -> list[dict | str]:
        """Everything still held (on shutdown)."""
        out = [item[2] for item in sorted(self._held)]
        self._held.clear()
        return out

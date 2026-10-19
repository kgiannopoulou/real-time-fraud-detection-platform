"""Behavioural fraud features computed from a card holder's own history.

One implementation serves both worlds:

* **offline** — ``ml/feature_pipeline.py`` replays the labelled history
  through :class:`UserState` to build the training set;
* **streaming** — the Spark job keeps one serialized :class:`UserState` per
  user in its state store (``applyInPandasWithState``) and calls the same
  :meth:`UserState.observe` for every arriving transaction.

Sharing the code removes train/serve skew by construction: a feature can't be
defined one way in a notebook and another way in production.

Every window is evaluated in **event time** relative to the transaction being
scored and only looks at *earlier* events, so a late, out-of-order event gets
the same features it would have had if it had arrived on time (as long as its
history is still retained).
"""

from __future__ import annotations

import json
import math
from bisect import bisect_left, insort
from dataclasses import dataclass, field

DAY = 86_400.0
HISTORY_SECONDS = DAY  # behavioural windows look back at most 24 h
RETENTION_SLACK = 1_800.0  # keep 30 min more so late events still see their history
KNOWN_TTL = 30 * DAY  # countries/devices count as "known" for 30 days
MAX_EVENTS = 1_000  # hard cap on per-user state size
MAX_KNOWN = 50

BEHAVIOURAL_FEATURES = [
    "tx_count_2m",
    "tx_count_5m",
    "tx_count_1h",
    "tx_count_24h",
    "avg_amount_24h",
    "amount_ratio",
    "unique_merchants_1h",
    "unique_countries_24h",
    "is_new_country",
    "is_new_device",
    "seconds_since_last_tx",
]


@dataclass
class UserState:
    """Rolling history of one card holder, sorted by event time."""

    ts: list[float] = field(default_factory=list)
    amounts: list[float] = field(default_factory=list)
    merchants: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    tx_ids: list[str] = field(default_factory=list)
    known_countries: dict[str, float] = field(default_factory=dict)
    known_devices: dict[str, float] = field(default_factory=dict)
    max_ts: float = 0.0

    # -- serialization (Spark state store) ---------------------------------- #

    def to_json(self) -> str:
        return json.dumps(
            [
                self.ts,
                self.amounts,
                self.merchants,
                self.countries,
                self.tx_ids,
                self.known_countries,
                self.known_devices,
                self.max_ts,
            ],
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, raw: str | None) -> UserState:
        if not raw:
            return cls()
        ts, amounts, merchants, countries, tx_ids, kc, kd, max_ts = json.loads(raw)
        return cls(ts, amounts, merchants, countries, tx_ids, kc, kd, max_ts)

    # -- core --------------------------------------------------------------- #

    def has_seen(self, tx_id: str) -> bool:
        """Duplicate detection within the retained window (Kafka is at-least-once)."""
        return tx_id in self.tx_ids

    def _count_since(self, hi: int, t: float, window: float) -> int:
        return hi - bisect_left(self.ts, t - window, 0, hi)

    def features(self, t: float, amount: float, country: str, device: str | None) -> dict[str, float]:
        """Features of a transaction at event time ``t`` from strictly earlier history."""
        hi = bisect_left(self.ts, t)  # events before t
        lo_24h = bisect_left(self.ts, t - HISTORY_SECONDS, 0, hi)
        lo_1h = bisect_left(self.ts, t - 3_600, lo_24h, hi)
        n_24h = hi - lo_24h
        avg = sum(self.amounts[lo_24h:hi]) / n_24h if n_24h else math.nan
        return {
            "tx_count_2m": float(self._count_since(hi, t, 120)),
            "tx_count_5m": float(self._count_since(hi, t, 300)),
            "tx_count_1h": float(hi - lo_1h),
            "tx_count_24h": float(n_24h),
            "avg_amount_24h": avg,
            "amount_ratio": amount / avg if n_24h and avg > 0 else math.nan,
            # including the current transaction
            "unique_merchants_1h": float(len(set(self.merchants[lo_1h:hi])) + 1),
            "unique_countries_24h": float(len(set(self.countries[lo_24h:hi]) | {country})),
            "is_new_country": float(not self._known(self.known_countries, country, t)),
            "is_new_device": float(not self._known(self.known_devices, device, t)),
            "seconds_since_last_tx": t - self.ts[hi - 1] if hi else math.nan,
        }

    @staticmethod
    def _known(known: dict[str, float], key: str | None, t: float) -> bool:
        if key is None:
            return True  # missing device id is not evidence of a new device
        last_seen = known.get(key)
        return last_seen is not None and t - last_seen <= KNOWN_TTL

    def add(
        self, t: float, tx_id: str, amount: float, merchant: str, country: str, device: str | None
    ) -> None:
        i = bisect_left(self.ts, t)
        if i == len(self.ts):  # common case: in-order event
            self.ts.append(t)
            self.amounts.append(amount)
            self.merchants.append(merchant)
            self.countries.append(country)
            self.tx_ids.append(tx_id)
        else:
            insort(self.ts, t)
            for column, value in (
                (self.amounts, amount),
                (self.merchants, merchant),
                (self.countries, country),
                (self.tx_ids, tx_id),
            ):
                column.insert(i, value)
        self.known_countries[country] = max(t, self.known_countries.get(country, 0.0))
        if device is not None:
            self.known_devices[device] = max(t, self.known_devices.get(device, 0.0))
        self.max_ts = max(self.max_ts, t)
        self._prune()

    def observe(
        self, t: float, tx_id: str, amount: float, merchant: str, country: str, device: str | None
    ) -> dict[str, float]:
        """Compute features for a transaction, then add it to the history."""
        feats = self.features(t, amount, country, device)
        self.add(t, tx_id, amount, merchant, country, device)
        return feats

    def _prune(self) -> None:
        cutoff = self.max_ts - HISTORY_SECONDS - RETENTION_SLACK
        drop = bisect_left(self.ts, cutoff)
        drop = max(drop, len(self.ts) - MAX_EVENTS)
        if drop > 0:
            for column in (self.ts, self.amounts, self.merchants, self.countries, self.tx_ids):
                del column[:drop]
        for known in (self.known_countries, self.known_devices):
            if len(known) > MAX_KNOWN or any(self.max_ts - v > KNOWN_TTL for v in known.values()):
                fresh = sorted(
                    ((k, v) for k, v in known.items() if self.max_ts - v <= KNOWN_TTL),
                    key=lambda kv: kv[1],
                )[-MAX_KNOWN:]
                known.clear()
                known.update(fresh)

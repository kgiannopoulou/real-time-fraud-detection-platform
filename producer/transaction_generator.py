"""Synthetic card-transaction generator.

The generator is an event-driven simulation of a card-holder population:

* every user has a home country, a set of devices, favourite merchants, a
  personal spending scale and a Poisson transaction rate;
* legitimate behaviour includes the "noise" that makes fraud detection hard:
  travel abroad, occasional big-ticket purchases and new phones;
* fraud is injected as *scenarios* (account takeover, card testing, stolen
  card, subtle misuse) that span several transactions, so behavioural features
  such as velocity and amount-vs-average carry real signal.

The same simulation backs both the labelled historical dataset used for
training (`generate_history`) and the live stream (`stream`), which keeps the
per-user feature distributions consistent between training and serving.

Activity is time-compressed: a card transacts every ~5 minutes on average so
that behavioural windows fill up within minutes of starting a demo stream.

Run directly to print a continuous stream of JSON transactions:

    python producer/transaction_generator.py --rate 20
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
import random
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #

# Home countries of the simulated card holders and their population share.
HOME_COUNTRIES: dict[str, float] = {
    "GR": 0.30,
    "DE": 0.15,
    "FR": 0.12,
    "IT": 0.10,
    "ES": 0.10,
    "NL": 0.06,
    "GB": 0.07,
    "PT": 0.04,
    "CY": 0.03,
    "AT": 0.03,
}
# Legitimate travel destinations.
TRAVEL_COUNTRIES = ["GR", "DE", "FR", "IT", "ES", "NL", "GB", "PT", "AT", "US", "CH", "BE", "TR"]
# Countries the simulated fraud rings operate from. Purely a property of this
# simulation; the model learns country risk from the labelled data.
FRAUD_ORIGIN_COUNTRIES = ["NG", "BR", "RU", "VN", "ID", "PH", "US", "RO"]

ALL_COUNTRIES = sorted(set(HOME_COUNTRIES) | set(TRAVEL_COUNTRIES) | set(FRAUD_ORIGIN_COUNTRIES))

# Local currency per country (everything else settles in EUR).
COUNTRY_CURRENCY = {"GB": "GBP", "US": "USD", "CH": "CHF"}
for _c in FRAUD_ORIGIN_COUNTRIES:
    COUNTRY_CURRENCY.setdefault(_c, "USD")
SUPPORTED_CURRENCIES = ("EUR", "USD", "GBP", "CHF")
# Static FX rates used to normalise amounts to EUR.
FX_TO_EUR = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17, "CHF": 1.05}


@dataclass(frozen=True)
class Category:
    name: str
    median_eur: float  # typical ticket size for an average spender
    sigma: float  # log-normal spread
    weight: float  # share of everyday (legitimate) spending


CATEGORIES: dict[str, Category] = {
    c.name: c
    for c in [
        Category("grocery", 35, 0.55, 0.22),
        Category("restaurants", 28, 0.55, 0.16),
        Category("fuel", 55, 0.35, 0.08),
        Category("pharmacy", 22, 0.6, 0.05),
        Category("clothing", 60, 0.7, 0.07),
        Category("online_retail", 45, 0.9, 0.12),
        Category("electronics", 180, 0.9, 0.03),
        Category("travel", 320, 0.8, 0.02),
        Category("entertainment", 25, 0.7, 0.07),
        Category("utilities", 70, 0.5, 0.05),
        Category("digital_goods", 12, 0.8, 0.08),
        Category("jewelry", 400, 0.9, 0.01),
        Category("gambling", 50, 0.9, 0.02),
        Category("crypto", 150, 1.0, 0.02),
    ]
}
MERCHANT_CATEGORIES = tuple(CATEGORIES)
BIG_TICKET_CATEGORIES = ("electronics", "travel", "jewelry")


def legit_hour_weight(hour: int) -> float:
    """Relative legitimate activity by hour of day (0.35 at night, 1.0 midday)."""
    return 0.35 + 0.65 * max(0.0, math.sin(math.pi * (hour - 6) / 17)) if 6 <= hour <= 23 else 0.35


def fraud_hour_weight(hour: int) -> float:
    """Fraud rings are more active at night in the card holder's time zone."""
    return 1.0 if hour < 6 else 0.6


# --------------------------------------------------------------------------- #
# Population
# --------------------------------------------------------------------------- #


@dataclass
class User:
    user_id: str
    home_country: str
    spend_scale: float
    mean_gap_s: float
    devices: list[str]
    favourite_merchants: dict[str, list[str]]
    category_weights: list[float]
    travel_country: str | None = None
    travelling_until: datetime | None = None

    @property
    def current_device(self) -> str:
        return self.devices[-1]

    def location(self, now: datetime) -> str:
        if self.travelling_until and now < self.travelling_until:
            return self.travel_country or self.home_country
        return self.home_country


@dataclass
class GeneratorConfig:
    n_users: int = 600
    n_merchants: int = 400
    mean_gap_s: float = 300.0
    # Fraud scenarios started per user per hour; calibrated to ~0.6-0.8 %
    # fraudulent transactions.
    fraud_scenarios_per_user_hour: float = 0.022
    travel_prob: float = 0.002
    new_device_prob: float = 0.001
    big_ticket_prob: float = 0.012
    # Multiplies every amount; > 1 simulates data drift (e.g. inflation, a new
    # customer segment) for the monitoring demo.
    amount_drift: float = 1.0


@dataclass(order=True)
class _Scheduled:
    at: datetime
    seq: int
    kind: str = field(compare=False)  # "legit" | "fraud" | "scenario"
    user_idx: int = field(compare=False)
    payload: dict | None = field(compare=False, default=None)


class TransactionGenerator:
    """Event-driven simulation producing time-ordered transactions."""

    def __init__(self, seed: int = 42, config: GeneratorConfig | None = None):
        self.cfg = config or GeneratorConfig()
        self.rng = random.Random(seed)
        self._seq = 0
        self.merchants = self._build_merchants()
        self.users = [self._build_user(i) for i in range(self.cfg.n_users)]

    # -- population -------------------------------------------------------- #

    def _device_id(self) -> str:
        return f"device_{self.rng.getrandbits(40):010x}"

    def _build_merchants(self) -> dict[str, list[str]]:
        by_category: dict[str, list[str]] = {name: [] for name in CATEGORIES}
        names = list(CATEGORIES)
        weights = [max(c.weight, 0.02) for c in CATEGORIES.values()]
        for i in range(self.cfg.n_merchants):
            by_category[self.rng.choices(names, weights)[0]].append(f"merchant_{i}")
        for name, merchants in by_category.items():  # every category needs a merchant
            if not merchants:
                merchants.append(f"merchant_{name}")
        return by_category

    def _build_user(self, idx: int) -> User:
        rng = self.rng
        home = rng.choices(list(HOME_COUNTRIES), list(HOME_COUNTRIES.values()))[0]
        favourites = {
            name: rng.sample(ms, k=min(len(ms), rng.randint(2, 5))) for name, ms in self.merchants.items()
        }
        # Personal taste: perturb the population category mix.
        weights = [c.weight * rng.lognormvariate(0, 0.5) for c in CATEGORIES.values()]
        return User(
            user_id=f"user_{idx}",
            home_country=home,
            spend_scale=rng.lognormvariate(0, 0.45),
            mean_gap_s=self.cfg.mean_gap_s * rng.lognormvariate(0, 0.35),
            devices=[self._device_id() for _ in range(rng.choice([1, 1, 2]))],
            favourite_merchants=favourites,
            category_weights=weights,
        )

    # -- helpers ------------------------------------------------------------ #

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _tx_id(self) -> str:
        return f"tx_{self.rng.getrandbits(48):012x}"

    def _amount(self, category: str, scale: float) -> float:
        cat = CATEGORIES[category]
        eur = cat.median_eur * scale * self.rng.lognormvariate(0, cat.sigma)
        return max(0.5, eur * self.cfg.amount_drift)

    def _transaction(
        self,
        user: User,
        at: datetime,
        category: str,
        amount_eur: float,
        country: str,
        device: str,
        merchant: str | None = None,
        fraud_type: str = "",
    ) -> dict:
        currency = COUNTRY_CURRENCY.get(country, "EUR")
        merchant = merchant or self.rng.choice(self.merchants[category])
        return {
            "transaction_id": self._tx_id(),
            "timestamp": at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "user_id": user.user_id,
            "merchant_id": merchant,
            "merchant_category": category,
            "amount": round(amount_eur / FX_TO_EUR[currency], 2),
            "currency": currency,
            "country": country,
            "device_id": device,
            "is_fraud": int(bool(fraud_type)),
            "fraud_type": fraud_type,
        }

    # -- behaviour ---------------------------------------------------------- #

    def _legit(self, user: User, at: datetime) -> dict:
        rng = self.rng
        if not (user.travelling_until and at < user.travelling_until) and rng.random() < self.cfg.travel_prob:
            destinations = FRAUD_ORIGIN_COUNTRIES if rng.random() < 0.3 else TRAVEL_COUNTRIES
            user.travel_country = rng.choice([c for c in destinations if c != user.home_country])
            user.travelling_until = at + timedelta(hours=rng.uniform(1, 6))
        if rng.random() < self.cfg.new_device_prob:
            user.devices.append(self._device_id())  # new phone

        if rng.random() < self.cfg.big_ticket_prob:
            category = rng.choice(BIG_TICKET_CATEGORIES)
            amount = self._amount(category, user.spend_scale * rng.uniform(1.5, 3))
        else:
            category = rng.choices(MERCHANT_CATEGORIES, user.category_weights)[0]
            amount = self._amount(category, user.spend_scale)
        merchant = rng.choice(user.favourite_merchants[category]) if rng.random() < 0.7 else None
        return self._transaction(user, at, category, amount, user.location(at), user.current_device, merchant)

    def _fraud_scenario(self, user: User, start: datetime) -> list[tuple[datetime, dict]]:
        """Return the (timestamp, transaction) pairs of one fraud scenario."""
        rng = self.rng
        kind = rng.choices(
            ["account_takeover", "card_testing", "stolen_card", "subtle"], [0.4, 0.3, 0.2, 0.1]
        )[0]
        events: list[tuple[datetime, dict]] = []
        t = start

        if kind == "account_takeover":
            device = self._device_id()
            country = (
                rng.choice(FRAUD_ORIGIN_COUNTRIES)
                if rng.random() < 0.5
                else rng.choice([c for c in TRAVEL_COUNTRIES if c != user.home_country])
            )
            for _ in range(rng.randint(1, 4)):
                category = rng.choice(["electronics", "jewelry", "travel", "crypto", "gambling"])
                amount = self._amount(category, user.spend_scale * rng.uniform(1.5, 4))
                events.append(
                    (t, self._transaction(user, t, category, amount, country, device, fraud_type=kind))
                )
                t += timedelta(seconds=rng.uniform(20, 180))

        elif kind == "card_testing":
            device = self._device_id() if rng.random() < 0.8 else user.current_device
            country = rng.choice(FRAUD_ORIGIN_COUNTRIES + [user.home_country])
            for _ in range(rng.randint(3, 8)):
                category = rng.choice(["digital_goods", "online_retail"])
                amount = rng.uniform(0.5, 9.99)
                events.append(
                    (t, self._transaction(user, t, category, amount, country, device, fraud_type=kind))
                )
                t += timedelta(seconds=rng.uniform(5, 40))
            if rng.random() < 0.5:
                amount = self._amount("electronics", user.spend_scale * rng.uniform(2, 4))
                events.append(
                    (t, self._transaction(user, t, "electronics", amount, country, device, fraud_type=kind))
                )

        elif kind == "stolen_card":
            device = self._device_id() if rng.random() < 0.5 else user.current_device
            for _ in range(rng.randint(1, 3)):
                category = rng.choice(["electronics", "jewelry", "clothing"])
                amount = self._amount(category, user.spend_scale * rng.uniform(2, 5))
                events.append(
                    (
                        t,
                        self._transaction(
                            user, t, category, amount, user.home_country, device, fraud_type=kind
                        ),
                    )
                )
                t += timedelta(minutes=rng.uniform(5, 30))

        else:  # subtle: looks almost like the card holder — the irreducible error
            category = rng.choice(["online_retail", "digital_goods", "clothing"])
            amount = self._amount(category, user.spend_scale * rng.uniform(1, 2.5))
            events.append(
                (
                    t,
                    self._transaction(
                        user, t, category, amount, user.location(t), user.current_device, fraud_type=kind
                    ),
                )
            )
        return events

    # -- simulation --------------------------------------------------------- #

    def simulate(self, start: datetime, end: datetime | None = None) -> Iterator[dict]:
        """Yield transactions in event-time order from ``start`` (until ``end``)."""
        rng = self.rng
        heap: list[_Scheduled] = []
        for idx, user in enumerate(self.users):
            first = start + timedelta(seconds=rng.expovariate(1 / user.mean_gap_s))
            heapq.heappush(heap, _Scheduled(first, self._next_seq(), "legit", idx))
        fraud_rate_per_s = self.cfg.fraud_scenarios_per_user_hour * len(self.users) / 3600
        if fraud_rate_per_s > 0:
            first = start + timedelta(seconds=rng.expovariate(fraud_rate_per_s))
            heapq.heappush(heap, _Scheduled(first, self._next_seq(), "scenario", -1))

        while heap:
            item = heapq.heappop(heap)
            if end is not None and item.at >= end:
                break
            if item.kind == "legit":
                user = self.users[item.user_idx]
                nxt = item.at + timedelta(seconds=rng.expovariate(1 / user.mean_gap_s))
                heapq.heappush(heap, _Scheduled(nxt, self._next_seq(), "legit", item.user_idx))
                if rng.random() < legit_hour_weight(item.at.hour):  # diurnal thinning
                    yield self._legit(user, item.at)
            elif item.kind == "scenario":
                nxt = item.at + timedelta(seconds=rng.expovariate(fraud_rate_per_s))
                heapq.heappush(heap, _Scheduled(nxt, self._next_seq(), "scenario", -1))
                if rng.random() < fraud_hour_weight(item.at.hour):
                    idx = rng.randrange(len(self.users))
                    for at, tx in self._fraud_scenario(self.users[idx], item.at):
                        heapq.heappush(heap, _Scheduled(at, self._next_seq(), "fraud", idx, tx))
            else:
                assert item.payload is not None
                yield item.payload

    def generate_history(self, days: float = 3.0, start: datetime | None = None) -> list[dict]:
        """Labelled historical transactions covering ``days`` of simulated time."""
        start = start or datetime(2026, 9, 1, tzinfo=timezone.utc)
        return list(self.simulate(start, start + timedelta(days=days)))

    def stream(self, realtime: bool = True, seed: int | None = None) -> Iterator[dict]:
        """Infinite stream of transactions stamped with the current time.

        The population (users, devices, merchants) is fixed by the constructor
        seed, but events are drawn from a fresh RNG (``seed=None``: OS entropy)
        so a restarted producer never replays the same transaction ids.

        When ``realtime`` is true the generator sleeps so that events are
        emitted when their simulated timestamp is reached.
        """
        self.rng = random.Random(seed)
        origin = datetime.now(timezone.utc)
        wall_origin = time.monotonic()
        for tx in self.simulate(origin):
            event_time = _parse_ts(tx["timestamp"])
            if realtime:
                delay = (event_time - origin).total_seconds() - (time.monotonic() - wall_origin)
                if delay > 0:
                    time.sleep(delay)
            yield tx


def users_for_rate(rate: float, mean_gap_s: float = GeneratorConfig.mean_gap_s) -> int:
    """Population size that produces roughly ``rate`` transactions per second."""
    avg_weight = sum(legit_hour_weight(h) for h in range(24)) / 24
    return max(1, round(rate * mean_gap_s / avg_weight))


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def strip_labels(tx: dict) -> dict:
    """Remove ground-truth labels before publishing an event."""
    return {k: v for k, v in tx.items() if k not in ("is_fraud", "fraud_type")}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Print a continuous stream of JSON transactions")
    parser.add_argument("--rate", type=float, default=20.0, help="approximate transactions/sec")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int, default=0, help="stop after N events (0 = forever)")
    parser.add_argument("--labels", action="store_true", help="include ground-truth labels")
    args = parser.parse_args(argv)

    cfg = GeneratorConfig(n_users=users_for_rate(args.rate))
    gen = TransactionGenerator(seed=args.seed, config=cfg)
    try:
        for i, tx in enumerate(gen.stream(), start=1):
            print(json.dumps(tx if args.labels else strip_labels(tx)), flush=True)
            if args.limit and i >= args.limit:
                break
    except (KeyboardInterrupt, BrokenPipeError):
        sys.exit(0)


if __name__ == "__main__":
    main()

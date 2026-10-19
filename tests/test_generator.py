from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from producer.chaos import CORRUPTIONS, ChaosConfig, ChaosInjector, corrupt
from producer.transaction_generator import (
    ALL_COUNTRIES,
    MERCHANT_CATEGORIES,
    SUPPORTED_CURRENCIES,
    GeneratorConfig,
    TransactionGenerator,
    strip_labels,
    users_for_rate,
)
from streaming.validation import validate_payload, validate_record

EVENT_FIELDS = {
    "transaction_id",
    "timestamp",
    "user_id",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "device_id",
}


def _gen(seed: int = 3) -> TransactionGenerator:
    return TransactionGenerator(seed=seed, config=GeneratorConfig(n_users=50))


def test_history_is_deterministic_for_a_seed():
    a = _gen().generate_history(days=0.1)
    b = _gen().generate_history(days=0.1)
    assert a == b
    assert a != _gen(seed=4).generate_history(days=0.1)


def test_history_events_follow_the_contract(history):
    assert len(history) > 5_000
    assert set(EVENT_FIELDS) <= set(history.columns)
    assert history["transaction_id"].is_unique
    assert history["timestamp"].is_monotonic_increasing
    assert (history["amount"] > 0).all()
    assert set(history["currency"]) <= set(SUPPORTED_CURRENCIES)
    assert set(history["country"]) <= set(ALL_COUNTRIES)
    assert set(history["merchant_category"]) <= set(MERCHANT_CATEGORIES)


def test_history_contains_labelled_fraud_scenarios(history):
    fraud = history[history["is_fraud"] == 1]
    assert 0.001 < history["is_fraud"].mean() < 0.05
    assert {"card_testing", "account_takeover"} <= set(fraud["fraud_type"])
    assert (history.loc[history["is_fraud"] == 0, "fraud_type"] == "").all()


def test_every_generated_event_passes_validation(history):
    sample = history.head(500).copy()
    sample["timestamp"] = sample["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    for record in sample.to_dict("records"):
        assert validate_record(record, now) == []


def test_stream_uses_current_time_and_fresh_ids():
    gen = _gen()
    first = [tx for tx, _ in zip(gen.stream(realtime=False), range(20), strict=False)]
    again = [tx for tx, _ in zip(gen.stream(realtime=False), range(20), strict=False)]
    ts = datetime.fromisoformat(first[0]["timestamp"].replace("Z", "+00:00"))
    assert abs(ts - datetime.now(timezone.utc)) < timedelta(minutes=10)
    assert {t["transaction_id"] for t in first}.isdisjoint({t["transaction_id"] for t in again})
    assert {t["user_id"] for t in first} <= {u.user_id for u in gen.users}


def test_strip_labels_removes_ground_truth():
    tx = _gen().generate_history(days=0.01)[0]
    assert set(strip_labels(tx)) == EVENT_FIELDS


def test_users_for_rate_scales_linearly():
    assert users_for_rate(40) == 2 * users_for_rate(20)


def test_every_corruption_is_rejected_by_validation():
    tx = strip_labels(_gen().generate_history(days=0.01)[0])
    tx["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    for kind in CORRUPTIONS:
        bad = corrupt(tx, kind)
        raw = bad if isinstance(bad, str) else json.dumps(bad)
        assert validate_payload(raw), kind


def test_chaos_injector_delays_and_duplicates():
    chaos = ChaosInjector(ChaosConfig(late_rate=1.0, max_late_seconds=10), seed=1)
    event = {"transaction_id": "tx_1"}
    assert chaos.process(event, now=0.0) == []  # held back
    assert chaos.release(now=1.0) == []  # late events are held at least 2 s
    assert chaos.stats["late"] == 1
    assert chaos.release(now=100.0) == [event]

    dup = ChaosInjector(ChaosConfig(duplicate_rate=1.0), seed=1)
    out = dup.process(event, now=0.0)
    assert out == [event]
    assert dup.release(now=100.0) == [event]

from __future__ import annotations

import math
import random

import numpy as np
import pandas as pd
import pytest

from ml.feature_pipeline import FEATURES, RiskTables, build_behavioural_features, time_split
from streaming.features import BEHAVIOURAL_FEATURES, MAX_EVENTS, UserState

T0 = 1_800_000_000.0


def _observe(
    state: UserState,
    t: float,
    amount: float = 20.0,
    merchant: str = "m1",
    country: str = "GR",
    device: str = "d1",
    tx_id: str | None = None,
) -> dict:
    return state.observe(t, tx_id or f"tx_{t}", amount, merchant, country, device)


def test_first_transaction_has_no_history():
    f = _observe(UserState(), T0)
    assert f["tx_count_24h"] == 0
    assert math.isnan(f["avg_amount_24h"]) and math.isnan(f["amount_ratio"])
    assert f["is_new_country"] == 1 and f["is_new_device"] == 1
    assert math.isnan(f["seconds_since_last_tx"])


def test_velocity_windows_count_only_earlier_events():
    state = UserState()
    for offset in (-7200, -1800, -240, -60, -30):
        _observe(state, T0 + offset)
    f = state.features(T0, 20.0, "GR", "d1")
    assert (f["tx_count_2m"], f["tx_count_5m"], f["tx_count_1h"], f["tx_count_24h"]) == (2, 3, 4, 5)
    assert f["seconds_since_last_tx"] == pytest.approx(30)


def test_amount_ratio_against_the_users_own_average():
    state = UserState()
    for i, amount in enumerate((20, 35, 15, 42)):
        _observe(state, T0 + i * 600, amount=amount)
    f = state.features(T0 + 3000, 2400.0, "GR", "d1")
    assert f["avg_amount_24h"] == pytest.approx(28.0)
    assert f["amount_ratio"] == pytest.approx(2400 / 28)


def test_new_country_and_device_are_relative_to_history():
    state = UserState()
    _observe(state, T0, country="GR", device="d1")
    f = state.features(T0 + 60, 10.0, "NG", "d2")
    assert f["is_new_country"] == 1 and f["is_new_device"] == 1
    assert f["unique_countries_24h"] == 2
    same = state.features(T0 + 60, 10.0, "GR", "d1")
    assert same["is_new_country"] == 0 and same["is_new_device"] == 0


def test_out_of_order_events_get_the_same_features_as_in_order():
    events = [
        (T0 + i * 45.0, f"tx{i}", 10.0 + i, f"m{i % 3}", "GR" if i % 4 else "DE", "d1") for i in range(12)
    ]
    in_order, shuffled = UserState(), UserState()
    expected = {e[1]: in_order.observe(*e) for e in events}

    # Late arrivals: everything is ingested, then the late event is evaluated
    # against strictly earlier history only.
    late = events[5]
    for e in events:
        if e is not late:
            shuffled.add(*e)
    got = shuffled.features(late[0], late[2], late[4], late[5])
    for name in (
        "tx_count_2m",
        "tx_count_5m",
        "tx_count_1h",
        "tx_count_24h",
        "avg_amount_24h",
        "unique_merchants_1h",
        "seconds_since_last_tx",
    ):
        assert got[name] == pytest.approx(expected[late[1]][name]), name


def test_duplicates_are_detected():
    state = UserState()
    _observe(state, T0, tx_id="tx_dup")
    assert state.has_seen("tx_dup")
    assert not state.has_seen("tx_other")


def test_state_is_bounded_and_serializable():
    state = UserState()
    for i in range(MAX_EVENTS + 200):
        _observe(state, T0 + i, merchant=f"m{i}")
    assert len(state.ts) <= MAX_EVENTS
    _observe(state, T0 + 3 * 86_400)  # two days later: old history pruned
    assert len(state.ts) == 1
    restored = UserState.from_json(state.to_json())
    assert restored == state


def test_offline_pipeline_matches_streaming_replay(history):
    """Train/serve parity: the offline features equal a per-user streaming replay."""
    offline = build_behavioural_features(history)
    users = random.Random(0).sample(sorted(offline["user_id"].unique()), 5)
    for user in users:
        rows = offline[offline["user_id"] == user]
        state = UserState()
        for row in rows.itertuples():
            feats = state.observe(
                row.timestamp.timestamp(),
                row.transaction_id,
                row.amount_eur,
                row.merchant_id,
                row.country,
                row.device_id,
            )
            for name in BEHAVIOURAL_FEATURES:
                a, b = feats[name], getattr(row, name)
                assert (math.isnan(a) and math.isnan(b)) or a == pytest.approx(b), (user, name)


def test_risk_tables_are_smoothed_towards_the_prior(history):
    risk = RiskTables.fit(history, smoothing=50)
    assert set(risk.merchant_category) == set(history["merchant_category"])
    assert all(0 < v < 1 for v in risk.country.values())
    restored = RiskTables.from_dict(risk.to_dict())
    assert restored == risk


def test_time_split_is_chronological(history):
    train, valid, test = time_split(history)
    assert train["timestamp"].max() <= valid["timestamp"].min()
    assert valid["timestamp"].max() <= test["timestamp"].min()
    assert len(train) + len(valid) + len(test) == len(history)


def test_feature_list_is_complete(history):
    frame = build_behavioural_features(history.head(200))
    assert set(FEATURES) - {"merchant_risk", "country_risk"} <= set(frame.columns)
    assert not np.isinf(frame[BEHAVIOURAL_FEATURES].to_numpy(dtype=float)).any()
    assert isinstance(frame, pd.DataFrame)

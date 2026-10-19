"""Offline feature pipeline: raw transactions -> model features.

Features fall in three groups:

* **transaction** — amount normalised to EUR and hour of day;
* **behavioural** — how the transaction compares with the card holder's own
  history: velocity over 2 min / 5 min / 1 h / 24 h, average amount and the
  amount ratio, merchant and country diversity, new country / new device and
  time since the previous transaction. They are computed by the *same*
  ``streaming.features.UserState`` class the Spark job uses;
* **risk encodings** — merchant category and country turned into smoothed
  fraud rates learned from the training split only.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from producer.transaction_generator import FX_TO_EUR
from streaming.features import BEHAVIOURAL_FEATURES, UserState

FEATURES = [
    "amount_eur",
    "hour_of_day",
    *BEHAVIOURAL_FEATURES,
    "merchant_risk",
    "country_risk",
]
TARGET = "is_fraud"


@dataclass
class RiskTables:
    """Smoothed target encoding: (n * rate + m * prior) / (n + m)."""

    prior: float
    merchant_category: dict[str, float] = field(default_factory=dict)
    country: dict[str, float] = field(default_factory=dict)

    @classmethod
    def fit(cls, df: pd.DataFrame, smoothing: float = 50.0) -> RiskTables:
        prior = float(df[TARGET].mean())

        def encode(column: str) -> dict[str, float]:
            stats = df.groupby(column)[TARGET].agg(["sum", "count"])
            rate = (stats["sum"] + smoothing * prior) / (stats["count"] + smoothing)
            return {str(k): round(float(v), 6) for k, v in rate.items()}

        return cls(prior, encode("merchant_category"), encode("country"))

    def to_dict(self) -> dict:
        return {"prior": self.prior, "merchant_category": self.merchant_category, "country": self.country}

    @classmethod
    def from_dict(cls, data: dict) -> RiskTables:
        return cls(data["prior"], data["merchant_category"], data["country"])


def amount_eur(df: pd.DataFrame) -> pd.Series:
    return (df["amount"] * df["currency"].map(FX_TO_EUR).fillna(1.0)).round(2)


def epoch_seconds(timestamps: pd.Series) -> pd.Series:
    return (timestamps - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()


def build_behavioural_features(df: pd.DataFrame) -> pd.DataFrame:
    """Replay transactions in event-time order through per-user ``UserState``s."""
    out = df.sort_values(["timestamp", "transaction_id"]).reset_index(drop=True)
    out["amount_eur"] = amount_eur(out)
    out["hour_of_day"] = out["timestamp"].dt.hour

    records = zip(
        epoch_seconds(out["timestamp"]).to_numpy(),
        out["user_id"].to_numpy(),
        out["transaction_id"].to_numpy(),
        out["amount_eur"].to_numpy(),
        out["merchant_id"].to_numpy(),
        out["country"].to_numpy(),
        out["device_id"].to_numpy(),
        strict=True,
    )
    states: dict[str, UserState] = {}
    rows = []
    for t, user, tx_id, amount, merchant, country, device in records:
        state = states.get(user)
        if state is None:
            state = states[user] = UserState()
        rows.append(state.observe(float(t), tx_id, float(amount), merchant, country, device))
    feats = pd.DataFrame(rows, columns=BEHAVIOURAL_FEATURES, index=out.index)
    return pd.concat([out, feats], axis=1)


def add_risk_features(df: pd.DataFrame, risk: RiskTables) -> pd.DataFrame:
    df["merchant_risk"] = df["merchant_category"].map(risk.merchant_category).fillna(risk.prior)
    df["country_risk"] = df["country"].map(risk.country).fillna(risk.prior)
    return df


def build_features(df: pd.DataFrame, risk: RiskTables) -> pd.DataFrame:
    return add_risk_features(build_behavioural_features(df), risk)


def time_split(
    df: pd.DataFrame, train: float = 0.7, valid: float = 0.15
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Chronological split: the model is always evaluated on the future."""
    df = df.sort_values("timestamp")
    t_train = df["timestamp"].quantile(train)
    t_valid = df["timestamp"].quantile(train + valid)
    return (
        df[df["timestamp"] <= t_train],
        df[(df["timestamp"] > t_train) & (df["timestamp"] <= t_valid)],
        df[df["timestamp"] > t_valid],
    )

"""Offline feature pipeline: raw transactions -> model features.

Features fall in three groups:

* **transaction** — amount (normalised to EUR) and hour of day;
* **behavioural** — how the transaction compares with the card holder's own
  recent history (frequency, average amount, amount ratio);
* **risk encodings** — merchant category and country turned into smoothed
  fraud rates learned from the training split only.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from producer.transaction_generator import FX_TO_EUR

FEATURES = [
    "amount",
    "hour_of_day",
    "tx_count_1h",
    "avg_amount",
    "amount_ratio",
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
    return df["amount"] * df["currency"].map(FX_TO_EUR).fillna(1.0)


def build_behavioural_features(df: pd.DataFrame) -> pd.DataFrame:
    """Per-user features computed only from each user's *earlier* transactions."""
    out = df.sort_values(["timestamp", "transaction_id"]).reset_index(drop=True)
    out["amount_eur"] = amount_eur(out)
    out["hour_of_day"] = out["timestamp"].dt.hour

    by_user = out.groupby("user_id", sort=False)
    # Transactions by the same user in the hour before this one.
    rolling = out.set_index("timestamp").groupby("user_id")["amount_eur"].rolling("1h", closed="left").count()
    out["tx_count_1h"] = np.nan_to_num(_align_rolling(out, rolling))
    # Mean of all previous amounts (NaN for the first transaction).
    previous_sum = by_user["amount_eur"].cumsum() - out["amount_eur"]
    previous_n = by_user.cumcount()
    out["avg_amount"] = (previous_sum / previous_n.replace(0, np.nan)).astype(float)
    out["amount_ratio"] = out["amount_eur"] / out["avg_amount"]
    return out


def _align_rolling(df: pd.DataFrame, rolling: pd.Series) -> np.ndarray:
    """Map a groupby-rolling result (grouped order) back onto ``df`` rows."""
    order = df.sort_values("user_id", kind="stable").index
    values = np.empty(len(df))
    values[order.to_numpy()] = rolling.to_numpy()
    return values


def build_features(df: pd.DataFrame, risk: RiskTables) -> pd.DataFrame:
    out = build_behavioural_features(df)
    out["amount"] = out["amount_eur"]
    out["merchant_risk"] = out["merchant_category"].map(risk.merchant_category).fillna(risk.prior)
    out["country_risk"] = out["country"].map(risk.country).fillna(risk.prior)
    return out


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

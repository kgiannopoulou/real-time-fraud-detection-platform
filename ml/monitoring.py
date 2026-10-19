"""Feature-drift monitoring with the Population Stability Index (PSI).

At training time each feature's distribution is summarised as decile bins
(plus a bucket for missing values). In production the same bins are filled
with recent traffic and compared:

    PSI = sum((actual% - expected%) * ln(actual% / expected%))

Rule of thumb: < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 significant
drift worth investigating (or retraining).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

EPSILON = 1e-4
PSI_WARN = 0.1
PSI_ALERT = 0.25

# Features whose distribution should be stationary in a short production
# window. Excluded on purpose: hour_of_day (a 5-minute window covers one hour
# by construction) and history-length features such as tx_count_24h or
# is_new_device, which drift every time the stream warms up from empty state.
DRIFT_FEATURES = ["amount_eur", "amount_ratio", "merchant_risk", "country_risk"]


def feature_baseline(values: pd.Series | np.ndarray, bins: int = 10) -> dict:
    """Bin edges (interior quantiles) and bucket proportions of a training feature."""
    arr = np.asarray(values, dtype=float)
    finite = arr[~np.isnan(arr)]
    edges = np.unique(np.quantile(finite, np.linspace(0, 1, bins + 1)[1:-1])) if finite.size else np.array([])
    props = _proportions(arr, edges)
    return {
        "edges": [float(e) for e in edges],
        "proportions": [round(float(p), 6) for p in props],
        "mean": float(finite.mean()) if finite.size else None,
    }


def _proportions(arr: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Share of values per bucket: (-inf, e1], ..., (ek, inf), then NaN."""
    nan = np.isnan(arr)
    idx = np.searchsorted(edges, arr[~nan], side="left")
    counts = np.bincount(idx, minlength=len(edges) + 1).astype(float)
    counts = np.append(counts, nan.sum())
    return counts / max(len(arr), 1)


def psi(baseline: dict, values: pd.Series | np.ndarray, include_missing: bool = False) -> float:
    """PSI of ``values`` against a training baseline.

    By default missing values are excluded on both sides: a stream warming up
    from empty state has many undefined ratios, which is a coverage question
    (tracked separately) rather than a shift in the distribution.
    """
    arr = np.asarray(values, dtype=float)
    expected = np.asarray(baseline["proportions"], dtype=float)
    actual = _proportions(arr, np.asarray(baseline["edges"], dtype=float))
    if not include_missing:
        arr = arr[~np.isnan(arr)]
        expected, actual = expected[:-1], actual[:-1]
        if expected.sum() == 0 or actual.sum() == 0:
            return math.nan
        expected, actual = expected / expected.sum(), actual / actual.sum()
    if arr.size == 0:
        return math.nan
    expected, actual = np.clip(expected, EPSILON, None), np.clip(actual, EPSILON, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def build_baselines(train: pd.DataFrame, features: list[str]) -> dict[str, dict]:
    return {name: feature_baseline(train[name]) for name in features}


def drift_report(baselines: dict[str, dict], recent: pd.DataFrame) -> pd.DataFrame:
    """PSI and mean shift per feature for a window of recent traffic."""
    rows = []
    for name, base in baselines.items():
        if name not in recent:
            continue
        values = pd.to_numeric(recent[name], errors="coerce")
        value = psi(base, values)
        rows.append(
            {
                "feature": name,
                "psi": value,
                "training_mean": base.get("mean"),
                "production_mean": float(values.mean()) if values.notna().any() else None,
                "status": "drift" if value > PSI_ALERT else "watch" if value > PSI_WARN else "stable",
            }
        )
    return pd.DataFrame(rows).sort_values("psi", ascending=False, ignore_index=True)

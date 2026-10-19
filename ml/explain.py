"""Explain individual fraud scores with SHAP values.

XGBoost computes exact TreeSHAP contributions natively
(``predict(..., pred_contribs=True)``), so no extra dependency is needed.
Each contribution is the feature's push on the transaction's log-odds of
fraud; the strongest positive pushes are turned into analyst-readable risk
factors such as "amount 8.2x higher than the customer's 24h average".

    python -m ml.explain        # global importance chart + example explanations
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import xgboost as xgb

MIN_CONTRIBUTION = 0.15  # log-odds; smaller pushes are noise for an analyst
MAX_FACTORS = 4


def shap_values(booster: xgb.Booster, rows: pd.DataFrame, features: list[str]) -> np.ndarray:
    """(n_rows, n_features) SHAP contributions in log-odds (bias column dropped)."""
    matrix = xgb.DMatrix(rows[features].astype(float), feature_names=features)
    return booster.predict(matrix, pred_contribs=True)[:, :-1]


def _fmt_int(value: float) -> str:
    return str(int(value)) if not math.isnan(value) else "?"


def describe(feature: str, value: float, row: dict) -> str | None:
    """Human-readable reason for a feature that pushed the score up."""
    nan = value is None or (isinstance(value, float) and math.isnan(value))
    country, device, category = row.get("country"), row.get("device_id"), row.get("merchant_category")
    if feature == "amount_ratio":
        if nan:
            return "no spending history to compare the amount with"
        if value >= 1.5:
            return f"amount {value:.1f}x higher than the customer's 24h average"
        if value <= 0.3:
            return f"amount far below the customer's usual spend ({value:.2g}x) - card-testing pattern"
        return None
    if feature == "amount_eur":
        return f"very small amount (EUR {value:.2f})" if value < 10 else f"high amount (EUR {value:,.0f})"
    if feature in ("tx_count_2m", "tx_count_5m", "tx_count_1h"):
        window = {"tx_count_2m": "2 minutes", "tx_count_5m": "5 minutes", "tx_count_1h": "hour"}[feature]
        return f"{_fmt_int(value)} transactions in the previous {window}" if value >= 2 else None
    if feature == "tx_count_24h":
        return "no recent history for this card" if value == 0 else None
    if feature == "unique_merchants_1h":
        return f"{_fmt_int(value)} different merchants within an hour" if value >= 3 else None
    if feature == "unique_countries_24h":
        return f"transactions from {_fmt_int(value)} countries within 24h" if value >= 2 else None
    if feature == "is_new_country":
        return f"first transaction from {country}" if value == 1 else None
    if feature == "is_new_device":
        return f"new device ({device})" if value == 1 else None
    if feature == "seconds_since_last_tx":
        return f"only {value:.0f}s since the previous transaction" if not nan and value < 120 else None
    if feature == "merchant_risk":
        return f"elevated fraud rate for {category} merchants ({value:.1%})"
    if feature == "country_risk":
        return f"elevated fraud rate for transactions in {country} ({value:.1%})"
    if feature == "hour_of_day":
        return f"unusual hour ({int(value):02d}:00 UTC)"
    if feature == "avg_amount_24h":
        return None  # only meaningful through amount_ratio
    return f"{feature} = {value}"


def risk_factors(
    contributions: np.ndarray, rows: pd.DataFrame, features: list[str], max_factors: int = MAX_FACTORS
) -> list[list[str]]:
    """Top positive contributions of every row, as readable sentences."""
    records = rows.to_dict("records")
    out: list[list[str]] = []
    for contrib, row in zip(contributions, records, strict=True):
        reasons: list[str] = []
        for idx in np.argsort(-contrib):
            if contrib[idx] < MIN_CONTRIBUTION or len(reasons) >= max_factors:
                break
            text = describe(features[idx], row.get(features[idx]), row)
            if text and text not in reasons:
                reasons.append(text)
        out.append(reasons)
    return out


def top_contributions(contributions: np.ndarray, features: list[str], k: int = 5) -> list[dict[str, float]]:
    """The k largest absolute SHAP values per row, for storage / audit."""
    out = []
    for contrib in contributions:
        idx = np.argsort(-np.abs(contrib))[:k]
        out.append({features[i]: round(float(contrib[i]), 4) for i in idx})
    return out


def plot_importance(booster: xgb.Booster, rows: pd.DataFrame, features: list[str], path) -> None:
    """Mean |SHAP| per feature — global importance chart."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    values = np.abs(shap_values(booster, rows, features)).mean(axis=0)
    order = np.argsort(values)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh([features[i] for i in order], values[order], color="#E45756")
    ax.set_title("Global feature importance (mean |SHAP|, log-odds)")
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main() -> None:
    from ml.dataset import ROOT, load_synthetic
    from ml.feature_pipeline import build_features, time_split
    from ml.predict import FraudModel

    model = FraudModel.load()
    df = load_synthetic()
    features = build_features(df, model.risk)
    _, _, test_raw = time_split(df)
    test = features[features["transaction_id"].isin(test_raw["transaction_id"])]

    path = ROOT / "docs" / "images" / "shap_importance.png"
    plot_importance(model.booster, test.sample(min(len(test), 20_000), random_state=0), model.features, path)
    print(f"wrote {path.relative_to(ROOT)}\n")

    scored = test.assign(fraud_probability=model.predict_proba(test))
    top = scored.sort_values("fraud_probability", ascending=False).head(5)
    reasons = risk_factors(shap_values(model.booster, top, model.features), top, model.features)
    for (_, row), why in zip(top.iterrows(), reasons, strict=True):
        print(
            f"{row['transaction_id']}  {row['fraud_type'] or 'legit':<16} p={row['fraud_probability']:.2f}"
            f"  EUR {row['amount_eur']:,.2f} {row['merchant_category']} {row['country']}"
        )
        for reason in why:
            print(f"    + {reason}")


if __name__ == "__main__":
    main()

"""Exploratory analysis of the labelled transaction dataset.

    python -m ml.eda

Prints a summary and writes charts to ``docs/images/eda_*.png``.
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from ml.dataset import ROOT, load_synthetic  # noqa: E402

IMAGES = ROOT / "docs" / "images"
LEGIT, FRAUD = "#4C78A8", "#E45756"


def summarize(df: pd.DataFrame) -> dict:
    per_user = df.groupby("user_id").size()
    return {
        "transactions": len(df),
        "users": df["user_id"].nunique(),
        "merchants": df["merchant_id"].nunique(),
        "fraud_rate": df["is_fraud"].mean(),
        "span": f"{df['timestamp'].min():%Y-%m-%d %H:%M} -> {df['timestamp'].max():%Y-%m-%d %H:%M}",
        "tx_per_user_median": float(per_user.median()),
        "fraud_by_type": df.loc[df["is_fraud"] == 1, "fraud_type"].value_counts().to_dict(),
        "median_amount_legit": float(df.loc[df["is_fraud"] == 0, "amount"].median()),
        "median_amount_fraud": float(df.loc[df["is_fraud"] == 1, "amount"].median()),
    }


def plot(df: pd.DataFrame) -> list[str]:
    IMAGES.mkdir(parents=True, exist_ok=True)
    written = []

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    bins = np.logspace(np.log10(0.5), np.log10(df["amount"].max()), 60)
    for label, color, name in [(0, LEGIT, "legitimate"), (1, FRAUD, "fraud")]:
        amounts = df.loc[df["is_fraud"] == label, "amount"]
        axes[0].hist(
            amounts,
            bins=bins,
            color=color,
            alpha=0.6,
            label=name,
            weights=np.full(len(amounts), 100 / len(amounts)),
        )
    axes[0].set_xscale("log")
    axes[0].set_title("Amount distribution (% of class, log scale)")
    axes[0].set_xlabel("amount")
    axes[0].legend()
    hourly = df.groupby(df["timestamp"].dt.hour)["is_fraud"].mean() * 100
    axes[1].bar(hourly.index, hourly.values, color=FRAUD)
    axes[1].set_title("Fraud rate by hour of day (%)")
    axes[1].set_xlabel("hour (UTC)")
    fig.tight_layout()
    fig.savefig(IMAGES / "eda_amount_hour.png", dpi=110)
    written.append("eda_amount_hour.png")
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    by_cat = df.groupby("merchant_category")["is_fraud"].mean().sort_values() * 100
    axes[0].barh(by_cat.index, by_cat.values, color=FRAUD)
    axes[0].set_title("Fraud rate by merchant category (%)")
    by_country = df.groupby("country")["is_fraud"].agg(["mean", "size"])
    by_country = by_country[by_country["size"] >= 50]["mean"].sort_values().tail(15) * 100
    axes[1].barh(by_country.index, by_country.values, color=FRAUD)
    axes[1].set_title("Fraud rate by country (%; top 15, n>=50)")
    fig.tight_layout()
    fig.savefig(IMAGES / "eda_category_country.png", dpi=110)
    written.append("eda_category_country.png")
    plt.close(fig)
    return written


def main() -> None:
    df = load_synthetic()
    for key, value in summarize(df).items():
        if isinstance(value, float):
            value = f"{value:.4f}"
        print(f"{key:>22}: {value}")
    for name in plot(df):
        print(f"wrote docs/images/{name}")


if __name__ == "__main__":
    main()

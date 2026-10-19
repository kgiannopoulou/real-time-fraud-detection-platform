"""Benchmark the model families on the public ULB credit-card fraud dataset.

    python -m ml.public_benchmark

The dataset (OpenML #1597) has 284,807 transactions over two days with 0.17 %
fraud and anonymised PCA features. The OpenML copy drops the ``Time`` column
but keeps the original row order, which is chronological, so the same
time-ordered train/valid/test split as the platform model is used.
"""

from __future__ import annotations

import time

import lightgbm as lgb
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.dataset import load_public
from ml.evaluate import best_f1_threshold, evaluate


def main() -> None:
    df = load_public()
    features = [c for c in df.columns if c not in ("is_fraud", "Time")]
    n = len(df)
    train, valid, test = df[: int(n * 0.7)], df[int(n * 0.7) : int(n * 0.85)], df[int(n * 0.85) :]
    print(f"{n:,} rows, fraud rate {df['is_fraud'].mean():.3%}; test frauds: {int(test['is_fraud'].sum())}")

    models = {
        "logistic_regression": make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000)),
        "random_forest": RandomForestClassifier(
            n_estimators=200, min_samples_leaf=3, n_jobs=-1, random_state=42
        ),
        "xgboost": xgb.XGBClassifier(
            n_estimators=600,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric="aucpr",
            early_stopping_rounds=50,
            random_state=42,
            n_jobs=-1,
        ),
        "lightgbm": lgb.LGBMClassifier(
            n_estimators=400,
            learning_rate=0.03,
            num_leaves=15,
            subsample=0.8,
            subsample_freq=1,
            colsample_bytree=0.8,
            random_state=42,
            verbose=-1,
        ),
    }
    rows = []
    for name, model in models.items():
        started = time.perf_counter()
        if name == "xgboost":
            model.fit(
                train[features],
                train["is_fraud"],
                eval_set=[(valid[features], valid["is_fraud"])],
                verbose=False,
            )
        else:
            model.fit(train[features], train["is_fraud"])
        threshold = best_f1_threshold(valid["is_fraud"], model.predict_proba(valid[features])[:, 1])
        m = evaluate(test["is_fraud"], model.predict_proba(test[features])[:, 1], threshold)
        rows.append(
            {
                "model": name,
                **{k: round(v, 4) for k, v in m.as_dict().items()},
                "secs": round(time.perf_counter() - started, 1),
            }
        )
    table = pd.DataFrame(rows)[
        ["model", "roc_auc", "pr_auc", "precision", "recall", "f1", "threshold", "secs"]
    ]
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()

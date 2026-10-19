"""Train and compare fraud models, then persist the production candidate.

    python -m ml.train

Pipeline: load labelled transactions -> chronological train/valid/test split
-> features (risk encodings fitted on train only) -> train Logistic
Regression, Random Forest, XGBoost and LightGBM -> pick each model's decision
threshold on the validation split -> report metrics on the untouched test
split -> save the XGBoost model to ``ml/artifacts/``.
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime

import lightgbm as lgb
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.dataset import load_synthetic
from ml.evaluate import Metrics, best_f1_threshold, evaluate, threshold_table
from ml.feature_pipeline import FEATURES, TARGET, RiskTables, build_features, time_split
from ml.predict import ARTIFACT_DIR, save_artifacts

XGB_PARAMS = {
    "n_estimators": 600,
    "max_depth": 5,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 2,
}


def candidate_models(seed: int, xgb_params: dict) -> dict:
    return {
        "logistic_regression": make_pipeline(
            SimpleImputer(strategy="median"), StandardScaler(), LogisticRegression(max_iter=2000)
        ),
        "random_forest": make_pipeline(
            SimpleImputer(strategy="median"),
            RandomForestClassifier(n_estimators=200, min_samples_leaf=5, n_jobs=-1, random_state=seed),
        ),
        "xgboost": xgb.XGBClassifier(
            **xgb_params,
            eval_metric="aucpr",
            early_stopping_rounds=50,
            random_state=seed,
            n_jobs=-1,
        ),
        "lightgbm": lgb.LGBMClassifier(
            n_estimators=600,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.8,
            subsample_freq=1,
            colsample_bytree=0.8,
            random_state=seed,
            verbose=-1,
        ),
    }


def prepare(df: pd.DataFrame):
    train_raw, valid_raw, test_raw = time_split(df)
    risk = RiskTables.fit(train_raw)
    # Behavioural features need each user's full history, so build them on the
    # whole timeline and split afterwards; risk tables only saw the train rows.
    features = build_features(df, risk).set_index("transaction_id")
    split = [features.loc[part["transaction_id"]] for part in (train_raw, valid_raw, test_raw)]
    return risk, *split


def fit_model(name: str, model, train: pd.DataFrame, valid: pd.DataFrame):
    X, y = train[FEATURES], train[TARGET]
    if name == "xgboost":
        model.fit(X, y, eval_set=[(valid[FEATURES], valid[TARGET])], verbose=False)
    else:
        model.fit(X, y)
    return model


def train(seed: int = 42, xgb_params: dict | None = None, verbose: bool = True) -> dict:
    """Train every candidate; returns models, metrics and the XGBoost booster."""
    xgb_params = {**XGB_PARAMS, **(xgb_params or {})}
    df = load_synthetic()
    risk, train_df, valid_df, test_df = prepare(df)
    if verbose:
        print(
            f"train {len(train_df):,} | valid {len(valid_df):,} | test {len(test_df):,} rows; "
            f"fraud rate {train_df[TARGET].mean():.3%}"
        )

    results: dict[str, dict] = {}
    for name, model in candidate_models(seed, xgb_params).items():
        started = time.perf_counter()
        fit_model(name, model, train_df, valid_df)
        threshold = best_f1_threshold(valid_df[TARGET], model.predict_proba(valid_df[FEATURES])[:, 1])
        test_scores = model.predict_proba(test_df[FEATURES])[:, 1]
        metrics = evaluate(test_df[TARGET], test_scores, threshold)
        results[name] = {
            "model": model,
            "metrics": metrics,
            "scores": test_scores,
            "seconds": time.perf_counter() - started,
        }

    xgb_model = results["xgboost"]["model"]
    booster = xgb_model.get_booster()[: xgb_model.best_iteration + 1]
    return {
        "results": results,
        "risk": risk,
        "booster": booster,
        "params": {**xgb_params, "best_iteration": int(xgb_model.best_iteration)},
        "test": test_df,
        "train_rows": len(train_df),
    }


def print_report(run: dict) -> None:
    results = run["results"]
    print("\nModel comparison (test split, threshold tuned for F1 on validation)\n")
    print(f"{'model':<22}{'ROC-AUC':>9}{'PR-AUC':>9}{'Prec':>8}{'Recall':>8}{'F1':>8}{'thr':>7}{'secs':>7}")
    for name, r in results.items():
        m: Metrics = r["metrics"]
        print(
            f"{name:<22}{m.roc_auc:>9.4f}{m.pr_auc:>9.4f}{m.precision:>8.3f}{m.recall:>8.3f}"
            f"{m.f1:>8.3f}{m.threshold:>7.2f}{r['seconds']:>7.1f}"
        )
    best = results["xgboost"]["metrics"]
    print("\nModel: XGBoost\n")
    for key in ("roc_auc", "pr_auc", "precision", "recall", "f1"):
        print(f"{key.upper().replace('_', '-'):>10}: {getattr(best, key):.4f}")
    print("\n" + best.confusion_matrix_text())
    test = run["test"]
    print("\nOperating points (XGBoost, test split; cost = 5 EUR per review + missed fraud amount)")
    for row in threshold_table(test[TARGET], results["xgboost"]["scores"], test["amount"]):
        print(
            f"  thr {row['threshold']:.1f}: precision {row['precision']:.3f} recall {row['recall']:.3f}"
            f"  alerts {row['alerts']:>5,}  missed {row['missed_fraud']:>4}"
            f"  cost EUR {row['cost_eur']:>10,.0f}"
        )


def build_spec(run: dict, version: str = "local") -> dict:
    metrics: Metrics = run["results"]["xgboost"]["metrics"]
    return {
        "model_type": "xgboost",
        "version": version,
        "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "features": FEATURES,
        "threshold": metrics.threshold,
        "risk_tables": run["risk"].to_dict(),
        "params": run["params"],
        "metrics": metrics.as_dict(),
        "train_rows": run["train_rows"],
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Train fraud models")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    np.random.seed(args.seed)
    run = train(seed=args.seed)
    print_report(run)
    save_artifacts(run["booster"], build_spec(run), ARTIFACT_DIR)
    print(f"\nmodel saved -> {ARTIFACT_DIR.relative_to(ARTIFACT_DIR.parents[1])}")


if __name__ == "__main__":
    main()

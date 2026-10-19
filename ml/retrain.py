"""Batch retraining steps, orchestrated by Airflow (``airflow/dags/retrain_fraud_model.py``).

Each step is a CLI sub-command that reads and writes files in a run
directory, so every Airflow task is independently retryable and inspectable:

    python -m ml.retrain validate        --run-dir RUN
    python -m ml.retrain extract         --run-dir RUN
    python -m ml.retrain features        --run-dir RUN
    python -m ml.retrain train           --run-dir RUN
    python -m ml.retrain evaluate        --run-dir RUN
    python -m ml.retrain promote         --run-dir RUN
    python -m ml.retrain pipeline        --run-dir RUN   # all of the above in one process

Label feed: in production the newest labels come from chargebacks and analyst
decisions with a delay of days to weeks. Here that feed is **simulated** — the
transaction simulator generates a freshly labelled week of history for each
run (seeded by the run date) on top of the base dataset. Analyst decisions on
alerts from PostgreSQL are reported alongside.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import zlib
from pathlib import Path

import pandas as pd

from ml.dataset import SYNTHETIC_PATH, build_synthetic
from ml.evaluate import evaluate
from ml.feature_pipeline import TARGET, RiskTables
from ml.predict import FraudModel

REQUIRED_COLUMNS = {
    "transaction_id",
    "timestamp",
    "user_id",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "device_id",
    "is_fraud",
}
MIN_ROWS = 50_000
FRAUD_RATE_RANGE = (0.001, 0.05)
# A candidate must beat the champion's PR-AUC by this margin to be promoted.
PROMOTION_MARGIN = float(os.getenv("PROMOTION_MARGIN", "0.005"))


def _write(run_dir: Path, name: str, payload: dict) -> None:
    (run_dir / name).write_text(json.dumps(payload, indent=2, default=str))
    print(json.dumps(payload, indent=2, default=str))


def _read(run_dir: Path, name: str) -> dict:
    return json.loads((run_dir / name).read_text())


def _label_feed(run_dir: Path, days: float, start=None) -> pd.DataFrame:
    """Freshly labelled transactions for this run (simulated label feed): the same
    card-holder population as the base data, a new stretch of their history."""
    feed = build_synthetic(days=days, n_users=600, start=start, event_seed=zlib.crc32(run_dir.name.encode()))
    feed["transaction_id"] = "r" + feed["transaction_id"]  # disjoint from the base ids
    return feed


# -- steps ------------------------------------------------------------------- #


def validate(run_dir: Path, base: Path, feed_days: float) -> None:
    """Data-quality gate on the training inputs; raises (fails the task) on violation."""
    base_df = pd.read_parquet(base)
    feed = _label_feed(run_dir, feed_days, base_df["timestamp"].max().to_pydatetime())
    frames = {"base": base_df, "label_feed": feed}
    report, problems = {}, []
    for name, df in frames.items():
        missing = REQUIRED_COLUMNS - set(df.columns)
        stats = {
            "rows": len(df),
            "missing_columns": sorted(missing),
            "fraud_rate": float(df[TARGET].mean()) if TARGET in df else None,
            "null_rate_max": float(df[list(REQUIRED_COLUMNS - missing)].isna().mean().max()),
            "non_positive_amounts": int((df["amount"] <= 0).sum()),
            "duplicate_ids": int(df["transaction_id"].duplicated().sum()),
        }
        report[name] = stats
        if missing:
            problems.append(f"{name}: missing columns {sorted(missing)}")
        if stats["rows"] < MIN_ROWS:
            problems.append(f"{name}: only {stats['rows']} rows")
        if not FRAUD_RATE_RANGE[0] <= stats["fraud_rate"] <= FRAUD_RATE_RANGE[1]:
            problems.append(f"{name}: fraud rate {stats['fraud_rate']:.4f} outside {FRAUD_RATE_RANGE}")
        if stats["null_rate_max"] > 0.01:
            problems.append(f"{name}: null rate {stats['null_rate_max']:.3f}")
        if stats["non_positive_amounts"] or stats["duplicate_ids"]:
            problems.append(f"{name}: invalid amounts or duplicate ids")
    report["analyst_feedback"] = _analyst_feedback()
    report["problems"] = problems
    _write(run_dir, "validation.json", report)
    if problems:
        raise SystemExit("data validation failed: " + "; ".join(problems))


def _analyst_feedback() -> dict:
    try:
        from storage.postgres import connect

        with connect() as conn:
            rows = conn.execute("SELECT status, count(*) FROM fraud_alerts GROUP BY status").fetchall()
        return {status: n for status, n in rows}
    except Exception as exc:  # the serving store is optional for training
        return {"unavailable": str(exc)[:120]}


def extract(run_dir: Path, base: Path, feed_days: float) -> None:
    """Base history + the newest labelled period, which starts where the base ends."""
    base_df = pd.read_parquet(base)
    feed = _label_feed(run_dir, feed_days, base_df["timestamp"].max().to_pydatetime())
    df = pd.concat([base_df, feed], ignore_index=True).sort_values("timestamp")
    df.to_parquet(run_dir / "training.parquet", index=False)
    _write(
        run_dir,
        "extract.json",
        {
            "rows": len(df),
            "base_rows": len(base_df),
            "feed_rows": len(feed),
            "fraud_rate": float(df[TARGET].mean()),
        },
    )


def features(run_dir: Path) -> None:
    from ml.train import prepare

    df = pd.read_parquet(run_dir / "training.parquet")
    risk, train_df, valid_df, test_df = prepare(df)
    for name, part in (("train", train_df), ("valid", valid_df), ("test", test_df)):
        part.reset_index().to_parquet(run_dir / f"{name}.parquet", index=False)
    (run_dir / "risk_tables.json").write_text(json.dumps(risk.to_dict()))
    _write(run_dir, "features.json", {"train": len(train_df), "valid": len(valid_df), "test": len(test_df)})


def _prepared(run_dir: Path):
    risk = RiskTables.from_dict(json.loads((run_dir / "risk_tables.json").read_text()))
    parts = [
        pd.read_parquet(run_dir / f"{n}.parquet").set_index("transaction_id")
        for n in ("train", "valid", "test")
    ]
    return risk, *parts


def train(run_dir: Path, tracking_uri: str | None) -> None:
    from ml.predict import save_artifacts
    from ml.train import build_spec
    from ml.train import train as train_models

    run = train_models(prepared=_prepared(run_dir), families=("logistic_regression", "xgboost"))
    spec = build_spec(run, version="candidate")
    version = None
    if tracking_uri:
        from ml.tracking import Tracker

        version = Tracker(tracking_uri).log_run(
            run,
            spec,
            run_name=f"retrain-{run_dir.name}",
            register=True,
            promote=False,
            tags={"trigger": "airflow", "run_dir": run_dir.name},
        )
        spec["version"] = version
    save_artifacts(run["booster"], spec, run_dir / "candidate")
    m = run["results"]["xgboost"]["metrics"]
    _write(run_dir, "train.json", {"version": version, "pr_auc": m.pr_auc, "f1": m.f1})


def _score(model: FraudModel, test: pd.DataFrame) -> dict:
    from ml.feature_pipeline import add_risk_features

    # Each model applies its own risk encodings and threshold.
    rows = add_risk_features(test.copy(), model.risk)
    return evaluate(rows[TARGET], model.predict_proba(rows), model.threshold).as_dict()


def evaluate_models(run_dir: Path, tracking_uri: str | None) -> None:
    """Candidate vs current champion on the same, newest test split."""
    test = pd.read_parquet(run_dir / "test.parquet")
    result = {"candidate": _score(FraudModel.load(run_dir / "candidate"), test)}
    result["candidate"]["version"] = _read(run_dir, "train.json")["version"]
    champion = None
    if tracking_uri:
        try:
            from ml.registry import download_champion

            champion = FraudModel.load(download_champion(tracking_uri, run_dir / "champion"))
        except Exception as exc:
            print(f"no champion available: {exc!s:.120}")
    if champion is not None:
        result["champion"] = {**_score(champion, test), "version": champion.version}
    cand, champ = result["candidate"], result.get("champion")
    result["promote"] = champ is None or cand["pr_auc"] >= champ["pr_auc"] + PROMOTION_MARGIN
    result["margin"] = PROMOTION_MARGIN
    _write(run_dir, "evaluation.json", result)


def promote(run_dir: Path, tracking_uri: str | None) -> None:
    evaluation = _read(run_dir, "evaluation.json")
    version = evaluation["candidate"]["version"]
    if not tracking_uri or version is None:
        raise SystemExit("no registry configured; nothing to promote")
    from ml.tracking import Tracker

    Tracker(tracking_uri).promote(str(version))
    _write(
        run_dir,
        "promotion.json",
        {"promoted_version": version, "previous": evaluation.get("champion", {}).get("version")},
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Retraining pipeline steps")
    parser.add_argument(
        "step", choices=["validate", "extract", "features", "train", "evaluate", "promote", "pipeline"]
    )
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--base", type=Path, default=Path(os.getenv("BASE_DATASET", str(SYNTHETIC_PATH))))
    parser.add_argument("--feed-days", type=float, default=1.0)
    parser.add_argument("--tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI"))
    args = parser.parse_args(argv)
    args.run_dir.mkdir(parents=True, exist_ok=True)

    if args.step == "pipeline":  # every step in one process (scheduled container, no Airflow)
        if not args.base.exists():
            from ml.dataset import main as build_dataset

            build_dataset(["--out", str(args.base)])
        validate(args.run_dir, args.base, args.feed_days)
        extract(args.run_dir, args.base, args.feed_days)
        features(args.run_dir)
        train(args.run_dir, args.tracking_uri)
        evaluate_models(args.run_dir, args.tracking_uri)
        if _read(args.run_dir, "evaluation.json")["promote"]:
            promote(args.run_dir, args.tracking_uri)
    elif args.step == "validate":
        validate(args.run_dir, args.base, args.feed_days)
    elif args.step == "extract":
        extract(args.run_dir, args.base, args.feed_days)
    elif args.step == "features":
        features(args.run_dir)
    elif args.step == "train":
        train(args.run_dir, args.tracking_uri)
    elif args.step == "evaluate":
        evaluate_models(args.run_dir, args.tracking_uri)
    else:
        promote(args.run_dir, args.tracking_uri)


if __name__ == "__main__":
    sys.exit(main())

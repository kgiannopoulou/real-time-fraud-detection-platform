"""Build the labelled training datasets.

Two sources are supported:

* **synthetic** (default) — the transaction simulator in
  ``producer/transaction_generator.py``. It produces the rich, event-level
  schema (user, merchant, device, country, timestamp) that the streaming
  platform works with, plus a ground-truth label per transaction.
* **public** — the ULB/Worldline credit-card fraud dataset (284,807 European
  card transactions, 492 frauds, PCA-anonymised features V1..V28), fetched
  from OpenML. It is used as an external benchmark for the model families;
  its anonymised features cannot drive behavioural feature engineering.

    python -m ml.dataset                    # synthetic -> data/raw/transactions.parquet
    python -m ml.dataset --source public    # OpenML 1597 -> data/raw/creditcard.parquet
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from producer.transaction_generator import GeneratorConfig, TransactionGenerator

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
SYNTHETIC_PATH = RAW_DIR / "transactions.parquet"
PUBLIC_PATH = RAW_DIR / "creditcard.parquet"
OPENML_CREDITCARD_ID = 1597


def build_synthetic(
    days: float = 3.0, n_users: int = 600, seed: int = 42, amount_drift: float = 1.0
) -> pd.DataFrame:
    cfg = GeneratorConfig(n_users=n_users, amount_drift=amount_drift)
    rows = TransactionGenerator(seed=seed, config=cfg).generate_history(days=days)
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    return df


def load_synthetic(path: Path = SYNTHETIC_PATH) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found — run `python -m ml.dataset` first")
    return pd.read_parquet(path)


def load_public(path: Path = PUBLIC_PATH) -> pd.DataFrame:
    """Load (and cache) the public credit-card fraud dataset from OpenML."""
    if path.exists():
        return pd.read_parquet(path)
    from sklearn.datasets import fetch_openml

    bunch = fetch_openml(data_id=OPENML_CREDITCARD_ID, as_frame=True, parser="auto")
    df = bunch.frame.rename(columns={"Class": "is_fraud"})
    df["is_fraud"] = df["is_fraud"].astype(int)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--source", choices=["synthetic", "public"], default="synthetic")
    parser.add_argument("--days", type=float, default=3.0)
    parser.add_argument("--users", type=int, default=600)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    started = time.perf_counter()
    if args.source == "public":
        df = load_public(args.out or PUBLIC_PATH)
        out = args.out or PUBLIC_PATH
    else:
        df = build_synthetic(days=args.days, n_users=args.users, seed=args.seed)
        out = args.out or SYNTHETIC_PATH
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
    print(
        f"{len(df):,} transactions, {int(df['is_fraud'].sum()):,} fraudulent "
        f"({df['is_fraud'].mean():.3%}) -> {out.relative_to(ROOT)} "
        f"[{time.perf_counter() - started:.1f}s]"
    )


if __name__ == "__main__":
    main()

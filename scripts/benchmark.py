"""Throughput / latency benchmark against the running docker compose stack.

    python scripts/benchmark.py --rates 50 100 200 --seconds 90

For each target rate, transactions are published to Kafka at that rate
(stamped with the send time) and the script measures, from the serving store:

* sustained scoring throughput (transactions scored per second);
* end-to-end latency = Kafka ingest timestamp -> scored (p50 / p95 / p99);
* whether the stream kept up (everything sent was scored within the window).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg  # noqa: E402

from producer.producer import TransactionProducer  # noqa: E402
from producer.transaction_generator import (  # noqa: E402
    GeneratorConfig,
    TransactionGenerator,
    strip_labels,
    users_for_rate,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud_local_pw@localhost:5436/fraud")


def run(rate: float, seconds: float, conn) -> dict:
    tag = f"bench{int(time.time())}r{int(rate)}"
    # Population capped to keep the generator light; per-card activity just gets busier.
    gen = TransactionGenerator(seed=5, config=GeneratorConfig(n_users=min(users_for_rate(rate), 20_000)))
    events = gen.stream(realtime=False)
    producer = TransactionProducer(BOOTSTRAP, "transactions")

    sent, started = 0, time.monotonic()
    while (elapsed := time.monotonic() - started) < seconds:
        due = int(elapsed * rate)
        while sent < due:
            tx = strip_labels(next(events))
            tx["transaction_id"] = f"tx_{tag}_{sent}"
            tx["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            producer.send(tx)
            sent += 1
        time.sleep(0.005)
    producer.flush(30)

    pattern = f"tx_{tag}_%"
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        scored = conn.execute(
            "SELECT count(*) FROM fraud_predictions WHERE transaction_id LIKE %s", (pattern,)
        ).fetchone()[0]
        if scored >= sent:
            break
        time.sleep(2)
    row = conn.execute(
        """SELECT count(*),
                  extract(epoch FROM max(scored_at) - min(scored_at)),
                  percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms),
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms),
                  percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)
           FROM fraud_predictions WHERE transaction_id LIKE %s""",
        (pattern,),
    ).fetchone()
    count, span, p50, p95, p99 = row
    return {
        "target_rate": rate,
        "sent": sent,
        "scored": count,
        "throughput": count / float(span) if span else float("nan"),
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--rates", type=float, nargs="+", default=[50, 100, 200])
    parser.add_argument("--seconds", type=float, default=90)
    args = parser.parse_args()

    results = []
    with psycopg.connect(DSN, autocommit=True) as conn:
        for rate in args.rates:
            print(f"running {rate:g} tx/s for {args.seconds:g}s ...", flush=True)
            results.append(run(rate, args.seconds, conn))
            time.sleep(10)  # let the stream drain between runs

    print("\n| Target rate | Sent | Scored | Sustained throughput | p50 | p95 | p99 |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for r in results:
        print(
            f"| {r['target_rate']:g} tx/s | {r['sent']:,} | {r['scored']:,} | {r['throughput']:.1f} tx/s "
            f"| {r['p50_ms'] / 1000:.2f} s | {r['p95_ms'] / 1000:.2f} s | {r['p99_ms'] / 1000:.2f} s |"
        )


if __name__ == "__main__":
    main()

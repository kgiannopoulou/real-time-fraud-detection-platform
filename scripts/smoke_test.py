"""End-to-end smoke test against a running ``docker compose`` stack.

    python scripts/smoke_test.py

1. checks every service's health endpoint;
2. publishes a burst of transactions (plus a few invalid records) to Kafka;
3. waits until they come out the other side: predictions in PostgreSQL and
   the API, invalid records in the dead-letter queue.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg  # noqa: E402

from producer.chaos import corrupt  # noqa: E402
from producer.producer import TransactionProducer  # noqa: E402
from producer.transaction_generator import TransactionGenerator, strip_labels  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud_local_pw@localhost:5436/fraud")
HEALTH = {
    "api": "http://localhost:8000/health",
    "dashboard": "http://localhost:8501/_stcore/health",
    "mlflow": "http://localhost:5000/health",
    "spark metrics": "http://localhost:9108/metrics",
    "prometheus": "http://localhost:9090/-/ready",
    "grafana": "http://localhost:3001/api/health",
    "airflow": "http://localhost:8080/health",
}


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name} {detail}")
    if not ok:
        raise SystemExit(1)


def wait_until(predicate, timeout: float, interval: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


def http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status == 200
    except OSError:
        return False


def main(n_events: int = 300, n_invalid: int = 5) -> None:
    print("health")
    for name, url in HEALTH.items():
        check(name, wait_until(lambda u=url: http_ok(u), timeout=180) is not None, url)

    run_tag = f"smoke{int(time.time())}"
    gen = TransactionGenerator(seed=7)
    events = [strip_labels(tx) for tx, _ in zip(gen.stream(realtime=False), range(n_events), strict=False)]
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    for tx in events:  # unique ids for this run, stamped "now" so none is late or in the future
        tx["transaction_id"] = f"tx_{run_tag}_{tx['transaction_id'][3:]}"
        tx["timestamp"] = now
    invalid = [
        corrupt({**events[i], "transaction_id": f"tx_{run_tag}_bad{i}"}, "negative_amount")
        for i in range(n_invalid)
    ]

    print(f"publishing {n_events} transactions + {n_invalid} invalid records")
    producer = TransactionProducer(BOOTSTRAP, "transactions")
    for event in events + invalid:
        producer.send(event)
    check("kafka delivery", producer.flush(30) == 0)

    pattern = f"tx_{run_tag}_%"
    with psycopg.connect(DSN, autocommit=True) as conn:

        def scored() -> int:
            return conn.execute(
                "SELECT count(*) FROM fraud_predictions WHERE transaction_id LIKE %s", (pattern,)
            ).fetchone()[0]

        def dead() -> int:
            return conn.execute(
                "SELECT count(*) FROM dead_letter_transactions WHERE transaction_id LIKE %s", (pattern,)
            ).fetchone()[0]

        print("end to end")
        started = time.monotonic()
        done = wait_until(lambda: scored() >= n_events, timeout=180)
        check(
            "predictions stored",
            done is not None,
            f"({scored()}/{n_events} in {time.monotonic() - started:.1f}s)",
        )
        check(
            "invalid records dead-lettered",
            wait_until(lambda: dead() >= n_invalid, 60) is not None,
            f"({dead()}/{n_invalid})",
        )

    with urllib.request.urlopen(f"http://localhost:8000/predictions/{events[0]['transaction_id']}") as r:
        body = json.load(r)
    check(
        "API returns the prediction",
        body["transaction_id"] == events[0]["transaction_id"],
        f"(p={body['fraud_probability']:.3f}, model v{body['model_version']})",
    )
    print("smoke test passed")


if __name__ == "__main__":
    main()

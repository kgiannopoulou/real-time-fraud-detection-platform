"""Kafka producer: publishes generated transactions to the ``transactions`` topic.

    python producer/producer.py --rate 20
    python -m producer.producer --rate 50 --duration 300

Design choices:

* messages are **keyed by user_id**, so all events of a card land in the same
  partition and keep their relative order — the streaming layer computes
  per-user behavioural features;
* ``acks=all`` + idempotence: a broker retry never duplicates a message;
* every message carries a ``schema_version`` header (see
  ``schemas/transaction.v1.json``) so consumers can evolve safely.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

if __package__ in (None, ""):  # allow `python producer/producer.py`
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import KafkaException, Producer

from producer.chaos import ChaosConfig, ChaosInjector
from producer.transaction_generator import (
    GeneratorConfig,
    TransactionGenerator,
    strip_labels,
    users_for_rate,
)

SCHEMA_VERSION = b"1"


class TransactionProducer:
    def __init__(self, bootstrap: str, topic: str):
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": "transaction-producer",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            print(f"delivery failed for {msg.key()}: {err}", file=sys.stderr)
        else:
            self.delivered += 1

    def send(self, event: dict | str) -> None:
        """Publish one event; a ``str`` is sent verbatim (used to inject malformed JSON)."""
        if isinstance(event, str):
            payload, key = event.encode(), b""
        else:
            payload, key = (
                json.dumps(event, separators=(",", ":")).encode(),
                str(event.get("user_id", "")).encode(),
            )
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=key,
                    value=payload,
                    headers={"schema_version": SCHEMA_VERSION},
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:  # local queue full: let it drain
                self.producer.poll(0.5)
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self.producer.flush(timeout)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic transactions to Kafka")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("TRANSACTIONS_TOPIC", "transactions"))
    parser.add_argument("--rate", type=float, default=float(os.getenv("GENERATOR_RATE", "20")))
    parser.add_argument(
        "--seed", type=int, default=int(os.getenv("GENERATOR_SEED", "42")), help="population seed"
    )
    parser.add_argument("--duration", type=float, default=0, help="seconds to run (0 = forever)")
    parser.add_argument("--limit", type=int, default=0, help="stop after N events (0 = no limit)")
    parser.add_argument(
        "--quiet", action="store_true", help="print a summary every 5 s instead of every event"
    )
    parser.add_argument("--late-rate", type=float, default=0.0, help="share of events delayed 2-N s")
    parser.add_argument("--max-late-seconds", type=float, default=60.0)
    parser.add_argument(
        "--very-late-rate", type=float, default=0.0, help="share delayed 3-5 min (beyond allowed lateness)"
    )
    parser.add_argument("--amount-drift", type=float, default=1.0, help="multiply amounts (drift demo)")
    parser.add_argument("--metrics-port", type=int, default=0, help="expose Prometheus metrics (0 = off)")
    parser.add_argument("--duplicate-rate", type=float, default=0.0, help="share of events sent twice")
    parser.add_argument("--invalid-rate", type=float, default=0.0, help="share of events corrupted")
    args = parser.parse_args(argv)

    chaos = ChaosInjector(
        ChaosConfig(
            late_rate=args.late_rate,
            max_late_seconds=args.max_late_seconds,
            very_late_rate=args.very_late_rate,
            duplicate_rate=args.duplicate_rate,
            invalid_rate=args.invalid_rate,
        )
    )
    gen = TransactionGenerator(
        seed=args.seed,
        config=GeneratorConfig(n_users=users_for_rate(args.rate), amount_drift=args.amount_drift),
    )
    produced = None
    if args.metrics_port:
        from prometheus_client import Counter, start_http_server

        start_http_server(args.metrics_port)
        produced = Counter("fraud_producer_events_total", "Events published", ["kind"])
    producer = TransactionProducer(args.bootstrap, args.topic)
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    started = last_report = time.monotonic()
    sent = 0
    print(f"producing ~{args.rate:g} tx/s to {args.topic} @ {args.bootstrap} ({len(gen.users):,} cards)")
    try:
        for tx in gen.stream():
            now = time.monotonic()
            for event in chaos.process(strip_labels(tx), now):
                producer.send(event)
                sent += 1
                if produced is not None:
                    produced.labels("raw" if isinstance(event, str) else "json").inc()
                if not args.quiet:
                    tx_id = event.get("transaction_id", "?") if isinstance(event, dict) else "<malformed>"
                    print(f"produced transaction {tx_id}")
            if args.quiet and now - last_report >= 5:
                print(
                    f"sent={sent:,} delivered={producer.delivered:,} failed={producer.failed} "
                    f"rate={sent / (now - started):.1f}/s chaos={chaos.stats}"
                )
                last_report = now
            if (
                stop
                or (args.limit and sent >= args.limit)
                or (args.duration and now - started >= args.duration)
            ):
                break
    except KafkaException as exc:
        print(f"kafka error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        for event in chaos.drain():  # publish held-back events before exiting
            producer.send(event)
            sent += 1
        remaining = producer.flush()
        print(
            f"done: sent={sent:,} delivered={producer.delivered:,} failed={producer.failed} "
            f"undelivered={remaining}"
        )


if __name__ == "__main__":
    main()

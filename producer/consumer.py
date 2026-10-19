"""Debug consumer: print events from a Kafka topic.

python producer/consumer.py                         # transactions
python producer/consumer.py --topic fraud_predictions --from-beginning
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import uuid

from confluent_kafka import Consumer, KafkaError

LABELS = {
    "transactions": "transaction",
    "fraud_predictions": "prediction",
    "fraud_alerts": "alert",
    "dead_letter_transactions": "dead letter",
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Print events from a Kafka topic")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"))
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true", help="print full payloads")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            # A throw-away group: this tool should never steal partitions or
            # commit offsets on behalf of a real consumer.
            "group.id": f"debug-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    running = True

    def _stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    label = LABELS.get(args.topic, "event")
    received = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"error: {msg.error()}")
                continue
            received += 1
            raw = msg.value().decode(errors="replace")
            if args.verbose:
                print(f"[p{msg.partition()} @{msg.offset()}] {raw}")
            else:
                try:
                    event_id = json.loads(raw).get("transaction_id", "?")
                except (json.JSONDecodeError, AttributeError):
                    event_id = "<unparseable>"
                print(f"received {label} {event_id}")
            if args.limit and received >= args.limit:
                break
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

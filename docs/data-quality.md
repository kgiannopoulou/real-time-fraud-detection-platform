# Data quality and the dead-letter queue

Bad records must never crash the stream, and they must never disappear
silently either.

```text
Kafka transactions
      │
      ▼
Spark: parse against explicit schema + validation rules
      │
      ├── VALID ──────────► features ─► scoring ─► predictions
      │                        │
      │                        └─ duplicate id ─┐
      ├── TOO LATE ────────────────────────────┤
      └── INVALID ─────────────────────────────┴─► dead_letter_transactions
                                                    (Kafka topic + PostgreSQL)
```

## Rules

Implemented twice with identical semantics: as Spark column expressions
(`streaming/schemas.py`) and in pure Python (`streaming/validation.py`, used
by tests to check parity).

| Rule | Reason code |
| --- | --- |
| Payload is parseable JSON matching the schema | `malformed_json` |
| `transaction_id`, `user_id`, `merchant_id` present and non-blank | `missing_<field>` |
| `amount` numeric | `missing_or_non_numeric_amount` |
| `amount > 0` | `non_positive_amount` |
| `amount <= 1,000,000` | `amount_above_limit` |
| `currency ∈ {EUR, USD, GBP, CHF}` | `unsupported_currency` |
| `country` is an ISO 3166-1 alpha-2 code | `invalid_country` |
| `timestamp` parseable as ISO-8601 | `invalid_timestamp` |
| `timestamp <= now + 5 min` (clock-skew tolerance) | `timestamp_in_future` |
| `transaction_id` unique (within the 24 h state window) | `duplicate_transaction_id` |
| Arrives within the allowed lateness (120 s) | `arrived_after_allowed_lateness` |

All violated rules are reported (a record with a negative amount *and* an
unknown currency carries both reasons); unparseable JSON reports only
`malformed_json`.

## The dead-letter queue

Each rejected record is published to the `dead_letter_transactions` topic —
so a replay tool can re-inject fixed records — and stored in PostgreSQL with
its Kafka coordinates, raw payload and reasons. The table's unique key
`(topic, partition, offset, reasons)` makes the write idempotent under
Spark's at-least-once `foreachBatch`.

The dashboard's **Data quality** tab shows accepted vs. rejected volume,
rejections by reason, late-but-accepted arrivals per minute and the most
recent dead letters.

## Try it

```bash
python producer/producer.py --rate 30 --invalid-rate 0.01 --duplicate-rate 0.01 \
    --late-rate 0.03 --very-late-rate 0.003 --quiet
python producer/consumer.py --topic dead_letter_transactions -v
```

Sample (2-minute run, 1 % invalid + 1 % duplicates):

| Reason | Records |
| --- | --- |
| duplicate_transaction_id | 42 |
| invalid_country | 7 |
| unsupported_currency | 7 |
| timestamp_in_future | 6 |
| non_positive_amount | 5 |
| missing_user_id | 5 |
| malformed_json | 4 |
| missing_or_non_numeric_amount | 3 |
| arrived_after_allowed_lateness | 1 |

The stream kept scoring throughout, with no failed batches.

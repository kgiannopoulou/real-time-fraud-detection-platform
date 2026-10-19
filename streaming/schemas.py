"""Explicit Spark schemas and validation rules for the event contracts.

Spark is never asked to infer a schema from the stream: inference needs a
sample, silently changes types when a producer misbehaves and makes the job's
behaviour depend on whatever data arrived first. The schema below mirrors
``schemas/transaction.v1.json``.

``timestamp`` is read as a string and converted with ``try_to_timestamp`` so
an unparseable value becomes a validation error for that record instead of a
failed micro-batch (Spark 4 runs with ANSI mode on).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from producer.transaction_generator import SUPPORTED_CURRENCIES
from streaming.contract import ISO_COUNTRIES, MAX_AMOUNT, MAX_CLOCK_SKEW_SECONDS

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=False),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("device_id", StringType(), nullable=True),
    ]
)


def parse_value(value: str = "value") -> Column:
    """Kafka ``value`` bytes parsed against the explicit schema (PERMISSIVE:
    malformed JSON yields nulls instead of failing the batch)."""
    return F.from_json(F.col(value).cast("string"), TRANSACTION_SCHEMA, {"mode": "PERMISSIVE"})


def event_time(col: Column) -> Column:
    """ISO-8601 string -> timestamp; NULL instead of an error when unparseable."""
    return F.try_to_timestamp(col)


def _blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def validation_errors(tx: str = "tx") -> Column:
    """Array of human-readable rule violations for the parsed struct column ``tx``.

    An empty array means the record is valid.
    """

    def c(name: str) -> Column:
        return F.col(f"{tx}.{name}")

    ts = event_time(c("timestamp"))
    all_null = F.coalesce(*[c(f.name).cast("string") for f in TRANSACTION_SCHEMA.fields]).isNull()
    rules: list[tuple[Column, str]] = [
        (F.col(tx).isNull() | all_null, "malformed_json"),
        (_blank(c("transaction_id")), "missing_transaction_id"),
        (_blank(c("user_id")), "missing_user_id"),
        (_blank(c("merchant_id")), "missing_merchant_id"),
        (c("amount").isNull(), "missing_or_non_numeric_amount"),
        (c("amount") <= 0, "non_positive_amount"),
        (c("amount") > MAX_AMOUNT, "amount_above_limit"),
        (~c("currency").isin(*SUPPORTED_CURRENCIES) | c("currency").isNull(), "unsupported_currency"),
        (~c("country").isin(*sorted(ISO_COUNTRIES)) | c("country").isNull(), "invalid_country"),
        (ts.isNull(), "invalid_timestamp"),
        (
            ts > F.current_timestamp() + F.expr(f"INTERVAL {MAX_CLOCK_SKEW_SECONDS} SECONDS"),
            "timestamp_in_future",
        ),
    ]
    malformed, field_rules = rules[0][0], rules[1:]
    checks = [F.when(condition, F.lit(message)) for condition, message in field_rules]
    # Unparseable JSON is reported as such, not as every field missing.
    return F.when(malformed, F.array(F.lit("malformed_json"))).otherwise(
        F.filter(F.array(*checks), lambda x: x.isNotNull())
    )

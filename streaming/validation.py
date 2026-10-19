"""Pure-Python mirror of the streaming validation rules.

``streaming/schemas.py`` expresses the rules as Spark column expressions; this
module expresses the same rules over a plain ``dict`` so they can be unit
tested quickly, reused outside Spark, and checked for parity with the Spark
implementation (``tests/test_streaming.py``).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from producer.transaction_generator import SUPPORTED_CURRENCIES
from streaming.contract import ISO_COUNTRIES, MAX_AMOUNT, MAX_CLOCK_SKEW_SECONDS

FIELDS = (
    "transaction_id",
    "timestamp",
    "user_id",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "device_id",
)


def _blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _parse_ts(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def validate_payload(raw: str | bytes, now: datetime | None = None) -> list[str]:
    """Rule violations of one raw Kafka value; empty list = valid."""
    try:
        record = json.loads(raw)
    except (ValueError, TypeError):
        return ["malformed_json"]
    if not isinstance(record, dict) or all(record.get(f) is None for f in FIELDS):
        return ["malformed_json"]
    return validate_record(record, now)


def validate_record(record: dict, now: datetime | None = None) -> list[str]:
    now = now or datetime.now(timezone.utc)
    errors = []
    for field in ("transaction_id", "user_id", "merchant_id"):
        if _blank(record.get(field)):
            errors.append(f"missing_{field}")
    amount = record.get("amount")
    if not isinstance(amount, (int, float)) or isinstance(amount, bool):
        errors.append("missing_or_non_numeric_amount")
    elif amount <= 0:
        errors.append("non_positive_amount")
    elif amount > MAX_AMOUNT:
        errors.append("amount_above_limit")
    if record.get("currency") not in SUPPORTED_CURRENCIES:
        errors.append("unsupported_currency")
    if record.get("country") not in ISO_COUNTRIES:
        errors.append("invalid_country")
    ts = _parse_ts(record.get("timestamp"))
    if ts is None:
        errors.append("invalid_timestamp")
    elif ts > now + timedelta(seconds=MAX_CLOCK_SKEW_SECONDS):
        errors.append("timestamp_in_future")
    return errors

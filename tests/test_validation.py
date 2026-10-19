from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from streaming.validation import validate_payload, validate_record

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
VALID = {
    "transaction_id": "tx_abc123",
    "timestamp": "2026-10-19T11:59:00.000Z",
    "user_id": "user_1",
    "merchant_id": "merchant_9",
    "merchant_category": "grocery",
    "amount": 42.5,
    "currency": "EUR",
    "country": "GR",
    "device_id": "device_1",
}


def test_valid_record_has_no_errors():
    assert validate_record(VALID, NOW) == []
    assert validate_payload(json.dumps(VALID), NOW) == []


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"transaction_id": None}, "missing_transaction_id"),
        ({"user_id": "  "}, "missing_user_id"),
        ({"merchant_id": None}, "missing_merchant_id"),
        ({"amount": "12,50"}, "missing_or_non_numeric_amount"),
        ({"amount": 0}, "non_positive_amount"),
        ({"amount": -3}, "non_positive_amount"),
        ({"amount": 5_000_000}, "amount_above_limit"),
        ({"currency": "XYZ"}, "unsupported_currency"),
        ({"country": "Greece"}, "invalid_country"),
        ({"country": "XX"}, "invalid_country"),
        ({"timestamp": "yesterday"}, "invalid_timestamp"),
        ({"timestamp": "2026-10-20T12:00:00Z"}, "timestamp_in_future"),
    ],
)
def test_each_rule(change, expected):
    assert validate_record({**VALID, **change}, NOW) == [expected]


def test_small_clock_skew_is_tolerated():
    assert validate_record({**VALID, "timestamp": "2026-10-19T12:04:00Z"}, NOW) == []


def test_multiple_violations_are_all_reported():
    errors = validate_record({**VALID, "amount": -1, "currency": "XYZ"}, NOW)
    assert errors == ["non_positive_amount", "unsupported_currency"]


@pytest.mark.parametrize("raw", ['{"transaction_id": "tx_1", "amount"', "not json", "[]", "{}", "null"])
def test_unparseable_payloads_report_only_malformed_json(raw):
    assert validate_payload(raw, NOW) == ["malformed_json"]

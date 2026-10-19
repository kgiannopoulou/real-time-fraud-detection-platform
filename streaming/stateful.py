"""Stateful per-user feature computation for Spark Structured Streaming.

``applyInPandasWithState`` groups each micro-batch by ``user_id`` and hands
this module one user's new events together with that user's state from the
previous batches. The state is the JSON-serialized
:class:`streaming.features.UserState` — the exact class used to build the
training set.
"""

from __future__ import annotations

import math
from collections.abc import Iterator

import pandas as pd
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.features import BEHAVIOURAL_FEATURES, UserState

PASSTHROUGH_FIELDS = [
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("device_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("amount_eur", DoubleType()),
    StructField("hour_of_day", IntegerType()),
]
PASSTHROUGH = [f.name for f in PASSTHROUGH_FIELDS]

FEATURES_SCHEMA = StructType(
    [
        *PASSTHROUGH_FIELDS,
        StructField("is_duplicate", BooleanType()),
        *[StructField(name, DoubleType()) for name in BEHAVIOURAL_FEATURES],
    ]
)
OUTPUT_COLUMNS = [f.name for f in FEATURES_SCHEMA.fields]
STATE_SCHEMA = StructType([StructField("history", StringType())])

_NAN_FEATURES = dict.fromkeys(BEHAVIOURAL_FEATURES, math.nan)


def compute_user_features(
    key: tuple, batches: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    """Score-ready behavioural features for one user's events in this batch."""
    events = pd.concat(list(batches), ignore_index=True)
    history = UserState.from_json(state.get[0] if state.exists else None)

    rows = []
    for rec in events.sort_values(["event_time", "transaction_id"]).itertuples(index=False):
        if history.has_seen(rec.transaction_id):  # replayed / duplicated event
            rows.append({**rec._asdict(), "is_duplicate": True, **_NAN_FEATURES})
            continue
        feats = history.observe(
            pd.Timestamp(rec.event_time).timestamp(),
            rec.transaction_id,
            float(rec.amount_eur),
            rec.merchant_id,
            rec.country,
            rec.device_id,
        )
        rows.append({**rec._asdict(), "is_duplicate": False, **feats})

    state.update((history.to_json(),))
    yield pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

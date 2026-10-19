"""Spark tests: the real streaming functions on a local SparkSession.

Kafka is replaced by a file source (same columns as the Kafka source), so the
parsing, validation, stateful feature and scoring logic run exactly as in
production, including state carried across micro-batches via checkpoints.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from producer.chaos import CORRUPTIONS, corrupt
from streaming.validation import validate_payload

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("fraud-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _event(i: int, user: str = "user_1", amount: float = 20.0, seconds_ago: float = 0.0, **extra) -> dict:
    ts = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    return {
        "transaction_id": f"tx_{user}_{i}",
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "user_id": user,
        "merchant_id": f"merchant_{i % 3}",
        "merchant_category": "grocery",
        "amount": amount,
        "currency": "EUR",
        "country": "GR",
        "device_id": "device_1",
        **extra,
    }


def _kafka_like(spark, payloads: list[str]):
    from pyspark.sql import functions as F

    rows = [(p, i) for i, p in enumerate(payloads)]
    df = spark.createDataFrame(rows, "raw string, offset long")
    return df.select(
        F.col("raw").cast("binary").alias("value"),
        F.current_timestamp().alias("timestamp"),
        F.lit(0).alias("partition"),
        F.col("offset"),
    )


def test_spark_validation_matches_the_python_rules(spark):
    from streaming.fraud_stream import parse

    good = _event(1)
    payloads = [json.dumps(good)]
    for kind in CORRUPTIONS:
        bad = corrupt(good, kind)
        payloads.append(bad if isinstance(bad, str) else json.dumps(bad))
    payloads += [json.dumps({**good, "amount": -1, "currency": "XYZ"}), "not json"]

    rows = parse(_kafka_like(spark, payloads)).select("raw_value", "errors", "is_valid").collect()
    assert len(rows) == len(payloads)
    for row in rows:
        assert sorted(row["errors"]) == sorted(validate_payload(row["raw_value"])), row["raw_value"]
        assert row["is_valid"] == (not row["errors"])


def test_enrich_normalises_currency(spark):
    from streaming.fraud_stream import enrich, parse

    payload = json.dumps(_event(1, amount=100.0, currency="USD", country="US"))
    row = enrich(parse(_kafka_like(spark, [payload]))).collect()[0]
    assert row["amount_eur"] == pytest.approx(92.0)
    assert row["arrival_delay_s"] < 60


def _run_stream(spark, source_dir, checkpoint, cfg, broadcaster=None):
    """One availableNow run of parse -> enrich -> stateful features (-> scoring)."""
    from pyspark.sql import functions as F

    from streaming.fraud_stream import enrich, parse, split_by_lateness, user_features
    from streaming.scoring import score

    raw = (
        spark.readStream.format("text")
        .load(str(source_dir))
        .select(
            F.col("value").cast("binary").alias("value"),
            F.current_timestamp().alias("timestamp"),
            F.lit(0).alias("partition"),
            F.lit(0).cast("long").alias("offset"),
        )
    )
    on_time, _ = split_by_lateness(enrich(parse(raw)), cfg)
    collected: list = []

    def collect(batch, _batch_id):
        frame = score(batch, broadcaster) if broadcaster else batch
        collected.extend(frame.collect())

    query = (
        user_features(on_time, cfg)
        .writeStream.foreachBatch(collect)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return collected


def test_stateful_features_carry_across_micro_batches(spark, tmp_path, trained):
    from streaming.fraud_stream import StreamConfig
    from streaming.scoring import ModelBroadcaster

    cfg = StreamConfig(watermark="10 minutes", allowed_lateness_s=600)
    source, checkpoint = tmp_path / "in", tmp_path / "chk"
    source.mkdir()

    first = [_event(i, seconds_ago=300 - i * 30, amount=20.0) for i in range(4)]
    (source / "batch1.json").write_text("\n".join(json.dumps(e) for e in first))
    rows = _run_stream(spark, source, checkpoint, cfg)
    by_id = {r["transaction_id"]: r for r in rows}
    assert len(rows) == 4
    assert by_id["tx_user_1_0"]["tx_count_24h"] == 0
    assert by_id["tx_user_1_3"]["tx_count_24h"] == 3

    # Second run: state restored from the checkpoint; a duplicate is flagged and
    # a large amount is compared with the user's average from the first run.
    second = [first[1], _event(9, seconds_ago=0, amount=2000.0)]
    (source / "batch2.json").write_text("\n".join(json.dumps(e) for e in second))
    broadcaster = ModelBroadcaster(spark, tracking_uri=None, fallback_dir=trained["dir"])
    raw_rows = _run_stream(spark, source, checkpoint, cfg)
    assert {r["transaction_id"]: r["is_duplicate"] for r in raw_rows} == {
        "tx_user_1_1": True,
        "tx_user_1_9": False,
    }
    big = next(r for r in raw_rows if r["transaction_id"] == "tx_user_1_9")
    assert big["tx_count_24h"] == 4
    assert big["amount_ratio"] == pytest.approx(100.0)

    # Scoring on the same features: duplicates are excluded, scores explained.
    (source / "batch3.json").write_text(json.dumps(_event(10, seconds_ago=0, amount=3000.0)))
    scored = _run_stream(spark, source, checkpoint, cfg, broadcaster)
    assert len(scored) == 1
    row = scored[0]
    assert 0 <= row["fraud_probability"] <= 1
    assert row["prediction"] in {"FRAUD", "LEGIT"}
    assert row["model_version"] == "test"
    assert isinstance(row["risk_factors"], list)

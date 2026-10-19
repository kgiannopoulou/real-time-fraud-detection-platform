"""Spark Structured Streaming job: Kafka -> validation -> enrichment -> sinks.

    spark-submit streaming/fraud_stream.py          # inside the spark container

Queries started by this job:

* ``lake``  — every valid, enriched transaction appended to the Parquet data
  lake, partitioned by event date (the history used for analytics/retraining);
* ``scoring`` — per-user behavioural features computed with
  ``applyInPandasWithState`` (one ``UserState`` per card in the state store),
  scored by the champion model and published to ``fraud_predictions`` (plus
  the lake) from ``foreachBatch``;
* ``window_stats`` — per-minute event-time statistics with a watermark;
* ``late_events`` — events that arrived later than the allowed lateness
  (side output for reconciliation instead of a silent drop).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from ml.feature_pipeline import FEATURES
from producer.transaction_generator import FX_TO_EUR
from streaming.schemas import event_time, parse_value, validation_errors
from streaming.scoring import ModelBroadcaster, latency_summary, model_settings, score
from streaming.stateful import FEATURES_SCHEMA, PASSTHROUGH, STATE_SCHEMA, compute_user_features


@dataclass(frozen=True)
class StreamConfig:
    bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    transactions_topic: str = field(default_factory=lambda: os.getenv("TRANSACTIONS_TOPIC", "transactions"))
    predictions_topic: str = field(
        default_factory=lambda: os.getenv("FRAUD_PREDICTIONS_TOPIC", "fraud_predictions")
    )
    starting_offsets: str = field(default_factory=lambda: os.getenv("STARTING_OFFSETS", "latest"))
    max_offsets_per_trigger: int = field(
        default_factory=lambda: int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
    )
    lake_path: str = field(default_factory=lambda: os.getenv("LAKE_PATH", "/data/lake"))
    checkpoint_root: str = field(default_factory=lambda: os.getenv("CHECKPOINT_ROOT", "/data/checkpoints"))
    trigger: str = field(default_factory=lambda: os.getenv("TRIGGER_INTERVAL", "2 seconds"))
    lake_trigger: str = field(default_factory=lambda: os.getenv("LAKE_TRIGGER_INTERVAL", "30 seconds"))
    # How far behind the newest event time an event may arrive and still be
    # processed by the stateful operators (and how long windows stay open).
    watermark: str = field(default_factory=lambda: os.getenv("WATERMARK_DELAY", "2 minutes"))
    allowed_lateness_s: int = field(default_factory=lambda: int(os.getenv("ALLOWED_LATENESS_SECONDS", "120")))

    def checkpoint(self, query: str) -> str:
        return f"{self.checkpoint_root}/{query}"


def build_spark(app_name: str = "fraud-stream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # The default of 200 shuffle partitions means 200 state stores per
        # stateful operator; a handful matches the topic's 6 partitions.
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .getOrCreate()
    )


def read_transactions(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", cfg.transactions_topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse(raw: DataFrame) -> DataFrame:
    """Deserialize JSON against the explicit schema and attach rule violations."""
    return (
        raw.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            parse_value().alias("tx"),
        )
        .withColumn("errors", validation_errors("tx"))
        .withColumn("is_valid", F.size("errors") == 0)
    )


def fx_rate(currency_col) -> F.Column:
    mapping = F.create_map(*[x for cur, rate in FX_TO_EUR.items() for x in (F.lit(cur), F.lit(rate))])
    return F.coalesce(mapping[currency_col], F.lit(1.0))


def enrich(parsed: DataFrame) -> DataFrame:
    """Flatten valid records and add derived columns."""
    return (
        parsed.filter("is_valid")
        .select("tx.*", "kafka_timestamp")
        .withColumn("event_time", event_time(F.col("timestamp")))
        .withColumn("amount_eur", F.round(F.col("amount") * fx_rate(F.col("currency")), 2))
        .withColumn("hour_of_day", F.hour("event_time"))
        .withColumn(
            "arrival_delay_s", F.col("kafka_timestamp").cast("double") - F.col("event_time").cast("double")
        )
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("merchant_category", F.coalesce("merchant_category", F.lit("unknown")))
    )


def start_lake_sink(transactions: DataFrame, cfg: StreamConfig):
    return (
        transactions.writeStream.queryName("lake")
        .format("parquet")
        .option("path", f"{cfg.lake_path}/transactions")
        .option("checkpointLocation", cfg.checkpoint("lake"))
        .partitionBy("event_date")
        .trigger(processingTime=cfg.lake_trigger)
        .outputMode("append")
        .start()
    )


def split_by_lateness(transactions: DataFrame, cfg: StreamConfig) -> tuple[DataFrame, DataFrame]:
    """On-time events feed the stateful path; events that arrive later than the
    allowed lateness go to a side output instead of being silently dropped."""
    too_late = F.col("arrival_delay_s") > cfg.allowed_lateness_s
    return transactions.filter(~too_late), transactions.filter(too_late)


def user_features(transactions: DataFrame, cfg: StreamConfig) -> DataFrame:
    """Behavioural features from each user's history kept in the state store.

    The watermark bounds state: per-user state expires via an event-time
    timeout, and events older than the watermark are not admitted.
    """
    return (
        transactions.withWatermark("event_time", cfg.watermark)
        .select(*PASSTHROUGH)
        .groupBy("user_id")
        .applyInPandasWithState(
            compute_user_features,
            outputStructType=FEATURES_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )


PREDICTION_FIELDS = [
    "transaction_id",
    "user_id",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "amount_eur",
    "country",
    "device_id",
    "event_time",
    "fraud_probability",
    "prediction",
    "model_version",
    "risk_factors",
    "scored_at",
]


def prediction_records(scored: DataFrame) -> DataFrame:
    """Kafka records for ``fraud_predictions``: keyed by user, JSON value."""
    features = F.struct(*[F.col(name) for name in FEATURES]).alias("features")
    return scored.select(
        F.col("user_id").alias("key"),
        F.to_json(F.struct(*PREDICTION_FIELDS, features)).alias("value"),
    )


def make_batch_processor(cfg: StreamConfig, broadcaster: ModelBroadcaster):
    def process(batch: DataFrame, batch_id: int) -> None:
        broadcaster.refresh()  # pick up a newly promoted champion
        started = time.perf_counter()
        scored = score(batch, broadcaster).persist()
        try:
            n = scored.count()
            if n == 0:
                return
            (
                prediction_records(scored)
                .write.format("kafka")
                .option("kafka.bootstrap.servers", cfg.bootstrap)
                .option("topic", cfg.predictions_topic)
                .save()
            )
            # Overwriting a batch-specific directory makes the lake write
            # idempotent when Spark replays a batch after a failure.
            scored.write.mode("overwrite").parquet(f"{cfg.lake_path}/predictions/batch_id={batch_id:08d}")
            summary = scored.select("kafka_timestamp", "scored_at", "prediction").toPandas()
            frauds = int((summary["prediction"] == "FRAUD").sum())
            latency = latency_summary(summary)
            print(
                f"[batch {batch_id}] scored={n} fraud={frauds} model=v{broadcaster.version} "
                f"e2e_p95={latency.get('p95_ms', 0):.0f}ms took={time.perf_counter() - started:.2f}s",
                flush=True,
            )
        finally:
            scored.unpersist()

    return process


def start_scoring(features: DataFrame, cfg: StreamConfig, broadcaster: ModelBroadcaster):
    return (
        features.writeStream.queryName("scoring")
        .foreachBatch(make_batch_processor(cfg, broadcaster))
        .option("checkpointLocation", cfg.checkpoint("scoring"))
        .trigger(processingTime=cfg.trigger)
        .start()
    )


def window_stats(transactions: DataFrame, cfg: StreamConfig) -> DataFrame:
    """Per-minute event-time statistics. A window is emitted once (append mode)
    when the watermark passes its end, so late events within the allowed
    lateness are still counted in the right minute."""
    return (
        transactions.withWatermark("event_time", cfg.watermark)
        .groupBy(F.window("event_time", "1 minute"))
        .agg(
            F.count("*").alias("transactions"),
            F.round(F.sum("amount_eur"), 2).alias("amount_eur"),
            F.approx_count_distinct("user_id").alias("active_users"),
            F.round(F.avg("arrival_delay_s"), 3).alias("avg_arrival_delay_s"),
            F.sum(F.when(F.col("arrival_delay_s") > 1, 1).otherwise(0)).alias("late_arrivals"),
        )
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*")
        .drop("window")
    )


def start_window_stats(stats: DataFrame, cfg: StreamConfig):
    return (
        stats.writeStream.queryName("window_stats")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", cfg.checkpoint("window_stats"))
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )


def start_late_sink(late: DataFrame, cfg: StreamConfig):
    return (
        late.writeStream.queryName("late_events")
        .format("parquet")
        .option("path", f"{cfg.lake_path}/late_events")
        .option("checkpointLocation", cfg.checkpoint("late_events"))
        .partitionBy("event_date")
        .trigger(processingTime=cfg.lake_trigger)
        .outputMode("append")
        .start()
    )


def main() -> None:
    cfg = StreamConfig()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    parsed = parse(read_transactions(spark, cfg))
    transactions = enrich(parsed)

    on_time, too_late = split_by_lateness(transactions, cfg)

    start_lake_sink(transactions, cfg)
    broadcaster = ModelBroadcaster(spark, **model_settings())
    start_scoring(user_features(on_time, cfg), cfg, broadcaster)
    start_window_stats(window_stats(on_time, cfg), cfg)
    start_late_sink(too_late, cfg)
    print(f"[fraud-stream] consuming {cfg.transactions_topic} @ {cfg.bootstrap}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

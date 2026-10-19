"""Spark Structured Streaming job: Kafka -> validation -> enrichment -> sinks.

    spark-submit streaming/fraud_stream.py          # inside the spark container

Queries started by this job:

* ``lake``  — every valid, enriched transaction appended to the Parquet data
  lake, partitioned by event date (the history used for analytics/retraining);
* ``scoring`` — per-user behavioural features computed with
  ``applyInPandasWithState`` (one ``UserState`` per card in the state store),
  scored by the champion model and published from ``foreachBatch`` to Kafka
  (``fraud_predictions``, ``fraud_alerts``), PostgreSQL and the lake;
* ``window_stats`` — per-minute event-time statistics with a watermark,
  upserted into PostgreSQL;
* ``dead_letters`` — the dead-letter queue: records that failed validation
  and events that arrived later than the allowed lateness (plus duplicates
  detected in the stateful stage) go to the ``dead_letter_transactions``
  topic and table with their rejection reasons, instead of crashing the job
  or vanishing silently.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from ml.feature_pipeline import FEATURES
from ml.monitoring import DRIFT_FEATURES
from producer.kafka_config import spark_kafka_options
from producer.transaction_generator import FX_TO_EUR
from storage.postgres import (
    ensure_schema,
    record_model,
    spark_partition_writer,
    write_dead_letters,
    write_window_stats,
)
from streaming import metrics
from streaming.schemas import TRANSACTION_SCHEMA, event_time, parse_value, validation_errors
from streaming.scoring import ModelBroadcaster, latency_summary, model_settings, score
from streaming.stateful import FEATURES_SCHEMA, PASSTHROUGH, STATE_SCHEMA, compute_user_features


@dataclass(frozen=True)
class StreamConfig:
    bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    transactions_topic: str = field(default_factory=lambda: os.getenv("TRANSACTIONS_TOPIC", "transactions"))
    metrics_port: int = field(default_factory=lambda: int(os.getenv("METRICS_PORT", "9108")))
    dead_letter_topic: str = field(
        default_factory=lambda: os.getenv("DEAD_LETTER_TOPIC", "dead_letter_transactions")
    )
    alerts_topic: str = field(default_factory=lambda: os.getenv("FRAUD_ALERTS_TOPIC", "fraud_alerts"))
    postgres_dsn: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_DSN", "postgresql://fraud:fraud_local_pw@postgres:5432/fraud"
        )
    )
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

    @property
    def kafka_options(self) -> dict[str, str]:
        return spark_kafka_options(self.bootstrap)

    def checkpoint(self, query: str) -> str:
        return f"{self.checkpoint_root}/{query}"


def build_spark(app_name: str = "fraud-stream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # The default of 200 shuffle partitions means 200 state stores per
        # stateful operator; a handful matches the topic's 6 partitions.
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
        # JVM / executor metrics in Prometheus format on the Spark UI port.
        .config("spark.ui.prometheus.enabled", "true")
        .config("spark.metrics.namespace", "fraud_stream")
        .config(
            "spark.metrics.conf.*.sink.prometheusServlet.class",
            "org.apache.spark.metrics.sink.PrometheusServlet",
        )
        .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus")
        .config("spark.metrics.conf.driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .getOrCreate()
    )


def read_transactions(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .options(**cfg.kafka_options)
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
        .select("tx.*", "kafka_timestamp", "kafka_partition", "kafka_offset")
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


def duplicate_records(batch: DataFrame, cfg: StreamConfig) -> DataFrame:
    """Duplicates detected by the stateful stage, in dead-letter shape."""
    return batch.filter("is_duplicate").select(
        F.lit(cfg.transactions_topic).alias("source_topic"),
        "kafka_partition",
        "kafka_offset",
        "transaction_id",
        F.array(F.lit("duplicate_transaction_id")).alias("reasons"),
        F.to_json(
            F.struct(
                *[c for c in TRANSACTION_SCHEMA.fieldNames() if c != "timestamp"],
                F.col("event_time").alias("timestamp"),
            )
        ).alias("raw_value"),
        "kafka_timestamp",
    )


def make_batch_processor(cfg: StreamConfig, broadcaster: ModelBroadcaster):
    def process(batch: DataFrame, batch_id: int) -> None:
        broadcaster.refresh()  # pick up a newly promoted champion
        started = time.perf_counter()
        # Persist once: every action below would otherwise re-run the stateful stage.
        batch = batch.persist()
        scored = score(batch, broadcaster).persist()
        try:
            duplicates = duplicate_records(batch, cfg)
            if not duplicates.isEmpty():
                publish_dead_letters(duplicates, cfg)
            n = scored.count()
            if n == 0:
                return
            # One Kafka write: every prediction, plus a copy of frauds on the alerts topic.
            records = prediction_records(scored).withColumn("topic", F.lit(cfg.predictions_topic))
            alerts = prediction_records(scored.filter(F.col("prediction") == "FRAUD"))
            records.unionByName(alerts.withColumn("topic", F.lit(cfg.alerts_topic))).write.format(
                "kafka"
            ).options(**cfg.kafka_options).save()
            # Serving store for the API/dashboard (idempotent upserts).
            scored.foreachPartition(spark_partition_writer(FEATURES, cfg.postgres_dsn))
            # Overwriting a batch-specific directory makes the lake write
            # idempotent when Spark replays a batch after a failure.
            scored.write.mode("overwrite").parquet(f"{cfg.lake_path}/predictions/batch_id={batch_id:08d}")
            summary = scored.select(
                "kafka_timestamp", "scored_at", "prediction", "fraud_probability", "score_ms", *DRIFT_FEATURES
            ).toPandas()
            metrics.observe_scored(summary, broadcaster.spec)
            frauds = int((summary["prediction"] == "FRAUD").sum())
            latency = latency_summary(summary)
            print(
                f"[batch {batch_id}] scored={n} fraud={frauds} model=v{broadcaster.version} "
                f"e2e_p95={latency.get('p95_ms', 0):.0f}ms took={time.perf_counter() - started:.2f}s",
                flush=True,
            )
        finally:
            scored.unpersist()
            batch.unpersist()

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
    def write(batch: DataFrame, batch_id: int) -> None:
        write_window_stats([row.asDict() for row in batch.collect()], cfg.postgres_dsn)

    return (
        stats.writeStream.queryName("window_stats")
        .foreachBatch(write)
        .option("checkpointLocation", cfg.checkpoint("window_stats"))
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )


def dead_letters(parsed: DataFrame, too_late: DataFrame, cfg: StreamConfig) -> DataFrame:
    """Invalid records plus events beyond the allowed lateness, in DLQ shape."""
    invalid = parsed.filter(~F.col("is_valid")).select(
        F.lit(cfg.transactions_topic).alias("source_topic"),
        "kafka_partition",
        "kafka_offset",
        F.col("tx.transaction_id").alias("transaction_id"),
        F.col("errors").alias("reasons"),
        "raw_value",
        "kafka_timestamp",
    )
    late = too_late.select(
        F.lit(cfg.transactions_topic).alias("source_topic"),
        "kafka_partition",
        "kafka_offset",
        "transaction_id",
        F.array(F.lit("arrived_after_allowed_lateness")).alias("reasons"),
        F.to_json(F.struct(*TRANSACTION_SCHEMA.fieldNames())).alias("raw_value"),
        "kafka_timestamp",
    )
    return invalid.unionByName(late)


def publish_dead_letters(records: DataFrame, cfg: StreamConfig) -> None:
    """Write DLQ records to Kafka (for replay tooling) and PostgreSQL (for triage)."""
    payload = F.to_json(F.struct(*records.columns, F.current_timestamp().alias("quarantined_at")))
    (
        records.select(F.col("transaction_id").alias("key"), payload.alias("value"))
        .write.format("kafka")
        .options(**cfg.kafka_options)
        .option("topic", cfg.dead_letter_topic)
        .save()
    )
    rows = [row.asDict() for row in records.collect()]
    write_dead_letters(rows, cfg.postgres_dsn)
    metrics.observe_dead_letters([r["reasons"] for r in rows])


def start_dead_letter_sink(records: DataFrame, cfg: StreamConfig):
    def process(batch: DataFrame, batch_id: int) -> None:
        if not batch.isEmpty():
            publish_dead_letters(batch, cfg)

    return (
        records.writeStream.queryName("dead_letters")
        .foreachBatch(process)
        .option("checkpointLocation", cfg.checkpoint("dead_letters"))
        .trigger(processingTime=cfg.trigger)
        .start()
    )


def main() -> None:
    cfg = StreamConfig()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    ensure_schema(cfg.postgres_dsn)

    parsed = parse(read_transactions(spark, cfg))
    transactions = enrich(parsed)

    on_time, too_late = split_by_lateness(transactions, cfg)

    start_lake_sink(transactions, cfg)
    metrics.start_server(cfg.metrics_port)
    spark.streams.addListener(metrics.ProgressListener())

    def on_model_change(spec: dict) -> None:
        metrics.observe_model(spec)
        record_model(spec, cfg.postgres_dsn)

    broadcaster = ModelBroadcaster(spark, on_change=on_model_change, **model_settings())
    start_scoring(user_features(on_time, cfg), cfg, broadcaster)
    start_window_stats(window_stats(on_time, cfg), cfg)
    start_dead_letter_sink(dead_letters(parsed, too_late, cfg), cfg)
    print(f"[fraud-stream] consuming {cfg.transactions_topic} @ {cfg.bootstrap}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

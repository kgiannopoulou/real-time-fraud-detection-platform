"""Spark Structured Streaming job: Kafka -> validation -> enrichment -> sinks.

    spark-submit streaming/fraud_stream.py          # inside the spark container

Queries started by this job:

* ``lake``  — every valid, enriched transaction appended to the Parquet data
  lake, partitioned by event date (the history used for analytics/retraining);
* ``features`` — per-user behavioural features computed with
  ``applyInPandasWithState`` (one ``UserState`` per card in the state store),
  appended to the lake;
* ``suspicious`` — console view of transactions with extreme behavioural
  signals (amount far above the user's average, bursts of transactions).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from producer.transaction_generator import FX_TO_EUR
from streaming.schemas import event_time, parse_value, validation_errors
from streaming.stateful import FEATURES_SCHEMA, PASSTHROUGH, STATE_SCHEMA, compute_user_features


@dataclass(frozen=True)
class StreamConfig:
    bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    transactions_topic: str = field(default_factory=lambda: os.getenv("TRANSACTIONS_TOPIC", "transactions"))
    starting_offsets: str = field(default_factory=lambda: os.getenv("STARTING_OFFSETS", "latest"))
    max_offsets_per_trigger: int = field(
        default_factory=lambda: int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
    )
    lake_path: str = field(default_factory=lambda: os.getenv("LAKE_PATH", "/data/lake"))
    checkpoint_root: str = field(default_factory=lambda: os.getenv("CHECKPOINT_ROOT", "/data/checkpoints"))
    trigger: str = field(default_factory=lambda: os.getenv("TRIGGER_INTERVAL", "5 seconds"))
    lake_trigger: str = field(default_factory=lambda: os.getenv("LAKE_TRIGGER_INTERVAL", "30 seconds"))

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


def user_features(transactions: DataFrame) -> DataFrame:
    """Behavioural features from each user's history kept in the state store."""
    return (
        transactions.select(*PASSTHROUGH)
        .groupBy("user_id")
        .applyInPandasWithState(
            compute_user_features,
            outputStructType=FEATURES_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )


def start_feature_sinks(features: DataFrame, cfg: StreamConfig):
    lake = (
        features.withColumn("event_date", F.to_date("event_time"))
        .writeStream.queryName("features")
        .format("parquet")
        .option("path", f"{cfg.lake_path}/features")
        .option("checkpointLocation", cfg.checkpoint("features"))
        .partitionBy("event_date")
        .trigger(processingTime=cfg.lake_trigger)
        .outputMode("append")
        .start()
    )
    suspicious = (
        features.filter(
            (F.col("tx_count_24h") > 0)  # ignore cold-start cards
            & (
                (F.col("amount_ratio") > 5)
                | (F.col("tx_count_2m") >= 3)
                | (F.col("is_new_country") == 1)
                | (F.col("is_new_device") == 1)
            )
        )
        .select(
            "event_time",
            "user_id",
            "amount_eur",
            "merchant_category",
            "country",
            F.round("amount_ratio", 1).alias("amount_ratio"),
            "tx_count_2m",
            "tx_count_1h",
            "is_new_country",
            "is_new_device",
        )
        .writeStream.queryName("suspicious")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", cfg.checkpoint("suspicious"))
        .trigger(processingTime=cfg.trigger)
        .outputMode("append")
        .start()
    )
    return lake, suspicious


def main() -> None:
    cfg = StreamConfig()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    parsed = parse(read_transactions(spark, cfg))
    transactions = enrich(parsed)

    start_lake_sink(transactions, cfg)
    start_feature_sinks(user_features(transactions), cfg)
    print(f"[fraud-stream] consuming {cfg.transactions_topic} @ {cfg.bootstrap}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

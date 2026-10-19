"""Spark Structured Streaming job: Kafka -> validation -> enrichment -> sinks.

    spark-submit streaming/fraud_stream.py          # inside the spark container

Queries started by this job:

* ``lake``  — every valid, enriched transaction appended to the Parquet data
  lake, partitioned by event date (the history used for analytics/retraining);
* ``stats`` — running per-category statistics printed to the console.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from producer.transaction_generator import FX_TO_EUR
from streaming.schemas import event_time, parse_value, validation_errors


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


def start_console_stats(transactions: DataFrame, cfg: StreamConfig):
    stats = transactions.groupBy("merchant_category").agg(
        F.count("*").alias("transactions"),
        F.round(F.avg("amount_eur"), 2).alias("avg_amount_eur"),
        F.round(F.max("amount_eur"), 2).alias("max_amount_eur"),
    )
    return (
        stats.writeStream.queryName("stats")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", cfg.checkpoint("stats"))
        .outputMode("complete")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main() -> None:
    cfg = StreamConfig()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    parsed = parse(read_transactions(spark, cfg))
    transactions = enrich(parsed)

    start_lake_sink(transactions, cfg)
    start_console_stats(transactions, cfg)
    print(f"[fraud-stream] consuming {cfg.transactions_topic} @ {cfg.bootstrap}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

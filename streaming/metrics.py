"""Prometheus metrics exported by the Spark driver (``:9108/metrics``).

Two families:

* **platform** — throughput, Kafka lag (offsets behind latest), micro-batch
  duration, end-to-end latency, per-row scoring time, state size, rows dropped
  by the watermark, dead letters by reason;
* **ML** — score distribution, rolling fraud rate, serving model version, and
  feature drift (PSI against the training baseline in the model spec).

Query-level numbers come from a ``StreamingQueryListener`` (Spark's own
progress reports); per-record numbers are observed in ``foreachBatch``.
"""

from __future__ import annotations

import math
from collections import deque

import pandas as pd
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql.streaming import StreamingQueryListener

from ml.monitoring import DRIFT_FEATURES, psi

LATENCY_BUCKETS = (0.5, 1, 2, 3, 4, 5, 7.5, 10, 15, 20, 30, 60)

SCORED = Counter("fraud_stream_scored_total", "Transactions scored", ["prediction"])
DEAD_LETTERS = Counter("fraud_stream_dead_letters_total", "Records sent to the dead-letter queue", ["reason"])
E2E_LATENCY = Histogram(
    "fraud_stream_e2e_latency_seconds",
    "Kafka ingest -> scored latency per transaction",
    buckets=LATENCY_BUCKETS,
)
SCORE_TIME = Histogram(
    "fraud_stream_scoring_seconds_per_row",
    "Model inference + explanation time per transaction",
    buckets=(0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05),
)
BATCH_DURATION = Histogram(
    "fraud_stream_batch_duration_seconds", "Micro-batch duration", ["query"], buckets=LATENCY_BUCKETS
)
INPUT_RATE = Gauge("fraud_stream_input_rows_per_second", "Input rate per query", ["query"])
PROCESSED_RATE = Gauge("fraud_stream_processed_rows_per_second", "Processing rate per query", ["query"])
KAFKA_LAG = Gauge(
    "fraud_stream_kafka_offsets_behind_latest", "Max offsets behind latest per query", ["query"]
)
STATE_ROWS = Gauge("fraud_stream_state_rows", "Rows in state stores", ["query"])
STATE_MEMORY = Gauge("fraud_stream_state_memory_bytes", "State store memory", ["query"])
WATERMARK_DROPS = Counter(
    "fraud_stream_rows_dropped_by_watermark_total", "Rows dropped by the watermark", ["query"]
)
SCORE = Histogram(
    "fraud_model_score",
    "Fraud probability distribution",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0),
)
FRAUD_RATE = Gauge("fraud_model_fraud_rate", "Share of FRAUD predictions over the rolling window")
TRAINING_FRAUD_RATE = Gauge("fraud_model_training_fraud_rate", "Fraud rate in the training data")
MODEL_VERSION = Gauge("fraud_model_version_info", "Serving model version (value 1)", ["version"])
MODEL_VERSION_NUMBER = Gauge("fraud_model_version", "Serving registry version number (0 = local artefacts)")
DRIFT_PSI = Gauge("fraud_feature_psi", "Population Stability Index vs training baseline", ["feature"])
FEATURE_MEAN = Gauge("fraud_feature_mean", "Rolling mean of a feature", ["feature", "source"])


class RollingWindow:
    """The most recent scored rows, for rate and drift computations."""

    def __init__(self, size: int = 5000):
        self.rows: deque[dict] = deque(maxlen=size)

    def extend(self, frame: pd.DataFrame) -> None:
        self.rows.extend(frame.to_dict("records"))

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(list(self.rows))


WINDOW = RollingWindow()


def start_server(port: int) -> None:
    start_http_server(port)


def observe_model(spec: dict) -> None:
    MODEL_VERSION.clear()
    version = str(spec.get("version", "local"))
    MODEL_VERSION.labels(version=version).set(1)
    MODEL_VERSION_NUMBER.set(float(version) if version.isdigit() else 0)
    if spec.get("training_fraud_rate") is not None:
        TRAINING_FRAUD_RATE.set(spec["training_fraud_rate"])
    for name, base in (spec.get("baseline") or {}).items():
        if name in DRIFT_FEATURES and base.get("mean") is not None:
            FEATURE_MEAN.labels(feature=name, source="training").set(base["mean"])


def observe_scored(summary: pd.DataFrame, spec: dict | None) -> None:
    """Per-transaction metrics for one scored micro-batch."""
    for prediction, n in summary["prediction"].value_counts().items():
        SCORED.labels(prediction=prediction).inc(int(n))
    latency = (summary["scored_at"] - summary["kafka_timestamp"]).dt.total_seconds()
    for value in latency.dropna():
        E2E_LATENCY.observe(value)
    for value in summary["fraud_probability"]:
        SCORE.observe(value)
    for value in summary["score_ms"].dropna():
        SCORE_TIME.observe(value / 1000)

    WINDOW.extend(summary[["prediction", *DRIFT_FEATURES]])
    window = WINDOW.frame()
    FRAUD_RATE.set(float((window["prediction"] == "FRAUD").mean()))
    baselines = (spec or {}).get("baseline") or {}
    for name in DRIFT_FEATURES:
        values = pd.to_numeric(window[name], errors="coerce")
        if values.notna().any():
            FEATURE_MEAN.labels(feature=name, source="production").set(float(values.mean()))
        if name in baselines and len(window) >= 500:  # PSI is noisy on tiny samples
            value = psi(baselines[name], values)
            if not math.isnan(value):
                DRIFT_PSI.labels(feature=name).set(value)


def observe_dead_letters(reasons: list[list[str]]) -> None:
    for record in reasons:
        for reason in record:
            DEAD_LETTERS.labels(reason=reason).inc()


class ProgressListener(StreamingQueryListener):
    """Turns Spark's per-batch query progress into Prometheus metrics."""

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark API
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = event.progress
        name = p.name or "unnamed"
        INPUT_RATE.labels(query=name).set(_num(p.inputRowsPerSecond))
        PROCESSED_RATE.labels(query=name).set(_num(p.processedRowsPerSecond))
        duration_ms = (p.durationMs or {}).get("triggerExecution")
        if duration_ms is not None:
            BATCH_DURATION.labels(query=name).observe(duration_ms / 1000)
        lag = 0.0
        for source in p.sources or []:
            behind = (source.metrics or {}).get("maxOffsetsBehindLatest")
            if behind is not None:
                lag = max(lag, float(behind))
        KAFKA_LAG.labels(query=name).set(lag)
        ops = p.stateOperators or []
        STATE_ROWS.labels(query=name).set(sum(op.numRowsTotal for op in ops))
        STATE_MEMORY.labels(query=name).set(sum(op.memoryUsedBytes for op in ops))
        dropped = sum(op.numRowsDroppedByWatermark for op in ops)
        if dropped:
            WATERMARK_DROPS.labels(query=name).inc(dropped)

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def _num(value) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(value) else value

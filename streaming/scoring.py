"""Real-time model inference inside Spark.

* The **driver** resolves the model version carrying the ``champion`` alias in
  the MLflow registry (falling back to local artefacts), and ships the raw
  artefacts (booster JSON + feature spec) to executors as a **broadcast
  variable**. It re-checks the registry periodically, so promoting a new
  version in MLflow hot-swaps the model without restarting the stream.
* Every score comes with its **explanation**: TreeSHAP contributions turned
  into readable risk factors (``ml/explain.py``).
* **Executors** deserialize the broadcast once per Python worker (cached by
  version) and score partitions with ``mapInPandas``.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ml.feature_pipeline import add_risk_features
from ml.predict import ARTIFACT_DIR, MODEL_FILE, SPEC_FILE, FraudModel
from streaming.stateful import FEATURES_SCHEMA

SCORE_FIELDS = [
    StructField("fraud_probability", DoubleType()),
    StructField("prediction", StringType()),
    StructField("model_version", StringType()),
    StructField("merchant_risk", DoubleType()),
    StructField("country_risk", DoubleType()),
    StructField("risk_factors", ArrayType(StringType())),
    StructField("shap_top", StringType()),  # JSON {feature: contribution}
    StructField("scored_at", TimestampType()),
]
PREDICTION_SCHEMA = StructType([*FEATURES_SCHEMA.fields, *SCORE_FIELDS])
PREDICTION_COLUMNS = [f.name for f in PREDICTION_SCHEMA.fields]


@dataclass(frozen=True)
class ModelPayload:
    version: str
    model_bytes: bytes
    spec_json: str


def _read_local(directory: Path) -> ModelPayload:
    spec = (directory / SPEC_FILE).read_text()
    model = FraudModel.from_bytes((directory / MODEL_FILE).read_bytes(), spec)
    return ModelPayload(model.version, (directory / MODEL_FILE).read_bytes(), spec)


class ModelBroadcaster:
    """Driver-side holder of the current model broadcast."""

    def __init__(
        self,
        spark: SparkSession,
        tracking_uri: str | None,
        alias: str = "champion",
        refresh_seconds: float = 60.0,
        fallback_dir: Path = ARTIFACT_DIR,
    ):
        self.spark = spark
        self.tracking_uri = tracking_uri
        self.alias = alias
        self.refresh_seconds = refresh_seconds
        self.fallback_dir = Path(fallback_dir)
        self.version: str | None = None
        self.broadcast = None
        self._checked_at = 0.0
        self.refresh(force=True)

    def _resolve(self) -> ModelPayload | None:
        if self.tracking_uri:
            try:
                from ml.registry import champion_version, download_champion

                version, _ = champion_version(self.tracking_uri, self.alias)
                if version == self.version:
                    return None  # unchanged
                return _read_local(download_champion(self.tracking_uri, "/tmp/models", self.alias))
            except Exception as exc:  # registry unreachable / no champion yet
                if self.version is not None:
                    print(f"[scoring] registry check failed ({exc!s:.100}); keeping v{self.version}")
                    return None
                print(f"[scoring] registry unavailable ({exc!s:.100}); using {self.fallback_dir}")
        if self.version is not None:
            return None
        return _read_local(self.fallback_dir)

    def refresh(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._checked_at < self.refresh_seconds:
            return
        self._checked_at = now
        payload = self._resolve()
        if payload is None:
            return
        old = self.broadcast
        self.broadcast = self.spark.sparkContext.broadcast(payload)
        print(f"[scoring] serving model version {payload.version} (was {self.version})", flush=True)
        self.version = payload.version
        if old is not None:
            old.unpersist()


_MODEL_CACHE: dict[str, FraudModel] = {}


def _model(payload: ModelPayload) -> FraudModel:
    model = _MODEL_CACHE.get(payload.version)
    if model is None:
        _MODEL_CACHE.clear()
        model = _MODEL_CACHE[payload.version] = FraudModel.from_bytes(payload.model_bytes, payload.spec_json)
    return model


def score_frame(pdf: pd.DataFrame, model: FraudModel) -> pd.DataFrame:
    """Add risk encodings, probability and decision to a features frame."""
    pdf = add_risk_features(pdf.copy(), model.risk)
    preds = model.predict(pdf)
    pdf["fraud_probability"] = preds["fraud_probability"].astype(float).round(6)
    pdf["prediction"] = preds["prediction"]
    pdf["model_version"] = model.version
    factors, contributions = model.explain(pdf)
    pdf["risk_factors"] = factors
    pdf["shap_top"] = [json.dumps(c) for c in contributions]
    pdf["scored_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    return pdf


def score(features: DataFrame, broadcaster: ModelBroadcaster) -> DataFrame:
    """Score non-duplicate feature rows on the executors."""
    bc = broadcaster.broadcast

    def _score_partitions(batches):
        model = _model(bc.value)
        for pdf in batches:
            if pdf.empty:
                continue
            yield score_frame(pdf, model)[PREDICTION_COLUMNS]

    return features.filter("NOT is_duplicate").mapInPandas(_score_partitions, PREDICTION_SCHEMA)


def latency_summary(pdf: pd.DataFrame) -> dict[str, float]:
    """End-to-end latency (Kafka ingest -> scored) of a scored batch, in ms."""
    if pdf.empty:
        return {}
    ms = (pdf["scored_at"] - pdf["kafka_timestamp"]).dt.total_seconds().to_numpy() * 1000
    return {
        "p50_ms": float(np.percentile(ms, 50)),
        "p95_ms": float(np.percentile(ms, 95)),
        "max_ms": float(ms.max()),
    }


def model_settings() -> dict:
    return {
        "tracking_uri": os.getenv("MLFLOW_TRACKING_URI") or None,
        "alias": os.getenv("MODEL_ALIAS", "champion"),
        "refresh_seconds": float(os.getenv("MODEL_REFRESH_SECONDS", "60")),
        "fallback_dir": Path(os.getenv("MODEL_DIR", str(ARTIFACT_DIR))),
    }

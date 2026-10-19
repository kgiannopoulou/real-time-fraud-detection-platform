"""FastAPI serving layer.

    uvicorn api.main:app --port 8000          # http://localhost:8000/docs

* ``POST /predict`` — synchronous scoring for an authorization flow: the
  card holder's behavioural state is rebuilt from their recent transactions
  in PostgreSQL with the same ``UserState`` class the stream uses, then the
  champion model scores and explains the transaction. Nothing is persisted;
  the stream remains the system of record.
* Read endpoints over the serving store (predictions, alerts, stats).
* ``PATCH /alerts/{id}`` — analyst feedback (confirmed / dismissed), the
  label source for retraining.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Literal

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from psycopg.types.numeric import FloatLoader
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from ml.feature_pipeline import add_risk_features
from ml.predict import FraudModel, load_serving_model
from producer.transaction_generator import FX_TO_EUR, SUPPORTED_CURRENCIES
from storage.postgres import dsn_from_env, recent_user_transactions
from streaming.features import KNOWN_TTL, UserState

MODEL_REFRESH_SECONDS = float(os.getenv("MODEL_REFRESH_SECONDS", "60"))


class ModelHolder:
    """Current champion model, re-resolved from the registry at most once a minute."""

    def __init__(self, tracking_uri: str | None):
        self.tracking_uri = tracking_uri
        self._lock = threading.Lock()
        self._model: FraudModel | None = None
        self._checked = 0.0

    def get(self) -> FraudModel:
        with self._lock:
            if self._model is None or time.monotonic() - self._checked > MODEL_REFRESH_SECONDS:
                self._checked = time.monotonic()
                try:
                    self._model = load_serving_model(self.tracking_uri)
                except Exception:
                    if self._model is None:
                        raise
            return self._model


class TransactionIn(BaseModel):
    transaction_id: str = Field(default_factory=lambda: f"tx_{uuid.uuid4().hex[:12]}")
    user_id: str = Field(min_length=1)
    merchant_id: str = Field(min_length=1)
    merchant_category: str = "unknown"
    amount: float = Field(gt=0, le=1_000_000)
    currency: Literal[SUPPORTED_CURRENCIES] = "EUR"  # type: ignore[valid-type]
    country: str = Field(pattern=r"^[A-Z]{2}$")
    device_id: str | None = None
    timestamp: datetime | None = None


class PredictionOut(BaseModel):
    transaction_id: str
    fraud_probability: float
    prediction: Literal["FRAUD", "LEGIT"]
    threshold: float
    model_version: str
    risk_factors: list[str]
    features: dict[str, float | None]


class AlertUpdate(BaseModel):
    status: Literal["confirmed", "dismissed", "open"]


def _configure(conn) -> None:
    conn.adapters.register_loader("numeric", FloatLoader)  # NUMERIC -> JSON numbers


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = ConnectionPool(
        dsn_from_env(),
        min_size=1,
        max_size=8,
        open=True,
        kwargs={"row_factory": dict_row},
        configure=_configure,
    )
    app.state.models = ModelHolder(os.getenv("MLFLOW_TRACKING_URI") or None)
    yield
    app.state.pool.close()


app = FastAPI(
    title="Real-Time Fraud Detection API",
    version="1.0.0",
    description="Online scoring and read access to the fraud platform's serving store.",
    lifespan=lifespan,
)


def _features_frame(tx: TransactionIn, history_rows: list[dict], model: FraudModel) -> pd.DataFrame:
    event_time = tx.timestamp or datetime.now(timezone.utc)
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    amount_eur = round(tx.amount * FX_TO_EUR[tx.currency], 2)

    state = UserState()
    for h in history_rows:
        if h["transaction_id"] == tx.transaction_id:
            continue
        state.add(
            h["event_time"].timestamp(),
            h["transaction_id"],
            float(h["amount_eur"]),
            h["merchant_id"],
            h["country"],
            h["device_id"],
        )
    feats = state.features(event_time.timestamp(), amount_eur, tx.country, tx.device_id)
    row = {
        **tx.model_dump(),
        "amount_eur": amount_eur,
        "hour_of_day": event_time.hour,
        **feats,
    }
    return add_risk_features(pd.DataFrame([row]), model.risk)


@app.post("/predict", response_model=PredictionOut)
def predict(tx: TransactionIn) -> PredictionOut:
    model = app.state.models.get()
    since = (tx.timestamp or datetime.now(timezone.utc)) - timedelta(seconds=KNOWN_TTL)
    with app.state.pool.connection() as conn:
        history = recent_user_transactions(conn, tx.user_id, since)
    frame = _features_frame(tx, history, model)
    scored = model.predict(frame).iloc[0]
    factors, _ = model.explain(frame)
    features = {
        name: (None if pd.isna(value) else float(value))
        for name, value in frame.iloc[0][model.features].items()
    }
    return PredictionOut(
        transaction_id=tx.transaction_id,
        fraud_probability=round(float(scored["fraud_probability"]), 6),
        prediction=scored["prediction"],
        threshold=model.threshold,
        model_version=model.version,
        risk_factors=factors[0],
        features=features,
    )


@app.get("/health")
def health() -> dict:
    status = {"status": "ok"}
    try:
        with app.state.pool.connection() as conn:
            conn.execute("SELECT 1")
        status["database"] = "ok"
    except Exception as exc:
        status.update(status="degraded", database=f"error: {exc!s:.80}")
    try:
        status["model_version"] = app.state.models.get().version
    except Exception as exc:
        status.update(status="degraded", model=f"error: {exc!s:.80}")
    return status


@app.get("/predictions")
def list_predictions(
    limit: int = Query(50, ge=1, le=500), prediction: Literal["FRAUD", "LEGIT"] | None = None
) -> list[dict]:
    sql = """SELECT transaction_id, user_id, event_time, amount_eur, merchant_category, country,
                    fraud_probability, prediction, model_version, risk_factors, scored_at, latency_ms
             FROM fraud_predictions {where} ORDER BY scored_at DESC LIMIT %s"""
    where, params = ("WHERE prediction = %s", [prediction]) if prediction else ("", [])
    with app.state.pool.connection() as conn:
        return conn.execute(sql.format(where=where), [*params, limit]).fetchall()


@app.get("/predictions/{transaction_id}")
def get_prediction(transaction_id: str) -> dict:
    with app.state.pool.connection() as conn:
        row = conn.execute(
            """SELECT p.*, t.merchant_id, t.device_id, t.amount, t.currency
               FROM fraud_predictions p JOIN transactions t USING (transaction_id)
               WHERE transaction_id = %s""",
            (transaction_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(404, f"transaction {transaction_id} not found")
    return row


@app.get("/alerts")
def list_alerts(
    status: Literal["open", "confirmed", "dismissed"] | None = "open", limit: int = Query(50, ge=1, le=500)
) -> list[dict]:
    where, params = ("WHERE status = %s", [status]) if status else ("", [])
    with app.state.pool.connection() as conn:
        return conn.execute(
            f"SELECT * FROM fraud_alerts {where} ORDER BY created_at DESC LIMIT %s", [*params, limit]
        ).fetchall()


@app.patch("/alerts/{alert_id}")
def update_alert(alert_id: int, update: AlertUpdate) -> dict:
    with app.state.pool.connection() as conn:
        row = conn.execute(
            "UPDATE fraud_alerts SET status = %s WHERE alert_id = %s RETURNING *", (update.status, alert_id)
        ).fetchone()
    if row is None:
        raise HTTPException(404, f"alert {alert_id} not found")
    return row


@app.get("/stats/summary")
def summary(minutes: int = Query(60, ge=1, le=7 * 24 * 60)) -> dict:
    with app.state.pool.connection() as conn:
        return conn.execute(
            """SELECT count(*) AS transactions,
                      count(*) FILTER (WHERE prediction = 'FRAUD') AS frauds,
                      coalesce(sum(amount_eur) FILTER (WHERE prediction = 'FRAUD'), 0) AS blocked_eur,
                      coalesce(avg(fraud_probability), 0) AS avg_probability,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms
               FROM fraud_predictions WHERE scored_at >= now() - make_interval(mins => %s)""",
            (minutes,),
        ).fetchone()

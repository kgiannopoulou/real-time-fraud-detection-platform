"""Integration tests against the docker compose services (Kafka + PostgreSQL).

docker compose up -d kafka kafka-init postgres
pytest -m integration
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer

from producer.producer import TransactionProducer
from storage.postgres import (
    connect,
    ensure_schema,
    recent_user_transactions,
    write_dead_letters,
    write_scored,
)

pytestmark = pytest.mark.integration

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
FEATURES = ["amount_eur", "tx_count_2m"]


def _scored_row(tx_id: str, user: str, prediction: str = "LEGIT", probability: float = 0.01) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "transaction_id": tx_id,
        "user_id": user,
        "merchant_id": "merchant_1",
        "merchant_category": "jewelry",
        "amount": 950.3,
        "currency": "EUR",
        "amount_eur": 950.3,
        "country": "GR",
        "device_id": "device_1",
        "event_time": now,
        "kafka_timestamp": now,
        "scored_at": now,
        "fraud_probability": probability,
        "prediction": prediction,
        "model_version": "test",
        "risk_factors": ["new device (device_1)"],
        "shap_top": json.dumps({"is_new_device": 2.1}),
        "tx_count_2m": 3.0,
    }


def test_kafka_round_trip_preserves_key_and_payload():
    topic = "transactions"
    tx_id = f"tx_it{uuid.uuid4().hex[:10]}"
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"it-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([topic])
    consumer.poll(5)  # join the group and get assignments before producing

    producer = TransactionProducer(BOOTSTRAP, topic)
    event = {"transaction_id": tx_id, "user_id": "user_it", "amount": 1.0}
    producer.send(event)
    assert producer.flush(15) == 0

    received = None
    for _ in range(30):
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error() and json.loads(msg.value())["transaction_id"] == tx_id:
            received = msg
            break
    consumer.close()
    assert received is not None
    assert received.key() == b"user_it"
    assert dict(received.headers())["schema_version"] == b"1"


def test_scored_writes_are_idempotent_and_raise_alerts():
    ensure_schema()
    user = f"user_it{uuid.uuid4().hex[:8]}"
    rows = [_scored_row(f"tx_{user}_1", user), _scored_row(f"tx_{user}_2", user, "FRAUD", 0.97)]
    write_scored(rows, FEATURES)
    write_scored(rows, FEATURES)  # a replayed micro-batch
    with connect() as conn:
        n_pred = conn.execute(
            "SELECT count(*) FROM fraud_predictions WHERE user_id = %s", (user,)
        ).fetchone()[0]
        alerts = conn.execute(
            "SELECT severity, risk_factors FROM fraud_alerts WHERE user_id = %s", (user,)
        ).fetchall()
        history = recent_user_transactions(conn, user, datetime(2000, 1, 1, tzinfo=timezone.utc))
        conn.execute("DELETE FROM fraud_alerts WHERE user_id = %s", (user,))
        conn.execute("DELETE FROM fraud_predictions WHERE user_id = %s", (user,))
        conn.execute("DELETE FROM transactions WHERE user_id = %s", (user,))
    assert n_pred == 2
    assert alerts == [("critical", ["new device (device_1)"])]
    assert len(history) == 2


def test_dead_letters_are_idempotent():
    ensure_schema()
    marker = uuid.uuid4().hex[:8]
    row = {
        "source_topic": f"it-{marker}",
        "kafka_partition": 0,
        "kafka_offset": 42,
        "transaction_id": None,
        "reasons": ["malformed_json"],
        "raw_value": "{bad",
        "kafka_timestamp": None,
    }
    write_dead_letters([row, row])
    with connect() as conn:
        n = conn.execute(
            "SELECT count(*) FROM dead_letter_transactions WHERE source_topic = %s", (row["source_topic"],)
        ).fetchone()[0]
        conn.execute("DELETE FROM dead_letter_transactions WHERE source_topic = %s", (row["source_topic"],))
    assert n == 1


@pytest.fixture
def api_client(trained, monkeypatch):
    monkeypatch.setenv("MODEL_DIR", str(trained["dir"]))
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    from fastapi.testclient import TestClient

    from api.main import app

    with TestClient(app) as client:
        yield client


def test_api_scores_with_history_from_postgres(api_client):
    user = f"user_api{uuid.uuid4().hex[:8]}"
    write_scored([_scored_row(f"tx_{user}_{i}", user) for i in range(3)], FEATURES)
    try:
        response = api_client.post(
            "/predict",
            json={
                "user_id": user,
                "merchant_id": "merchant_9",
                "merchant_category": "jewelry",
                "amount": 2400,
                "currency": "EUR",
                "country": "NG",
                "device_id": "device_new",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["model_version"] == "test"
        assert 0 <= body["fraud_probability"] <= 1
        assert body["features"]["tx_count_24h"] == 3  # rebuilt from PostgreSQL
        assert body["features"]["is_new_device"] == 1
        assert body["features"]["is_new_country"] == 1

        bad = api_client.post(
            "/predict",
            json={"user_id": user, "merchant_id": "m", "amount": -1, "currency": "XYZ", "country": "Greece"},
        )
        assert bad.status_code == 422

        assert api_client.get("/health").json()["database"] == "ok"
        assert api_client.get("/predictions/does-not-exist").status_code == 404
        assert "fraud_api_requests_total" in api_client.get("/metrics/").text
    finally:
        with connect() as conn:
            conn.execute("DELETE FROM fraud_predictions WHERE user_id = %s", (user,))
            conn.execute("DELETE FROM transactions WHERE user_id = %s", (user,))

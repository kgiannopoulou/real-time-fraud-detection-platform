"""PostgreSQL access shared by the Spark sinks, the API and the dashboard.

All writes are idempotent upserts keyed by natural ids, because the producers
of these rows (Spark ``foreachBatch``) are at-least-once: a replayed
micro-batch must not create duplicate predictions or alerts.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import psycopg

DEFAULT_DSN = "postgresql://fraud:fraud_local_pw@localhost:5436/fraud"

ALERT_CRITICAL_PROBABILITY = 0.9
ALERT_CRITICAL_AMOUNT_EUR = 1_000.0


def dsn_from_env() -> str:
    return os.getenv("POSTGRES_DSN", DEFAULT_DSN)


def connect(dsn: str | None = None, **kwargs) -> psycopg.Connection:
    return psycopg.connect(dsn or dsn_from_env(), **kwargs)


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _clean(value):
    """JSON-safe value (NaN -> null)."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def alert_severity(probability: float, amount_eur: float) -> str:
    critical = probability >= ALERT_CRITICAL_PROBABILITY or amount_eur >= ALERT_CRITICAL_AMOUNT_EUR
    return "critical" if critical else "high"


INSERT_TRANSACTION = """
INSERT INTO transactions (transaction_id, user_id, merchant_id, merchant_category, amount, currency,
                          amount_eur, country, device_id, event_time, ingested_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""

INSERT_PREDICTION = """
INSERT INTO fraud_predictions (transaction_id, user_id, event_time, amount_eur, merchant_category, country,
                               fraud_probability, prediction, model_version, risk_factors, shap_top,
                               features, scored_at, latency_ms)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""

INSERT_ALERT = """
INSERT INTO fraud_alerts (transaction_id, user_id, severity, fraud_probability, amount_eur, risk_factors)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""


def write_scored(rows: Iterable[dict], feature_names: list[str], dsn: str | None = None) -> int:
    """Persist scored transactions: raw transaction, prediction and (if fraud) an alert."""
    tx_rows, pred_rows, alert_rows = [], [], []
    for r in rows:
        event_time, kafka_ts, scored_at = (
            _utc(r["event_time"]),
            _utc(r["kafka_timestamp"]),
            _utc(r["scored_at"]),
        )
        tx_rows.append(
            (
                r["transaction_id"],
                r["user_id"],
                r["merchant_id"],
                r["merchant_category"],
                r["amount"],
                r["currency"],
                r["amount_eur"],
                r["country"],
                r["device_id"],
                event_time,
                kafka_ts,
            )
        )
        factors = json.dumps(list(r.get("risk_factors") or []))
        features = json.dumps({name: _clean(r.get(name)) for name in feature_names})
        latency_ms = (scored_at - kafka_ts).total_seconds() * 1000 if kafka_ts and scored_at else None
        pred_rows.append(
            (
                r["transaction_id"],
                r["user_id"],
                event_time,
                r["amount_eur"],
                r["merchant_category"],
                r["country"],
                r["fraud_probability"],
                r["prediction"],
                r["model_version"],
                factors,
                r.get("shap_top"),
                features,
                scored_at,
                latency_ms,
            )
        )
        if r["prediction"] == "FRAUD":
            alert_rows.append(
                (
                    r["transaction_id"],
                    r["user_id"],
                    alert_severity(r["fraud_probability"], r["amount_eur"]),
                    r["fraud_probability"],
                    r["amount_eur"],
                    factors,
                )
            )
    if not tx_rows:
        return 0
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.executemany(INSERT_TRANSACTION, tx_rows)
        cur.executemany(INSERT_PREDICTION, pred_rows)
        if alert_rows:
            cur.executemany(INSERT_ALERT, alert_rows)
    return len(tx_rows)


def spark_partition_writer(feature_names: list[str], dsn: str):
    """``foreachPartition`` callback writing one partition of scored rows."""

    def write(partition) -> None:
        write_scored((row.asDict() for row in partition), feature_names, dsn)

    return write


UPSERT_WINDOW = """
INSERT INTO transaction_stats_minute (window_start, window_end, transactions, amount_eur, active_users,
                                      avg_arrival_delay_s, late_arrivals)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start) DO UPDATE SET
    transactions = EXCLUDED.transactions, amount_eur = EXCLUDED.amount_eur,
    active_users = EXCLUDED.active_users, avg_arrival_delay_s = EXCLUDED.avg_arrival_delay_s,
    late_arrivals = EXCLUDED.late_arrivals
"""


def write_window_stats(rows: Iterable[dict], dsn: str | None = None) -> None:
    values = [
        (
            _utc(r["window_start"]),
            _utc(r["window_end"]),
            r["transactions"],
            r["amount_eur"],
            r["active_users"],
            _clean(r["avg_arrival_delay_s"]),
            r["late_arrivals"],
        )
        for r in rows
    ]
    if values:
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.executemany(UPSERT_WINDOW, values)


UPSERT_MODEL = """
INSERT INTO model_metadata (version, run_id, threshold, metrics, features, trained_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (version) DO UPDATE SET last_served_at = now()
"""


def record_model(spec: dict, dsn: str | None = None) -> None:
    trained_at = spec.get("trained_at")
    with connect(dsn) as conn:
        conn.execute(
            UPSERT_MODEL,
            (
                str(spec.get("version", "local")),
                spec.get("run_id"),
                spec["threshold"],
                json.dumps(spec.get("metrics", {})),
                json.dumps(spec.get("features", [])),
                datetime.fromisoformat(trained_at) if trained_at else None,
            ),
        )


def recent_user_transactions(conn: psycopg.Connection, user_id: str, since: datetime) -> list[tuple]:
    """A user's transactions after ``since`` (oldest first) — to rebuild behavioural state."""
    return conn.execute(
        """SELECT transaction_id, event_time, amount_eur, merchant_id, country, device_id
           FROM transactions WHERE user_id = %s AND event_time >= %s ORDER BY event_time""",
        (user_id, since),
    ).fetchall()


SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def ensure_schema(dsn: str | None = None) -> None:
    """Apply the (idempotent, IF NOT EXISTS) schema — lets an existing database
    pick up tables added after it was first initialised."""
    with connect(dsn) as conn:
        conn.execute(SCHEMA_FILE.read_text())


INSERT_DEAD_LETTER = """
INSERT INTO dead_letter_transactions (source_topic, kafka_partition, kafka_offset, transaction_id, reasons,
                                      raw_value, kafka_timestamp)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


def write_dead_letters(rows: Iterable[dict], dsn: str | None = None) -> int:
    values = [
        (
            r["source_topic"],
            r["kafka_partition"],
            r["kafka_offset"],
            r.get("transaction_id"),
            json.dumps(list(r["reasons"])),
            r.get("raw_value"),
            _utc(r.get("kafka_timestamp")),
        )
        for r in rows
    ]
    if values:
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.executemany(INSERT_DEAD_LETTER, values)
    return len(values)

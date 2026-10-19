"""SQL queries behind the dashboard (PostgreSQL serving store)."""

from __future__ import annotations

import pandas as pd
import psycopg

from storage.postgres import dsn_from_env


def _frame(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg.connect(dsn_from_env()) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=columns)
    for col in df.columns:  # NUMERIC arrives as Decimal
        if df[col].dtype == object and len(df) and type(df[col].iloc[0]).__name__ == "Decimal":
            df[col] = df[col].astype(float)
    return df


def kpis(minutes: int) -> dict:
    df = _frame(
        """SELECT count(*) AS transactions,
                  count(*) FILTER (WHERE prediction = 'FRAUD') AS frauds,
                  coalesce(sum(amount_eur) FILTER (WHERE prediction = 'FRAUD'), 0) AS blocked_eur,
                  coalesce(sum(amount_eur), 0) AS volume_eur,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
                  max(model_version) AS model_version
           FROM fraud_predictions WHERE scored_at >= now() - make_interval(mins => %s)""",
        (minutes,),
    )
    return df.iloc[0].to_dict()


def per_minute(minutes: int) -> pd.DataFrame:
    return _frame(
        """SELECT date_trunc('minute', event_time) AS minute,
                  count(*) AS transactions,
                  count(*) FILTER (WHERE prediction = 'FRAUD') AS frauds
           FROM fraud_predictions WHERE event_time >= now() - make_interval(mins => %s)
           GROUP BY 1 ORDER BY 1""",
        (minutes,),
    )


def probabilities(minutes: int) -> pd.DataFrame:
    return _frame(
        """SELECT fraud_probability FROM fraud_predictions
           WHERE scored_at >= now() - make_interval(mins => %s)""",
        (minutes,),
    )


def fraud_by(column: str, minutes: int, limit: int = 10) -> pd.DataFrame:
    assert column in {"country", "merchant_category"}
    return _frame(
        f"""SELECT {column} AS label, count(*) FILTER (WHERE prediction = 'FRAUD') AS frauds,
                   count(*) AS transactions
            FROM fraud_predictions WHERE scored_at >= now() - make_interval(mins => %s)
            GROUP BY 1 HAVING count(*) FILTER (WHERE prediction = 'FRAUD') > 0
            ORDER BY 2 DESC LIMIT %s""",
        (minutes, limit),
    )


def fraud_by_hour(minutes: int) -> pd.DataFrame:
    return _frame(
        """SELECT extract(hour FROM event_time)::int AS hour,
                  count(*) FILTER (WHERE prediction = 'FRAUD') AS frauds, count(*) AS transactions
           FROM fraud_predictions WHERE scored_at >= now() - make_interval(mins => %s)
           GROUP BY 1 ORDER BY 1""",
        (minutes,),
    )


def latest(limit: int = 25, only_fraud: bool = False) -> pd.DataFrame:
    where = "WHERE prediction = 'FRAUD'" if only_fraud else ""
    return _frame(
        f"""SELECT transaction_id, event_time, user_id, amount_eur, merchant_category, country,
                   fraud_probability, prediction, risk_factors
            FROM fraud_predictions {where} ORDER BY scored_at DESC LIMIT %s""",
        (limit,),
    )


def transaction_detail(transaction_id: str) -> dict | None:
    df = _frame(
        """SELECT p.*, t.merchant_id, t.device_id, t.amount, t.currency
           FROM fraud_predictions p JOIN transactions t USING (transaction_id)
           WHERE transaction_id = %s""",
        (transaction_id,),
    )
    return None if df.empty else df.iloc[0].to_dict()


def alerts(status: str, limit: int = 100) -> pd.DataFrame:
    return _frame(
        """SELECT alert_id, created_at, transaction_id, user_id, severity, fraud_probability,
                  amount_eur, risk_factors, status
           FROM fraud_alerts WHERE status = %s ORDER BY created_at DESC LIMIT %s""",
        (status, limit),
    )


def set_alert_status(alert_id: int, status: str) -> None:
    with psycopg.connect(dsn_from_env()) as conn:
        conn.execute("UPDATE fraud_alerts SET status = %s WHERE alert_id = %s", (status, alert_id))

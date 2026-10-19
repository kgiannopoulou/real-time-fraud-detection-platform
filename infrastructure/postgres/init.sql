-- Serving store for recent transactions, predictions and alerts.
-- Historical data lives in the Parquet lake; PostgreSQL holds what the
-- dashboard and API query interactively.

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id    TEXT PRIMARY KEY,
    user_id           TEXT        NOT NULL,
    merchant_id       TEXT        NOT NULL,
    merchant_category TEXT,
    amount            NUMERIC(14, 2) NOT NULL,
    currency          CHAR(3)     NOT NULL,
    amount_eur        NUMERIC(14, 2) NOT NULL,
    country           CHAR(2)     NOT NULL,
    device_id         TEXT,
    event_time        TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL
);
-- The API rebuilds a user's behavioural state from their last 24 h.
CREATE INDEX IF NOT EXISTS transactions_user_time ON transactions (user_id, event_time DESC);
CREATE INDEX IF NOT EXISTS transactions_time ON transactions (event_time DESC);

CREATE TABLE IF NOT EXISTS fraud_predictions (
    transaction_id    TEXT PRIMARY KEY,
    user_id           TEXT        NOT NULL,
    event_time        TIMESTAMPTZ NOT NULL,
    amount_eur        NUMERIC(14, 2) NOT NULL,
    merchant_category TEXT,
    country           CHAR(2),
    fraud_probability DOUBLE PRECISION NOT NULL,
    prediction        TEXT        NOT NULL CHECK (prediction IN ('FRAUD', 'LEGIT')),
    model_version     TEXT        NOT NULL,
    risk_factors      JSONB       NOT NULL DEFAULT '[]',
    shap_top          JSONB,
    features          JSONB,
    scored_at         TIMESTAMPTZ NOT NULL,
    latency_ms        DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS predictions_scored_at ON fraud_predictions (scored_at DESC);
CREATE INDEX IF NOT EXISTS predictions_fraud ON fraud_predictions (scored_at DESC) WHERE prediction = 'FRAUD';

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id          BIGSERIAL PRIMARY KEY,
    transaction_id    TEXT UNIQUE NOT NULL,
    user_id           TEXT        NOT NULL,
    severity          TEXT        NOT NULL CHECK (severity IN ('critical', 'high')),
    fraud_probability DOUBLE PRECISION NOT NULL,
    amount_eur        NUMERIC(14, 2) NOT NULL,
    risk_factors      JSONB       NOT NULL DEFAULT '[]',
    status            TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'confirmed', 'dismissed')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alerts_open ON fraud_alerts (created_at DESC) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS transaction_stats_minute (
    window_start        TIMESTAMPTZ PRIMARY KEY,
    window_end          TIMESTAMPTZ NOT NULL,
    transactions        INTEGER     NOT NULL,
    amount_eur          NUMERIC(16, 2) NOT NULL,
    active_users        INTEGER     NOT NULL,
    avg_arrival_delay_s DOUBLE PRECISION,
    late_arrivals       INTEGER     NOT NULL
);

CREATE TABLE IF NOT EXISTS model_metadata (
    version          TEXT PRIMARY KEY,
    run_id           TEXT,
    threshold        DOUBLE PRECISION NOT NULL,
    metrics          JSONB,
    features         JSONB,
    trained_at       TIMESTAMPTZ,
    first_served_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_served_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

#!/usr/bin/env bash
# Single-container Airflow for the local stack: scheduler + webserver with the
# LocalExecutor and its metadata database in the platform's PostgreSQL.
set -euo pipefail

python - <<'PY'
import os, psycopg2
dsn = os.environ["AIRFLOW_ADMIN_DSN"]
conn = psycopg2.connect(dsn); conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if not cur.fetchone():
    cur.execute("CREATE DATABASE airflow")
    print("created database airflow")
PY

airflow db migrate
airflow users create --username admin --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin --lastname User --role Admin --email admin@example.com >/dev/null 2>&1 || true

airflow scheduler &
exec airflow webserver --port 8080

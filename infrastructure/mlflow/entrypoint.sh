#!/usr/bin/env bash
# Create the MLflow database on first start, then run the tracking server.
#   MLFLOW_BACKEND_STORE_URI      postgresql://user:pass@host:5432/mlflow
#   MLFLOW_ARTIFACTS_DESTINATION  s3://bucket/mlflow
#   MLFLOW_ALLOWED_HOSTS          comma-separated Host headers
set -euo pipefail
python - <<'PY'
import os, psycopg2
from urllib.parse import urlparse
url = urlparse(os.environ["MLFLOW_BACKEND_STORE_URI"])
conn = psycopg2.connect(host=url.hostname, port=url.port or 5432, user=url.username,
                        password=url.password, dbname="postgres")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (url.path.lstrip("/"),))
if not cur.fetchone():
    cur.execute(f'CREATE DATABASE "{url.path.lstrip("/")}"')
PY
exec mlflow server --host 0.0.0.0 --port 5000 \
  --backend-store-uri "$MLFLOW_BACKEND_STORE_URI" \
  --artifacts-destination "$MLFLOW_ARTIFACTS_DESTINATION" --serve-artifacts \
  --allowed-hosts "$MLFLOW_ALLOWED_HOSTS"

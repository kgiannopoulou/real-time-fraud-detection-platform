#!/usr/bin/env bash
# Run the Spark tests inside the platform's Spark image (no local Java needed).
#   docker compose build spark && scripts/test_spark.sh
set -euo pipefail
cd "$(dirname "$0")/.."
docker run --rm -v "$PWD:/work" -w /work --entrypoint bash \
  -e HOME=/tmp -e PYTHONWARNINGS=ignore::FutureWarning fraud-platform/spark:latest -c '
    export PYTHONPATH=/work:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip)
    pip install -q --user pytest scikit-learn lightgbm confluent-kafka
    python3 -m pytest -q -p no:cacheprovider -m spark tests/'

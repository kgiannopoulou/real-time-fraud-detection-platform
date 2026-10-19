"""Shared fixtures.

Markers:
* ``integration`` — needs the docker compose services (Kafka on localhost:9094,
  PostgreSQL on localhost:5436); skipped automatically when unreachable.
* ``spark`` — needs ``pyspark`` and a Java runtime; skipped otherwise.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import socket
from datetime import datetime, timezone

import pandas as pd
import pytest

from ml.dataset import build_synthetic


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    kafka = _port_open("localhost", int(os.getenv("KAFKA_PORT", "9094")))
    postgres = _port_open("localhost", int(os.getenv("POSTGRES_PORT", "5436")))
    spark = importlib.util.find_spec("pyspark") is not None and (
        shutil.which("java") is not None or os.getenv("JAVA_HOME")
    )
    for item in items:
        if "integration" in item.keywords and not (kafka and postgres):
            item.add_marker(pytest.mark.skip(reason="Kafka/PostgreSQL not reachable"))
        if "spark" in item.keywords and not spark:
            item.add_marker(pytest.mark.skip(reason="pyspark/Java not available"))


@pytest.fixture(scope="session")
def history() -> pd.DataFrame:
    """A small but realistic labelled history (shared, read-only)."""
    return build_synthetic(days=0.75, n_users=200, seed=11, start=datetime(2026, 9, 1, tzinfo=timezone.utc))


@pytest.fixture(scope="session")
def trained(history, tmp_path_factory):
    """A quick XGBoost model trained on ``history`` and saved as serving artefacts."""
    from ml.predict import FraudModel, save_artifacts
    from ml.train import build_spec, prepare, train

    prepared = prepare(history)
    run = train(
        seed=1, xgb_params={"n_estimators": 120}, families=("xgboost",), prepared=prepared, verbose=False
    )
    directory = tmp_path_factory.mktemp("model")
    save_artifacts(run["booster"], build_spec(run, version="test"), directory)
    return {"run": run, "dir": directory, "model": FraudModel.load(directory), "prepared": prepared}

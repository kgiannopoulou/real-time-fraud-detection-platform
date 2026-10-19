"""Make sure a champion model exists before the serving components start.

    python -m ml.bootstrap

Run once by ``docker compose up`` (service ``model-bootstrap``): on a fresh
checkout the MLflow registry is empty, so the labelled dataset is generated
and a first model is trained, registered and given the ``champion`` alias.
If a champion already exists this is a no-op.
"""

from __future__ import annotations

import os
import sys
import time

from ml.dataset import SYNTHETIC_PATH
from ml.registry import champion_version


def wait_for_registry(tracking_uri: str, timeout: float = 120) -> None:
    import urllib.request

    deadline = time.monotonic() + timeout
    while True:
        try:
            urllib.request.urlopen(f"{tracking_uri}/health", timeout=5)
            return
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(3)


def main() -> int:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        print("MLFLOW_TRACKING_URI not set", file=sys.stderr)
        return 1
    wait_for_registry(tracking_uri)
    try:
        version, _ = champion_version(tracking_uri)
        print(f"champion already registered: fraud-detector v{version}")
        return 0
    except Exception:
        print("no champion model yet - bootstrapping")

    if not SYNTHETIC_PATH.exists():
        from ml.dataset import main as build_dataset

        build_dataset([])
    from ml.train import main as train

    train(["--tracking-uri", tracking_uri, "--promote"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Lightweight MLflow model-registry client used by serving components.

Kept separate from ``ml/tracking.py`` so the Spark image and the API only
need ``mlflow-skinny`` — no plotting or training dependencies.
"""

from __future__ import annotations

from pathlib import Path

MODEL_NAME = "fraud-detector"
CHAMPION = "champion"


def champion_version(tracking_uri: str, alias: str = CHAMPION) -> tuple[str, str]:
    """(version, run_id) of the model version carrying ``alias``."""
    from mlflow.tracking import MlflowClient

    version = MlflowClient(tracking_uri=tracking_uri).get_model_version_by_alias(MODEL_NAME, alias)
    return str(version.version), version.run_id


def download_champion(tracking_uri: str, destination: Path | str, alias: str = CHAMPION) -> Path:
    """Download the serving artefacts (booster JSON + feature spec) of the aliased version."""
    import mlflow

    version, run_id = champion_version(tracking_uri, alias)
    destination = Path(destination) / version
    destination.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(tracking_uri)
    path = mlflow.artifacts.download_artifacts(
        run_id=run_id, artifact_path="serving", dst_path=str(destination)
    )
    return Path(path)

"""MLflow experiment tracking and model registry integration.

Every training run records its parameters, test metrics, evaluation plots
and the serving artefacts. The XGBoost model is registered as
``fraud-detector``; serving components resolve the version carrying the
``champion`` alias.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import mlflow  # noqa: E402
from mlflow.tracking import MlflowClient  # noqa: E402
from sklearn.metrics import PrecisionRecallDisplay  # noqa: E402

from ml.evaluate import Metrics  # noqa: E402
from ml.feature_pipeline import TARGET  # noqa: E402
from ml.predict import save_artifacts  # noqa: E402

EXPERIMENT = "fraud-detection"
MODEL_NAME = "fraud-detector"
CHAMPION = "champion"


class Tracker:
    def __init__(self, tracking_uri: str, experiment: str = EXPERIMENT):
        mlflow.set_tracking_uri(tracking_uri)
        exp = mlflow.set_experiment(experiment)
        self.client = MlflowClient()
        # Open the experiment in the UI's "model training" view.
        self.client.set_experiment_tag(exp.experiment_id, "mlflow.experimentKind", "custom_model_development")

    def log_run(
        self,
        run: dict,
        spec: dict,
        run_name: str,
        register: bool = True,
        promote: bool = False,
        tags: dict | None = None,
    ) -> str | None:
        """Log one training run; returns the registered model version (if any)."""
        xgb_metrics: Metrics = run["results"]["xgboost"]["metrics"]
        with mlflow.start_run(run_name=run_name, tags=tags) as active:
            mlflow.log_params({k: v for k, v in run["params"].items()})
            mlflow.log_params({"train_rows": run["train_rows"], "features": ",".join(spec["features"])})
            mlflow.log_metrics(_metric_dict(xgb_metrics))
            mlflow.log_metric("valid_pr_auc", run["results"]["xgboost"]["valid_pr_auc"])
            # Comparison models are logged with a family prefix on the same run.
            for family, result in run["results"].items():
                if family != "xgboost":
                    mlflow.log_metrics(_metric_dict(result["metrics"], prefix=f"{family}_"))

            with tempfile.TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                self._log_plots(run, tmp_path)

                version = None
                if register:
                    info = mlflow.xgboost.log_model(
                        run["booster"], name="model", registered_model_name=MODEL_NAME
                    )
                    version = str(info.registered_model_version)
                    spec = {**spec, "version": version, "run_id": active.info.run_id}
                    self.client.set_model_version_tag(
                        MODEL_NAME, version, "pr_auc", f"{xgb_metrics.pr_auc:.4f}"
                    )
                # Portable serving artefacts (booster JSON + feature spec) used by
                # Spark and the API without importing MLflow model flavours.
                save_artifacts(run["booster"], spec, tmp_path / "serving")
                mlflow.log_artifacts(str(tmp_path / "serving"), artifact_path="serving")

        if version and (promote or self.champion_version() is None):
            self.promote(version)
        return version

    def _log_plots(self, run: dict, out: Path) -> None:
        test = run["test"]
        fig, ax = plt.subplots(figsize=(6, 5))
        for family, result in run["results"].items():
            PrecisionRecallDisplay.from_predictions(test[TARGET], result["scores"], name=family, ax=ax)
        ax.set_title("Precision-recall (test split)")
        fig.tight_layout()
        fig.savefig(out / "pr_curve.png", dpi=110)
        plt.close(fig)

        m: Metrics = run["results"]["xgboost"]["metrics"]
        (out / "confusion_matrix.txt").write_text(m.confusion_matrix_text())
        (out / "metrics.json").write_text(json.dumps(m.as_dict(), indent=2))
        mlflow.log_artifact(str(out / "pr_curve.png"), artifact_path="evaluation")
        mlflow.log_artifact(str(out / "confusion_matrix.txt"), artifact_path="evaluation")
        mlflow.log_artifact(str(out / "metrics.json"), artifact_path="evaluation")

    def champion_version(self) -> str | None:
        try:
            return str(self.client.get_model_version_by_alias(MODEL_NAME, CHAMPION).version)
        except mlflow.exceptions.MlflowException:
            return None

    def promote(self, version: str) -> None:
        self.client.set_registered_model_alias(MODEL_NAME, CHAMPION, version)


def _metric_dict(m: Metrics, prefix: str = "") -> dict[str, float]:
    keys = ("roc_auc", "pr_auc", "precision", "recall", "f1", "threshold")
    return {f"{prefix}{k}": float(getattr(m, k)) for k in keys}


def download_champion(tracking_uri: str, destination: Path | str, alias: str = CHAMPION) -> Path:
    """Download the serving artefacts of the aliased model version."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    version = client.get_model_version_by_alias(MODEL_NAME, alias)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    path = mlflow.artifacts.download_artifacts(
        run_id=version.run_id, artifact_path="serving", dst_path=str(destination)
    )
    return Path(path)

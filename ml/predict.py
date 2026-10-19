"""Load the trained fraud model and score feature rows.

The model is persisted in a portable form — the XGBoost booster as JSON plus a
``feature_spec.json`` describing feature order, risk encodings and the
decision threshold — so the exact same artefact can be loaded by the batch
pipeline, the Spark streaming job and the API regardless of their Python or
scikit-learn versions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.feature_pipeline import RiskTables

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"
MODEL_FILE = "model.json"
SPEC_FILE = "feature_spec.json"


@dataclass
class FraudModel:
    booster: xgb.Booster
    features: list[str]
    threshold: float
    risk: RiskTables
    version: str = "local"

    @classmethod
    def load(cls, directory: Path | str = ARTIFACT_DIR) -> FraudModel:
        directory = Path(directory)
        spec = json.loads((directory / SPEC_FILE).read_text())
        booster = xgb.Booster()
        booster.load_model(str(directory / MODEL_FILE))
        return cls(
            booster=booster,
            features=spec["features"],
            threshold=spec["threshold"],
            risk=RiskTables.from_dict(spec["risk_tables"]),
            version=str(spec.get("version", "local")),
        )

    def _matrix(self, rows: pd.DataFrame) -> xgb.DMatrix:
        return xgb.DMatrix(rows[self.features].astype(float), feature_names=self.features)

    def predict_proba(self, rows: pd.DataFrame) -> np.ndarray:
        if rows.empty:
            return np.empty(0)
        return self.booster.predict(self._matrix(rows))

    def predict(self, rows: pd.DataFrame) -> pd.DataFrame:
        proba = self.predict_proba(rows)
        return pd.DataFrame(
            {
                "fraud_probability": proba,
                "prediction": np.where(proba >= self.threshold, "FRAUD", "LEGIT"),
            },
            index=rows.index,
        )


def save_artifacts(
    booster: xgb.Booster, spec: dict, directory: Path | str = ARTIFACT_DIR
) -> tuple[Path, Path]:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    model_path, spec_path = directory / MODEL_FILE, directory / SPEC_FILE
    booster.save_model(str(model_path))
    spec_path.write_text(json.dumps(spec, indent=2))
    return model_path, spec_path


def main() -> None:
    """Score the most recent transactions of the training dataset."""
    from ml.dataset import load_synthetic
    from ml.feature_pipeline import build_features

    model = FraudModel.load()
    df = load_synthetic().sort_values("timestamp")
    features = build_features(df, model.risk).tail(10)
    scored = features.join(model.predict(features))
    cols = ["transaction_id", "user_id", "amount", "merchant_category", "country"]
    print(scored[cols + ["fraud_probability", "prediction", "is_fraud"]].to_string(index=False))


if __name__ == "__main__":
    main()

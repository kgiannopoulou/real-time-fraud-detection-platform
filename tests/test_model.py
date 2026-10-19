from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from ml.evaluate import best_f1_threshold, evaluate, expected_cost, threshold_table
from ml.explain import describe, risk_factors, shap_values
from ml.feature_pipeline import FEATURES, TARGET, add_risk_features
from ml.monitoring import PSI_ALERT, feature_baseline, psi
from ml.predict import MODEL_FILE, SPEC_FILE, FraudModel, load_serving_model


def test_metrics_and_confusion_matrix():
    y = np.array([0, 0, 0, 1, 1])
    scores = np.array([0.1, 0.2, 0.8, 0.9, 0.3])
    m = evaluate(y, scores, threshold=0.5)
    assert (m.tp, m.fp, m.tn, m.fn) == (1, 1, 2, 1)
    assert m.precision == pytest.approx(0.5) and m.recall == pytest.approx(0.5)
    assert 0 <= m.pr_auc <= 1 and 0 <= m.roc_auc <= 1


def test_threshold_and_cost_helpers():
    y = np.array([0, 0, 1, 1])
    scores = np.array([0.1, 0.4, 0.35, 0.8])
    assert best_f1_threshold(y, scores) in scores
    amounts = np.array([10, 10, 500, 100])
    # threshold 0.9 flags nothing: both frauds missed
    assert expected_cost(y, scores, amounts, 0.9, review_cost=5) == 600
    # threshold 0.3 flags three: one false positive, nothing missed
    assert expected_cost(y, scores, amounts, 0.3, review_cost=5) == 15
    assert len(threshold_table(y, scores, amounts, thresholds=(0.3, 0.9))) == 2


def test_model_beats_the_trivial_baseline(trained):
    metrics = trained["run"]["results"]["xgboost"]["metrics"]
    fraud_rate = trained["prepared"][3][TARGET].mean()
    assert metrics.pr_auc > 5 * fraud_rate  # a random model scores ~fraud_rate
    assert metrics.roc_auc > 0.9


def test_artefacts_round_trip(trained):
    directory = trained["dir"]
    spec = json.loads((directory / SPEC_FILE).read_text())
    assert spec["features"] == FEATURES
    assert set(spec["baseline"]) == set(FEATURES)
    model = FraudModel.from_bytes((directory / MODEL_FILE).read_bytes(), (directory / SPEC_FILE).read_text())
    assert model.version == "test" and model.threshold == pytest.approx(spec["threshold"])


def test_predictions_and_decisions(trained):
    model = trained["model"]
    test = trained["prepared"][3]
    out = model.predict(test)
    assert out["fraud_probability"].between(0, 1).all()
    assert set(out["prediction"]) <= {"FRAUD", "LEGIT"}
    flagged = out["fraud_probability"] >= model.threshold
    assert (out.loc[flagged, "prediction"] == "FRAUD").all()
    assert model.predict(test.iloc[0:0]).empty


def test_serving_model_falls_back_to_local_artefacts(trained, monkeypatch):
    monkeypatch.setenv("MODEL_DIR", str(trained["dir"]))
    assert load_serving_model(tracking_uri=None).version == "test"


def test_explanations_name_the_drivers(trained):
    model = trained["model"]
    test = trained["prepared"][3]
    top = test.assign(p=model.predict_proba(test)).sort_values("p", ascending=False).head(10)
    contributions = shap_values(model.booster, top, model.features)
    assert contributions.shape == (10, len(model.features))
    factors, shap_top = model.explain(top)
    assert all(isinstance(f, list) and len(f) <= 4 for f in factors)
    assert sum(len(f) for f in factors) > 0  # high-risk rows come with reasons
    assert all(len(s) == 5 for s in shap_top)


def test_risk_factor_wording():
    row = {"country": "NG", "device_id": "device_x", "merchant_category": "jewelry"}
    assert describe("amount_ratio", 8.2, row) == "amount 8.2x higher than the customer's 24h average"
    assert describe("tx_count_2m", 5, row) == "5 transactions in the previous 2 minutes"
    assert describe("is_new_country", 1, row) == "first transaction from NG"
    assert describe("is_new_device", 0, row) is None
    frame = pd.DataFrame([{**row, "amount_ratio": 8.2, "tx_count_2m": 5.0}])
    reasons = risk_factors(np.array([[2.0, 0.05]]), frame, ["amount_ratio", "tx_count_2m"])
    assert reasons == [["amount 8.2x higher than the customer's 24h average"]]


def test_psi_detects_shift_but_not_noise():
    rng = np.random.default_rng(0)
    base = feature_baseline(rng.lognormal(3.5, 0.8, 20_000))
    assert psi(base, rng.lognormal(3.5, 0.8, 5_000)) < 0.02
    assert psi(base, 2 * rng.lognormal(3.5, 0.8, 5_000)) > PSI_ALERT


def test_psi_ignores_missing_values_by_default():
    base = feature_baseline(np.r_[np.arange(1000.0), [np.nan] * 10])
    shifted_coverage = np.r_[np.arange(1000.0), [np.nan] * 2000]
    assert psi(base, shifted_coverage) < 0.01
    assert psi(base, shifted_coverage, include_missing=True) > PSI_ALERT


def test_risk_features_use_the_models_encodings(trained):
    model = trained["model"]
    frame = pd.DataFrame({"merchant_category": ["jewelry", "never-seen"], "country": ["GR", "ZZ"]})
    add_risk_features(frame, model.risk)
    assert frame.loc[1, "merchant_risk"] == pytest.approx(model.risk.prior)
    assert frame.loc[0, "merchant_risk"] == pytest.approx(model.risk.merchant_category["jewelry"])

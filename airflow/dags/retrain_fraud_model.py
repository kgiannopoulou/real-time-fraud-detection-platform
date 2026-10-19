"""Weekly fraud-model retraining.

Airflow orchestrates *batch* work only. The streaming path (Kafka -> Spark ->
scoring) runs continuously on its own; this DAG produces new model versions
and decides whether one replaces the production champion:

    bootstrap_dataset -> validate_data -> extract_training_data -> feature_engineering
      -> train_model -> evaluate_model -> compare_against_production
                                            |-> promote_model   (candidate wins)
                                            '-> keep_champion   (candidate loses)

Every task runs one step of ``ml/retrain.py`` in an isolated ML virtualenv and
exchanges data through files in a per-run directory, so tasks are retryable
and their outputs are inspectable after the fact. Promotion only moves the
MLflow ``champion`` alias; the Spark job hot-swaps the model within a minute.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

ML_PYTHON = "/opt/ml-venv/bin/python"
PROJECT = "/opt/fraud"
RUN_DIR = PROJECT + "/data/processed/runs/{{ run_id | replace(':', '-') | replace('+', '-') }}"


def step(task_id: str, command: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT} && {ML_PYTHON} -m ml.retrain {command} --run-dir '{RUN_DIR}'",
        **kwargs,
    )


def choose_branch(run_dir: str) -> str:
    evaluation = json.loads((Path(run_dir) / "evaluation.json").read_text())
    candidate = evaluation["candidate"]["pr_auc"]
    champion = evaluation.get("champion", {}).get("pr_auc")
    print(f"candidate PR-AUC {candidate:.4f} vs champion {champion} (margin {evaluation['margin']})")
    return "promote_model" if evaluation["promote"] else "keep_champion"


with DAG(
    dag_id="retrain_fraud_model",
    description="Validate data, retrain, evaluate against production and promote if better",
    schedule="0 3 * * 0",  # Sundays 03:00 UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2), "owner": "ml-platform"},
    tags=["fraud", "ml", "retraining"],
    doc_md=__doc__,
) as dag:
    bootstrap = BashOperator(
        task_id="bootstrap_dataset",
        bash_command=f"cd {PROJECT} && (test -f data/raw/transactions.parquet || {ML_PYTHON} -m ml.dataset)",
    )
    validate = step("validate_data", "validate")
    extract = step("extract_training_data", "extract")
    features = step("feature_engineering", "features")
    train = step("train_model", "train", execution_timeout=timedelta(minutes=30))
    evaluate = step("evaluate_model", "evaluate")
    compare = BranchPythonOperator(
        task_id="compare_against_production",
        python_callable=choose_branch,
        op_kwargs={"run_dir": RUN_DIR},
    )
    promote = step("promote_model", "promote")
    keep = EmptyOperator(task_id="keep_champion")

    bootstrap >> validate >> extract >> features >> train >> evaluate >> compare >> [promote, keep]

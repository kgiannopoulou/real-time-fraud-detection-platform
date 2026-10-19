# Tests

```bash
pytest                                   # unit tests (+ integration tests if the stack is up)
docker compose up -d kafka kafka-init postgres && pytest -m integration
scripts/test_spark.sh                    # Spark tests inside the Spark image (no local Java needed)
python scripts/smoke_test.py             # end-to-end against the full docker compose stack
```

| File | Covers |
| --- | --- |
| `test_generator.py` | determinism per seed, event contract, labelled fraud scenarios, fresh ids per stream run, every injected corruption is rejected, late/duplicate injection |
| `test_validation.py` | every data-quality rule, clock-skew tolerance, multiple violations, malformed payloads |
| `test_features.py` | velocity windows, amount ratio, new country/device, **out-of-order events get the same features as in-order**, duplicate detection, bounded + serializable state, **offline pipeline == streaming replay (train/serve parity)**, risk encodings, chronological split |
| `test_model.py` | metrics and confusion matrix, cost/threshold helpers, model beats the trivial baseline, artefact round-trip, decisions vs threshold, local fallback, SHAP explanations and wording, PSI drift detection |
| `test_integration.py` (`-m integration`) | Kafka round-trip keeps key + schema header, idempotent PostgreSQL upserts and alerting, idempotent dead letters, API scoring with history rebuilt from PostgreSQL, input validation (422), metrics endpoint |
| `test_streaming.py` (`-m spark`) | Spark validation **agrees with the Python rules** for every corruption, currency normalisation, stateful features carried across micro-batches via checkpoints, duplicate flagging, scoring + explanations on streamed features |

Integration and Spark tests skip themselves when Kafka/PostgreSQL or
pyspark/Java are not available, so `pytest` always runs on a laptop.

Latest local run: 55 passed (unit + integration), 3 Spark tests passed in the
Spark image, smoke test passed against a freshly created stack.

# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- README: failure scenarios, production considerations, 100× scale notes and SQL overview.
- MIT licence, `CONTRIBUTING.md` and this changelog.

## [1.0.0] - 2026-10-20

First complete release: the full platform runs with one `docker compose up`.

### Added
- Synthetic transaction generator with injected fraud scenarios and realistic confounders; EDA.
- Baseline models (Logistic Regression, Random Forest, XGBoost, LightGBM) on a chronological split, compared on PR-AUC.
- MLflow experiment tracking and model registry with a `champion` alias.
- Kafka ingestion with an idempotent, keyed producer and fault injection (late, duplicate, invalid events, drift).
- Spark Structured Streaming job with explicit schemas, event-time watermarks and a late-event side output.
- Stateful per-card behavioural features shared by training and streaming (no train/serve skew).
- Real-time scoring with a broadcast, hot-swappable champion model and TreeSHAP explanations.
- PostgreSQL serving store with idempotent upserts, FastAPI scoring/read API, Streamlit dashboard.
- Airflow retraining DAG with champion/challenger promotion.
- 11 data-quality rules and a dead-letter queue.
- Prometheus/Grafana monitoring: pipeline metrics, score distribution, PSI drift, six alert rules.
- Unit, integration and Spark tests; GitHub Actions CI with Docker builds and an end-to-end smoke test.
- AWS deployment design and Terraform (VPC, MSK, RDS, S3, ECS Fargate, Managed Prometheus).
- Architecture diagram, benchmark results and documentation.

[Unreleased]: https://github.com/kgiannopoulou/real-time-fraud-detection-platform/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/kgiannopoulou/real-time-fraud-detection-platform/releases/tag/v1.0.0

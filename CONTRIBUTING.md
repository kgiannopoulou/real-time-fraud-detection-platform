# Contributing

Thanks for taking a look. This is a personal portfolio project, but issues and
pull requests are welcome — bug reports with a reproduction are the most useful.

## Development setup

Requirements: Docker (with Compose v2), Python 3.11+, ~8 GB of free RAM for the
full stack.

```bash
git clone https://github.com/kgiannopoulou/real-time-fraud-detection-platform.git
cd real-time-fraud-detection-platform
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
docker compose up -d --build                      # the whole platform (13 services)
python scripts/smoke_test.py                      # verify it end to end
```

## Tests

| Command | What it runs | Needs |
| --- | --- | --- |
| `pytest -m "not integration and not spark"` | Unit tests (features, validation, model, generator) | Python only |
| `pytest -m integration` | Kafka + PostgreSQL round trips | `docker compose up -d kafka postgres` |
| `scripts/test_spark.sh` | Spark Structured Streaming tests in the Spark image | Docker |
| `python scripts/smoke_test.py` | Health checks + Kafka → Spark → PostgreSQL → API | Full stack running |

Lint and format with `ruff check .` and `ruff format .` — CI fails on either.

## Making a change

1. Open an issue first for anything bigger than a small fix, so we can agree on the approach.
2. Create a branch from `main`: `feat/<short-name>`, `fix/<short-name>` or `docs/<short-name>`.
3. Keep feature logic shared between training and streaming in `streaming/features.py` —
   a test asserts both paths produce identical features; don't fork it.
4. Add or update tests with the change. If you touch the schema, update
   `storage/schema.sql` and the writers together.
5. Use [Conventional Commits](https://www.conventionalcommits.org/):
   `feat: add device-sharing feature`, `fix: keep model on registry timeout`,
   `test: cover late events in the DLQ`.
6. Open a pull request describing *what* changed, *why*, and how you tested it.
   CI (lint → unit → integration → Spark → image builds → end-to-end smoke test)
   must be green before merge.

## Reporting security issues

Please don't open a public issue for vulnerabilities — use GitHub's private
vulnerability reporting on this repository instead.

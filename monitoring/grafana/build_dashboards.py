"""Generate the provisioned Grafana dashboards.

    python monitoring/grafana/build_dashboards.py

Dashboards are code: panels are declared here and rendered to JSON in
``monitoring/grafana/dashboards/``, which Grafana loads at start-up.
"""

from __future__ import annotations

import json
from itertools import count
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}

# Dark-mode categorical slots + the reserved "critical" status color.
BLUE, ORANGE, AQUA, YELLOW, VIOLET = "#3987e5", "#d95926", "#199e70", "#c98500", "#9085e9"
CRITICAL, MUTED = "#d03b3b", "#8a897f"

_ids = count(1)


def target(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": ref}


def stat(
    title: str,
    expr: str,
    x: int,
    y: int,
    unit: str = "short",
    w: int = 4,
    decimals: int | None = None,
    color: str = BLUE,
    legend: str = "",
) -> dict:
    return {
        "id": next(_ids),
        "type": "stat",
        "title": title,
        "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": w, "h": 4},
        "targets": [target(expr, legend)],
        "options": {
            "colorMode": "value",
            "graphMode": "area",
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "textMode": "value_and_name" if legend else "value",
        },
        "fieldConfig": {
            "defaults": {"unit": unit, "decimals": decimals, "color": {"mode": "fixed", "fixedColor": color}},
            "overrides": [],
        },
    }


def series(
    title: str,
    targets: list[dict],
    x: int,
    y: int,
    unit: str = "short",
    w: int = 12,
    h: int = 8,
    colors: dict[str, str] | None = None,
    thresholds: list[tuple[float, str]] | None = None,
    stack: bool = False,
    bars: bool = False,
) -> dict:
    overrides = [
        {
            "matcher": {"id": "byName", "options": name},
            "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}],
        }
        for name, c in (colors or {}).items()
    ]
    defaults = {
        "unit": unit,
        "color": {"mode": "palette-classic"},
        "custom": {
            "lineWidth": 2,
            "fillOpacity": 70 if bars else 12,
            "showPoints": "never",
            "drawStyle": "bars" if bars else "line",
            "stacking": {"mode": "normal" if stack else "none"},
            "thresholdsStyle": {"mode": "dashed" if thresholds else "off"},
        },
    }
    if thresholds:
        defaults["thresholds"] = {
            "mode": "absolute",
            "steps": [{"color": "transparent", "value": None}]
            + [{"color": c, "value": v} for v, c in thresholds],
        }
    return {
        "id": next(_ids),
        "type": "timeseries",
        "title": title,
        "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{**t, "refId": chr(65 + i)} for i, t in enumerate(targets)],
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "fieldConfig": {"defaults": defaults, "overrides": overrides},
    }


def row(title: str, y: int) -> dict:
    return {
        "id": next(_ids),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def dashboard(uid: str, title: str, panels: list[dict], description: str) -> dict:
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["fraud"],
        "timezone": "utc",
        "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"},
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "panels": panels,
    }


def quantile(q: float, metric: str, by: str = "") -> str:
    group = f"le{', ' + by if by else ''}"
    return f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[2m])))"


def p95(metric: str, by: str = "") -> str:
    return quantile(0.95, metric, by)


def platform() -> dict:
    scored = "sum(rate(fraud_stream_scored_total[1m]))"
    panels = [
        row("Throughput & latency", 0),
        stat("Scored / s", scored, 0, 1, "reqps", decimals=1),
        stat(
            "Kafka lag (offsets)",
            'max(fraud_stream_kafka_offsets_behind_latest{query="scoring"})',
            4,
            1,
            color=ORANGE,
        ),
        stat(
            "p95 ingest → score", p95("fraud_stream_e2e_latency_seconds"), 8, 1, "s", decimals=2, color=AQUA
        ),
        stat(
            "p95 inference / tx",
            p95("fraud_stream_scoring_seconds_per_row"),
            12,
            1,
            "s",
            decimals=4,
            color=VIOLET,
        ),
        stat(
            "Dead letters / s",
            "sum(rate(fraud_stream_dead_letters_total[1m]))",
            16,
            1,
            "reqps",
            decimals=2,
            color=YELLOW,
        ),
        stat(
            "Fraud flagged / min",
            'sum(rate(fraud_stream_scored_total{prediction="FRAUD"}[5m])) * 60',
            20,
            1,
            decimals=1,
            color=CRITICAL,
        ),
        series(
            "Transactions per second",
            [
                target("sum(rate(fraud_producer_events_total[1m]))", "produced"),
                target(
                    'sum(rate(kafka_topic_partition_current_offset{topic="transactions"}[1m]))',
                    "ingested (Kafka)",
                ),
                target(scored, "scored"),
            ],
            0,
            5,
            "reqps",
            colors={"produced": MUTED, "ingested (Kafka)": BLUE, "scored": AQUA},
        ),
        series(
            "End-to-end latency (Kafka ingest → scored)",
            [
                target(quantile(0.50, "fraud_stream_e2e_latency_seconds"), "p50"),
                target(p95("fraud_stream_e2e_latency_seconds"), "p95"),
                target(quantile(0.99, "fraud_stream_e2e_latency_seconds"), "p99"),
            ],
            12,
            5,
            "s",
            colors={"p50": BLUE, "p95": ORANGE, "p99": CRITICAL},
            thresholds=[(10, CRITICAL)],
        ),
        row("Streaming engine", 13),
        series(
            "Kafka lag by query (offsets behind latest)",
            [target("max by (query) (fraud_stream_kafka_offsets_behind_latest)", "{{query}}")],
            0,
            14,
        ),
        series(
            "Micro-batch duration p95 by query",
            [target(p95("fraud_stream_batch_duration_seconds", "query"), "{{query}}")],
            12,
            14,
            "s",
        ),
        series("State store rows", [target("sum by (query) (fraud_stream_state_rows)", "{{query}}")], 0, 22),
        series(
            "Rows dropped by watermark / s",
            [target("sum by (query) (rate(fraud_stream_rows_dropped_by_watermark_total[5m]))", "{{query}}")],
            12,
            22,
            "reqps",
        ),
        row("Data quality", 30),
        series(
            "Dead letters by reason",
            [
                target("sum by (reason) (rate(fraud_stream_dead_letters_total[2m])) * 60", "{{reason}}"),
            ],
            0,
            31,
            "short",
            stack=True,
            bars=True,
        ),
        series(
            "Inference time per transaction",
            [
                target(p95("fraud_stream_scoring_seconds_per_row"), "p95"),
            ],
            12,
            31,
            "s",
            colors={"p95": VIOLET},
        ),
        row("Resources", 39),
        series(
            "CPU (cores)",
            [target("sum by (job) (rate(process_cpu_seconds_total[1m]))", "{{job}}")],
            0,
            40,
            "short",
        ),
        series(
            "Memory",
            [
                target("sum by (job) (process_resident_memory_bytes)", "{{job}} RSS"),
                target("max(metrics_fraud_stream_driver_jvm_heap_used_Value)", "spark JVM heap"),
            ],
            12,
            40,
            "bytes",
        ),
    ]
    return dashboard(
        "fraud-platform",
        "Fraud Platform — Streaming",
        panels,
        "Throughput, Kafka lag, latency, streaming state, data quality and resources",
    )


def ml() -> dict:
    panels = [
        row("Model", 0),
        stat("Serving model version", "fraud_model_version", 0, 1, w=6, color=BLUE),
        stat(
            "Predicted fraud rate",
            "fraud_model_fraud_rate",
            6,
            1,
            "percentunit",
            decimals=2,
            w=6,
            color=CRITICAL,
        ),
        stat(
            "Training fraud rate",
            "fraud_model_training_fraud_rate",
            12,
            1,
            "percentunit",
            decimals=2,
            w=6,
            color=MUTED,
        ),
        stat("Max feature PSI", "max(fraud_feature_psi)", 18, 1, decimals=3, w=6, color=ORANGE),
        series(
            "Fraud rate: production vs training",
            [
                target("fraud_model_fraud_rate", "production (rolling 5k)"),
                target("fraud_model_training_fraud_rate", "training"),
            ],
            0,
            5,
            "percentunit",
            colors={"production (rolling 5k)": CRITICAL, "training": MUTED},
        ),
        series(
            "Transactions per minute by score band (tail of the distribution)",
            [
                target(
                    'sum(increase(fraud_model_score_bucket{le="0.5"}[1m]))'
                    ' - sum(increase(fraud_model_score_bucket{le="0.1"}[1m]))',
                    "score 0.1-0.5",
                ),
                target(
                    'sum(increase(fraud_model_score_bucket{le="+Inf"}[1m]))'
                    ' - sum(increase(fraud_model_score_bucket{le="0.5"}[1m]))',
                    "score >= 0.5",
                ),
            ],
            12,
            5,
            "short",
            colors={"score 0.1-0.5": YELLOW, "score >= 0.5": CRITICAL},
            stack=True,
            bars=True,
        ),
        row("Feature drift", 13),
        series(
            "Population Stability Index by feature",
            [target("fraud_feature_psi", "{{feature}}")],
            0,
            14,
            "short",
            thresholds=[(0.1, YELLOW), (0.25, CRITICAL)],
        ),
        series(
            "Average transaction amount (EUR): production vs training",
            [
                target('fraud_feature_mean{feature="amount_eur", source="production"}', "production"),
                target('fraud_feature_mean{feature="amount_eur", source="training"}', "training"),
            ],
            12,
            14,
            "currencyEUR",
            colors={"production": BLUE, "training": MUTED},
        ),
        row("Online API", 22),
        series(
            "API requests / s by route",
            [target("sum by (route) (rate(fraud_api_requests_total[1m]))", "{{route}}")],
            0,
            23,
            "reqps",
        ),
        series(
            "API p95 latency by route",
            [target(p95("fraud_api_request_seconds", "route"), "{{route}}")],
            12,
            23,
            "s",
        ),
    ]
    return dashboard(
        "fraud-ml",
        "Fraud Platform — Model & Drift",
        panels,
        "Prediction distribution, fraud rate, serving version and feature drift (PSI)",
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, board in (("platform.json", platform()), ("ml.json", ml())):
        (OUT / name).write_text(json.dumps(board, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {OUT / name}")


if __name__ == "__main__":
    main()

# Benchmark

`python scripts/benchmark.py --rates 50 200 500 1000 --seconds 60` publishes
transactions to Kafka at a fixed rate (each stamped with its send time) and
measures, from the serving store, how fast they come out scored.

**Environment:** the whole platform on one laptop — Docker Desktop (WSL2),
16 vCPU / 8 GB for all 13 containers; Spark in `local[*]` mode in a single
container, 6 Kafka partitions, 2-second micro-batch trigger.

| Target rate | Sent | Scored | Sustained throughput | p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 50 tx/s | 2,999 | 2,999 | 51.1 tx/s | 2.80 s | 4.17 s | 5.65 s |
| 200 tx/s | 11,998 | 11,998 | 189.9 tx/s | 3.54 s | 6.42 s | 7.62 s |
| 500 tx/s | 29,999 | 29,999 | 452.9 tx/s | 10.64 s | 15.73 s | 17.21 s |
| 1000 tx/s | 59,993 | 59,993 | 682.7 tx/s | 31.18 s | 44.16 s | 45.92 s |

Latency is **Kafka ingest -> prediction written**, i.e. it includes waiting
for the next micro-batch, the stateful feature step, model inference with
SHAP explanations, and the Kafka / PostgreSQL / Parquet writes. Nothing was
lost or duplicated at any rate (sent = scored).

## Reading the numbers

* Up to ~200 tx/s the stream keeps up with a p95 of 4-6 s. The floor is the
  micro-batch design: up to 2 s waiting for the trigger plus ~2-3 s of
  per-batch work.
* At 500 tx/s the single container falls slightly behind (453 tx/s
  sustained), so a backlog and latency build up during the run; at 1000 tx/s
  it saturates at **~680 tx/s**.
* Model inference is not the bottleneck: TreeSHAP + XGBoost cost ~4 ms per
  transaction per partition (`fraud_stream_scoring_seconds_per_row`); the
  per-batch fixed costs (stateful pandas UDF round-trips, three sinks) are.

## Scaling levers

| Lever | Effect |
| --- | --- |
| More Kafka partitions + executors (EMR / Spark on Kubernetes) | stateful work parallelises by `user_id`; throughput scales with cores |
| Move the Parquet lake write to its own query | removes a sink from the latency-critical batch |
| Explain only transactions above a score threshold | skips TreeSHAP for the ~99 % of clearly legitimate traffic |
| Shorter trigger / continuous processing for the scoring query | lowers the latency floor at the cost of more, smaller batches |
| `transformWithStateInPandas` (Spark 4) | finer-grained state API, timers, less serialization |

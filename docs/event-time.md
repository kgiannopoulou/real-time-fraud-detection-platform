# Event time, lateness and watermarks

Card events do not arrive in the order they happened. A terminal on a train
loses connectivity, a payment gateway batches its uploads, a partition lags
behind the others:

```text
Event time   Arrival
12:01        12:01
12:02        12:02
11:59        12:03   <- late, out of order
12:03        12:03
```

The platform therefore processes **event time** (the `timestamp` field —
when the card was used), never processing time (when Spark saw the record).

## What the job does

**Event-time windows and watermarking handle late-arriving transactions while
limiting streaming state growth.**

| Mechanism | Where | Effect |
| --- | --- | --- |
| Behavioural windows evaluated at the event's own timestamp | `UserState.features` | A late event gets the features it would have had on time; only *earlier* history is counted |
| Out-of-order insert into the per-user history | `UserState.add` | History stays sorted by event time |
| `withWatermark("event_time", "2 minutes")` | stateful operator and window aggregation | Spark tracks `max(event_time) - 2 min`; events older than that are not admitted into state |
| Event-time state timeout (`GroupStateTimeout.EventTimeTimeout`) | `streaming/stateful.py` | A user's state is evicted once the watermark passes last activity + 24 h, so state does not grow forever |
| 1-minute tumbling windows in append mode | `window_stats` | A window is emitted exactly once, when the watermark passes its end — late events inside the allowed lateness still land in the right minute |
| Lateness side output | `split_by_lateness` -> dead-letter queue | Events that arrive more than 120 s after their event time are routed to `dead_letter_transactions` (reason `arrived_after_allowed_lateness`) for reconciliation instead of being silently dropped |

### The trade-off

The watermark delay is a dial between **completeness** and **latency/state**:

* longer delay -> more late events are handled exactly, but window results are
  emitted later and more state is held in memory/RocksDB;
* shorter delay -> fresher results and less state, but more events miss the
  stateful path.

Two minutes covers the lateness this stream exhibits (the producer's
`--late-rate` delays events by up to 60 s) with margin. Events beyond it are
rare enough to reconcile in batch from the side output.

Fraud scoring must never silently skip a transaction, which is why too-late
events are split off *before* the stateful operator and stored, rather than
relying on Spark's implicit dropping. Spark's own count of rows dropped by
the watermark (`numRowsDroppedByWatermark` in the query progress) is exported
as a metric in the monitoring phase.

## Try it

```bash
python producer/producer.py --rate 30 --late-rate 0.05 --very-late-rate 0.005 --quiet
docker compose logs -f spark | grep -A4 window_start
```

`late_arrivals` in each window counts events that arrived after their
timestamp but were still accepted; the dead-letter queue collects the ones
that exceeded the allowed lateness.

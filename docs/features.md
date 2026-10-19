# Behavioural features

A transaction on its own says little: EUR 2,400 is normal for one card
holder and absurd for another. The model therefore scores each transaction
against **the card holder's own recent behaviour**.

| Feature | Definition | Catches |
| --- | --- | --- |
| `tx_count_2m`, `tx_count_5m`, `tx_count_1h`, `tx_count_24h` | Transactions by the same user in the window **before** this one | Velocity: card testing, bursts after a takeover |
| `avg_amount_24h` | Mean EUR amount of the user's transactions in the previous 24 h | Baseline spend |
| `amount_ratio` | `amount_eur / avg_amount_24h` | "€20, €35, €15, €42 … then €2,400" |
| `unique_merchants_1h` | Distinct merchants in the last hour (incl. this one) | Spraying many merchants |
| `unique_countries_24h` | Distinct countries in the last 24 h (incl. this one) | Impossible travel |
| `is_new_country` | Country not seen for this user in the last 30 days | Foreign use after takeover |
| `is_new_device` | Device not seen for this user in the last 30 days | Account takeover |
| `seconds_since_last_tx` | Gap to the previous transaction | Scripted attacks |
| `amount_eur`, `hour_of_day` | Transaction attributes | Size, night-time activity |
| `merchant_risk`, `country_risk` | Smoothed fraud rate of the category / country (train split only) | Risky segments |

## One implementation for training and serving

The features are implemented once, in `streaming/features.py`
(`UserState.observe`):

```text
                    ┌──────────────────────────────┐
labelled history ──►│ ml/feature_pipeline.py       │──► training set
                    │   replays events per user    │
                    │        │                     │
                    │        ▼                     │
                    │ streaming/features.UserState │
                    │        ▲                     │
                    │        │                     │
Kafka ─► Spark ────►│ streaming/stateful.py        │──► features ─► model
                    │ applyInPandasWithState       │
                    └──────────────────────────────┘
```

In Spark, `groupBy("user_id").applyInPandasWithState(...)` hands each user's
new events plus their serialized `UserState` to the function, which computes
features and writes the updated state back to the (RocksDB) state store.
Because the offline training set is built by replaying the history through
the very same class, there is no train/serve skew to debug.

State per user is bounded: events older than 24 h (+30 min slack for late
data) are pruned, at most 1,000 events and 50 known countries/devices are
kept.

## Duplicates

Kafka delivery is at-least-once, so a transaction id can arrive twice (e.g. a
producer retry without idempotence, or a replay). The state remembers the ids
in its window; a repeated id is emitted with `is_duplicate = true` and no
features, and is excluded from scoring.

## Impact on the model

| Model (XGBoost, test split) | PR-AUC | Precision | Recall | F1 |
| --- | --- | --- | --- | --- |
| Baseline features (Phase 2) | 0.503 | 0.521 | 0.518 | 0.520 |
| + behavioural features | **0.795** | **0.893** | **0.693** | **0.780** |

All four model families improve; Logistic Regression jumps from 0.13 to 0.57
PR-AUC because velocity and amount-ratio features are close to linearly
separable for card testing and takeovers.

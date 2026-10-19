# Baseline model comparison (Phase 2)

All models are trained on a **chronological** split (70 % train, 15 %
validation, 15 % test). Random splits leak future behaviour of the same card
into training and overstate performance. The decision threshold for each
model is chosen on the validation split (max F1) and all numbers below are on
the untouched test split.

## Platform dataset (synthetic, baseline features)

Features: amount (EUR), hour of day, transactions by the user in the previous
hour, user's average previous amount, amount ratio, merchant-category risk,
country risk.

| Model | ROC-AUC | PR-AUC | Precision | Recall | F1 |
| --- | --- | --- | --- | --- | --- |
| Logistic Regression | 0.937 | 0.131 | 0.229 | 0.243 | 0.236 |
| Random Forest | 0.969 | 0.482 | 0.571 | 0.495 | 0.531 |
| **XGBoost** | **0.976** | **0.503** | 0.521 | 0.518 | 0.520 |
| LightGBM | 0.973 | 0.449 | 0.507 | 0.477 | 0.492 |

ROC-AUC looks excellent for every model while PR-AUC ranges from 0.13 to
0.50 — the clearest illustration of why ROC-AUC is not a useful headline
metric at 0.7 % prevalence. The linear model cannot express interactions such
as "small amount *and* digital goods *and* many transactions in the last hour".

XGBoost becomes the production candidate: best PR-AUC, fast to train (1.5 s
vs 12 s for the forest), small portable artefact (JSON booster), and native
TreeSHAP contributions for explainability later.

### Operating points

The threshold is a business decision, not a modelling one. Cost model: every
alert costs an analyst review (EUR 5); every missed fraud costs its amount.

| Threshold | Precision | Recall | Alerts | Missed fraud | Cost (EUR) |
| --- | --- | --- | --- | --- | --- |
| 0.1 | 0.421 | 0.587 | 304 | 90 | 56,819 |
| 0.3 | 0.521 | 0.514 | 215 | 106 | 62,621 |
| 0.5 | 0.610 | 0.459 | 164 | 118 | 77,022 |
| 0.9 | 1.000 | 0.055 | 12 | 206 | 96,357 |

Because a missed fraud is far more expensive than a review, the cheapest
operating point sits at a *low* threshold — higher recall, more false
positives.

## With behavioural features (Phase 6, current model)

| Model | ROC-AUC | PR-AUC | Precision | Recall | F1 |
| --- | --- | --- | --- | --- | --- |
| Logistic Regression | 0.967 | 0.568 | 0.574 | 0.518 | 0.545 |
| Random Forest | 0.982 | 0.772 | 0.878 | 0.693 | 0.774 |
| **XGBoost** | **0.990** | **0.795** | **0.893** | 0.693 | **0.780** |
| LightGBM | 0.988 | 0.784 | 0.842 | 0.706 | 0.768 |

See [features.md](features.md) for the features themselves.

## Public benchmark (ULB credit-card fraud, OpenML #1597)

`python -m ml.public_benchmark` — 284,807 real European card transactions,
0.17 % fraud, PCA-anonymised features; 52 frauds in the test split.

| Model | ROC-AUC | PR-AUC | Precision | Recall | F1 |
| --- | --- | --- | --- | --- | --- |
| Logistic Regression | 0.964 | 0.643 | 0.660 | 0.635 | 0.647 |
| Random Forest | 0.925 | 0.750 | 0.881 | 0.712 | 0.787 |
| **XGBoost** | **0.978** | **0.754** | 0.844 | 0.731 | 0.784 |
| LightGBM | 0.980 | 0.675 | 0.711 | 0.615 | 0.660 |

The same ranking holds on real data: gradient-boosted trees and random
forests clearly beat the linear baseline on PR-AUC, with XGBoost best overall.
With only 52 test frauds the confidence intervals are wide, so differences of
a few points between the tree models are not significant.

"""Model evaluation for a heavily imbalanced problem.

Accuracy is deliberately absent: at ~0.6 % fraud, "approve everything" is
99.4 % accurate. The metrics that matter are

* **PR-AUC** (average precision) — threshold-free, focuses on the positive class;
* **ROC-AUC** — reported for completeness, but optimistic under imbalance;
* **precision / recall / F1 at the operating threshold** — what the business
  actually experiences: analyst workload (false positives) versus missed
  fraud (false negatives).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)


@dataclass
class Metrics:
    roc_auc: float
    pr_auc: float
    precision: float
    recall: float
    f1: float
    threshold: float
    tp: int
    fp: int
    tn: int
    fn: int

    def as_dict(self) -> dict:
        return asdict(self)

    def confusion_matrix_text(self) -> str:
        return (
            "                 predicted legit  predicted fraud\n"
            f"actual legit     {self.tn:>15,}  {self.fp:>15,}\n"
            f"actual fraud     {self.fn:>15,}  {self.tp:>15,}"
        )


def best_f1_threshold(y_true: np.ndarray, scores: np.ndarray) -> float:
    """Threshold that maximises F1 — chosen on the validation split."""
    precision, recall, thresholds = precision_recall_curve(y_true, scores)
    f1 = 2 * precision * recall / np.clip(precision + recall, 1e-12, None)
    return float(thresholds[int(np.argmax(f1[:-1]))])


def evaluate(y_true: np.ndarray, scores: np.ndarray, threshold: float) -> Metrics:
    y_true = np.asarray(y_true).astype(int)
    y_pred = (np.asarray(scores) >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return Metrics(
        roc_auc=float(roc_auc_score(y_true, scores)),
        pr_auc=float(average_precision_score(y_true, scores)),
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        threshold=float(threshold),
        tp=int(tp),
        fp=int(fp),
        tn=int(tn),
        fn=int(fn),
    )


def expected_cost(
    y_true: np.ndarray,
    scores: np.ndarray,
    amounts: np.ndarray,
    threshold: float,
    review_cost: float = 5.0,
) -> float:
    """Business cost at a threshold: every alert costs an analyst review,
    every missed fraud costs its amount (chargeback)."""
    y_true = np.asarray(y_true).astype(bool)
    flagged = np.asarray(scores) >= threshold
    missed = (~flagged) & y_true
    return float(flagged.sum() * review_cost + np.asarray(amounts)[missed].sum())


def threshold_table(y_true, scores, amounts, thresholds=(0.1, 0.2, 0.3, 0.5, 0.7, 0.9)) -> list[dict]:
    """Precision/recall/cost trade-off at several operating points."""
    rows = []
    for t in thresholds:
        m = evaluate(y_true, scores, t)
        rows.append(
            {
                "threshold": t,
                "precision": m.precision,
                "recall": m.recall,
                "alerts": m.tp + m.fp,
                "missed_fraud": m.fn,
                "cost_eur": expected_cost(y_true, scores, amounts, t),
            }
        )
    return rows

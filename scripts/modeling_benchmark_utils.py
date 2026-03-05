#!/usr/bin/env python3
"""Shared utilities for benchmark modeling and calibration."""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np


def build_numeric_interactions(
    x_numeric: np.ndarray,
    numeric_feature_names: List[str],
) -> Tuple[np.ndarray, List[str]]:
    if x_numeric.size == 0:
        return np.zeros((len(x_numeric), 0), dtype=np.float64), []
    cols: List[np.ndarray] = []
    names: List[str] = []

    # Squared terms to capture simple non-linearity.
    for idx, name in enumerate(numeric_feature_names):
        cols.append(np.square(x_numeric[:, idx]))
        names.append(f"sq_{name}")

    # Pairwise interactions for cross-effects.
    for i in range(len(numeric_feature_names)):
        for j in range(i + 1, len(numeric_feature_names)):
            cols.append(x_numeric[:, i] * x_numeric[:, j])
            names.append(f"int_{numeric_feature_names[i]}__x__{numeric_feature_names[j]}")

    return np.column_stack(cols).astype(np.float64), names


def logit_from_probs(probs: np.ndarray) -> np.ndarray:
    p = np.clip(probs.astype(np.float64), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def stable_sigmoid(x: np.ndarray) -> np.ndarray:
    out = np.empty_like(x, dtype=np.float64)
    pos = x >= 0
    neg = ~pos
    out[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    exp_x = np.exp(x[neg])
    out[neg] = exp_x / (1.0 + exp_x)
    return out


def fit_platt_scaler(
    raw_probs: np.ndarray,
    y_true: np.ndarray,
    epochs: int = 400,
    learning_rate: float = 0.05,
    l2: float = 1e-3,
) -> Tuple[float, float]:
    z = logit_from_probs(raw_probs)
    y = y_true.astype(np.float64)
    a = 1.0
    b = 0.0
    for _ in range(int(epochs)):
        logits = a * z + b
        probs = stable_sigmoid(logits)
        err = probs - y
        grad_a = float(np.mean(err * z) + l2 * a)
        grad_b = float(np.mean(err))
        a -= learning_rate * grad_a
        b -= learning_rate * grad_b
    return float(a), float(b)


def apply_platt_scaler(raw_probs: np.ndarray, a: float, b: float) -> np.ndarray:
    z = logit_from_probs(raw_probs)
    return stable_sigmoid(a * z + b)


def roc_auc_score_np(y_true: np.ndarray, scores: np.ndarray) -> float:
    y = y_true.astype(np.int64)
    s = scores.astype(np.float64)
    pos = int(np.sum(y == 1))
    neg = int(np.sum(y == 0))
    if pos == 0 or neg == 0:
        return 0.0

    order = np.argsort(s)
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, len(s) + 1, dtype=np.float64)

    # Average ties.
    sorted_scores = s[order]
    start = 0
    while start < len(s):
        end = start + 1
        while end < len(s) and sorted_scores[end] == sorted_scores[start]:
            end += 1
        if end - start > 1:
            avg_rank = (start + 1 + end) / 2.0
            ranks[order[start:end]] = avg_rank
        start = end

    rank_sum_pos = float(np.sum(ranks[y == 1]))
    auc = (rank_sum_pos - (pos * (pos + 1) / 2.0)) / (pos * neg)
    return float(auc)


def brier_score_np(y_true: np.ndarray, probs: np.ndarray) -> float:
    y = y_true.astype(np.float64)
    p = probs.astype(np.float64)
    return float(np.mean(np.square(p - y)))


def calibration_bins(
    y_true: np.ndarray,
    probs: np.ndarray,
    n_bins: int = 10,
) -> List[Dict[str, float]]:
    y = y_true.astype(np.float64)
    p = np.clip(probs.astype(np.float64), 0.0, 1.0)
    bins = np.linspace(0.0, 1.0, int(n_bins) + 1)
    rows: List[Dict[str, float]] = []
    for i in range(len(bins) - 1):
        low = bins[i]
        high = bins[i + 1]
        if i == len(bins) - 2:
            mask = (p >= low) & (p <= high)
        else:
            mask = (p >= low) & (p < high)
        cnt = int(np.sum(mask))
        if cnt == 0:
            pred_mean = 0.0
            event_rate = 0.0
        else:
            pred_mean = float(np.mean(p[mask]))
            event_rate = float(np.mean(y[mask]))
        rows.append(
            {
                "bin_index": int(i),
                "bin_low": float(low),
                "bin_high": float(high),
                "count": cnt,
                "pred_mean": pred_mean,
                "event_rate": event_rate,
            }
        )
    return rows

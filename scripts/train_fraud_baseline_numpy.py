#!/usr/bin/env python3
"""
Train a lightweight fraud baseline model from SQLite warehouse using numpy.

No scikit-learn dependency required.
Model: logistic regression (mini-batch gradient descent, weighted BCE + L2)
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class SplitData:
    x_train: np.ndarray
    y_train: np.ndarray
    x_valid: np.ndarray
    y_valid: np.ndarray
    feature_names: List[str]
    numeric_feature_names: List[str]
    numeric_means: np.ndarray
    numeric_stds: np.ndarray


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train fraud baseline (numpy logistic regression).")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets to use (default: ieee_cis creditcard_fraud paysim)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional hard cap on total rows loaded from SQL",
    )
    parser.add_argument(
        "--max-rows-per-dataset",
        type=int,
        default=None,
        help="Optional per-dataset cap before merge",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Optional random downsample fraction after load (0 < f <= 1)",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--epochs", type=int, default=8, help="Training epochs")
    parser.add_argument("--learning-rate", type=float, default=0.05, help="Learning rate")
    parser.add_argument("--l2", type=float, default=1e-4, help="L2 regularization")
    parser.add_argument("--batch-size", type=int, default=65536, help="Mini-batch size")
    parser.add_argument(
        "--train-time-fraction",
        type=float,
        default=0.8,
        help="Time-based train split fraction",
    )
    parser.add_argument("--fp-cost", type=float, default=1.0, help="False positive cost")
    parser.add_argument("--fn-cost", type=float, default=25.0, help="False negative cost")
    parser.add_argument(
        "--output-root",
        default="artifacts/models/fraud_baseline",
        help="Output root for model artifacts",
    )
    return parser.parse_args()


def stable_sigmoid(x: np.ndarray) -> np.ndarray:
    out = np.empty_like(x, dtype=np.float64)
    pos = x >= 0
    neg = ~pos
    out[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    exp_x = np.exp(x[neg])
    out[neg] = exp_x / (1.0 + exp_x)
    return out


def average_precision_score_np(y_true: np.ndarray, scores: np.ndarray) -> float:
    order = np.argsort(-scores)
    y = y_true[order].astype(np.int64)
    positives = y.sum()
    if positives == 0:
        return 0.0
    tp = np.cumsum(y == 1)
    fp = np.cumsum(y == 0)
    precision = tp / np.maximum(tp + fp, 1)
    recall = tp / positives
    recall_prev = np.r_[0.0, recall[:-1]]
    ap = np.sum((recall - recall_prev) * precision)
    return float(ap)


def pr_curve_np(y_true: np.ndarray, scores: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    order = np.argsort(-scores)
    y = y_true[order].astype(np.int64)
    s = scores[order]
    positives = y.sum()
    if positives == 0:
        return np.array([1.0]), np.array([0.0]), np.array([np.inf])

    tp = np.cumsum(y == 1)
    fp = np.cumsum(y == 0)
    precision = tp / np.maximum(tp + fp, 1)
    recall = tp / positives
    thresholds = s
    return precision, recall, thresholds


def pr_auc_trapz(y_true: np.ndarray, scores: np.ndarray) -> float:
    precision, recall, _ = pr_curve_np(y_true, scores)
    if len(precision) < 2:
        return 0.0
    return float(np.trapezoid(precision, recall))


def threshold_by_cost(
    y_true: np.ndarray,
    scores: np.ndarray,
    fp_cost: float,
    fn_cost: float,
) -> Dict[str, float]:
    q = np.linspace(0.001, 0.999, 250)
    thresholds = np.unique(np.quantile(scores, q))

    best = {
        "threshold": 0.5,
        "cost": float("inf"),
        "tp": 0.0,
        "fp": 0.0,
        "tn": 0.0,
        "fn": 0.0,
        "precision": 0.0,
        "recall": 0.0,
    }

    y = y_true.astype(np.int64)
    for t in thresholds:
        pred = (scores >= t).astype(np.int64)
        tp = float(np.sum((pred == 1) & (y == 1)))
        fp = float(np.sum((pred == 1) & (y == 0)))
        tn = float(np.sum((pred == 0) & (y == 0)))
        fn = float(np.sum((pred == 0) & (y == 1)))
        cost = fp * fp_cost + fn * fn_cost
        if cost < best["cost"]:
            precision = tp / max(tp + fp, 1.0)
            recall = tp / max(tp + fn, 1.0)
            best = {
                "threshold": float(t),
                "cost": float(cost),
                "tp": tp,
                "fp": fp,
                "tn": tn,
                "fn": fn,
                "precision": float(precision),
                "recall": float(recall),
            }
    return best


def load_training_frame(
    db_path: Path,
    datasets: List[str],
    max_rows: int | None,
    max_rows_per_dataset: int | None,
    sample_fraction: float,
    seed: int,
) -> pd.DataFrame:
    base_query = """
    SELECT
      tm.event_id,
      tm.dataset_id,
      tm.event_time,
      tm.channel,
      tm.txn_type,
      tm.currency,
      tm.amount,
      COALESCE(f.payer_txn_count_24h, 0.0) AS payer_txn_count_24h,
      COALESCE(f.payer_amt_sum_24h, 0.0) AS payer_amt_sum_24h,
      tm.label_fraud
    FROM transaction_mart tm
    LEFT JOIN feature_payer_24h f
      ON f.event_id = tm.event_id
    WHERE tm.label_fraud IS NOT NULL
      AND tm.dataset_id = ?
    """

    conn = sqlite3.connect(db_path)
    frames: List[pd.DataFrame] = []

    if max_rows_per_dataset is not None:
        limit_sql = f" LIMIT {int(max_rows_per_dataset)}"
        for ds in datasets:
            frames.append(pd.read_sql_query(base_query + limit_sql, conn, params=[ds]))
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        placeholders = ",".join(["?"] * len(datasets))
        max_clause = f" LIMIT {int(max_rows)}" if max_rows else ""
        query = base_query.replace("tm.dataset_id = ?", f"tm.dataset_id IN ({placeholders})") + max_clause
        df = pd.read_sql_query(query, conn, params=datasets)

    conn.close()

    if max_rows is not None and max_rows_per_dataset is not None and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=seed).reset_index(drop=True)

    if sample_fraction < 1.0:
        df = df.sample(frac=sample_fraction, random_state=seed).sort_values("event_time").reset_index(drop=True)
    return df


def build_feature_matrix(df: pd.DataFrame, train_time_fraction: float) -> SplitData:
    frame = df.copy()
    frame["event_time"] = pd.to_datetime(frame["event_time"], errors="coerce")
    frame = frame.dropna(subset=["event_time", "label_fraud"]).reset_index(drop=True)
    frame = frame.sort_values("event_time").reset_index(drop=True)

    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce").fillna(0.0)
    frame["payer_txn_count_24h"] = pd.to_numeric(frame["payer_txn_count_24h"], errors="coerce").fillna(0.0)
    frame["payer_amt_sum_24h"] = pd.to_numeric(frame["payer_amt_sum_24h"], errors="coerce").fillna(0.0)

    frame["log_amount"] = np.log1p(frame["amount"].clip(lower=0))
    frame["log_payer_amt_sum_24h"] = np.log1p(frame["payer_amt_sum_24h"].clip(lower=0))
    frame["hour_of_day"] = frame["event_time"].dt.hour.astype(np.float64)
    frame["day_of_week"] = frame["event_time"].dt.dayofweek.astype(np.float64)

    numeric_cols = [
        "log_amount",
        "payer_txn_count_24h",
        "log_payer_amt_sum_24h",
        "hour_of_day",
        "day_of_week",
    ]
    categorical_cols = ["dataset_id", "channel", "txn_type", "currency"]

    cat_frame = pd.get_dummies(
        frame[categorical_cols].fillna("UNK").astype(str),
        columns=categorical_cols,
        prefix=categorical_cols,
        dtype=np.float64,
    )
    x_df = pd.concat([frame[numeric_cols].astype(np.float64), cat_frame], axis=1)
    y = frame["label_fraud"].astype(np.int64).to_numpy()

    split_idx = int(len(frame) * train_time_fraction)
    split_idx = max(1, min(split_idx, len(frame) - 1))

    x_train_df = x_df.iloc[:split_idx].copy()
    x_valid_df = x_df.iloc[split_idx:].copy()
    y_train = y[:split_idx]
    y_valid = y[split_idx:]

    # Standardize only numeric columns using train stats.
    means = x_train_df[numeric_cols].mean().to_numpy(dtype=np.float64)
    stds = x_train_df[numeric_cols].std(ddof=0).replace(0, 1.0).fillna(1.0).to_numpy(dtype=np.float64)
    means = np.nan_to_num(means, nan=0.0, posinf=0.0, neginf=0.0)
    stds = np.nan_to_num(stds, nan=1.0, posinf=1.0, neginf=1.0)

    x_train_num = x_train_df[numeric_cols].to_numpy(dtype=np.float64)
    x_valid_num = x_valid_df[numeric_cols].to_numpy(dtype=np.float64)
    x_train_num = (x_train_num - means) / stds
    x_valid_num = (x_valid_num - means) / stds

    other_cols = [c for c in x_df.columns if c not in numeric_cols]
    x_train_other = x_train_df[other_cols].to_numpy(dtype=np.float64)
    x_valid_other = x_valid_df[other_cols].to_numpy(dtype=np.float64)

    x_train = np.hstack([x_train_num, x_train_other])
    x_valid = np.hstack([x_valid_num, x_valid_other])
    x_train = np.nan_to_num(x_train, nan=0.0, posinf=0.0, neginf=0.0)
    x_valid = np.nan_to_num(x_valid, nan=0.0, posinf=0.0, neginf=0.0)

    feature_names = numeric_cols + other_cols
    return SplitData(
        x_train=x_train,
        y_train=y_train,
        x_valid=x_valid,
        y_valid=y_valid,
        feature_names=feature_names,
        numeric_feature_names=numeric_cols,
        numeric_means=means,
        numeric_stds=stds,
    )


def train_logistic_regression(
    x_train: np.ndarray,
    y_train: np.ndarray,
    epochs: int,
    learning_rate: float,
    l2: float,
    batch_size: int,
    seed: int,
) -> Tuple[np.ndarray, float, List[Dict[str, float]]]:
    rng = np.random.default_rng(seed)
    n_samples, n_features = x_train.shape
    w = np.zeros(n_features, dtype=np.float64)
    b = 0.0

    positives = max(int(np.sum(y_train == 1)), 1)
    negatives = max(int(np.sum(y_train == 0)), 1)
    pos_weight = negatives / positives

    history: List[Dict[str, float]] = []

    for epoch in range(1, epochs + 1):
        perm = rng.permutation(n_samples)
        x_shuf = x_train[perm]
        y_shuf = y_train[perm].astype(np.float64)

        epoch_loss = 0.0
        batches = 0

        for start in range(0, n_samples, batch_size):
            end = min(start + batch_size, n_samples)
            xb = x_shuf[start:end]
            yb = y_shuf[start:end]

            with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
                logits = xb @ w + b
            logits = np.nan_to_num(logits, nan=0.0, posinf=50.0, neginf=-50.0)
            probs = stable_sigmoid(logits)

            sample_w = np.where(yb == 1.0, pos_weight, 1.0)
            probs_clipped = np.clip(probs, 1e-9, 1 - 1e-9)
            bce = -(yb * np.log(probs_clipped) + (1 - yb) * np.log(1 - probs_clipped))
            loss = float(np.mean(sample_w * bce) + 0.5 * l2 * np.sum(w * w))
            epoch_loss += loss
            batches += 1

            error = (probs - yb) * sample_w
            with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
                grad_w = (xb.T @ error) / len(yb) + l2 * w
            grad_w = np.nan_to_num(grad_w, nan=0.0, posinf=0.0, neginf=0.0)
            grad_b = float(np.mean(error))

            w -= learning_rate * grad_w
            b -= learning_rate * grad_b

        history.append({"epoch": float(epoch), "loss": epoch_loss / max(batches, 1)})
        print(f"[TRAIN] epoch={epoch} loss={history[-1]['loss']:.6f}", flush=True)

    return w, float(b), history


def save_pr_curve_csv(path: Path, precision: np.ndarray, recall: np.ndarray, thresholds: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["precision", "recall", "threshold"])
        for p, r, t in zip(precision, recall, thresholds):
            writer.writerow([f"{float(p):.10f}", f"{float(r):.10f}", f"{float(t):.10f}"])


def main() -> None:
    args = parse_args()
    if not (0 < args.sample_fraction <= 1.0):
        raise ValueError("--sample-fraction must be in (0, 1].")

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse DB not found: {db_path}")

    print("[INFO] loading training frame from sqlite", flush=True)
    df = load_training_frame(
        db_path=db_path,
        datasets=args.datasets,
        max_rows=args.max_rows,
        max_rows_per_dataset=args.max_rows_per_dataset,
        sample_fraction=args.sample_fraction,
        seed=args.seed,
    )
    if df.empty:
        raise RuntimeError("No training rows loaded. Check datasets/warehouse build.")
    print(f"[INFO] loaded_rows={len(df)}", flush=True)

    split = build_feature_matrix(df, train_time_fraction=args.train_time_fraction)
    print(
        f"[INFO] train_rows={len(split.y_train)} valid_rows={len(split.y_valid)} "
        f"features={len(split.feature_names)}",
        flush=True,
    )

    w, b, history = train_logistic_regression(
        x_train=split.x_train,
        y_train=split.y_train,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        l2=args.l2,
        batch_size=args.batch_size,
        seed=args.seed,
    )

    with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
        valid_logits = split.x_valid @ w + b
    valid_logits = np.nan_to_num(valid_logits, nan=0.0, posinf=50.0, neginf=-50.0)
    valid_scores = stable_sigmoid(valid_logits)
    ap = average_precision_score_np(split.y_valid, valid_scores)
    pr_auc = pr_auc_trapz(split.y_valid, valid_scores)
    cost_best = threshold_by_cost(
        y_true=split.y_valid,
        scores=valid_scores,
        fp_cost=args.fp_cost,
        fn_cost=args.fn_cost,
    )
    precision, recall, thresholds = pr_curve_np(split.y_valid, valid_scores)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_root) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    np.savez_compressed(
        out_dir / "model.npz",
        weights=w,
        bias=np.array([b], dtype=np.float64),
        feature_names=np.array(split.feature_names, dtype=object),
        numeric_feature_names=np.array(split.numeric_feature_names, dtype=object),
        numeric_means=split.numeric_means,
        numeric_stds=split.numeric_stds,
    )

    save_pr_curve_csv(out_dir / "pr_curve.csv", precision, recall, thresholds)

    metrics = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "datasets": args.datasets,
        "loaded_rows": int(len(df)),
        "train_rows": int(len(split.y_train)),
        "valid_rows": int(len(split.y_valid)),
        "feature_count": int(len(split.feature_names)),
        "class_balance": {
            "train_positive": int(np.sum(split.y_train == 1)),
            "train_negative": int(np.sum(split.y_train == 0)),
            "valid_positive": int(np.sum(split.y_valid == 1)),
            "valid_negative": int(np.sum(split.y_valid == 0)),
        },
        "metrics": {
            "average_precision": float(ap),
            "pr_auc_trapz": float(pr_auc),
        },
        "cost_optimized_threshold": cost_best,
        "training": {
            "epochs": args.epochs,
            "learning_rate": args.learning_rate,
            "l2": args.l2,
            "batch_size": args.batch_size,
            "seed": args.seed,
            "history": history,
        },
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    top_idx = np.argsort(np.abs(w))[::-1][:20]
    top_features = [{"feature": split.feature_names[i], "weight": float(w[i])} for i in top_idx]
    (out_dir / "top_features.json").write_text(json.dumps(top_features, indent=2), encoding="utf-8")

    latest = Path(args.output_root) / "latest"
    symlink_ok = False
    try:
        if latest.is_symlink() or latest.exists():
            if latest.is_dir() and not latest.is_symlink():
                shutil.rmtree(latest)
            else:
                latest.unlink()
        latest.symlink_to(out_dir.name, target_is_directory=True)
        symlink_ok = True
    except Exception:
        symlink_ok = False

    # Fallback for filesystems where symlink is unavailable.
    if not symlink_ok:
        if latest.exists():
            if latest.is_dir():
                shutil.rmtree(latest)
            else:
                latest.unlink()
        shutil.copytree(out_dir, latest)

    print("[DONE] model artifacts:", out_dir, flush=True)
    print(json.dumps(metrics["metrics"], indent=2), flush=True)
    print("best_threshold:", round(cost_best["threshold"], 6), "cost:", round(cost_best["cost"], 2), flush=True)


if __name__ == "__main__":
    main()

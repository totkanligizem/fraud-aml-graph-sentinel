#!/usr/bin/env python3
"""
Train an enhanced benchmark fraud model (numpy logistic + interaction features + Platt calibration).
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np

from modeling_benchmark_utils import (
    apply_platt_scaler,
    brier_score_np,
    build_numeric_interactions,
    calibration_bins,
    fit_platt_scaler,
    roc_auc_score_np,
)
from train_fraud_baseline_numpy import (
    average_precision_score_np,
    build_feature_matrix,
    load_training_frame,
    pr_auc_trapz,
    stable_sigmoid,
    threshold_by_cost,
    train_logistic_regression,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train enhanced benchmark fraud model.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets to use",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Optional hard cap on total rows")
    parser.add_argument("--max-rows-per-dataset", type=int, default=None, help="Optional per-dataset row cap")
    parser.add_argument("--sample-fraction", type=float, default=1.0, help="Optional random downsample fraction")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--epochs", type=int, default=10, help="Training epochs")
    parser.add_argument("--learning-rate", type=float, default=0.05, help="Learning rate")
    parser.add_argument("--l2", type=float, default=1e-4, help="L2 regularization")
    parser.add_argument("--batch-size", type=int, default=65536, help="Mini-batch size")
    parser.add_argument("--train-time-fraction", type=float, default=0.8, help="Train split fraction")
    parser.add_argument(
        "--split-mode",
        choices=["global_time", "per_dataset_time"],
        default="per_dataset_time",
        help="Split strategy",
    )
    parser.add_argument("--fp-cost", type=float, default=1.0, help="False positive cost")
    parser.add_argument("--fn-cost", type=float, default=25.0, help="False negative cost")
    parser.add_argument("--calibration-epochs", type=int, default=400, help="Platt scaling optimization epochs")
    parser.add_argument("--calibration-learning-rate", type=float, default=0.05, help="Platt scaling learning rate")
    parser.add_argument("--calibration-l2", type=float, default=1e-3, help="Platt scaling L2 weight")
    parser.add_argument("--calibration-bins", type=int, default=10, help="Calibration chart bins")
    parser.add_argument(
        "--output-root",
        default="artifacts/models/fraud_benchmark_numpy",
        help="Output root for benchmark artifacts",
    )
    return parser.parse_args()


def counts_by_dataset(dataset_ids: np.ndarray) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for ds, cnt in zip(*np.unique(dataset_ids.astype(str), return_counts=True)):
        counts[str(ds)] = int(cnt)
    return counts


def metrics_block(y_true: np.ndarray, scores: np.ndarray) -> Dict[str, float]:
    return {
        "average_precision": float(average_precision_score_np(y_true, scores)),
        "pr_auc_trapz": float(pr_auc_trapz(y_true, scores)),
        "roc_auc": float(roc_auc_score_np(y_true, scores)),
        "brier_score": float(brier_score_np(y_true, scores)),
    }


def metrics_by_dataset(y_true: np.ndarray, scores: np.ndarray, dataset_ids: np.ndarray) -> Dict[str, Dict[str, float]]:
    output: Dict[str, Dict[str, float]] = {}
    ds_values = dataset_ids.astype(str)
    for ds in sorted(np.unique(ds_values).tolist()):
        mask = ds_values == ds
        y_ds = y_true[mask]
        s_ds = scores[mask]
        positives = int(np.sum(y_ds == 1))
        negatives = int(np.sum(y_ds == 0))
        row = metrics_block(y_ds, s_ds)
        row.update(
            {
                "rows": int(len(y_ds)),
                "positives": positives,
                "negatives": negatives,
                "positive_rate": float(positives / len(y_ds)) if len(y_ds) > 0 else 0.0,
            }
        )
        output[ds] = row
    return output


def save_calibration_csv(path: Path, rows: List[Dict[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["bin_index", "bin_low", "bin_high", "count", "pred_mean", "event_rate"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    if not (0 < args.sample_fraction <= 1.0):
        raise ValueError("--sample-fraction must be in (0, 1].")

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse DB not found: {db_path}")

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

    split = build_feature_matrix(
        df=df,
        train_time_fraction=args.train_time_fraction,
        split_mode=args.split_mode,
    )
    n_numeric = len(split.numeric_feature_names)
    x_train_num = split.x_train[:, :n_numeric]
    x_valid_num = split.x_valid[:, :n_numeric]
    x_train_inter, interaction_feature_names = build_numeric_interactions(
        x_numeric=x_train_num,
        numeric_feature_names=split.numeric_feature_names,
    )
    x_valid_inter, _ = build_numeric_interactions(
        x_numeric=x_valid_num,
        numeric_feature_names=split.numeric_feature_names,
    )
    x_train = np.hstack([split.x_train, x_train_inter]).astype(np.float64)
    x_valid = np.hstack([split.x_valid, x_valid_inter]).astype(np.float64)
    feature_names = split.feature_names + interaction_feature_names

    print(
        f"[INFO] loaded_rows={len(df)} train_rows={len(split.y_train)} valid_rows={len(split.y_valid)} "
        f"features={len(feature_names)} split_mode={split.split_mode}",
        flush=True,
    )
    print(
        "[INFO] split_dataset_rows train="
        + json.dumps(counts_by_dataset(split.train_dataset_ids), ensure_ascii=False)
        + " valid="
        + json.dumps(counts_by_dataset(split.valid_dataset_ids), ensure_ascii=False),
        flush=True,
    )

    w, b, history = train_logistic_regression(
        x_train=x_train,
        y_train=split.y_train,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        l2=args.l2,
        batch_size=args.batch_size,
        seed=args.seed,
    )

    with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
        train_logits = x_train @ w + b
        valid_logits = x_valid @ w + b
    train_logits = np.nan_to_num(train_logits, nan=0.0, posinf=50.0, neginf=-50.0)
    valid_logits = np.nan_to_num(valid_logits, nan=0.0, posinf=50.0, neginf=-50.0)
    train_raw_probs = stable_sigmoid(train_logits)
    valid_raw_probs = stable_sigmoid(valid_logits)

    cal_a, cal_b = fit_platt_scaler(
        raw_probs=train_raw_probs,
        y_true=split.y_train,
        epochs=args.calibration_epochs,
        learning_rate=args.calibration_learning_rate,
        l2=args.calibration_l2,
    )
    valid_cal_probs = apply_platt_scaler(valid_raw_probs, cal_a, cal_b)

    raw_metrics = metrics_block(split.y_valid, valid_raw_probs)
    calibrated_metrics = metrics_block(split.y_valid, valid_cal_probs)
    raw_cost = threshold_by_cost(
        y_true=split.y_valid,
        scores=valid_raw_probs,
        fp_cost=args.fp_cost,
        fn_cost=args.fn_cost,
    )
    calibrated_cost = threshold_by_cost(
        y_true=split.y_valid,
        scores=valid_cal_probs,
        fp_cost=args.fp_cost,
        fn_cost=args.fn_cost,
    )

    metrics_raw_by_ds = metrics_by_dataset(
        y_true=split.y_valid,
        scores=valid_raw_probs,
        dataset_ids=split.valid_dataset_ids,
    )
    metrics_cal_by_ds = metrics_by_dataset(
        y_true=split.y_valid,
        scores=valid_cal_probs,
        dataset_ids=split.valid_dataset_ids,
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_root) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    np.savez_compressed(
        out_dir / "model.npz",
        weights=w,
        bias=np.array([b], dtype=np.float64),
        feature_names=np.array(feature_names, dtype=object),
        base_feature_names=np.array(split.feature_names, dtype=object),
        interaction_feature_names=np.array(interaction_feature_names, dtype=object),
        numeric_feature_names=np.array(split.numeric_feature_names, dtype=object),
        numeric_means=split.numeric_means,
        numeric_stds=split.numeric_stds,
        calibration_a=np.array([cal_a], dtype=np.float64),
        calibration_b=np.array([cal_b], dtype=np.float64),
    )

    cal_raw_rows = calibration_bins(split.y_valid, valid_raw_probs, n_bins=args.calibration_bins)
    cal_adj_rows = calibration_bins(split.y_valid, valid_cal_probs, n_bins=args.calibration_bins)
    save_calibration_csv(out_dir / "calibration_raw.csv", cal_raw_rows)
    save_calibration_csv(out_dir / "calibration_calibrated.csv", cal_adj_rows)

    metrics = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model_name": "fraud_benchmark_numpy_interactions_platt",
        "db_path": str(db_path),
        "datasets": args.datasets,
        "loaded_rows": int(len(df)),
        "train_rows": int(len(split.y_train)),
        "valid_rows": int(len(split.y_valid)),
        "feature_count": int(len(feature_names)),
        "base_feature_count": int(len(split.feature_names)),
        "interaction_feature_count": int(len(interaction_feature_names)),
        "class_balance": {
            "train_positive": int(np.sum(split.y_train == 1)),
            "train_negative": int(np.sum(split.y_train == 0)),
            "valid_positive": int(np.sum(split.y_valid == 1)),
            "valid_negative": int(np.sum(split.y_valid == 0)),
        },
        "split": {
            "mode": split.split_mode,
            "train_time_fraction": float(args.train_time_fraction),
            "train_rows_by_dataset": counts_by_dataset(split.train_dataset_ids),
            "valid_rows_by_dataset": counts_by_dataset(split.valid_dataset_ids),
        },
        "metrics_raw": raw_metrics,
        "metrics_calibrated": calibrated_metrics,
        "metrics_by_dataset_raw": metrics_raw_by_ds,
        "metrics_by_dataset_calibrated": metrics_cal_by_ds,
        "cost_optimized_threshold_raw": raw_cost,
        "cost_optimized_threshold_calibrated": calibrated_cost,
        "calibration": {
            "method": "platt_scaling",
            "fit_on": "train",
            "a": float(cal_a),
            "b": float(cal_b),
            "bins": int(args.calibration_bins),
        },
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
    top_features = [{"feature": feature_names[i], "weight": float(w[i])} for i in top_idx]
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

    if not symlink_ok:
        if latest.exists():
            if latest.is_dir():
                shutil.rmtree(latest)
            else:
                latest.unlink()
        shutil.copytree(out_dir, latest)

    print("[DONE] benchmark model artifacts:", out_dir, flush=True)
    print(json.dumps({"raw": raw_metrics, "calibrated": calibrated_metrics}, indent=2), flush=True)


if __name__ == "__main__":
    main()

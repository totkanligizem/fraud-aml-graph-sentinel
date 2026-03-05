#!/usr/bin/env python3
"""
Train optional tree-based benchmark model (sklearn HistGradientBoostingClassifier).

This script is dependency-gated:
- Requires scikit-learn in runtime environment.
- Keeps project pipeline deterministic and comparable against numpy baselines.
"""

from __future__ import annotations

import argparse
import json
import pickle
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import numpy as np

from modeling_benchmark_utils import (
    apply_platt_scaler,
    brier_score_np,
    calibration_bins,
    fit_platt_scaler,
    roc_auc_score_np,
)
from train_fraud_baseline_numpy import (
    average_precision_score_np,
    build_feature_matrix,
    load_training_frame,
    pr_auc_trapz,
    threshold_by_cost,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train optional tree-based fraud benchmark model.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets to use",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Optional hard cap on total rows")
    parser.add_argument("--max-rows-per-dataset", type=int, default=300000, help="Optional per-dataset row cap")
    parser.add_argument("--sample-fraction", type=float, default=1.0, help="Optional random downsample fraction")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--train-time-fraction", type=float, default=0.8, help="Train split fraction")
    parser.add_argument(
        "--split-mode",
        choices=["global_time", "per_dataset_time"],
        default="per_dataset_time",
        help="Split strategy",
    )
    parser.add_argument("--fp-cost", type=float, default=1.0, help="False positive cost")
    parser.add_argument("--fn-cost", type=float, default=25.0, help="False negative cost")

    parser.add_argument("--max-iter", type=int, default=300, help="Tree boosting max iterations")
    parser.add_argument("--learning-rate", type=float, default=0.05, help="Tree learning rate")
    parser.add_argument("--max-depth", type=int, default=8, help="Tree max depth")
    parser.add_argument("--min-samples-leaf", type=int, default=50, help="Tree min samples leaf")
    parser.add_argument("--l2-regularization", type=float, default=1e-3, help="Tree L2 regularization")

    parser.add_argument("--calibration-epochs", type=int, default=400, help="Platt scaling epochs")
    parser.add_argument("--calibration-learning-rate", type=float, default=0.05, help="Platt scaling learning rate")
    parser.add_argument("--calibration-l2", type=float, default=1e-3, help="Platt scaling L2 weight")
    parser.add_argument("--calibration-bins", type=int, default=10, help="Calibration bins")
    parser.add_argument(
        "--output-root",
        default="artifacts/models/fraud_tree_benchmark",
        help="Output root for artifacts",
    )
    return parser.parse_args()


def counts_by_dataset(dataset_ids: np.ndarray) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for ds, cnt in zip(*np.unique(dataset_ids.astype(str), return_counts=True)):
        out[str(ds)] = int(cnt)
    return out


def metrics_block(y_true: np.ndarray, scores: np.ndarray) -> Dict[str, float]:
    return {
        "average_precision": float(average_precision_score_np(y_true, scores)),
        "pr_auc_trapz": float(pr_auc_trapz(y_true, scores)),
        "roc_auc": float(roc_auc_score_np(y_true, scores)),
        "brier_score": float(brier_score_np(y_true, scores)),
    }


def metrics_by_dataset(y_true: np.ndarray, scores: np.ndarray, dataset_ids: np.ndarray) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    values = dataset_ids.astype(str)
    for ds in sorted(np.unique(values).tolist()):
        mask = values == ds
        y_ds = y_true[mask]
        s_ds = scores[mask]
        pos = int(np.sum(y_ds == 1))
        neg = int(np.sum(y_ds == 0))
        row = metrics_block(y_ds, s_ds)
        row.update(
            {
                "rows": int(len(y_ds)),
                "positives": pos,
                "negatives": neg,
                "positive_rate": float(pos / len(y_ds)) if len(y_ds) > 0 else 0.0,
            }
        )
        out[ds] = row
    return out


def main() -> None:
    args = parse_args()
    if not (0 < args.sample_fraction <= 1.0):
        raise ValueError("--sample-fraction must be in (0, 1].")

    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
    except Exception as exc:
        raise RuntimeError(
            "scikit-learn is required for train_fraud_tree_benchmark.py. "
            "Install with: python3 -m pip install scikit-learn"
        ) from exc

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
        raise RuntimeError("No training rows loaded.")

    split = build_feature_matrix(
        df=df,
        train_time_fraction=args.train_time_fraction,
        split_mode=args.split_mode,
    )

    print(
        f"[INFO] loaded_rows={len(df)} train_rows={len(split.y_train)} valid_rows={len(split.y_valid)} "
        f"features={len(split.feature_names)} split_mode={split.split_mode}",
        flush=True,
    )
    print(
        "[INFO] split_dataset_rows train="
        + json.dumps(counts_by_dataset(split.train_dataset_ids), ensure_ascii=False)
        + " valid="
        + json.dumps(counts_by_dataset(split.valid_dataset_ids), ensure_ascii=False),
        flush=True,
    )

    positives = max(int(np.sum(split.y_train == 1)), 1)
    negatives = max(int(np.sum(split.y_train == 0)), 1)
    pos_weight = negatives / positives
    sample_weight = np.where(split.y_train == 1, pos_weight, 1.0).astype(np.float64)

    model = HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=float(args.learning_rate),
        max_iter=int(args.max_iter),
        max_depth=int(args.max_depth),
        min_samples_leaf=int(args.min_samples_leaf),
        l2_regularization=float(args.l2_regularization),
        random_state=int(args.seed),
    )
    model.fit(split.x_train, split.y_train.astype(np.int64), sample_weight=sample_weight)

    train_raw_probs = model.predict_proba(split.x_train)[:, 1].astype(np.float64)
    valid_raw_probs = model.predict_proba(split.x_valid)[:, 1].astype(np.float64)

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
    raw_cost = threshold_by_cost(split.y_valid, valid_raw_probs, fp_cost=args.fp_cost, fn_cost=args.fn_cost)
    calibrated_cost = threshold_by_cost(
        split.y_valid, valid_cal_probs, fp_cost=args.fp_cost, fn_cost=args.fn_cost
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_root) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with (out_dir / "model.pkl").open("wb") as f:
        pickle.dump(
            {
                "model": model,
                "feature_names": split.feature_names,
                "numeric_feature_names": split.numeric_feature_names,
                "numeric_means": split.numeric_means,
                "numeric_stds": split.numeric_stds,
                "calibration_a": float(cal_a),
                "calibration_b": float(cal_b),
            },
            f,
            protocol=pickle.HIGHEST_PROTOCOL,
        )

    raw_bins = calibration_bins(split.y_valid, valid_raw_probs, n_bins=args.calibration_bins)
    cal_bins = calibration_bins(split.y_valid, valid_cal_probs, n_bins=args.calibration_bins)
    (out_dir / "calibration_raw.json").write_text(json.dumps(raw_bins, indent=2), encoding="utf-8")
    (out_dir / "calibration_calibrated.json").write_text(json.dumps(cal_bins, indent=2), encoding="utf-8")

    metrics = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model_name": "fraud_tree_benchmark_hgb_platt",
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
        "split": {
            "mode": split.split_mode,
            "train_time_fraction": float(args.train_time_fraction),
            "train_rows_by_dataset": counts_by_dataset(split.train_dataset_ids),
            "valid_rows_by_dataset": counts_by_dataset(split.valid_dataset_ids),
        },
        "metrics_raw": raw_metrics,
        "metrics_calibrated": calibrated_metrics,
        "metrics_by_dataset_raw": metrics_by_dataset(split.y_valid, valid_raw_probs, split.valid_dataset_ids),
        "metrics_by_dataset_calibrated": metrics_by_dataset(split.y_valid, valid_cal_probs, split.valid_dataset_ids),
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
            "model": "HistGradientBoostingClassifier",
            "max_iter": int(args.max_iter),
            "learning_rate": float(args.learning_rate),
            "max_depth": int(args.max_depth),
            "min_samples_leaf": int(args.min_samples_leaf),
            "l2_regularization": float(args.l2_regularization),
            "seed": int(args.seed),
            "positive_weight": float(pos_weight),
        },
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

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

    print("[DONE] tree benchmark artifacts:", out_dir, flush=True)
    print(json.dumps({"raw": raw_metrics, "calibrated": calibrated_metrics}, indent=2), flush=True)


if __name__ == "__main__":
    main()

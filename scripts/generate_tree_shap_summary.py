#!/usr/bin/env python3
"""Generate SHAP-based explainability summary for the tree benchmark model."""

from __future__ import annotations

import argparse
import json
import pickle
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from score_fraud_baseline_numpy import build_matrix


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate SHAP explainability summary for tree benchmark model.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument("--model-path", required=True, help="Tree benchmark model.pkl path")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets to sample",
    )
    parser.add_argument("--max-rows-per-dataset", type=int, default=80000, help="Per-dataset SQL row cap")
    parser.add_argument("--sample-rows", type=int, default=12000, help="Rows sampled for SHAP computation")
    parser.add_argument("--top-n", type=int, default=12, help="Top features to report")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed")
    parser.add_argument("--output-root", default="artifacts/models/fraud_tree_shap", help="Output root")
    return parser.parse_args()


def load_frame(conn: sqlite3.Connection, datasets: List[str], per_limit: int) -> pd.DataFrame:
    query = """
    SELECT
      tm.event_id,
      tm.dataset_id,
      tm.event_time,
      tm.channel,
      tm.txn_type,
      tm.currency,
      tm.amount,
      tm.label_fraud,
      COALESCE(f.payer_txn_count_24h, 0.0) AS payer_txn_count_24h,
      COALESCE(f.payer_amt_sum_24h, 0.0) AS payer_amt_sum_24h,
      COALESCE(g.graph_payer_incoming_txn_count_24h, 0.0) AS graph_payer_incoming_txn_count_24h,
      COALESCE(g.graph_payer_unique_payees_24h, 0.0) AS graph_payer_unique_payees_24h,
      COALESCE(g.graph_pair_txn_count_30d, 0.0) AS graph_pair_txn_count_30d,
      COALESCE(g.graph_pair_amt_sum_30d, 0.0) AS graph_pair_amt_sum_30d,
      COALESCE(g.graph_reciprocal_pair_txn_count_30d, 0.0) AS graph_reciprocal_pair_txn_count_30d
    FROM transaction_mart tm
    LEFT JOIN feature_payer_24h f
      ON f.event_id = tm.event_id
    LEFT JOIN feature_graph_24h g
      ON g.event_id = tm.event_id
    WHERE tm.dataset_id = ?
    ORDER BY tm.event_time
    LIMIT ?
    """
    frames: List[pd.DataFrame] = []
    for dataset_id in datasets:
        frames.append(pd.read_sql_query(query, conn, params=[dataset_id, int(per_limit)]))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    args = parse_args()

    db_path = Path(args.db_path)
    model_path = Path(args.model_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")
    if not model_path.exists():
        raise FileNotFoundError(f"Missing model: {model_path}")

    try:
        import shap  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "shap is required for SHAP explainability. Install with: python3 -m pip install shap"
        ) from exc

    with model_path.open("rb") as f:
        bundle = pickle.load(f)

    model = bundle["model"]
    feature_names = [str(x) for x in bundle["feature_names"]]
    numeric_names = [str(x) for x in bundle["numeric_feature_names"]]
    numeric_means = np.asarray(bundle["numeric_means"], dtype=np.float64)
    numeric_stds = np.asarray(bundle["numeric_stds"], dtype=np.float64)

    conn = sqlite3.connect(db_path)
    frame = load_frame(conn, args.datasets, int(args.max_rows_per_dataset))
    conn.close()
    if frame.empty:
        raise RuntimeError("No rows available for SHAP computation.")

    x = build_matrix(
        df=frame,
        feature_names=feature_names,
        numeric_names=numeric_names,
        numeric_means=numeric_means,
        numeric_stds=numeric_stds,
    )

    rng = np.random.default_rng(int(args.seed))
    if x.shape[0] > int(args.sample_rows):
        idx = rng.choice(x.shape[0], size=int(args.sample_rows), replace=False)
        x_sample = x[idx]
    else:
        x_sample = x

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(x_sample)
    if isinstance(shap_values, list):
        values = np.asarray(shap_values[-1], dtype=np.float64)
    else:
        values = np.asarray(shap_values, dtype=np.float64)

    if values.ndim != 2 or values.shape[1] != len(feature_names):
        raise RuntimeError("Unexpected SHAP values shape.")

    mean_abs = np.mean(np.abs(values), axis=0)
    order = np.argsort(-mean_abs)

    top_n = max(1, int(args.top_n))
    rows: List[Dict[str, float | str | int]] = []
    for rank, index in enumerate(order[:top_n], start=1):
        rows.append(
            {
                "rank": rank,
                "feature": feature_names[int(index)],
                "mean_abs_shap": float(mean_abs[int(index)]),
            }
        )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_root) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": run_id,
        "db_path": str(db_path),
        "model_path": str(model_path),
        "datasets": args.datasets,
        "sample_rows": int(x_sample.shape[0]),
        "feature_count": len(feature_names),
        "top_features": rows,
    }

    out_json = out_dir / "tree-shap-summary.json"
    out_md = out_dir / "tree-shap-summary.md"
    out_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    md_lines = [
        "# Tree SHAP Summary",
        "",
        f"Generated at (UTC): {summary['created_at_utc']}",
        "",
        "| Rank | Feature | Mean |SHAP| |",
        "|---:|---|---:|",
    ]
    for row in rows:
        md_lines.append(f"| {row['rank']} | {row['feature']} | {row['mean_abs_shap']:.6f} |")
    md_lines.append("")
    out_md.write_text("\n".join(md_lines), encoding="utf-8")

    latest_dir = Path(args.output_root) / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    for stale in latest_dir.glob("*"):
        if stale.is_file():
            stale.unlink()
    for file in out_dir.glob("*"):
        (latest_dir / file.name).write_text(file.read_text(encoding="utf-8"), encoding="utf-8")

    print(json.dumps({"output_dir": str(out_dir), "latest_dir": str(latest_dir), **summary}, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Score transaction_mart rows with benchmark numpy model and write fraud_scores_benchmark."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from modeling_benchmark_utils import apply_platt_scaler, build_numeric_interactions
from score_fraud_baseline_numpy import build_matrix
from train_fraud_baseline_numpy import stable_sigmoid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score transaction_mart with benchmark numpy model.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument("--model-path", required=True, help="Path to benchmark model.npz")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets to score",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Optional max rows to score")
    parser.add_argument("--max-rows-per-dataset", type=int, default=None, help="Optional per-dataset row cap")
    parser.add_argument("--chunksize", type=int, default=100000, help="SQL read chunksize")
    parser.add_argument(
        "--destination-table",
        default="fraud_scores_benchmark",
        help="Destination SQLite table",
    )
    parser.add_argument(
        "--output-root",
        default="artifacts/models/fraud_scoring_benchmark",
        help="Output root for scoring summary",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    model_path = Path(args.model_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")
    if not model_path.exists():
        raise FileNotFoundError(f"Missing model file: {model_path}")

    model = np.load(model_path, allow_pickle=True)
    w = model["weights"].astype(np.float64)
    b = float(model["bias"][0])
    base_feature_names = [str(x) for x in model["base_feature_names"].tolist()]
    numeric_feature_names = [str(x) for x in model["numeric_feature_names"].tolist()]
    numeric_means = model["numeric_means"].astype(np.float64)
    numeric_stds = model["numeric_stds"].astype(np.float64)
    cal_a = float(model["calibration_a"][0])
    cal_b = float(model["calibration_b"][0])

    read_conn = sqlite3.connect(db_path)
    write_conn = sqlite3.connect(db_path)
    read_conn.execute("PRAGMA journal_mode = WAL;")
    write_conn.execute("PRAGMA journal_mode = WAL;")
    read_conn.execute("PRAGMA busy_timeout = 60000;")
    write_conn.execute("PRAGMA busy_timeout = 60000;")

    write_conn.execute(f"DROP TABLE IF EXISTS {args.destination_table}")
    write_conn.commit()

    total_rows = 0
    dataset_rows: Dict[str, int] = {}

    def consume_chunk(chunk: pd.DataFrame) -> None:
        nonlocal total_rows
        x_base = build_matrix(
            df=chunk,
            feature_names=base_feature_names,
            numeric_names=numeric_feature_names,
            numeric_means=numeric_means,
            numeric_stds=numeric_stds,
        )
        n_numeric = len(numeric_feature_names)
        x_num = x_base[:, :n_numeric]
        x_inter, _ = build_numeric_interactions(x_numeric=x_num, numeric_feature_names=numeric_feature_names)
        x = np.hstack([x_base, x_inter]).astype(np.float64)

        with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
            logits = x @ w + b
        logits = np.nan_to_num(logits, nan=0.0, posinf=50.0, neginf=-50.0)
        raw_scores = stable_sigmoid(logits)
        calibrated_scores = apply_platt_scaler(raw_scores, cal_a, cal_b)

        out = pd.DataFrame(
            {
                "event_id": chunk["event_id"].astype(str),
                "dataset_id": chunk["dataset_id"].astype(str),
                "event_time": chunk["event_time"].astype(str),
                "fraud_score_raw": raw_scores.astype(np.float64),
                "fraud_score_calibrated": calibrated_scores.astype(np.float64),
                "fraud_score": calibrated_scores.astype(np.float64),
                "label_fraud": pd.to_numeric(chunk["label_fraud"], errors="coerce"),
            }
        )
        out.to_sql(args.destination_table, write_conn, if_exists="append", index=False, chunksize=1000)
        write_conn.commit()
        total_rows += len(out)
        for ds, cnt in out["dataset_id"].value_counts().items():
            dataset_rows[str(ds)] = dataset_rows.get(str(ds), 0) + int(cnt)
        print(f"[SCORE_BENCHMARK] rows={total_rows}", flush=True)

    base_query = """
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
    if args.max_rows_per_dataset is not None:
        per_limit = int(args.max_rows_per_dataset)
        for ds in args.datasets:
            for chunk in pd.read_sql_query(base_query, read_conn, params=[ds, per_limit], chunksize=args.chunksize):
                consume_chunk(chunk)
    else:
        placeholders = ",".join(["?"] * len(args.datasets))
        max_clause = f"LIMIT {int(args.max_rows)}" if args.max_rows else ""
        query = f"""
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
        WHERE tm.dataset_id IN ({placeholders})
        ORDER BY tm.event_time
        {max_clause}
        """
        for chunk in pd.read_sql_query(query, read_conn, params=args.datasets, chunksize=args.chunksize):
            consume_chunk(chunk)

    cur = write_conn.cursor()
    cur.executescript(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{args.destination_table}_event_id ON {args.destination_table}(event_id);
        CREATE INDEX IF NOT EXISTS idx_{args.destination_table}_dataset_time ON {args.destination_table}(dataset_id, event_time);
        """
    )
    write_conn.commit()

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "model_path": str(model_path),
        "destination_table": args.destination_table,
        "datasets": args.datasets,
        "total_scored_rows": total_rows,
        "rows_by_dataset": dataset_rows,
    }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_root) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "scoring-summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    read_conn.close()
    write_conn.close()
    print(f"[DONE] benchmark scoring summary: {out_dir / 'scoring-summary.json'}", flush=True)


if __name__ == "__main__":
    main()

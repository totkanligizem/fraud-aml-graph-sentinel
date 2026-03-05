#!/usr/bin/env python3
"""Score transaction_mart rows with optional tree benchmark model."""

from __future__ import annotations

import argparse
import json
import pickle
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from modeling_benchmark_utils import apply_platt_scaler
from score_fraud_baseline_numpy import build_matrix


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score with optional tree benchmark model.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument("--model-path", required=True, help="Path to tree benchmark model.pkl")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets to score",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Optional max rows")
    parser.add_argument("--max-rows-per-dataset", type=int, default=300000, help="Per-dataset row cap")
    parser.add_argument("--chunksize", type=int, default=100000, help="Read chunksize")
    parser.add_argument("--destination-table", default="fraud_scores_tree", help="Destination table name")
    parser.add_argument(
        "--output-root",
        default="artifacts/models/fraud_scoring_tree",
        help="Output root for scoring summary",
    )
    return parser.parse_args()


def validate_identifier(name: str, label: str) -> str:
    value = str(name).strip()
    if not value:
        raise ValueError(f"{label} cannot be empty.")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ValueError(f"Invalid {label}: {value}")
    return value


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    model_path = Path(args.model_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")
    if not model_path.exists():
        raise FileNotFoundError(f"Missing model file: {model_path}")

    try:
        # Ensures sklearn classes required by pickle are importable.
        import sklearn  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "scikit-learn is required to load tree benchmark model. "
            "Install with: python3 -m pip install scikit-learn"
        ) from exc

    with model_path.open("rb") as f:
        bundle = pickle.load(f)
    model = bundle["model"]
    feature_names = [str(x) for x in bundle["feature_names"]]
    numeric_names = [str(x) for x in bundle["numeric_feature_names"]]
    numeric_means = np.asarray(bundle["numeric_means"], dtype=np.float64)
    numeric_stds = np.asarray(bundle["numeric_stds"], dtype=np.float64)
    cal_a = float(bundle.get("calibration_a", 1.0))
    cal_b = float(bundle.get("calibration_b", 0.0))

    destination_table = validate_identifier(args.destination_table, "destination table")

    read_conn = sqlite3.connect(db_path)
    write_conn = sqlite3.connect(db_path)
    read_conn.execute("PRAGMA journal_mode = WAL;")
    write_conn.execute("PRAGMA journal_mode = WAL;")
    read_conn.execute("PRAGMA busy_timeout = 60000;")
    write_conn.execute("PRAGMA busy_timeout = 60000;")

    write_conn.execute(f"DROP TABLE IF EXISTS {destination_table}")
    write_conn.commit()

    total_rows = 0
    dataset_rows: Dict[str, int] = {}

    def consume_chunk(chunk: pd.DataFrame) -> None:
        nonlocal total_rows
        x = build_matrix(
            df=chunk,
            feature_names=feature_names,
            numeric_names=numeric_names,
            numeric_means=numeric_means,
            numeric_stds=numeric_stds,
        )
        raw_scores = model.predict_proba(x)[:, 1].astype(np.float64)
        calibrated_scores = apply_platt_scaler(raw_scores, cal_a, cal_b)

        out = pd.DataFrame(
            {
                "event_id": chunk["event_id"].astype(str),
                "dataset_id": chunk["dataset_id"].astype(str),
                "event_time": chunk["event_time"].astype(str),
                "fraud_score_raw": raw_scores,
                "fraud_score_calibrated": calibrated_scores,
                "fraud_score": calibrated_scores,
                "label_fraud": pd.to_numeric(chunk["label_fraud"], errors="coerce"),
            }
        )
        out.to_sql(destination_table, write_conn, if_exists="append", index=False, chunksize=1000)
        write_conn.commit()
        total_rows += len(out)
        for ds, cnt in out["dataset_id"].value_counts().items():
            dataset_rows[str(ds)] = dataset_rows.get(str(ds), 0) + int(cnt)
        print(f"[SCORE_TREE] rows={total_rows}", flush=True)

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
        CREATE INDEX IF NOT EXISTS idx_{destination_table}_event_id ON {destination_table}(event_id);
        CREATE INDEX IF NOT EXISTS idx_{destination_table}_dataset_time ON {destination_table}(dataset_id, event_time);
        """
    )
    write_conn.commit()

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "model_path": str(model_path),
        "destination_table": destination_table,
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
    print(f"[DONE] tree scoring summary: {out_dir / 'scoring-summary.json'}", flush=True)


if __name__ == "__main__":
    main()

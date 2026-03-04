#!/usr/bin/env python3
"""
Build investigation queue from fraud_scores and compute ranking metrics.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ranked investigation queue from fraud_scores.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument("--top-k", type=int, default=50, help="Top-K cutoff for ranking metrics")
    parser.add_argument(
        "--output-root",
        default="artifacts/models/ranking",
        help="Output root for ranking metrics",
    )
    return parser.parse_args()


def dcg_at_k(relevance: np.ndarray, k: int) -> float:
    r = relevance[:k].astype(np.float64)
    if len(r) == 0:
        return 0.0
    discounts = np.log2(np.arange(2, len(r) + 2))
    gains = (2**r - 1) / discounts
    return float(np.sum(gains))


def ndcg_at_k(relevance: np.ndarray, k: int) -> float:
    dcg = dcg_at_k(relevance, k)
    ideal = np.sort(relevance)[::-1]
    idcg = dcg_at_k(ideal, k)
    if idcg == 0:
        return 0.0
    return float(dcg / idcg)


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
          event_id,
          dataset_id,
          event_time,
          fraud_score,
          COALESCE(label_fraud, 0) AS label_fraud
        FROM fraud_scores
        """,
        conn,
    )
    if df.empty:
        raise RuntimeError("fraud_scores table is empty. Run scoring first.")

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df = df.dropna(subset=["event_time"]).copy()
    df["event_date"] = df["event_time"].dt.date.astype(str)
    df["queue_id"] = df["dataset_id"].astype(str) + "|" + df["event_date"]
    df["label_fraud"] = pd.to_numeric(df["label_fraud"], errors="coerce").fillna(0).astype(int)
    df["fraud_score"] = pd.to_numeric(df["fraud_score"], errors="coerce").fillna(0.0)

    df = df.sort_values(["queue_id", "fraud_score"], ascending=[True, False]).reset_index(drop=True)
    df["rank_in_queue"] = df.groupby("queue_id").cumcount() + 1

    df.to_sql("alert_queue", conn, if_exists="replace", index=False, chunksize=1000)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aq_queue_rank ON alert_queue(queue_id, rank_in_queue)")
    conn.commit()

    queue_metrics: List[Dict[str, float]] = []
    for queue_id, g in df.groupby("queue_id", sort=False):
        rel = g["label_fraud"].to_numpy(dtype=np.int64)
        k = min(args.top_k, len(rel))
        topk = rel[:k]
        precision_k = float(np.mean(topk)) if k > 0 else 0.0
        ndcg_k = ndcg_at_k(rel, k)
        positives = int(np.sum(rel))
        queue_metrics.append(
            {
                "queue_id": str(queue_id),
                "queue_size": int(len(rel)),
                "positives": positives,
                f"precision_at_{args.top_k}": precision_k,
                f"ndcg_at_{args.top_k}": ndcg_k,
            }
        )

    qm = pd.DataFrame(queue_metrics)
    metric_p = f"precision_at_{args.top_k}"
    metric_n = f"ndcg_at_{args.top_k}"

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "top_k": args.top_k,
        "queue_count": int(len(qm)),
        "mean_precision_at_k": float(qm[metric_p].mean()),
        "mean_ndcg_at_k": float(qm[metric_n].mean()),
        "median_precision_at_k": float(qm[metric_p].median()),
        "median_ndcg_at_k": float(qm[metric_n].median()),
        "queues_with_positive_labels": int((qm["positives"] > 0).sum()),
    }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_root) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    qm.to_csv(out_dir / "queue_metrics.csv", index=False)
    (out_dir / "ranking-summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    conn.close()

    print(json.dumps(summary, indent=2), flush=True)
    print(f"[DONE] ranking outputs: {out_dir}", flush=True)


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Validate graph layer tables in the local SQLite warehouse.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, List


REQUIRED_TABLES = [
    "graph_party_node",
    "graph_party_edge",
    "graph_account_node",
    "graph_account_edge",
    "graph_party_cluster_membership",
    "graph_party_cluster_summary",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate graph layer tables in SQLite.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    return parser.parse_args()


def ensure(cond: bool, msg: str, errors: List[str]) -> None:
    if not cond:
        errors.append(msg)


def scalar(cur: sqlite3.Cursor, query: str) -> int:
    return int(cur.execute(query).fetchone()[0])


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    existing = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    errors: List[str] = []
    table_counts: Dict[str, int] = {}

    for table_name in REQUIRED_TABLES:
        ensure(table_name in existing, f"Missing table: {table_name}", errors)
        if table_name not in existing:
            continue
        cnt = scalar(cur, f"SELECT COUNT(*) FROM {table_name}")
        table_counts[table_name] = cnt
        ensure(cnt > 0, f"Empty table: {table_name}", errors)

    if all(t in existing for t in REQUIRED_TABLES):
        quality_metrics = {
            "null_party_node_id": scalar(cur, "SELECT COUNT(*) FROM graph_party_node WHERE party_id IS NULL OR TRIM(party_id) = ''"),
            "null_party_edge_src": scalar(cur, "SELECT COUNT(*) FROM graph_party_edge WHERE src_party_id IS NULL OR TRIM(src_party_id) = ''"),
            "null_party_edge_dst": scalar(cur, "SELECT COUNT(*) FROM graph_party_edge WHERE dst_party_id IS NULL OR TRIM(dst_party_id) = ''"),
            "duplicate_party_node_id": scalar(
                cur,
                """
                SELECT COUNT(*) FROM (
                  SELECT party_id, COUNT(*) AS c
                  FROM graph_party_node
                  GROUP BY 1
                  HAVING COUNT(*) > 1
                )
                """,
            ),
            "duplicate_account_node_id": scalar(
                cur,
                """
                SELECT COUNT(*) FROM (
                  SELECT account_id, COUNT(*) AS c
                  FROM graph_account_node
                  GROUP BY 1
                  HAVING COUNT(*) > 1
                )
                """,
            ),
            "invalid_party_node_risk_score": scalar(
                cur,
                "SELECT COUNT(*) FROM graph_party_node WHERE risk_score IS NULL OR risk_score < 0 OR risk_score > 1",
            ),
            "invalid_party_edge_risk_score": scalar(
                cur,
                "SELECT COUNT(*) FROM graph_party_edge WHERE risk_score IS NULL OR risk_score < 0 OR risk_score > 1",
            ),
            "invalid_account_node_risk_score": scalar(
                cur,
                "SELECT COUNT(*) FROM graph_account_node WHERE risk_score IS NULL OR risk_score < 0 OR risk_score > 1",
            ),
            "invalid_account_edge_risk_score": scalar(
                cur,
                "SELECT COUNT(*) FROM graph_account_edge WHERE risk_score IS NULL OR risk_score < 0 OR risk_score > 1",
            ),
            "shared_party_account_node_ids": scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM graph_party_node p
                INNER JOIN graph_account_node a
                  ON p.party_id = a.account_id
                """,
            ),
            "shared_party_account_edge_pairs": scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM graph_party_edge p
                INNER JOIN graph_account_edge a
                  ON p.src_party_id = a.src_account_id
                 AND p.dst_party_id = a.dst_account_id
                """,
            ),
            "cluster_membership_without_summary": scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM graph_party_cluster_membership m
                LEFT JOIN graph_party_cluster_summary s
                  ON m.cluster_id = s.cluster_id
                WHERE s.cluster_id IS NULL
                """,
            ),
            "cluster_summary_party_count_mismatch": scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM (
                  SELECT s.cluster_id
                  FROM graph_party_cluster_summary s
                  LEFT JOIN (
                    SELECT cluster_id, COUNT(*) AS membership_count
                    FROM graph_party_cluster_membership
                    GROUP BY 1
                  ) m
                    ON s.cluster_id = m.cluster_id
                  WHERE COALESCE(m.membership_count, 0) <> s.party_count
                )
                """,
            ),
        }
        for name, value in quality_metrics.items():
            ensure(value == 0, f"{name}: {value}", errors)
    else:
        quality_metrics = {}

    conn.close()

    report = {
        "ok": len(errors) == 0,
        "table_counts": table_counts,
        "quality_metrics": quality_metrics,
        "errors": errors,
    }
    print(json.dumps(report, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

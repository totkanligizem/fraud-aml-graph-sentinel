#!/usr/bin/env python3
"""
Build graph-oriented tables from the local SQLite warehouse.

Outputs:
- graph_party_node
- graph_party_edge
- graph_account_node
- graph_account_edge
- graph_party_cluster_membership
- graph_party_cluster_summary
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build graph layer tables from warehouse tables.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument(
        "--fraud-score-threshold",
        type=float,
        default=0.80,
        help="Event fraud score threshold for high-risk flags",
    )
    parser.add_argument(
        "--cluster-edge-risk-threshold",
        type=float,
        default=0.65,
        help="Minimum edge risk score to include in suspicious graph clustering",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=2,
        help="Minimum party count for cluster summary output",
    )
    return parser.parse_args()


def ensure_required_tables(conn: sqlite3.Connection, required_tables: Iterable[str]) -> None:
    cur = conn.cursor()
    existing = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    missing = [t for t in required_tables if t not in existing]
    if missing:
        raise RuntimeError(f"Missing required tables: {', '.join(missing)}")


def drop_graph_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        DROP TABLE IF EXISTS graph_party_cluster_summary;
        DROP TABLE IF EXISTS graph_party_cluster_membership;
        DROP TABLE IF EXISTS graph_account_node;
        DROP TABLE IF EXISTS graph_account_edge;
        DROP TABLE IF EXISTS graph_party_node;
        DROP TABLE IF EXISTS graph_party_edge;
        """
    )
    conn.commit()


def build_enriched_event_temp(conn: sqlite3.Connection, fraud_score_threshold: float) -> None:
    cur = conn.cursor()
    cur.executescript("DROP TABLE IF EXISTS graph_event_enriched;")
    cur.execute(
        f"""
        CREATE TEMP TABLE graph_event_enriched AS
        SELECT
          tm.event_id,
          tm.dataset_id,
          tm.event_time,
          tm.event_date,
          tm.payer_party_id,
          tm.payee_party_id,
          tm.payer_account_id,
          tm.payee_account_id,
          CAST(tm.amount AS REAL) AS amount,
          COALESCE(CAST(fs.fraud_score AS REAL), 0.0) AS fraud_score,
          COALESCE(CAST(tm.label_fraud AS INTEGER), 0) AS label_fraud,
          COALESCE(CAST(tm.label_aml AS INTEGER), 0) AS label_aml,
          CASE WHEN aq.event_id IS NOT NULL THEN 1 ELSE 0 END AS is_alert,
          CASE
            WHEN COALESCE(CAST(fs.fraud_score AS REAL), 0.0) >= {float(fraud_score_threshold)}
            THEN 1 ELSE 0
          END AS is_high_risk
        FROM transaction_mart tm
        LEFT JOIN fraud_scores fs
          ON tm.event_id = fs.event_id
        LEFT JOIN alert_queue aq
          ON tm.event_id = aq.event_id
        WHERE tm.event_time IS NOT NULL;
        """
    )
    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_gee_event_id ON graph_event_enriched(event_id);
        CREATE INDEX IF NOT EXISTS idx_gee_payer_party ON graph_event_enriched(payer_party_id);
        CREATE INDEX IF NOT EXISTS idx_gee_payee_party ON graph_event_enriched(payee_party_id);
        CREATE INDEX IF NOT EXISTS idx_gee_payer_account ON graph_event_enriched(payer_account_id);
        CREATE INDEX IF NOT EXISTS idx_gee_payee_account ON graph_event_enriched(payee_account_id);
        """
    )
    conn.commit()


def build_entity_graph(conn: sqlite3.Connection, entity_name: str, src_col: str, dst_col: str) -> None:
    cur = conn.cursor()
    edge_table = f"graph_{entity_name}_edge"
    node_table = f"graph_{entity_name}_node"
    entity_id_col = f"{entity_name}_id"
    src_id_col = f"src_{entity_name}_id"
    dst_id_col = f"dst_{entity_name}_id"

    cur.executescript(
        f"""
        DROP TABLE IF EXISTS {edge_table};
        CREATE TABLE {edge_table} AS
        SELECT
          {src_col} AS {src_id_col},
          {dst_col} AS {dst_id_col},
          MIN(event_time) AS first_seen,
          MAX(event_time) AS last_seen,
          COUNT(*) AS txn_count,
          ROUND(SUM(COALESCE(amount, 0.0)), 6) AS amount_sum,
          ROUND(AVG(COALESCE(amount, 0.0)), 6) AS avg_amount,
          COUNT(DISTINCT dataset_id) AS dataset_count,
          SUM(label_fraud) AS fraud_event_count,
          SUM(label_aml) AS aml_event_count,
          SUM(is_alert) AS alert_event_count,
          SUM(is_high_risk) AS high_risk_event_count,
          ROUND(MAX(fraud_score), 6) AS max_fraud_score,
          0.0 AS risk_score
        FROM graph_event_enriched
        WHERE {src_col} IS NOT NULL AND TRIM({src_col}) <> ''
          AND {dst_col} IS NOT NULL AND TRIM({dst_col}) <> ''
        GROUP BY 1, 2;

        CREATE INDEX IF NOT EXISTS idx_{edge_table}_src ON {edge_table}({src_id_col});
        CREATE INDEX IF NOT EXISTS idx_{edge_table}_dst ON {edge_table}({dst_id_col});
        """
    )
    cur.executescript(
        f"""
        UPDATE {edge_table}
        SET risk_score = ROUND(
          MIN(
            1.0,
            (0.55 * COALESCE(max_fraud_score, 0.0))
            + CASE WHEN fraud_event_count > 0 THEN 0.20 ELSE 0.0 END
            + CASE WHEN aml_event_count > 0 THEN 0.20 ELSE 0.0 END
            + MIN(0.10, COALESCE(high_risk_event_count, 0) / 20.0)
            + MIN(0.10, COALESCE(alert_event_count, 0) / 20.0)
          ),
          6
        );
        CREATE INDEX IF NOT EXISTS idx_{edge_table}_risk ON {edge_table}(risk_score);
        """
    )

    cur.executescript(
        f"""
        DROP TABLE IF EXISTS {node_table};
        CREATE TABLE {node_table} AS
        WITH valid_events AS (
          SELECT *
          FROM graph_event_enriched
          WHERE {src_col} IS NOT NULL AND TRIM({src_col}) <> ''
            AND {dst_col} IS NOT NULL AND TRIM({dst_col}) <> ''
        ),
        node_events AS (
          SELECT
            event_id,
            {src_col} AS node_id,
            {dst_col} AS counterparty_id,
            event_time,
            dataset_id,
            amount,
            1 AS out_txn_count,
            0 AS in_txn_count,
            amount AS out_amount_sum,
            0.0 AS in_amount_sum,
            label_fraud,
            label_aml,
            is_alert,
            is_high_risk,
            fraud_score
          FROM valid_events
          UNION ALL
          SELECT
            event_id,
            {dst_col} AS node_id,
            {src_col} AS counterparty_id,
            event_time,
            dataset_id,
            amount,
            0 AS out_txn_count,
            1 AS in_txn_count,
            0.0 AS out_amount_sum,
            amount AS in_amount_sum,
            label_fraud,
            label_aml,
            is_alert,
            is_high_risk,
            fraud_score
          FROM valid_events
        )
        SELECT
          node_id AS {entity_id_col},
          MIN(event_time) AS first_seen,
          MAX(event_time) AS last_seen,
          SUM(out_txn_count) AS out_txn_count,
          SUM(in_txn_count) AS in_txn_count,
          COUNT(DISTINCT event_id) AS total_txn_count,
          ROUND(SUM(COALESCE(out_amount_sum, 0.0)), 6) AS out_amount_sum,
          ROUND(SUM(COALESCE(in_amount_sum, 0.0)), 6) AS in_amount_sum,
          ROUND(SUM(COALESCE(out_amount_sum, 0.0) + COALESCE(in_amount_sum, 0.0)), 6) AS total_amount_sum,
          COUNT(DISTINCT counterparty_id) AS distinct_counterparty_count,
          COUNT(DISTINCT dataset_id) AS dataset_count,
          SUM(label_fraud) AS fraud_event_count,
          SUM(label_aml) AS aml_event_count,
          SUM(is_alert) AS alert_event_count,
          SUM(is_high_risk) AS high_risk_event_count,
          ROUND(MAX(fraud_score), 6) AS max_fraud_score,
          0.0 AS risk_score
        FROM node_events
        GROUP BY node_id;

        CREATE INDEX IF NOT EXISTS idx_{node_table}_id ON {node_table}({entity_id_col});
        """
    )
    cur.executescript(
        f"""
        UPDATE {node_table}
        SET risk_score = ROUND(
          MIN(
            1.0,
            (0.50 * COALESCE(max_fraud_score, 0.0))
            + CASE WHEN fraud_event_count > 0 THEN 0.20 ELSE 0.0 END
            + CASE WHEN aml_event_count > 0 THEN 0.20 ELSE 0.0 END
            + MIN(0.05, COALESCE(high_risk_event_count, 0) / 50.0)
            + MIN(0.05, COALESCE(alert_event_count, 0) / 50.0)
          ),
          6
        );
        CREATE INDEX IF NOT EXISTS idx_{node_table}_risk ON {node_table}(risk_score);
        """
    )
    conn.commit()


def _find(parent: Dict[str, str], x: str) -> str:
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


def _union(parent: Dict[str, str], a: str, b: str) -> None:
    ra = _find(parent, a)
    rb = _find(parent, b)
    if ra != rb:
        parent[rb] = ra


def build_party_clusters(
    conn: sqlite3.Connection,
    cluster_edge_risk_threshold: float,
    min_cluster_size: int,
) -> Dict[str, int]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    suspicious_edges = list(
        cur.execute(
            """
            SELECT
              src_party_id,
              dst_party_id,
              first_seen,
              last_seen,
              txn_count,
              amount_sum,
              fraud_event_count,
              aml_event_count,
              alert_event_count,
              high_risk_event_count,
              max_fraud_score,
              risk_score
            FROM graph_party_edge
            WHERE (risk_score >= ? OR fraud_event_count > 0 OR aml_event_count > 0
                   OR alert_event_count > 0 OR high_risk_event_count > 0)
              AND src_party_id IS NOT NULL AND TRIM(src_party_id) <> ''
              AND dst_party_id IS NOT NULL AND TRIM(dst_party_id) <> ''
            """,
            (float(cluster_edge_risk_threshold),),
        )
    )

    parent: Dict[str, str] = {}
    for row in suspicious_edges:
        _union(parent, str(row["src_party_id"]), str(row["dst_party_id"]))

    root_to_members: Dict[str, set[str]] = {}
    for node in list(parent):
        root = _find(parent, node)
        root_to_members.setdefault(root, set()).add(node)

    component_edges: Dict[str, List[sqlite3.Row]] = {}
    for row in suspicious_edges:
        root = _find(parent, str(row["src_party_id"]))
        component_edges.setdefault(root, []).append(row)

    filtered_components: List[Tuple[str, set[str], List[sqlite3.Row]]] = []
    for root, members in root_to_members.items():
        edges = component_edges.get(root, [])
        if len(members) >= int(min_cluster_size) and edges:
            filtered_components.append((root, members, edges))

    filtered_components.sort(
        key=lambda x: (
            -len(x[1]),
            -len(x[2]),
            -max(float(r["risk_score"] or 0.0) for r in x[2]),
            x[0],
        )
    )

    cur.executescript(
        """
        DROP TABLE IF EXISTS graph_party_cluster_membership;
        DROP TABLE IF EXISTS graph_party_cluster_summary;
        CREATE TABLE graph_party_cluster_membership (
          cluster_id TEXT,
          party_id TEXT
        );
        CREATE TABLE graph_party_cluster_summary (
          cluster_id TEXT,
          party_count INTEGER,
          edge_count INTEGER,
          txn_count INTEGER,
          amount_sum REAL,
          fraud_event_count INTEGER,
          aml_event_count INTEGER,
          alert_event_count INTEGER,
          high_risk_event_count INTEGER,
          max_fraud_score REAL,
          mean_edge_risk_score REAL,
          max_edge_risk_score REAL,
          first_seen TEXT,
          last_seen TEXT
        );
        """
    )

    membership_rows: List[Tuple[str, str]] = []
    summary_rows: List[Tuple[object, ...]] = []
    for idx, (_, members, edges) in enumerate(filtered_components, start=1):
        cluster_id = f"gcl_{idx:05d}"
        for party_id in sorted(members):
            membership_rows.append((cluster_id, party_id))

        edge_count = len(edges)
        txn_count = sum(int(r["txn_count"] or 0) for r in edges)
        amount_sum = round(sum(float(r["amount_sum"] or 0.0) for r in edges), 6)
        fraud_event_count = sum(int(r["fraud_event_count"] or 0) for r in edges)
        aml_event_count = sum(int(r["aml_event_count"] or 0) for r in edges)
        alert_event_count = sum(int(r["alert_event_count"] or 0) for r in edges)
        high_risk_event_count = sum(int(r["high_risk_event_count"] or 0) for r in edges)
        max_fraud_score = round(max(float(r["max_fraud_score"] or 0.0) for r in edges), 6)
        mean_edge_risk_score = round(
            sum(float(r["risk_score"] or 0.0) for r in edges) / max(edge_count, 1),
            6,
        )
        max_edge_risk_score = round(max(float(r["risk_score"] or 0.0) for r in edges), 6)
        first_seen = min(str(r["first_seen"]) for r in edges)
        last_seen = max(str(r["last_seen"]) for r in edges)

        summary_rows.append(
            (
                cluster_id,
                len(members),
                edge_count,
                txn_count,
                amount_sum,
                fraud_event_count,
                aml_event_count,
                alert_event_count,
                high_risk_event_count,
                max_fraud_score,
                mean_edge_risk_score,
                max_edge_risk_score,
                first_seen,
                last_seen,
            )
        )

    if membership_rows:
        cur.executemany(
            "INSERT INTO graph_party_cluster_membership(cluster_id, party_id) VALUES (?, ?)",
            membership_rows,
        )
    if summary_rows:
        cur.executemany(
            """
            INSERT INTO graph_party_cluster_summary(
              cluster_id, party_count, edge_count, txn_count, amount_sum,
              fraud_event_count, aml_event_count, alert_event_count, high_risk_event_count,
              max_fraud_score, mean_edge_risk_score, max_edge_risk_score, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            summary_rows,
        )

    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_gpcm_cluster_id ON graph_party_cluster_membership(cluster_id);
        CREATE INDEX IF NOT EXISTS idx_gpcm_party_id ON graph_party_cluster_membership(party_id);
        CREATE INDEX IF NOT EXISTS idx_gpcs_cluster_id ON graph_party_cluster_summary(cluster_id);
        CREATE INDEX IF NOT EXISTS idx_gpcs_max_risk ON graph_party_cluster_summary(max_edge_risk_score);
        """
    )
    conn.commit()

    return {
        "cluster_count": len(summary_rows),
        "cluster_membership_rows": len(membership_rows),
        "suspicious_edge_count": len(suspicious_edges),
    }


def query_count(cur: sqlite3.Cursor, table_name: str) -> int:
    return int(cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    conn = sqlite3.connect(db_path)
    ensure_required_tables(conn, ["transaction_mart", "fraud_scores", "alert_queue"])
    drop_graph_tables(conn)
    build_enriched_event_temp(conn, fraud_score_threshold=args.fraud_score_threshold)
    build_entity_graph(conn, entity_name="party", src_col="payer_party_id", dst_col="payee_party_id")
    build_entity_graph(conn, entity_name="account", src_col="payer_account_id", dst_col="payee_account_id")
    cluster_summary = build_party_clusters(
        conn,
        cluster_edge_risk_threshold=args.cluster_edge_risk_threshold,
        min_cluster_size=args.min_cluster_size,
    )

    cur = conn.cursor()
    table_counts = {
        "graph_party_node": query_count(cur, "graph_party_node"),
        "graph_party_edge": query_count(cur, "graph_party_edge"),
        "graph_account_node": query_count(cur, "graph_account_node"),
        "graph_account_edge": query_count(cur, "graph_account_edge"),
        "graph_party_cluster_membership": query_count(cur, "graph_party_cluster_membership"),
        "graph_party_cluster_summary": query_count(cur, "graph_party_cluster_summary"),
    }
    conn.close()

    report = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "fraud_score_threshold": args.fraud_score_threshold,
        "cluster_edge_risk_threshold": args.cluster_edge_risk_threshold,
        "min_cluster_size": args.min_cluster_size,
        "table_counts": table_counts,
        "cluster_summary": cluster_summary,
    }
    out_dir = Path("artifacts/graph")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"graph-build-summary-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"[DONE] summary: {out_file}")


if __name__ == "__main__":
    main()

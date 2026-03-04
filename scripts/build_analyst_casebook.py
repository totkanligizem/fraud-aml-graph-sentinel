#!/usr/bin/env python3
"""Build deterministic analyst case packets from queue and graph layers."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analyst-ready case packets from local warehouse tables.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument("--top-queues", type=int, default=12, help="Number of top queues to package")
    parser.add_argument("--per-dataset-cap", type=int, default=4, help="Initial per-dataset cap before overall fill")
    parser.add_argument("--events-per-queue", type=int, default=8, help="Top ranked events to include per queue")
    parser.add_argument("--output-root", default="artifacts/agent/casebook", help="Output root directory")
    return parser.parse_args()


def fetch_rows(conn: sqlite3.Connection, query: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def build_in_clause(items: Iterable[str]) -> tuple[str, List[str]]:
    values = [str(item) for item in items if item]
    if not values:
        return "(NULL)", []
    return "(" + ",".join("?" for _ in values) + ")", values


def load_ranked_queues(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    return fetch_rows(
        conn,
        """
        WITH queue_stats AS (
          SELECT
            dataset_id,
            queue_id,
            event_date,
            COUNT(*) AS queue_rows,
            AVG(fraud_score) AS avg_queue_score,
            MAX(fraud_score) AS max_queue_score,
            SUM(COALESCE(label_fraud, 0)) AS positive_rows
          FROM alert_queue
          GROUP BY dataset_id, queue_id, event_date
        )
        SELECT
          dataset_id,
          queue_id,
          event_date,
          queue_rows,
          avg_queue_score,
          max_queue_score,
          positive_rows,
          ROW_NUMBER() OVER (
            PARTITION BY dataset_id
            ORDER BY avg_queue_score DESC, positive_rows DESC, queue_rows DESC, queue_id ASC
          ) AS dataset_rank,
          ROW_NUMBER() OVER (
            ORDER BY avg_queue_score DESC, positive_rows DESC, queue_rows DESC, queue_id ASC
          ) AS overall_rank
        FROM queue_stats
        ORDER BY overall_rank
        """
    )


def select_casebook_queues(rows: List[Dict[str, Any]], top_n: int, per_dataset_cap: int) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    seen: set[str] = set()
    seeded_datasets: set[str] = set()

    # First pass: guarantee at least one queue from each scored dataset if available.
    for row in rows:
        dataset_id = str(row["dataset_id"])
        queue_id = str(row["queue_id"])
        if dataset_id in seeded_datasets or queue_id in seen:
            continue
        selected.append(row)
        seen.add(queue_id)
        seeded_datasets.add(dataset_id)
        if len(selected) >= int(top_n):
            return selected

    # Second pass: respect the per-dataset cap while filling breadth-first.
    for row in rows:
        if int(row["dataset_rank"]) > int(per_dataset_cap):
            continue
        queue_id = str(row["queue_id"])
        if queue_id in seen:
            continue
        selected.append(row)
        seen.add(queue_id)
        if len(selected) >= int(top_n):
            return selected

    for row in rows:
        queue_id = str(row["queue_id"])
        if queue_id in seen:
            continue
        selected.append(row)
        seen.add(queue_id)
        if len(selected) >= int(top_n):
            break

    return selected


def load_queue_events(conn: sqlite3.Connection, queue_id: str, event_limit: int) -> List[Dict[str, Any]]:
    return fetch_rows(
        conn,
        """
        SELECT
          aq.queue_id,
          aq.rank_in_queue,
          aq.fraud_score,
          aq.label_fraud,
          tm.event_id,
          tm.source_event_id,
          tm.dataset_id,
          tm.event_time,
          tm.event_date,
          tm.amount,
          tm.currency,
          tm.channel,
          tm.txn_type,
          tm.mcc_category,
          tm.geo,
          tm.device_id,
          tm.ip_prefix,
          tm.email_domain,
          tm.label_aml,
          tm.label_source,
          tm.payer_party_id,
          tm.payee_party_id,
          tm.payer_account_id,
          tm.payee_account_id
        FROM alert_queue aq
        INNER JOIN transaction_mart tm
          ON aq.event_id = tm.event_id
        WHERE aq.queue_id = ?
        ORDER BY aq.rank_in_queue ASC
        LIMIT ?
        """,
        (queue_id, int(event_limit)),
    )


def load_queue_metrics(conn: sqlite3.Connection, queue_id: str) -> Dict[str, Any]:
    rows = fetch_rows(
        conn,
        """
        SELECT
          COUNT(*) AS queue_rows,
          AVG(aq.fraud_score) AS avg_queue_score,
          MAX(aq.fraud_score) AS max_queue_score,
          SUM(COALESCE(aq.label_fraud, 0)) AS positive_rows,
          SUM(COALESCE(tm.label_aml, 0)) AS aml_rows,
          AVG(tm.amount) AS avg_amount,
          SUM(tm.amount) AS amount_sum
        FROM alert_queue aq
        INNER JOIN transaction_mart tm
          ON aq.event_id = tm.event_id
        WHERE aq.queue_id = ?
        """,
        (queue_id,),
    )
    return rows[0] if rows else {}


def load_party_risk(conn: sqlite3.Connection, parties: Sequence[str]) -> List[Dict[str, Any]]:
    clause, params = build_in_clause(parties)
    return fetch_rows(
        conn,
        f"""
        SELECT
          party_id,
          total_txn_count,
          distinct_counterparty_count,
          total_amount_sum,
          fraud_event_count,
          aml_event_count,
          alert_event_count,
          high_risk_event_count,
          max_fraud_score,
          risk_score
        FROM graph_party_node
        WHERE party_id IN {clause}
        ORDER BY risk_score DESC, total_amount_sum DESC, total_txn_count DESC, party_id ASC
        """,
        params,
    )


def load_cluster_risk(conn: sqlite3.Connection, parties: Sequence[str]) -> List[Dict[str, Any]]:
    clause, params = build_in_clause(parties)
    return fetch_rows(
        conn,
        f"""
        SELECT
          m.cluster_id,
          COUNT(*) AS matched_party_count,
          s.party_count,
          s.edge_count,
          s.txn_count,
          s.amount_sum,
          s.fraud_event_count,
          s.aml_event_count,
          s.alert_event_count,
          s.high_risk_event_count,
          s.max_fraud_score,
          s.mean_edge_risk_score,
          s.max_edge_risk_score,
          s.first_seen,
          s.last_seen
        FROM graph_party_cluster_membership m
        INNER JOIN graph_party_cluster_summary s
          ON m.cluster_id = s.cluster_id
        WHERE m.party_id IN {clause}
        GROUP BY
          m.cluster_id,
          s.party_count,
          s.edge_count,
          s.txn_count,
          s.amount_sum,
          s.fraud_event_count,
          s.aml_event_count,
          s.alert_event_count,
          s.high_risk_event_count,
          s.max_fraud_score,
          s.mean_edge_risk_score,
          s.max_edge_risk_score,
          s.first_seen,
          s.last_seen
        ORDER BY s.max_edge_risk_score DESC, matched_party_count DESC, s.amount_sum DESC, m.cluster_id ASC
        """,
        params,
    )


def build_event_evidence(events: List[Dict[str, Any]]) -> List[str]:
    evidence: List[str] = []
    if not events:
        return evidence
    top_event = events[0]
    evidence.append(
        f"Top ranked event {top_event['event_id']} score {float(top_event['fraud_score']):.3f} on {top_event['event_time']}."
    )
    fraud_hits = sum(int(event.get("label_fraud") or 0) for event in events)
    aml_hits = sum(int(event.get("label_aml") or 0) for event in events)
    if fraud_hits:
        evidence.append(f"Top packet slice contains {fraud_hits} fraud-labeled events.")
    if aml_hits:
        evidence.append(f"Top packet slice contains {aml_hits} AML-labeled events.")
    party_count = len(
        {
            party
            for event in events
            for party in [event.get("payer_party_id"), event.get("payee_party_id")]
            if party
        }
    )
    evidence.append(f"Top packet slice spans {party_count} distinct parties.")
    return evidence


def build_case_packet(
    conn: sqlite3.Connection,
    queue_row: Dict[str, Any],
    event_limit: int,
) -> Dict[str, Any]:
    queue_id = str(queue_row["queue_id"])
    events = load_queue_events(conn, queue_id, event_limit)
    metrics = load_queue_metrics(conn, queue_id)
    parties = sorted(
        {
            str(party)
            for event in events
            for party in [event.get("payer_party_id"), event.get("payee_party_id")]
            if party
        }
    )
    accounts = sorted(
        {
            str(account)
            for event in events
            for account in [event.get("payer_account_id"), event.get("payee_account_id")]
            if account
        }
    )
    party_watchlist = load_party_risk(conn, parties)[:8]
    cluster_watchlist = load_cluster_risk(conn, parties)[:6]

    return {
        "queue_id": queue_id,
        "dataset_id": queue_row["dataset_id"],
        "event_date": queue_row["event_date"],
        "queue_metrics": {
            "queue_rows": int(metrics.get("queue_rows") or 0),
            "avg_queue_score": float(metrics.get("avg_queue_score") or 0.0),
            "max_queue_score": float(metrics.get("max_queue_score") or 0.0),
            "positive_rows": int(metrics.get("positive_rows") or 0),
            "aml_rows": int(metrics.get("aml_rows") or 0),
            "avg_amount": float(metrics.get("avg_amount") or 0.0),
            "amount_sum": float(metrics.get("amount_sum") or 0.0),
        },
        "surface": {
            "top_event_count": len(events),
            "party_count_in_packet": len(parties),
            "account_count_in_packet": len(accounts),
        },
        "top_events": events,
        "party_watchlist": party_watchlist,
        "cluster_watchlist": cluster_watchlist,
        "evidence_notes": build_event_evidence(events),
    }


def build_markdown(casebook: Dict[str, Any]) -> str:
    lines = [
        "# Analyst Casebook",
        "",
        f"Created at (UTC): {casebook['created_at_utc']}",
        f"Queue packet count: {casebook['queue_packet_count']}",
        f"Per dataset cap: {casebook['per_dataset_cap']}",
        f"Events per queue cap: {casebook['events_per_queue']}",
        "",
        "## Queue Packets",
    ]
    for packet in casebook["case_packets"]:
        metrics = packet["queue_metrics"]
        lines.extend(
            [
                f"### {packet['queue_id']}",
                f"- Dataset: {packet['dataset_id']}",
                f"- Queue rows: {metrics['queue_rows']}",
                f"- Avg queue score: {metrics['avg_queue_score']:.3f}",
                f"- Max queue score: {metrics['max_queue_score']:.3f}",
                f"- Fraud labels: {metrics['positive_rows']}",
                f"- AML labels: {metrics['aml_rows']}",
                f"- Packet parties/accounts: {packet['surface']['party_count_in_packet']} / {packet['surface']['account_count_in_packet']}",
                f"- Top clusters in packet: {len(packet['cluster_watchlist'])}",
                "- Evidence:",
            ]
        )
        lines.extend([f"  - {note}" for note in packet["evidence_notes"]])
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    output_root = Path(args.output_root)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        ranked_queues = load_ranked_queues(conn)
        top_queues = select_casebook_queues(ranked_queues, int(args.top_queues), int(args.per_dataset_cap))
        case_packets = [build_case_packet(conn, row, int(args.events_per_queue)) for row in top_queues]

    casebook = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "queue_packet_count": len(case_packets),
        "per_dataset_cap": int(args.per_dataset_cap),
        "events_per_queue": int(args.events_per_queue),
        "selection_strategy": "seed_each_dataset_then_fill_under_cap_then_global_fill",
        "case_packets": case_packets,
    }

    (out_dir / "casebook.json").write_text(json.dumps(casebook, indent=2, ensure_ascii=False), encoding="utf-8")
    (out_dir / "casebook.md").write_text(build_markdown(casebook), encoding="utf-8")

    latest_dir = output_root / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    (latest_dir / "casebook.json").write_text(json.dumps(casebook, indent=2, ensure_ascii=False), encoding="utf-8")
    (latest_dir / "casebook.md").write_text(build_markdown(casebook), encoding="utf-8")

    summary = {
        "created_at_utc": casebook["created_at_utc"],
        "output_dir": str(out_dir),
        "latest_dir": str(latest_dir),
        "queue_packet_count": len(case_packets),
        "per_dataset_cap": int(args.per_dataset_cap),
        "events_per_queue": int(args.events_per_queue),
        "selection_strategy": casebook["selection_strategy"],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

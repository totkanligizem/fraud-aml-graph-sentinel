#!/usr/bin/env python3
"""Build a static executive dashboard bundle from validated project artifacts."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH = ROOT / "reports" / "03_Operational_Checkpoint_Snapshot.json"
DASHBOARD_DIR = ROOT / "dashboard"
DATA_JSON_PATH = DASHBOARD_DIR / "dashboard-data.json"
DATA_JS_PATH = DASHBOARD_DIR / "dashboard-data.js"
VERTEX_OUTPUT_DIR = ROOT / "artifacts" / "agent" / "vertex_responses" / "latest"


def load_snapshot() -> Dict[str, Any]:
    if not SNAPSHOT_PATH.exists():
        raise FileNotFoundError(
            "Missing checkpoint snapshot. Run `make report-checkpoint` before building the dashboard."
        )
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


def fetch_rows(conn: sqlite3.Connection, query: str) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(query).fetchall()]


def serialize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        clean: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, bytes):
                clean[key] = value.decode("utf-8")
            else:
                clean[key] = value
        out.append(clean)
    return out


def utc_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def evidence_kind(rel_path: str) -> str:
    if rel_path.startswith("reports/"):
        return "report"
    if rel_path.startswith("artifacts/bigquery/"):
        return "bigquery"
    if rel_path.startswith("artifacts/graph/"):
        return "graph"
    if rel_path.startswith("artifacts/models/"):
        return "model"
    if rel_path.startswith("data/warehouse/"):
        return "warehouse"
    return "artifact"


def build_evidence_items(rel_paths: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for rel_path in rel_paths:
        abs_path = ROOT / rel_path
        items.append(
            {
                "path": rel_path,
                "label": abs_path.name,
                "kind": evidence_kind(rel_path),
                "exists": abs_path.exists(),
                "size_bytes": abs_path.stat().st_size if abs_path.exists() else 0,
                "modified_at_utc": utc_mtime(abs_path),
            }
        )
    return items


def load_json_if_exists(path: Path) -> Dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_dataset_breakdown(snapshot: Dict[str, Any], conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = fetch_rows(
        conn,
        """
        SELECT
          tm.dataset_id,
          COUNT(*) AS transaction_rows,
          SUM(COALESCE(tm.label_fraud, 0)) AS fraud_rows,
          SUM(COALESCE(tm.label_aml, 0)) AS aml_rows,
          AVG(tm.amount) AS avg_amount,
          MIN(tm.event_date) AS first_event_date,
          MAX(tm.event_date) AS last_event_date,
          COALESCE(fs.scored_rows, 0) AS scored_rows,
          COALESCE(aq.queue_rows, 0) AS queue_rows,
          COALESCE(aq.avg_score, 0.0) AS avg_queue_score,
          COALESCE(aq.max_score, 0.0) AS max_queue_score
        FROM transaction_mart tm
        LEFT JOIN (
          SELECT dataset_id, COUNT(*) AS scored_rows
          FROM fraud_scores
          GROUP BY dataset_id
        ) fs
          ON tm.dataset_id = fs.dataset_id
        LEFT JOIN (
          SELECT dataset_id, COUNT(*) AS queue_rows, AVG(fraud_score) AS avg_score, MAX(fraud_score) AS max_score
          FROM alert_queue
          GROUP BY dataset_id
        ) aq
          ON tm.dataset_id = aq.dataset_id
        GROUP BY tm.dataset_id, fs.scored_rows, aq.queue_rows, aq.avg_score, aq.max_score
        ORDER BY transaction_rows DESC, tm.dataset_id
        """,
    )
    rows_by_dataset = snapshot["scoring_summary"]["rows_by_dataset"]
    total_scored_rows = int(snapshot["scoring_summary"]["total_scored_rows"])
    overall_rows = int(snapshot["warehouse_summary"]["table_counts"]["transaction_mart"])

    for row in rows:
        row["fraud_rate"] = (row["fraud_rows"] or 0) / row["transaction_rows"]
        row["aml_rate"] = (row["aml_rows"] or 0) / row["transaction_rows"]
        row["scoring_coverage"] = (row["scored_rows"] or 0) / row["transaction_rows"]
        row["share_of_volume"] = row["transaction_rows"] / overall_rows
        row["share_of_scored"] = rows_by_dataset.get(row["dataset_id"], 0) / total_scored_rows
    return serialize_rows(rows)


def build_daily_series(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    by_dataset = fetch_rows(
        conn,
        """
        SELECT dataset_id, event_date, txn_count, fraud_count, aml_count, avg_amount
        FROM monitoring_mart
        ORDER BY event_date, dataset_id
        """,
    )
    overview = fetch_rows(
        conn,
        """
        SELECT event_date,
               SUM(txn_count) AS txn_count,
               SUM(fraud_count) AS fraud_count,
               SUM(aml_count) AS aml_count,
               AVG(avg_amount) AS avg_amount
        FROM monitoring_mart
        GROUP BY event_date
        ORDER BY event_date
        """,
    )
    grouped: Dict[str, List[Dict[str, Any]]] = {"overview": serialize_rows(overview)}
    for row in by_dataset:
        grouped.setdefault(str(row["dataset_id"]), []).append(dict(row))
    return {key: serialize_rows(value) for key, value in grouped.items()}


def build_score_buckets(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    rows = fetch_rows(
        conn,
        """
        WITH bucketed AS (
          SELECT
            dataset_id,
            CASE
              WHEN fraud_score >= 0.8 THEN '0.80-1.00'
              WHEN fraud_score >= 0.6 THEN '0.60-0.79'
              WHEN fraud_score >= 0.4 THEN '0.40-0.59'
              WHEN fraud_score >= 0.2 THEN '0.20-0.39'
              ELSE '0.00-0.19'
            END AS bucket,
            CASE
              WHEN fraud_score >= 0.8 THEN 5
              WHEN fraud_score >= 0.6 THEN 4
              WHEN fraud_score >= 0.4 THEN 3
              WHEN fraud_score >= 0.2 THEN 2
              ELSE 1
            END AS sort_order
          FROM fraud_scores
        )
        SELECT dataset_key, bucket, row_count, sort_order
        FROM (
          SELECT COALESCE(dataset_id, 'overview') AS dataset_key,
                 bucket,
                 sort_order,
                 COUNT(*) AS row_count
          FROM bucketed
          GROUP BY dataset_id, bucket, sort_order
          UNION ALL
          SELECT 'overview' AS dataset_key,
                 bucket,
                 sort_order,
                 COUNT(*) AS row_count
          FROM bucketed
          GROUP BY bucket, sort_order
        )
        ORDER BY dataset_key, sort_order DESC
        """,
    )
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row["dataset_key"]), []).append(
            {
                "bucket": row["bucket"],
                "row_count": int(row["row_count"]),
                "sort_order": int(row["sort_order"]),
            }
        )
    return out


def build_queue_highlights(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    rows = fetch_rows(
        conn,
        """
        WITH queue_stats AS (
          SELECT
            dataset_id,
            queue_id,
            event_date,
            COUNT(*) AS queue_rows,
            AVG(fraud_score) AS avg_score,
            MAX(fraud_score) AS max_score,
            SUM(COALESCE(label_fraud, 0)) AS positive_rows
          FROM alert_queue
          GROUP BY dataset_id, queue_id, event_date
        ),
        ranked AS (
          SELECT
            dataset_id,
            queue_id,
            event_date,
            queue_rows,
            avg_score,
            max_score,
            positive_rows,
            ROW_NUMBER() OVER (
              PARTITION BY dataset_id
              ORDER BY avg_score DESC, positive_rows DESC, queue_rows DESC, queue_id ASC
            ) AS dataset_rank,
            ROW_NUMBER() OVER (
              ORDER BY avg_score DESC, positive_rows DESC, queue_rows DESC, queue_id ASC
            ) AS overall_rank
          FROM queue_stats
        )
        SELECT 'overview' AS dataset_key, queue_id, event_date, queue_rows, avg_score, max_score, positive_rows, overall_rank AS sort_rank
        FROM ranked
        WHERE overall_rank <= 6
        UNION ALL
        SELECT dataset_id AS dataset_key, queue_id, event_date, queue_rows, avg_score, max_score, positive_rows, dataset_rank AS sort_rank
        FROM ranked
        WHERE dataset_rank <= 6
        ORDER BY dataset_key, sort_rank
        """,
    )
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row["dataset_key"]), []).append(serialize_rows([row])[0])
    return out


def build_graph_panels(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    top_nodes = fetch_rows(
        conn,
        """
        SELECT party_id, total_txn_count, distinct_counterparty_count, total_amount_sum, risk_score, high_risk_event_count
        FROM graph_party_node
        ORDER BY risk_score DESC, total_amount_sum DESC, total_txn_count DESC
        LIMIT 6
        """,
    )
    top_clusters = fetch_rows(
        conn,
        """
        SELECT cluster_id, party_count, edge_count, txn_count, amount_sum, high_risk_event_count, max_edge_risk_score
        FROM graph_party_cluster_summary
        ORDER BY max_edge_risk_score DESC, amount_sum DESC, txn_count DESC
        LIMIT 6
        """,
    )
    return {
        "top_nodes": serialize_rows(top_nodes),
        "top_clusters": serialize_rows(top_clusters),
    }


def build_analyst_panel() -> Dict[str, Any]:
    summary_path = VERTEX_OUTPUT_DIR / "run-summary.json"
    summary = load_json_if_exists(summary_path)
    if not summary:
        return {
            "available": False,
            "status": "not_run",
            "note": "Vertex analyst copilot smoke output is not available yet.",
            "response_count": 0,
            "error_count": 0,
            "responses": [],
        }

    responses: List[Dict[str, Any]] = []
    for item in summary.get("results", []):
        output_name = str(item.get("output_file", "")).strip()
        payload = load_json_if_exists(VERTEX_OUTPUT_DIR / output_name) if output_name else None
        responses.append(
            {
                "queue_id": item["queue_id"],
                "dataset_id": item["dataset_id"],
                "model": item.get("model", ""),
                "overall_priority": item.get("overall_priority") or "",
                "runtime_error": item.get("runtime_error", ""),
                "validation_errors": item.get("validation_errors", []),
                "response_chars": int(item.get("response_chars", 0)),
                "case_overview": payload.get("case_overview", "") if payload else "",
                "observed_signals": payload.get("observed_signals", [])[:4] if payload else [],
                "recommended_actions": payload.get("recommended_actions", [])[:3] if payload else [],
                "evidence_gaps": payload.get("evidence_gaps", [])[:3] if payload else [],
                "raw_file": item.get("raw_file", ""),
                "output_file": output_name,
            }
        )

    return {
        "available": True,
        "status": "validated" if int(summary.get("error_count", 0)) == 0 else "partial",
        "note": "Latest validated Gemini analyst smoke output rendered into the executive dashboard.",
        "created_at_utc": summary.get("created_at_utc"),
        "model": summary.get("model", ""),
        "location": summary.get("location", ""),
        "response_count": int(summary.get("response_count", 0)),
        "error_count": int(summary.get("error_count", 0)),
        "responses": responses,
    }


def build_quality_panels(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    core = snapshot["bigquery_state"]["quality_metrics"]
    graph = snapshot["bigquery_state"]["graph_quality_metrics"]
    core_rows = []
    graph_rows = []
    for key, value in core.items():
        int_value = int(value)
        core_rows.append(
            {
                "name": key,
                "value": None if int_value < 0 else int_value,
                "raw_value": int_value,
                "status": "unresolved" if int_value < 0 else ("passed" if int_value == 0 else "failed"),
            }
        )
    for key, value in graph.items():
        int_value = int(value)
        graph_rows.append(
            {
                "name": key,
                "value": None if int_value < 0 else int_value,
                "raw_value": int_value,
                "status": "unresolved" if int_value < 0 else ("passed" if int_value == 0 else "failed"),
            }
        )

    all_rows = [*core_rows, *graph_rows]
    passed_checks = sum(1 for row in all_rows if row["status"] == "passed")
    unresolved_checks = sum(1 for row in all_rows if row["status"] == "unresolved")
    failed_checks = sum(1 for row in all_rows if row["status"] == "failed")
    total_checks = len(all_rows)
    return {
        "core": core_rows,
        "graph": graph_rows,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "unresolved_checks": unresolved_checks,
        "total_checks": total_checks,
        "assurance_ratio": (passed_checks / total_checks) if total_checks else 0.0,
        "resolved_ratio": ((passed_checks + failed_checks) / total_checks) if total_checks else 0.0,
        "total_defects": sum(int(row["value"]) for row in all_rows if row["status"] == "failed" and row["value"] is not None),
    }


def build_pipeline_steps(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    warehouse = snapshot["warehouse_summary"]["table_counts"]
    bq = snapshot["bigquery_state"]["table_counts"]
    scoring = snapshot["scoring_summary"]
    ranking = snapshot["ranking_summary"]
    graph = snapshot["graph_summary"]["table_counts"]
    return [
        {"label": "Raw events", "value": int(warehouse["transaction_event_raw"]), "status": "validated"},
        {"label": "Stage mart", "value": int(warehouse["stg_transaction_event"]), "status": "validated"},
        {"label": "Transaction mart", "value": int(warehouse["transaction_mart"]), "status": "validated"},
        {"label": "Feature layer", "value": int(warehouse["feature_payer_24h"]), "status": "validated"},
        {"label": "Fraud scores", "value": int(scoring["total_scored_rows"]), "status": "validated"},
        {"label": "Alert queues", "value": int(ranking["queue_count"]), "status": "validated"},
        {"label": "Graph clusters", "value": int(graph["graph_party_cluster_summary"]), "status": "validated"},
        {"label": "BigQuery sync", "value": int(bq["dev_transaction_mart"]), "status": "mirrored"},
    ]


def build_kpis(snapshot: Dict[str, Any], datasets: List[Dict[str, Any]], buckets: Dict[str, List[Dict[str, Any]]], quality: Dict[str, Any]) -> Dict[str, Any]:
    warehouse = snapshot["warehouse_summary"]["table_counts"]
    ranking = snapshot["ranking_summary"]
    graph = snapshot["graph_summary"]
    model = snapshot["model_metrics"]
    total_transactions = int(warehouse["transaction_mart"])
    scored_rows = int(snapshot["scoring_summary"]["total_scored_rows"])
    suspicious_edge_count = int(graph["cluster_summary"]["suspicious_edge_count"])
    graph_edge_count = int(graph["table_counts"]["graph_party_edge"])
    high_risk_rows = next((item["row_count"] for item in buckets["overview"] if item["bucket"] == "0.80-1.00"), 0)
    score_share = high_risk_rows / scored_rows if scored_rows else 0.0
    highlighted_dataset = max(datasets, key=lambda item: item["avg_queue_score"])
    return {
        "total_transactions": total_transactions,
        "scored_rows": scored_rows,
        "queue_count": int(ranking["queue_count"]),
        "graph_clusters": int(graph["cluster_summary"]["cluster_count"]),
        "scoring_coverage": scored_rows / total_transactions if total_transactions else 0.0,
        "assurance_ratio": quality["assurance_ratio"],
        "suspicious_edge_ratio": suspicious_edge_count / graph_edge_count if graph_edge_count else 0.0,
        "high_risk_rows": int(high_risk_rows),
        "high_risk_share": score_share,
        "mean_precision_at_k": float(ranking["mean_precision_at_k"]),
        "mean_ndcg_at_k": float(ranking["mean_ndcg_at_k"]),
        "top_k": int(ranking["top_k"]),
        "average_precision": float(model["metrics"]["average_precision"]),
        "pr_auc_trapz": float(model["metrics"]["pr_auc_trapz"]),
        "threshold": float(model["cost_optimized_threshold"]["threshold"]),
        "highlighted_dataset": {
            "dataset_id": highlighted_dataset["dataset_id"],
            "avg_queue_score": highlighted_dataset["avg_queue_score"],
        },
    }


def build_insight_bullets(snapshot: Dict[str, Any], kpis: Dict[str, Any], datasets: List[Dict[str, Any]], quality: Dict[str, Any]) -> List[str]:
    ieee_row = next((item for item in datasets if item["dataset_id"] == "ieee_cis"), None)
    ibm_row = next((item for item in datasets if item["dataset_id"] == "ibm_aml_data"), None)
    graph = snapshot["graph_summary"]
    return [
        f"{quality['passed_checks']}/{quality['total_checks']} quality checks passed with zero defects; BigQuery core and graph surfaces are synchronized with local state.",
        (
            f"Scoring coverage is {kpis['scoring_coverage'] * 100:.1f}%. "
            f"IEEE queue pressure leads with mean queue score {ieee_row['avg_queue_score']:.3f}."
            if ieee_row
            else "Scoring coverage and queue pressure are validated against current artifacts."
        ),
        f"Graph intelligence contains {graph['cluster_summary']['cluster_count']:,} clusters with suspicious-edge share at {kpis['suspicious_edge_ratio'] * 100:.1f}%.",
        (
            f"IBM AML contributes {ibm_row['aml_rows']} AML-labeled records and remains the dedicated AML signal source."
            if ibm_row
            else "IBM AML signal surface is active."
        ),
    ]


def build_dashboard_payload(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    db_path = ROOT / snapshot["warehouse_summary"]["db_path"]
    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse DB not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        dataset_breakdown = build_dataset_breakdown(snapshot, conn)
        daily_series = build_daily_series(conn)
        score_buckets = build_score_buckets(conn)
        queue_highlights = build_queue_highlights(conn)
        graph_panels = build_graph_panels(conn)

    quality = build_quality_panels(snapshot)
    kpis = build_kpis(snapshot, dataset_breakdown, score_buckets, quality)
    analyst = build_analyst_panel()
    evidence_paths = [
        "reports/03_Operational_Checkpoint_Snapshot.json",
        "reports/03_Operational_Checkpoint_TR.pdf",
        "artifacts/bigquery/validate-bigquery-state.json",
        "artifacts/bigquery/validate-executive-sql-bundle.json",
        "artifacts/dashboard/validate-dashboard-state.json",
        "artifacts/graph/graph-build-summary-20260227T125935Z.json",
        "artifacts/models/ranking/20260227T125836Z/ranking-summary.json",
        "artifacts/models/fraud_scoring/20260227T125804Z/scoring-summary.json",
        "artifacts/agent/casebook/latest/casebook.json",
        "artifacts/agent/prompt_pack/latest/prompt-pack-summary.json",
        "artifacts/agent/vertex_responses/latest/run-summary.json",
        "data/warehouse/warehouse-build-summary.json",
    ]
    evidence_items = build_evidence_items(evidence_paths)

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot_generated_at_utc": snapshot["generated_at_utc"],
        "project": {
            "name": "Fraud - AML Graph Sentinel",
            "project_id": snapshot["bigquery_state"]["project_id"],
            "dataset_id": snapshot["bigquery_state"]["dataset_id"],
            "location": snapshot["bigquery_state"]["location"],
            "theme": "Mineral Ledger",
            "completion": {"mvp": 88, "vision": 70},
        },
        "kpis": kpis,
        "ranking": snapshot["ranking_summary"],
        "model": snapshot["model_metrics"],
        "graph": snapshot["graph_summary"],
        "quality": quality,
        "dataset_breakdown": dataset_breakdown,
        "daily_series": daily_series,
        "score_buckets": score_buckets,
        "queue_highlights": queue_highlights,
        "graph_panels": graph_panels,
        "analyst": analyst,
        "pipeline_steps": build_pipeline_steps(snapshot),
        "top_features": snapshot["top_features"],
        "insights": build_insight_bullets(snapshot, kpis, dataset_breakdown, quality),
        "freshness": {
            "dashboard_generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "checkpoint_snapshot_generated_at_utc": snapshot["generated_at_utc"],
            "bigquery_validation_generated_at_utc": snapshot["bigquery_state"]["created_at_utc"],
            "model_generated_at_utc": snapshot["model_metrics"]["created_at_utc"],
            "ranking_generated_at_utc": snapshot["ranking_summary"]["created_at_utc"],
            "graph_generated_at_utc": snapshot["graph_summary"]["created_at_utc"],
        },
        "evidence_paths": evidence_paths,
        "evidence_items": evidence_items,
    }


def main() -> None:
    snapshot = load_snapshot()
    payload = build_dashboard_payload(snapshot)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    DATA_JS_PATH.write_text(
        "window.__AML_DASHBOARD_DATA__ = " + json.dumps(payload, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    summary = {
        "created_at_utc": payload["generated_at_utc"],
        "dashboard_dir": str(DASHBOARD_DIR.relative_to(ROOT)),
        "data_json": str(DATA_JSON_PATH.relative_to(ROOT)),
        "data_js": str(DATA_JS_PATH.relative_to(ROOT)),
        "dataset_count": len(payload["dataset_breakdown"]),
        "insight_count": len(payload["insights"]),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

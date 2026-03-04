#!/usr/bin/env python3
"""
Validate core BigQuery dev tables and basic data quality checks.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from google.cloud import bigquery
from google.oauth2 import service_account

REQUIRED_MIN_ROWS = {
    "dev_transaction_mart": 300000,
    "dev_feature_payer_24h": 50000,
    "dev_monitoring_mart": 1,
    "dev_fraud_scores": 300000,
    "dev_alert_queue": 300000,
    "dev_alert_queue_top50": 1,
    "dev_risk_dashboard_daily": 1,
}

GRAPH_REQUIRED_MIN_ROWS = {
    "dev_graph_party_node": 1000,
    "dev_graph_party_edge": 1000,
    "dev_graph_account_node": 1000,
    "dev_graph_account_edge": 1000,
    "dev_graph_party_cluster_membership": 1,
    "dev_graph_party_cluster_summary": 1,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate BigQuery table state.")
    parser.add_argument(
        "--require-graph",
        action="store_true",
        help="Require graph tables and graph quality checks",
    )
    return parser.parse_args()


def load_env_file(env_path: Path = Path(".env.local")) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ[key] = value


def query_scalar(client: bigquery.Client, query: str) -> int:
    rows = list(client.query(query).result())
    if not rows:
        return 0
    first = rows[0]
    value = list(dict(first.items()).values())[0]
    return int(value or 0)


def main() -> None:
    args = parse_args()
    load_env_file()

    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    bq_dataset = os.environ.get("BQ_DATASET", "").strip()
    bq_location = os.environ.get("BQ_LOCATION", "EU").strip()

    if not project_id:
        raise RuntimeError("Missing env: GCP_PROJECT_ID")
    if not bq_dataset:
        raise RuntimeError("Missing env: BQ_DATASET")
    if not key_path:
        raise RuntimeError("Missing env: GOOGLE_APPLICATION_CREDENTIALS")
    if not Path(key_path).exists():
        raise FileNotFoundError(f"Credential file not found: {key_path}")

    creds = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project_id, credentials=creds, location=bq_location)
    full_dataset = f"{project_id}.{bq_dataset}"

    errors: List[str] = []
    table_counts: Dict[str, int] = {}
    required_tables = dict(REQUIRED_MIN_ROWS)
    if args.require_graph:
        required_tables.update(GRAPH_REQUIRED_MIN_ROWS)

    for table_name, min_rows in required_tables.items():
        count_q = f"SELECT COUNT(*) AS c FROM `{full_dataset}.{table_name}`"
        try:
            row_count = query_scalar(client, count_q)
        except Exception as exc:  # noqa: BLE001
            row_count = 0
            errors.append(f"{table_name}: query_failed ({exc})")
        table_counts[table_name] = row_count
        if row_count < min_rows:
            errors.append(f"{table_name}: row_count {row_count} < min_required {min_rows}")

    metric_queries = {
        "null_event_id_transaction_mart": f"""
            SELECT COUNT(*) AS c
            FROM `{full_dataset}.dev_transaction_mart`
            WHERE event_id IS NULL OR event_id = ''
        """,
        "null_source_event_id_transaction_mart": f"""
            SELECT COUNT(*) AS c
            FROM `{full_dataset}.dev_transaction_mart`
            WHERE source_event_id IS NULL OR source_event_id = ''
        """,
        "duplicate_event_id_transaction_mart": f"""
            SELECT COUNT(*) AS c
            FROM (
              SELECT event_id, COUNT(*) AS n
              FROM `{full_dataset}.dev_transaction_mart`
              GROUP BY event_id
              HAVING COUNT(*) > 1
            )
        """,
        "invalid_fraud_score_range": f"""
            SELECT COUNT(*) AS c
            FROM `{full_dataset}.dev_fraud_scores`
            WHERE fraud_score IS NULL OR fraud_score < 0 OR fraud_score > 1
        """,
        "invalid_alert_queue_rank": f"""
            SELECT COUNT(*) AS c
            FROM `{full_dataset}.dev_alert_queue`
            WHERE rank_in_queue IS NULL OR rank_in_queue < 1
        """,
        "missing_alert_queue_id": f"""
            SELECT COUNT(*) AS c
            FROM `{full_dataset}.dev_alert_queue`
            WHERE queue_id IS NULL OR queue_id = ''
        """,
        "invalid_label_aml_transaction_mart": f"""
            SELECT COUNT(*) AS c
            FROM `{full_dataset}.dev_transaction_mart`
            WHERE label_aml IS NOT NULL AND label_aml NOT IN (0, 1)
        """,
    }
    quality_metrics: Dict[str, int] = {}
    for name, query in metric_queries.items():
        try:
            val = query_scalar(client, query)
        except Exception as exc:  # noqa: BLE001
            val = -1
            errors.append(f"{name}: query_failed ({exc})")
        quality_metrics[name] = val
        if val > 0:
            errors.append(f"{name}: {val}")

    graph_quality_metrics: Dict[str, int] = {}
    if args.require_graph:
        graph_metric_queries = {
            "null_graph_party_node_id": f"""
                SELECT COUNT(*) AS c
                FROM `{full_dataset}.dev_graph_party_node`
                WHERE party_id IS NULL OR party_id = ''
            """,
            "duplicate_graph_party_node_id": f"""
                SELECT COUNT(*) AS c
                FROM (
                  SELECT party_id, COUNT(*) AS n
                  FROM `{full_dataset}.dev_graph_party_node`
                  GROUP BY party_id
                  HAVING COUNT(*) > 1
                )
            """,
            "invalid_graph_party_edge_risk_score": f"""
                SELECT COUNT(*) AS c
                FROM `{full_dataset}.dev_graph_party_edge`
                WHERE risk_score IS NULL OR risk_score < 0 OR risk_score > 1
            """,
            "cluster_membership_without_summary": f"""
                SELECT COUNT(*) AS c
                FROM `{full_dataset}.dev_graph_party_cluster_membership` m
                LEFT JOIN `{full_dataset}.dev_graph_party_cluster_summary` s
                  ON m.cluster_id = s.cluster_id
                WHERE s.cluster_id IS NULL
            """,
            "shared_party_account_node_ids": f"""
                SELECT COUNT(*) AS c
                FROM `{full_dataset}.dev_graph_party_node` p
                INNER JOIN `{full_dataset}.dev_graph_account_node` a
                  ON p.party_id = a.account_id
            """,
            "shared_party_account_edge_pairs": f"""
                SELECT COUNT(*) AS c
                FROM `{full_dataset}.dev_graph_party_edge` p
                INNER JOIN `{full_dataset}.dev_graph_account_edge` a
                  ON p.src_party_id = a.src_account_id
                 AND p.dst_party_id = a.dst_account_id
            """,
        }
        for name, query in graph_metric_queries.items():
            try:
                val = query_scalar(client, query)
            except Exception as exc:  # noqa: BLE001
                val = -1
                errors.append(f"{name}: query_failed ({exc})")
            graph_quality_metrics[name] = val
            if val > 0:
                errors.append(f"{name}: {val}")

    fraud_by_dataset_q = f"""
        SELECT dataset_id, COUNT(*) AS c
        FROM `{full_dataset}.dev_fraud_scores`
        GROUP BY dataset_id
        ORDER BY dataset_id
    """
    fraud_scores_rows_by_dataset: Dict[str, int] = {}
    try:
        for row in client.query(fraud_by_dataset_q).result():
            fraud_scores_rows_by_dataset[str(row["dataset_id"])] = int(row["c"])
    except Exception as exc:  # noqa: BLE001
        errors.append(f"fraud_scores_rows_by_dataset: query_failed ({exc})")

    queue_count_q = f"SELECT COUNT(DISTINCT queue_id) AS c FROM `{full_dataset}.dev_alert_queue`"
    try:
        queue_count = query_scalar(client, queue_count_q)
        if queue_count < 1:
            errors.append(f"distinct_queue_id_count too low: {queue_count}")
    except Exception as exc:  # noqa: BLE001
        queue_count = 0
        errors.append(f"distinct_queue_id_count: query_failed ({exc})")

    out = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "project_id": project_id,
        "dataset_id": bq_dataset,
        "location": bq_location,
        "require_graph": args.require_graph,
        "ok": len(errors) == 0,
        "table_counts": table_counts,
        "quality_metrics": quality_metrics,
        "graph_quality_metrics": graph_quality_metrics,
        "fraud_scores_rows_by_dataset": fraud_scores_rows_by_dataset,
        "distinct_queue_id_count": queue_count,
        "errors": errors,
    }

    out_dir = Path("artifacts/bigquery")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "validate-bigquery-state.json"
    out_file.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))

    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

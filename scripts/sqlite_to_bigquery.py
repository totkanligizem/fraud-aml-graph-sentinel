#!/usr/bin/env python3
"""
Upload SQLite tables to BigQuery in chunks (JSON load jobs).

Designed for controlled dev uploads and avoids pyarrow dependency.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery
from google.oauth2 import service_account


DEFAULT_TABLES = [
    "transaction_mart",
    "feature_payer_24h",
    "monitoring_mart",
    "fraud_scores",
    "alert_queue",
]

TABLE_SCHEMAS: Dict[str, List[bigquery.SchemaField]] = {
    "transaction_mart": [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("source_event_id", "STRING"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("event_time_grain", "STRING"),
        bigquery.SchemaField("time_step", "INT64"),
        bigquery.SchemaField("payer_party_id", "STRING"),
        bigquery.SchemaField("payee_party_id", "STRING"),
        bigquery.SchemaField("payer_account_id", "STRING"),
        bigquery.SchemaField("payee_account_id", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("txn_type", "STRING"),
        bigquery.SchemaField("mcc_category", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("fx_rate_to_usd", "FLOAT64"),
        bigquery.SchemaField("device_id", "STRING"),
        bigquery.SchemaField("ip_prefix", "STRING"),
        bigquery.SchemaField("email_domain", "STRING"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("label_fraud", "INT64"),
        bigquery.SchemaField("label_aml", "INT64"),
        bigquery.SchemaField("label_source", "STRING"),
        bigquery.SchemaField("pii_class", "STRING"),
        bigquery.SchemaField("consent_class", "STRING"),
        bigquery.SchemaField("retention_class", "STRING"),
        bigquery.SchemaField("raw_partition", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("adapter_version", "STRING"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("event_hour", "TIMESTAMP"),
    ],
    "feature_payer_24h": [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("payer_party_id", "STRING"),
        bigquery.SchemaField("payer_txn_count_24h", "INT64"),
        bigquery.SchemaField("payer_amt_sum_24h", "FLOAT64"),
    ],
    "monitoring_mart": [
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("txn_count", "INT64"),
        bigquery.SchemaField("avg_amount", "FLOAT64"),
        bigquery.SchemaField("fraud_count", "INT64"),
        bigquery.SchemaField("aml_count", "INT64"),
    ],
    "fraud_scores": [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("fraud_score", "FLOAT64"),
        bigquery.SchemaField("label_fraud", "INT64"),
    ],
    "alert_queue": [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("fraud_score", "FLOAT64"),
        bigquery.SchemaField("label_fraud", "INT64"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("queue_id", "STRING"),
        bigquery.SchemaField("rank_in_queue", "INT64"),
    ],
    "graph_party_node": [
        bigquery.SchemaField("party_id", "STRING"),
        bigquery.SchemaField("first_seen", "TIMESTAMP"),
        bigquery.SchemaField("last_seen", "TIMESTAMP"),
        bigquery.SchemaField("out_txn_count", "INT64"),
        bigquery.SchemaField("in_txn_count", "INT64"),
        bigquery.SchemaField("total_txn_count", "INT64"),
        bigquery.SchemaField("out_amount_sum", "FLOAT64"),
        bigquery.SchemaField("in_amount_sum", "FLOAT64"),
        bigquery.SchemaField("total_amount_sum", "FLOAT64"),
        bigquery.SchemaField("distinct_counterparty_count", "INT64"),
        bigquery.SchemaField("dataset_count", "INT64"),
        bigquery.SchemaField("fraud_event_count", "INT64"),
        bigquery.SchemaField("aml_event_count", "INT64"),
        bigquery.SchemaField("alert_event_count", "INT64"),
        bigquery.SchemaField("high_risk_event_count", "INT64"),
        bigquery.SchemaField("max_fraud_score", "FLOAT64"),
        bigquery.SchemaField("risk_score", "FLOAT64"),
    ],
    "graph_party_edge": [
        bigquery.SchemaField("src_party_id", "STRING"),
        bigquery.SchemaField("dst_party_id", "STRING"),
        bigquery.SchemaField("first_seen", "TIMESTAMP"),
        bigquery.SchemaField("last_seen", "TIMESTAMP"),
        bigquery.SchemaField("txn_count", "INT64"),
        bigquery.SchemaField("amount_sum", "FLOAT64"),
        bigquery.SchemaField("avg_amount", "FLOAT64"),
        bigquery.SchemaField("dataset_count", "INT64"),
        bigquery.SchemaField("fraud_event_count", "INT64"),
        bigquery.SchemaField("aml_event_count", "INT64"),
        bigquery.SchemaField("alert_event_count", "INT64"),
        bigquery.SchemaField("high_risk_event_count", "INT64"),
        bigquery.SchemaField("max_fraud_score", "FLOAT64"),
        bigquery.SchemaField("risk_score", "FLOAT64"),
    ],
    "graph_account_node": [
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("first_seen", "TIMESTAMP"),
        bigquery.SchemaField("last_seen", "TIMESTAMP"),
        bigquery.SchemaField("out_txn_count", "INT64"),
        bigquery.SchemaField("in_txn_count", "INT64"),
        bigquery.SchemaField("total_txn_count", "INT64"),
        bigquery.SchemaField("out_amount_sum", "FLOAT64"),
        bigquery.SchemaField("in_amount_sum", "FLOAT64"),
        bigquery.SchemaField("total_amount_sum", "FLOAT64"),
        bigquery.SchemaField("distinct_counterparty_count", "INT64"),
        bigquery.SchemaField("dataset_count", "INT64"),
        bigquery.SchemaField("fraud_event_count", "INT64"),
        bigquery.SchemaField("aml_event_count", "INT64"),
        bigquery.SchemaField("alert_event_count", "INT64"),
        bigquery.SchemaField("high_risk_event_count", "INT64"),
        bigquery.SchemaField("max_fraud_score", "FLOAT64"),
        bigquery.SchemaField("risk_score", "FLOAT64"),
    ],
    "graph_account_edge": [
        bigquery.SchemaField("src_account_id", "STRING"),
        bigquery.SchemaField("dst_account_id", "STRING"),
        bigquery.SchemaField("first_seen", "TIMESTAMP"),
        bigquery.SchemaField("last_seen", "TIMESTAMP"),
        bigquery.SchemaField("txn_count", "INT64"),
        bigquery.SchemaField("amount_sum", "FLOAT64"),
        bigquery.SchemaField("avg_amount", "FLOAT64"),
        bigquery.SchemaField("dataset_count", "INT64"),
        bigquery.SchemaField("fraud_event_count", "INT64"),
        bigquery.SchemaField("aml_event_count", "INT64"),
        bigquery.SchemaField("alert_event_count", "INT64"),
        bigquery.SchemaField("high_risk_event_count", "INT64"),
        bigquery.SchemaField("max_fraud_score", "FLOAT64"),
        bigquery.SchemaField("risk_score", "FLOAT64"),
    ],
    "graph_party_cluster_membership": [
        bigquery.SchemaField("cluster_id", "STRING"),
        bigquery.SchemaField("party_id", "STRING"),
    ],
    "graph_party_cluster_summary": [
        bigquery.SchemaField("cluster_id", "STRING"),
        bigquery.SchemaField("party_count", "INT64"),
        bigquery.SchemaField("edge_count", "INT64"),
        bigquery.SchemaField("txn_count", "INT64"),
        bigquery.SchemaField("amount_sum", "FLOAT64"),
        bigquery.SchemaField("fraud_event_count", "INT64"),
        bigquery.SchemaField("aml_event_count", "INT64"),
        bigquery.SchemaField("alert_event_count", "INT64"),
        bigquery.SchemaField("high_risk_event_count", "INT64"),
        bigquery.SchemaField("max_fraud_score", "FLOAT64"),
        bigquery.SchemaField("mean_edge_risk_score", "FLOAT64"),
        bigquery.SchemaField("max_edge_risk_score", "FLOAT64"),
        bigquery.SchemaField("first_seen", "TIMESTAMP"),
        bigquery.SchemaField("last_seen", "TIMESTAMP"),
    ],
}

INT_COLUMNS = {
    "time_step",
    "label_fraud",
    "label_aml",
    "payer_txn_count_24h",
    "txn_count",
    "fraud_count",
    "aml_count",
    "rank_in_queue",
    "out_txn_count",
    "in_txn_count",
    "total_txn_count",
    "distinct_counterparty_count",
    "dataset_count",
    "party_count",
    "fraud_event_count",
    "aml_event_count",
    "alert_event_count",
    "high_risk_event_count",
    "edge_count",
}

FLOAT_COLUMNS = {
    "amount",
    "fx_rate_to_usd",
    "payer_amt_sum_24h",
    "avg_amount",
    "fraud_score",
    "out_amount_sum",
    "in_amount_sum",
    "total_amount_sum",
    "amount_sum",
    "max_fraud_score",
    "risk_score",
    "mean_edge_risk_score",
    "max_edge_risk_score",
}

TIMESTAMP_COLUMNS = {
    "event_time",
    "event_hour",
    "ingested_at",
    "first_seen",
    "last_seen",
}

DATE_COLUMNS = {
    "event_date",
}

STRING_COLUMNS = {
    "source_event_id",
}


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload selected SQLite tables to BigQuery.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        help="Tables to upload",
    )
    parser.add_argument("--chunksize", type=int, default=25000, help="Rows per upload chunk")
    parser.add_argument(
        "--max-rows-per-table",
        type=int,
        default=200000,
        help="Cap rows per table (set -1 for no cap)",
    )
    parser.add_argument(
        "--table-prefix",
        default="dev_",
        help="Prefix for BigQuery table names (default: dev_)",
    )
    return parser.parse_args()


def get_client() -> tuple[bigquery.Client, str, str]:
    load_env_file()
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    bq_dataset = os.environ.get("BQ_DATASET", "fraud_aml_graph_dev").strip()
    bq_location = os.environ.get("BQ_LOCATION", "EU").strip()

    if not project_id:
        raise RuntimeError("Missing env: GCP_PROJECT_ID")
    if not key_path:
        raise RuntimeError("Missing env: GOOGLE_APPLICATION_CREDENTIALS")
    if not Path(key_path).exists():
        raise RuntimeError(f"Credential file not found: {key_path}")

    creds = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project_id, credentials=creds, location=bq_location)
    ds = bigquery.Dataset(f"{project_id}.{bq_dataset}")
    ds.location = bq_location
    try:
        client.create_dataset(ds, exists_ok=True)
    except Forbidden as exc:
        raise RuntimeError(
            "BigQuery dataset access denied. Grant service account "
            "`roles/bigquery.user` on project or pre-create dataset and grant "
            "`roles/bigquery.dataEditor`."
        ) from exc
    return client, project_id, bq_dataset


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.replace({np.nan: None})
    for col in INT_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")
    for col in FLOAT_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in TIMESTAMP_COLUMNS:
        if col in out.columns:
            ts = pd.to_datetime(out[col], errors="coerce")
            out[col] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    for col in DATE_COLUMNS:
        if col in out.columns:
            dt = pd.to_datetime(out[col], errors="coerce")
            out[col] = dt.dt.strftime("%Y-%m-%d")
    for col in STRING_COLUMNS:
        if col in out.columns:
            out[col] = out[col].astype("string")
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype("string")
    out = out.astype(object).where(pd.notna(out), None)
    return out


def iter_sqlite_chunks(
    conn: sqlite3.Connection,
    table: str,
    chunksize: int,
    max_rows: int | None,
) -> Iterable[pd.DataFrame]:
    limit_clause = ""
    if max_rows is not None and max_rows > 0:
        limit_clause = f" LIMIT {int(max_rows)}"
    query = f"SELECT * FROM {table}{limit_clause}"
    yield from pd.read_sql_query(query, conn, chunksize=chunksize)


def build_job_config(table: str, first_chunk: bool) -> bigquery.LoadJobConfig:
    schema = TABLE_SCHEMAS.get(table)
    return bigquery.LoadJobConfig(
        schema=schema,
        autodetect=schema is None,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if first_chunk
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
        ignore_unknown_values=True,
    )


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    client, project_id, dataset_id = get_client()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    existing = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    summary: Dict[str, Dict[str, int]] = {}
    for table in args.tables:
        if table not in existing:
            print(f"[SKIP] missing table in sqlite: {table}")
            continue

        bq_table = f"{project_id}.{dataset_id}.{args.table_prefix}{table}"
        loaded_rows = 0
        first = True
        print(f"[INFO] uploading table={table} -> {bq_table}")

        for chunk in iter_sqlite_chunks(
            conn,
            table=table,
            chunksize=args.chunksize,
            max_rows=None if args.max_rows_per_table < 0 else args.max_rows_per_table,
        ):
            if chunk.empty:
                continue
            norm = normalize_df(chunk)
            records = norm.to_dict(orient="records")
            job_config = build_job_config(table=table, first_chunk=first)
            try:
                load_job = client.load_table_from_json(records, bq_table, job_config=job_config)
                load_job.result()
            except Forbidden as exc:
                raise RuntimeError(
                    "BigQuery load denied. Grant service account "
                    "`roles/bigquery.jobUser` on project and "
                    "`roles/bigquery.dataEditor` on dataset/project."
                ) from exc
            first = False
            loaded_rows += len(records)
            print(f"[LOAD] table={table} rows={loaded_rows}")

        summary[table] = {"rows_loaded": loaded_rows}
        print(f"[DONE] table={table} rows_loaded={loaded_rows}")

    conn.close()

    out = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_prefix": args.table_prefix,
        "summary": summary,
    }
    out_dir = Path("artifacts/bigquery")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"upload-summary-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out_file.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"[DONE] summary: {out_file}")


if __name__ == "__main__":
    main()

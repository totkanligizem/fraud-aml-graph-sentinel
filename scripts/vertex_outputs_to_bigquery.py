#!/usr/bin/env python3
"""Upload latest validated Vertex analyst outputs to BigQuery."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from google.api_core.exceptions import Forbidden
from google.cloud import bigquery
from google.oauth2 import service_account


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "agent" / "vertex_responses" / "latest"
DEFAULT_TABLE_NAME = "analyst_case_summary"

TABLE_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("created_at_utc", "TIMESTAMP"),
    bigquery.SchemaField("project_id", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("model", "STRING"),
    bigquery.SchemaField("queue_id", "STRING"),
    bigquery.SchemaField("dataset_id", "STRING"),
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("overall_priority", "STRING"),
    bigquery.SchemaField("fraud_risk", "STRING"),
    bigquery.SchemaField("aml_risk", "STRING"),
    bigquery.SchemaField("network_risk", "STRING"),
    bigquery.SchemaField("response_chars", "INT64"),
    bigquery.SchemaField("observed_signal_count", "INT64"),
    bigquery.SchemaField("hypothesis_count", "INT64"),
    bigquery.SchemaField("action_count", "INT64"),
    bigquery.SchemaField("case_overview", "STRING"),
    bigquery.SchemaField("observed_signals", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "investigation_hypotheses",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("hypothesis", "STRING"),
            bigquery.SchemaField("confidence", "STRING"),
        ],
    ),
    bigquery.SchemaField("recommended_actions", "STRING", mode="REPEATED"),
    bigquery.SchemaField("evidence_gaps", "STRING", mode="REPEATED"),
    bigquery.SchemaField("output_file", "STRING"),
    bigquery.SchemaField("raw_file", "STRING"),
    bigquery.SchemaField("response_json", "STRING"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload latest validated Vertex analyst outputs to BigQuery.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Vertex response directory")
    parser.add_argument("--table-prefix", default="dev_", help="BigQuery table prefix")
    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME, help="BigQuery table base name")
    parser.add_argument("--credentials-path", default="", help="Credential JSON path override")
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


def resolve_credentials_path(explicit: str) -> Path:
    candidates = [
        explicit,
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        str(ROOT / "api keys" / "fraud-aml-graph.json"),
        str(ROOT / ".secrets" / "gcp-service-account.json"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return Path(candidate)
    raise FileNotFoundError("No usable BigQuery credentials path found.")


def get_client(credentials_path: Path) -> tuple[bigquery.Client, str, str]:
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    bq_dataset = os.environ.get("BQ_DATASET", "").strip()
    bq_location = os.environ.get("BQ_LOCATION", "EU").strip()
    if not project_id:
        raise RuntimeError("Missing env: GCP_PROJECT_ID")
    if not bq_dataset:
        raise RuntimeError("Missing env: BQ_DATASET")

    creds = service_account.Credentials.from_service_account_file(str(credentials_path))
    client = bigquery.Client(project=project_id, credentials=creds, location=bq_location)
    dataset = bigquery.Dataset(f"{project_id}.{bq_dataset}")
    dataset.location = bq_location
    try:
        client.get_dataset(dataset)
    except Forbidden as exc:
        raise RuntimeError(
            "BigQuery dataset access denied. Grant service account "
            "`roles/bigquery.jobUser` on project and `roles/bigquery.dataEditor` on dataset/project."
        ) from exc
    return client, project_id, bq_dataset


def load_summary(output_dir: Path) -> Dict[str, Any]:
    summary_path = output_dir / "run-summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing run summary: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("error_count", 0)) != 0:
        raise RuntimeError("Refusing to upload analyst outputs with runtime or validation errors.")
    if int(summary.get("response_count", 0)) <= 0:
        raise RuntimeError("No analyst outputs found to upload.")
    return summary


def derive_event_date(queue_id: str) -> str | None:
    parts = str(queue_id).split("|", 1)
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return None


def build_rows(output_dir: Path, summary: Dict[str, Any], project_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in summary.get("results", []):
        output_name = str(item.get("output_file", "")).strip()
        if not output_name:
            raise RuntimeError(f"Missing parsed output_file for queue {item.get('queue_id')}")
        payload_path = output_dir / output_name
        if not payload_path.exists():
            raise FileNotFoundError(f"Missing parsed payload: {payload_path}")
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        risk = payload.get("risk_assessment", {})
        rows.append(
            {
                "run_id": summary.get("run_id", ""),
                "created_at_utc": summary["created_at_utc"],
                "project_id": project_id,
                "location": summary.get("location", ""),
                "model": item.get("model") or summary.get("model", ""),
                "queue_id": item["queue_id"],
                "dataset_id": item["dataset_id"],
                "event_date": derive_event_date(str(item["queue_id"])),
                "overall_priority": risk.get("overall_priority") or item.get("overall_priority", ""),
                "fraud_risk": risk.get("fraud_risk", ""),
                "aml_risk": risk.get("aml_risk", ""),
                "network_risk": risk.get("network_risk", ""),
                "response_chars": int(item.get("response_chars", 0)),
                "observed_signal_count": int(item.get("observed_signal_count", 0)),
                "hypothesis_count": int(item.get("hypothesis_count", 0)),
                "action_count": int(item.get("action_count", 0)),
                "case_overview": payload.get("case_overview", ""),
                "observed_signals": payload.get("observed_signals", []),
                "investigation_hypotheses": payload.get("investigation_hypotheses", []),
                "recommended_actions": payload.get("recommended_actions", []),
                "evidence_gaps": payload.get("evidence_gaps", []),
                "output_file": output_name,
                "raw_file": item.get("raw_file", ""),
                "response_json": json.dumps(payload, ensure_ascii=False),
            }
        )
    return rows


def build_job_config() -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )


def main() -> None:
    load_env_file()
    args = parse_args()

    output_dir = Path(args.output_dir)
    summary = load_summary(output_dir)
    credentials_path = resolve_credentials_path(args.credentials_path)
    client, project_id, bq_dataset = get_client(credentials_path)
    rows = build_rows(output_dir, summary, project_id)

    table_id = f"{project_id}.{bq_dataset}.{args.table_prefix}{args.table_name}"
    job = client.load_table_from_json(rows, table_id, job_config=build_job_config())
    try:
        job.result()
    except Forbidden as exc:
        raise RuntimeError(
            "BigQuery load denied. Grant service account "
            "`roles/bigquery.jobUser` on project and `roles/bigquery.dataEditor` on dataset/project."
        ) from exc

    out_dir = ROOT / "artifacts" / "bigquery"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"analyst-upload-summary-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    report = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "project_id": project_id,
        "dataset_id": bq_dataset,
        "table_id": table_id,
        "source_output_dir": str(output_dir),
        "response_count": len(rows),
        "model": summary.get("model", ""),
        "location": summary.get("location", ""),
        "run_id": summary.get("run_id", ""),
        "credentials_path": str(credentials_path),
    }
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"summary_file": str(out_file), **report}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

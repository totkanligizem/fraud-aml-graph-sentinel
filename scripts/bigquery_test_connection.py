#!/usr/bin/env python3
"""
Smoke test for BigQuery access using service account credentials.
"""

from __future__ import annotations

import os
from pathlib import Path

from google.api_core.exceptions import Forbidden
from google.cloud import bigquery
from google.oauth2 import service_account


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


def main() -> None:
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

    try:
        rows = list(client.query("SELECT 1 AS ok, CURRENT_TIMESTAMP() AS ts").result())
    except Forbidden as exc:
        print("[ERROR] BigQuery query permission denied.")
        print(f"[ERROR] {exc}")
        print(
            "[HINT] Service account needs at least "
            "`roles/bigquery.jobUser` on project "
            f"`{project_id}` for query jobs."
        )
        raise SystemExit(2) from exc
    print("[OK] Query test:", dict(rows[0].items()))

    dataset_ref = bigquery.Dataset(f"{project_id}.{bq_dataset}")
    dataset_ref.location = bq_location
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
    except Forbidden as exc:
        print("[ERROR] Dataset create/check permission denied.")
        print(f"[ERROR] {exc}")
        print(
            "[HINT] Grant service account one of: "
            "`roles/bigquery.user` (project) or pre-create dataset and grant "
            "`roles/bigquery.dataEditor` on dataset."
        )
        raise SystemExit(3) from exc
    print(f"[OK] Dataset ready: {project_id}.{bq_dataset} ({bq_location})")


if __name__ == "__main__":
    main()

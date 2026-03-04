#!/usr/bin/env python3
"""
Run BigQuery SQL files (single-statement per file) with simple placeholders.

Supported placeholders:
- {{PROJECT_ID}}
- {{BQ_DATASET}}
- {{FULL_DATASET}}   (project.dataset)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run BigQuery SQL files from a directory or a single file.")
    parser.add_argument(
        "--sql-path",
        required=True,
        help="Path to SQL directory or SQL file",
    )
    parser.add_argument(
        "--write-select-results",
        action="store_true",
        help="Write SELECT query results to CSV under artifacts/bigquery/sql-runs",
    )
    return parser.parse_args()


def collect_sql_files(sql_path: Path) -> List[Path]:
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL path not found: {sql_path}")
    if sql_path.is_file():
        return [sql_path]
    files = sorted(p for p in sql_path.rglob("*.sql") if p.is_file())
    if not files:
        raise RuntimeError(f"No .sql files found under: {sql_path}")
    return files


def render_sql(text: str, replacements: Dict[str, str]) -> str:
    out = text
    for key, value in replacements.items():
        out = out.replace(f"{{{{{key}}}}}", value)
    return out


def main() -> None:
    load_env_file()
    args = parse_args()

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

    sql_files = collect_sql_files(Path(args.sql_path))
    full_dataset = f"{project_id}.{bq_dataset}"
    replacements = {
        "PROJECT_ID": project_id,
        "BQ_DATASET": bq_dataset,
        "FULL_DATASET": full_dataset,
    }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path("artifacts/bigquery/sql-runs") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, object]] = []
    for sql_file in sql_files:
        raw_sql = sql_file.read_text(encoding="utf-8")
        query = render_sql(raw_sql, replacements).strip()
        if not query:
            print(f"[SKIP] empty sql: {sql_file}")
            continue

        print(f"[RUN] {sql_file}")
        job = client.query(query)
        row_iter = job.result()
        row_count = row_iter.total_rows if row_iter.total_rows is not None else 0
        bytes_processed = int(job.total_bytes_processed or 0)
        statement_type = str((job.statement_type or "").upper())

        csv_file = ""
        if args.write_select_results and statement_type == "SELECT":
            csv_file = f"{sql_file.stem}.csv"
            out_csv = out_dir / csv_file
            row_iter = iter(row_iter)
            first_row = next(row_iter, None)
            header = [field.name for field in (job.schema or [])]
            if not header and first_row is not None:
                header = list(first_row.keys())
            with out_csv.open("w", encoding="utf-8", newline="") as fp:
                writer = csv.writer(fp)
                if header:
                    writer.writerow(header)
                if first_row is not None:
                    writer.writerow([first_row[col] if first_row[col] is not None else "" for col in header])
                for row in row_iter:
                    writer.writerow([row[col] if row[col] is not None else "" for col in header])

        item = {
            "sql_file": str(sql_file),
            "statement_type": statement_type,
            "row_count": row_count,
            "total_bytes_processed": bytes_processed,
            "csv_file": csv_file,
        }
        results.append(item)
        print(
            f"[DONE] {sql_file.name} type={statement_type} rows={row_count} bytes={bytes_processed}"
        )

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "project_id": project_id,
        "dataset_id": bq_dataset,
        "location": bq_location,
        "sql_path": args.sql_path,
        "result_count": len(results),
        "results": results,
    }
    out_file = out_dir / "run-summary.json"
    out_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[DONE] summary: {out_file}")


if __name__ == "__main__":
    main()

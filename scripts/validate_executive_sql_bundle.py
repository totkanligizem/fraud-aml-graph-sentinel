#!/usr/bin/env python3
"""Validate the BigQuery executive view SQL bundle locally."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql" / "bigquery" / "executive_views"
REPORT_PATH = ROOT / "artifacts" / "bigquery" / "validate-executive-sql-bundle.json"

EXPECTED_FILES = {
    "01_create_exec_overview_kpi.sql": "dev_exec_overview_kpi",
    "02_create_exec_dataset_surface.sql": "dev_exec_dataset_surface",
    "03_create_exec_daily_surface.sql": "dev_exec_daily_surface",
    "04_create_exec_queue_watchlist.sql": "dev_exec_queue_watchlist",
    "05_create_exec_graph_watchlists.sql": "dev_exec_graph_watchlists",
}


def ensure(condition: bool, message: str, errors: List[str]) -> None:
    if not condition:
        errors.append(message)


def main() -> None:
    errors: List[str] = []
    warnings: List[str] = []
    file_summaries: List[Dict[str, object]] = []

    ensure(SQL_DIR.exists(), f"Missing SQL dir: {SQL_DIR}", errors)
    files = sorted(path.name for path in SQL_DIR.glob("*.sql"))
    ensure(files == sorted(EXPECTED_FILES.keys()), "Executive SQL bundle file set is unexpected", errors)

    for file_name, expected_view in EXPECTED_FILES.items():
        path = SQL_DIR / file_name
        ensure(path.exists(), f"Missing SQL file: {file_name}", errors)
        if not path.exists():
            continue

        text = path.read_text(encoding="utf-8").strip()
        normalized = re.sub(r"\s+", " ", text.upper())
        ensure(text.startswith("CREATE OR REPLACE VIEW"), f"{file_name} does not start with CREATE OR REPLACE VIEW", errors)
        ensure("{{FULL_DATASET}}" in text, f"{file_name} is missing {{FULL_DATASET}} placeholder", errors)
        ensure(expected_view in text, f"{file_name} does not target expected view name {expected_view}", errors)
        ensure(normalized.count("CREATE OR REPLACE VIEW") == 1, f"{file_name} should contain exactly one CREATE OR REPLACE VIEW", errors)
        ensure(text.endswith(";"), f"{file_name} should end with semicolon", errors)

        referenced_tables = sorted(set(re.findall(r"dev_[a-z0-9_]+", text)))
        referenced_tables = [item for item in referenced_tables if item != expected_view]
        if not referenced_tables:
            warnings.append(f"{file_name} has no explicit dev_* table references")

        file_summaries.append(
            {
                "file": file_name,
                "view": expected_view,
                "referenced_tables": referenced_tables,
                "size_bytes": path.stat().st_size,
            }
        )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ok": len(errors) == 0,
        "sql_dir": str(SQL_DIR.relative_to(ROOT)),
        "file_count": len(file_summaries),
        "files": file_summaries,
        "warnings": warnings,
        "errors": errors,
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

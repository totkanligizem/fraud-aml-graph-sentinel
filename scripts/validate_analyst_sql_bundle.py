#!/usr/bin/env python3
"""Validate the BigQuery analyst SQL bundles locally."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parent.parent
VIEWS_DIR = ROOT / "sql" / "bigquery" / "analyst_views"
VALIDATION_DIR = ROOT / "sql" / "bigquery" / "analyst_validation"
REPORT_PATH = ROOT / "artifacts" / "bigquery" / "validate-analyst-sql-bundle.json"

EXPECTED_VIEWS = {
    "01_create_exec_analyst_surface.sql": "dev_exec_analyst_surface",
    "02_create_exec_analyst_action_items.sql": "dev_exec_analyst_action_items",
}

EXPECTED_VALIDATIONS = {
    "01_analyst_view_shapes.sql",
    "02_analyst_view_quality.sql",
}


def ensure(condition: bool, message: str, errors: List[str]) -> None:
    if not condition:
        errors.append(message)


def summarize_view_bundle(errors: List[str], warnings: List[str]) -> List[Dict[str, object]]:
    files = sorted(path.name for path in VIEWS_DIR.glob("*.sql"))
    ensure(files == sorted(EXPECTED_VIEWS.keys()), "Analyst view SQL bundle file set is unexpected", errors)

    summaries: List[Dict[str, object]] = []
    for file_name, expected_view in EXPECTED_VIEWS.items():
        path = VIEWS_DIR / file_name
        ensure(path.exists(), f"Missing analyst view SQL file: {file_name}", errors)
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8").strip()
        normalized = re.sub(r"\s+", " ", text.upper())
        ensure(text.startswith("CREATE OR REPLACE VIEW"), f"{file_name} does not start with CREATE OR REPLACE VIEW", errors)
        ensure("{{FULL_DATASET}}" in text, f"{file_name} is missing {{FULL_DATASET}} placeholder", errors)
        ensure(expected_view in text, f"{file_name} does not target expected view name {expected_view}", errors)
        ensure(text.endswith(";"), f"{file_name} should end with semicolon", errors)
        ensure(normalized.count("CREATE OR REPLACE VIEW") == 1, f"{file_name} should contain exactly one CREATE OR REPLACE VIEW", errors)
        referenced = sorted(set(re.findall(r"dev_[a-z0-9_]+", text)))
        referenced = [item for item in referenced if item != expected_view]
        if not referenced:
            warnings.append(f"{file_name} has no explicit dev_* references")
        summaries.append(
            {
                "file": file_name,
                "view": expected_view,
                "referenced_tables": referenced,
                "size_bytes": path.stat().st_size,
            }
        )
    return summaries


def summarize_validation_bundle(errors: List[str]) -> List[Dict[str, object]]:
    files = sorted(path.name for path in VALIDATION_DIR.glob("*.sql"))
    ensure(files == sorted(EXPECTED_VALIDATIONS), "Analyst validation SQL bundle file set is unexpected", errors)

    summaries: List[Dict[str, object]] = []
    for file_name in sorted(EXPECTED_VALIDATIONS):
        path = VALIDATION_DIR / file_name
        ensure(path.exists(), f"Missing analyst validation SQL file: {file_name}", errors)
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8").strip()
        ensure("{{FULL_DATASET}}" in text, f"{file_name} is missing {{FULL_DATASET}} placeholder", errors)
        ensure(text.endswith(";"), f"{file_name} should end with semicolon", errors)
        summaries.append(
            {
                "file": file_name,
                "referenced_objects": sorted(set(re.findall(r"dev_[a-z0-9_]+", text))),
                "size_bytes": path.stat().st_size,
            }
        )
    return summaries


def main() -> None:
    errors: List[str] = []
    warnings: List[str] = []

    ensure(VIEWS_DIR.exists(), f"Missing SQL dir: {VIEWS_DIR}", errors)
    ensure(VALIDATION_DIR.exists(), f"Missing SQL dir: {VALIDATION_DIR}", errors)

    view_summaries = summarize_view_bundle(errors, warnings)
    validation_summaries = summarize_validation_bundle(errors)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ok": len(errors) == 0,
        "views_dir": str(VIEWS_DIR.relative_to(ROOT)),
        "validation_dir": str(VALIDATION_DIR.relative_to(ROOT)),
        "view_file_count": len(view_summaries),
        "validation_file_count": len(validation_summaries),
        "views": view_summaries,
        "validations": validation_summaries,
        "warnings": warnings,
        "errors": errors,
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

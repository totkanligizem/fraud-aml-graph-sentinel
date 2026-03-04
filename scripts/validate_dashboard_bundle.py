#!/usr/bin/env python3
"""Validate the static dashboard bundle and fail fast on publish-breaking drift."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_JSON_PATH = DASHBOARD_DIR / "dashboard-data.json"
DATA_JS_PATH = DASHBOARD_DIR / "dashboard-data.js"
INDEX_PATH = DASHBOARD_DIR / "index.html"
APP_JS_PATH = DASHBOARD_DIR / "app.js"
CSS_PATH = DASHBOARD_DIR / "styles.css"
REPORT_PATH = ROOT / "artifacts" / "dashboard" / "validate-dashboard-state.json"

REQUIRED_TOP_LEVEL_KEYS = {
    "generated_at_utc",
    "snapshot_generated_at_utc",
    "project",
    "kpis",
    "ranking",
    "model",
    "graph",
    "quality",
    "dataset_breakdown",
    "daily_series",
    "score_buckets",
    "queue_highlights",
    "graph_panels",
    "analyst",
    "pipeline_steps",
    "top_features",
    "insights",
    "evidence_paths",
    "evidence_items",
    "freshness",
}


def ensure(condition: bool, message: str, errors: List[str]) -> None:
    if not condition:
        errors.append(message)


def approx_equal(left: float, right: float, tolerance: float = 1e-9) -> bool:
    return abs(left - right) <= tolerance


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def iso_to_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def sum_quality_rows(rows: Iterable[Dict[str, Any]]) -> int:
    return sum(int(row["value"]) for row in rows if row.get("value") is not None)


def count_zero_quality_rows(rows: Iterable[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("status") == "passed")


def count_unresolved_quality_rows(rows: Iterable[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("status") == "unresolved")


def validate_payload(payload: Dict[str, Any], errors: List[str], warnings: List[str]) -> Dict[str, Any]:
    ensure(REQUIRED_TOP_LEVEL_KEYS.issubset(payload.keys()), "dashboard-data.json top-level keys are incomplete", errors)

    datasets = payload.get("dataset_breakdown", [])
    dataset_ids = [row["dataset_id"] for row in datasets]
    unique_dataset_ids = sorted(set(dataset_ids))
    ensure(len(dataset_ids) == len(unique_dataset_ids), "dataset_breakdown contains duplicate dataset_id values", errors)
    ensure(len(dataset_ids) >= 4, "dataset_breakdown should contain at least four active datasets", errors)

    total_transactions = sum(int(row["transaction_rows"]) for row in datasets)
    total_scored_rows = sum(int(row["scored_rows"]) for row in datasets)
    total_volume_share = sum(float(row["share_of_volume"]) for row in datasets)

    ensure(total_transactions == int(payload["kpis"]["total_transactions"]), "kpis.total_transactions does not match dataset_breakdown sum", errors)
    ensure(total_scored_rows == int(payload["kpis"]["scored_rows"]), "kpis.scored_rows does not match dataset_breakdown sum", errors)
    ensure(approx_equal(total_volume_share, 1.0, tolerance=1e-6), "dataset_breakdown share_of_volume does not sum to 1.0", errors)

    daily_keys = set(payload.get("daily_series", {}).keys())
    bucket_keys = set(payload.get("score_buckets", {}).keys())
    queue_keys = set(payload.get("queue_highlights", {}).keys())

    ensure("overview" in daily_keys, "daily_series is missing overview lens", errors)
    ensure("overview" in bucket_keys, "score_buckets is missing overview lens", errors)
    ensure("overview" in queue_keys, "queue_highlights is missing overview lens", errors)

    for row in datasets:
        dataset_id = row["dataset_id"]
        scored_rows = int(row["scored_rows"])
        ensure(dataset_id in daily_keys, f"daily_series missing dataset lens: {dataset_id}", errors)
        if scored_rows > 0:
            ensure(dataset_id in bucket_keys, f"score_buckets missing scored dataset lens: {dataset_id}", errors)
            ensure(dataset_id in queue_keys, f"queue_highlights missing scored dataset lens: {dataset_id}", errors)
            bucket_total = sum(int(item["row_count"]) for item in payload["score_buckets"].get(dataset_id, []))
            ensure(bucket_total == scored_rows, f"score bucket total mismatch for dataset {dataset_id}", errors)
            ensure(len(payload["queue_highlights"].get(dataset_id, [])) > 0, f"queue_highlights empty for scored dataset {dataset_id}", errors)
        else:
            ensure(dataset_id not in bucket_keys, f"unscored dataset {dataset_id} should not have score buckets", errors)
            ensure(dataset_id not in queue_keys, f"unscored dataset {dataset_id} should not have queue highlights", errors)

    overview_bucket_total = sum(int(item["row_count"]) for item in payload["score_buckets"].get("overview", []))
    ensure(overview_bucket_total == int(payload["kpis"]["scored_rows"]), "overview score bucket total does not match kpis.scored_rows", errors)

    analyst = payload.get("analyst", {})
    ensure(isinstance(analyst, dict), "analyst panel payload is invalid", errors)
    if isinstance(analyst, dict):
        ensure("available" in analyst, "analyst.available missing", errors)
        if analyst.get("available"):
            responses = analyst.get("responses", [])
            ensure(int(analyst.get("error_count", 0)) == 0, "analyst panel contains model errors", errors)
            ensure(int(analyst.get("response_count", 0)) == len(responses), "analyst response_count mismatch", errors)
            ensure(len(responses) > 0, "analyst available but no responses present", errors)
            for index, item in enumerate(responses, start=1):
                ensure(bool(item.get("queue_id")), f"analyst response {index} missing queue_id", errors)
                ensure(bool(item.get("overall_priority")), f"analyst response {index} missing overall_priority", errors)
                ensure(bool(item.get("case_overview")), f"analyst response {index} missing case_overview", errors)
                ensure(isinstance(item.get("observed_signals"), list), f"analyst response {index} observed_signals invalid", errors)
                ensure(isinstance(item.get("recommended_actions"), list), f"analyst response {index} recommended_actions invalid", errors)

    quality = payload["quality"]
    core_rows = quality["core"]
    graph_rows = quality["graph"]
    all_rows = [*core_rows, *graph_rows]
    passed_checks = count_zero_quality_rows(all_rows)
    unresolved_checks = count_unresolved_quality_rows(all_rows)
    failed_checks = sum(1 for row in all_rows if row.get("status") == "failed")
    total_checks = len(all_rows)
    total_defects = sum_quality_rows(all_rows)
    ensure(int(quality["passed_checks"]) == passed_checks, "quality.passed_checks mismatch", errors)
    ensure(int(quality["failed_checks"]) == failed_checks, "quality.failed_checks mismatch", errors)
    ensure(int(quality["unresolved_checks"]) == unresolved_checks, "quality.unresolved_checks mismatch", errors)
    ensure(int(quality["total_checks"]) == total_checks, "quality.total_checks mismatch", errors)
    ensure(int(quality["total_defects"]) == total_defects, "quality.total_defects mismatch", errors)
    expected_assurance = (passed_checks / total_checks) if total_checks else 0.0
    ensure(approx_equal(float(quality["assurance_ratio"]), expected_assurance), "quality.assurance_ratio mismatch", errors)
    expected_resolved = ((passed_checks + failed_checks) / total_checks) if total_checks else 0.0
    ensure(approx_equal(float(quality["resolved_ratio"]), expected_resolved), "quality.resolved_ratio mismatch", errors)
    ensure(total_defects >= 0, "quality.total_defects cannot be negative", errors)
    for row in all_rows:
        ensure(row.get("status") in {"passed", "failed", "unresolved"}, f"invalid quality status for {row.get('name')}", errors)
        if row.get("status") == "unresolved":
            ensure(row.get("value") is None, f"unresolved quality row should have null value for {row.get('name')}", errors)
        else:
            ensure(isinstance(row.get("value"), int), f"resolved quality row must have integer value for {row.get('name')}", errors)

    evidence_paths = payload.get("evidence_paths", [])
    evidence_items = payload.get("evidence_items", [])
    ensure(len(evidence_paths) == len(evidence_items), "evidence_paths and evidence_items length mismatch", errors)
    for item in evidence_items:
        rel_path = item["path"]
        abs_path = ROOT / rel_path
        ensure(abs_path.exists(), f"Missing evidence artifact: {rel_path}", errors)
        if abs_path.exists():
            ensure(bool(item["exists"]), f"evidence_items marks existing file as missing: {rel_path}", errors)
            ensure(int(item["size_bytes"]) == abs_path.stat().st_size, f"evidence size mismatch: {rel_path}", errors)

    snapshot_age_days = (datetime.now(timezone.utc) - iso_to_dt(payload["snapshot_generated_at_utc"])).total_seconds() / 86400
    if snapshot_age_days > 7:
        warnings.append(f"checkpoint snapshot is {snapshot_age_days:.1f} days old")

    bq_age_days = (datetime.now(timezone.utc) - iso_to_dt(payload["freshness"]["bigquery_validation_generated_at_utc"])).total_seconds() / 86400
    if bq_age_days > 7:
        warnings.append(f"BigQuery validation artifact is {bq_age_days:.1f} days old")

    return {
        "dataset_count": len(dataset_ids),
        "total_transactions": total_transactions,
        "total_scored_rows": total_scored_rows,
        "passed_checks": passed_checks,
        "total_checks": total_checks,
        "total_defects": total_defects,
        "snapshot_age_days": round(snapshot_age_days, 2),
        "bigquery_artifact_age_days": round(bq_age_days, 2),
    }


def validate_js_bundle(payload: Dict[str, Any], errors: List[str]) -> None:
    js_text = DATA_JS_PATH.read_text(encoding="utf-8")
    prefix = "window.__AML_DASHBOARD_DATA__ = "
    ensure(js_text.startswith(prefix), "dashboard-data.js prefix is invalid", errors)
    if not js_text.startswith(prefix):
        return
    js_payload = json.loads(js_text[len(prefix) :].rstrip(";\n"))
    ensure(js_payload == payload, "dashboard-data.js payload does not match dashboard-data.json", errors)


def validate_html_bindings(errors: List[str]) -> Dict[str, Any]:
    html_text = INDEX_PATH.read_text(encoding="utf-8")
    app_text = APP_JS_PATH.read_text(encoding="utf-8")
    css_text = CSS_PATH.read_text(encoding="utf-8")

    html_ids = re.findall(r'id="([^"]+)"', html_text)
    duplicate_html_ids = sorted({item for item in html_ids if html_ids.count(item) > 1})
    js_ids = sorted(set(re.findall(r'\$\("([^"]+)"\)', app_text)))
    missing_ids = sorted(set(js_ids) - set(html_ids))

    ensure(not duplicate_html_ids, f"index.html contains duplicate ids: {', '.join(duplicate_html_ids)}", errors)
    ensure(not missing_ids, f"app.js references missing HTML ids: {', '.join(missing_ids)}", errors)

    ensure('src="dashboard-data.js"' in html_text, "index.html is missing dashboard-data.js reference", errors)
    ensure('src="app.js"' in html_text, "index.html is missing app.js reference", errors)
    ensure('href="styles.css"' in html_text, "index.html is missing styles.css reference", errors)

    ensure("Fraunces" in css_text and "Sora" in css_text, "styles.css is missing custom typography tokens", errors)
    ensure("linear-gradient" in css_text and "radial-gradient" in css_text, "styles.css is missing layered gradient background", errors)
    ensure("@media" in css_text, "styles.css is missing responsive media queries", errors)

    return {
        "html_id_count": len(set(html_ids)),
        "js_bound_id_count": len(js_ids),
        "duplicate_html_ids": duplicate_html_ids,
        "missing_html_ids": missing_ids,
    }


def main() -> None:
    for path in [DATA_JSON_PATH, DATA_JS_PATH, INDEX_PATH, APP_JS_PATH, CSS_PATH]:
        if not path.exists():
            raise FileNotFoundError(f"Missing dashboard bundle file: {path.relative_to(ROOT)}")

    errors: List[str] = []
    warnings: List[str] = []
    payload = load_json(DATA_JSON_PATH)

    payload_summary = validate_payload(payload, errors, warnings)
    validate_js_bundle(payload, errors)
    dom_summary = validate_html_bindings(errors)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ok": len(errors) == 0,
        "payload_summary": payload_summary,
        "dom_summary": dom_summary,
        "warnings": warnings,
        "errors": errors,
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

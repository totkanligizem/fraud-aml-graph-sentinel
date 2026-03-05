#!/usr/bin/env python3
"""Evaluate prompt-governance and response quality for Vertex analyst outputs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent

REQUIRED_PROMPT_KEYS = {
    "queue_id",
    "dataset_id",
    "prompt_version",
    "payload_policy_version",
    "masking_applied",
    "messages",
}

ALLOWED_CASE_PACKET_KEYS = {
    "queue_id",
    "dataset_id",
    "event_date",
    "queue_metrics",
    "surface",
    "top_events",
    "party_watchlist",
    "cluster_watchlist",
    "evidence_notes",
}

BLOCKED_SENSITIVE_KEYS = {
    "payer_account_id",
    "payee_account_id",
    "email_domain",
    "ip_prefix",
    "device_id",
    "payer_party_id",
    "payee_party_id",
}

REQUIRED_RESPONSE_KEYS = {
    "case_overview",
    "observed_signals",
    "investigation_hypotheses",
    "recommended_actions",
    "risk_assessment",
    "evidence_gaps",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Vertex prompt and response quality.")
    parser.add_argument("--prompt-pack-dir", default="artifacts/agent/prompt_pack/latest", help="Prompt pack dir")
    parser.add_argument("--response-dir", default="artifacts/agent/vertex_responses/latest", help="Vertex output dir")
    parser.add_argument(
        "--golden-cases",
        default="docs/agent_prompt_golden_cases.json",
        help="Golden-case expectation file",
    )
    parser.add_argument("--out-root", default="artifacts/agent/evals", help="Output root")
    return parser.parse_args()


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def validate_prompt_payload(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    missing = sorted(REQUIRED_PROMPT_KEYS - payload.keys())
    if missing:
        errors.append(f"missing_prompt_keys: {', '.join(missing)}")
    if not bool(payload.get("masking_applied")):
        errors.append("masking_applied_false")

    messages = payload.get("messages")
    if not isinstance(messages, list) or len(messages) < 2:
        errors.append("messages_invalid")
        return errors

    user_content = str(messages[1].get("content", ""))
    try:
        user_payload = json.loads(user_content)
    except Exception:
        errors.append("user_content_not_json")
        return errors

    lowered = user_content.lower()
    for key in BLOCKED_SENSITIVE_KEYS:
        if f'"{key.lower()}"' in lowered:
            errors.append(f"blocked_key_present:{key}")

    if not isinstance(user_payload, dict):
        errors.append("user_payload_not_object")
        return errors

    case_packet = user_payload.get("case_packet")
    if not isinstance(case_packet, dict):
        errors.append("case_packet_invalid")
        return errors

    unknown = sorted(set(case_packet.keys()) - ALLOWED_CASE_PACKET_KEYS)
    if unknown:
        errors.append("unknown_case_packet_keys:" + ",".join(unknown))
    return errors


def validate_response_payload(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    missing = sorted(REQUIRED_RESPONSE_KEYS - payload.keys())
    if missing:
        errors.append(f"missing_response_keys: {', '.join(missing)}")

    risk = payload.get("risk_assessment")
    if not isinstance(risk, dict):
        errors.append("risk_assessment_invalid")
    else:
        for field in ["fraud_risk", "aml_risk", "network_risk", "overall_priority"]:
            if not str(risk.get(field, "")).strip():
                errors.append(f"risk_assessment_missing:{field}")

    if not isinstance(payload.get("observed_signals"), list):
        errors.append("observed_signals_not_list")
    if not isinstance(payload.get("recommended_actions"), list):
        errors.append("recommended_actions_not_list")
    if not isinstance(payload.get("evidence_gaps"), list):
        errors.append("evidence_gaps_not_list")
    return errors


def main() -> None:
    args = parse_args()
    prompt_pack_dir = ROOT / args.prompt_pack_dir
    response_dir = ROOT / args.response_dir
    golden_path = ROOT / args.golden_cases
    out_root = ROOT / args.out_root

    if not prompt_pack_dir.exists():
        raise FileNotFoundError(f"Prompt pack dir missing: {prompt_pack_dir}")
    if not response_dir.exists():
        raise FileNotFoundError(f"Response dir missing: {response_dir}")

    prompt_files = sorted(path for path in prompt_pack_dir.glob("*.json") if path.name != "prompt-pack-summary.json")
    if not prompt_files:
        raise RuntimeError(f"No prompt files in: {prompt_pack_dir}")

    prompt_checks: List[Dict[str, Any]] = []
    for path in prompt_files:
        payload = load_json(path)
        errors = validate_prompt_payload(payload)
        prompt_checks.append(
            {
                "file": path.name,
                "queue_id": payload.get("queue_id", ""),
                "dataset_id": payload.get("dataset_id", ""),
                "prompt_version": payload.get("prompt_version", ""),
                "payload_policy_version": payload.get("payload_policy_version", ""),
                "ok": len(errors) == 0,
                "errors": errors,
            }
        )

    run_summary_path = response_dir / "run-summary.json"
    run_summary = load_json(run_summary_path) if run_summary_path.exists() else {}
    response_checks: List[Dict[str, Any]] = []
    responses_by_queue: Dict[str, Dict[str, Any]] = {}
    for item in ensure_list(run_summary.get("results")):
        queue_id = str(item.get("queue_id", ""))
        output_name = str(item.get("output_file", ""))
        payload = load_json(response_dir / output_name) if output_name else {}
        errors = validate_response_payload(payload) if payload else ["missing_output_payload"]
        response_checks.append(
            {
                "queue_id": queue_id,
                "dataset_id": item.get("dataset_id", ""),
                "model": item.get("model", ""),
                "ok": len(errors) == 0,
                "errors": errors,
            }
        )
        if payload:
            responses_by_queue[queue_id] = payload

    golden_cases = load_json(golden_path).get("cases", []) if golden_path.exists() else []
    golden_results: List[Dict[str, Any]] = []
    for case in golden_cases:
        queue_id = str(case.get("queue_id", ""))
        expected = case.get("expected", {}) if isinstance(case.get("expected"), dict) else {}
        actual = responses_by_queue.get(queue_id)
        if not actual:
            golden_results.append({"queue_id": queue_id, "matched": False, "reason": "missing_response"})
            continue

        risk = actual.get("risk_assessment", {}) if isinstance(actual.get("risk_assessment"), dict) else {}
        checks = {
            "overall_priority": str(risk.get("overall_priority", "")) == str(expected.get("overall_priority", "")),
            "fraud_risk": str(risk.get("fraud_risk", "")) == str(expected.get("fraud_risk", "")),
            "aml_risk": str(risk.get("aml_risk", "")) == str(expected.get("aml_risk", "")),
            "network_risk": str(risk.get("network_risk", "")) == str(expected.get("network_risk", "")),
        }
        golden_results.append(
            {
                "queue_id": queue_id,
                "matched": all(checks.values()),
                "checks": checks,
            }
        )

    prompt_ok = sum(1 for row in prompt_checks if row["ok"])
    response_ok = sum(1 for row in response_checks if row["ok"])
    golden_covered = sum(1 for row in golden_results if row.get("reason") != "missing_response")
    golden_matched = sum(1 for row in golden_results if row.get("matched"))

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "prompt_pack_dir": str(prompt_pack_dir.relative_to(ROOT)),
        "response_dir": str(response_dir.relative_to(ROOT)),
        "golden_case_path": str(golden_path.relative_to(ROOT)) if golden_path.exists() else str(golden_path),
        "prompt_contract": {
            "total": len(prompt_checks),
            "passed": prompt_ok,
            "pass_rate": (prompt_ok / len(prompt_checks)) if prompt_checks else 0.0,
        },
        "response_schema": {
            "total": len(response_checks),
            "passed": response_ok,
            "pass_rate": (response_ok / len(response_checks)) if response_checks else 0.0,
        },
        "golden_eval": {
            "total_cases": len(golden_results),
            "covered_cases": golden_covered,
            "covered_rate": (golden_covered / len(golden_results)) if golden_results else 0.0,
            "matched_cases": golden_matched,
            "match_rate": (golden_matched / golden_covered) if golden_covered else 0.0,
        },
        "prompt_checks": prompt_checks,
        "response_checks": response_checks,
        "golden_results": golden_results,
    }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "prompt-eval-summary.json"
    out_md = out_dir / "prompt-eval-summary.md"
    out_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Vertex Prompt Governance Evaluation",
        "",
        f"Generated at (UTC): {summary['created_at_utc']}",
        "",
        "## Prompt Contract",
        f"- Passed: {prompt_ok}/{len(prompt_checks)}",
        f"- Pass rate: {summary['prompt_contract']['pass_rate']:.2%}",
        "",
        "## Response Schema",
        f"- Passed: {response_ok}/{len(response_checks)}",
        f"- Pass rate: {summary['response_schema']['pass_rate']:.2%}",
        "",
        "## Golden Cases",
        f"- Covered: {golden_covered}/{len(golden_results)}",
        f"- Matched: {golden_matched}/{max(golden_covered, 1)}",
        f"- Match rate: {summary['golden_eval']['match_rate']:.2%}",
        "",
    ]
    out_md.write_text("\n".join(lines), encoding="utf-8")

    latest_dir = out_root / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    for stale in latest_dir.glob("*"):
        if stale.is_file():
            stale.unlink()
    for file in out_dir.glob("*"):
        (latest_dir / file.name).write_text(file.read_text(encoding="utf-8"), encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": str(out_dir),
                "latest_dir": str(latest_dir),
                "prompt_pass_rate": summary["prompt_contract"]["pass_rate"],
                "response_pass_rate": summary["response_schema"]["pass_rate"],
                "golden_match_rate": summary["golden_eval"]["match_rate"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

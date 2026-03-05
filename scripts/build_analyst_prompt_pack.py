#!/usr/bin/env python3
"""Build provider-agnostic analyst prompt packs from the latest casebook."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


SYSTEM_PROMPT = """You are a senior fraud and AML investigations analyst.

Work only from the supplied case packet.
Do not invent missing facts.
Separate observed facts from inference.
Keep fraud, AML and graph signals distinct.
If evidence is insufficient, state that explicitly.
Return concise, audit-friendly output.
"""

PROMPT_VERSION = "2026-03-v1"
PAYLOAD_POLICY_VERSION = "allowlist-mask-v1"
MASK_SALT = "fraud-aml-graph-sentinel"

OUTPUT_CONTRACT = {
    "case_overview": "short factual summary of the queue and why it is important",
    "observed_signals": [
        "bullet list of directly observed queue, event, graph and label signals",
    ],
    "investigation_hypotheses": [
        "numbered hypotheses grounded in evidence; each must include confidence low/medium/high",
    ],
    "recommended_actions": [
        "ordered next steps for an investigator",
    ],
    "risk_assessment": {
        "fraud_risk": "low|medium|high",
        "aml_risk": "low|medium|high",
        "network_risk": "low|medium|high",
        "overall_priority": "low|medium|high|critical",
    },
    "evidence_gaps": [
        "missing context or limitations that prevent stronger conclusions",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analyst prompt packs from casebook artifacts.")
    parser.add_argument("--casebook-path", default="artifacts/agent/casebook/latest/casebook.json", help="Casebook JSON path")
    parser.add_argument("--output-root", default="artifacts/agent/prompt_pack", help="Output root directory")
    return parser.parse_args()


def mask_identifier(value: Any, prefix: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digest = hashlib.sha256(f"{MASK_SALT}:{text}".encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def sanitize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "rank_in_queue": to_int_or_none(event.get("rank_in_queue")),
        "fraud_score": to_float_or_none(event.get("fraud_score")),
        "label_fraud": to_int_or_none(event.get("label_fraud")),
        "event_id": mask_identifier(event.get("event_id"), "evt"),
        "source_event_id": mask_identifier(event.get("source_event_id"), "src"),
        "event_time": str(event.get("event_time") or ""),
        "event_date": str(event.get("event_date") or ""),
        "amount": to_float_or_none(event.get("amount")),
        "txn_type": str(event.get("txn_type") or ""),
        "channel": str(event.get("channel") or ""),
    }


def sanitize_party(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "party_id": mask_identifier(item.get("party_id"), "pty"),
        "risk_score": to_float_or_none(item.get("risk_score")),
        "max_fraud_score": to_float_or_none(item.get("max_fraud_score")),
        "total_txn_count": to_int_or_none(item.get("total_txn_count")),
        "fraud_event_count": to_int_or_none(item.get("fraud_event_count")),
        "aml_event_count": to_int_or_none(item.get("aml_event_count")),
    }


def sanitize_cluster(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "cluster_id": str(item.get("cluster_id") or ""),
        "matched_party_count": to_int_or_none(item.get("matched_party_count")),
        "party_count": to_int_or_none(item.get("party_count")),
        "txn_count": to_int_or_none(item.get("txn_count")),
        "max_fraud_score": to_float_or_none(item.get("max_fraud_score")),
        "max_edge_risk_score": to_float_or_none(item.get("max_edge_risk_score")),
    }


def sanitize_case_packet(packet: Dict[str, Any]) -> Dict[str, Any]:
    queue_metrics = packet.get("queue_metrics", {}) if isinstance(packet.get("queue_metrics"), dict) else {}
    surface = packet.get("surface", {}) if isinstance(packet.get("surface"), dict) else {}
    top_events = packet.get("top_events", []) if isinstance(packet.get("top_events"), list) else []
    party_watchlist = packet.get("party_watchlist", []) if isinstance(packet.get("party_watchlist"), list) else []
    cluster_watchlist = packet.get("cluster_watchlist", []) if isinstance(packet.get("cluster_watchlist"), list) else []
    evidence_notes = packet.get("evidence_notes", []) if isinstance(packet.get("evidence_notes"), list) else []

    return {
        "queue_id": str(packet.get("queue_id") or ""),
        "dataset_id": str(packet.get("dataset_id") or ""),
        "event_date": str(packet.get("event_date") or ""),
        "queue_metrics": {
            "queue_rows": to_int_or_none(queue_metrics.get("queue_rows")),
            "avg_queue_score": to_float_or_none(queue_metrics.get("avg_queue_score")),
            "max_queue_score": to_float_or_none(queue_metrics.get("max_queue_score")),
            "positive_rows": to_int_or_none(queue_metrics.get("positive_rows")),
            "aml_rows": to_int_or_none(queue_metrics.get("aml_rows")),
            "avg_amount": to_float_or_none(queue_metrics.get("avg_amount")),
            "amount_sum": to_float_or_none(queue_metrics.get("amount_sum")),
        },
        "surface": {
            "top_event_count": to_int_or_none(surface.get("top_event_count")),
            "party_count_in_packet": to_int_or_none(surface.get("party_count_in_packet")),
            "account_count_in_packet": to_int_or_none(surface.get("account_count_in_packet")),
        },
        "top_events": [sanitize_event(item) for item in top_events[:5] if isinstance(item, dict)],
        "party_watchlist": [sanitize_party(item) for item in party_watchlist[:6] if isinstance(item, dict)],
        "cluster_watchlist": [sanitize_cluster(item) for item in cluster_watchlist[:6] if isinstance(item, dict)],
        "evidence_notes": [str(item)[:160] for item in evidence_notes[:6]],
    }


def build_user_prompt(packet: Dict[str, Any]) -> str:
    sanitized_packet = sanitize_case_packet(packet)
    return json.dumps(
        {
            "task": "Produce an investigation-ready case summary using the provided output contract.",
            "prompt_version": PROMPT_VERSION,
            "payload_policy": {
                "version": PAYLOAD_POLICY_VERSION,
                "identifier_masking": "sha256-prefix12",
                "pii_mode": "allowlist_only_masked_identifiers",
            },
            "output_contract": OUTPUT_CONTRACT,
            "case_packet": sanitized_packet,
        },
        indent=2,
        ensure_ascii=False,
    )


def build_packet_prompt(packet: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "queue_id": packet["queue_id"],
        "dataset_id": packet["dataset_id"],
        "prompt_version": PROMPT_VERSION,
        "payload_policy_version": PAYLOAD_POLICY_VERSION,
        "masking_applied": True,
        "model_recommendation": {
            "default_model": "gemini-2.5-flash",
            "escalation_model": "gemini-2.5-pro",
            "reason": "Default to Flash for routine case summarization; escalate to Pro for ambiguous or high-impact queues.",
        },
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(packet)},
        ],
    }


def main() -> None:
    args = parse_args()
    casebook_path = Path(args.casebook_path)
    if not casebook_path.exists():
        raise FileNotFoundError(f"Missing casebook: {casebook_path}")

    casebook = json.loads(casebook_path.read_text(encoding="utf-8"))
    output_root = Path(args.output_root)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    packs: List[Dict[str, Any]] = []
    for packet in casebook["case_packets"]:
        pack = build_packet_prompt(packet)
        packs.append(pack)
        safe_name = packet["queue_id"].replace("|", "__").replace("/", "_")
        (out_dir / f"{safe_name}.json").write_text(json.dumps(pack, indent=2, ensure_ascii=False), encoding="utf-8")

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "casebook_path": str(casebook_path),
        "prompt_pack_count": len(packs),
        "default_model": "gemini-2.5-flash",
        "escalation_model": "gemini-2.5-pro",
        "prompt_version": PROMPT_VERSION,
        "payload_policy_version": PAYLOAD_POLICY_VERSION,
        "masking_applied": True,
    }
    (out_dir / "prompt-pack-summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    latest_dir = output_root / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    for stale_file in latest_dir.glob("*.json"):
        stale_file.unlink()
    (latest_dir / "prompt-pack-summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    for packet in casebook["case_packets"]:
        safe_name = packet["queue_id"].replace("|", "__").replace("/", "_")
        src = out_dir / f"{safe_name}.json"
        dst = latest_dir / f"{safe_name}.json"
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    print(json.dumps({"output_dir": str(out_dir), "latest_dir": str(latest_dir), **summary}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Build provider-agnostic analyst prompt packs from the latest casebook."""

from __future__ import annotations

import argparse
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


def build_user_prompt(packet: Dict[str, Any]) -> str:
    return json.dumps(
        {
            "task": "Produce an investigation-ready case summary using the provided output contract.",
            "output_contract": OUTPUT_CONTRACT,
            "case_packet": packet,
        },
        indent=2,
        ensure_ascii=False,
    )


def build_packet_prompt(packet: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "queue_id": packet["queue_id"],
        "dataset_id": packet["dataset_id"],
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

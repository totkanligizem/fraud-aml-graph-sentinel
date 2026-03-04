#!/usr/bin/env python3
"""Validate analyst casebook artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate latest analyst casebook output.")
    parser.add_argument("--casebook-path", default="artifacts/agent/casebook/latest/casebook.json", help="Casebook JSON path")
    return parser.parse_args()


def ensure(condition: bool, message: str, errors: List[str]) -> None:
    if not condition:
        errors.append(message)


def main() -> None:
    args = parse_args()
    path = Path(args.casebook_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing casebook: {path}")

    payload: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    errors: List[str] = []
    packets = payload.get("case_packets", [])
    dataset_ids = sorted({str(packet.get("dataset_id")) for packet in packets if packet.get("dataset_id")})

    ensure(payload.get("queue_packet_count") == len(packets), "queue_packet_count mismatch", errors)
    ensure(len(packets) > 0, "casebook contains no case packets", errors)
    ensure(bool(payload.get("selection_strategy")), "selection_strategy missing", errors)
    ensure(len(dataset_ids) >= 2, "casebook should cover at least two scored datasets", errors)

    for index, packet in enumerate(packets, start=1):
        ensure(bool(packet.get("queue_id")), f"packet {index} missing queue_id", errors)
        ensure(bool(packet.get("dataset_id")), f"packet {index} missing dataset_id", errors)
        ensure("queue_metrics" in packet, f"packet {index} missing queue_metrics", errors)
        ensure("top_events" in packet, f"packet {index} missing top_events", errors)
        ensure("party_watchlist" in packet, f"packet {index} missing party_watchlist", errors)
        ensure("cluster_watchlist" in packet, f"packet {index} missing cluster_watchlist", errors)
        ensure("evidence_notes" in packet, f"packet {index} missing evidence_notes", errors)
        ensure(len(packet.get("top_events", [])) <= int(payload.get("events_per_queue", 0)), f"packet {index} exceeds events_per_queue cap", errors)
        ensure(len(packet.get("top_events", [])) > 0, f"packet {index} has no top events", errors)
        ensure(len(packet.get("evidence_notes", [])) > 0, f"packet {index} has no evidence notes", errors)

    report = {
        "ok": len(errors) == 0,
        "casebook_path": str(path),
        "queue_packet_count": len(packets),
        "dataset_count": len(dataset_ids),
        "events_per_queue": int(payload.get("events_per_queue", 0)),
        "errors": errors,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

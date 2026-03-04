#!/usr/bin/env python3
"""Validate latest Vertex analyst copilot outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate latest Vertex analyst outputs.")
    parser.add_argument("--output-dir", default="artifacts/agent/vertex_responses/latest", help="Latest Vertex response dir")
    parser.add_argument("--min-response-count", type=int, default=1, help="Minimum successful response count required")
    parser.add_argument("--min-dataset-count", type=int, default=1, help="Minimum unique dataset count required")
    return parser.parse_args()


def ensure(condition: bool, message: str, errors: List[str]) -> None:
    if not condition:
        errors.append(message)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    if not output_dir.exists():
        raise FileNotFoundError(f"Missing output dir: {output_dir}")

    summary_path = output_dir / "run-summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing run summary: {summary_path}")

    summary: Dict[str, Any] = json.loads(summary_path.read_text(encoding="utf-8"))
    errors: List[str] = []
    results = summary.get("results", [])
    dataset_ids = sorted({str(item.get("dataset_id", "")).strip() for item in results if item.get("dataset_id")})

    ensure(summary.get("response_count") == len(results), "response_count mismatch", errors)
    ensure(int(summary.get("error_count", 0)) == 0, "summary reports runtime or validation errors", errors)
    ensure(len(results) >= int(args.min_response_count), "response count below minimum", errors)
    ensure(len(dataset_ids) >= int(args.min_dataset_count), "dataset diversity below minimum", errors)

    for index, item in enumerate(results, start=1):
        ensure(not item.get("runtime_error"), f"response {index} has runtime_error", errors)
        ensure(not item.get("validation_errors"), f"response {index} has validation_errors", errors)
        ensure(bool(item.get("overall_priority")), f"response {index} missing overall_priority", errors)
        ensure(int(item.get("response_chars", 0)) > 0, f"response {index} empty", errors)

        output_file_name = str(item.get("output_file", "")).strip()
        ensure(bool(output_file_name), f"response {index} missing parsed output_file", errors)
        output_file = output_dir / output_file_name if output_file_name else output_dir / "__missing__.json"
        raw_file = output_dir / str(item["raw_file"])
        ensure(output_file.exists(), f"response {index} missing parsed json file", errors)
        ensure(raw_file.exists(), f"response {index} missing raw text file", errors)

        if output_file_name and output_file.exists() and output_file.is_file():
            payload = json.loads(output_file.read_text(encoding="utf-8"))
            ensure(bool(payload.get("case_overview")), f"response {index} missing case_overview", errors)
            ensure(isinstance(payload.get("observed_signals"), list), f"response {index} observed_signals invalid", errors)
            ensure(isinstance(payload.get("recommended_actions"), list), f"response {index} recommended_actions invalid", errors)

    report = {
        "ok": len(errors) == 0,
        "output_dir": str(output_dir),
        "response_count": len(results),
        "dataset_count": len(dataset_ids),
        "errors": errors,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Validate latest analyst prompt pack artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate latest analyst prompt pack.")
    parser.add_argument("--prompt-pack-dir", default="artifacts/agent/prompt_pack/latest", help="Prompt pack directory")
    return parser.parse_args()


def ensure(condition: bool, message: str, errors: List[str]) -> None:
    if not condition:
        errors.append(message)


def main() -> None:
    args = parse_args()
    prompt_dir = Path(args.prompt_pack_dir)
    if not prompt_dir.exists():
        raise FileNotFoundError(f"Missing prompt pack dir: {prompt_dir}")

    summary_path = prompt_dir / "prompt-pack-summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing prompt pack summary: {summary_path}")

    summary: Dict[str, Any] = json.loads(summary_path.read_text(encoding="utf-8"))
    errors: List[str] = []
    pack_files = sorted(path for path in prompt_dir.glob("*.json") if path.name != "prompt-pack-summary.json")

    ensure(len(pack_files) == int(summary.get("prompt_pack_count", -1)), "prompt_pack_count mismatch", errors)
    ensure(summary.get("default_model") == "gemini-2.5-flash", "unexpected default_model", errors)
    ensure(summary.get("escalation_model") == "gemini-2.5-pro", "unexpected escalation_model", errors)

    for index, path in enumerate(pack_files, start=1):
        payload = json.loads(path.read_text(encoding="utf-8"))
        ensure(bool(payload.get("queue_id")), f"pack {index} missing queue_id", errors)
        ensure(bool(payload.get("dataset_id")), f"pack {index} missing dataset_id", errors)
        ensure("messages" in payload and len(payload["messages"]) == 2, f"pack {index} invalid messages length", errors)
        ensure(payload["messages"][0]["role"] == "system", f"pack {index} first message must be system", errors)
        ensure(payload["messages"][1]["role"] == "user", f"pack {index} second message must be user", errors)

    report = {
        "ok": len(errors) == 0,
        "prompt_pack_dir": str(prompt_dir),
        "prompt_pack_count": len(pack_files),
        "errors": errors,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

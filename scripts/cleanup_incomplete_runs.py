#!/usr/bin/env python3
"""
Find and optionally delete incomplete canonical run directories.

A run is considered incomplete if:
- manifest.json is missing, or
- there are no part-*.csv files.
"""

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class Candidate:
    dataset: str
    run_dir: Path
    size_bytes: int
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cleanup incomplete canonical run directories.")
    parser.add_argument(
        "--root",
        default="data/curated/transaction_event",
        help="Canonical root directory",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete candidates (default is dry-run).",
    )
    return parser.parse_args()


def dir_size_bytes(path: Path) -> int:
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(n)
    for u in units:
        if val < 1024 or u == units[-1]:
            return f"{val:.1f}{u}"
        val /= 1024
    return f"{n}B"


def find_candidates(root: Path) -> List[Candidate]:
    candidates: List[Candidate] = []
    if not root.exists():
        return candidates

    for ds_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        dataset = ds_dir.name
        for run_dir in sorted([p for p in ds_dir.iterdir() if p.is_dir()]):
            manifest = run_dir / "manifest.json"
            part_files = list(run_dir.glob("part-*.csv"))

            if not manifest.exists():
                candidates.append(
                    Candidate(
                        dataset=dataset,
                        run_dir=run_dir,
                        size_bytes=dir_size_bytes(run_dir),
                        reason="missing manifest.json",
                    )
                )
                continue

            if len(part_files) == 0:
                candidates.append(
                    Candidate(
                        dataset=dataset,
                        run_dir=run_dir,
                        size_bytes=dir_size_bytes(run_dir),
                        reason="no part-*.csv files",
                    )
                )
                continue

            # Optional sanity: if manifest claims more rows than parts exist unexpectedly, keep it for safety.
            # We only auto-mark clearly incomplete runs above.

    return candidates


def main() -> None:
    args = parse_args()
    root = Path(args.root)
    candidates = find_candidates(root)

    report = {
        "root": str(root),
        "candidate_count": len(candidates),
        "total_candidate_size_bytes": int(sum(c.size_bytes for c in candidates)),
        "total_candidate_size_human": human_bytes(int(sum(c.size_bytes for c in candidates))),
        "candidates": [
            {
                "dataset": c.dataset,
                "run_dir": str(c.run_dir),
                "size_bytes": c.size_bytes,
                "size_human": human_bytes(c.size_bytes),
                "reason": c.reason,
            }
            for c in candidates
        ],
        "mode": "apply" if args.apply else "dry-run",
    }
    print(json.dumps(report, indent=2))

    if args.apply:
        for c in candidates:
            shutil.rmtree(c.run_dir, ignore_errors=True)
        print(f"[DONE] deleted {len(candidates)} incomplete runs")


if __name__ == "__main__":
    main()


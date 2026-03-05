#!/usr/bin/env python3
"""Fail if tracked files contain likely secret material.

This scanner is intentionally conservative and only checks git-tracked files.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("openai_api_key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b")),
    ("github_pat", re.compile(r"\bghp_[A-Za-z0-9]{30,}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("oauth_bearer", re.compile(r"\bya29\.[A-Za-z0-9_-]+\b")),
    ("private_key_block", re.compile(r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----")),
    ("service_account_private_key", re.compile(r'"private_key"\s*:\s*"-----BEGIN PRIVATE KEY-----')),
]

ALLOW_EXTS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".pdf",
    ".mp4",
    ".mov",
    ".zip",
    ".gz",
    ".bz2",
    ".xz",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".pkl",
    ".npz",
    ".woff",
    ".woff2",
}


def list_tracked_files() -> list[Path]:
    proc = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    return [ROOT / p for p in proc.stdout.decode("utf-8", errors="ignore").split("\0") if p]


def likely_text(path: Path) -> bool:
    if path.suffix.lower() in ALLOW_EXTS:
        return False
    try:
        chunk = path.read_bytes()[:4096]
    except OSError:
        return False
    return b"\x00" not in chunk


def scan_file(path: Path) -> list[dict[str, str | int]]:
    if not likely_text(path):
        return []
    rel = path.relative_to(ROOT)
    hits: list[dict[str, str | int]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []

    for name, pattern in PATTERNS:
        for m in pattern.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            snippet = m.group(0)
            if len(snippet) > 42:
                snippet = snippet[:12] + "..." + snippet[-8:]
            hits.append(
                {
                    "file": str(rel),
                    "line": line,
                    "pattern": name,
                    "snippet": snippet,
                }
            )
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate tracked files are secret-free")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    findings: list[dict[str, str | int]] = []
    for fp in list_tracked_files():
        findings.extend(scan_file(fp))

    payload = {"ok": len(findings) == 0, "finding_count": len(findings), "findings": findings}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, indent=2))

    return 0 if not findings else 2


if __name__ == "__main__":
    raise SystemExit(main())

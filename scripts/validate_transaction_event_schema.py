#!/usr/bin/env python3
"""Validate canonical transaction_event CSV parts against schema contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


REQUIRED_COLUMNS = {
    "event_id",
    "source_event_id",
    "dataset_id",
    "event_time",
    "payer_party_id",
    "payee_party_id",
    "payer_account_id",
    "payee_account_id",
    "channel",
    "txn_type",
    "amount",
    "currency",
    "fx_rate_to_usd",
    "label_fraud",
    "label_aml",
    "label_source",
    "pii_class",
    "consent_class",
    "retention_class",
    "ingested_at",
    "adapter_version",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate canonical transaction_event schema contract.")
    parser.add_argument("--canonical-root", default="data/curated/transaction_event", help="Canonical root path")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data"],
        help="Datasets to validate",
    )
    parser.add_argument("--sample-rows", type=int, default=5000, help="Rows to inspect per dataset")
    parser.add_argument("--out", default="artifacts/schema/validate-transaction-event-schema.json", help="Output report path")
    return parser.parse_args()


def latest_run_dir(dataset_root: Path) -> Path:
    runs = sorted(path for path in dataset_root.iterdir() if path.is_dir())
    if not runs:
        raise RuntimeError(f"No run directories under: {dataset_root}")
    return runs[-1]


def main() -> None:
    args = parse_args()
    canonical_root = Path(args.canonical_root)
    errors: List[str] = []
    by_dataset: Dict[str, Dict[str, object]] = {}

    for dataset_id in args.datasets:
        dataset_root = canonical_root / dataset_id
        if not dataset_root.exists():
            errors.append(f"Missing dataset root: {dataset_root}")
            continue

        run_dir = latest_run_dir(dataset_root)
        parts = sorted(run_dir.glob("part-*.csv"))
        if not parts:
            errors.append(f"No part files for dataset {dataset_id} under {run_dir}")
            continue

        rows_read = 0
        columns_seen: set[str] = set()
        local_errors: List[str] = []
        for part in parts:
            if rows_read >= args.sample_rows:
                break
            frame = pd.read_csv(part, nrows=max(1, args.sample_rows - rows_read))
            rows_read += len(frame)
            columns_seen.update(frame.columns)

            if "event_id" in frame.columns:
                if frame["event_id"].astype(str).str.strip().eq("").any():
                    local_errors.append("blank event_id values")
            if "event_time" in frame.columns:
                if frame["event_time"].astype(str).str.strip().eq("").any():
                    local_errors.append("blank event_time values")
            for label_col in ["label_fraud", "label_aml"]:
                if label_col in frame.columns:
                    normalized = pd.to_numeric(frame[label_col], errors="coerce")
                    invalid = normalized.dropna().isin([0, 1])
                    if not bool(invalid.all()):
                        local_errors.append(f"invalid domain in {label_col}")

        missing = sorted(REQUIRED_COLUMNS - columns_seen)
        if missing:
            local_errors.append("missing columns: " + ", ".join(missing))

        by_dataset[dataset_id] = {
            "run_dir": str(run_dir),
            "rows_checked": rows_read,
            "missing_columns": missing,
            "errors": sorted(set(local_errors)),
            "ok": len(local_errors) == 0,
        }
        errors.extend([f"{dataset_id}: {msg}" for msg in sorted(set(local_errors))])

    report = {
        "ok": len(errors) == 0,
        "canonical_root": str(canonical_root),
        "datasets": args.datasets,
        "by_dataset": by_dataset,
        "errors": errors,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

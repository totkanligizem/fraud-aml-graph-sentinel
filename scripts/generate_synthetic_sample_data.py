#!/usr/bin/env python3
"""Generate deterministic synthetic canonical datasets for reproducible smoke runs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


CANONICAL_COLUMNS = [
    "event_id",
    "source_event_id",
    "dataset_id",
    "event_time",
    "event_time_grain",
    "time_step",
    "payer_party_id",
    "payee_party_id",
    "payer_account_id",
    "payee_account_id",
    "channel",
    "txn_type",
    "mcc_category",
    "amount",
    "currency",
    "fx_rate_to_usd",
    "device_id",
    "ip_prefix",
    "email_domain",
    "geo",
    "label_fraud",
    "label_aml",
    "label_source",
    "pii_class",
    "consent_class",
    "retention_class",
    "raw_partition",
    "ingested_at",
    "adapter_version",
]


DATASET_START = {
    "creditcard_fraud": datetime(2013, 9, 1, 0, 0, 0, tzinfo=timezone.utc),
    "paysim": datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    "ieee_cis": datetime(2017, 12, 2, 0, 0, 0, tzinfo=timezone.utc),
    "ibm_aml_data": datetime(2022, 8, 1, 0, 0, 0, tzinfo=timezone.utc),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic canonical transaction_event sample data.")
    parser.add_argument(
        "--output-root",
        default="data/sample/transaction_event",
        help="Canonical sample output root",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data"],
        help="Datasets to generate",
    )
    parser.add_argument("--rows-per-dataset", type=int, default=2500, help="Rows per dataset")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--run-id", default="", help="Optional explicit run id")
    parser.add_argument("--adapter-version", default="sample-v1", help="Adapter version marker")
    return parser.parse_args()


def fraud_probability(dataset_id: str) -> float:
    if dataset_id == "ieee_cis":
        return 0.04
    if dataset_id == "creditcard_fraud":
        return 0.01
    if dataset_id == "paysim":
        return 0.008
    return 0.006


def aml_probability(dataset_id: str) -> float:
    if dataset_id == "ibm_aml_data":
        return 0.05
    return 0.002


def dataset_partition(dataset_id: str) -> str:
    return {
        "ieee_cis": "ieee",
        "creditcard_fraud": "creditcard",
        "paysim": "paysim",
        "ibm_aml_data": "ibmaml",
    }.get(dataset_id, dataset_id)


def make_frame(dataset_id: str, rows: int, rng: np.random.Generator, ingested_at: str, adapter_version: str) -> pd.DataFrame:
    start = DATASET_START.get(dataset_id, datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    times = [start + timedelta(minutes=int(i * 5 + rng.integers(0, 3))) for i in range(rows)]

    payer_ids = [f"{dataset_id[:3]}_payer_{int(i % 6000):05d}" for i in range(rows)]
    payee_ids = [f"{dataset_id[:3]}_payee_{int((i * 7) % 8000):05d}" for i in range(rows)]
    payer_accounts = [f"{dataset_id[:3]}_pa_{int(i % 9000):05d}" for i in range(rows)]
    payee_accounts = [f"{dataset_id[:3]}_ra_{int((i * 11) % 9000):05d}" for i in range(rows)]

    channels = np.array(["card", "bank_transfer", "mobile", "online"], dtype=object)
    txn_types = np.array(["purchase", "transfer", "withdrawal", "payment"], dtype=object)
    mcc = np.array(["retail", "gaming", "utility", "travel"], dtype=object)
    currencies = np.array(["USD", "EUR"], dtype=object)
    geos = np.array(["US-NY", "US-CA", "GB-LON", "TR-IST"], dtype=object)

    is_fraud = (rng.random(rows) < fraud_probability(dataset_id)).astype(int)
    is_aml = (rng.random(rows) < aml_probability(dataset_id)).astype(int)
    if dataset_id == "ibm_aml_data":
        is_aml = np.maximum(is_aml, (rng.random(rows) < 0.02).astype(int))

    amount_base = rng.lognormal(mean=4.2, sigma=1.1, size=rows)
    amount = np.round(np.clip(amount_base, 1.0, 120000.0), 2)

    # Inject stronger anomalies for positive labels to make sample ranking meaningful.
    boost_mask = (is_fraud == 1) | (is_aml == 1)
    amount[boost_mask] = np.round(np.clip(amount[boost_mask] * rng.uniform(2.0, 8.0, size=int(np.sum(boost_mask))), 5.0, 250000.0), 2)

    frame = pd.DataFrame(
        {
            "event_id": [f"{dataset_id}_{i:07d}" for i in range(rows)],
            "source_event_id": [f"src_{dataset_id}_{i:07d}" for i in range(rows)],
            "dataset_id": dataset_id,
            "event_time": [ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in times],
            "event_time_grain": "minute",
            "time_step": np.arange(rows, dtype=int),
            "payer_party_id": payer_ids,
            "payee_party_id": payee_ids,
            "payer_account_id": payer_accounts,
            "payee_account_id": payee_accounts,
            "channel": rng.choice(channels, size=rows, replace=True),
            "txn_type": rng.choice(txn_types, size=rows, replace=True),
            "mcc_category": rng.choice(mcc, size=rows, replace=True),
            "amount": amount,
            "currency": rng.choice(currencies, size=rows, replace=True),
            "fx_rate_to_usd": 1.0,
            "device_id": [f"dev_{dataset_id}_{int(i % 3000):04d}" for i in range(rows)],
            "ip_prefix": [f"10.{int(i % 200)}.{int((i * 3) % 200)}" for i in range(rows)],
            "email_domain": rng.choice(np.array(["mail.com", "bank.net", "corp.org"], dtype=object), size=rows, replace=True),
            "geo": rng.choice(geos, size=rows, replace=True),
            "label_fraud": is_fraud,
            "label_aml": is_aml,
            "label_source": "synthetic_sample",
            "pii_class": "masked",
            "consent_class": "research",
            "retention_class": "sample",
            "raw_partition": dataset_partition(dataset_id),
            "ingested_at": ingested_at,
            "adapter_version": adapter_version,
        }
    )
    return frame[CANONICAL_COLUMNS]


def write_dataset(output_root: Path, dataset_id: str, run_id: str, frame: pd.DataFrame, created_at_utc: str) -> Dict[str, object]:
    run_dir = output_root / dataset_id / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    part_name = "part-00001.csv"
    part_path = run_dir / part_name
    frame.to_csv(part_path, index=False)
    manifest = {
        "dataset_id": dataset_id,
        "run_id": run_id,
        "total_rows": int(len(frame)),
        "parts": [{"part": part_name, "rows": int(len(frame))}],
        "canonical_columns": CANONICAL_COLUMNS,
        "created_at_utc": created_at_utc,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {"dataset_id": dataset_id, "run_dir": str(run_dir), "rows": int(len(frame))}


def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    output_root = Path(args.output_root)
    created_at = datetime.now(timezone.utc)
    created_at_utc = created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = args.run_id.strip() or created_at.strftime("%Y%m%dT%H%M%SZ")

    records: List[Dict[str, object]] = []
    total_rows = 0
    for dataset_id in args.datasets:
        frame = make_frame(
            dataset_id=dataset_id,
            rows=int(args.rows_per_dataset),
            rng=rng,
            ingested_at=created_at_utc,
            adapter_version=args.adapter_version,
        )
        rec = write_dataset(output_root, dataset_id, run_id, frame, created_at_utc)
        records.append(rec)
        total_rows += int(rec["rows"])
        print(f"[DONE] dataset={dataset_id} rows={rec['rows']} run={run_id}", flush=True)

    summary = {
        "created_at_utc": created_at_utc,
        "output_root": str(output_root),
        "run_id": run_id,
        "rows_per_dataset": int(args.rows_per_dataset),
        "total_rows": int(total_rows),
        "datasets": records,
    }
    summary_path = output_root / "sample-generation-summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[DONE] summary={summary_path}", flush=True)


if __name__ == "__main__":
    main()

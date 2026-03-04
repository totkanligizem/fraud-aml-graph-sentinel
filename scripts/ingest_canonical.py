#!/usr/bin/env python3
"""
Build canonical transaction_event files from downloaded datasets.

Current adapters:
- ieee_cis (train_transaction.csv)
- creditcard_fraud (creditcard.csv)
- paysim (PS_20174392719_1491204439457_log.csv)
- ibm_aml_data (*_Trans.csv)
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional

import numpy as np
import pandas as pd


CANONICAL_COLUMNS: List[str] = [
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

ADAPTER_VERSION = "v0.1.0"


@dataclass
class AdapterContext:
    chunksize: int
    max_rows: Optional[int]
    ingested_at: str


def prefixed(series: pd.Series, prefix: str) -> pd.Series:
    s = series.astype("string")
    out = pd.Series(pd.NA, index=s.index, dtype="string")
    mask = s.notna() & (s.str.len() > 0)
    out.loc[mask] = prefix + s.loc[mask]
    return out


def sanitize_token(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.upper()
    s = s.str.replace(r"[^A-Z0-9]+", "_", regex=True).str.strip("_")
    s = s.replace("", pd.NA)
    return s


def to_event_time_iso(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m-%dT%H:%M:%S")


def finalize(df: pd.DataFrame) -> pd.DataFrame:
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[CANONICAL_COLUMNS]


def apply_common_defaults(
    df: pd.DataFrame,
    dataset_id: str,
    raw_partition: str,
    ingested_at: str,
) -> pd.DataFrame:
    df["dataset_id"] = dataset_id
    df["event_time_grain"] = df.get("event_time_grain", pd.Series("second", index=df.index))
    df["fx_rate_to_usd"] = df.get("fx_rate_to_usd", pd.Series(1.0, index=df.index))
    df["pii_class"] = "SYNTHETIC_OR_PUBLIC"
    df["consent_class"] = "PUBLIC_OR_SIMULATED"
    df["retention_class"] = "PORTFOLIO_NON_PROD"
    df["raw_partition"] = raw_partition
    df["ingested_at"] = ingested_at
    df["adapter_version"] = ADAPTER_VERSION
    return df


def enforce_max_rows(
    chunk: pd.DataFrame,
    total_rows: int,
    max_rows: Optional[int],
) -> pd.DataFrame:
    if max_rows is None:
        return chunk
    remaining = max_rows - total_rows
    if remaining <= 0:
        return chunk.iloc[0:0]
    if len(chunk) > remaining:
        return chunk.iloc[:remaining].copy()
    return chunk


def ieee_cis_adapter(data_root: Path, ctx: AdapterContext) -> Iterator[pd.DataFrame]:
    path = data_root / "raw" / "ieee_cis" / "extracted" / "train_transaction.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    usecols = [
        "TransactionID",
        "isFraud",
        "TransactionDT",
        "TransactionAmt",
        "ProductCD",
        "card1",
        "addr1",
        "P_emaildomain",
    ]
    channel_map = {
        "W": "ECOMMERCE",
        "C": "CARD",
        "R": "BANK_TRANSFER",
        "H": "HOUSEHOLD",
        "S": "OTHER",
    }

    total_rows = 0
    for part_no, chunk in enumerate(pd.read_csv(path, usecols=usecols, chunksize=ctx.chunksize), start=1):
        chunk = enforce_max_rows(chunk, total_rows, ctx.max_rows)
        if chunk.empty:
            break
        total_rows += len(chunk)

        event_time = pd.to_datetime("2017-12-01") + pd.to_timedelta(chunk["TransactionDT"], unit="s")
        out = pd.DataFrame(index=chunk.index)
        out["event_id"] = "ieee_cis-" + chunk["TransactionID"].astype("Int64").astype("string")
        out["source_event_id"] = chunk["TransactionID"].astype("Int64").astype("string")
        out["event_time"] = event_time.dt.strftime("%Y-%m-%dT%H:%M:%S")
        out["time_step"] = chunk["TransactionDT"]
        out["payer_party_id"] = prefixed(chunk["card1"], "party:card1:")
        out["payee_party_id"] = prefixed(chunk["addr1"], "party:addr1:")
        out["payer_account_id"] = prefixed(chunk["card1"], "account:card1:")
        out["payee_account_id"] = prefixed(chunk["addr1"], "account:addr1:")
        out["channel"] = chunk["ProductCD"].map(channel_map).fillna("OTHER")
        out["txn_type"] = "IEEE_CIS_" + chunk["ProductCD"].astype("string")
        out["mcc_category"] = pd.NA
        out["amount"] = chunk["TransactionAmt"]
        out["currency"] = "USD"
        out["email_domain"] = chunk["P_emaildomain"].astype("string")
        out["label_fraud"] = chunk["isFraud"].astype("Int64")
        out["label_aml"] = pd.NA
        out["label_source"] = "isFraud"
        out = apply_common_defaults(out, "ieee_cis", f"train_transaction_part_{part_no:05d}", ctx.ingested_at)
        yield finalize(out)

        if ctx.max_rows is not None and total_rows >= ctx.max_rows:
            break


def creditcard_adapter(data_root: Path, ctx: AdapterContext) -> Iterator[pd.DataFrame]:
    path = data_root / "raw" / "creditcard_fraud" / "extracted" / "creditcard.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    usecols = ["Time", "Amount", "Class"]
    total_rows = 0
    for part_no, chunk in enumerate(pd.read_csv(path, usecols=usecols, chunksize=ctx.chunksize), start=1):
        chunk = enforce_max_rows(chunk, total_rows, ctx.max_rows)
        if chunk.empty:
            break
        total_rows += len(chunk)

        row_id = (chunk.index.astype("int64") + 1).astype(str)
        event_time = pd.to_datetime("2013-09-01") + pd.to_timedelta(chunk["Time"], unit="s")

        out = pd.DataFrame(index=chunk.index)
        out["event_id"] = "creditcard_fraud-" + row_id
        out["source_event_id"] = row_id
        out["event_time"] = event_time.dt.strftime("%Y-%m-%dT%H:%M:%S")
        out["time_step"] = chunk["Time"]
        out["payer_party_id"] = pd.NA
        out["payee_party_id"] = pd.NA
        out["payer_account_id"] = pd.NA
        out["payee_account_id"] = pd.NA
        out["channel"] = "CARD"
        out["txn_type"] = "CARD_TRANSACTION"
        out["mcc_category"] = pd.NA
        out["amount"] = chunk["Amount"]
        out["currency"] = "EUR"
        out["label_fraud"] = chunk["Class"].astype("Int64")
        out["label_aml"] = pd.NA
        out["label_source"] = "Class"
        out = apply_common_defaults(
            out,
            "creditcard_fraud",
            f"creditcard_part_{part_no:05d}",
            ctx.ingested_at,
        )
        yield finalize(out)

        if ctx.max_rows is not None and total_rows >= ctx.max_rows:
            break


def paysim_adapter(data_root: Path, ctx: AdapterContext) -> Iterator[pd.DataFrame]:
    path = data_root / "raw" / "paysim" / "extracted" / "PS_20174392719_1491204439457_log.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    usecols = ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]
    total_rows = 0
    for part_no, chunk in enumerate(pd.read_csv(path, usecols=usecols, chunksize=ctx.chunksize), start=1):
        chunk = enforce_max_rows(chunk, total_rows, ctx.max_rows)
        if chunk.empty:
            break
        total_rows += len(chunk)

        row_id = (chunk.index.astype("int64") + 1).astype(str)
        event_time = pd.to_datetime("2017-01-01") + pd.to_timedelta(chunk["step"], unit="h")

        out = pd.DataFrame(index=chunk.index)
        out["event_id"] = "paysim-" + row_id
        out["source_event_id"] = row_id
        out["event_time"] = event_time.dt.strftime("%Y-%m-%dT%H:%M:%S")
        out["event_time_grain"] = "hour"
        out["time_step"] = chunk["step"]
        out["payer_party_id"] = prefixed(chunk["nameOrig"], "party:")
        out["payee_party_id"] = prefixed(chunk["nameDest"], "party:")
        out["payer_account_id"] = prefixed(chunk["nameOrig"], "account:")
        out["payee_account_id"] = prefixed(chunk["nameDest"], "account:")
        out["channel"] = sanitize_token(chunk["type"]).fillna("OTHER")
        out["txn_type"] = sanitize_token(chunk["type"]).fillna("UNKNOWN")
        out["mcc_category"] = pd.NA
        out["amount"] = chunk["amount"]
        out["currency"] = "USD"
        out["label_fraud"] = chunk["isFraud"].astype("Int64")
        out["label_aml"] = pd.NA
        out["label_source"] = "isFraud"
        out = apply_common_defaults(out, "paysim", f"paysim_part_{part_no:05d}", ctx.ingested_at)
        yield finalize(out)

        if ctx.max_rows is not None and total_rows >= ctx.max_rows:
            break


def ibm_aml_data_adapter(data_root: Path, ctx: AdapterContext) -> Iterator[pd.DataFrame]:
    extracted_dir = data_root / "raw" / "ibm_aml_data" / "extracted"
    if not extracted_dir.exists():
        raise FileNotFoundError(f"Missing folder: {extracted_dir}")

    files = sorted(extracted_dir.glob("*_Trans.csv"))
    if not files:
        raise FileNotFoundError(f"No *_Trans.csv files found in {extracted_dir}")

    usecols = [
        "Timestamp",
        "From Bank",
        "Account",
        "To Bank",
        "Account.1",
        "Amount Paid",
        "Payment Currency",
        "Payment Format",
        "Is Laundering",
    ]

    total_rows = 0
    per_file_limit = None
    if ctx.max_rows is not None:
        # Balanced sampling across IBM files to avoid biasing toward the first sorted file.
        per_file_limit = max(1, int(np.ceil(ctx.max_rows / len(files))))

    for file in files:
        safe_stem = re.sub(r"[^A-Za-z0-9_-]+", "_", file.stem)
        file_rows = 0
        for part_no, chunk in enumerate(pd.read_csv(file, usecols=usecols, chunksize=ctx.chunksize), start=1):
            if per_file_limit is not None:
                remaining_in_file = per_file_limit - file_rows
                if remaining_in_file <= 0:
                    break
                if len(chunk) > remaining_in_file:
                    chunk = chunk.iloc[:remaining_in_file].copy()

            chunk = enforce_max_rows(chunk, total_rows, ctx.max_rows)
            if chunk.empty:
                break
            total_rows += len(chunk)
            file_rows += len(chunk)

            row_id = (chunk.index.astype("int64") + 1).astype(str)
            from_bank = chunk["From Bank"].astype("Int64").astype("string")
            from_acc = chunk["Account"].astype("string")
            to_bank = chunk["To Bank"].astype("Int64").astype("string")
            to_acc = chunk["Account.1"].astype("string")

            out = pd.DataFrame(index=chunk.index)
            out["event_id"] = "ibm_aml_data-" + safe_stem + "-" + row_id
            out["source_event_id"] = safe_stem + "-" + row_id
            out["event_time"] = to_event_time_iso(
                pd.to_datetime(chunk["Timestamp"], format="%Y/%m/%d %H:%M", errors="coerce")
            )
            out["time_step"] = pd.NA
            out["payer_party_id"] = (
                "party:bank:" + from_bank.fillna("UNK") + ":acct_holder:" + from_acc.fillna("UNK")
            )
            out["payee_party_id"] = (
                "party:bank:" + to_bank.fillna("UNK") + ":acct_holder:" + to_acc.fillna("UNK")
            )
            out["payer_account_id"] = (
                "account:bank:" + from_bank.fillna("UNK") + ":acct:" + from_acc.fillna("UNK")
            )
            out["payee_account_id"] = (
                "account:bank:" + to_bank.fillna("UNK") + ":acct:" + to_acc.fillna("UNK")
            )
            out["channel"] = "BANK_TRANSFER"
            out["txn_type"] = sanitize_token(chunk["Payment Format"]).fillna("BANK_TRANSFER")
            out["mcc_category"] = pd.NA
            out["amount"] = chunk["Amount Paid"]
            out["currency"] = chunk["Payment Currency"].astype("string")
            out["label_fraud"] = pd.NA
            out["label_aml"] = chunk["Is Laundering"].astype("Int64")
            out["label_source"] = "Is_Laundering"
            out = apply_common_defaults(
                out,
                "ibm_aml_data",
                f"{safe_stem}_part_{part_no:05d}",
                ctx.ingested_at,
            )
            yield finalize(out)

            if ctx.max_rows is not None and total_rows >= ctx.max_rows:
                return


def write_canonical_csv(
    dataset_id: str,
    chunk_iter: Iterator[pd.DataFrame],
    output_root: Path,
    run_id: str,
) -> Dict[str, object]:
    ds_out = output_root / dataset_id / run_id
    ds_out.mkdir(parents=True, exist_ok=True)

    parts: List[Dict[str, object]] = []
    total_rows = 0
    for idx, df in enumerate(chunk_iter, start=1):
        if df.empty:
            continue
        part_path = ds_out / f"part-{idx:05d}.csv"
        df.to_csv(part_path, index=False)
        row_count = int(len(df))
        total_rows += row_count
        parts.append({"part": part_path.name, "rows": row_count})

    manifest = {
        "dataset_id": dataset_id,
        "run_id": run_id,
        "total_rows": total_rows,
        "parts": parts,
        "canonical_columns": CANONICAL_COLUMNS,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    (ds_out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def build_adapters() -> Dict[str, Callable[[Path, AdapterContext], Iterator[pd.DataFrame]]]:
    return {
        "ieee_cis": ieee_cis_adapter,
        "creditcard_fraud": creditcard_adapter,
        "paysim": paysim_adapter,
        "ibm_aml_data": ibm_aml_data_adapter,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest datasets into canonical transaction_event CSV files.")
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data", "all"],
        default=[],
        help="Dataset to ingest. Repeat for multiple.",
    )
    parser.add_argument("--data-root", default="data", help="Data root (default: data)")
    parser.add_argument(
        "--output-root",
        default="data/curated/transaction_event",
        help="Output root for canonical files",
    )
    parser.add_argument("--chunksize", type=int, default=200000, help="CSV chunksize (default: 200000)")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional max rows per dataset for smoke run (default: all rows)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_root = Path(args.data_root)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    selected = args.dataset or ["all"]
    if "all" in selected:
        selected = ["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data"]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ctx = AdapterContext(chunksize=args.chunksize, max_rows=args.max_rows, ingested_at=ingested_at)

    adapters = build_adapters()
    all_manifest: List[Dict[str, object]] = []

    for dataset_id in selected:
        print(f"[INFO] ingesting dataset={dataset_id} chunksize={ctx.chunksize} max_rows={ctx.max_rows}")
        adapter_fn = adapters[dataset_id]
        manifest = write_canonical_csv(dataset_id, adapter_fn(data_root, ctx), output_root, run_id)
        all_manifest.append(manifest)
        print(f"[DONE] dataset={dataset_id} rows={manifest['total_rows']} parts={len(manifest['parts'])}")

    summary = {
        "run_id": run_id,
        "datasets": all_manifest,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    summary_path = output_root / f"run-summary-{run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[DONE] summary={summary_path}")


if __name__ == "__main__":
    main()

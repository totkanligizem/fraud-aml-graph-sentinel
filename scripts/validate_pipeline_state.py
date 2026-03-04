#!/usr/bin/env python3
"""
Validate current project pipeline state and fail fast on missing/invalid artifacts.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate pipeline outputs and database state.")
    parser.add_argument("--db-path", default="data/warehouse/ledger_sentinel.db", help="SQLite DB path")
    parser.add_argument(
        "--canonical-root",
        default="data/curated/transaction_event",
        help="Canonical root path",
    )
    parser.add_argument(
        "--required-datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data"],
        help="Datasets expected in canonical outputs",
    )
    parser.add_argument(
        "--min-model-runs",
        type=int,
        default=1,
        help="Minimum number of fraud model runs expected",
    )
    parser.add_argument(
        "--scoring-datasets",
        nargs="+",
        default=["ieee_cis", "creditcard_fraud", "paysim"],
        help="Datasets expected in fraud_scores table",
    )
    return parser.parse_args()


def latest_run_with_manifest(dataset_root: Path) -> Path:
    run_dirs = sorted([p for p in dataset_root.iterdir() if p.is_dir()])
    if not run_dirs:
        raise RuntimeError(f"No runs under {dataset_root}")
    best = None
    best_rows = -1
    for rd in run_dirs:
        mf = rd / "manifest.json"
        if not mf.exists():
            continue
        try:
            rows = int(json.loads(mf.read_text(encoding="utf-8")).get("total_rows", -1))
        except Exception:
            rows = -1
        if rows > best_rows:
            best_rows = rows
            best = rd
    if best is None:
        raise RuntimeError(f"No valid manifest run under {dataset_root}")
    return best


def ensure(cond: bool, msg: str, errors: List[str]) -> None:
    if not cond:
        errors.append(msg)


def main() -> None:
    args = parse_args()
    errors: List[str] = []
    canonical_root = Path(args.canonical_root)
    db_path = Path(args.db_path)

    # Canonical manifests
    dataset_rows: Dict[str, int] = {}
    for ds in args.required_datasets:
        ds_root = canonical_root / ds
        ensure(ds_root.exists(), f"Missing dataset root: {ds_root}", errors)
        if not ds_root.exists():
            continue
        try:
            run_dir = latest_run_with_manifest(ds_root)
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            rows = int(manifest.get("total_rows", 0))
            dataset_rows[ds] = rows
            ensure(rows > 0, f"Dataset {ds} has non-positive rows in {run_dir}", errors)
        except Exception as exc:
            errors.append(f"Dataset {ds} manifest validation failed: {exc}")

    # Database tables
    ensure(db_path.exists(), f"Missing database: {db_path}", errors)
    table_counts: Dict[str, int] = {}
    fraud_score_rows_by_dataset: Dict[str, int] = {}
    alert_queue_distinct_queues = 0
    quality_metrics: Dict[str, int] = {}
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        required_tables = [
            "transaction_event_raw",
            "stg_transaction_event",
            "transaction_mart",
            "feature_payer_24h",
            "monitoring_mart",
            "fraud_scores",
            "alert_queue",
        ]
        for t in required_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                c = int(cur.fetchone()[0])
                table_counts[t] = c
                ensure(c > 0, f"Table {t} is empty", errors)
            except Exception as exc:
                errors.append(f"Table {t} check failed: {exc}")

        try:
            for ds, cnt in cur.execute("SELECT dataset_id, COUNT(*) FROM fraud_scores GROUP BY 1"):
                fraud_score_rows_by_dataset[str(ds)] = int(cnt)
            for ds in args.scoring_datasets:
                ensure(
                    fraud_score_rows_by_dataset.get(ds, 0) > 0,
                    f"fraud_scores missing/empty for dataset={ds}",
                    errors,
                )
        except Exception as exc:
            errors.append(f"fraud_scores dataset distribution check failed: {exc}")

        try:
            alert_queue_distinct_queues = int(cur.execute("SELECT COUNT(DISTINCT queue_id) FROM alert_queue").fetchone()[0])
            ensure(
                alert_queue_distinct_queues >= len(args.scoring_datasets),
                "alert_queue has too few distinct queues; check score->queue execution order",
                errors,
            )
        except Exception as exc:
            errors.append(f"alert_queue distinct queue check failed: {exc}")

        try:
            quality_metrics = {
                "null_event_id_transaction_mart": int(
                    cur.execute(
                        "SELECT COUNT(*) FROM transaction_mart WHERE event_id IS NULL OR TRIM(event_id) = ''"
                    ).fetchone()[0]
                ),
                "null_source_event_id_transaction_mart": int(
                    cur.execute(
                        "SELECT COUNT(*) FROM transaction_mart WHERE source_event_id IS NULL OR TRIM(CAST(source_event_id AS TEXT)) = ''"
                    ).fetchone()[0]
                ),
                "invalid_label_aml_transaction_mart": int(
                    cur.execute(
                        "SELECT COUNT(*) FROM transaction_mart WHERE label_aml IS NOT NULL AND label_aml NOT IN (0, 1)"
                    ).fetchone()[0]
                ),
                "shared_party_account_ids_transaction_mart": int(
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM transaction_mart
                        WHERE (
                          payer_party_id IS NOT NULL AND payer_account_id IS NOT NULL
                          AND TRIM(payer_party_id) <> '' AND TRIM(payer_account_id) <> ''
                          AND payer_party_id = payer_account_id
                        ) OR (
                          payee_party_id IS NOT NULL AND payee_account_id IS NOT NULL
                          AND TRIM(payee_party_id) <> '' AND TRIM(payee_account_id) <> ''
                          AND payee_party_id = payee_account_id
                        )
                        """
                    ).fetchone()[0]
                ),
            }
            for name, value in quality_metrics.items():
                ensure(value == 0, f"{name}: {value}", errors)
        except Exception as exc:
            errors.append(f"transaction_mart quality checks failed: {exc}")
        conn.close()

    # Model artifacts
    model_root = Path("artifacts/models/fraud_baseline")
    ensure(model_root.exists(), "Missing artifacts/models/fraud_baseline", errors)
    if model_root.exists():
        runs = [p for p in model_root.iterdir() if p.is_dir() and p.name != "latest"]
        ensure(len(runs) >= args.min_model_runs, f"Expected >= {args.min_model_runs} model runs, got {len(runs)}", errors)
        latest = model_root / "latest"
        ensure(latest.exists(), "Missing latest model pointer/folder", errors)
        if latest.exists():
            ensure((latest / "model.npz").exists(), "latest/model.npz missing", errors)
            ensure((latest / "metrics.json").exists(), "latest/metrics.json missing", errors)

    report = {
        "ok": len(errors) == 0,
        "dataset_rows": dataset_rows,
        "table_counts": table_counts,
        "fraud_scores_rows_by_dataset": fraud_score_rows_by_dataset,
        "alert_queue_distinct_queues": alert_queue_distinct_queues,
        "quality_metrics": quality_metrics,
        "errors": errors,
    }
    print(json.dumps(report, indent=2))

    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

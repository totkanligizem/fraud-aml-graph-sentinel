#!/usr/bin/env python3
"""
Build a local SQLite warehouse from canonical transaction_event CSV outputs.

Outputs tables:
- transaction_event_raw
- stg_transaction_event
- transaction_mart
- feature_payer_24h (point-in-time safe, limited base rows)
- monitoring_mart
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


DEFAULT_DATASETS = ["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local SQLite warehouse from canonical CSV parts.")
    parser.add_argument(
        "--canonical-root",
        default="data/curated/transaction_event",
        help="Canonical output root from ingest script",
    )
    parser.add_argument(
        "--db-path",
        default="data/warehouse/ledger_sentinel.db",
        help="SQLite database path",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=DEFAULT_DATASETS,
        help="Datasets to include (space separated)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Specific run id to use for all datasets. If omitted, latest run per dataset is used.",
    )
    parser.add_argument("--chunksize", type=int, default=100000, help="CSV read chunksize")
    parser.add_argument(
        "--max-rows-per-dataset",
        type=int,
        default=None,
        help="Optional cap for each dataset while loading into SQLite",
    )
    parser.add_argument(
        "--feature-base-limit",
        type=int,
        default=200000,
        help="Max base rows for feature_payer_24h table to control runtime",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop/recreate warehouse tables before loading",
    )
    return parser.parse_args()


def latest_dataset_run(canonical_root: Path, dataset_id: str) -> Path:
    ds_root = canonical_root / dataset_id
    if not ds_root.exists():
        raise FileNotFoundError(f"Dataset root not found: {ds_root}")
    run_dirs = sorted([p for p in ds_root.iterdir() if p.is_dir()])
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found under: {ds_root}")

    # Prefer the run with the largest row count (avoids accidentally picking smoke runs).
    best_run = run_dirs[-1]
    best_rows = -1
    for run_dir in run_dirs:
        manifest_path = run_dir / "manifest.json"
        rows = -1
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                rows = int(manifest.get("total_rows", -1))
            except Exception:
                rows = -1
        if rows > best_rows:
            best_rows = rows
            best_run = run_dir
        elif rows == best_rows and run_dir.name > best_run.name:
            best_run = run_dir
    return best_run


def resolve_dataset_run(canonical_root: Path, dataset_id: str, run_id: Optional[str]) -> Path:
    if run_id:
        run_dir = canonical_root / dataset_id / run_id
        if not run_dir.exists():
            raise FileNotFoundError(f"Run dir not found for dataset={dataset_id}: {run_dir}")
        return run_dir
    return latest_dataset_run(canonical_root, dataset_id)


def csv_parts(run_dir: Path) -> List[Path]:
    parts = sorted(run_dir.glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(f"No part files under: {run_dir}")
    return parts


def drop_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        DROP TABLE IF EXISTS monitoring_mart;
        DROP TABLE IF EXISTS feature_payer_24h;
        DROP TABLE IF EXISTS transaction_mart;
        DROP TABLE IF EXISTS stg_transaction_event;
        DROP TABLE IF EXISTS transaction_event_raw;
        """
    )
    conn.commit()


def load_raw_table(
    conn: sqlite3.Connection,
    dataset_id: str,
    run_dir: Path,
    chunksize: int,
    max_rows: Optional[int],
    replace_table: bool,
) -> int:
    loaded = 0
    first = replace_table
    parts = csv_parts(run_dir)

    for part in parts:
        for chunk in pd.read_csv(part, chunksize=chunksize):
            if max_rows is not None:
                remaining = max_rows - loaded
                if remaining <= 0:
                    return loaded
                if len(chunk) > remaining:
                    chunk = chunk.iloc[:remaining].copy()

            chunk.to_sql(
                "transaction_event_raw",
                conn,
                if_exists="replace" if first else "append",
                index=False,
                chunksize=1000,
            )
            first = False
            loaded += len(chunk)
        print(f"[LOAD] dataset={dataset_id} part={part.name} cumulative_rows={loaded}")
    return loaded


def build_staging_and_marts(conn: sqlite3.Connection, feature_base_limit: int) -> None:
    cur = conn.cursor()

    cur.executescript(
        """
        DROP TABLE IF EXISTS stg_transaction_event;
        CREATE TABLE stg_transaction_event AS
        SELECT
          event_id,
          CAST(source_event_id AS TEXT) AS source_event_id,
          dataset_id,
          event_time,
          event_time_grain,
          time_step,
          payer_party_id,
          payee_party_id,
          payer_account_id,
          payee_account_id,
          COALESCE(NULLIF(channel, ''), 'UNKNOWN') AS channel,
          COALESCE(NULLIF(txn_type, ''), 'UNKNOWN') AS txn_type,
          mcc_category,
          CAST(amount AS REAL) AS amount,
          COALESCE(NULLIF(currency, ''), 'USD') AS currency,
          CAST(fx_rate_to_usd AS REAL) AS fx_rate_to_usd,
          device_id,
          ip_prefix,
          email_domain,
          geo,
          CAST(label_fraud AS INTEGER) AS label_fraud,
          CAST(label_aml AS INTEGER) AS label_aml,
          label_source,
          pii_class,
          consent_class,
          retention_class,
          raw_partition,
          ingested_at,
          adapter_version
        FROM transaction_event_raw
        WHERE event_time IS NOT NULL;
        """
    )

    cur.executescript(
        """
        DROP TABLE IF EXISTS transaction_mart;
        CREATE TABLE transaction_mart AS
        SELECT
          *,
          date(event_time) AS event_date,
          strftime('%Y-%m-%dT%H:00:00', event_time) AS event_hour
        FROM stg_transaction_event;

        CREATE INDEX IF NOT EXISTS idx_tm_event_time ON transaction_mart(event_time);
        CREATE INDEX IF NOT EXISTS idx_tm_payer_event_time ON transaction_mart(payer_party_id, event_time);
        CREATE INDEX IF NOT EXISTS idx_tm_dataset ON transaction_mart(dataset_id);
        CREATE INDEX IF NOT EXISTS idx_tm_dataset_event_time ON transaction_mart(dataset_id, event_time);
        CREATE INDEX IF NOT EXISTS idx_tm_event_id ON transaction_mart(event_id);
        """
    )

    cur.executescript(
        f"""
        DROP TABLE IF EXISTS feature_payer_24h;
        CREATE TABLE feature_payer_24h AS
        WITH base AS (
          SELECT
            event_id,
            event_time,
            payer_party_id
          FROM transaction_mart
          WHERE payer_party_id IS NOT NULL
          ORDER BY event_time
          LIMIT {int(feature_base_limit)}
        )
        SELECT
          b.event_id,
          b.event_time,
          b.payer_party_id,
          (
            SELECT COUNT(*)
            FROM transaction_mart h
            WHERE h.payer_party_id = b.payer_party_id
              AND h.event_time < b.event_time
              AND h.event_time >= datetime(b.event_time, '-24 hours')
          ) AS payer_txn_count_24h,
          COALESCE((
            SELECT SUM(h.amount)
            FROM transaction_mart h
            WHERE h.payer_party_id = b.payer_party_id
              AND h.event_time < b.event_time
              AND h.event_time >= datetime(b.event_time, '-24 hours')
          ), 0.0) AS payer_amt_sum_24h
        FROM base b;

        CREATE INDEX IF NOT EXISTS idx_f24_event_id ON feature_payer_24h(event_id);
        CREATE INDEX IF NOT EXISTS idx_f24_payer_event_time ON feature_payer_24h(payer_party_id, event_time);
        """
    )

    cur.executescript(
        """
        DROP TABLE IF EXISTS monitoring_mart;
        CREATE TABLE monitoring_mart AS
        SELECT
          dataset_id,
          date(event_time) AS event_date,
          COUNT(*) AS txn_count,
          AVG(amount) AS avg_amount,
          SUM(CASE WHEN label_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
          SUM(CASE WHEN label_aml = 1 THEN 1 ELSE 0 END) AS aml_count
        FROM transaction_mart
        GROUP BY 1, 2;
        """
    )

    conn.commit()


def table_count(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cur.fetchone()[0])


def main() -> None:
    args = parse_args()
    canonical_root = Path(args.canonical_root)
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")

    if args.recreate:
        drop_tables(conn)

    run_map: Dict[str, str] = {}
    loaded_map: Dict[str, int] = {}
    first_dataset = True

    for dataset_id in args.datasets:
        run_dir = resolve_dataset_run(canonical_root, dataset_id, args.run_id)
        run_map[dataset_id] = run_dir.name
        print(f"[INFO] loading dataset={dataset_id} run={run_dir.name}")
        loaded = load_raw_table(
            conn=conn,
            dataset_id=dataset_id,
            run_dir=run_dir,
            chunksize=args.chunksize,
            max_rows=args.max_rows_per_dataset,
            replace_table=first_dataset,
        )
        first_dataset = False
        loaded_map[dataset_id] = loaded
        print(f"[DONE] dataset={dataset_id} loaded_rows={loaded}")

    print("[INFO] building staging/marts/features")
    build_staging_and_marts(conn, feature_base_limit=args.feature_base_limit)

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "datasets": loaded_map,
        "runs": run_map,
        "table_counts": {
            "transaction_event_raw": table_count(conn, "transaction_event_raw"),
            "stg_transaction_event": table_count(conn, "stg_transaction_event"),
            "transaction_mart": table_count(conn, "transaction_mart"),
            "feature_payer_24h": table_count(conn, "feature_payer_24h"),
            "monitoring_mart": table_count(conn, "monitoring_mart"),
        },
    }

    summary_path = db_path.parent / "warehouse-build-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[DONE] summary={summary_path}")
    print(json.dumps(summary["table_counts"], indent=2))
    conn.close()


if __name__ == "__main__":
    main()

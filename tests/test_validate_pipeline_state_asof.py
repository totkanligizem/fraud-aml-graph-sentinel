from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def create_canonical_manifests(root: Path) -> None:
    for dataset_id in ["ieee_cis", "creditcard_fraud", "paysim", "ibm_aml_data"]:
        run_dir = root / dataset_id / "20260305T000000Z"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").write_text(json.dumps({"total_rows": 1}), encoding="utf-8")


def create_minimal_db(db_path: Path, asof_offset_seconds: int) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("CREATE TABLE transaction_event_raw (dummy INTEGER)")
    cur.execute("INSERT INTO transaction_event_raw VALUES (1)")

    cur.execute("CREATE TABLE stg_transaction_event (dummy INTEGER)")
    cur.execute("INSERT INTO stg_transaction_event VALUES (1)")

    cur.execute(
        """
        CREATE TABLE transaction_mart (
          event_id TEXT,
          source_event_id TEXT,
          dataset_id TEXT,
          event_time TEXT,
          payer_party_id TEXT,
          payee_party_id TEXT,
          payer_account_id TEXT,
          payee_account_id TEXT,
          label_aml INTEGER,
          label_type TEXT
        )
        """
    )
    rows = [
        ("evt_ieee", "src_ieee", "ieee_cis", "2026-01-01T00:00:00Z", "pty1", "pty2", "acc1", "acc2", 0, "fraud"),
        (
            "evt_cc",
            "src_cc",
            "creditcard_fraud",
            "2026-01-01T00:05:00Z",
            "pty3",
            "pty4",
            "acc3",
            "acc4",
            0,
            "fraud",
        ),
        ("evt_ps", "src_ps", "paysim", "2026-01-01T00:10:00Z", "pty5", "pty6", "acc5", "acc6", 0, "fraud"),
        (
            "evt_aml",
            "src_aml",
            "ibm_aml_data",
            "2026-01-01T00:15:00Z",
            "pty7",
            "pty8",
            "acc7",
            "acc8",
            1,
            "aml",
        ),
    ]
    cur.executemany("INSERT INTO transaction_mart VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

    cur.execute(
        """
        CREATE TABLE feature_payer_24h (
          event_id TEXT,
          event_time TEXT,
          feature_asof_ts TEXT,
          payer_txn_count_24h INTEGER,
          payer_amt_sum_24h REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE feature_graph_24h (
          event_id TEXT,
          event_time TEXT,
          feature_asof_ts TEXT,
          graph_payer_incoming_txn_count_24h INTEGER,
          graph_payer_unique_payees_24h INTEGER,
          graph_pair_txn_count_30d INTEGER,
          graph_pair_amt_sum_30d REAL,
          graph_reciprocal_pair_txn_count_30d INTEGER
        )
        """
    )

    payer_asof = "2026-01-01T00:00:01Z" if asof_offset_seconds > 0 else "2025-12-31T23:59:59Z"
    graph_asof = "2026-01-01T00:00:01Z" if asof_offset_seconds > 0 else "2025-12-31T23:59:59Z"
    cur.execute("INSERT INTO feature_payer_24h VALUES (?, ?, ?, ?, ?)", ("evt_ieee", "2026-01-01T00:00:00Z", payer_asof, 0, 0.0))
    cur.execute("INSERT INTO feature_graph_24h VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("evt_ieee", "2026-01-01T00:00:00Z", graph_asof, 0, 0, 0, 0.0, 0))

    cur.execute("CREATE TABLE monitoring_mart (dummy INTEGER)")
    cur.execute("INSERT INTO monitoring_mart VALUES (1)")

    cur.execute("CREATE TABLE fraud_scores (dataset_id TEXT)")
    cur.executemany(
        "INSERT INTO fraud_scores VALUES (?)",
        [("ieee_cis",), ("creditcard_fraud",), ("paysim",)],
    )

    cur.execute("CREATE TABLE alert_queue (queue_id TEXT)")
    cur.executemany(
        "INSERT INTO alert_queue VALUES (?)",
        [("ieee_cis|2026-01-01",), ("creditcard_fraud|2026-01-01",), ("paysim|2026-01-01",)],
    )

    conn.commit()
    conn.close()


def run_validator(db_path: Path, canonical_root: Path) -> subprocess.CompletedProcess[str]:
    root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        str(root / "scripts" / "validate_pipeline_state.py"),
        "--db-path",
        str(db_path),
        "--canonical-root",
        str(canonical_root),
        "--min-model-runs",
        "0",
        "--min-feature-coverage-over-payer",
        "0",
        "--min-graph-feature-coverage-over-payer",
        "0",
    ]
    return subprocess.run(cmd, cwd=str(root), capture_output=True, text=True, check=False)


def test_validate_pipeline_state_fails_on_invalid_asof(tmp_path: Path) -> None:
    db_path = tmp_path / "bad.db"
    canonical_root = tmp_path / "canonical"
    create_canonical_manifests(canonical_root)
    create_minimal_db(db_path, asof_offset_seconds=1)

    result = run_validator(db_path, canonical_root)
    assert result.returncode != 0
    assert "feature_payer_24h_invalid_asof" in result.stdout


def test_validate_pipeline_state_passes_on_valid_asof(tmp_path: Path) -> None:
    db_path = tmp_path / "good.db"
    canonical_root = tmp_path / "canonical"
    create_canonical_manifests(canonical_root)
    create_minimal_db(db_path, asof_offset_seconds=-1)

    result = run_validator(db_path, canonical_root)
    assert result.returncode == 0
    assert '"ok": true' in result.stdout

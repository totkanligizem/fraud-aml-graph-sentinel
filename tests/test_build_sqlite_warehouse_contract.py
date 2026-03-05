from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "scripts"))

from build_sqlite_warehouse import build_staging_and_marts  # noqa: E402


def test_label_type_and_feature_asof_contract(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(db_path)

    rows = [
        {
            "event_id": "evt_1",
            "source_event_id": "src_1",
            "dataset_id": "ieee_cis",
            "event_time": "2026-01-01T00:00:00Z",
            "event_time_grain": "second",
            "time_step": 1,
            "payer_party_id": "pty_1",
            "payee_party_id": "pty_2",
            "payer_account_id": "acc_1",
            "payee_account_id": "acc_2",
            "channel": "card",
            "txn_type": "purchase",
            "mcc_category": "retail",
            "amount": 100.0,
            "currency": "USD",
            "fx_rate_to_usd": 1.0,
            "device_id": "dev_1",
            "ip_prefix": "10.0.0",
            "email_domain": "mail.com",
            "geo": "US",
            "label_fraud": 1,
            "label_aml": None,
            "label_source": "dataset",
            "pii_class": "masked",
            "consent_class": "synthetic",
            "retention_class": "standard",
            "raw_partition": "2026-01-01",
            "ingested_at": "2026-01-01T01:00:00Z",
            "adapter_version": "v1",
        },
        {
            "event_id": "evt_2",
            "source_event_id": "src_2",
            "dataset_id": "ieee_cis",
            "event_time": "2026-01-01T01:00:00Z",
            "event_time_grain": "second",
            "time_step": 2,
            "payer_party_id": "pty_1",
            "payee_party_id": "pty_3",
            "payer_account_id": "acc_1",
            "payee_account_id": "acc_3",
            "channel": "card",
            "txn_type": "purchase",
            "mcc_category": "retail",
            "amount": 120.0,
            "currency": "USD",
            "fx_rate_to_usd": 1.0,
            "device_id": "dev_1",
            "ip_prefix": "10.0.0",
            "email_domain": "mail.com",
            "geo": "US",
            "label_fraud": 0,
            "label_aml": None,
            "label_source": "dataset",
            "pii_class": "masked",
            "consent_class": "synthetic",
            "retention_class": "standard",
            "raw_partition": "2026-01-01",
            "ingested_at": "2026-01-01T01:00:00Z",
            "adapter_version": "v1",
        },
        {
            "event_id": "evt_3",
            "source_event_id": "src_3",
            "dataset_id": "ibm_aml_data",
            "event_time": "2026-01-01T02:00:00Z",
            "event_time_grain": "second",
            "time_step": 3,
            "payer_party_id": "pty_a",
            "payee_party_id": "pty_b",
            "payer_account_id": "acc_a",
            "payee_account_id": "acc_b",
            "channel": "wire",
            "txn_type": "transfer",
            "mcc_category": "finance",
            "amount": 240.0,
            "currency": "USD",
            "fx_rate_to_usd": 1.0,
            "device_id": "dev_2",
            "ip_prefix": "10.0.1",
            "email_domain": "mail.com",
            "geo": "US",
            "label_fraud": None,
            "label_aml": 1,
            "label_source": "dataset",
            "pii_class": "masked",
            "consent_class": "synthetic",
            "retention_class": "standard",
            "raw_partition": "2026-01-01",
            "ingested_at": "2026-01-01T01:00:00Z",
            "adapter_version": "v1",
        },
    ]

    pd.DataFrame(rows).to_sql("transaction_event_raw", conn, index=False, if_exists="replace")
    build_staging_and_marts(conn, feature_base_limit=100, feature_base_mode="full")

    label_types = {
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT label_type FROM transaction_mart ORDER BY label_type"
        ).fetchall()
    }
    assert label_types == {"aml", "fraud"}

    payer_features = conn.execute(
        """
        SELECT event_id, payer_txn_count_24h, feature_asof_ts <= event_time AS asof_ok
        FROM feature_payer_24h
        WHERE payer_party_id = 'pty_1'
        ORDER BY event_time
        """
    ).fetchall()
    assert len(payer_features) == 2
    assert payer_features[0][1] == 0
    assert payer_features[1][1] == 1
    assert all(int(row[2]) == 1 for row in payer_features)

    graph_asof_violations = conn.execute(
        "SELECT COUNT(*) FROM feature_graph_24h WHERE feature_asof_ts > event_time OR feature_asof_ts IS NULL"
    ).fetchone()[0]
    assert graph_asof_violations == 0

    conn.close()

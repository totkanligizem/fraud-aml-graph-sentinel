from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "scripts"))

from build_analyst_prompt_pack import (  # noqa: E402
    PAYLOAD_POLICY_VERSION,
    PROMPT_VERSION,
    build_packet_prompt,
    sanitize_case_packet,
)


def test_sanitize_case_packet_masks_sensitive_identifiers() -> None:
    packet = {
        "queue_id": "ieee_cis|2026-01-01",
        "dataset_id": "ieee_cis",
        "event_date": "2026-01-01",
        "queue_metrics": {
            "queue_rows": 50,
            "avg_queue_score": 0.88,
            "max_queue_score": 0.99,
            "positive_rows": 8,
            "aml_rows": 0,
            "avg_amount": 220.5,
            "amount_sum": 11025.0,
        },
        "surface": {"top_event_count": 5, "party_count_in_packet": 12, "account_count_in_packet": 18},
        "top_events": [
            {
                "rank_in_queue": 1,
                "fraud_score": 0.99,
                "label_fraud": 1,
                "event_id": "evt_plain",
                "source_event_id": "source_plain",
                "event_time": "2026-01-01T00:00:00Z",
                "event_date": "2026-01-01",
                "amount": 500.0,
                "txn_type": "purchase",
                "channel": "card",
                "email_domain": "private.com",
                "ip_prefix": "10.10.1",
            }
        ],
        "party_watchlist": [
            {
                "party_id": "party_plain",
                "risk_score": 0.9,
                "max_fraud_score": 0.99,
                "total_txn_count": 10,
                "fraud_event_count": 2,
                "aml_event_count": 0,
            }
        ],
        "cluster_watchlist": [
            {
                "cluster_id": "cluster_1",
                "matched_party_count": 2,
                "party_count": 5,
                "txn_count": 8,
                "max_fraud_score": 0.99,
                "max_edge_risk_score": 0.95,
            }
        ],
        "evidence_notes": ["note-1"],
    }

    sanitized = sanitize_case_packet(packet)
    assert sanitized["top_events"][0]["event_id"].startswith("evt_")
    assert sanitized["top_events"][0]["event_id"] != "evt_plain"
    assert sanitized["top_events"][0]["source_event_id"].startswith("src_")
    assert sanitized["party_watchlist"][0]["party_id"].startswith("pty_")
    assert "email_domain" not in json.dumps(sanitized)
    assert "ip_prefix" not in json.dumps(sanitized)


def test_build_packet_prompt_includes_governance_versions() -> None:
    packet = {
        "queue_id": "ieee_cis|2026-01-01",
        "dataset_id": "ieee_cis",
        "event_date": "2026-01-01",
        "queue_metrics": {},
        "surface": {},
        "top_events": [],
        "party_watchlist": [],
        "cluster_watchlist": [],
        "evidence_notes": [],
    }
    prompt_payload = build_packet_prompt(packet)
    assert prompt_payload["prompt_version"] == PROMPT_VERSION
    assert prompt_payload["payload_policy_version"] == PAYLOAD_POLICY_VERSION
    assert prompt_payload["masking_applied"] is True

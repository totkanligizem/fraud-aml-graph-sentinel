#!/usr/bin/env python3
"""Run Vertex AI Gemini analyst copilot prompts against the latest prompt pack."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from google import genai
from google.genai import types
from google.oauth2 import service_account


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROMPT_PACK_DIR = ROOT / "artifacts" / "agent" / "prompt_pack" / "latest"
DEFAULT_OUTPUT_ROOT = ROOT / "artifacts" / "agent" / "vertex_responses"
DEFAULT_VERTEX_LOCATION = "europe-west4"
DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

RISK_VALUES = {"low", "medium", "high"}
PRIORITY_VALUES = {"low", "medium", "high", "critical"}

SYSTEM_FALLBACK = (
    "You are a senior fraud and AML investigations analyst. "
    "Work only from supplied evidence and return compact audit-friendly JSON."
)


class ModelOutputError(RuntimeError):
    def __init__(self, message: str, raw_text: str = "") -> None:
        super().__init__(message)
        self.raw_text = raw_text

ANALYST_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "propertyOrdering": [
        "case_overview",
        "observed_signals",
        "investigation_hypotheses",
        "recommended_actions",
        "risk_assessment",
        "evidence_gaps",
    ],
    "required": [
        "case_overview",
        "observed_signals",
        "investigation_hypotheses",
        "recommended_actions",
        "risk_assessment",
        "evidence_gaps",
    ],
    "properties": {
        "case_overview": {"type": "string", "minLength": 1},
        "observed_signals": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
        "investigation_hypotheses": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["hypothesis", "confidence"],
                "properties": {
                    "hypothesis": {"type": "string", "minLength": 1},
                    "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
                },
            },
            "minItems": 1,
        },
        "recommended_actions": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
        "risk_assessment": {
            "type": "object",
            "additionalProperties": False,
            "required": ["fraud_risk", "aml_risk", "network_risk", "overall_priority"],
            "properties": {
                "fraud_risk": {"type": "string", "enum": ["low", "medium", "high"]},
                "aml_risk": {"type": "string", "enum": ["low", "medium", "high"]},
                "network_risk": {"type": "string", "enum": ["low", "medium", "high"]},
                "overall_priority": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
            },
        },
        "evidence_gaps": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        },
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Vertex Gemini over analyst prompt packs.")
    parser.add_argument("--prompt-pack-dir", default=str(DEFAULT_PROMPT_PACK_DIR), help="Prompt pack directory")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Vertex Gemini model name")
    parser.add_argument("--fallback-model", default="", help="Optional fallback model for parse/runtime-sensitive cases")
    parser.add_argument("--project-id", default="", help="GCP project id override")
    parser.add_argument("--location", default=DEFAULT_VERTEX_LOCATION, help="Vertex region")
    parser.add_argument("--credentials-path", default="", help="Service account JSON path override")
    parser.add_argument("--max-prompts", type=int, default=2, help="Maximum prompt packs to call")
    parser.add_argument("--temperature", type=float, default=0.15, help="Sampling temperature")
    parser.add_argument("--max-output-tokens", type=int, default=1400, help="Max output tokens")
    parser.add_argument(
        "--selection-strategy",
        choices=["sorted", "round_robin_dataset"],
        default="sorted",
        help="Prompt selection strategy when max-prompts is less than available prompts",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=0.0,
        help="Sleep between prompt calls to reduce quota pressure",
    )
    parser.add_argument(
        "--skip-latest-on-error",
        action="store_true",
        help="Do not promote this run into latest/ if any runtime or validation errors occurred",
    )
    parser.add_argument(
        "--disable-deterministic-fallback",
        action="store_true",
        help="Disable deterministic synthesis fallback when model output cannot be parsed.",
    )
    return parser.parse_args()


def load_env_file(env_path: Path = Path(".env.local")) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ[key] = value


def resolve_credentials_path(explicit: str) -> Path:
    candidates = [
        explicit,
        os.environ.get("VERTEX_AGENT_CREDENTIALS", ""),
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        str(ROOT / "api keys" / "fraud-aml-graph-.json"),
        str(ROOT / "api keys" / "fraud-aml-graph.json"),
        str(ROOT / ".secrets" / "gcp-service-account.json"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return Path(candidate)
    raise FileNotFoundError("No usable Vertex credentials path found.")


def prompt_dataset_id(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        dataset_id = str(payload.get("dataset_id", "")).strip()
        if dataset_id:
            return dataset_id
    except Exception:
        pass
    return path.stem.split("__", 1)[0]


def select_prompt_files_round_robin(files: List[Path], max_prompts: int) -> List[Path]:
    grouped: Dict[str, List[Path]] = {}
    dataset_order: List[str] = []
    for path in files:
        dataset_id = prompt_dataset_id(path)
        if dataset_id not in grouped:
            grouped[dataset_id] = []
            dataset_order.append(dataset_id)
        grouped[dataset_id].append(path)

    selected: List[Path] = []
    index = 0
    while len(selected) < max_prompts:
        added_any = False
        for dataset_id in dataset_order:
            candidates = grouped[dataset_id]
            if index < len(candidates):
                selected.append(candidates[index])
                added_any = True
                if len(selected) >= max_prompts:
                    break
        if not added_any:
            break
        index += 1
    return selected


def load_prompt_files(prompt_pack_dir: Path, max_prompts: int, selection_strategy: str) -> List[Path]:
    if not prompt_pack_dir.exists():
        raise FileNotFoundError(f"Prompt pack dir not found: {prompt_pack_dir}")
    files = sorted(path for path in prompt_pack_dir.glob("*.json") if path.name != "prompt-pack-summary.json")
    if not files:
        raise RuntimeError(f"No prompt pack JSON files found in: {prompt_pack_dir}")
    limit = max(1, int(max_prompts))
    if selection_strategy == "round_robin_dataset":
        return select_prompt_files_round_robin(files, limit)
    return files[:limit]


def extract_text(response: Any) -> str:
    primary_text = str(getattr(response, "text", "") or "")
    candidates = getattr(response, "candidates", None) or []
    parts_text: List[str] = []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None) or []
        for part in parts:
            part_text = getattr(part, "text", None)
            if part_text:
                parts_text.append(str(part_text))
    joined_parts = "".join(parts_text).strip()
    if primary_text.strip() and joined_parts:
        return joined_parts if len(joined_parts) >= len(primary_text.strip()) else primary_text.strip()
    if primary_text.strip():
        return primary_text.strip()
    if joined_parts:
        return joined_parts
    raise RuntimeError("Vertex response did not contain text.")


def extract_text_or_empty(response: Any) -> str:
    try:
        return extract_text(response)
    except RuntimeError:
        return ""


def extract_json_payload(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start : end + 1])
        raise


def validate_model_output(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required_keys = {
        "case_overview",
        "observed_signals",
        "investigation_hypotheses",
        "recommended_actions",
        "risk_assessment",
        "evidence_gaps",
    }
    missing = required_keys - payload.keys()
    if missing:
        errors.append(f"missing keys: {', '.join(sorted(missing))}")

    if not isinstance(payload.get("case_overview"), str) or not payload.get("case_overview", "").strip():
        errors.append("case_overview must be a non-empty string")

    for key in ["observed_signals", "investigation_hypotheses", "recommended_actions", "evidence_gaps"]:
        if not isinstance(payload.get(key), list):
            errors.append(f"{key} must be a list")

    risk = payload.get("risk_assessment", {})
    if not isinstance(risk, dict):
        errors.append("risk_assessment must be an object")
    else:
        if risk.get("fraud_risk") not in RISK_VALUES:
            errors.append("fraud_risk invalid")
        if risk.get("aml_risk") not in RISK_VALUES:
            errors.append("aml_risk invalid")
        if risk.get("network_risk") not in RISK_VALUES:
            errors.append("network_risk invalid")
        if risk.get("overall_priority") not in PRIORITY_VALUES:
            errors.append("overall_priority invalid")
    return errors


def build_config(
    system_prompt: str,
    temperature: float,
    max_output_tokens: int,
    *,
    strict_schema: bool,
) -> types.GenerateContentConfig:
    base_kwargs: Dict[str, Any] = {
        "system_instruction": (system_prompt or SYSTEM_FALLBACK)
        + " Return valid JSON only. Do not wrap JSON in markdown or prose.",
        "temperature": float(temperature),
        "max_output_tokens": int(max_output_tokens),
        "response_mime_type": "application/json",
        "seed": 7,
    }
    if strict_schema:
        base_kwargs["response_json_schema"] = ANALYST_RESPONSE_SCHEMA
    return types.GenerateContentConfig(
        **base_kwargs,
    )


def safe_stem(queue_id: str) -> str:
    return queue_id.replace("|", "__").replace("/", "_")


def compact_event(event: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "rank_in_queue",
        "fraud_score",
        "label_fraud",
        "event_id",
        "source_event_id",
        "event_time",
        "event_date",
        "amount",
        "txn_type",
    ]
    return {key: event.get(key) for key in keys}


def compact_party_watchlist(item: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "party_id",
        "risk_score",
        "max_fraud_score",
        "total_txn_count",
        "fraud_event_count",
        "aml_event_count",
    ]
    return {key: item.get(key) for key in keys}


def compact_cluster_watchlist(item: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "cluster_id",
        "matched_party_count",
        "party_count",
        "txn_count",
        "max_fraud_score",
        "max_edge_risk_score",
    ]
    return {key: item.get(key) for key in keys}


def build_compact_prompt_payload(prompt_payload: Dict[str, Any]) -> Dict[str, Any]:
    messages = prompt_payload.get("messages", [])
    if len(messages) < 2:
        return prompt_payload
    user_content = str(messages[1].get("content", ""))
    try:
        payload = json.loads(user_content)
    except json.JSONDecodeError:
        return prompt_payload

    case_packet = payload.get("case_packet", {})
    top_events = case_packet.get("top_events", []) if isinstance(case_packet.get("top_events"), list) else []
    party_watchlist = case_packet.get("party_watchlist", []) if isinstance(case_packet.get("party_watchlist"), list) else []
    cluster_watchlist = case_packet.get("cluster_watchlist", []) if isinstance(case_packet.get("cluster_watchlist"), list) else []

    metrics = case_packet.get("queue_metrics", {}) if isinstance(case_packet.get("queue_metrics"), dict) else {}
    surface = case_packet.get("surface", {}) if isinstance(case_packet.get("surface"), dict) else {}
    compact_packet = {
        "queue_id": case_packet.get("queue_id"),
        "dataset_id": case_packet.get("dataset_id"),
        "event_date": case_packet.get("event_date"),
        "queue_metrics": {
            "queue_rows": metrics.get("queue_rows"),
            "avg_queue_score": metrics.get("avg_queue_score"),
            "max_queue_score": metrics.get("max_queue_score"),
            "positive_rows": metrics.get("positive_rows"),
            "aml_rows": metrics.get("aml_rows"),
            "avg_amount": metrics.get("avg_amount"),
            "amount_sum": metrics.get("amount_sum"),
        },
        "surface": {
            "top_event_count": surface.get("top_event_count"),
            "party_count_in_packet": surface.get("party_count_in_packet"),
            "account_count_in_packet": surface.get("account_count_in_packet"),
        },
        "top_events": [compact_event(item) for item in top_events[:3] if isinstance(item, dict)],
        "party_watchlist": [compact_party_watchlist(item) for item in party_watchlist[:3] if isinstance(item, dict)],
        "cluster_watchlist": [compact_cluster_watchlist(item) for item in cluster_watchlist[:2] if isinstance(item, dict)],
        "evidence_notes": case_packet.get("evidence_notes", [])[:3],
    }

    compact_user_payload = {
        "task": "Return a compact investigation summary as valid JSON only.",
        "response_rules": {
            "keys": [
                "case_overview",
                "observed_signals",
                "investigation_hypotheses",
                "recommended_actions",
                "risk_assessment",
                "evidence_gaps",
            ],
            "limits": {
                "case_overview_max_words": 80,
                "observed_signals_max_items": 8,
                "investigation_hypotheses_max_items": 3,
                "recommended_actions_max_items": 4,
                "evidence_gaps_max_items": 4,
            },
            "risk_values": {
                "fraud_risk": ["low", "medium", "high"],
                "aml_risk": ["low", "medium", "high"],
                "network_risk": ["low", "medium", "high"],
                "overall_priority": ["low", "medium", "high", "critical"],
            },
        },
        "case_packet": compact_packet,
    }
    compact_messages = [dict(messages[0]), dict(messages[1])]
    compact_messages[1]["content"] = json.dumps(compact_user_payload, ensure_ascii=False, separators=(",", ":"))
    compact_payload = dict(prompt_payload)
    compact_payload["messages"] = compact_messages
    return compact_payload


def run_model_call(
    client: genai.Client,
    model_name: str,
    prompt_payload: Dict[str, Any],
    temperature: float,
    max_output_tokens: int,
    strict_schema: bool = True,
) -> Tuple[str, Dict[str, Any] | None]:
    system_prompt = prompt_payload["messages"][0]["content"]
    user_prompt = prompt_payload["messages"][1]["content"]
    response = client.models.generate_content(
        model=model_name,
        contents=user_prompt,
        config=build_config(system_prompt, temperature, max_output_tokens, strict_schema=strict_schema),
    )
    raw_text = extract_text_or_empty(response)
    parsed = getattr(response, "parsed", None)
    if hasattr(parsed, "model_dump"):
        parsed = parsed.model_dump()
    if isinstance(parsed, dict):
        return raw_text, parsed
    if raw_text:
        try:
            return raw_text, extract_json_payload(raw_text)
        except json.JSONDecodeError as exc:
            raise ModelOutputError(f"{exc.__class__.__name__}: {exc}", raw_text=raw_text) from exc
    raise ModelOutputError("Vertex response did not contain parseable content.", raw_text=raw_text)


def summarize_response(raw_text: str, parsed: Dict[str, Any], errors: List[str]) -> Dict[str, Any]:
    risk = parsed.get("risk_assessment", {}) if isinstance(parsed, dict) else {}
    return {
        "response_chars": len(raw_text),
        "observed_signal_count": len(parsed.get("observed_signals", [])) if isinstance(parsed.get("observed_signals"), list) else 0,
        "hypothesis_count": len(parsed.get("investigation_hypotheses", [])) if isinstance(parsed.get("investigation_hypotheses"), list) else 0,
        "action_count": len(parsed.get("recommended_actions", [])) if isinstance(parsed.get("recommended_actions"), list) else 0,
        "overall_priority": risk.get("overall_priority"),
        "validation_errors": errors,
    }


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def derive_risk_assessment(packet: Dict[str, Any]) -> Dict[str, str]:
    metrics = packet.get("queue_metrics", {}) if isinstance(packet.get("queue_metrics"), dict) else {}
    avg_score = to_float(metrics.get("avg_queue_score"))
    max_score = to_float(metrics.get("max_queue_score"))
    positive_rows = to_int(metrics.get("positive_rows"))
    aml_rows = to_int(metrics.get("aml_rows"))
    fraud_risk = "low"
    if avg_score >= 0.70 or max_score >= 0.85:
        fraud_risk = "high"
    elif avg_score >= 0.35 or max_score >= 0.60 or positive_rows > 0:
        fraud_risk = "medium"

    aml_risk = "low"
    if aml_rows > 0:
        aml_risk = "high" if aml_rows >= 10 else "medium"

    cluster_watchlist = packet.get("cluster_watchlist", [])
    network_risk = "low"
    if isinstance(cluster_watchlist, list) and cluster_watchlist:
        max_edge_risk = max(to_float(item.get("max_edge_risk_score")) for item in cluster_watchlist if isinstance(item, dict))
        if max_edge_risk >= 0.80:
            network_risk = "high"
        elif max_edge_risk >= 0.50:
            network_risk = "medium"

    overall_priority = "medium"
    if fraud_risk == "high" or aml_risk == "high" or network_risk == "high":
        overall_priority = "high"
    if aml_risk == "high" and (fraud_risk == "high" or network_risk == "high"):
        overall_priority = "critical"
    if fraud_risk == "low" and aml_risk == "low" and network_risk == "low":
        overall_priority = "low"

    return {
        "fraud_risk": fraud_risk,
        "aml_risk": aml_risk,
        "network_risk": network_risk,
        "overall_priority": overall_priority,
    }


def build_deterministic_output(prompt_payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        user_payload = json.loads(prompt_payload["messages"][1]["content"])
    except Exception:
        user_payload = {}
    packet = user_payload.get("case_packet", {}) if isinstance(user_payload, dict) else {}
    metrics = packet.get("queue_metrics", {}) if isinstance(packet.get("queue_metrics"), dict) else {}
    top_events = packet.get("top_events", []) if isinstance(packet.get("top_events"), list) else []
    party_watchlist = packet.get("party_watchlist", []) if isinstance(packet.get("party_watchlist"), list) else []
    cluster_watchlist = packet.get("cluster_watchlist", []) if isinstance(packet.get("cluster_watchlist"), list) else []
    queue_id = str(packet.get("queue_id") or prompt_payload.get("queue_id") or "unknown_queue")
    dataset_id = str(packet.get("dataset_id") or prompt_payload.get("dataset_id") or "unknown_dataset")
    event_date = str(packet.get("event_date") or "unknown_date")
    queue_rows = to_int(metrics.get("queue_rows"))
    avg_score = to_float(metrics.get("avg_queue_score"))
    max_score = to_float(metrics.get("max_queue_score"))
    positive_rows = to_int(metrics.get("positive_rows"))
    aml_rows = to_int(metrics.get("aml_rows"))
    risk = derive_risk_assessment(packet)

    observed_signals: List[str] = [
        f"Queue {queue_id} ({dataset_id}, {event_date}) has {queue_rows} events.",
        f"Average fraud score {avg_score:.3f}, max fraud score {max_score:.3f}.",
        f"Observed positive fraud labels: {positive_rows}; AML labels: {aml_rows}.",
    ]
    if top_events:
        top = top_events[0] if isinstance(top_events[0], dict) else {}
        observed_signals.append(
            f"Top ranked event {top.get('event_id', 'unknown')} scored {to_float(top.get('fraud_score')):.3f}."
        )
    if party_watchlist:
        observed_signals.append(f"Party watchlist candidates in packet: {len(party_watchlist)}.")
    if cluster_watchlist:
        observed_signals.append(f"Cluster watchlist candidates in packet: {len(cluster_watchlist)}.")

    hypotheses = [
        {
            "hypothesis": "The queue is likely driven by recurrent high-risk behavioral patterns that require investigator triage.",
            "confidence": "high" if risk["fraud_risk"] == "high" else "medium",
        },
        {
            "hypothesis": "Network concentration around top parties/clusters may indicate coordinated or repeated transaction behavior.",
            "confidence": "medium" if risk["network_risk"] in {"medium", "high"} else "low",
        },
    ]
    if aml_rows > 0:
        hypotheses.append(
            {
                "hypothesis": "AML-labeled activity exists and should be traced with counterparty chain analysis.",
                "confidence": "medium",
            }
        )

    recommended_actions = [
        "Review top-ranked events with full transaction lineage and linked accounts.",
        "Prioritize parties with repeated high-risk signals for enhanced due diligence.",
        "Run temporal and counterparty concentration checks for potential structuring patterns.",
        "Escalate to manual investigation if additional corroborating signals emerge.",
    ]

    evidence_notes = packet.get("evidence_notes", []) if isinstance(packet.get("evidence_notes"), list) else []
    evidence_gaps = [
        "Beneficial ownership and customer KYC context are not included in the case packet.",
        "External sanctions/adverse media enrichments are not present in this run.",
    ]
    if not evidence_notes:
        evidence_gaps.append("Case packet evidence notes are limited; add investigator annotations.")

    case_overview = (
        f"Queue {queue_id} from {dataset_id} on {event_date} contains {queue_rows} events "
        f"with average score {avg_score:.3f} and peak score {max_score:.3f}. "
        f"Current signal mix indicates {risk['overall_priority']} investigation priority."
    )

    payload = {
        "case_overview": case_overview,
        "observed_signals": observed_signals[:8],
        "investigation_hypotheses": hypotheses[:3],
        "recommended_actions": recommended_actions[:4],
        "risk_assessment": risk,
        "evidence_gaps": evidence_gaps[:4],
    }
    if validate_model_output(payload):
        # Last-resort safe schema payload.
        return {
            "case_overview": case_overview or f"Queue {queue_id} requires manual review.",
            "observed_signals": [f"Queue {queue_id} generated analyst fallback synthesis."],
            "investigation_hypotheses": [{"hypothesis": "Insufficient model output; apply manual triage.", "confidence": "low"}],
            "recommended_actions": ["Escalate queue for manual investigator review."],
            "risk_assessment": {
                "fraud_risk": "medium",
                "aml_risk": "low",
                "network_risk": "low",
                "overall_priority": "medium",
            },
            "evidence_gaps": ["Primary LLM response was incomplete; use deterministic fallback output."],
        }
    return payload


def is_retryable_exception(exc: Exception) -> bool:
    message = f"{exc.__class__.__name__}: {exc}".lower()
    retry_markers = [
        "429",
        "resource_exhausted",
        "rate limit",
        "quota",
        "unavailable",
        "deadline_exceeded",
        "internal",
    ]
    return any(marker in message for marker in retry_markers)


def run_model_call_with_retry(
    client: genai.Client,
    model_name: str,
    fallback_model_name: str,
    prompt_payload: Dict[str, Any],
    temperature: float,
    max_output_tokens: int,
    max_attempts: int = 3,
) -> Tuple[str, Dict[str, Any], str]:
    last_error: Exception | None = None
    last_raw_text = ""
    for attempt in range(1, max_attempts + 1):
        try:
            raw_text, parsed = run_model_call(
                client=client,
                model_name=model_name,
                prompt_payload=prompt_payload,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
            last_raw_text = raw_text
            if parsed:
                return raw_text, parsed, model_name
            retry_text, retry_parsed = run_model_call(
                client=client,
                model_name=model_name,
                prompt_payload=prompt_payload,
                temperature=0.0,
                max_output_tokens=max_output_tokens,
            )
            last_raw_text = retry_text
            if retry_parsed:
                return retry_text, retry_parsed, model_name
            if fallback_model_name and fallback_model_name != model_name:
                fallback_text, fallback_parsed = run_model_call(
                    client=client,
                    model_name=fallback_model_name,
                    prompt_payload=prompt_payload,
                    temperature=0.0,
                    max_output_tokens=max_output_tokens,
                )
                last_raw_text = fallback_text
                if fallback_parsed:
                    return fallback_text, fallback_parsed, fallback_model_name
                compact_payload = build_compact_prompt_payload(prompt_payload)
                if compact_payload is not prompt_payload:
                    fallback_compact_text, fallback_compact_parsed = run_model_call(
                        client=client,
                        model_name=fallback_model_name,
                        prompt_payload=compact_payload,
                        temperature=0.0,
                        max_output_tokens=max_output_tokens,
                        strict_schema=False,
                    )
                    last_raw_text = fallback_compact_text
                    if fallback_compact_parsed:
                        return fallback_compact_text, fallback_compact_parsed, fallback_model_name
            raise ModelOutputError("Vertex response could not be parsed into the analyst schema.", raw_text=last_raw_text)
        except Exception as exc:
            if isinstance(exc, ModelOutputError) and fallback_model_name and fallback_model_name != model_name:
                try:
                    fallback_text, fallback_parsed = run_model_call(
                        client=client,
                        model_name=fallback_model_name,
                        prompt_payload=prompt_payload,
                        temperature=0.0,
                        max_output_tokens=max_output_tokens,
                    )
                    last_raw_text = fallback_text
                    if fallback_parsed:
                        return fallback_text, fallback_parsed, fallback_model_name
                    compact_payload = build_compact_prompt_payload(prompt_payload)
                    if compact_payload is not prompt_payload:
                        fallback_compact_text, fallback_compact_parsed = run_model_call(
                            client=client,
                            model_name=fallback_model_name,
                            prompt_payload=compact_payload,
                            temperature=0.0,
                            max_output_tokens=max_output_tokens,
                            strict_schema=False,
                        )
                        last_raw_text = fallback_compact_text
                        if fallback_compact_parsed:
                            return fallback_compact_text, fallback_compact_parsed, fallback_model_name
                except Exception as fallback_exc:
                    if isinstance(fallback_exc, ModelOutputError):
                        last_raw_text = fallback_exc.raw_text or last_raw_text
                    exc = fallback_exc
            last_error = exc
            if attempt < max_attempts and (is_retryable_exception(exc) or isinstance(exc, (json.JSONDecodeError, ModelOutputError))):
                time.sleep(4 * attempt)
                continue
            if isinstance(exc, ModelOutputError):
                raise ModelOutputError(str(exc), raw_text=exc.raw_text or last_raw_text)
            raise
    assert last_error is not None
    if isinstance(last_error, ModelOutputError):
        raise ModelOutputError(str(last_error), raw_text=last_error.raw_text or last_raw_text)
    raise last_error


def main() -> None:
    load_env_file()
    args = parse_args()

    prompt_pack_dir = Path(args.prompt_pack_dir)
    output_root = Path(args.output_root)
    creds_path = resolve_credentials_path(args.credentials_path)
    credentials = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=DEFAULT_SCOPES,
    )

    project_id = args.project_id or os.environ.get("GCP_PROJECT_ID", "").strip()
    if not project_id:
        raise RuntimeError("Missing GCP project id. Set --project-id or GCP_PROJECT_ID.")

    prompt_files = load_prompt_files(prompt_pack_dir, args.max_prompts, args.selection_strategy)
    client = genai.Client(
        vertexai=True,
        credentials=credentials,
        project=project_id,
        location=args.location,
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []
    for index, path in enumerate(prompt_files, start=1):
        prompt_payload = json.loads(path.read_text(encoding="utf-8"))
        queue_id = str(prompt_payload["queue_id"])
        stem = safe_stem(queue_id)
        parsed: Dict[str, Any] = {}
        raw_text = ""
        validation_errors: List[str] = []
        runtime_error = ""
        model_used = args.model

        try:
            raw_text, parsed, model_used = run_model_call_with_retry(
                client=client,
                model_name=args.model,
                fallback_model_name=args.fallback_model,
                prompt_payload=prompt_payload,
                temperature=args.temperature,
                max_output_tokens=args.max_output_tokens,
            )
            validation_errors = validate_model_output(parsed)
        except Exception as exc:
            if isinstance(exc, ModelOutputError):
                raw_text = exc.raw_text
            if not args.disable_deterministic_fallback:
                parsed = build_deterministic_output(prompt_payload)
                validation_errors = validate_model_output(parsed)
                if not validation_errors:
                    model_used = f"{args.model}+deterministic_fallback"
                    runtime_error = ""
                    raw_text = raw_text or json.dumps(parsed, ensure_ascii=False)
                else:
                    runtime_error = f"{exc.__class__.__name__}: {exc}"
            else:
                runtime_error = f"{exc.__class__.__name__}: {exc}"

        (out_dir / f"{stem}.raw.txt").write_text(raw_text, encoding="utf-8")
        if parsed:
            (out_dir / f"{stem}.json").write_text(json.dumps(parsed, indent=2, ensure_ascii=False), encoding="utf-8")

        item = {
            "queue_id": queue_id,
            "dataset_id": prompt_payload["dataset_id"],
            "model": model_used,
            "output_file": f"{stem}.json" if parsed else "",
            "raw_file": f"{stem}.raw.txt",
            "runtime_error": runtime_error,
            **summarize_response(raw_text, parsed, validation_errors),
        }
        results.append(item)
        if float(args.request_delay_seconds) > 0 and index < len(prompt_files):
            time.sleep(float(args.request_delay_seconds))

    error_count = sum(1 for item in results if item["runtime_error"] or item["validation_errors"])
    try:
        prompt_pack_dir_public = str(prompt_pack_dir.relative_to(ROOT))
    except ValueError:
        prompt_pack_dir_public = prompt_pack_dir.name

    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": run_id,
        "project_id": project_id,
        "location": args.location,
        "model": args.model,
        "fallback_model": args.fallback_model,
        "runtime_identity": "service_account_json",
        "prompt_pack_dir": prompt_pack_dir_public,
        "selection_strategy": args.selection_strategy,
        "request_delay_seconds": float(args.request_delay_seconds),
        "response_count": len(results),
        "error_count": error_count,
        "results": results,
    }

    promoted_to_latest = True
    if args.skip_latest_on_error and error_count > 0:
        promoted_to_latest = False
    summary["promoted_to_latest"] = promoted_to_latest
    (out_dir / "run-summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    last_dir = output_root / "last"
    last_dir.mkdir(parents=True, exist_ok=True)
    for stale_file in last_dir.glob("*"):
        if stale_file.is_file():
            stale_file.unlink()
    for file in out_dir.iterdir():
        if file.is_file():
            (last_dir / file.name).write_text(file.read_text(encoding="utf-8"), encoding="utf-8")

    latest_dir = output_root / "latest"

    if promoted_to_latest:
        latest_dir.mkdir(parents=True, exist_ok=True)
        for stale_file in latest_dir.glob("*"):
            if stale_file.is_file():
                stale_file.unlink()
        for file in out_dir.iterdir():
            if file.is_file():
                (latest_dir / file.name).write_text(file.read_text(encoding="utf-8"), encoding="utf-8")

    print(
        json.dumps(
            {"output_dir": str(out_dir), "last_dir": str(last_dir), "latest_dir": str(latest_dir), **summary},
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

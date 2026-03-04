#!/usr/bin/env python3
"""Generate the EN version of the master final report from the master snapshot."""

from __future__ import annotations

import json
import os
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

MPLCONFIGDIR = Path("artifacts/mplconfig")
XDG_CACHE_HOME = Path("artifacts/xdg-cache")
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
XDG_CACHE_HOME.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR.resolve()))
os.environ.setdefault("XDG_CACHE_HOME", str(XDG_CACHE_HOME.resolve()))

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / "reports"
MASTER_SNAPSHOT_PATH = REPORTS_DIR / "07_Master_Final_Rapor_TR_Snapshot.json"


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt_int(value: Any) -> str:
    return f"{int(value):,}"


def fmt_float(value: Any, digits: int = 4) -> str:
    return f"{float(value):.{digits}f}"


def pct(value: Any, digits: int = 2) -> str:
    return f"{float(value) * 100:.{digits}f}%"


def wrap_lines(lines: Iterable[str], width: int = 96) -> List[str]:
    out: List[str] = []
    for line in lines:
        if not line:
            out.append("")
            continue
        out.extend(textwrap.wrap(line, width=width, break_long_words=False, replace_whitespace=False))
    return out


def paginate_lines(lines: List[str], max_lines: int = 46) -> List[List[str]]:
    pages: List[List[str]] = []
    cur: List[str] = []
    for line in lines:
        cur.append(line)
        if len(cur) >= max_lines:
            pages.append(cur)
            cur = []
    if cur:
        pages.append(cur)
    return pages


def add_text_pages(pdf: PdfPages, title: str, lines: List[str], subtitle: str = "") -> None:
    paged = paginate_lines(wrap_lines(lines))
    for page_no, page_lines in enumerate(paged, start=1):
        fig = plt.figure(figsize=(8.27, 11.69))
        ax = fig.add_axes([0, 0, 1, 1])
        ax.axis("off")
        ax.text(0.05, 0.965, title, fontsize=20, fontweight="bold", va="top", family="DejaVu Sans")
        if subtitle:
            ax.text(0.05, 0.935, subtitle, fontsize=10, color="#555555", va="top", family="DejaVu Sans")
        ax.text(
            0.05,
            0.90,
            "\n".join(page_lines),
            fontsize=10.2,
            va="top",
            family="DejaVu Sans Mono",
            linespacing=1.34,
        )
        ax.text(0.95, 0.03, f"Page {page_no}/{len(paged)}", fontsize=9, ha="right", color="#666666")
        pdf.savefig(fig)
        plt.close(fig)


def build_markdown(report: Dict[str, Any]) -> str:
    project = report["project"]
    snapshot = report["snapshot"]
    dashboard_validate = report["dashboard_validate"]
    vertex = report["vertex"]
    bq = report["bigquery_state"]
    sql_runs = report["sql_runs"]
    derived = report["derived"]
    warehouse_counts = snapshot["warehouse_summary"]["table_counts"]
    graph_counts = snapshot["graph_summary"]["table_counts"]
    ranking = snapshot["ranking_summary"]
    model = snapshot["model_metrics"]
    scored_by_dataset = snapshot["scoring_summary"]["rows_by_dataset"]

    lines: List[str] = [
        f"# {project['name']} | Master Final Report (EN)",
        "",
        f"Generated at (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "",
        "## 1. Executive Summary",
        f"- Publish readiness: **{'READY FOR PUBLISH' if derived['publish_ready'] else 'NOT READY'}**",
        f"- Dashboard quality gate: `{dashboard_validate['ok']}`",
        f"- BigQuery state gate: `{bq['ok']}`",
        f"- Vertex analyst gate: `error_count={vertex['error_count']}`",
        f"- Executive invalid checks zero: `{derived['exec_invalid_zero']}`",
        f"- Analyst defect checks zero: `{derived['analyst_defects_zero']}`",
        "",
        "## 2. Project Identity and Scope",
        f"- Project name: {project['name']}",
        f"- Project id: {project['project_id']}",
        f"- BigQuery dataset: {project['dataset_id']} ({project['location']})",
        "- Scope: Canonical ingestion, local warehouse, baseline model, ranking, graph layer, BigQuery mirror, dashboard, analyst copilot",
        "",
        "## 3. End-to-End Workflow (Mermaid)",
        report["flow_mermaid"],
        "",
        "## 4. Data Inventory and Active Volumes",
        f"- transaction_event_raw: {fmt_int(warehouse_counts['transaction_event_raw'])}",
        f"- stg_transaction_event: {fmt_int(warehouse_counts['stg_transaction_event'])}",
        f"- transaction_mart: {fmt_int(warehouse_counts['transaction_mart'])}",
        f"- feature_payer_24h: {fmt_int(warehouse_counts['feature_payer_24h'])}",
        f"- monitoring_mart: {fmt_int(warehouse_counts['monitoring_mart'])}",
        f"- fraud_scores total: {fmt_int(snapshot['scoring_summary']['total_scored_rows'])}",
        f"- distinct alert queues: {fmt_int(ranking['queue_count'])}",
        f"- scored rows by dataset: creditcard={fmt_int(scored_by_dataset['creditcard_fraud'])}, ieee={fmt_int(scored_by_dataset['ieee_cis'])}, paysim={fmt_int(scored_by_dataset['paysim'])}",
        "",
        "## 5. Model and Ranking Metrics",
        f"- average_precision: {fmt_float(model['metrics']['average_precision'])}",
        f"- pr_auc_trapz: {fmt_float(model['metrics']['pr_auc_trapz'])}",
        f"- cost_optimized_threshold: {fmt_float(model['cost_optimized_threshold']['threshold'], 6)}",
        f"- mean_precision_at_k: {pct(ranking['mean_precision_at_k'])}",
        f"- mean_ndcg_at_k: {pct(ranking['mean_ndcg_at_k'])}",
        f"- queues_with_positive_labels: {fmt_int(ranking['queues_with_positive_labels'])}",
        "",
        "## 6. Graph Layer",
        f"- graph_party_node: {fmt_int(graph_counts['graph_party_node'])}",
        f"- graph_party_edge: {fmt_int(graph_counts['graph_party_edge'])}",
        f"- graph_account_node: {fmt_int(graph_counts['graph_account_node'])}",
        f"- graph_account_edge: {fmt_int(graph_counts['graph_account_edge'])}",
        f"- graph_party_cluster_membership: {fmt_int(graph_counts['graph_party_cluster_membership'])}",
        f"- graph_party_cluster_summary: {fmt_int(graph_counts['graph_party_cluster_summary'])}",
        "",
        "## 7. BigQuery Live Validation",
        f"- state ok: `{bq['ok']}`",
        f"- dev_transaction_mart: {fmt_int(bq['table_counts']['dev_transaction_mart'])}",
        f"- dev_fraud_scores: {fmt_int(bq['table_counts']['dev_fraud_scores'])}",
        f"- dev_alert_queue: {fmt_int(bq['table_counts']['dev_alert_queue'])}",
        f"- dev_graph_party_node: {fmt_int(bq['table_counts']['dev_graph_party_node'])}",
        f"- dev_graph_party_edge: {fmt_int(bq['table_counts']['dev_graph_party_edge'])}",
        "",
        "### 7.1 Executive View Checks",
    ]
    for row in sql_runs["exec_shapes"]:
        lines.append(f"- {row['view_name']}: {fmt_int(row['row_count'])}")
    for row in sql_runs["exec_quality"]:
        lines.append(f"- {row['check_name']}: {row['observed_value']}")
    lines += ["", "### 7.2 Analyst View Checks"]
    for row in sql_runs["analyst_shapes"]:
        lines.append(f"- {row['object_name']}: {fmt_int(row['row_count'])}")
    for row in sql_runs["analyst_quality"]:
        lines.append(f"- {row['check_name']}: {row['defect_count']}")
    lines += [
        "",
        "## 8. Vertex Gemini Analyst Layer",
        f"- run_id: {vertex.get('run_id', '-')}",
        f"- location: {vertex.get('location', '-')}",
        f"- model: {vertex.get('model', '-')}",
        f"- fallback_model: {vertex.get('fallback_model', '-')}",
        f"- response_count: {fmt_int(vertex.get('response_count', 0))}",
        f"- error_count: {fmt_int(vertex.get('error_count', 0))}",
        f"- deterministic_fallback_count: {fmt_int(derived['vertex_fallback_count'])}",
        f"- promoted_to_latest: {vertex.get('promoted_to_latest', False)}",
        "",
        "## 9. Dashboard Publish Layer",
        f"- dashboard validator ok: `{dashboard_validate['ok']}`",
        f"- total_transactions: {fmt_int(dashboard_validate['payload_summary']['total_transactions'])}",
        f"- total_scored_rows: {fmt_int(dashboard_validate['payload_summary']['total_scored_rows'])}",
        f"- passed_checks/total_checks: {dashboard_validate['payload_summary']['passed_checks']}/{dashboard_validate['payload_summary']['total_checks']}",
        f"- total_defects: {dashboard_validate['payload_summary']['total_defects']}",
        "",
        "## 10. Risks and Guardrails",
        "- Keep no-score datasets out of score-bucket/queue logic.",
        "- Preserve party/account namespace isolation in graph IDs.",
        "- Keep dashboard validator as a hard publish gate.",
        "- Keep deterministic fallback active for quota/truncation edge cases.",
        "",
        "## 11. Final Decision",
        f"- Publish readiness: **{'READY FOR PUBLISH' if derived['publish_ready'] else 'NOT READY'}**",
        "- No blocking defects found within current scope.",
    ]
    return "\n".join(lines) + "\n"


def build_text(markdown: str) -> List[str]:
    return [line.rstrip() for line in markdown.replace("```mermaid", "MERMAID_FLOW_START").replace("```", "MERMAID_FLOW_END").splitlines()]


def main() -> None:
    if not MASTER_SNAPSHOT_PATH.exists():
        raise FileNotFoundError("Missing master TR snapshot. Run `make report-master` first.")

    report = load_json(MASTER_SNAPSHOT_PATH)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    md_text = build_markdown(report)
    txt_lines = build_text(md_text)

    md_path = REPORTS_DIR / "07_Master_Final_Report_EN.md"
    txt_path = REPORTS_DIR / "07_Master_Final_Report_EN.txt"
    pdf_path = REPORTS_DIR / "07_Master_Final_Report_EN.pdf"
    json_path = REPORTS_DIR / "07_Master_Final_Report_EN_Snapshot.json"

    md_path.write_text(md_text, encoding="utf-8")
    txt_path.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    with PdfPages(pdf_path) as pdf:
        add_text_pages(
            pdf,
            "Fraud - AML Graph Sentinel | Master Final Report (EN)",
            txt_lines,
            subtitle="Comprehensive end-to-end report generated from validated local + live artifacts.",
        )

    print(
        json.dumps(
            {
                "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "markdown": str(md_path.relative_to(ROOT)),
                "text": str(txt_path.relative_to(ROOT)),
                "pdf": str(pdf_path.relative_to(ROOT)),
                "snapshot": str(json_path.relative_to(ROOT)),
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()


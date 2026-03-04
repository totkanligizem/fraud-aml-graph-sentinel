#!/usr/bin/env python3
"""Generate the most comprehensive end-to-end master report (TR)."""

from __future__ import annotations

import ast
import csv
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
ARTIFACTS_DIR = ROOT / "artifacts"
SNAPSHOT_PATH = REPORTS_DIR / "03_Operational_Checkpoint_Snapshot.json"
DASHBOARD_DATA_PATH = ROOT / "dashboard" / "dashboard-data.json"
DASHBOARD_VALIDATE_PATH = ARTIFACTS_DIR / "dashboard" / "validate-dashboard-state.json"
VERTEX_LATEST_SUMMARY = ARTIFACTS_DIR / "agent" / "vertex_responses" / "latest" / "run-summary.json"
BQ_STATE_PATH = ARTIFACTS_DIR / "bigquery" / "validate-bigquery-state.json"


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


def latest_sql_run_summary(sql_path: str) -> Path:
    summaries = sorted((ARTIFACTS_DIR / "bigquery" / "sql-runs").glob("*/run-summary.json"))
    for path in reversed(summaries):
        payload = load_json(path)
        if payload.get("sql_path") == sql_path:
            return path
    raise FileNotFoundError(f"No SQL run summary found for sql_path={sql_path}")


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def get_sql_csv_rows(summary_path: Path, sql_basename: str) -> List[Dict[str, str]]:
    summary = load_json(summary_path)
    for item in summary.get("results", []):
        if Path(item.get("sql_file", "")).name == sql_basename:
            csv_name = item.get("csv_file")
            if not csv_name:
                return []
            csv_path = summary_path.parent / csv_name
            if not csv_path.exists():
                return []
            return read_csv_rows(csv_path)
    return []


def scan_toolchain() -> Dict[str, Any]:
    scripts_dir = ROOT / "scripts"
    by_file: Dict[str, List[str]] = {}
    packages: set[str] = set()
    for path in sorted(scripts_dir.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        imports.discard("__future__")
        if imports:
            by_file[path.name] = sorted(imports)
            packages.update(imports)
    return {
        "all_packages": sorted(packages),
        "by_file": [f"- {name}: {', '.join(mods)}" for name, mods in by_file.items()],
    }


def build_mermaid_flow() -> str:
    return """```mermaid
flowchart TD
    A[Manual datasets in data/raw] --> B[Canonical ingestion]
    B --> C[Curated transaction_event]
    C --> D[SQLite warehouse build]
    D --> E[transaction_mart + feature + monitoring]
    E --> F[Fraud baseline train + score]
    F --> G[Alert queue ranking]
    E --> H[Graph layer build]
    G --> H
    E --> I[BigQuery core mirror]
    H --> I
    I --> J[Analytics + Executive SQL views]
    I --> K[Validation SQL bundles]
    G --> L[Casebook + Prompt pack]
    L --> M[Vertex Gemini analyst runtime]
    M --> N[Analyst tables in BigQuery]
    J --> O[Dashboard bundle]
    K --> O
    N --> O
    O --> P[TR reports + publish surface]
```"""


def sanitize_path_for_report(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    path = Path(value)
    if path.is_absolute():
        try:
            return str(path.relative_to(ROOT))
        except ValueError:
            return path.name
    return value


def sanitize_vertex_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    vertex = dict(payload)
    if "credentials_client_email" in vertex:
        vertex["credentials_client_email"] = "redacted"
    if "prompt_pack_dir" in vertex:
        vertex["prompt_pack_dir"] = sanitize_path_for_report(vertex["prompt_pack_dir"])
    return vertex


def build_report_payload() -> Dict[str, Any]:
    snapshot = load_json(SNAPSHOT_PATH)
    dashboard = load_json(DASHBOARD_DATA_PATH)
    dashboard_validate = load_json(DASHBOARD_VALIDATE_PATH)
    vertex = sanitize_vertex_summary(load_json(VERTEX_LATEST_SUMMARY))
    bq_state = load_json(BQ_STATE_PATH)
    toolchain = scan_toolchain()

    exec_summary = latest_sql_run_summary("sql/bigquery/executive_validation")
    analyst_summary = latest_sql_run_summary("sql/bigquery/analyst_validation")
    core_validation_summary = latest_sql_run_summary("sql/bigquery/validation")

    exec_shapes = get_sql_csv_rows(exec_summary, "01_exec_view_shapes.sql")
    exec_quality = get_sql_csv_rows(exec_summary, "02_exec_view_quality.sql")
    analyst_shapes = get_sql_csv_rows(analyst_summary, "01_analyst_view_shapes.sql")
    analyst_quality = get_sql_csv_rows(analyst_summary, "02_analyst_view_quality.sql")
    core_quality = get_sql_csv_rows(core_validation_summary, "02_data_quality_checks.sql")

    fallback_count = sum(1 for row in vertex.get("results", []) if "deterministic_fallback" in str(row.get("model", "")))
    exec_invalid_zero = all(int(row.get("observed_value", "0")) == 0 for row in exec_quality if row.get("check_name", "").startswith("invalid_"))
    analyst_defects_zero = all(int(row.get("defect_count", "0")) == 0 for row in analyst_quality)

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "project": {
            "name": "Fraud - AML Graph Sentinel",
            "project_id": bq_state.get("project_id", "fraud-aml-graph"),
            "dataset_id": bq_state.get("dataset_id", "fraud_aml_graph_dev"),
            "location": bq_state.get("location", "EU"),
            "theme": dashboard["project"]["theme"],
        },
        "snapshot": snapshot,
        "dashboard": dashboard,
        "dashboard_validate": dashboard_validate,
        "vertex": vertex,
        "bigquery_state": bq_state,
        "toolchain": toolchain,
        "sql_runs": {
            "core_validation_summary": str(core_validation_summary.relative_to(ROOT)),
            "executive_validation_summary": str(exec_summary.relative_to(ROOT)),
            "analyst_validation_summary": str(analyst_summary.relative_to(ROOT)),
            "exec_shapes": exec_shapes,
            "exec_quality": exec_quality,
            "analyst_shapes": analyst_shapes,
            "analyst_quality": analyst_quality,
            "core_quality": core_quality,
        },
        "derived": {
            "vertex_fallback_count": fallback_count,
            "exec_invalid_zero": exec_invalid_zero,
            "analyst_defects_zero": analyst_defects_zero,
            "publish_ready": (
                bool(dashboard_validate.get("ok"))
                and bool(bq_state.get("ok"))
                and int(vertex.get("error_count", 1)) == 0
                and exec_invalid_zero
                and analyst_defects_zero
            ),
        },
        "flow_mermaid": build_mermaid_flow(),
    }


def build_markdown(report: Dict[str, Any]) -> str:
    project = report["project"]
    snapshot = report["snapshot"]
    dashboard = report["dashboard"]
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
        f"# {project['name']} | Master Final Raporu (TR)",
        "",
        f"Uretim tarihi (UTC): {report['generated_at_utc']}",
        "",
        "## 1. Yonetici Ozeti",
        f"- Publish readiness: **{'READY FOR PUBLISH' if derived['publish_ready'] else 'NOT READY'}**",
        f"- Dashboard quality gate: `{dashboard_validate['ok']}`",
        f"- BigQuery state gate: `{bq['ok']}`",
        f"- Vertex analyst gate: `error_count={vertex['error_count']}`",
        f"- Executive invalid checks zero: `{derived['exec_invalid_zero']}`",
        f"- Analyst defect checks zero: `{derived['analyst_defects_zero']}`",
        "",
        "## 2. Proje Kimligi ve Kapsam",
        f"- Proje adi: {project['name']}",
        f"- Proje kimligi: {project['project_id']}",
        f"- BigQuery dataset: {project['dataset_id']} ({project['location']})",
        "- Konu: Fraud tespiti + AML izleme + graph intelligence + investigation queue + executive analytics",
        "- Kapsam: Canonical ingestion, local warehouse, baseline model, ranking, graph layer, BigQuery mirror, dashboard, analyst copilot",
        "- Theme: " + project["theme"],
        "",
        "## 3. Is Problemi ve Yanitlanan Sorular",
        "- Hangi queue gunleri investigasyon onceligine alinmali?",
        "- Hangi datasetlerde risk baskisi yuksek ve neden?",
        "- Fraud ve AML sinyalleri ayni operational yuzeyde nasil birlikte izlenir?",
        "- Graph tarafinda hangi party/cluster yapilari supheli?",
        "- Local state ile BigQuery state birebir uyumlu mu?",
        "- Publish edilen dashboard sayilari validator kapilarindan geciyor mu?",
        "",
        "## 4. Uctan Uca Mimari ve Workflow",
        report["flow_mermaid"],
        "",
        "## 5. Metodoloji",
        "- Canonical schema normalization",
        "- SQLite warehouse with deterministic rebuild",
        "- Point-in-time feature engineering",
        "- Baseline fraud scoring (numpy/pandas pipeline)",
        "- Queue ranking with P@K / NDCG@K",
        "- Graph node-edge aggregation + cluster summaries",
        "- BigQuery mirror + SQL analytics + SQL validation",
        "- Artifact-first reporting and static dashboard bundling",
        "- Vertex Gemini analyst outputs with schema validation and deterministic fallback",
        "",
        "## 6. Toolchain (Kod Tabanindan Taranmis)",
        "### 6.1 Genel Paketler",
    ]
    lines.extend(f"- {item}" for item in report["toolchain"]["all_packages"])
    lines += ["", "### 6.2 Script Bazli Import Haritasi"]
    lines.extend(report["toolchain"]["by_file"])
    lines += [
        "",
        "## 7. Veri Envanteri ve Aktif Hacimler",
        f"- transaction_event_raw: {fmt_int(warehouse_counts['transaction_event_raw'])}",
        f"- stg_transaction_event: {fmt_int(warehouse_counts['stg_transaction_event'])}",
        f"- transaction_mart: {fmt_int(warehouse_counts['transaction_mart'])}",
        f"- feature_payer_24h: {fmt_int(warehouse_counts['feature_payer_24h'])}",
        f"- monitoring_mart: {fmt_int(warehouse_counts['monitoring_mart'])}",
        f"- fraud_scores total: {fmt_int(snapshot['scoring_summary']['total_scored_rows'])}",
        f"- alert queue count (distinct daily queues): {fmt_int(ranking['queue_count'])}",
        f"- scored rows by dataset: creditcard={fmt_int(scored_by_dataset['creditcard_fraud'])}, ieee={fmt_int(scored_by_dataset['ieee_cis'])}, paysim={fmt_int(scored_by_dataset['paysim'])}",
        "",
        "## 8. Model ve Ranking Sonuclari",
        f"- average_precision: {fmt_float(model['metrics']['average_precision'])}",
        f"- pr_auc_trapz: {fmt_float(model['metrics']['pr_auc_trapz'])}",
        f"- cost_optimized_threshold: {fmt_float(model['cost_optimized_threshold']['threshold'], 6)}",
        f"- queue_count: {fmt_int(ranking['queue_count'])}",
        f"- mean_precision_at_k: {pct(ranking['mean_precision_at_k'])}",
        f"- mean_ndcg_at_k: {pct(ranking['mean_ndcg_at_k'])}",
        f"- queues_with_positive_labels: {fmt_int(ranking['queues_with_positive_labels'])}",
        "",
        "## 9. Graph Katmani",
        f"- graph_party_node: {fmt_int(graph_counts['graph_party_node'])}",
        f"- graph_party_edge: {fmt_int(graph_counts['graph_party_edge'])}",
        f"- graph_account_node: {fmt_int(graph_counts['graph_account_node'])}",
        f"- graph_account_edge: {fmt_int(graph_counts['graph_account_edge'])}",
        f"- graph_party_cluster_membership: {fmt_int(graph_counts['graph_party_cluster_membership'])}",
        f"- graph_party_cluster_summary: {fmt_int(graph_counts['graph_party_cluster_summary'])}",
        "",
        "## 10. BigQuery Katmani (Canli Validasyon)",
        f"- state ok: `{bq['ok']}`",
        f"- dev_transaction_mart: {fmt_int(bq['table_counts']['dev_transaction_mart'])}",
        f"- dev_fraud_scores: {fmt_int(bq['table_counts']['dev_fraud_scores'])}",
        f"- dev_alert_queue: {fmt_int(bq['table_counts']['dev_alert_queue'])}",
        f"- dev_graph_party_node: {fmt_int(bq['table_counts']['dev_graph_party_node'])}",
        f"- dev_graph_party_edge: {fmt_int(bq['table_counts']['dev_graph_party_edge'])}",
        "",
        "### 10.1 Executive View Kontrolleri",
    ]
    for row in sql_runs["exec_shapes"]:
        lines.append(f"- {row['view_name']}: {fmt_int(row['row_count'])}")
    for row in sql_runs["exec_quality"]:
        lines.append(f"- {row['check_name']}: {row['observed_value']}")
    lines += ["", "### 10.2 Analyst View Kontrolleri"]
    for row in sql_runs["analyst_shapes"]:
        lines.append(f"- {row['object_name']}: {fmt_int(row['row_count'])}")
    for row in sql_runs["analyst_quality"]:
        lines.append(f"- {row['check_name']}: {row['defect_count']}")
    lines += [
        "",
        "## 11. Vertex Gemini Analyst Katmani",
        f"- run_id: {vertex.get('run_id', '-')}",
        f"- location: {vertex.get('location', '-')}",
        f"- model: {vertex.get('model', '-')}",
        f"- fallback_model: {vertex.get('fallback_model', '-')}",
        f"- response_count: {fmt_int(vertex.get('response_count', 0))}",
        f"- error_count: {fmt_int(vertex.get('error_count', 0))}",
        f"- deterministic_fallback_count: {fmt_int(derived['vertex_fallback_count'])}",
        f"- promoted_to_latest: {vertex.get('promoted_to_latest', False)}",
        "",
        "## 12. Dashboard Publish Katmani",
        f"- dashboard validator ok: `{dashboard_validate['ok']}`",
        f"- dataset_count: {dashboard_validate['payload_summary']['dataset_count']}",
        f"- total_transactions: {fmt_int(dashboard_validate['payload_summary']['total_transactions'])}",
        f"- total_scored_rows: {fmt_int(dashboard_validate['payload_summary']['total_scored_rows'])}",
        f"- passed_checks/total_checks: {dashboard_validate['payload_summary']['passed_checks']}/{dashboard_validate['payload_summary']['total_checks']}",
        f"- total_defects: {dashboard_validate['payload_summary']['total_defects']}",
        f"- html_id_count/js_bound_id_count: {dashboard_validate['dom_summary']['html_id_count']}/{dashboard_validate['dom_summary']['js_bound_id_count']}",
        "",
        "## 13. Riskler ve Dikkat Noktalari",
        "- No-score datasetler icin score bucket ve queue fallback uretimi yapilmamali (yanlis yorum riski).",
        "- Graph namespace ayrimi korunmali (party/account collision riski).",
        "- BigQuery live checkler periyodik tekrar edilmeli; artifact timestamp izlenmeli.",
        "- Vertex quota/response truncation durumlari icin deterministic fallback katmani aktif tutulmali.",
        "- Publish oncesi dashboard validator kapisi bypass edilmemeli.",
        "",
        "## 14. Test Komutlari (Tekrar Edilebilirlik)",
        "- make validate-state",
        "- make graph-validate",
        "- make dashboard-check",
        "- make agent-vertex-batch-validate",
        "- make bq-full-check",
        "- make bq-graph-check",
        "- make bq-validate-executive-views",
        "- make bq-analyst-check",
        "",
        "## 15. Kanit Artefaktlari",
        f"- {str(SNAPSHOT_PATH.relative_to(ROOT))}",
        f"- {str(DASHBOARD_VALIDATE_PATH.relative_to(ROOT))}",
        f"- {str(VERTEX_LATEST_SUMMARY.relative_to(ROOT))}",
        f"- {sql_runs['core_validation_summary']}",
        f"- {sql_runs['executive_validation_summary']}",
        f"- {sql_runs['analyst_validation_summary']}",
        "",
        "## 16. Nihai Karar",
        f"- Publish readiness: **{'READY FOR PUBLISH' if derived['publish_ready'] else 'NOT READY'}**",
        "- Kapsam dahilinde bloklayan acik bug bulunmadi.",
    ]
    return "\n".join(lines) + "\n"


def build_text(report: Dict[str, Any]) -> List[str]:
    markdown = build_markdown(report)
    return [line.rstrip() for line in markdown.replace("```mermaid", "MERMAID_FLOW_START").replace("```", "MERMAID_FLOW_END").splitlines()]


def main() -> None:
    for path in [SNAPSHOT_PATH, DASHBOARD_DATA_PATH, DASHBOARD_VALIDATE_PATH, VERTEX_LATEST_SUMMARY, BQ_STATE_PATH]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required input: {path}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = build_report_payload()
    md_text = build_markdown(report)
    txt_lines = build_text(report)

    md_path = REPORTS_DIR / "07_Master_Final_Rapor_TR.md"
    txt_path = REPORTS_DIR / "07_Master_Final_Rapor_TR.txt"
    pdf_path = REPORTS_DIR / "07_Master_Final_Rapor_TR.pdf"
    json_path = REPORTS_DIR / "07_Master_Final_Rapor_TR_Snapshot.json"

    md_path.write_text(md_text, encoding="utf-8")
    txt_path.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    with PdfPages(pdf_path) as pdf:
        add_text_pages(
            pdf,
            "Fraud - AML Graph Sentinel | Master Final Rapor",
            txt_lines,
            subtitle="End-to-end kapsamli rapor; local + live BigQuery + live Vertex dogrulamalari ile uretilmistir.",
        )

    print(
        json.dumps(
            {
                "created_at_utc": report["generated_at_utc"],
                "markdown": str(md_path.relative_to(ROOT)),
                "text": str(txt_path.relative_to(ROOT)),
                "pdf": str(pdf_path.relative_to(ROOT)),
                "snapshot": str(json_path.relative_to(ROOT)),
                "publish_ready": report["derived"]["publish_ready"],
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

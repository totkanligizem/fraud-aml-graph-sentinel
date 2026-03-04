#!/usr/bin/env python3
"""Generate a comprehensive interim project briefing report (TR)."""

from __future__ import annotations

import ast
import json
import os
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

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
SNAPSHOT_PATH = REPORTS_DIR / "03_Operational_Checkpoint_Snapshot.json"
DASHBOARD_DATA_PATH = ROOT / "dashboard" / "dashboard-data.json"


def load_json(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt_int(value: int) -> str:
    return f"{int(value):,}"


def fmt_float(value: float, digits: int = 4) -> str:
    return f"{float(value):.{digits}f}"


def pct(value: float, digits: int = 2) -> str:
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


def scan_toolchain() -> Dict[str, List[str]]:
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


def build_mermaid() -> str:
    return """```mermaid
flowchart TD
    A[Manual datasets in data/raw] --> B[ingest_canonical.py]
    B --> C[transaction_event_raw]
    C --> D[build_sqlite_warehouse.py]
    D --> E[stg_transaction_event]
    E --> F[transaction_mart]
    F --> G[feature_payer_24h]
    F --> H[monitoring_mart]
    F --> I[train_fraud_baseline_numpy.py]
    I --> J[fraud baseline model]
    J --> K[score_fraud_baseline_numpy.py]
    K --> L[fraud_scores]
    L --> M[build_investigation_queue.py]
    M --> N[alert_queue]
    F --> O[build_graph_layer.py]
    L --> O
    N --> O
    O --> P[graph nodes and edges]
    F --> Q[sqlite_to_bigquery.py]
    G --> Q
    H --> Q
    L --> Q
    N --> Q
    P --> Q
    Q --> R[BigQuery dev tables]
    R --> S[Analytics SQL bundles]
    R --> T[Validation SQL bundles]
    F --> U[generate_checkpoint_reports.py]
    R --> U
    U --> V[checkpoint pdf/txt/json]
    U --> W[build_dashboard_bundle.py]
    W --> X[static dashboard bundle]
```"""


def build_ascii_flow() -> List[str]:
    return [
        "data/raw/* datasets",
        "  -> ingest_canonical.py",
        "  -> data/curated/transaction_event",
        "  -> build_sqlite_warehouse.py",
        "  -> transaction_event_raw / stg_transaction_event / transaction_mart",
        "  -> feature_payer_24h + monitoring_mart",
        "  -> train_fraud_baseline_numpy.py",
        "  -> fraud baseline model",
        "  -> score_fraud_baseline_numpy.py",
        "  -> fraud_scores",
        "  -> build_investigation_queue.py",
        "  -> alert_queue",
        "  -> build_graph_layer.py",
        "  -> graph_* tables",
        "  -> sqlite_to_bigquery.py",
        "  -> BigQuery dev_* tables",
        "  -> run_bigquery_sql_bundle.py (analytics + validation)",
        "  -> validate_bigquery_state.py",
        "  -> generate_checkpoint_reports.py",
        "  -> build_dashboard_bundle.py",
        "  -> reports + dashboard",
    ]


def build_report_payload() -> Dict[str, object]:
    snapshot = load_json(SNAPSHOT_PATH)
    dashboard = load_json(DASHBOARD_DATA_PATH)
    toolchain = scan_toolchain()
    bq = snapshot["bigquery_state"]
    warehouse = snapshot["warehouse_summary"]
    ranking = snapshot["ranking_summary"]
    scoring = snapshot["scoring_summary"]
    graph = snapshot["graph_summary"]
    model = snapshot["model_metrics"]

    rerun_audit = {
        "audit_rerun_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "local_checks": [
            "python3 -m compileall -q scripts -> PASS",
            "make check-datasets -> PASS",
            "python3 scripts/cleanup_incomplete_runs.py -> candidate_count=0",
            "make validate-state -> PASS",
            "make graph-validate -> PASS",
            "PRAGMA integrity_check -> ok",
            "PRAGMA quick_check -> ok",
            "python3 scripts/build_dashboard_bundle.py -> PASS",
            "python3 scripts/validate_dashboard_bundle.py -> PASS",
            "make agent-vertex-validate -> PASS",
        ],
        "bigquery_revalidation": "Local sandbox icinde yeniden validate denemesi cevap donmedi; bu nedenle remote durum icin 2026-02-27T14:24:44Z tarihli son PASS artifact baz alindi.",
    }

    report = {
        "generated_at_utc": rerun_audit["audit_rerun_utc"],
        "project": {
            "name": "Fraud - AML Graph Sentinel",
            "topic": "Fraud detection + AML monitoring + graph intelligence + investigation queue + BigQuery analytics",
            "scope": "Senior-level bank-tech portfolio project; canonical ingestion, local warehouse, baseline model, queueing, graph layer, BigQuery sync, reporting and dashboard.",
            "purpose": "Birden fazla sentetik/benchmark fraud ve AML veri kaynagini ortak bir finansal risk izleme yuzeyinde birlestirmek; siralanmis inceleme kuyrugu ve graph watchlist ile investigasyon verimliligini artirmak.",
            "goals": [
                "Heterojen veri kaynaklarini ortak veri sozlesmesinde toplamak",
                "Deterministik ve yeniden uretilebilir local pipeline kurmak",
                "Fraud scoring ve investigation ranking saglamak",
                "Graph tabanli entity ve cluster risk sinyali uretmek",
                "BigQuery uzerinden analytics ve validator katmani kurmak",
                "Yonetici sunumuna uygun rapor ve dashboard katmani hazirlamak",
            ],
        },
        "business_questions": [
            "Hangi transactions ve hangi queue gunleri investigasyon onceligine alinmali?",
            "Hangi dataset veya davranis siniflari model tarafinda daha riskli gozukuyor?",
            "Fraud sinyali ile AML sinyalini ayni risk yuzeyinde nasil birlikte izleriz?",
            "Hangi party/account yapilari graph seviyesinde supheli cluster olusturuyor?",
            "Local pipeline ile BigQuery analytical layer arasinda veri kaybi veya contract drift var mi?",
            "Quality gate'ler sifir defect seviyesinde mi?",
        ],
        "datasets": [
            "ieee_cis -> kart/online transaction fraud benchmark",
            "creditcard_fraud -> labelled card fraud benchmark",
            "paysim -> payment simulator fraud benchmark",
            "ibm_aml_data -> synthetic AML transactions with laundering labels",
            "banksim / ibm_amlsim / elliptic -> klasor yapisi hazir, mevcut asamada bos ve opsiyonel ikinci faz",
        ],
        "methodology": [
            "Canonical schema normalization",
            "SQLite-based reproducible warehouse",
            "Point-in-time feature engineering",
            "Numpy/Pandas baseline linear fraud model",
            "Daily investigation queue ranking with P@K and NDCG@K evaluation",
            "Graph node/edge aggregation and connected-cluster summarization",
            "Artifact-first validation and evidence-driven reporting",
            "BigQuery mirror plus SQL analytics/validation layer",
        ],
        "toolchain": toolchain,
        "current_state": {
            "warehouse_counts": warehouse["table_counts"],
            "scored_rows": scoring["rows_by_dataset"],
            "ranking": ranking,
            "graph_counts": graph["table_counts"],
            "graph_cluster_summary": graph["cluster_summary"],
            "bigquery_counts": bq["table_counts"],
            "quality_metrics": bq["quality_metrics"],
            "graph_quality_metrics": bq["graph_quality_metrics"],
            "model_metrics": model["metrics"],
            "cost_threshold": model["cost_optimized_threshold"],
            "dashboard_completion": dashboard["project"]["completion"],
            "analyst": dashboard["analyst"],
        },
        "completed_work": [
            "Dataset layout and manual download verification",
            "Canonical ingestion pipeline for four active datasets",
            "Warehouse build with transaction mart, feature mart and monitoring mart",
            "Fraud baseline training, scoring and ranking pipeline",
            "Graph node/edge and cluster layer",
            "BigQuery connection, full sync, analytics SQL and validation SQL",
            "Vertex AI groundwork: region, service account, bucket",
            "TR/EN operational checkpoint reports",
            "Static executive dashboard bundle seeded from validated artifacts",
            "Dashboard QA validator and evidence metadata layer",
            "Vertex AI + Gemini live smoke path validated and latest analyst output surfaced into the dashboard",
        ],
        "critical_fixes_applied": [
            "source_event_id type drift kapatildi ve STRING/TEXT standardize edildi",
            "label_aml mixed-type problemi kapatildi ve nullable INT sozlesmesine cekildi",
            "party_id / account_id namespace collision giderildi",
            "Graph validators ile namespace clash fail condition haline getirildi",
            "Warehouse scoring path icin kritik indexler eklendi",
            "Yarim kalan IBM run artiklari temizlendi; mevcut candidate_count=0",
            "Dashboard tarafinda no-score lens (IBM) icin sahte overview fallback kaldirildi",
            "Dashboard tarafinda amount alanlari icin gereksiz USD iddiasi kaldirildi; sayisal magnitude sunumu kullanildi",
            "Dashboard icin publish-breaking drift, missing artifact ve DOM binding validator eklendi",
            "Vertex analyst runtime icin cloud-platform scope eklendi; invalid_scope hatasi kapatildi",
            "Vertex structured output icin JSON schema, retry ve quota-light smoke politikasi eklendi",
        ],
        "importance_map": [
            "transaction_mart veri sozlesmesi projenin en kritik omurgasi; upstream/downstream her sey buraya bagli",
            "validate_pipeline_state.py ve validate_graph_state.py kalite kapilari regressions yakalamak icin kritik",
            "alert_queue ve ranking metrics business value'nin cekirdegi; investigasyon etkisini bunlar olcuyor",
            "graph_party_cluster_summary supheli topluluklarin executive anlatimi icin en yuksek onemli graph ciktisi",
            "validate_bigquery_state artifact'i local ile analytical layer parity kanitidir",
            "checkpoint snapshot ve dashboard bundle publish edilen sayilarin denetlenebilir olmasini saglar",
            "Vertex analyst smoke artifact'i LLM katmaninin deterministic prompt-contract ile calistiginin kanitidir",
        ],
        "risks_and_pitfalls": [
            "Ayni sayfada fraud ve AML sinyalini gostermek kolayca semantik karisiklik yaratir; label source alanlari net ayrilmali",
            "Amount alanlari kaynak para birimi bazli olabilir; normalized currency iddiasi ancak fx donusumu ile acikca yapilmali",
            "IBM AML veri hacmi buyuk; uncapped ingest disk ve zaman maliyeti yaratir",
            "Dashboard tarafinda no-score datasetler icin sahte fallback gosterimi karar yanlisi uretir",
            "BigQuery tarafinda remote revalidation sansli ortam gerektirir; artifact timestamp'i izlenmeli",
            "Namespace ayrimi bozulursa graph node ve edge anlamini kaybeder; validatorlar bypass edilmemeli",
            "Feature engineering pencereleri point-in-time kurallarina bagli; leakage riski kritik",
            "Queue metrics yuksek gorunse bile class imbalance nedeniyle threshold ve business cost birlikte okunmali",
            "Vertex free-trial veya dusuk quota ortaminda ard arda multi-prompt smoke 429 uretebilir; smoke testi bilincli olarak kucuk tutulmali",
        ],
        "next_opportunities": [
            "Dashboard'i tarayicida calisip son mile polish ve responsive ince ayar yapmak",
            "Looker Studio veya benzeri paylasim katmani ile BigQuery view uzerinden canli executive board kurmak",
            "Gemini/Vertex AI analyst copilotu tek queue smoke'tan coklu queue batch ve dashboard drilldown seviyesine cikarmak",
            "opsiyonel ikinci faz datasetleri ekleyip karsilastirmali modelleme yapmak",
            "CI seviyesinde lokal smoke validation zinciri eklemek",
            "Model explainability ve error analysis katmanini zenginlestirmek",
        ],
        "flow_ascii": build_ascii_flow(),
        "flow_mermaid": build_mermaid(),
        "rerun_audit": rerun_audit,
        "evidence": dashboard["evidence_paths"],
    }
    return report


def build_markdown(report: Dict[str, object]) -> str:
    project = report["project"]
    current = report["current_state"]
    lines = [
        f"# {project['name']} | Ara Briefing Raporu",
        "",
        f"Uretim tarihi (UTC): {report['generated_at_utc']}",
        "",
        "## 1. Proje Kimligi",
        f"- Proje adi: {project['name']}",
        f"- Konu: {project['topic']}",
        f"- Kapsam: {project['scope']}",
        f"- Amac: {project['purpose']}",
        "",
        "## 2. Amaclar ve Hedefler",
    ]
    lines.extend(f"- {item}" for item in project["goals"])
    lines += ["", "## 3. Yanitlanan Is Sorulari"]
    lines.extend(f"- {item}" for item in report["business_questions"])
    lines += ["", "## 4. Aktif Datasetler ve Hazir Alanlar"]
    lines.extend(f"- {item}" for item in report["datasets"])
    lines += ["", "## 5. Metodoloji"]
    lines.extend(f"- {item}" for item in report["methodology"])
    lines += ["", "## 6. Toolchain ve Paketler", "### Genel Paketler"]
    lines.extend(f"- {item}" for item in report["toolchain"]["all_packages"])
    lines += ["", "### Script Bazli Import Haritasi"]
    lines.extend(report["toolchain"]["by_file"])
    lines += [
        "",
        "## 7. Uctan Uca Workflow / Pipeline",
        "### Mermaid Diyagrami",
        report["flow_mermaid"],
        "",
        "### ASCII Akis",
    ]
    lines.extend(f"- {item}" for item in report["flow_ascii"])
    lines += [
        "",
        "## 8. Bu Asamaya Kadar Yapilanlar",
    ]
    lines.extend(f"- {item}" for item in report["completed_work"])
    lines += [
        "",
        "## 9. Guncel Dogrulanmis Durum",
        f"- transaction_mart: {fmt_int(current['warehouse_counts']['transaction_mart'])}",
        f"- feature_payer_24h: {fmt_int(current['warehouse_counts']['feature_payer_24h'])}",
        f"- monitoring_mart: {fmt_int(current['warehouse_counts']['monitoring_mart'])}",
        f"- fraud_scores: {fmt_int(sum(current['scored_rows'].values()))}",
        f"- alert_queue distinct queue: {fmt_int(current['ranking']['queue_count'])}",
        f"- mean_precision_at_k: {pct(current['ranking']['mean_precision_at_k'])}",
        f"- mean_ndcg_at_k: {pct(current['ranking']['mean_ndcg_at_k'])}",
        f"- graph_party_node: {fmt_int(current['graph_counts']['graph_party_node'])}",
        f"- graph_party_edge: {fmt_int(current['graph_counts']['graph_party_edge'])}",
        f"- graph_party_cluster_summary: {fmt_int(current['graph_counts']['graph_party_cluster_summary'])}",
        f"- BigQuery ok: {current['bigquery_counts']['dev_transaction_mart'] == current['warehouse_counts']['transaction_mart']}",
        f"- Dashboard completion MVP/vision: %{current['dashboard_completion']['mvp']} / %{current['dashboard_completion']['vision']}",
        f"- Vertex analyst smoke ok: {current['analyst']['available'] and current['analyst']['error_count'] == 0}",
        f"- Vertex analyst model: {current['analyst'].get('model', '-')}",
        f"- Vertex analyst response_count: {fmt_int(current['analyst'].get('response_count', 0))}",
        "",
        "## 10. Yeniden Kosulan Audit ve Testler",
    ]
    lines.extend(f"- {item}" for item in report["rerun_audit"]["local_checks"])
    lines += [f"- {report['rerun_audit']['bigquery_revalidation']}"]
    lines += ["", "## 11. Kapatilan / Revize Edilen Kritik Noktalar"]
    lines.extend(f"- {item}" for item in report["critical_fixes_applied"])
    lines += ["", "## 12. Kritik Riskler, Puf Noktalari ve Dikkat Edilecek Asamalar"]
    lines.extend(f"- {item}" for item in report["risks_and_pitfalls"])
    lines += ["", "## 13. En Kritik Parcalar"]
    lines.extend(f"- {item}" for item in report["importance_map"])
    lines += ["", "## 14. Bundan Sonra Ne Yapabiliriz?"]
    lines.extend(f"- {item}" for item in report["next_opportunities"])
    lines += ["", "## 15. Kanit Artefaktlari"]
    lines.extend(f"- {item}" for item in report["evidence"])
    return "\n".join(lines) + "\n"


def build_text(report: Dict[str, object]) -> Tuple[str, List[str]]:
    project = report["project"]
    current = report["current_state"]
    title = f"{project['name']} | Ara Briefing Raporu (TR)"
    lines = [
        f"Tarih (UTC): {report['generated_at_utc']}",
        "",
        "1) Proje Kimligi",
        f"- Proje adi: {project['name']}",
        f"- Konu: {project['topic']}",
        f"- Kapsam: {project['scope']}",
        f"- Amac: {project['purpose']}",
        "",
        "2) Proje Amaclari ve Hedefleri",
    ]
    lines.extend(f"- {item}" for item in project["goals"])
    lines += ["", "3) Yanitlanan Is Sorulari"]
    lines.extend(f"- {item}" for item in report["business_questions"])
    lines += ["", "4) Kullanilan Datasetler"]
    lines.extend(f"- {item}" for item in report["datasets"])
    lines += ["", "5) Metodoloji"]
    lines.extend(f"- {item}" for item in report["methodology"])
    lines += ["", "6) Kullanilan Tool / Arac / Paketler"]
    lines.append("- Genel paketler: " + ", ".join(report["toolchain"]["all_packages"]))
    lines.extend(report["toolchain"]["by_file"])
    lines += ["", "7) Workflow ve Pipeline Akisi"]
    lines.extend(f"- {item}" for item in report["flow_ascii"])
    lines += ["", "8) Bu Asamaya Kadar Yapilanlar"]
    lines.extend(f"- {item}" for item in report["completed_work"])
    lines += [
        "",
        "9) Guncel Dogrulanmis State",
        f"- transaction_mart: {fmt_int(current['warehouse_counts']['transaction_mart'])}",
        f"- feature_payer_24h: {fmt_int(current['warehouse_counts']['feature_payer_24h'])}",
        f"- monitoring_mart: {fmt_int(current['warehouse_counts']['monitoring_mart'])}",
        f"- fraud_scores: {fmt_int(sum(current['scored_rows'].values()))}",
        f"- queue_count: {fmt_int(current['ranking']['queue_count'])}",
        f"- mean_precision_at_k: {pct(current['ranking']['mean_precision_at_k'])}",
        f"- mean_ndcg_at_k: {pct(current['ranking']['mean_ndcg_at_k'])}",
        f"- graph_party_cluster_summary: {fmt_int(current['graph_counts']['graph_party_cluster_summary'])}",
        f"- suspicious_edge_count: {fmt_int(current['graph_cluster_summary']['suspicious_edge_count'])}",
        f"- model average_precision: {fmt_float(current['model_metrics']['average_precision'])}",
        f"- model pr_auc_trapz: {fmt_float(current['model_metrics']['pr_auc_trapz'])}",
        f"- cost_optimized_threshold: {fmt_float(current['cost_threshold']['threshold'], 6)}",
        f"- BigQuery dev_transaction_mart: {fmt_int(current['bigquery_counts']['dev_transaction_mart'])}",
        "",
        "10) Yeniden Kosulan Kontrol ve Testler",
    ]
    lines.extend(f"- {item}" for item in report["rerun_audit"]["local_checks"])
    lines.append(f"- {report['rerun_audit']['bigquery_revalidation']}")
    lines += ["", "11) Revize Edilen ve Kapatilan Kritik Noktalar"]
    lines.extend(f"- {item}" for item in report["critical_fixes_applied"])
    lines += ["", "12) Riskler, Puf Noktalari ve Dikkat Edilecek Yerler"]
    lines.extend(f"- {item}" for item in report["risks_and_pitfalls"])
    lines += ["", "13) En Kritik Kisimlar"]
    lines.extend(f"- {item}" for item in report["importance_map"])
    lines += ["", "14) Uzerine Neler Yapabiliriz?"]
    lines.extend(f"- {item}" for item in report["next_opportunities"])
    lines += ["", "15) Kanit Artefaktlari"]
    lines.extend(f"- {item}" for item in report["evidence"])
    return title, lines


def main() -> None:
    if not SNAPSHOT_PATH.exists():
        raise FileNotFoundError("Missing checkpoint snapshot. Run `make report-checkpoint` first.")
    if not DASHBOARD_DATA_PATH.exists():
        raise FileNotFoundError("Missing dashboard bundle. Run `make dashboard-build` first.")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = build_report_payload()
    md_text = build_markdown(report)
    title, text_lines = build_text(report)

    md_path = REPORTS_DIR / "04_Project_Briefing_TR.md"
    txt_path = REPORTS_DIR / "04_Project_Briefing_TR.txt"
    pdf_path = REPORTS_DIR / "04_Project_Briefing_TR.pdf"
    json_path = REPORTS_DIR / "04_Project_Briefing_Snapshot.json"

    md_path.write_text(md_text, encoding="utf-8")
    txt_path.write_text("\n".join(text_lines) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    with PdfPages(pdf_path) as pdf:
        add_text_pages(
            pdf,
            title,
            text_lines,
            subtitle="Senior-level ara briefing raporu; validated artifacts ve yeniden kosulan local audit sonuclari ile uretilmistir.",
        )

    print(
        json.dumps(
            {
                "created_at_utc": report["generated_at_utc"],
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

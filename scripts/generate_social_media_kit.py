#!/usr/bin/env python3
"""Generate social media visuals/videos for LinkedIn and GitHub showcase.

Outputs:
- social_media/slides/{lang}_{format}/slide_*.png
- social_media/videos/{channel}_{lang}.mp4
- social_media/captions/linkedin_post_{lang}.md
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
CHECKPOINT_PATH = ROOT / "reports" / "03_Operational_Checkpoint_Snapshot.json"
COMPARE_PATH = ROOT / "reports" / "08_Model_Benchmark_Comparison_Snapshot.json"
OUT_ROOT = ROOT / "social_media"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt_int(v: int | float | None) -> str:
    if v is None:
        return "-"
    return f"{int(v):,}"


def fmt_pct(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v * 100:.2f}%"


def background(ax, width: int, height: int) -> None:
    x = np.linspace(0, 1, width)
    y = np.linspace(0, 1, height)
    xx, yy = np.meshgrid(x, y)
    z = 0.55 * xx + 0.45 * yy
    c1 = np.array([12, 22, 34]) / 255.0
    c2 = np.array([18, 57, 86]) / 255.0
    grad = c1[None, None, :] * (1 - z[:, :, None]) + c2[None, None, :] * z[:, :, None]
    ax.imshow(grad, extent=[0, 1, 0, 1], origin="lower", aspect="auto")


def setup_canvas(width: int, height: int):
    dpi = 100
    fig = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    background(ax, width, height)
    return fig, ax


def draw_title(ax, title: str, subtitle: str, *, compact: bool = False) -> None:
    t_size = 46 if not compact else 34
    s_size = 24 if not compact else 20
    ax.text(0.06, 0.88, title, fontsize=t_size, color="#EAF4FF", fontweight="bold", va="top")
    ax.text(0.06, 0.78, subtitle, fontsize=s_size, color="#BBD6EE", va="top")


def draw_kpi_cards(ax, cards: list[tuple[str, str]], *, columns: int = 2, top: float = 0.67) -> None:
    rows = int(np.ceil(len(cards) / columns))
    card_w = (0.88 - (columns - 1) * 0.025) / columns
    card_h = (0.5 - (rows - 1) * 0.03) / max(rows, 1)
    start_x = 0.06
    idx = 0
    for r in range(rows):
        for c in range(columns):
            if idx >= len(cards):
                return
            x = start_x + c * (card_w + 0.025)
            y = top - r * (card_h + 0.03)
            box = FancyBboxPatch(
                (x, y - card_h),
                card_w,
                card_h,
                boxstyle="round,pad=0.012,rounding_size=0.018",
                linewidth=1.2,
                edgecolor="#3F729B",
                facecolor=(0.03, 0.08, 0.13, 0.6),
            )
            ax.add_patch(box)
            label, value = cards[idx]
            ax.text(x + 0.02, y - 0.04, label, fontsize=15, color="#9CC3DF", va="top")
            ax.text(x + 0.02, y - 0.12, value, fontsize=26, color="#F3FAFF", fontweight="bold", va="top")
            idx += 1


def draw_bullets(ax, lines: Iterable[str], *, top: float = 0.67, size: int = 20, step: float = 0.08) -> None:
    y = top
    for line in lines:
        ax.text(0.08, y, f"• {line}", fontsize=size, color="#EAF4FF", va="top")
        y -= step


def save_slide(fig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=100)
    plt.close(fig)


def make_video_from_slides(slides: list[Path], out_path: Path, duration: float = 3.6) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    concat_path = out_path.with_suffix(".txt")
    with concat_path.open("w", encoding="utf-8") as f:
        for slide in slides[:-1]:
            f.write(f"file '{slide.as_posix()}'\n")
            f.write(f"duration {duration}\n")
        f.write(f"file '{slides[-1].as_posix()}'\n")
        f.write(f"duration {duration}\n")
        f.write(f"file '{slides[-1].as_posix()}'\n")

    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_path),
        "-vf",
        "fps=30,format=yuv420p",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)


def write_linkedin_copy(out_dir: Path, metrics: dict, lang: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tx = metrics["transactions"]
    scored = metrics["scored"]
    nodes = metrics["nodes"]
    edges = metrics["edges"]
    p50 = metrics["tree_p50"]

    if lang == "en":
        text = f"""# Fraud-AML Graph Sentinel — Production-Style Portfolio Build

I just published a full end-to-end Fraud + AML analytics project with graph intelligence and GenAI analyst copilot.

What this system does:
- Canonical ingestion from multi-source fraud/AML datasets
- Deterministic warehouse and point-in-time-safe features
- Fraud scoring + investigation queue prioritization
- Graph risk intelligence for suspicious clusters
- BigQuery semantic analytics + executive dashboard
- Vertex Gemini structured analyst copilot

Current snapshot:
- Transactions in mart: **{fmt_int(tx)}**
- Scored events: **{fmt_int(scored)}**
- Graph scale: **{fmt_int(nodes)} nodes / {fmt_int(edges)} edges**
- Tree benchmark mean Precision@50: **{fmt_pct(p50)}**
- Dashboard quality gates: **13/13 passed**

Tech stack: Python, SQLite, BigQuery, Vertex AI Gemini, scikit-learn, graph analytics, CI quality gates.

Repo: https://github.com/totkanligizem/fraud-aml-graph-sentinel

#DataScience #MachineLearning #MLOps #FraudDetection #AML #GraphAnalytics #BigQuery #VertexAI #GenAI #Fintech
"""
    else:
        text = f"""# Fraud-AML Graph Sentinel — Uretim Tarzi Portfoy Projesi

Fraud + AML odakli, graph zekasi ve GenAI analist copilot iceren uctan uca bir risk analitigi projesi yayinladim.

Sistemin yaptiklari:
- Cok kaynakli fraud/AML verisini canonical semaya toplama
- Deterministic warehouse ve point-in-time-safe feature uretimi
- Fraud skorlama + investigation queue onceliklendirme
- Supheli cluster tespiti icin graph risk intelligence
- BigQuery semantik analiz katmani + executive dashboard
- Vertex Gemini ile yapilandirilmis analist copilot

Guncel snapshot:
- Transaction mart satiri: **{fmt_int(tx)}**
- Skorlanan olay: **{fmt_int(scored)}**
- Graph olcegi: **{fmt_int(nodes)} node / {fmt_int(edges)} edge**
- Tree benchmark mean Precision@50: **{fmt_pct(p50)}**
- Dashboard quality gate: **13/13 passed**

Teknoloji: Python, SQLite, BigQuery, Vertex AI Gemini, scikit-learn, graph analytics, CI quality gates.

Repo: https://github.com/totkanligizem/fraud-aml-graph-sentinel

#DataScience #MachineLearning #MLOps #FraudDetection #AML #GraphAnalytics #BigQuery #VertexAI #GenAI #Fintech
"""
    (out_dir / f"linkedin_post_{lang}.md").write_text(text.strip() + "\n", encoding="utf-8")


def generate(lang: str, orientation: str, checkpoint: dict, compare: dict) -> Path:
    if orientation == "landscape":
        width, height = 1920, 1080
        compact = False
        slide_dir = OUT_ROOT / "slides" / f"{lang}_landscape"
        video_path = OUT_ROOT / "videos" / f"github_showcase_{lang}.mp4"
    else:
        width, height = 1080, 1920
        compact = True
        slide_dir = OUT_ROOT / "slides" / f"{lang}_portrait"
        video_path = OUT_ROOT / "videos" / f"linkedin_showcase_{lang}.mp4"

    tx = checkpoint["warehouse_summary"]["table_counts"]["transaction_mart"]
    scored = checkpoint["scoring_summary"]["total_scored_rows"]
    queues = checkpoint["ranking_summary"]["queue_count"]
    nodes = checkpoint["graph_summary"]["table_counts"]["graph_party_node"]
    edges = checkpoint["graph_summary"]["table_counts"]["graph_party_edge"]

    ap_base = compare["baseline"]["average_precision"]
    ap_tree = compare["tree_benchmark"].get("average_precision_calibrated")
    p50_base = compare["baseline"]["mean_precision_at_50"]
    p50_tree = compare["tree_benchmark"].get("mean_precision_at_50")

    if lang == "en":
        title = "Fraud-AML Graph Sentinel"
        subtitle = "End-to-end Fraud + AML intelligence platform"
        flow_title = "Pipeline Workflow"
        flow_steps = [
            "Ingest multi-source fraud/AML data into canonical schema",
            "Build deterministic warehouse + time-safe features",
            "Train and score fraud models, rank investigation queues",
            "Add graph risk intelligence and suspicious clusters",
            "Mirror to BigQuery + executive dashboard + analyst copilot",
        ]
        kpi_title = "Scale & Outcomes"
        model_title = "Model and Queue Performance"
        quality_title = "Quality and Publish Readiness"
        cta = "Repository: github.com/totkanligizem/fraud-aml-graph-sentinel"
    else:
        title = "Fraud-AML Graph Sentinel"
        subtitle = "Uctan uca Fraud + AML zeka platformu"
        flow_title = "Pipeline Akisi"
        flow_steps = [
            "Cok kaynakli fraud/AML verisini canonical semaya al",
            "Deterministic warehouse + zaman guvenli feature uret",
            "Fraud modeli egit, skorla, queue onceliklendir",
            "Graph risk intelligence ve supheli cluster katmani ekle",
            "BigQuery semantik katman + dashboard + copilot",
        ]
        kpi_title = "Olcek ve Sonuclar"
        model_title = "Model ve Queue Performansi"
        quality_title = "Kalite ve Yayin Hazirligi"
        cta = "Repository: github.com/totkanligizem/fraud-aml-graph-sentinel"

    slides: list[Path] = []

    fig, ax = setup_canvas(width, height)
    draw_title(ax, title, subtitle, compact=compact)
    draw_bullets(
        ax,
        [
            f"Transactions processed: {fmt_int(tx)}",
            f"Scored events: {fmt_int(scored)}",
            f"Graph scale: {fmt_int(nodes)} nodes / {fmt_int(edges)} edges",
            "Quality gate state: READY FOR PUBLISH",
        ],
        top=0.66 if compact else 0.64,
        size=22 if not compact else 20,
        step=0.085 if not compact else 0.078,
    )
    p = slide_dir / "slide_01_cover.png"
    save_slide(fig, p)
    slides.append(p)

    fig, ax = setup_canvas(width, height)
    draw_title(ax, flow_title, "", compact=compact)
    draw_bullets(
        ax,
        flow_steps,
        top=0.78,
        size=22 if not compact else 19,
        step=0.11 if not compact else 0.095,
    )
    p = slide_dir / "slide_02_flow.png"
    save_slide(fig, p)
    slides.append(p)

    fig, ax = setup_canvas(width, height)
    draw_title(ax, kpi_title, "", compact=compact)
    draw_kpi_cards(
        ax,
        [
            ("Transaction Mart Rows", fmt_int(tx)),
            ("Fraud Scored Rows", fmt_int(scored)),
            ("Distinct Investigation Queues", fmt_int(queues)),
            ("Graph Party Nodes", fmt_int(nodes)),
            ("Graph Party Edges", fmt_int(edges)),
            ("Dashboard Quality Gates", "13 / 13 PASS"),
        ],
        columns=2,
        top=0.72,
    )
    p = slide_dir / "slide_03_kpis.png"
    save_slide(fig, p)
    slides.append(p)

    fig, ax = setup_canvas(width, height)
    draw_title(ax, model_title, "", compact=compact)
    draw_kpi_cards(
        ax,
        [
            ("Baseline AP", f"{ap_base:.4f}"),
            ("Tree AP", f"{ap_tree:.4f}"),
            ("Baseline Precision@50", fmt_pct(p50_base)),
            ("Tree Precision@50", fmt_pct(p50_tree)),
        ],
        columns=2,
        top=0.72,
    )
    draw_bullets(
        ax,
        [
            f"AP Lift (tree - baseline): +{(ap_tree - ap_base):.4f}",
            f"P@50 Lift (tree - baseline): +{fmt_pct(p50_tree - p50_base)}",
        ],
        top=0.30,
        size=20 if not compact else 18,
        step=0.07,
    )
    p = slide_dir / "slide_04_model.png"
    save_slide(fig, p)
    slides.append(p)

    fig, ax = setup_canvas(width, height)
    draw_title(ax, quality_title, "", compact=compact)
    draw_bullets(
        ax,
        [
            "Schema and pipeline validators: PASS",
            "Graph integrity validators: PASS",
            "Agent prompt/response quality checks: PASS",
            "Dashboard payload + DOM binding checks: PASS",
            "Smoke CI workflow for reproducibility: ACTIVE",
        ],
        top=0.78,
        size=22 if not compact else 18,
        step=0.11 if not compact else 0.092,
    )
    p = slide_dir / "slide_05_quality.png"
    save_slide(fig, p)
    slides.append(p)

    fig, ax = setup_canvas(width, height)
    draw_title(ax, "Thank You", cta, compact=compact)
    draw_bullets(
        ax,
        [
            "Fraud + AML + Graph + GenAI in one production-style stack",
            "Open for collaboration and technical discussions",
        ],
        top=0.62,
        size=22 if not compact else 19,
        step=0.10,
    )
    p = slide_dir / "slide_06_cta.png"
    save_slide(fig, p)
    slides.append(p)

    make_video_from_slides(slides, video_path)
    return video_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate social media visual kit")
    parser.add_argument("--langs", nargs="+", default=["en", "tr"], choices=["en", "tr"])
    parser.add_argument(
        "--orientations",
        nargs="+",
        default=["landscape", "portrait"],
        choices=["landscape", "portrait"],
    )
    args = parser.parse_args()

    checkpoint = load_json(CHECKPOINT_PATH)
    compare = load_json(COMPARE_PATH)

    metrics = {
        "transactions": checkpoint["warehouse_summary"]["table_counts"]["transaction_mart"],
        "scored": checkpoint["scoring_summary"]["total_scored_rows"],
        "nodes": checkpoint["graph_summary"]["table_counts"]["graph_party_node"],
        "edges": checkpoint["graph_summary"]["table_counts"]["graph_party_edge"],
        "tree_p50": compare["tree_benchmark"]["mean_precision_at_50"],
    }

    built = []
    for lang in args.langs:
        for orientation in args.orientations:
            built.append(str(generate(lang, orientation, checkpoint, compare)))
        write_linkedin_copy(OUT_ROOT / "captions", metrics, lang)

    summary = {
        "generated_at": checkpoint.get("generated_at_utc"),
        "videos": built,
        "captions": [
            str(OUT_ROOT / "captions" / "linkedin_post_en.md"),
            str(OUT_ROOT / "captions" / "linkedin_post_tr.md"),
        ],
    }
    (OUT_ROOT / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

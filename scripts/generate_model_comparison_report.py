#!/usr/bin/env python3
"""Generate baseline vs benchmark model comparison report."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate model comparison report.")
    parser.add_argument(
        "--expected-db-path",
        default="data/warehouse/ledger_sentinel.db",
        help="Expected warehouse db_path for artifact selection",
    )
    parser.add_argument(
        "--baseline-metrics",
        default="artifacts/models/fraud_baseline/latest/metrics.json",
        help="Baseline metrics.json path",
    )
    parser.add_argument(
        "--benchmark-metrics",
        default="artifacts/models/fraud_benchmark_numpy/latest/metrics.json",
        help="Benchmark metrics.json path",
    )
    parser.add_argument(
        "--baseline-ranking",
        default="artifacts/models/ranking/latest/ranking-summary.json",
        help="Baseline ranking summary path",
    )
    parser.add_argument(
        "--benchmark-ranking",
        default="artifacts/models/ranking_benchmark/latest/ranking-summary.json",
        help="Benchmark ranking summary path",
    )
    parser.add_argument(
        "--tree-metrics",
        default="artifacts/models/fraud_tree_benchmark/latest/metrics.json",
        help="Optional tree benchmark metrics.json path",
    )
    parser.add_argument(
        "--tree-ranking",
        default="artifacts/models/ranking_tree/latest/ranking-summary.json",
        help="Optional tree benchmark ranking summary path",
    )
    parser.add_argument(
        "--out-json",
        default="reports/08_Model_Benchmark_Comparison_Snapshot.json",
        help="Output JSON snapshot path",
    )
    parser.add_argument(
        "--out-md",
        default="reports/08_Model_Benchmark_Comparison_EN.md",
        help="Output markdown path",
    )
    return parser.parse_args()


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_path(value: object) -> str:
    return Path(str(value)).as_posix()


def json_matches_db_path(path: Path, expected_db_path: str) -> bool:
    payload = load_json(path)
    db_path = payload.get("db_path")
    if db_path is None:
        return False
    return normalize_path(db_path) == normalize_path(expected_db_path)


def resolve_json_path(preferred: Path) -> Path:
    if preferred.exists():
        return preferred
    if preferred.name == "ranking-summary.json" and preferred.parent.name == "latest":
        root = preferred.parent.parent
        candidates = sorted(root.glob("*/ranking-summary.json"))
        if candidates:
            return candidates[-1]
    raise FileNotFoundError(f"Missing file: {preferred}")


def resolve_optional_json_path(preferred: Path) -> Path | None:
    try:
        return resolve_json_path(preferred)
    except FileNotFoundError:
        return None


def resolve_json_path_matching_db(preferred: Path, expected_db_path: str) -> Path:
    # First try preferred path if it exists and matches the expected db.
    if preferred.exists() and json_matches_db_path(preferred, expected_db_path):
        return preferred

    # Then try siblings under the same artifact family.
    if preferred.parent.name == "latest":
        root = preferred.parent.parent
        candidates = sorted(root.glob(f"*/{preferred.name}"))
        for candidate in reversed(candidates):
            if json_matches_db_path(candidate, expected_db_path):
                return candidate

    if preferred.exists():
        raise ValueError(
            f"Artifact db_path mismatch: {preferred} does not match expected db_path={expected_db_path}"
        )

    raise FileNotFoundError(
        f"Missing artifact matching db_path={expected_db_path}: {preferred}"
    )


def resolve_optional_json_path_matching_db(preferred: Path, expected_db_path: str) -> Path | None:
    try:
        return resolve_json_path_matching_db(preferred, expected_db_path)
    except (FileNotFoundError, ValueError):
        return None


def safe_get(d: Dict[str, Any], *keys: str, default: float | None = None) -> float | None:
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    if isinstance(cur, (int, float)):
        return float(cur)
    return default


def fmt(v: float | None, digits: int = 6) -> str:
    if v is None:
        return "n/a"
    return f"{v:.{digits}f}"


def feature_count(payload: Dict[str, Any] | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("feature_count")
    if isinstance(value, (int, float)):
        return int(value)
    return None


def main() -> None:
    args = parse_args()
    expected_db_path = args.expected_db_path
    baseline_metrics_path = resolve_json_path_matching_db(Path(args.baseline_metrics), expected_db_path)
    benchmark_metrics_path = resolve_json_path(Path(args.benchmark_metrics))
    baseline_ranking_path = resolve_json_path_matching_db(Path(args.baseline_ranking), expected_db_path)
    benchmark_ranking_path = resolve_json_path(Path(args.benchmark_ranking))

    baseline_metrics = load_json(baseline_metrics_path)
    benchmark_metrics = load_json(benchmark_metrics_path)
    baseline_ranking = load_json(baseline_ranking_path)
    benchmark_ranking = load_json(benchmark_ranking_path)
    tree_metrics_path = resolve_optional_json_path_matching_db(Path(args.tree_metrics), expected_db_path)
    tree_ranking_path = resolve_optional_json_path_matching_db(Path(args.tree_ranking), expected_db_path)
    tree_metrics = load_json(tree_metrics_path) if tree_metrics_path else None
    tree_ranking = load_json(tree_ranking_path) if tree_ranking_path else None
    tree_skip_reason = ""

    baseline_feature_count = feature_count(baseline_metrics)
    tree_feature_count = feature_count(tree_metrics)
    if (
        tree_metrics is not None
        and baseline_feature_count is not None
        and tree_feature_count is not None
        and tree_feature_count != baseline_feature_count
    ):
        tree_skip_reason = (
            "tree feature_count mismatch vs baseline "
            f"({tree_feature_count} != {baseline_feature_count}); rerun tree benchmark"
        )
        tree_metrics = None
        tree_ranking = None
        tree_metrics_path = None
        tree_ranking_path = None

    base_ap = safe_get(baseline_metrics, "metrics", "average_precision")
    base_pr_auc = safe_get(baseline_metrics, "metrics", "pr_auc_trapz")
    base_roc_auc = safe_get(baseline_metrics, "metrics", "roc_auc")

    bench_ap_raw = safe_get(benchmark_metrics, "metrics_raw", "average_precision")
    bench_ap_cal = safe_get(benchmark_metrics, "metrics_calibrated", "average_precision")
    bench_pr_auc_raw = safe_get(benchmark_metrics, "metrics_raw", "pr_auc_trapz")
    bench_pr_auc_cal = safe_get(benchmark_metrics, "metrics_calibrated", "pr_auc_trapz")
    bench_roc_raw = safe_get(benchmark_metrics, "metrics_raw", "roc_auc")
    bench_roc_cal = safe_get(benchmark_metrics, "metrics_calibrated", "roc_auc")

    base_p50 = safe_get(baseline_ranking, "mean_precision_at_k")
    base_ndcg50 = safe_get(baseline_ranking, "mean_ndcg_at_k")
    bench_p50 = safe_get(benchmark_ranking, "mean_precision_at_k")
    bench_ndcg50 = safe_get(benchmark_ranking, "mean_ndcg_at_k")

    tree_ap_cal = safe_get(tree_metrics, "metrics_calibrated", "average_precision") if tree_metrics else None
    tree_pr_auc_cal = safe_get(tree_metrics, "metrics_calibrated", "pr_auc_trapz") if tree_metrics else None
    tree_roc_cal = safe_get(tree_metrics, "metrics_calibrated", "roc_auc") if tree_metrics else None
    tree_p50 = safe_get(tree_ranking, "mean_precision_at_k") if tree_ranking else None
    tree_ndcg50 = safe_get(tree_ranking, "mean_ndcg_at_k") if tree_ranking else None

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "baseline": {
            "metrics_path": str(baseline_metrics_path),
            "ranking_path": str(baseline_ranking_path),
            "average_precision": base_ap,
            "pr_auc_trapz": base_pr_auc,
            "roc_auc": base_roc_auc,
            "mean_precision_at_50": base_p50,
            "mean_ndcg_at_50": base_ndcg50,
        },
        "benchmark": {
            "metrics_path": str(benchmark_metrics_path),
            "ranking_path": str(benchmark_ranking_path),
            "average_precision_raw": bench_ap_raw,
            "average_precision_calibrated": bench_ap_cal,
            "pr_auc_trapz_raw": bench_pr_auc_raw,
            "pr_auc_trapz_calibrated": bench_pr_auc_cal,
            "roc_auc_raw": bench_roc_raw,
            "roc_auc_calibrated": bench_roc_cal,
            "mean_precision_at_50": bench_p50,
            "mean_ndcg_at_50": bench_ndcg50,
        },
        "tree_benchmark": {
            "available": bool(tree_metrics and tree_ranking),
            "metrics_path": str(tree_metrics_path) if tree_metrics_path else "",
            "ranking_path": str(tree_ranking_path) if tree_ranking_path else "",
            "skip_reason": tree_skip_reason,
            "average_precision_calibrated": tree_ap_cal,
            "pr_auc_trapz_calibrated": tree_pr_auc_cal,
            "roc_auc_calibrated": tree_roc_cal,
            "mean_precision_at_50": tree_p50,
            "mean_ndcg_at_50": tree_ndcg50,
        },
        "deltas": {
            "ap_delta_calibrated_vs_baseline": None if base_ap is None or bench_ap_cal is None else bench_ap_cal - base_ap,
            "pr_auc_delta_calibrated_vs_baseline": None
            if base_pr_auc is None or bench_pr_auc_cal is None
            else bench_pr_auc_cal - base_pr_auc,
            "precision_at_50_delta_vs_baseline": None if base_p50 is None or bench_p50 is None else bench_p50 - base_p50,
            "ndcg_at_50_delta_vs_baseline": None if base_ndcg50 is None or bench_ndcg50 is None else bench_ndcg50 - base_ndcg50,
            "tree_ap_delta_calibrated_vs_baseline": None if base_ap is None or tree_ap_cal is None else tree_ap_cal - base_ap,
            "tree_pr_auc_delta_calibrated_vs_baseline": None
            if base_pr_auc is None or tree_pr_auc_cal is None
            else tree_pr_auc_cal - base_pr_auc,
            "tree_precision_at_50_delta_vs_baseline": None if base_p50 is None or tree_p50 is None else tree_p50 - base_p50,
            "tree_ndcg_at_50_delta_vs_baseline": None
            if base_ndcg50 is None or tree_ndcg50 is None
            else tree_ndcg50 - base_ndcg50,
        },
    }

    lines = [
        "# Model Benchmark Comparison (EN)",
        "",
        f"Generated at (UTC): {summary['generated_at_utc']}",
        "",
        "## Baseline vs Benchmark",
        "",
        "| Metric | Baseline | Benchmark Raw | Benchmark Calibrated | Delta (Calibrated - Baseline) |",
        "|---|---:|---:|---:|---:|",
        f"| Average Precision | {fmt(base_ap)} | {fmt(bench_ap_raw)} | {fmt(bench_ap_cal)} | {fmt(summary['deltas']['ap_delta_calibrated_vs_baseline'])} |",
        f"| PR-AUC (trapz) | {fmt(base_pr_auc)} | {fmt(bench_pr_auc_raw)} | {fmt(bench_pr_auc_cal)} | {fmt(summary['deltas']['pr_auc_delta_calibrated_vs_baseline'])} |",
        f"| ROC-AUC | {fmt(base_roc_auc)} | {fmt(bench_roc_raw)} | {fmt(bench_roc_cal)} | n/a |",
        "",
        "## Queue Ranking Comparison",
        "",
        "| Metric | Baseline | Benchmark | Delta |",
        "|---|---:|---:|---:|",
        f"| Mean Precision@50 | {fmt(base_p50)} | {fmt(bench_p50)} | {fmt(summary['deltas']['precision_at_50_delta_vs_baseline'])} |",
        f"| Mean NDCG@50 | {fmt(base_ndcg50)} | {fmt(bench_ndcg50)} | {fmt(summary['deltas']['ndcg_at_50_delta_vs_baseline'])} |",
        "",
        "## Optional Tree Benchmark",
        "",
    ]
    if tree_metrics and tree_ranking:
        lines.extend(
            [
                "| Metric | Baseline | Tree Calibrated | Delta |",
                "|---|---:|---:|---:|",
                f"| Average Precision | {fmt(base_ap)} | {fmt(tree_ap_cal)} | {fmt(summary['deltas']['tree_ap_delta_calibrated_vs_baseline'])} |",
                f"| PR-AUC (trapz) | {fmt(base_pr_auc)} | {fmt(tree_pr_auc_cal)} | {fmt(summary['deltas']['tree_pr_auc_delta_calibrated_vs_baseline'])} |",
                f"| ROC-AUC | {fmt(base_roc_auc)} | {fmt(tree_roc_cal)} | n/a |",
                f"| Mean Precision@50 | {fmt(base_p50)} | {fmt(tree_p50)} | {fmt(summary['deltas']['tree_precision_at_50_delta_vs_baseline'])} |",
                f"| Mean NDCG@50 | {fmt(base_ndcg50)} | {fmt(tree_ndcg50)} | {fmt(summary['deltas']['tree_ndcg_at_50_delta_vs_baseline'])} |",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "Tree benchmark artifacts are not available yet.",
                (f"Reason: {tree_skip_reason}" if tree_skip_reason else "Reason: no db-matched tree artifacts found."),
                "To generate them, install `scikit-learn` and run:",
                "- `make tree-benchmark-pipeline`",
                "",
            ]
        )

    lines.extend(
        [
        "## Notes",
        "",
        "- Benchmark model: interaction-augmented numpy logistic regression with Platt calibration.",
        f"- Artifact selection is pinned to db_path: `{expected_db_path}`.",
        "- Ranking summaries are loaded from db-matched baseline/benchmark artifacts.",
        "",
        ]
    )

    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    out_md.write_text("\n".join(lines), encoding="utf-8")

    print(
        json.dumps(
            {
                "generated_at_utc": summary["generated_at_utc"],
                "out_json": str(out_json),
                "out_md": str(out_md),
                "ap_delta": summary["deltas"]["ap_delta_calibrated_vs_baseline"],
                "pr_auc_delta": summary["deltas"]["pr_auc_delta_calibrated_vs_baseline"],
                "p50_delta": summary["deltas"]["precision_at_50_delta_vs_baseline"],
                "tree_available": summary["tree_benchmark"]["available"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

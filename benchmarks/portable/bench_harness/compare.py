"""Render a markdown side-by-side comparison from a combined run dir.

Reads `<run-dir>/summary.json` and emits `<run-dir>/COMPARISON.md`
with headline metric tables (throughput, end-to-end latency, claim
latency, dead-tuple totals) plus methodology and caveats. Pure
post-processing — safe to re-run, no docker, no Postgres.

CLI is wired in `long_horizon.py` as the `compare` subcommand.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Headline metrics shown in the cross-system table. Order is the
# table row order. Adapter sample sets vary — missing metrics render
# as "—" rather than failing.
HEADLINE_METRICS = [
    ("completion_rate", "Throughput (jobs/s, sustained)"),
    ("enqueue_rate", "Enqueue rate (jobs/s offered)"),
    ("end_to_end_p50_ms", "End-to-end latency p50 (ms)"),
    ("end_to_end_p95_ms", "End-to-end latency p95 (ms)"),
    ("end_to_end_p99_ms", "End-to-end latency p99 (ms)"),
    ("claim_p95_ms", "Claim latency p95 (ms)"),
    ("producer_p95_ms", "Producer latency p95 (ms)"),
    ("producer_call_p95_ms", "Producer call latency p95 (ms)"),
]


def _fmt_value(metric_value: dict | None, key: str = "median") -> str:
    if metric_value is None:
        return "—"
    v = metric_value.get(key)
    if v is None:
        return "—"
    if isinstance(v, float):
        if abs(v) < 0.01:
            return f"{v:.4f}"
        if abs(v) < 100:
            return f"{v:.3f}"
        return f"{v:,.1f}"
    return str(v)


def _system_dead_tuple_total(system_payload: dict, phase: str) -> tuple[int, int]:
    """Sum `n_dead_tup@*` median + peak for a single system in a single
    phase. Single number per system makes the cross-system comparison
    readable at a glance; per-table breakdown stays in `summary.json`
    and the per-table plots."""
    metrics = system_payload.get("phases", {}).get(phase, {}).get("metrics", {})
    median_total = 0.0
    peak_total = 0.0
    for key, payload in metrics.items():
        if not key.startswith("n_dead_tup@"):
            continue
        if not isinstance(payload, dict):
            continue
        median_total += payload.get("median") or 0.0
        peak_total += payload.get("peak") or 0.0
    return int(round(median_total)), int(round(peak_total))


def render(summary: dict, out_path: Path, *, phase: str = "clean_1") -> None:
    systems = list(summary.get("systems", {}).keys())
    if not systems:
        raise SystemExit("summary has no systems")

    lines: list[str] = []
    lines.append("# Cross-System Comparison")
    lines.append("")
    run_id = summary.get("run_id", "(unknown)")
    lines.append(f"Generated from `{run_id}`. Phase: `{phase}`.")
    lines.append("")
    lines.append(
        "Numbers are **medians across the clean phase**. The interactive "
        "cross-system report (`index.html`) carries the full timeline and "
        "per-replica overlays. Raw samples are in `raw.csv`."
    )
    lines.append("")

    phase_secs = None
    for p in summary.get("phases", []):
        if p.get("label") == phase:
            phase_secs = p.get("duration_s")
    if phase_secs:
        lines.append(
            f"**Phase shape:** `{phase}` ran for {phase_secs}s "
            f"({phase_secs // 60} minutes) after a 5-minute warmup that's "
            "excluded from these aggregates."
        )
        lines.append("")
    lines.append(
        "**Workload:** 200 jobs/s offered load, 8 worker concurrency, 1 "
        "replica per system. Same Postgres image, same scenario, same producer."
    )
    lines.append("")

    # Headline metric table.
    lines.append("## Headline metrics")
    lines.append("")
    header = "| Metric | " + " | ".join(systems) + " |"
    sep = "|" + "---|" * (len(systems) + 1)
    lines.append(header)
    lines.append(sep)
    for metric_key, label in HEADLINE_METRICS:
        cells = [label]
        for sys_name in systems:
            metrics = (
                summary["systems"][sys_name]
                .get("phases", {})
                .get(phase, {})
                .get("metrics", {})
            )
            cells.append(_fmt_value(metrics.get(metric_key)))
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    lines.append(
        "A dash (`—`) means the adapter doesn't sample that metric. Adapter "
        "metric sets vary; the awa adapter samples producer / claim / end-to-end "
        "latency, while peer adapters often sample only their native equivalent. "
        "See each adapter's `main.py` / `bench.rs` for the exact sample set."
    )
    lines.append("")

    # Dead-tuple totals.
    lines.append("## Dead tuples — totals across queue-storage / adapter tables")
    lines.append("")
    lines.append(
        "Sum of `n_dead_tup@*` across every sampled table. Median and peak "
        "are taken across the clean phase; lower is better. Per-table breakdown "
        "lives in `summary.json` under each system's metrics block."
    )
    lines.append("")
    lines.append("| System | Median | Peak |")
    lines.append("|---|---:|---:|")
    for sys_name in systems:
        med, peak = _system_dead_tuple_total(summary["systems"][sys_name], phase)
        lines.append(f"| {sys_name} | {med:,} | {peak:,} |")
    lines.append("")

    # Caveats.
    lines.append("## Caveats")
    lines.append("")
    lines.append(
        "- `awa-canonical` is the same Rust binary as `awa` forced onto the "
        "pre-0.6 storage path. Useful as the within-codebase before/after; "
        "not an independent system."
    )
    lines.append(
        "- `awa-python` runs the same Rust core via PyO3 — differences vs. "
        "`awa` reflect the FFI overhead, not the storage engine. The "
        "Python-side bench harness samples a smaller metric set than the "
        "Rust adapter, so several latency rows show as `—`."
    )
    lines.append(
        "- River is a Go job framework, not Postgres-native — its claim path "
        "uses different SQL and its lifecycle is framework-shaped. Compare "
        "on what each system promises, not on apples-to-apples lifecycle."
    )
    lines.append(
        "- Dead tuples are heavily affected by autovacuum cadence "
        "(`autovacuum_naptime=60s` here) and per-table thresholds. Lower "
        "median is good; a large peak with a stable median means vacuum is "
        "keeping up — that's not a regression."
    )
    lines.append(
        "- Throughput should track offered load (200/s). A system completing "
        "well below that means the worker pool is the bottleneck, not the "
        "queue engine."
    )
    lines.append("")

    out_path.write_text("\n".join(lines))


def add_subparser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "compare",
        help="Render a markdown side-by-side comparison from a combined run dir.",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "run_dir",
        type=Path,
        help="Combined run directory containing summary.json (output of `combine`).",
    )
    parser.add_argument(
        "--phase",
        default="clean_1",
        help="Phase label to summarise (default: clean_1).",
    )
    parser.set_defaults(func=run)
    return parser


def run(args: argparse.Namespace) -> int:
    summary_path: Path = args.run_dir / "summary.json"
    if not summary_path.exists():
        raise SystemExit(f"summary.json not found at {summary_path}")
    summary: dict[str, Any] = json.loads(summary_path.read_text())

    out_path: Path = args.run_dir / "COMPARISON.md"
    render(summary, out_path, phase=args.phase)
    print(f"wrote {out_path}", file=sys.stderr)
    return 0

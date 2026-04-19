#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import re
import shutil
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent.resolve()
RUNNER = SCRIPT_DIR / "run.py"
CHAOS = SCRIPT_DIR / "chaos.py"
RESULTS_DIR = SCRIPT_DIR / "results"
RESULT_PATH_RE = re.compile(r"Results saved to (.+)")

sys.path.insert(0, str(SCRIPT_DIR))
from bench_harness.versions import capture_all, format_revision_oneline  # noqa: E402

DEFAULT_SYSTEMS = [
    "awa",
    "awa-docker",
    "awa-python",
    "procrastinate",
    "river",
    "oban",
    "pgque",
]

CHART_COLORS = [
    "#2563eb",
    "#7c3aed",
    "#db2777",
    "#ea580c",
    "#0891b2",
    "#16a34a",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_log(log_path: Path, message: str) -> None:
    with log_path.open("a") as handle:
        handle.write(message)


def write_status(status_path: Path, payload: dict) -> None:
    status_path.write_text(json.dumps(payload, indent=2) + "\n")


def run_command(
    cmd: list[str], *, log_path: Path, phase_label: str
) -> subprocess.CompletedProcess[str]:
    command_text = f"$ {' '.join(cmd)}"
    print(command_text, file=sys.stderr)
    append_log(log_path, f"\n[{utc_now()}] {phase_label}\n{command_text}\n")
    completed = subprocess.run(
        cmd,
        cwd=str(SCRIPT_DIR.parent.parent),
        capture_output=True,
        text=True,
        timeout=7200,
    )
    if completed.stdout:
        sys.stdout.write(completed.stdout)
        append_log(log_path, f"[stdout]\n{completed.stdout}\n")
    if completed.stderr:
        sys.stderr.write(completed.stderr)
        append_log(log_path, f"[stderr]\n{completed.stderr}\n")
    append_log(log_path, f"[exit] {completed.returncode}\n")
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}: {' '.join(cmd)}"
        )
    return completed


def extract_result_path(stderr: str) -> Path:
    match = RESULT_PATH_RE.search(stderr)
    if not match:
        raise RuntimeError("Could not find result file in command output")
    return Path(match.group(1).strip())


def run_benchmarks(system: str, args: argparse.Namespace, *, log_path: Path) -> dict:
    cmd = [
        "uv",
        "run",
        "python",
        str(RUNNER),
        "--systems",
        system,
        "--scenario",
        "all",
        "--job-count",
        str(args.benchmark_job_count),
        "--worker-count",
        str(args.benchmark_worker_count),
        "--latency-iterations",
        str(args.latency_iterations),
        "--pg-image",
        args.pg_image,
    ]
    if args.skip_build:
        cmd.append("--skip-build")

    completed = run_command(cmd, log_path=log_path, phase_label=f"benchmark:{system}")
    result_path = extract_result_path(completed.stderr)
    with result_path.open() as handle:
        return json.load(handle)


def run_chaos(system: str, args: argparse.Namespace, *, log_path: Path) -> list[dict]:
    cmd = [
        "uv",
        "run",
        "python",
        str(CHAOS),
        "--systems",
        system,
        "--suite",
        args.chaos_suite,
        "--job-count",
        str(args.chaos_job_count),
        "--pg-image",
        args.pg_image,
    ]
    completed = run_command(cmd, log_path=log_path, phase_label=f"chaos:{system}")
    result_path = extract_result_path(completed.stderr)
    with result_path.open() as handle:
        return json.load(handle)["results"]


def summarize_system(benchmark_payload: dict, chaos_payload: list[dict]) -> dict:
    benchmark_results = {
        item["scenario"]: item["results"] for item in benchmark_payload["results"]
    }
    chaos_results = {}
    for item in chaos_payload:
        if "results" in item:
            chaos_results[item["scenario"]] = item["results"]
        else:
            chaos_results[item["scenario"]] = {"error": item.get("error", "unknown")}
    return {
        "benchmarks": benchmark_results,
        "chaos": chaos_results,
    }


def summarize_repetitions(repetitions: list[dict]) -> dict:
    benchmark_scenarios: dict[str, list[dict]] = {}
    chaos_scenarios: dict[str, list[dict]] = {}

    for repetition in repetitions:
        for scenario, results in repetition["summary"]["benchmarks"].items():
            benchmark_scenarios.setdefault(scenario, []).append(results)
        for scenario, results in repetition["summary"]["chaos"].items():
            chaos_scenarios.setdefault(scenario, []).append(results)

    benchmark_summary: dict[str, dict] = {}
    for scenario, runs in benchmark_scenarios.items():
        if "jobs_per_sec" in runs[0]:
            values = [run["jobs_per_sec"] for run in runs]
            benchmark_summary[scenario] = {
                "mean_jobs_per_sec": statistics.mean(values),
                "min_jobs_per_sec": min(values),
                "max_jobs_per_sec": max(values),
                "stdev_jobs_per_sec": statistics.stdev(values)
                if len(values) > 1
                else 0.0,
            }
        else:
            p50_values = [run["p50_us"] for run in runs]
            p95_values = [run["p95_us"] for run in runs]
            p99_values = [run["p99_us"] for run in runs]
            benchmark_summary[scenario] = {
                "mean_p50_us": statistics.mean(p50_values),
                "min_p50_us": min(p50_values),
                "max_p50_us": max(p50_values),
                "mean_p95_us": statistics.mean(p95_values),
                "mean_p99_us": statistics.mean(p99_values),
            }

    chaos_summary: dict[str, dict] = {}
    for scenario, runs in chaos_scenarios.items():
        successful_runs = [run for run in runs if "error" not in run]
        if not successful_runs:
            chaos_summary[scenario] = {
                "errors": [run.get("error", "unknown") for run in runs],
            }
            continue

        summary = {
            "runs": len(runs),
            "successes": len(successful_runs),
            "max_jobs_lost": max(run.get("jobs_lost", 0) for run in successful_runs),
        }
        if "total_time_secs" in successful_runs[0]:
            total_times = [run["total_time_secs"] for run in successful_runs]
            summary.update(
                {
                    "mean_total_time_secs": statistics.mean(total_times),
                    "max_total_time_secs": max(total_times),
                }
            )
        if "rescue_time_secs" in successful_runs[0]:
            rescue_times = [run["rescue_time_secs"] for run in successful_runs]
            summary.update(
                {
                    "mean_rescue_time_secs": statistics.mean(rescue_times),
                    "max_rescue_time_secs": max(rescue_times),
                }
            )
        chaos_summary[scenario] = summary

    return {
        "benchmarks": benchmark_summary,
        "chaos": chaos_summary,
    }


def print_summary(summary: dict[str, dict]) -> None:
    print("\n" + "=" * 70)
    print("PORTABLE FULL SUITE SUMMARY")
    print("=" * 70)
    for system, data in summary.items():
        print(f"\n--- {system} ---")
        worker = data["benchmarks"].get("worker_throughput", {})
        latency = data["benchmarks"].get("pickup_latency", {})
        print(
            "  benchmark: "
            f"worker_throughput={worker.get('jobs_per_sec', 0):,.0f} jobs/s, "
            f"pickup p50={latency.get('p50_us', 0):,.0f}us"
        )
        for scenario, result in data["chaos"].items():
            if "error" in result:
                print(f"  chaos {scenario}: ERROR {result['error']}")
                continue
            if "jobs_lost" in result:
                print(
                    f"  chaos {scenario}: lost={result['jobs_lost']} "
                    f"total={result.get('total_time_secs', 0):.1f}s"
                )
            elif "low_priority_starved" in result:
                print(
                    f"  chaos {scenario}: low_starved={result['low_priority_starved']} "
                    f"total={result.get('total_time_secs', 0):.1f}s"
                )


def print_repetition_summary(summary: dict[str, dict]) -> None:
    print("\n" + "=" * 70)
    print("PORTABLE FULL SUITE REPETITION SUMMARY")
    print("=" * 70)
    for system, data in summary.items():
        print(f"\n--- {system} ---")
        worker = data["benchmarks"].get("worker_throughput", {})
        latency = data["benchmarks"].get("pickup_latency", {})
        if worker:
            print(
                "  benchmark worker_throughput: "
                f"mean={worker.get('mean_jobs_per_sec', 0):,.0f} jobs/s "
                f"stdev={worker.get('stdev_jobs_per_sec', 0):,.0f} "
                f"range=[{worker.get('min_jobs_per_sec', 0):,.0f}, {worker.get('max_jobs_per_sec', 0):,.0f}]"
            )
        if latency:
            print(
                "  benchmark pickup_latency: "
                f"mean_p50={latency.get('mean_p50_us', 0):,.0f}us "
                f"range=[{latency.get('min_p50_us', 0):,.0f}, {latency.get('max_p50_us', 0):,.0f}]"
            )
        for scenario, result in data["chaos"].items():
            if "errors" in result:
                print(f"  chaos {scenario}: all runs errored")
                continue
            print(
                f"  chaos {scenario}: successes={result.get('successes', 0)}/{result.get('runs', 0)} "
                f"max_lost={result.get('max_jobs_lost', 0)} "
                f"mean_total={result.get('mean_total_time_secs', 0):.1f}s"
            )


def _versions_preamble(systems: list[str]) -> list[str]:
    """Emit an 'Adapter versions' table at the top of a full_suite report.

    full_suite.py runs multiple reps across adapters and aggregates. The
    per-run manifest.json already carries revision info (orchestrator
    writes it); at the aggregate layer the operator-facing Markdown
    benefits from having the same info inlined so a report stands alone.
    """
    revisions = capture_all(systems)
    lines = ["## Adapter versions", ""]
    lines.append("| System | Revision |")
    lines.append("| :--- | :--- |")
    for system in sorted(revisions):
        # The aggregate report doesn't have the runtime descriptor
        # (adapter.started_at etc.) — synthesise a minimal entry so
        # format_revision_oneline renders just the revision half.
        entry = {"revision": revisions[system]}
        lines.append(
            f"| `{system}` | {format_revision_oneline(system, entry)} |"
        )
    lines.append("")
    lines.append(
        "_Full detail (adapter runtime version, schema version, docker image "
        "digest) is in each run's `manifest.json` under "
        "`adapters.<system>`._"
    )
    lines.append("")
    return lines


def write_benchmark_exports(summary: dict[str, dict], timestamp: str) -> None:
    markdown_path = RESULTS_DIR / f"benchmark_summary_{timestamp}.md"
    csv_path = RESULTS_DIR / f"benchmark_summary_{timestamp}.csv"

    benchmark_rows: list[dict[str, object]] = []
    for system, data in summary.items():
        row: dict[str, object] = {"system": system}
        for scenario, values in data["benchmarks"].items():
            if "mean_jobs_per_sec" in values:
                row[f"{scenario}_mean_jobs_per_sec"] = round(
                    values["mean_jobs_per_sec"], 3
                )
                row[f"{scenario}_stdev_jobs_per_sec"] = round(
                    values["stdev_jobs_per_sec"], 3
                )
                row[f"{scenario}_min_jobs_per_sec"] = round(
                    values["min_jobs_per_sec"], 3
                )
                row[f"{scenario}_max_jobs_per_sec"] = round(
                    values["max_jobs_per_sec"], 3
                )
            else:
                row[f"{scenario}_mean_p50_us"] = round(values["mean_p50_us"], 3)
                row[f"{scenario}_mean_p95_us"] = round(values["mean_p95_us"], 3)
                row[f"{scenario}_mean_p99_us"] = round(values["mean_p99_us"], 3)
                row[f"{scenario}_min_p50_us"] = round(values["min_p50_us"], 3)
                row[f"{scenario}_max_p50_us"] = round(values["max_p50_us"], 3)
        benchmark_rows.append(row)

    fieldnames = sorted(
        {key for row in benchmark_rows for key in row.keys()},
        key=lambda key: (key != "system", key),
    )
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(benchmark_rows)

    lines: list[str] = ["# Benchmark Summary", ""]
    lines.extend(_versions_preamble(list(summary.keys())))
    lines.extend(
        [
            "## Results",
            "",
            "| System | Worker Throughput Mean | Worker Throughput Stdev | Pickup p50 Mean |",
            "|---|---:|---:|---:|",
        ]
    )
    for system, data in summary.items():
        worker = data["benchmarks"].get("worker_throughput", {})
        latency = data["benchmarks"].get("pickup_latency", {})
        lines.append(
            f"| {system} | {worker.get('mean_jobs_per_sec', 0):,.0f} | {worker.get('stdev_jobs_per_sec', 0):,.0f} | {latency.get('mean_p50_us', 0):,.0f} |"
        )
    markdown_path.write_text("\n".join(lines) + "\n")


def write_chaos_exports(summary: dict[str, dict], timestamp: str) -> None:
    markdown_path = RESULTS_DIR / f"chaos_summary_{timestamp}.md"
    csv_path = RESULTS_DIR / f"chaos_summary_{timestamp}.csv"

    chaos_rows: list[dict[str, object]] = []
    for system, data in summary.items():
        for scenario, values in data["chaos"].items():
            row = {
                "system": system,
                "scenario": scenario,
            }
            row.update(values)
            chaos_rows.append(row)

    fieldnames = sorted(
        {key for row in chaos_rows for key in row.keys()},
        key=lambda key: (key not in {"system", "scenario"}, key),
    )
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chaos_rows)

    lines: list[str] = ["# Chaos Summary", ""]
    lines.extend(_versions_preamble(list(summary.keys())))
    lines.extend(
        [
            "## Results",
            "",
            "| System | Scenario | Successes | Max Lost | Mean Total (s) |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for system, data in summary.items():
        for scenario, values in data["chaos"].items():
            lines.append(
                f"| {system} | {scenario} | {values.get('successes', 0)}/{values.get('runs', 0)} | {values.get('max_jobs_lost', 0)} | {values.get('mean_total_time_secs', 0):.1f} |"
            )
    markdown_path.write_text("\n".join(lines) + "\n")


def _safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _mean_total_time(values: dict) -> float:
    if "mean_total_time_secs" in values:
        return float(values["mean_total_time_secs"])
    return 0.0


def _chaos_failure_count(data: dict) -> int:
    failures = 0
    for values in data["chaos"].values():
        if "errors" in values:
            failures += 1
            continue
        if values.get("max_jobs_lost", 0) > 0:
            failures += 1
    return failures


def _chart_card(title: str, description: str, asset_name: str) -> str:
    return (
        f"<div class='chart-card'><h3>{html.escape(title)}</h3>"
        f"<p class='muted'>{html.escape(description)}</p>"
        f"<img src='assets/{html.escape(asset_name)}' alt='{html.escape(title)}'></div>"
    )


def _svg_bar_chart(
    title: str,
    subtitle: str,
    items: list[tuple[str, float]],
    *,
    value_suffix: str = "",
    decimals: int = 0,
) -> str:
    width = 920
    height = 420
    margin_left = 180
    margin_right = 80
    margin_top = 70
    margin_bottom = 50
    chart_width = width - margin_left - margin_right
    row_gap = 12
    bar_height = 32
    chart_height = len(items) * (bar_height + row_gap) - row_gap if items else 1
    # When every item is 0 (e.g. chaos_failures when all systems pass),
    # max() returns 0 and chart_width / 0 would crash. Treat as unit scale:
    # every bar collapses to the minimum 2px width rendered below.
    max_value = max((value for _, value in items), default=1.0)
    scale = chart_width / max_value if max_value > 0 else 0.0
    svg_height = max(height, margin_top + margin_bottom + chart_height)
    rows: list[str] = [
        f"<text x='24' y='34' font-size='24' font-weight='700' fill='#0f172a'>{html.escape(title)}</text>",
        f"<text x='24' y='56' font-size='13' fill='#475569'>{html.escape(subtitle)}</text>",
    ]
    for index, (label, value) in enumerate(items):
        y = margin_top + index * (bar_height + row_gap)
        bar_width = max(2.0, value * scale)
        color = CHART_COLORS[index % len(CHART_COLORS)]
        value_text = f"{value:,.{decimals}f}{value_suffix}"
        rows.append(
            f"<text x='{margin_left - 12}' y='{y + 21}' text-anchor='end' font-size='13' fill='#0f172a'>{html.escape(label)}</text>"
        )
        rows.append(
            f"<rect x='{margin_left}' y='{y}' width='{bar_width:.1f}' height='{bar_height}' rx='6' fill='{color}' />"
        )
        rows.append(
            f"<text x='{margin_left + bar_width + 10:.1f}' y='{y + 21}' font-size='12' fill='#334155'>{html.escape(value_text)}</text>"
        )
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{svg_height}' viewBox='0 0 {width} {svg_height}'>"
        "<rect width='100%' height='100%' fill='white' rx='12' />"
        + "".join(rows)
        + "</svg>"
    )


def write_chart_assets(summary: dict[str, dict], report_dir: Path) -> list[str]:
    assets_dir = report_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    charts: list[str] = []
    systems = list(summary.keys())

    worker_items = [
        (
            system,
            float(
                summary[system]["benchmarks"]
                .get("worker_throughput", {})
                .get("mean_jobs_per_sec", 0.0)
            ),
        )
        for system in systems
    ]
    enqueue_items = [
        (
            system,
            float(
                summary[system]["benchmarks"]
                .get("enqueue_throughput", {})
                .get("mean_jobs_per_sec", 0.0)
            ),
        )
        for system in systems
    ]
    pickup_items = [
        (
            system,
            float(
                summary[system]["benchmarks"]
                .get("pickup_latency", {})
                .get("mean_p50_us", 0.0)
                / 1000.0
            ),
        )
        for system in systems
    ]
    failure_items = [
        (system, float(_chaos_failure_count(summary[system]))) for system in systems
    ]
    recovery_items = [
        (system, _mean_total_time(summary[system]["chaos"].get("crash_recovery", {})))
        for system in systems
    ]

    chart_defs = [
        (
            "worker-throughput.svg",
            _svg_bar_chart(
                "Worker Throughput",
                "Mean jobs/sec from the steady-state worker throughput benchmark.",
                sorted(worker_items, key=lambda item: item[1], reverse=True),
                value_suffix=" jobs/s",
                decimals=0,
            ),
        ),
        (
            "enqueue-throughput.svg",
            _svg_bar_chart(
                "Enqueue Throughput",
                "Mean jobs/sec from the bulk enqueue benchmark.",
                sorted(enqueue_items, key=lambda item: item[1], reverse=True),
                value_suffix=" jobs/s",
                decimals=0,
            ),
        ),
        (
            "pickup-latency-p50.svg",
            _svg_bar_chart(
                "Pickup Latency p50",
                "Mean pickup latency median, converted to milliseconds. Lower is better.",
                sorted(pickup_items, key=lambda item: item[1]),
                value_suffix=" ms",
                decimals=1,
            ),
        ),
        (
            "chaos-failures.svg",
            _svg_bar_chart(
                "Chaos Failure Count",
                "Number of chaos scenarios that errored or lost jobs in this run.",
                sorted(failure_items, key=lambda item: item[1], reverse=True),
                value_suffix=" failures",
                decimals=0,
            ),
        ),
        (
            "crash-recovery-total-time.svg",
            _svg_bar_chart(
                "Crash Recovery Total Time",
                "Mean total time for the crash recovery scenario. Lower is better.",
                sorted(recovery_items, key=lambda item: item[1]),
                value_suffix=" s",
                decimals=1,
            ),
        ),
    ]

    for filename, svg in chart_defs:
        (assets_dir / filename).write_text(svg)
        charts.append(filename)

    return charts


def write_html_report(
    *,
    summary: dict[str, dict],
    timestamp: str,
    config: dict,
    output_path: Path,
    log_path: Path,
    status_path: Path,
) -> Path:
    report_dir = RESULTS_DIR / f"full_suite_report_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)
    chart_assets = write_chart_assets(summary, report_dir)

    styles = """
body { font-family: Inter, Arial, sans-serif; margin: 32px auto; max-width: 1280px; color: #1f2937; background: #f8fafc; }
h1 { color: #0f172a; margin-bottom: 4px; }
h2, h3 { color: #0f172a; }
.subtitle { color: #475569; margin: 0 0 24px 0; }
.meta { background: white; border: 1px solid #dbe4ee; border-radius: 8px; padding: 16px; margin-bottom: 24px; display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 8px 24px; }
.meta strong { color: #0f172a; }
.section { margin-bottom: 32px; }
.section h2 { border-bottom: 2px solid #dbe4ee; padding-bottom: 6px; }
table { border-collapse: collapse; width: 100%; background: white; margin: 12px 0 8px 0; font-size: 14px; }
th, td { border: 1px solid #dbe4ee; padding: 8px 10px; text-align: left; }
th { background: #eff6ff; font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
tr:nth-child(even) td { background: #f8fbff; }
tr.row-pass td:first-child { border-left: 4px solid #16a34a; }
tr.row-warn td:first-child { border-left: 4px solid #f59e0b; }
tr.row-fail td:first-child { border-left: 4px solid #dc2626; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
.badge-pass { background: #dcfce7; color: #166534; }
.badge-warn { background: #fef3c7; color: #92400e; }
.badge-fail { background: #fee2e2; color: #991b1b; }
.muted { color: #475569; }
.mono { font-family: ui-monospace, SFMono-Regular, monospace; }
.highlights { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; }
.highlight-card { background: white; border: 1px solid #dbe4ee; border-radius: 8px; padding: 14px 16px; }
.highlight-card .label { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }
.highlight-card .value { font-size: 22px; font-weight: 700; color: #0f172a; margin-top: 4px; }
.highlight-card .detail { color: #475569; font-size: 13px; margin-top: 4px; }
.charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 20px; }
.chart-card { background: white; border: 1px solid #dbe4ee; border-radius: 8px; padding: 16px; }
.chart-card h3 { margin: 0 0 4px 0; font-size: 16px; }
.chart-card img { width: 100%; height: auto; border: 1px solid #e2e8f0; border-radius: 6px; background: white; }
.bundle-files { background: white; border: 1px solid #dbe4ee; border-radius: 8px; padding: 12px 16px; }
.bundle-files ul { margin: 6px 0 0 0; padding-left: 20px; }
.bundle-files li { margin: 2px 0; }
""".strip()
    (report_dir / "styles.css").write_text(styles)

    def table(headers: list[str], rows: list[list[str]]) -> str:
        head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
        body = "".join(
            "<tr>" + "".join(f"<td>{html.escape(cell)}</td>" for cell in row) + "</tr>"
            for row in rows
        )
        return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"

    def chaos_row_class(values: dict) -> str:
        if "errors" in values:
            return "row-fail"
        if values.get("max_jobs_lost", 0) > 0:
            return "row-fail"
        runs = values.get("runs", 0)
        successes = values.get("successes", 0)
        if runs and successes < runs:
            return "row-warn"
        return "row-pass"

    def chaos_badge(values: dict) -> str:
        if "errors" in values:
            return "<span class='badge badge-fail'>error</span>"
        if values.get("max_jobs_lost", 0) > 0:
            return "<span class='badge badge-fail'>job loss</span>"
        runs = values.get("runs", 0)
        successes = values.get("successes", 0)
        if runs and successes < runs:
            return "<span class='badge badge-warn'>partial</span>"
        return "<span class='badge badge-pass'>pass</span>"

    def styled_table(
        headers: list[str],
        rows: list[tuple[str, list[str]]],
        numeric_columns: set[int] | None = None,
    ) -> str:
        numeric_columns = numeric_columns or set()
        head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
        body_parts: list[str] = []
        for row_class, cells in rows:
            cell_html = "".join(
                f"<td class='num'>{cell}</td>"
                if i in numeric_columns
                else f"<td>{cell}</td>"
                for i, cell in enumerate(cells)
            )
            cls = f" class='{row_class}'" if row_class else ""
            body_parts.append(f"<tr{cls}>{cell_html}</tr>")
        return (
            f"<table><thead><tr>{head}</tr></thead>"
            f"<tbody>{''.join(body_parts)}</tbody></table>"
        )

    benchmark_rows: list[tuple[str, list[str]]] = []
    chaos_rows: list[tuple[str, list[str]]] = []
    system_sections: list[str] = []
    chart_cards = [
        _chart_card(
            "Worker Throughput",
            "Cross-system comparison of worker throughput.",
            chart_assets[0],
        ),
        _chart_card(
            "Enqueue Throughput",
            "Cross-system comparison of enqueue throughput.",
            chart_assets[1],
        ),
        _chart_card(
            "Pickup Latency p50",
            "Cross-system comparison of pickup latency medians.",
            chart_assets[2],
        ),
        _chart_card(
            "Chaos Failure Count",
            "How many chaos scenarios failed or errored per system.",
            chart_assets[3],
        ),
        _chart_card(
            "Crash Recovery Total Time",
            "Mean total time for crash recovery by system.",
            chart_assets[4],
        ),
    ]

    for system, data in summary.items():
        worker = data["benchmarks"].get("worker_throughput", {})
        enqueue = data["benchmarks"].get("enqueue_throughput", {})
        latency = data["benchmarks"].get("pickup_latency", {})
        benchmark_rows.append(
            (
                "",
                [
                    html.escape(system),
                    f"{worker.get('mean_jobs_per_sec', 0):,.0f}",
                    f"{enqueue.get('mean_jobs_per_sec', 0):,.0f}",
                    f"{latency.get('mean_p50_us', 0) / 1000:,.2f}",
                    f"{latency.get('mean_p95_us', 0) / 1000:,.2f}",
                    f"{latency.get('mean_p99_us', 0) / 1000:,.2f}",
                ],
            )
        )

        throughput_rows: list[tuple[str, list[str]]] = []
        latency_rows: list[tuple[str, list[str]]] = []
        for scenario, values in sorted(data["benchmarks"].items()):
            if "mean_jobs_per_sec" in values:
                throughput_rows.append(
                    (
                        "",
                        [
                            html.escape(scenario),
                            f"{values.get('mean_jobs_per_sec', 0):,.0f}",
                            f"{values.get('stdev_jobs_per_sec', 0):,.0f}",
                            f"{values.get('min_jobs_per_sec', 0):,.0f}",
                            f"{values.get('max_jobs_per_sec', 0):,.0f}",
                        ],
                    )
                )
            else:
                latency_rows.append(
                    (
                        "",
                        [
                            html.escape(scenario),
                            f"{values.get('mean_p50_us', 0) / 1000:,.2f}",
                            f"{values.get('mean_p95_us', 0) / 1000:,.2f}",
                            f"{values.get('mean_p99_us', 0) / 1000:,.2f}",
                            f"{values.get('min_p50_us', 0) / 1000:,.2f} to {values.get('max_p50_us', 0) / 1000:,.2f}",
                        ],
                    )
                )

        system_chaos_rows: list[tuple[str, list[str]]] = []
        for scenario, values in sorted(data["chaos"].items()):
            cls = chaos_row_class(values)
            badge = chaos_badge(values)
            if "errors" in values:
                errors = "; ".join(values["errors"])
                chaos_rows.append(
                    (
                        cls,
                        [
                            html.escape(system),
                            html.escape(scenario),
                            badge,
                            "0/0",
                            "-",
                            "-",
                            html.escape(errors),
                        ],
                    )
                )
                system_chaos_rows.append(
                    (
                        cls,
                        [
                            html.escape(scenario),
                            badge,
                            "0/0",
                            "-",
                            "-",
                            html.escape(errors),
                        ],
                    )
                )
                continue
            successes = f"{values.get('successes', 0)}/{values.get('runs', 0)}"
            max_lost = str(values.get("max_jobs_lost", 0))
            mean_total = f"{values.get('mean_total_time_secs', 0):.1f}"
            max_total = f"{values.get('max_total_time_secs', 0):.1f}"
            chaos_rows.append(
                (
                    cls,
                    [
                        html.escape(system),
                        html.escape(scenario),
                        badge,
                        successes,
                        max_lost,
                        mean_total,
                        max_total,
                    ],
                )
            )
            system_chaos_rows.append(
                (
                    cls,
                    [
                        html.escape(scenario),
                        badge,
                        successes,
                        max_lost,
                        mean_total,
                        max_total,
                    ],
                )
            )

        throughput_table = (
            styled_table(
                ["Scenario", "Mean jobs/s", "Stdev jobs/s", "Min jobs/s", "Max jobs/s"],
                throughput_rows,
                numeric_columns={1, 2, 3, 4},
            )
            if throughput_rows
            else "<p class='muted'>No throughput data.</p>"
        )
        latency_table = (
            styled_table(
                ["Scenario", "p50 (ms)", "p95 (ms)", "p99 (ms)", "p50 range (ms)"],
                latency_rows,
                numeric_columns={1, 2, 3, 4},
            )
            if latency_rows
            else "<p class='muted'>No latency data.</p>"
        )
        chaos_table = (
            styled_table(
                [
                    "Scenario",
                    "Status",
                    "Successes",
                    "Max Jobs Lost",
                    "Mean Total (s)",
                    "Max Total (s)",
                ],
                system_chaos_rows,
                numeric_columns={2, 3, 4, 5},
            )
            if system_chaos_rows
            else "<p class='muted'>No chaos data.</p>"
        )

        system_sections.append(
            f"<div class='section' id='system-{_safe_slug(system)}'>"
            f"<h2>{html.escape(system)}</h2>"
            f"<h3>Throughput</h3>{throughput_table}"
            f"<h3>Latency</h3>{latency_table}"
            f"<h3>Chaos</h3>{chaos_table}"
            f"</div>"
        )

    # Highlights
    def best(items: list[tuple[str, float]], reverse: bool) -> tuple[str, float] | None:
        filtered = [item for item in items if item[1] > 0]
        if not filtered:
            return None
        return sorted(filtered, key=lambda item: item[1], reverse=reverse)[0]

    worker_items = [
        (
            system,
            float(
                summary[system]["benchmarks"]
                .get("worker_throughput", {})
                .get("mean_jobs_per_sec", 0.0)
            ),
        )
        for system in summary
    ]
    enqueue_items = [
        (
            system,
            float(
                summary[system]["benchmarks"]
                .get("enqueue_throughput", {})
                .get("mean_jobs_per_sec", 0.0)
            ),
        )
        for system in summary
    ]
    pickup_items = [
        (
            system,
            float(
                summary[system]["benchmarks"]
                .get("pickup_latency", {})
                .get("mean_p50_us", 0.0)
            )
            / 1000.0,
        )
        for system in summary
    ]
    failure_items = [
        (system, _chaos_failure_count(summary[system])) for system in summary
    ]
    total_failures = sum(count for _, count in failure_items)
    clean_systems = sum(1 for _, count in failure_items if count == 0)
    worst = sorted(failure_items, key=lambda item: item[1], reverse=True)
    worst_system = worst[0] if worst and worst[0][1] > 0 else None

    best_worker = best(worker_items, reverse=True)
    best_enqueue = best(enqueue_items, reverse=True)
    best_pickup = best(pickup_items, reverse=False)

    def highlight(label: str, value: str, detail: str) -> str:
        return (
            "<div class='highlight-card'>"
            f"<div class='label'>{html.escape(label)}</div>"
            f"<div class='value'>{value}</div>"
            f"<div class='detail'>{detail}</div>"
            "</div>"
        )

    highlight_cards: list[str] = []
    if best_worker:
        highlight_cards.append(
            highlight(
                "Best worker throughput",
                html.escape(best_worker[0]),
                f"{best_worker[1]:,.0f} jobs/s mean",
            )
        )
    if best_enqueue:
        highlight_cards.append(
            highlight(
                "Best enqueue throughput",
                html.escape(best_enqueue[0]),
                f"{best_enqueue[1]:,.0f} jobs/s mean",
            )
        )
    if best_pickup:
        highlight_cards.append(
            highlight(
                "Lowest pickup p50",
                html.escape(best_pickup[0]),
                f"{best_pickup[1]:,.2f} ms",
            )
        )
    highlight_cards.append(
        highlight(
            "Chaos failures",
            f"{total_failures}",
            (
                f"{clean_systems}/{len(failure_items)} systems clean"
                + (
                    f"; worst: {html.escape(worst_system[0])} ({int(worst_system[1])})"
                    if worst_system
                    else ""
                )
            ),
        )
    )

    cross_benchmark_table = styled_table(
        [
            "System",
            "Worker Throughput (jobs/s)",
            "Enqueue Throughput (jobs/s)",
            "Pickup p50 (ms)",
            "Pickup p95 (ms)",
            "Pickup p99 (ms)",
        ],
        benchmark_rows,
        numeric_columns={1, 2, 3, 4, 5},
    )
    cross_chaos_table = styled_table(
        [
            "System",
            "Scenario",
            "Status",
            "Successes",
            "Max Jobs Lost",
            "Mean Total (s)",
            "Max Total (s)",
        ],
        chaos_rows,
        numeric_columns={3, 4, 5, 6},
    )

    bundle_files: list[str] = ["index.html", "styles.css", "assets/ (charts)"]
    for path in [
        output_path,
        log_path,
        status_path,
        RESULTS_DIR / f"benchmark_summary_{timestamp}.csv",
        RESULTS_DIR / f"benchmark_summary_{timestamp}.md",
        RESULTS_DIR / f"chaos_summary_{timestamp}.csv",
        RESULTS_DIR / f"chaos_summary_{timestamp}.md",
    ]:
        if path.exists():
            bundle_files.append(path.name)
    bundle_list = "".join(
        f"<li><span class='mono'>{html.escape(name)}</span></li>"
        for name in bundle_files
    )

    index_html = f"""
<!doctype html>
<html lang='en'>
  <head>
    <meta charset='utf-8'>
    <title>Portable Full Suite Report {html.escape(timestamp)}</title>
    <link rel='stylesheet' href='styles.css'>
  </head>
  <body>
    <h1>Portable Full Suite Report</h1>
    <p class='subtitle'>Cross-system benchmark and chaos comparison for {len(summary)} job runners.</p>
    <div class='meta'>
      <div><strong>Timestamp:</strong><br><span class='mono'>{html.escape(timestamp)}</span></div>
      <div><strong>Systems:</strong><br>{html.escape(", ".join(config["systems"]))}</div>
      <div><strong>Benchmark job count:</strong><br>{config["benchmark_job_count"]:,}</div>
      <div><strong>Chaos suite:</strong><br>{html.escape(config["chaos_suite"])}</div>
      <div><strong>PG image:</strong><br><span class='mono'>{html.escape(config["pg_image"])}</span></div>
    </div>
    <div class='section'>
      <h2>Highlights</h2>
      <div class='highlights'>{"".join(highlight_cards)}</div>
    </div>
    <div class='section'>
      <h2>Charts</h2>
      <div class='charts'>{"".join(chart_cards)}</div>
    </div>
    <div class='section'>
      <h2>Cross-System Benchmarks</h2>
      <p class='muted'>Throughput numbers are means across repetitions; latency columns are mean of per-run percentiles, expressed in milliseconds.</p>
      {cross_benchmark_table}
    </div>
    <div class='section'>
      <h2>Cross-System Chaos</h2>
      <p class='muted'>Status reflects whether all jobs were processed without errors and without job loss. Times are in seconds.</p>
      {cross_chaos_table}
    </div>
    {"".join(system_sections)}
    <div class='section'>
      <h2>Bundle Contents</h2>
      <div class='bundle-files'>
        <div class='muted'>Files included in this report archive:</div>
        <ul>{bundle_list}</ul>
      </div>
    </div>
  </body>
</html>
""".strip()
    (report_dir / "index.html").write_text(index_html)

    for path in [
        output_path,
        log_path,
        status_path,
        RESULTS_DIR / f"benchmark_summary_{timestamp}.csv",
        RESULTS_DIR / f"benchmark_summary_{timestamp}.md",
        RESULTS_DIR / f"chaos_summary_{timestamp}.csv",
        RESULTS_DIR / f"chaos_summary_{timestamp}.md",
    ]:
        if path.exists():
            shutil.copy2(path, report_dir / path.name)

    archive_path = shutil.make_archive(str(report_dir), "zip", root_dir=report_dir)
    return Path(archive_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run benchmark and chaos suites per system"
    )
    parser.add_argument("--systems", default=",".join(DEFAULT_SYSTEMS))
    parser.add_argument("--benchmark-job-count", type=int, default=10000)
    parser.add_argument("--benchmark-worker-count", type=int, default=50)
    parser.add_argument("--latency-iterations", type=int, default=100)
    parser.add_argument("--chaos-job-count", type=int, default=10)
    parser.add_argument(
        "--chaos-suite", choices=["portable", "extended"], default="portable"
    )
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--pg-image", default="postgres:17-alpine")
    args = parser.parse_args()

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    RESULTS_DIR.mkdir(exist_ok=True)
    log_path = RESULTS_DIR / f"full_suite_{run_timestamp}.log"
    status_path = RESULTS_DIR / f"full_suite_{run_timestamp}.status.json"

    systems = [system.strip() for system in args.systems.split(",") if system.strip()]
    combined_results: dict[str, list[dict]] = {system: [] for system in systems}
    built_systems: set[str] = set()
    run_status = {
        "timestamp": run_timestamp,
        "started_at": utc_now(),
        "updated_at": utc_now(),
        "state": "running",
        "config": {
            "systems": systems,
            "benchmark_job_count": args.benchmark_job_count,
            "benchmark_worker_count": args.benchmark_worker_count,
            "latency_iterations": args.latency_iterations,
            "chaos_job_count": args.chaos_job_count,
            "chaos_suite": args.chaos_suite,
            "repetitions": args.repetitions,
            "skip_build": args.skip_build,
            "pg_image": args.pg_image,
        },
        "current": None,
        "completed": [],
        "log_file": str(log_path),
    }
    append_log(log_path, f"[{utc_now()}] full_suite start\n")
    write_status(status_path, run_status)

    try:
        for repetition in range(1, args.repetitions + 1):
            for system in systems:
                args.skip_build = args.skip_build or system in built_systems
                run_status["current"] = {
                    "repetition": repetition,
                    "system": system,
                    "phase": "benchmarks",
                    "started_at": utc_now(),
                }
                run_status["updated_at"] = utc_now()
                write_status(status_path, run_status)
                print(
                    f"\n=== repetition {repetition}/{args.repetitions}: {system} benchmarks ===",
                    file=sys.stderr,
                )
                benchmark_payload = run_benchmarks(system, args, log_path=log_path)
                built_systems.add(system)

                run_status["current"] = {
                    "repetition": repetition,
                    "system": system,
                    "phase": "chaos",
                    "started_at": utc_now(),
                }
                run_status["updated_at"] = utc_now()
                write_status(status_path, run_status)
                print(
                    f"\n=== repetition {repetition}/{args.repetitions}: {system} chaos ({args.chaos_suite}) ===",
                    file=sys.stderr,
                )
                chaos_payload = run_chaos(system, args, log_path=log_path)

                combined_entry = {
                    "repetition": repetition,
                    "benchmark_result_file": benchmark_payload["timestamp"],
                    "benchmark_payload": benchmark_payload,
                    "chaos_payload": chaos_payload,
                    "summary": summarize_system(benchmark_payload, chaos_payload),
                }
                combined_results[system].append(combined_entry)
                run_status["completed"].append(
                    {
                        "repetition": repetition,
                        "system": system,
                        "finished_at": utc_now(),
                    }
                )
                run_status["current"] = None
                run_status["updated_at"] = utc_now()
                write_status(status_path, run_status)
    except Exception as exc:
        run_status["state"] = "failed"
        run_status["updated_at"] = utc_now()
        run_status["error"] = str(exc)
        write_status(status_path, run_status)
        append_log(log_path, f"[{utc_now()}] full_suite failed: {exc}\n")
        raise

    summary = {
        system: summarize_repetitions(repetitions)
        for system, repetitions in combined_results.items()
    }

    timestamp = run_timestamp
    output_path = RESULTS_DIR / f"full_suite_{timestamp}.json"
    with output_path.open("w") as handle:
        json.dump(
            {
                "timestamp": timestamp,
                "config": {
                    "systems": systems,
                    "benchmark_job_count": args.benchmark_job_count,
                    "benchmark_worker_count": args.benchmark_worker_count,
                    "latency_iterations": args.latency_iterations,
                    "chaos_job_count": args.chaos_job_count,
                    "chaos_suite": args.chaos_suite,
                    "repetitions": args.repetitions,
                    "skip_build": args.skip_build,
                    "pg_image": args.pg_image,
                },
                "systems": combined_results,
                "summary": summary,
            },
            handle,
            indent=2,
        )
    write_benchmark_exports(summary, timestamp)
    write_chaos_exports(summary, timestamp)
    report_zip = write_html_report(
        summary=summary,
        timestamp=timestamp,
        config=run_status["config"],
        output_path=output_path,
        log_path=log_path,
        status_path=status_path,
    )
    run_status["state"] = "completed"
    run_status["updated_at"] = utc_now()
    run_status["finished_at"] = utc_now()
    run_status["result_file"] = str(output_path)
    run_status["html_report_zip"] = str(report_zip)
    run_status["current"] = None
    write_status(status_path, run_status)
    append_log(log_path, f"[{utc_now()}] full_suite completed\n")
    print(f"\nResults saved to {output_path}", file=sys.stderr)
    print(f"HTML report bundle saved to {report_zip}", file=sys.stderr)
    if args.repetitions == 1:
        print_summary(
            {system: runs[0]["summary"] for system, runs in combined_results.items()}
        )
    else:
        print_repetition_summary(summary)


if __name__ == "__main__":
    main()

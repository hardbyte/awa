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

DEFAULT_SYSTEMS = [
    "awa",
    "awa-docker",
    "awa-python",
    "procrastinate",
    "river",
    "oban",
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


def write_benchmark_exports(summary: dict[str, dict], timestamp: str) -> None:
    markdown_path = RESULTS_DIR / f"benchmark_summary_{timestamp}.md"
    csv_path = RESULTS_DIR / f"benchmark_summary_{timestamp}.csv"

    benchmark_rows: list[dict[str, object]] = []
    for system, data in summary.items():
        row = {"system": system}
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

    lines = [
        "# Benchmark Summary",
        "",
        "| System | Worker Throughput Mean | Worker Throughput Stdev | Pickup p50 Mean |",
        "|---|---:|---:|---:|",
    ]
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

    lines = [
        "# Chaos Summary",
        "",
        "| System | Scenario | Successes | Max Lost | Mean Total (s) |",
        "|---|---|---:|---:|---:|",
    ]
    for system, data in summary.items():
        for scenario, values in data["chaos"].items():
            lines.append(
                f"| {system} | {scenario} | {values.get('successes', 0)}/{values.get('runs', 0)} | {values.get('max_jobs_lost', 0)} | {values.get('mean_total_time_secs', 0):.1f} |"
            )
    markdown_path.write_text("\n".join(lines) + "\n")


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

    styles = """
body { font-family: Inter, Arial, sans-serif; margin: 32px; color: #1f2937; background: #f8fafc; }
h1, h2, h3 { color: #0f172a; }
.meta { background: white; border: 1px solid #dbe4ee; border-radius: 8px; padding: 16px; margin-bottom: 24px; }
.section { margin-bottom: 28px; }
table { border-collapse: collapse; width: 100%; background: white; margin: 12px 0 24px 0; }
th, td { border: 1px solid #dbe4ee; padding: 8px 10px; text-align: left; }
th { background: #eff6ff; }
tr:nth-child(even) td { background: #f8fbff; }
.muted { color: #475569; }
.mono { font-family: ui-monospace, SFMono-Regular, monospace; }
""".strip()
    (report_dir / "styles.css").write_text(styles)

    def table(headers: list[str], rows: list[list[str]]) -> str:
        head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
        body = "".join(
            "<tr>" + "".join(f"<td>{html.escape(cell)}</td>" for cell in row) + "</tr>"
            for row in rows
        )
        return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"

    benchmark_rows: list[list[str]] = []
    chaos_rows: list[list[str]] = []
    system_sections: list[str] = []

    for system, data in summary.items():
        worker = data["benchmarks"].get("worker_throughput", {})
        latency = data["benchmarks"].get("pickup_latency", {})
        benchmark_rows.append(
            [
                system,
                f"{worker.get('mean_jobs_per_sec', 0):,.0f}",
                f"{worker.get('stdev_jobs_per_sec', 0):,.0f}",
                f"{latency.get('mean_p50_us', 0):,.0f}",
                f"{latency.get('mean_p95_us', 0):,.0f}",
                f"{latency.get('mean_p99_us', 0):,.0f}",
            ]
        )

        system_benchmark_rows: list[list[str]] = []
        for scenario, values in sorted(data["benchmarks"].items()):
            if "mean_jobs_per_sec" in values:
                system_benchmark_rows.append(
                    [
                        scenario,
                        f"{values.get('mean_jobs_per_sec', 0):,.0f}",
                        f"{values.get('stdev_jobs_per_sec', 0):,.0f}",
                        f"{values.get('min_jobs_per_sec', 0):,.0f}",
                        f"{values.get('max_jobs_per_sec', 0):,.0f}",
                    ]
                )
            else:
                system_benchmark_rows.append(
                    [
                        scenario,
                        f"{values.get('mean_p50_us', 0):,.0f}",
                        f"{values.get('mean_p95_us', 0):,.0f}",
                        f"{values.get('mean_p99_us', 0):,.0f}",
                        f"{values.get('min_p50_us', 0):,.0f}..{values.get('max_p50_us', 0):,.0f}",
                    ]
                )

        system_chaos_rows: list[list[str]] = []
        for scenario, values in sorted(data["chaos"].items()):
            if "errors" in values:
                errors = "; ".join(values["errors"])
                chaos_rows.append([system, scenario, "0/0", "-", errors])
                system_chaos_rows.append([scenario, "0/0", "-", errors])
                continue
            successes = f"{values.get('successes', 0)}/{values.get('runs', 0)}"
            max_lost = str(values.get("max_jobs_lost", 0))
            mean_total = f"{values.get('mean_total_time_secs', 0):.1f}s"
            chaos_rows.append([system, scenario, successes, max_lost, mean_total])
            system_chaos_rows.append([scenario, successes, max_lost, mean_total])

        system_sections.append(
            f"<div class='section'><h2>{html.escape(system)}</h2>"
            f"<h3>Benchmark Summary</h3>"
            f"{table(['Scenario', 'Metric 1', 'Metric 2', 'Metric 3', 'Range'], system_benchmark_rows or [['-', '-', '-', '-', '-']])}"
            f"<h3>Chaos Summary</h3>"
            f"{table(['Scenario', 'Successes', 'Max Lost', 'Mean Total / Errors'], system_chaos_rows or [['-', '-', '-', '-']])}"
            f"</div>"
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
    <div class='meta'>
      <div><strong>Timestamp:</strong> <span class='mono'>{html.escape(timestamp)}</span></div>
      <div><strong>Systems:</strong> {html.escape(", ".join(config["systems"]))}</div>
      <div><strong>Benchmark job count:</strong> {config["benchmark_job_count"]}</div>
      <div><strong>Chaos suite:</strong> {html.escape(config["chaos_suite"])}</div>
      <div><strong>PG image:</strong> <span class='mono'>{html.escape(config["pg_image"])}</span></div>
      <div class='muted'>This bundle includes the HTML summary plus the source JSON, CSV, Markdown, log, and status files from the run.</div>
    </div>
    <div class='section'>
      <h2>Cross-System Benchmarks</h2>
      {table(["System", "Worker Throughput Mean", "Worker Throughput Stdev", "Pickup p50 Mean", "Pickup p95 Mean", "Pickup p99 Mean"], benchmark_rows)}
    </div>
    <div class='section'>
      <h2>Cross-System Chaos</h2>
      {table(["System", "Scenario", "Successes", "Max Lost", "Mean Total / Errors"], chaos_rows)}
    </div>
    {"".join(system_sections)}
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

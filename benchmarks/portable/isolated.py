#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent.resolve()
RUNNER = SCRIPT_DIR / "run.py"
RESULTS_DIR = SCRIPT_DIR / "results"
RESULT_PATH_RE = re.compile(r"Results saved to (.+)")


def run_once(
    system: str,
    scenario: str,
    job_count: int,
    worker_count: int,
    latency_iterations: int,
    skip_build: bool,
    pg_image: str,
) -> dict:
    cmd = [
        "uv",
        "run",
        "python",
        str(RUNNER),
        "--systems",
        system,
        "--scenario",
        scenario,
        "--job-count",
        str(job_count),
        "--worker-count",
        str(worker_count),
        "--latency-iterations",
        str(latency_iterations),
        "--pg-image",
        pg_image,
    ]
    if skip_build:
        cmd.append("--skip-build")

    print(f"\n=== {system} ===", file=sys.stderr)
    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    completed = subprocess.run(
        cmd,
        cwd=str(SCRIPT_DIR.parent.parent),
        capture_output=True,
        text=True,
        timeout=7200,
    )
    sys.stderr.write(completed.stderr)
    sys.stdout.write(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(f"isolated run failed for {system}")

    match = RESULT_PATH_RE.search(completed.stderr)
    if not match:
        raise RuntimeError(f"could not find results file for {system}")

    result_path = Path(match.group(1).strip())
    with result_path.open() as f:
        payload = json.load(f)

    if len(payload["config"]["systems"]) != 1 or payload["config"]["systems"][0] != system:
        raise RuntimeError(f"unexpected systems in results for {system}: {payload['config']['systems']}")

    return payload


def summarize(runs: list[dict]) -> dict[str, dict[str, dict[str, float]]]:
    grouped: dict[str, dict[str, list[dict]]] = {}
    for run in runs:
        system = run["system"]
        scenario = run["scenario"]
        grouped.setdefault(system, {}).setdefault(scenario, []).append(run["results"])

    summary: dict[str, dict[str, dict[str, float]]] = {}
    for system, scenario_map in grouped.items():
        system_summary: dict[str, dict[str, float]] = {}
        for scenario, results_list in scenario_map.items():
            if scenario in ("enqueue_throughput", "worker_throughput"):
                values = [r["jobs_per_sec"] for r in results_list]
                system_summary[scenario] = {
                    "mean_jobs_per_sec": statistics.mean(values),
                    "min_jobs_per_sec": min(values),
                    "max_jobs_per_sec": max(values),
                    "stdev_jobs_per_sec": statistics.stdev(values) if len(values) > 1 else 0.0,
                }
            else:
                p50_values = [r["p50_us"] for r in results_list]
                p95_values = [r["p95_us"] for r in results_list]
                p99_values = [r["p99_us"] for r in results_list]
                mean_values = [r["mean_us"] for r in results_list]
                system_summary[scenario] = {
                    "mean_p50_us": statistics.mean(p50_values),
                    "min_p50_us": min(p50_values),
                    "max_p50_us": max(p50_values),
                    "mean_p95_us": statistics.mean(p95_values),
                    "mean_p99_us": statistics.mean(p99_values),
                    "mean_mean_us": statistics.mean(mean_values),
                }
        summary[system] = system_summary
    return summary


def print_summary(summary: dict[str, dict[str, dict[str, float]]]) -> None:
    for scenario in ("enqueue_throughput", "worker_throughput", "pickup_latency"):
        print(f"\n--- {scenario} ---")
        if scenario in ("enqueue_throughput", "worker_throughput"):
            rows = sorted(
                (
                    (
                        system,
                        stats[scenario]["mean_jobs_per_sec"],
                        stats[scenario]["stdev_jobs_per_sec"],
                        stats[scenario]["min_jobs_per_sec"],
                        stats[scenario]["max_jobs_per_sec"],
                    )
                    for system, stats in summary.items()
                    if scenario in stats
                ),
                key=lambda row: row[1],
                reverse=True,
            )
            print(f"  {'System':<14} {'Mean j/s':>12} {'Stddev':>10} {'Min':>12} {'Max':>12}")
            print(f"  {'-'*14} {'-'*12} {'-'*10} {'-'*12} {'-'*12}")
            for system, mean_val, stdev, min_val, max_val in rows:
                print(f"  {system:<14} {mean_val:>12,.0f} {stdev:>10,.0f} {min_val:>12,.0f} {max_val:>12,.0f}")
        else:
            rows = sorted(
                (
                    (
                        system,
                        stats[scenario]["mean_p50_us"],
                        stats[scenario]["mean_p95_us"],
                        stats[scenario]["mean_p99_us"],
                    )
                    for system, stats in summary.items()
                    if scenario in stats
                ),
                key=lambda row: row[1],
            )
            print(f"  {'System':<14} {'Mean p50 (us)':>14} {'Mean p95 (us)':>14} {'Mean p99 (us)':>14}")
            print(f"  {'-'*14} {'-'*14} {'-'*14} {'-'*14}")
            for system, p50, p95, p99 in rows:
                print(f"  {system:<14} {p50:>14,.0f} {p95:>14,.0f} {p99:>14,.0f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run isolated portable benchmark repetitions")
    parser.add_argument("--scenario", default="all", choices=["all", "enqueue_throughput", "worker_throughput", "pickup_latency"])
    parser.add_argument("--job-count", type=int, default=50000)
    parser.add_argument("--worker-count", type=int, default=200)
    parser.add_argument("--latency-iterations", type=int, default=100)
    parser.add_argument("--systems", default="awa,awa-docker,awa-python,procrastinate,river,oban")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--pg-image", default="postgres:17-alpine")
    args = parser.parse_args()

    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    all_runs: list[dict] = []

    for system in systems:
        for repetition in range(1, args.repetitions + 1):
            payload = run_once(
                system=system,
                scenario=args.scenario,
                job_count=args.job_count,
                worker_count=args.worker_count,
                latency_iterations=args.latency_iterations,
                skip_build=args.skip_build,
                pg_image=args.pg_image,
            )
            for result in payload["results"]:
                all_runs.append(
                    {
                        "system": system,
                        "repetition": repetition,
                        "scenario": result["scenario"],
                        "config": result["config"],
                        "results": result["results"],
                        "source_file": str(payload["timestamp"]),
                    }
                )

    summary = summarize(all_runs)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"isolated_{timestamp}.json"
    with out_path.open("w") as f:
        json.dump(
            {
                "timestamp": timestamp,
                "config": {
                    "scenario": args.scenario,
                    "job_count": args.job_count,
                    "worker_count": args.worker_count,
                    "latency_iterations": args.latency_iterations,
                    "systems": systems,
                    "repetitions": args.repetitions,
                    "skip_build": args.skip_build,
                    "pg_image": args.pg_image,
                },
                "runs": all_runs,
                "summary": summary,
            },
            f,
            indent=2,
        )
    print(f"\nIsolated results saved to {out_path}", file=sys.stderr)
    print_summary(summary)


if __name__ == "__main__":
    main()

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
LONG_HORIZON = SCRIPT_DIR / "long_horizon.py"
RESULTS_DIR = SCRIPT_DIR / "results"
RESULT_PATH_RE = re.compile(r"results at:\s*(.+)")


def run_once(
    *,
    systems: str,
    scenario: str | None,
    phase_specs: list[str],
    worker_count: int,
    producer_rate: int,
    replicas: int,
    sample_every: int,
    skip_build: bool,
    fast: bool,
    extra_args: list[str],
) -> dict:
    cmd = [
        "uv",
        "run",
        "python",
        str(LONG_HORIZON),
        "run",
        "--systems",
        systems,
        "--worker-count",
        str(worker_count),
        "--producer-rate",
        str(producer_rate),
        "--replicas",
        str(replicas),
        "--sample-every",
        str(sample_every),
    ]
    if scenario:
        cmd.extend(["--scenario", scenario])
    else:
        for phase in phase_specs:
            cmd.extend(["--phase", phase])
    if skip_build:
        cmd.append("--skip-build")
    if fast:
        cmd.append("--fast")
    cmd.extend(extra_args)

    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    completed = subprocess.run(
        cmd,
        cwd=str(SCRIPT_DIR.parent.parent),
        capture_output=True,
        text=True,
        timeout=6 * 3600,
    )
    sys.stderr.write(completed.stderr)
    sys.stdout.write(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(f"worker-scale run failed for worker_count={worker_count}")

    combined = f"{completed.stdout}\n{completed.stderr}"
    match = RESULT_PATH_RE.search(combined)
    if not match:
        raise RuntimeError("could not find long_horizon result path")

    run_dir = Path(match.group(1).strip())
    summary = json.loads((run_dir / "summary.json").read_text())
    return {
        "run_dir": str(run_dir),
        "summary": summary,
    }


def collect_phase_table(summary: dict) -> dict[str, dict[str, dict[str, float | None]]]:
    out: dict[str, dict[str, dict[str, float | None]]] = {}
    for system, system_block in summary["systems"].items():
        phase_rows: dict[str, dict[str, float | None]] = {}
        for phase_label, phase_block in system_block["phases"].items():
            phase_rows[phase_label] = {
                "throughput": phase_block.get("median_throughput_per_s"),
                "producer_p99_ms": phase_block.get("median_producer_p99_ms"),
                "producer_call_p99_ms": phase_block.get("median_producer_call_p99_ms"),
                "subscriber_p99_ms": phase_block.get("median_subscriber_p99_ms"),
                "end_to_end_p99_ms": phase_block.get("median_end_to_end_p99_ms"),
                "dead_tuples": phase_block.get("median_dead_tup"),
            }
        out[system] = phase_rows
    return out


def print_summary_matrix(results: list[dict]) -> None:
    for phase in ["clean_1", "readers_1", "pressure_1", "recovery_1"]:
        rows = []
        for row in results:
            for system, phase_map in row["phase_table"].items():
                metrics = phase_map.get(phase)
                if not metrics:
                    continue
                rows.append(
                    (
                        row["worker_count"],
                        system,
                        metrics["throughput"],
                        metrics["subscriber_p99_ms"],
                        metrics["dead_tuples"],
                    )
                )
        if not rows:
            continue
        print(f"\n--- {phase} ---")
        print(
            f"  {'Workers':>7} {'System':<14} {'Throughput':>12} "
            f"{'Sub p99 ms':>12} {'Dead tuples':>12}"
        )
        print(f"  {'-'*7} {'-'*14} {'-'*12} {'-'*12} {'-'*12}")
        for worker_count, system, throughput, subscriber_p99, dead_tuples in rows:
            print(
                f"  {worker_count:>7} {system:<14} "
                f"{(throughput or 0):>12,.1f} {(subscriber_p99 or 0):>12,.1f} "
                f"{(dead_tuples or 0):>12,.1f}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sweep long-horizon throughput/latency by worker count."
    )
    parser.add_argument("--systems", default="awa,pgque")
    parser.add_argument("--scenario", default=None)
    parser.add_argument("--worker-counts", default="1,4,8,16,32,64")
    parser.add_argument("--producer-rate", type=int, default=800)
    parser.add_argument("--replicas", type=int, default=1)
    parser.add_argument("--sample-every", type=int, default=5)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--no-fast",
        action="store_true",
        help="Use full cold-start harness mode instead of reusing one Postgres instance.",
    )
    parser.add_argument(
        "--phase",
        action="append",
        default=[],
        help=(
            "Custom phase spec label=type:duration. If omitted and --scenario is not "
            "set, a short steady-state/pressure profile is used."
        ),
    )
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="Pass through an extra flag to long_horizon.py",
    )
    args = parser.parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    phase_specs = args.phase or [
        "warmup=warmup:30s",
        "clean_1=clean:60s",
        "pressure_1=high-load:60s",
        "recovery_1=clean:60s",
    ]

    worker_counts = [int(part) for part in args.worker_counts.split(",") if part.strip()]
    results = []
    for worker_count in worker_counts:
        payload = run_once(
            systems=args.systems,
            scenario=args.scenario,
            phase_specs=phase_specs,
            worker_count=worker_count,
            producer_rate=args.producer_rate,
            replicas=args.replicas,
            sample_every=args.sample_every,
            skip_build=args.skip_build,
            fast=not args.no_fast,
            extra_args=args.extra_arg,
        )
        results.append(
            {
                "worker_count": worker_count,
                "run_dir": payload["run_dir"],
                "phase_table": collect_phase_table(payload["summary"]),
            }
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"worker_scale_{timestamp}.json"
    out_path.write_text(
        json.dumps(
            {
                "timestamp": timestamp,
                "config": {
                    "systems": args.systems,
                    "scenario": args.scenario or "custom_short_worker_scale",
                    "phase_specs": phase_specs if args.scenario is None else [],
                    "worker_counts": worker_counts,
                    "producer_rate": args.producer_rate,
                    "replicas": args.replicas,
                    "sample_every": args.sample_every,
                    "skip_build": args.skip_build,
                    "fast": not args.no_fast,
                    "extra_args": args.extra_arg,
                },
                "runs": results,
            },
            indent=2,
        )
    )
    print(f"\nWorker-scale results saved to {out_path}", file=sys.stderr)
    print_summary_matrix(results)


if __name__ == "__main__":
    main()

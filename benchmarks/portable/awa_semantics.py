#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
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

SEMANTIC_SCENARIOS = {
    "retry_priority_mix": {
        "scenario": None,
        "phases": [
            "warmup=warmup:30s",
            "clean_1=clean:60s",
            "pressure_1=high-load:120s",
            "recovery_1=clean:60s",
        ],
        "replicas": 1,
        "env": {
            "JOB_FAIL_FIRST_MOD": "7",
            "JOB_MAX_ATTEMPTS": "4",
            "JOB_PRIORITY_PATTERN": "1,4,4,4",
            "PRIORITY_AGING_MS": "10000",
            "RUST_LOG": "error",
        },
        "target_utilization": 0.9,
        "calibration_phases": [
            "warmup=warmup:20s",
            "clean_1=clean:60s",
        ],
    },
    "crash_recovery_under_load": {
        "scenario": "crash_recovery_under_load",
        "phases": [],
        "replicas": 2,
        "env": {},
    },
}


def run_once(
    *,
    semantic_scenario: str,
    phase_specs: list[str],
    sample_every: int,
    worker_count: int,
    producer_rate: int,
    skip_build: bool,
    fast: bool,
) -> tuple[Path, dict]:
    config = SEMANTIC_SCENARIOS[semantic_scenario]
    cmd = [
        "uv",
        "run",
        "python",
        str(LONG_HORIZON),
        "run",
        "--systems",
        "awa",
        "--replicas",
        str(config["replicas"]),
        "--worker-count",
        str(worker_count),
        "--producer-rate",
        str(producer_rate),
        "--sample-every",
        str(sample_every),
    ]
    if phase_specs:
        for phase in phase_specs:
            cmd.extend(["--phase", phase])
    elif config["scenario"] is not None:
        cmd.extend(["--scenario", config["scenario"]])
    else:
        for phase in config["phases"]:
            cmd.extend(["--phase", phase])
    if skip_build:
        cmd.append("--skip-build")
    if fast:
        cmd.append("--fast")

    env = {**os.environ, **config["env"]}
    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    completed = subprocess.run(
        cmd,
        cwd=str(SCRIPT_DIR.parent.parent),
        capture_output=True,
        text=True,
        timeout=6 * 3600,
        env=env,
    )
    sys.stderr.write(completed.stderr)
    sys.stdout.write(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(f"awa semantics run failed for {semantic_scenario}")
    combined = f"{completed.stdout}\n{completed.stderr}"
    match = RESULT_PATH_RE.search(combined)
    if not match:
        raise RuntimeError("could not find long_horizon result path")
    run_dir = Path(match.group(1).strip())
    summary = json.loads((run_dir / "summary.json").read_text())
    return run_dir, summary


def calibrate_producer_rate(
    *,
    semantic_scenario: str,
    sample_every: int,
    worker_count: int,
    skip_build: bool,
    fast: bool,
) -> tuple[int, Path, dict]:
    config = SEMANTIC_SCENARIOS[semantic_scenario]
    target_utilization = float(config.get("target_utilization", 0.8))
    run_dir, summary = run_once(
        semantic_scenario=semantic_scenario,
        phase_specs=list(
            config.get(
                "calibration_phases",
                [
                    "warmup=warmup:10s",
                    "clean_1=clean:20s",
                ],
            )
        ),
        sample_every=sample_every,
        worker_count=worker_count,
        producer_rate=800,
        skip_build=skip_build,
        fast=fast,
    )
    metrics = adapter_metric_medians(run_dir / "raw.csv")
    clean = metrics.get("clean_1", {})
    completion_rate = float(clean.get("completion_rate", 0.0))
    if completion_rate <= 0:
        raise RuntimeError("calibration run did not produce a usable completion_rate")
    producer_rate = max(1, int(completion_rate * target_utilization))
    return producer_rate, run_dir, summary


def adapter_metric_medians(raw_csv: Path) -> dict[str, dict[str, float]]:
    rows = list(csv.DictReader(raw_csv.open()))
    metrics = [
        "retryable_failure_rate",
        "completed_priority_1_rate",
        "completed_priority_4_rate",
        "completed_original_priority_1_rate",
        "completed_original_priority_4_rate",
        "aged_completion_rate",
        "completion_rate",
        "subscriber_p99_ms",
        "end_to_end_p99_ms",
        "queue_depth",
        "running_depth",
        "retryable_depth",
        "scheduled_depth",
        "total_backlog",
    ]
    out: dict[str, dict[str, float]] = {}
    for phase in sorted({r["phase_label"] for r in rows if r["phase_label"] and r["phase_label"] != "warmup"}):
        phase_metrics: dict[str, float] = {}
        for metric in metrics:
            vals = [
                float(r["value"])
                for r in rows
                if r["system"] == "awa"
                and r["subject_kind"] == "adapter"
                and r["phase_label"] == phase
                and r["metric"] == metric
            ]
            if vals:
                phase_metrics[metric] = statistics.median(vals)
        if phase_metrics:
            out[phase] = phase_metrics
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Awa-only semantics scenarios for retries, priorities, and crash recovery."
    )
    parser.add_argument("--scenario", choices=sorted(SEMANTIC_SCENARIOS), default="retry_priority_mix")
    parser.add_argument("--worker-count", type=int, default=32)
    parser.add_argument("--producer-rate", type=int, default=None)
    parser.add_argument("--sample-every", type=int, default=5)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--phase",
        action="append",
        default=[],
        help="Override the scenario with explicit phase specs label=type:duration.",
    )
    parser.add_argument(
        "--no-fast",
        action="store_true",
        help="Use full cold-start harness mode instead of reusing one Postgres instance.",
    )
    args = parser.parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    calibrated_from = None
    producer_rate = args.producer_rate
    if producer_rate is None and "target_utilization" in SEMANTIC_SCENARIOS[args.scenario]:
        producer_rate, calibration_run_dir, calibration_summary = calibrate_producer_rate(
            semantic_scenario=args.scenario,
            sample_every=args.sample_every,
            worker_count=args.worker_count,
            skip_build=args.skip_build,
            fast=not args.no_fast,
        )
        calibrated_from = {
            "run_dir": str(calibration_run_dir),
            "summary": calibration_summary,
            "producer_rate": producer_rate,
            "target_utilization": SEMANTIC_SCENARIOS[args.scenario]["target_utilization"],
        }

    run_dir, summary = run_once(
        semantic_scenario=args.scenario,
        phase_specs=args.phase,
        sample_every=args.sample_every,
        worker_count=args.worker_count,
        producer_rate=producer_rate if producer_rate is not None else 800,
        skip_build=args.skip_build,
        fast=not args.no_fast,
    )
    phase_metrics = adapter_metric_medians(run_dir / "raw.csv")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"awa_semantics_{args.scenario}_{timestamp}.json"
    out_path.write_text(
        json.dumps(
            {
                "timestamp": timestamp,
                "scenario": args.scenario,
                "phase_specs": args.phase,
                "worker_count": args.worker_count,
                "producer_rate": producer_rate,
                "calibrated_from": calibrated_from,
                "run_dir": str(run_dir),
                "summary": summary,
                "phase_metrics": phase_metrics,
            },
            indent=2,
        )
    )
    print(f"\nAwa semantics results saved to {out_path}", file=sys.stderr)
    if calibrated_from is not None:
        print(
            f"Calibrated producer_rate={producer_rate} from {calibrated_from['run_dir']}",
            file=sys.stderr,
        )
    for phase, metrics in phase_metrics.items():
        print(f"\n--- {phase} ---")
        for key, value in sorted(metrics.items()):
            print(f"  {key}: {value:.3f}")


if __name__ == "__main__":
    main()

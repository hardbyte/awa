"""Warmed baseline/candidate checks, using private copies of the benchmark harness."""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import csv
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time


def command(argv, **kwargs):
    return subprocess.check_output(argv, text=True, **kwargs).strip()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def run_cell(args, build, cell, source, cpus, port, helper, summary):
    directory = args.output / build
    binary = directory / "frozen-target/release/awa-bench"
    identity = json.loads((directory / "build.json").read_text())
    assert sha(binary) == identity["binary_sha256"]
    env = {
        **os.environ,
        "COMPOSE_PROJECT_NAME": f"awa-review{args.pr}-{build}",
        "BENCH_PG_PORT": str(port),
        "POSTGRES_CPUS": "4",
        "CARGO_TARGET_DIR": str(directory / "frozen-target"),
        "SQLX_OFFLINE": "true",
        "AWA_RING_ROTATION_POLICY": "cadence",
        "JOB_MAX_ATTEMPTS": "5",
        "QUEUE_SLOT_COUNT": "16",
        "LEASE_SLOT_COUNT": "8",
        "CLAIM_SLOT_COUNT": "8",
        "QUEUE_STRIPE_COUNT": "1",
        "LEASE_CLAIM_RECEIPTS": "true",
        "QUEUE_ROTATE_MS": "1000",
        "LEASE_ROTATE_MS": "250",
        "AWA_CLAIM_BATCH_SIZE": "512",
        "JOB_PRIORITY_PATTERN": args.priorities,
    }
    options = (
        ["--producer-rate", "800", "--worker-count", "32"]
        if cell == "reference"
        else [
            "--producer-rate",
            "50000",
            "--producer-mode",
            "depth-target",
            "--target-depth",
            "4000",
            "--worker-count",
            "64",
        ]
    )
    argv = [
        "taskset",
        "-c",
        cpus,
        sys.executable,
        "-m",
        "bench_harness.orchestrator",
        "run",
        "--systems",
        "awa",
        "--pg-image",
        "postgres:18.6-alpine",
        "--skip-build",
        "--sample-every",
        "5",
        *options,
        "--phase",
        "warmup=warmup:240s",
        "--phase",
        "measure=clean:240s",
    ]
    print("START", build, cell, flush=True)
    container = None
    containers = []
    next_container_check = 0.0
    with (directory / f"{cell}.log").open("w") as log:
        process = subprocess.Popen(
            argv, cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            while process.poll() is None:
                if time.monotonic() >= next_container_check:
                    next_container_check = time.monotonic() + 5
                    ids = command(
                        [
                            "docker",
                            "ps",
                            "-q",
                            "--filter",
                            f"label=com.docker.compose.project={env['COMPOSE_PROJECT_NAME']}",
                            "--filter",
                            "label=com.docker.compose.service=postgres",
                        ]
                    ).splitlines()
                    if ids and all(item["id"] != ids[0] for item in containers):
                        data = json.loads(command(["docker", "inspect", ids[0]]))[0]
                        host = data["HostConfig"]
                        assert host["NanoCpus"] == 4000000000, host
                        assert host["Memory"] == 8 * 2**30, host
                        assert host["CpusetCpus"] == cpus, host
                        container = {
                            "id": ids[0],
                            "observed_at": datetime.now(timezone.utc).isoformat(),
                            "created_at": data["Created"],
                            "started_at": data["State"]["StartedAt"],
                            "image_id": data["Image"],
                            "nano_cpus": host["NanoCpus"],
                            "memory_bytes": host["Memory"],
                            "cpuset": cpus,
                        }
                        containers.append(container)
                time.sleep(0.5)
            if process.returncode != 0:
                raise RuntimeError(
                    f"{build}/{cell} exit {process.returncode}; read {directory / (cell + '.log')}"
                )
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            subprocess.run(
                ["docker", "compose", "down", "-v"],
                cwd=directory,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                check=True,
            )
    assert container is not None
    assert sha(binary) == identity["binary_sha256"]
    run = max((directory / "results").iterdir(), key=lambda path: path.stat().st_mtime)
    manifest = json.loads((run / "manifest.json").read_text())
    raw = list(csv.DictReader((run / "raw.csv").open()))
    durations = {phase["label"]: phase["duration_s"] for phase in manifest["phases"]}
    bounds = {
        row["phase_label"]: (
            float(row["value"]),
            float(row["value"]) + durations[row["phase_label"]],
        )
        for row in raw
        if row["metric"] == "phase_start_elapsed_s"
    }
    assert set(bounds) == set(durations)
    groups = defaultdict(list)
    samples = []
    excluded = []
    for row in raw:
        if row["metric"] not in summary.COUNTERS | summary.GAUGES:
            continue
        value, elapsed = float(row["value"]), float(row["elapsed_s"])
        assert math.isfinite(value) and math.isfinite(elapsed), row
        sample = {"build": build, "cell": cell, **row}
        start, end = bounds[row["phase_label"]]
        if not start <= elapsed < end:
            excluded.append(sample)
            continue
        samples.append(sample)
        groups[
            row["phase_label"], row["subject_kind"], row["subject"], row["metric"]
        ].append((elapsed, value))
    metrics = []
    for (phase, kind, subject, metric), points in sorted(groups.items()):
        points.sort()
        values = [value for _, value in points]
        result = {
            "phase": phase,
            "kind": kind,
            "subject": subject,
            "metric": metric,
            "samples": len(values),
            "first_elapsed_s": points[0][0],
            "last_elapsed_s": points[-1][0],
            "first": values[0],
            "last": values[-1],
        }
        if metric in summary.COUNTERS:
            assert all(right >= left for left, right in zip(values, values[1:])), result
            if points[-1][0] > points[0][0]:
                result["per_second"] = (values[-1] - values[0]) / (
                    points[-1][0] - points[0][0]
                )
        else:
            result.update(
                mean=statistics.mean(values),
                median=statistics.median(values),
                maximum=max(values),
            )
        metrics.append(result)
    warmup = {
        family: max(
            row["last"]
            for row in metrics
            if row["phase"] == "warmup"
            and row["metric"] == "ring_generation"
            and row["subject"] == family
        )
        for family in ("queue", "claim")
    }
    assert warmup["queue"] >= 32 and warmup["claim"] >= 16, warmup
    failures = (
        (directory / f"{cell}.log")
        .read_text(errors="replace")
        .count("Failed to complete job")
    )
    assert failures == 0, failures
    record = {
        "build": build,
        "cell": cell,
        "run_dir": str(run),
        "source": identity,
        "argv": argv,
        "runtime_env": {
            key: value
            for key, value in env.items()
            if key
            in {
                "COMPOSE_PROJECT_NAME",
                "BENCH_PG_PORT",
                "POSTGRES_CPUS",
                "AWA_RING_ROTATION_POLICY",
                "JOB_MAX_ATTEMPTS",
                "QUEUE_SLOT_COUNT",
                "LEASE_SLOT_COUNT",
                "CLAIM_SLOT_COUNT",
                "QUEUE_STRIPE_COUNT",
                "LEASE_CLAIM_RECEIPTS",
                "QUEUE_ROTATE_MS",
                "LEASE_ROTATE_MS",
                "AWA_CLAIM_BATCH_SIZE",
                "JOB_PRIORITY_PATTERN",
            }
        },
        "container": container,
        "observed_containers": containers,
        "raw_sha256": sha(run / "raw.csv"),
        "phase_bounds": bounds,
        "warmup_generations": warmup,
        "finalize_errors": failures,
        "samples_excluded_outside_phase_window": len(excluded),
        "metrics": metrics,
    }
    evidence = args.output / "evidence"
    shutil.copy2(run / "manifest.json", evidence / f"{build}-{cell}-manifest.json")
    write_json(evidence / f"{build}-{cell}.json", record)
    for suffix, rows in (("samples", samples), ("excluded", excluded)):
        with gzip.open(
            evidence / f"{build}-{cell}-{suffix}.csv.gz", "wt", newline=""
        ) as output:
            writer = csv.DictWriter(output, fieldnames=list(samples[0]))
            writer.writeheader()
            writer.writerows(rows)
    print("DONE", build, cell, warmup, flush=True)
    return record


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ("baseline", "candidate", "harness", "output", "adapter-lock"):
        parser.add_argument("--" + argument, type=Path, required=True)
    parser.add_argument("--pr", type=int, required=True)
    parser.add_argument("--priorities", default="1")
    parser.add_argument("--start-gate", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "evidence").mkdir(exist_ok=True)
    helper = load(
        args.candidate / "scripts/benchmark-ring-cadence.py", "cadence_runner"
    )
    summary = load(
        args.candidate / "scripts/summarize-ring-cadence.py", "cadence_summary"
    )
    frozen_build = Path("/home/brian/dev/awa-bench-review-target")
    for build, source, cpus in [
        ("baseline", args.baseline, "0-3,12-15"),
        ("candidate", args.candidate, "6-9,18-21"),
    ]:
        directory = args.output / build
        if (directory / "build.json").exists():
            existing = json.loads((directory / "build.json").read_text())
            assert existing["head"] == command(["git", "rev-parse", "HEAD"], cwd=source)
            assert existing["binary_sha256"] == sha(
                directory / "frozen-target/release/awa-bench"
            )
            continue
        if not directory.exists():
            helper.prepare(args.harness, directory, source, cpus)
            (directory / "awa-bench/Cargo.lock").write_bytes(
                gzip.decompress(args.adapter_lock.read_bytes())
            )
        if build == "baseline":
            subprocess.run(
                ["cargo", "fetch", "--locked"], cwd=directory / "awa-bench", check=True
            )
        with (directory / "resolution.json").open("w") as resolution:
            subprocess.run(
                ["cargo", "metadata", "--offline", "--format-version", "1"],
                cwd=directory / "awa-bench",
                stdout=resolution,
                check=True,
            )
        with (directory / "build.log").open("w") as log:
            subprocess.run(
                ["cargo", "build", "--release", "--locked"],
                cwd=directory / "awa-bench",
                env={
                    **os.environ,
                    "CARGO_TARGET_DIR": str(frozen_build),
                    "SQLX_OFFLINE": "true",
                },
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
        target = directory / "frozen-target/release/awa-bench"
        target.parent.mkdir(parents=True)
        shutil.copy2(frozen_build / "release/awa-bench", target)
        write_json(
            directory / "build.json",
            {
                "head": command(["git", "rev-parse", "HEAD"], cwd=source),
                "runtime_diff": command(
                    [
                        "git",
                        "diff",
                        "HEAD",
                        "--",
                        "awa-model",
                        "awa-worker",
                        "awa-macros",
                        "awa-metrics",
                        "Cargo.toml",
                        "Cargo.lock",
                    ],
                    cwd=source,
                ),
                "binary_sha256": sha(target),
                "adapter_lock_sha256": sha(directory / "awa-bench/Cargo.lock"),
            },
        )
    if args.start_gate:
        print("WAIT for local validation gate", args.start_gate, flush=True)
        while not args.start_gate.exists():
            time.sleep(1)
    for cell in ("reference", "saturation"):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    run_cell, args, build, cell, source, cpus, port, helper, summary
                )
                for build, source, cpus, port in [
                    ("baseline", args.baseline, "0-3,12-15", 15581),
                    ("candidate", args.candidate, "6-9,18-21", 15582),
                ]
            ]
            for future in futures:
                future.result()
    write_json(
        args.output / "evidence/runner-provenance.json",
        {
            "runner_sha256": sha(__file__),
            "harness_head": command(["git", "rev-parse", "HEAD"], cwd=args.harness),
            "harness_status": command(["git", "status", "--short"], cwd=args.harness),
            "priorities": args.priorities,
        },
    )
    shutil.copy2(__file__, args.output / "evidence/runner.py")


if __name__ == "__main__":
    main()

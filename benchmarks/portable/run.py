#!/usr/bin/env python3
"""
Portable benchmark orchestrator for Awa, Awa-Python, Procrastinate, River, and Oban.

Usage:
    python run.py [--scenario SCENARIO] [--job-count N] [--worker-count N]
                  [--systems awa,awa-docker,awa-python,procrastinate,river,oban] [--skip-build]

Requires: Docker, cargo (for Awa adapter).
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
RESULTS_DIR = SCRIPT_DIR / "results"
REPO_ROOT = SCRIPT_DIR.parent.parent

PG_PORT = 15555
PG_USER = "bench"
PG_PASS = "bench"
DEFAULT_PG_IMAGE = "postgres:17-alpine"


def pg_url(dbname: str, host: str = "localhost") -> str:
    return f"postgres://{PG_USER}:{PG_PASS}@{host}:{PG_PORT}/{dbname}"


def awa_binary_path() -> Path:
    result = run_cmd(
        [
            "cargo",
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            str(SCRIPT_DIR / "awa-bench" / "Cargo.toml"),
        ],
        capture=True,
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to resolve cargo target directory for awa-bench")
    target_dir = Path(json.loads(result.stdout)["target_directory"])
    return target_dir / "release" / "awa-bench"


def run_cmd(
    cmd: list[str],
    cwd: str | None = None,
    env: dict | None = None,
    capture: bool = False,
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    merged_env = {**os.environ, **(env or {})}
    print(f"  $ {' '.join(cmd)}", file=sys.stderr)
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=merged_env,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )


def with_system_name(results: list[dict], system: str) -> list[dict]:
    """Override adapter-reported system names when the harness is comparing variants."""
    return [{**result, "system": system} for result in results]


def postgres_env(pg_image: str) -> dict[str, str]:
    return {"POSTGRES_IMAGE": pg_image}


def start_postgres(pg_image: str):
    """Start the shared Postgres via docker compose."""
    print(f"\n=== Starting Postgres ({pg_image}) ===", file=sys.stderr)
    run_cmd(
        ["docker", "compose", "up", "-d", "--wait"],
        cwd=str(SCRIPT_DIR),
        env=postgres_env(pg_image),
    )
    # Wait for readiness
    for _ in range(30):
        result = run_cmd(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                PG_USER,
            ],
            cwd=str(SCRIPT_DIR),
            capture=True,
            env=postgres_env(pg_image),
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("Postgres did not become ready in time")


def stop_postgres(pg_image: str):
    print("\n=== Stopping Postgres ===", file=sys.stderr)
    run_cmd(
        ["docker", "compose", "down", "-v"],
        cwd=str(SCRIPT_DIR),
        env=postgres_env(pg_image),
    )


def build_awa():
    """Build the Awa benchmark binary from workspace."""
    print("\n=== Building Awa benchmark ===", file=sys.stderr)
    result = run_cmd(
        ["cargo", "build", "--release"],
        cwd=str(SCRIPT_DIR / "awa-bench"),
        env={"SQLX_OFFLINE": "true"},
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to build awa-bench")


def build_awa_docker():
    """Build the Dockerized Awa benchmark image."""
    print("\n=== Building Awa (Docker) benchmark ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "build",
            "-f",
            str(SCRIPT_DIR / "awa-bench" / "Dockerfile"),
            "-t",
            "awa-bench-docker",
            ".",
        ],
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to build awa-bench-docker image")


def run_awa(
    scenario: str, job_count: int, worker_count: int, latency_iterations: int
) -> list[dict]:
    """Run Awa benchmark natively."""
    print("\n=== Running Awa benchmarks ===", file=sys.stderr)
    result = run_cmd(
        [str(awa_binary_path())],
        env={
            "DATABASE_URL": pg_url("awa_bench"),
            "SCENARIO": scenario,
            "JOB_COUNT": str(job_count),
            "WORKER_COUNT": str(worker_count),
            "LATENCY_ITERATIONS": str(latency_iterations),
        },
        capture=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(f"Awa stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"awa-bench failed: {result.stderr}")
    print(result.stderr, file=sys.stderr, end="")
    return with_system_name(json.loads(result.stdout), "awa")


def run_awa_docker(
    scenario: str, job_count: int, worker_count: int, latency_iterations: int
) -> list[dict]:
    """Run Awa benchmark in Docker."""
    print("\n=== Running Awa (Docker) benchmarks ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            f"DATABASE_URL={pg_url('awa_docker_bench')}",
            "-e",
            f"SCENARIO={scenario}",
            "-e",
            f"JOB_COUNT={job_count}",
            "-e",
            f"WORKER_COUNT={worker_count}",
            "-e",
            f"LATENCY_ITERATIONS={latency_iterations}",
            "awa-bench-docker",
        ],
        capture=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(f"Awa (Docker) stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"awa-bench-docker failed: {result.stderr}")
    print(result.stderr, file=sys.stderr, end="")
    return with_system_name(json.loads(result.stdout), "awa-docker")


def build_awa_python():
    """Build the Awa-Python Docker image."""
    print("\n=== Building Awa-Python benchmark ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "build",
            "-f",
            str(SCRIPT_DIR / "awa-python-bench" / "Dockerfile"),
            "-t",
            "awa-python-bench",
            ".",
        ],
        cwd=str(REPO_ROOT),
        timeout=1800,
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to build awa-python-bench image")


def build_procrastinate():
    """Build the Procrastinate Docker image."""
    print("\n=== Building Procrastinate benchmark ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "build",
            "-f",
            str(SCRIPT_DIR / "procrastinate-bench" / "Dockerfile"),
            "-t",
            "procrastinate-bench",
            ".",
        ],
        cwd=str(REPO_ROOT),
        timeout=1800,
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to build procrastinate-bench image")


def run_awa_python(
    scenario: str, job_count: int, worker_count: int, latency_iterations: int
) -> list[dict]:
    """Run Awa-Python benchmark in Docker."""
    print("\n=== Running Awa-Python benchmarks ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            f"DATABASE_URL={pg_url('awa_python_bench')}",
            "-e",
            f"SCENARIO={scenario}",
            "-e",
            f"JOB_COUNT={job_count}",
            "-e",
            f"WORKER_COUNT={worker_count}",
            "-e",
            f"LATENCY_ITERATIONS={latency_iterations}",
            "awa-python-bench",
        ],
        capture=True,
        timeout=600,
    )
    if result.returncode != 0:
        print(f"Awa-Python stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"awa-python-bench failed: {result.stderr}")
    print(result.stderr, file=sys.stderr, end="")
    return with_system_name(json.loads(result.stdout), "awa-python")


def run_procrastinate(
    scenario: str, job_count: int, worker_count: int, latency_iterations: int
) -> list[dict]:
    """Run Procrastinate benchmark in Docker."""
    print("\n=== Running Procrastinate benchmarks ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            f"DATABASE_URL={pg_url('procrastinate_bench')}",
            "-e",
            f"SCENARIO={scenario}",
            "-e",
            f"JOB_COUNT={job_count}",
            "-e",
            f"WORKER_COUNT={worker_count}",
            "-e",
            f"LATENCY_ITERATIONS={latency_iterations}",
            "procrastinate-bench",
        ],
        capture=True,
        timeout=1800,
    )
    if result.returncode != 0:
        print(f"Procrastinate stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"procrastinate-bench failed: {result.stderr}")
    print(result.stderr, file=sys.stderr, end="")
    return json.loads(result.stdout)


def build_river():
    """Build the River Docker image."""
    print("\n=== Building River benchmark ===", file=sys.stderr)
    result = run_cmd(
        ["docker", "build", "-t", "river-bench", "."],
        cwd=str(SCRIPT_DIR / "river-bench"),
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to build river-bench image")


def run_river(
    scenario: str, job_count: int, worker_count: int, latency_iterations: int
) -> list[dict]:
    """Run River benchmark in Docker."""
    print("\n=== Running River benchmarks ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            f"DATABASE_URL={pg_url('river_bench')}",
            "-e",
            f"SCENARIO={scenario}",
            "-e",
            f"JOB_COUNT={job_count}",
            "-e",
            f"WORKER_COUNT={worker_count}",
            "-e",
            f"LATENCY_ITERATIONS={latency_iterations}",
            "river-bench",
        ],
        capture=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(f"River stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"river-bench failed: {result.stderr}")
    print(result.stderr, file=sys.stderr, end="")
    return json.loads(result.stdout)


def build_oban():
    """Build the Oban Docker image."""
    print("\n=== Building Oban benchmark ===", file=sys.stderr)
    result = run_cmd(
        ["docker", "build", "-t", "oban-bench", "."],
        cwd=str(SCRIPT_DIR / "oban-bench"),
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to build oban-bench image")


def run_oban(
    scenario: str, job_count: int, worker_count: int, latency_iterations: int
) -> list[dict]:
    """Run Oban benchmark in Docker."""
    print("\n=== Running Oban benchmarks ===", file=sys.stderr)
    result = run_cmd(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            f"DATABASE_URL={pg_url('oban_bench')}",
            "-e",
            f"SCENARIO={scenario}",
            "-e",
            f"JOB_COUNT={job_count}",
            "-e",
            f"WORKER_COUNT={worker_count}",
            "-e",
            f"LATENCY_ITERATIONS={latency_iterations}",
            "oban-bench",
        ],
        capture=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(f"Oban stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"oban-bench failed: {result.stderr}")
    print(result.stderr, file=sys.stderr, end="")
    return json.loads(result.stdout)


def print_comparison(all_results: list[dict]):
    """Print a comparison table."""
    print("\n" + "=" * 70)
    print("PORTABLE BENCHMARK RESULTS")
    print("=" * 70)

    # Group by scenario
    by_scenario: dict[str, list[dict]] = {}
    for r in all_results:
        by_scenario.setdefault(r["scenario"], []).append(r)

    for scenario, results in by_scenario.items():
        system_width = max(10, max(len(r["system"]) for r in results))
        print(f"\n--- {scenario} ---")
        if scenario in ("enqueue_throughput", "worker_throughput"):
            # Sort by jobs_per_sec descending
            results.sort(key=lambda r: r["results"]["jobs_per_sec"], reverse=True)
            print(
                f"  {'System':<{system_width}} {'Jobs/sec':>12} {'Duration (ms)':>15}"
            )
            print(f"  {'-' * system_width} {'-' * 12} {'-' * 15}")
            for r in results:
                res = r["results"]
                print(
                    f"  {r['system']:<{system_width}} {res['jobs_per_sec']:>12,.0f} {res['duration_ms']:>15,}"
                )
        elif scenario == "pickup_latency":
            results.sort(key=lambda r: r["results"]["p50_us"])
            print(
                f"  {'System':<{system_width}} {'p50 (us)':>10} {'p95 (us)':>10} {'p99 (us)':>10} {'mean (us)':>10}"
            )
            print(f"  {'-' * system_width} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}")
            for r in results:
                res = r["results"]
                print(
                    f"  {r['system']:<{system_width}} {res['p50_us']:>10,.0f} {res['p95_us']:>10,.0f} {res['p99_us']:>10,.0f} {res['mean_us']:>10,.0f}"
                )
    print()


def main():
    parser = argparse.ArgumentParser(description="Portable benchmark runner")
    parser.add_argument(
        "--scenario",
        default="all",
        choices=["all", "enqueue_throughput", "worker_throughput", "pickup_latency"],
    )
    parser.add_argument("--job-count", type=int, default=10000)
    parser.add_argument("--worker-count", type=int, default=50)
    parser.add_argument("--latency-iterations", type=int, default=100)
    parser.add_argument(
        "--systems",
        default="awa,awa-docker,awa-python,procrastinate,river,oban",
        help="Comma-separated list of systems to benchmark",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip building, use cached images/binaries",
    )
    parser.add_argument(
        "--keep-pg", action="store_true", help="Don't stop Postgres after benchmarks"
    )
    parser.add_argument(
        "--pg-image",
        default=DEFAULT_PG_IMAGE,
        help="Docker image to use for the shared Postgres service",
    )
    args = parser.parse_args()

    systems = [s.strip() for s in args.systems.split(",")]
    RESULTS_DIR.mkdir(exist_ok=True)

    builders = {
        "awa": build_awa,
        "awa-docker": build_awa_docker,
        "awa-python": build_awa_python,
        "procrastinate": build_procrastinate,
        "river": build_river,
        "oban": build_oban,
    }
    runners = {
        "awa": run_awa,
        "awa-docker": run_awa_docker,
        "awa-python": run_awa_python,
        "procrastinate": run_procrastinate,
        "river": run_river,
        "oban": run_oban,
    }

    try:
        start_postgres(args.pg_image)

        # Build phase
        if not args.skip_build:
            for system in systems:
                builders[system]()

        # Run phase
        all_results = []
        for system in systems:
            try:
                results = runners[system](
                    args.scenario,
                    args.job_count,
                    args.worker_count,
                    args.latency_iterations,
                )
                all_results.extend(results)
            except Exception as e:
                print(f"\nERROR running {system}: {e}", file=sys.stderr)

        # Save results
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        result_file = RESULTS_DIR / f"results_{timestamp}.json"
        with open(result_file, "w") as f:
            json.dump(
                {
                    "timestamp": timestamp,
                    "config": {
                        "scenario": args.scenario,
                        "job_count": args.job_count,
                        "worker_count": args.worker_count,
                        "latency_iterations": args.latency_iterations,
                        "systems": systems,
                        "pg_image": args.pg_image,
                    },
                    "results": all_results,
                },
                f,
                indent=2,
            )
        print(f"\nResults saved to {result_file}", file=sys.stderr)

        # Print comparison
        if all_results:
            print_comparison(all_results)

    finally:
        if not args.keep_pg:
            stop_postgres(args.pg_image)


if __name__ == "__main__":
    main()

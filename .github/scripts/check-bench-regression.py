#!/usr/bin/env python3
"""Compare nightly benchmark results against baseline thresholds.

Reads @@BENCH_JSON@@ lines from artifact files, compares key metrics
against thresholds in benchmarks/baseline.json, and emits a GitHub
Actions job summary table with pass/fail indicators.

Exit code 1 if any metric regresses beyond the allowed tolerance.
"""

import json
import os
import sys
from pathlib import Path

JSONL_PREFIX = "@@BENCH_JSON@@"
# Regressing more than this fraction below baseline triggers a failure.
# 50% accommodates GitHub-hosted runner variance. Shared VMs routinely
# show 30-40% run-to-run swings; a 50% drop catches real regressions
# without false-alarming on VM noise.
DEFAULT_TOLERANCE = 0.50  # 50%


def add_row(
    rows: list[dict],
    failures: list[dict],
    *,
    scenario: str,
    metric: str,
    baseline: str,
    actual: str,
    passed: bool,
):
    row = {
        "scenario": scenario,
        "metric": metric,
        "baseline": baseline,
        "actual": actual,
        "status": "Pass" if passed else "**REGRESSION**",
    }
    rows.append(row)
    if not passed:
        failures.append(row)


def load_baseline(path: Path) -> dict:
    if not path.exists():
        print(f"No baseline file at {path}, skipping regression check")
        return {}
    with open(path) as f:
        return json.load(f)


def extract_results(artifact_dir: Path) -> list[dict]:
    results = []
    for txt_file in sorted(artifact_dir.glob("**/*.txt")):
        for line in txt_file.read_text().splitlines():
            if line.startswith(JSONL_PREFIX):
                results.append(json.loads(line[len(JSONL_PREFIX) :]))
    return results


def check_regressions(
    results: list[dict], baseline: dict, tolerance: float
) -> tuple[list[dict], list[dict]]:
    """Returns (rows, failures) for the summary table."""
    rows = []
    failures = []

    for result in results:
        lang = result.get("language", "unknown")
        scenario = f"{lang}/{result.get('scenario', 'unknown')}"
        bl = baseline.get(scenario)
        if bl is None:
            rows.append(
                {
                    "scenario": scenario,
                    "metric": "-",
                    "baseline": "-",
                    "actual": "-",
                    "status": "No baseline",
                }
            )
            continue

        metrics = result.get("metrics", {})
        metadata = result.get("metadata") or {}

        # Check throughput (handler_per_s)
        tp = metrics.get("throughput")
        bl_tp = bl.get("handler_per_s")
        if tp and bl_tp:
            actual = tp["handler_per_s"]
            threshold = bl_tp * (1 - tolerance)
            passed = actual >= threshold
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="handler_per_s",
                baseline=f"{bl_tp:.0f}",
                actual=f"{actual:.0f}",
                passed=passed,
            )

        # Check enqueue throughput
        enq = metrics.get("enqueue_per_s")
        bl_enq = bl.get("enqueue_per_s")
        if enq and bl_enq:
            threshold = bl_enq * (1 - tolerance)
            passed = enq >= threshold
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="enqueue_per_s",
                baseline=f"{bl_enq:.0f}",
                actual=f"{enq:.0f}",
                passed=passed,
            )

        # Check p99 latency (higher is worse)
        lat = metrics.get("latency_ms")
        bl_p99 = bl.get("p99_ms")
        if lat and bl_p99 and lat.get("p99"):
            actual = lat["p99"]
            threshold = bl_p99 * (1 + tolerance)
            passed = actual <= threshold
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="p99_ms",
                baseline=f"{bl_p99:.1f}",
                actual=f"{actual:.1f}",
                passed=passed,
            )

        # Check MVCC overlap throughput as a ratio against the same run's
        # baseline window. This is more stable than comparing absolute numbers
        # across shared CI runners.
        min_overlap_ratio = bl.get("min_overlap_vs_baseline")
        baseline_handler = metadata.get("baseline_handler_per_s")
        overlap_handler = metadata.get("overlap_handler_per_s")
        if (
            min_overlap_ratio is not None
            and baseline_handler
            and overlap_handler is not None
        ):
            actual_ratio = overlap_handler / baseline_handler
            passed = actual_ratio >= min_overlap_ratio
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="overlap_vs_baseline",
                baseline=f">={min_overlap_ratio:.2f}",
                actual=f"{actual_ratio:.2f}",
                passed=passed,
            )

        min_cooldown_ratio = bl.get("min_cooldown_vs_baseline")
        cooldown_handler = metadata.get("cooldown_handler_per_s")
        if (
            min_cooldown_ratio is not None
            and baseline_handler
            and cooldown_handler is not None
        ):
            actual_ratio = cooldown_handler / baseline_handler
            passed = actual_ratio >= min_cooldown_ratio
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="cooldown_vs_baseline",
                baseline=f">={min_cooldown_ratio:.2f}",
                actual=f"{actual_ratio:.2f}",
                passed=passed,
            )

        max_dead_tup_delta = bl.get("max_dead_tup_delta")
        dead_tup_delta = metadata.get("dead_tup_delta")
        if max_dead_tup_delta is not None and dead_tup_delta is not None:
            passed = dead_tup_delta <= max_dead_tup_delta
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="dead_tup_delta",
                baseline=f"<={max_dead_tup_delta:.0f}",
                actual=f"{dead_tup_delta:.0f}",
                passed=passed,
            )

        max_available = bl.get("max_available")
        actual_max_available = metadata.get("max_available")
        if max_available is not None and actual_max_available is not None:
            passed = actual_max_available <= max_available
            add_row(
                rows,
                failures,
                scenario=scenario,
                metric="max_available",
                baseline=f"<={max_available:.0f}",
                actual=f"{actual_max_available:.0f}",
                passed=passed,
            )

    return rows, failures


def write_summary(rows: list[dict], failures: list[dict], summary_path: str):
    with open(summary_path, "a") as f:
        f.write("### Benchmark regression check\n\n")
        if not rows:
            f.write("No benchmark results found.\n")
            return
        f.write("| Scenario | Metric | Baseline | Actual | Status |\n")
        f.write("|----------|--------|----------|--------|--------|\n")
        for r in rows:
            f.write(
                f"| {r['scenario']} | {r['metric']} | {r['baseline']} | {r['actual']} | {r['status']} |\n"
            )
        f.write("\n")
        if failures:
            f.write(
                f"**{len(failures)} regression(s) detected** (>{DEFAULT_TOLERANCE:.0%} below baseline)\n"
            )
        else:
            f.write("All metrics within tolerance.\n")


def main():
    artifact_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("artifacts/nightly")
    baseline_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("benchmarks/baseline.json")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

    baseline = load_baseline(baseline_path)
    results = extract_results(artifact_dir)

    if not results:
        print("No benchmark results extracted from artifacts")
        write_summary([], [], summary_path)
        return

    print(f"Found {len(results)} benchmark results, {len(baseline)} baseline entries")
    rows, failures = check_regressions(results, baseline, DEFAULT_TOLERANCE)
    write_summary(rows, failures, summary_path)

    for r in rows:
        marker = "FAIL" if r["status"].startswith("**") else "ok"
        print(f"  [{marker}] {r['scenario']}/{r['metric']}: {r['actual']} (baseline: {r['baseline']})")

    if failures:
        print(f"\n{len(failures)} regression(s) detected")
        # The regression gate is enforced. Shared-runner variance is
        # real, so the baseline thresholds in
        # `benchmarks/baseline.json` should be set with enough margin
        # that healthy runs don't trip — but a genuine regression must
        # fail the job rather than be buried in a passing summary.
        sys.exit(1)


if __name__ == "__main__":
    main()

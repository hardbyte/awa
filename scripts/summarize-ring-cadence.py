#!/usr/bin/env python3
"""Export compact, auditable measurements from benchmark-ring-cadence.py."""

import argparse
from collections import defaultdict
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import shutil
import statistics


COUNTERS = {
    "catalog_updated_total",
    "catalog_inserted_total",
    "catalog_deleted_total",
    "ring_generation",
    "relfilenode_churn_total",
    "pg_db_xacts_total",
    "pg_wal_bytes",
    "pg_cpu_seconds_total",
    "adapter_cpu_seconds_total",
    "logical_consumer_bytes_total",
    "logical_consumer_disconnects_total",
    "logical_consumer_errors_total",
    "logical_consumer_exits_total",
}
GAUGES = {
    "phase_start_elapsed_s",
    "enqueue_rate",
    "completion_rate",
    "retryable_failure_rate",
    "terminal_failure_rate",
    "injected_failure_rate",
    "retried_completion_rate",
    "poison_exhausted_rate",
    "logical_consumer_paused",
    "producer_call_p99_ms",
    "subscriber_p99_ms",
    "end_to_end_p99_ms",
    "queue_depth",
    "running_depth",
    "retryable_depth",
    "scheduled_depth",
    "total_backlog",
    "ring_bytes",
    "catalog_n_dead_tup",
    "slot_active",
    "slot_catalog_xmin_age",
    "slot_retained_wal_bytes",
    "publication_tables_without_replica_identity",
}


def validate_results(results: list[dict]) -> dict:
    expected = {
        (cell, policy)
        for cell in ("idle", "low", "reference", "saturation", "retry", "logical")
        for policy in ("eager", "cadence")
    }
    if (
        len(results) != len(expected)
        or {(r["cell"], r["policy"]) for r in results} != expected
    ):
        raise ValueError("Expected six distinct paired cells")

    def value(
        record: dict,
        name: str,
        stat: str,
        phase: str = "measure",
        subject: str | None = None,
    ) -> float:
        matches = [
            metric
            for metric in record["metrics"]
            if metric["metric"] == name
            and metric["phase"] == phase
            and (subject is None or metric["subject"] == subject)
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Missing or ambiguous metric: {record['cell']}/{record['policy']} {phase}/{name}"
            )
        return matches[0][stat]

    def require(condition: bool, record: dict, message: str) -> None:
        if not condition:
            raise ValueError(f"{record['cell']}/{record['policy']}: {message}")

    consumers = {}
    for record in results:
        if record["cell"] != "idle":
            for family, rotations in (("queue", 32), ("claim", 16)):
                require(
                    value(record, "ring_generation", "last", "warmup", family)
                    >= rotations,
                    record,
                    f"insufficient in-window {family} warmup",
                )
        if record["cell"] == "idle":
            require(
                value(record, "catalog_updated_total", "per_second", subject="pg_class")
                == 0,
                record,
                "idle catalog churn",
            )
        if record["cell"] == "retry":
            require(
                value(record, "retryable_depth", "last", "recovery") == 0,
                record,
                "retryable backlog did not recover",
            )
            require(
                value(record, "injected_failure_rate", "last", "recovery") == 0,
                record,
                "injected failures did not stop",
            )
            require(
                value(record, "poison_exhausted_rate", "maximum") > 0,
                record,
                "poison workload did not exhaust attempts",
            )
        if record["cell"] == "logical":
            require(
                value(record, "logical_consumer_paused", "mean") == 1,
                record,
                "consumer was not paused for the sampled stall",
            )
            require(
                value(record, "logical_consumer_bytes_total", "first")
                == value(record, "logical_consumer_bytes_total", "last"),
                record,
                "consumer advanced during stall",
            )
            for name in ("slot_catalog_xmin_age", "slot_retained_wal_bytes"):
                require(
                    value(record, name, "last") > value(record, name, "first"),
                    record,
                    f"{name} did not increase during stall",
                )
                require(
                    value(record, name, "last", "catchup")
                    < value(record, name, "last"),
                    record,
                    f"{name} did not recover",
                )
            require(
                value(record, "slot_active", "last", "catchup") == 1,
                record,
                "consumer did not reconnect",
            )
            require(
                value(record, "logical_consumer_exits_total", "last", "catchup") == 0,
                record,
                "consumer process exited",
            )
            require(
                value(record, "publication_tables_without_replica_identity", "maximum")
                == 0,
                record,
                "published table lacks replica identity",
            )
            errors = value(record, "logical_consumer_errors_total", "last", "catchup")
            disconnects = value(
                record, "logical_consumer_disconnects_total", "last", "catchup"
            )
            require(
                errors == disconnects and errors <= 1,
                record,
                "unexpected consumer errors",
            )
            consumers[record["policy"]] = {
                "errors": errors,
                "disconnects": disconnects,
                "process_exits": 0,
            }
    return {
        "runs": len(results),
        "all_active_runs_completed_two_wraps": True,
        "retry_recovery_retryable_depth_zero": True,
        "logical_pause_output_stopped_and_wal_xmin_pinned": True,
        "logical_catchup_active_lag_reduced_no_process_exits": True,
        "logical_consumer_counters": consumers,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    args.destination.mkdir(parents=True, exist_ok=True)
    provenance = json.loads((args.source / "provenance.json").read_text())
    results = []
    selected = []
    excluded = []
    for policy in ("eager", "cadence"):
        for record_path in sorted((args.source / policy).glob("*.json")):
            record = json.loads(record_path.read_text())
            if record["binary_sha256"] != provenance["binary_sha256"]:
                raise ValueError(f"Mixed benchmark executables: {record_path}")
            run = Path(record["run_dir"])
            raw_path = run / "raw.csv"
            record["raw_sha256"] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            manifest = json.loads((run / "manifest.json").read_text())
            durations = {
                phase["label"]: phase["duration_s"] for phase in manifest["phases"]
            }
            with raw_path.open() as source:
                starts = [
                    row
                    for row in csv.DictReader(source)
                    if row["metric"] == "phase_start_elapsed_s"
                ]
            if len(starts) != len(durations) or {
                row["phase_label"] for row in starts
            } != set(durations):
                raise ValueError(
                    f"Missing or duplicate phase boundaries: {record_path}"
                )
            bounds = {
                row["phase_label"]: {
                    "start_elapsed_s": float(row["value"]),
                    "end_elapsed_s": float(row["value"])
                    + durations[row["phase_label"]],
                }
                for row in starts
            }
            record["phase_bounds"] = bounds
            record["samples_excluded_outside_phase_window"] = 0
            groups = defaultdict(list)
            with raw_path.open() as source:
                for row in csv.DictReader(source):
                    if row["metric"] not in COUNTERS | GAUGES:
                        continue
                    elapsed = float(row["elapsed_s"])
                    value = float(row["value"])
                    if not math.isfinite(elapsed) or not math.isfinite(value):
                        raise ValueError(f"Non-finite sample: {record_path} {row}")
                    window = bounds[row["phase_label"]]
                    if (
                        not window["start_elapsed_s"]
                        <= elapsed
                        < window["end_elapsed_s"]
                    ):
                        record["samples_excluded_outside_phase_window"] += 1
                        excluded.append(
                            {"cell": record["cell"], "policy": policy, **row}
                        )
                        continue
                    selected.append({"cell": record["cell"], "policy": policy, **row})
                    key = (
                        row["phase_label"],
                        row["subject_kind"],
                        row["subject"],
                        row["metric"],
                    )
                    groups[key].append((elapsed, value))
            metrics = []
            for (phase, kind, subject, metric), samples in sorted(groups.items()):
                samples.sort()
                values = [value for _, value in samples]
                summary = {
                    "phase": phase,
                    "kind": kind,
                    "subject": subject,
                    "metric": metric,
                    "samples": len(samples),
                    "first_elapsed_s": samples[0][0],
                    "last_elapsed_s": samples[-1][0],
                    "first": values[0],
                    "last": values[-1],
                }
                if metric in COUNTERS:
                    elapsed = samples[-1][0] - samples[0][0]
                    if elapsed > 0:
                        if any(right < left for left, right in zip(values, values[1:])):
                            raise ValueError(f"Counter reset: {record_path} {summary}")
                        summary["per_second"] = (values[-1] - values[0]) / elapsed
                else:
                    summary.update(
                        mean=statistics.mean(values),
                        median=statistics.median(values),
                        maximum=max(values),
                    )
                metrics.append(summary)
            record["metrics"] = metrics
            results.append(record)
            for name in ("manifest.json",):
                shutil.copy2(
                    run / name, args.destination / f"{policy}-{record['cell']}-{name}"
                )
    validation = validate_results(results)
    validation["selected_sample_rows"] = len(selected)
    validation["excluded_sample_rows"] = len(excluded)
    validation["monotonic_counter_groups"] = sum(
        metric["metric"] in COUNTERS
        for record in results
        for metric in record["metrics"]
    )
    validation["single_executable_sha256"] = provenance["binary_sha256"]
    (args.destination / "validation.json").write_text(
        json.dumps(validation, indent=2) + "\n"
    )
    (args.destination / "measurements.json").write_text(
        json.dumps(results, indent=2) + "\n"
    )
    shutil.copy2(args.source / "provenance.json", args.destination / "provenance.json")
    with gzip.open(args.destination / "adapter-Cargo.lock.gz", "wb") as output:
        output.write((args.source / "eager/awa-bench/Cargo.lock").read_bytes())
    for filename, rows in (
        ("samples.csv.gz", selected),
        ("excluded-samples.csv.gz", excluded),
    ):
        with gzip.open(args.destination / filename, "wt", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(selected[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()

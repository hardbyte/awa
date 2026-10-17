#!/usr/bin/env python3
"""Export compact, auditable measurements from benchmark-ring-cadence.py."""

import argparse
from collections import defaultdict
import csv
import gzip
import hashlib
import json
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
}
GAUGES = {
    "enqueue_rate",
    "completion_rate",
    "retryable_failure_rate",
    "terminal_failure_rate",
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    args.destination.mkdir(parents=True, exist_ok=True)
    results = []
    selected = []
    for policy in ("eager", "cadence"):
        for record_path in sorted((args.source / policy).glob("*.json")):
            record = json.loads(record_path.read_text())
            run = Path(record["run_dir"])
            raw_path = run / "raw.csv"
            record["raw_sha256"] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            groups = defaultdict(list)
            with raw_path.open() as source:
                for row in csv.DictReader(source):
                    if row["metric"] not in COUNTERS | GAUGES:
                        continue
                    selected.append({"cell": record["cell"], "policy": policy, **row})
                    key = (
                        row["phase_label"],
                        row["subject_kind"],
                        row["subject"],
                        row["metric"],
                    )
                    groups[key].append((float(row["elapsed_s"]), float(row["value"])))
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
                        if values[-1] < values[0]:
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
    if len(results) != 12:
        raise ValueError(f"Expected six paired cells; found {len(results)}")
    (args.destination / "measurements.json").write_text(
        json.dumps(results, indent=2) + "\n"
    )
    shutil.copy2(args.source / "provenance.json", args.destination / "provenance.json")
    with gzip.open(args.destination / "samples.csv.gz", "wt", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(selected[0]))
        writer.writeheader()
        writer.writerows(selected)


if __name__ == "__main__":
    main()

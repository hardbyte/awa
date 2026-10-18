"""Package completed review measurements and their audit inputs."""

import argparse
import csv
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import statistics


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compress(source, destination):
    with source.open("rb") as original, gzip.open(destination, "wb") as packed:
        shutil.copyfileobj(original, packed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pr", choices=("521", "524"))
    parser.add_argument("destination", type=Path)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--host-load", type=Path)
    args = parser.parse_args()
    root = args.root or Path(f"/tmp/awa-{args.pr}-benchmark")
    evidence = root / "evidence"
    assert (evidence / "runner-provenance.json").exists(), "matrix is incomplete"
    destination = args.destination
    destination.mkdir(parents=True, exist_ok=True)
    for source in evidence.iterdir():
        shutil.copy2(source, destination / source.name)

    records = []
    for build in ("baseline", "candidate"):
        directory = root / build
        compress(
            directory / "awa-bench/Cargo.lock",
            destination / f"{build}-adapter-Cargo.lock.gz",
        )
        shutil.copy2(directory / "build.json", destination / f"{build}-build.json")
        compress(directory / "build.log", destination / f"{build}-build.log.gz")
        shutil.copy2(
            directory / "awa-bench/Cargo.toml",
            destination / f"{build}-adapter-Cargo.toml",
        )
        shutil.copy2(
            directory / "awa-bench/.cargo/config.toml",
            destination / f"{build}-cargo-patch.toml",
        )
        for name in (
            "docker-compose.yml",
            "docker-compose.override.yml",
            "postgres.conf",
            "init-databases.sql",
        ):
            shutil.copy2(directory / name, destination / f"{build}-{name}")
        for cell in ("reference", "saturation"):
            record = json.loads((evidence / f"{build}-{cell}.json").read_text())
            records.append(record)
            raw = Path(record["run_dir"]) / "raw.csv"
            assert digest(raw) == record["raw_sha256"]
            compress(raw, destination / f"{build}-{cell}-raw.csv.gz")
            compress(directory / f"{cell}.log", destination / f"{build}-{cell}.log.gz")

    def metric(record, name, stat, subject=None):
        matches = [
            m
            for m in record["metrics"]
            if m["phase"] == "measure"
            and m["metric"] == name
            and (subject is None or m["subject"] == subject)
        ]
        assert len(matches) == 1, (name, subject, len(matches))
        return matches[0][stat]

    results = []
    intervals = []
    for record in records:
        assert record["source"]["runtime_diff"] == ""
        assert record["warmup_generations"]["queue"] >= 32
        assert record["warmup_generations"]["claim"] >= 16
        assert record["finalize_errors"] == 0
        with gzip.open(
            destination / f"{record['build']}-{record['cell']}-raw.csv.gz", "rt"
        ) as source:
            phase = [
                r
                for r in csv.DictReader(source)
                if r["metric"] == "phase_start_elapsed_s"
            ]
        intervals.extend(
            (
                datetime.fromisoformat(
                    r["sampled_at"].replace("Z", "+00:00")
                ).timestamp(),
                datetime.fromisoformat(
                    r["sampled_at"].replace("Z", "+00:00")
                ).timestamp()
                + 240,
            )
            for r in phase
        )
        results.append(
            {
                "build": record["build"],
                "cell": record["cell"],
                "handler_returns_per_second": metric(record, "completion_rate", "mean"),
                "handler_window_p99_median_ms": metric(
                    record, "end_to_end_p99_ms", "median"
                ),
                "handler_window_p99_worst_ms": metric(
                    record, "end_to_end_p99_ms", "maximum"
                ),
                "pg_class_updates_per_second": metric(
                    record, "catalog_updated_total", "per_second", "pg_class"
                ),
                "queue_depth_mean": metric(record, "queue_depth", "mean"),
                "queue_depth_maximum": metric(record, "queue_depth", "maximum"),
                "ring_mib_mean": metric(record, "ring_bytes", "mean") / 2**20,
                "queue_warmup_generations": record["warmup_generations"]["queue"],
                "claim_warmup_generations": record["warmup_generations"]["claim"],
            }
        )

    host = list(csv.DictReader(args.host_load.open())) if args.host_load else []
    host = [
        r
        for r in host
        if r["matrix"] == args.pr
        and any(
            start <= datetime.fromisoformat(r["utc"]).timestamp() < end
            for start, end in intervals
        )
    ]
    if host:
        with (destination / "host-load.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(host[0]))
            writer.writeheader()
            writer.writerows(host)
    load = {
        key: {
            "median": statistics.median(float(r[key]) for r in host),
            "maximum": max(float(r[key]) for r in host),
        }
        for key in (
            ()
            if not host
            else (
                "load1",
                "cpu_some_avg10_pct",
                "io_some_avg10_pct",
                "io_full_avg10_pct",
                "memory_full_avg10_pct",
            )
        )
    }
    (destination / "results.json").write_text(
        json.dumps({"measurements": results, "host_load": load}, indent=2) + "\n"
    )
    print(json.dumps({"measurements": results, "host_load": load}, indent=2))


if __name__ == "__main__":
    main()

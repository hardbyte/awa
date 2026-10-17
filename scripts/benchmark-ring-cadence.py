#!/usr/bin/env python3
"""Run an isolated, warmed A/B using postgresql-job-queue-benchmarking.

Run with that repository's locked Python environment. Copies are private;
the source harness and its adapter checkout are never modified.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def command(argv: list[str], **kwargs) -> str:
    return subprocess.check_output(argv, text=True, **kwargs).strip()


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if text.count(old) != 1:
        raise ValueError(f"Expected exactly one patch anchor in {path}: {old!r}")
    path.write_text(text.replace(old, new))


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def prepare(harness: Path, destination: Path, awa: Path, cpus: str) -> None:
    destination.mkdir(parents=True)
    for name in ("bench_harness", "awa-bench"):
        shutil.copytree(
            harness / name,
            destination / name,
            ignore=shutil.ignore_patterns("target", ".venv", "__pycache__"),
        )
    for name in ("docker-compose.yml", "postgres.conf", "init-databases.sql"):
        shutil.copy2(harness / name, destination / name)
    (destination / "docker-compose.override.yml").write_text(
        f'services:\n  postgres:\n    cpuset: "{cpus}"\n'
    )
    (destination / "awa-bench/.cargo/config.toml").write_text(
        '[patch."https://github.com/hardbyte/awa"]\n'
        + "".join(
            f"{crate} = {{ path = {json.dumps(str(awa / crate))} }}\n"
            for crate in ("awa-model", "awa-worker", "awa-macros")
        )
    )
    replace_once(
        destination / "bench_harness/orchestrator.py",
        "    entry.builder(False)\n",
        "    # Use the verified prebuilt A/B executable.\n",
    )
    replace_once(
        destination / "awa-bench/src/long_horizon.rs",
        ".transition_role(TransitionWorkerRole::QueueStorageTarget)",
        '.ring_rotation_cadence(match std::env::var("AWA_RING_ROTATION_POLICY").as_deref() {\n'
        '                Ok("eager") => awa_model::RingRotationCadence::EAGER,\n'
        '                Ok("cadence") => awa_model::RingRotationCadence::DEFAULT,\n'
        '                policy => panic!("invalid rotation policy: {policy:?}"),\n'
        "            })\n            .transition_role(TransitionWorkerRole::QueueStorageTarget)",
    )
    extra_metrics = '''            catalog_counters = self._tick_query(conn, cur, """
                SELECT relname, n_tup_upd, n_tup_ins, n_tup_del FROM pg_stat_sys_tables
                WHERE relname IN ('pg_class', 'pg_statistic')
            """, fetchall=True) or []
            for name, updated, inserted, deleted in catalog_counters:
                for metric, value in (("catalog_updated_total", updated),
                                      ("catalog_inserted_total", inserted),
                                      ("catalog_deleted_total", deleted)):
                    self._emit(subject_kind="catalog", subject=name,
                               metric=metric, value=float(value))
            for family in ("queue", "claim", "lease"):
                ring = self._tick_query(conn, cur,
                    f"SELECT max(generation) FROM awa.{family}_ring_rotations")
                if ring is not None:
                    self._emit(subject_kind="ring", subject=family,
                               metric="ring_generation", value=float(ring[0]))
            size = self._tick_query(conn, cur, """
                SELECT COALESCE(sum(pg_total_relation_size(inhrelid)), 0)
                FROM pg_inherits i JOIN pg_class parent ON parent.oid = i.inhparent
                JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
                WHERE namespace.nspname = 'awa' AND parent.relkind = 'p'
            """)
            if size is not None:
                self._emit(subject_kind="ring", subject="all", metric="ring_bytes", value=float(size[0]))

'''
    replace_once(
        destination / "bench_harness/metrics.py",
        "            slot_rows = self._tick_query(\n",
        extra_metrics + "            slot_rows = self._tick_query(\n",
    )


def run_cell(
    root: Path,
    target: Path,
    policy: str,
    cell: str,
    cpus: str,
    port: int,
    options: list[str],
) -> dict:
    directory = root / policy
    log = directory / f"{cell}.log"
    expected_digest = json.loads((root / "provenance.json").read_text())[
        "binary_sha256"
    ]
    if digest(target / "release/awa-bench") != expected_digest:
        raise RuntimeError("Benchmark executable changed before cell")
    env = {
        **os.environ,
        "COMPOSE_PROJECT_NAME": f"awa518-{policy}",
        "BENCH_PG_PORT": str(port),
        "POSTGRES_CPUS": "4",
        "CARGO_TARGET_DIR": str(target),
        "SQLX_OFFLINE": "true",
        "AWA_RING_ROTATION_POLICY": policy,
        "JOB_MAX_ATTEMPTS": "5",
        "QUEUE_SLOT_COUNT": "16",
        "LEASE_SLOT_COUNT": "8",
        "CLAIM_SLOT_COUNT": "8",
        "QUEUE_STRIPE_COUNT": "1",
        "LEASE_CLAIM_RECEIPTS": "true",
        "QUEUE_ROTATE_MS": "1000",
        "LEASE_ROTATE_MS": "250",
    }
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
    ]
    print(f"START {policy}/{cell}", flush=True)
    with log.open("w") as output:
        result = subprocess.run(
            argv, cwd=directory, env=env, stdout=output, stderr=subprocess.STDOUT
        )
    if result.returncode:
        raise RuntimeError(f"{policy}/{cell} failed ({result.returncode}); see {log}")
    if digest(target / "release/awa-bench") != expected_digest:
        raise RuntimeError("Benchmark executable changed during cell")
    run_dir = max((directory / "results").iterdir(), key=lambda p: p.stat().st_mtime)
    record = {
        "policy": policy,
        "cell": cell,
        "run_dir": str(run_dir),
        "argv": argv,
        "cpus": cpus,
        "port": port,
        "binary_sha256": expected_digest,
    }
    rows = list(csv.DictReader((run_dir / "raw.csv").open()))
    if cell != "idle":
        generations = {
            family: max(
                float(row["value"])
                for row in rows
                if row["phase_label"] == "warmup"
                and row["metric"] == "ring_generation"
                and row["subject"] == family
            )
            for family in ("queue", "claim")
        }
        record["warmup_generations"] = generations
        if generations["queue"] < 32 or generations["claim"] < 16:
            raise RuntimeError(f"Insufficient actual ring warmup: {record}")
    (directory / f"{cell}.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"DONE {policy}/{cell} {record.get('warmup_generations', {})}", flush=True)
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--adapter-lock", type=Path, help="Cargo.lock or Cargo.lock.gz for the adapter"
    )
    parser.add_argument(
        "--cells", default="idle,low,reference,saturation,retry,logical"
    )
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    awa = Path(__file__).resolve().parent.parent
    harness, root = args.harness.resolve(), args.output.resolve()
    target = root / "target"
    policies = [("eager", "0-3,12-15", 15558), ("cadence", "6-9,18-21", 15559)]
    root.mkdir(parents=True, exist_ok=True)
    if not args.resume:
        for policy, cpus, _ in policies:
            prepare(harness, root / policy, awa, cpus)
            if args.adapter_lock:
                lock = args.adapter_lock.read_bytes()
                if args.adapter_lock.suffix == ".gz":
                    lock = gzip.decompress(lock)
                (root / policy / "awa-bench/Cargo.lock").write_bytes(lock)
        with (root / "build.log").open("w") as log:
            subprocess.run(
                ["cargo", "build", "--release", "--locked"],
                cwd=root / "eager/awa-bench",
                env={
                    **os.environ,
                    "CARGO_TARGET_DIR": str(target),
                    "SQLX_OFFLINE": "true",
                },
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
        provenance = {
            "awa_head": command(["git", "rev-parse", "HEAD"], cwd=awa),
            "awa_diff_sha256": hashlib.sha256(
                subprocess.check_output(["git", "diff", "HEAD"], cwd=awa)
            ).hexdigest(),
            "harness_head": command(["git", "rev-parse", "HEAD"], cwd=harness),
            "harness_status": command(["git", "status", "--short"], cwd=harness),
            "awa_runtime_diff_sha256": hashlib.sha256(
                subprocess.check_output(
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
                    cwd=awa,
                )
            ).hexdigest(),
            "rustc_version": command(["rustc", "--version", "--verbose"]),
            "adapter_lock_sha256": digest(root / "eager/awa-bench/Cargo.lock"),
            "runner_sha256": digest(Path(__file__).resolve()),
            "binary_sha256": hashlib.file_digest(
                (target / "release/awa-bench").open("rb"), "sha256"
            ).hexdigest(),
        }
        (root / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    common = ["--phase", "warmup=warmup:240s"]
    cells = {
        "idle": [
            "--producer-rate",
            "0",
            "--worker-count",
            "8",
            "--phase",
            "warmup=warmup:60s",
            "--phase",
            "measure=idle-background(rate=0):180s",
        ],
        "low": [
            "--producer-rate",
            "200",
            "--worker-count",
            "8",
            *common,
            "--phase",
            "measure=clean:240s",
        ],
        "reference": [
            "--producer-rate",
            "800",
            "--worker-count",
            "32",
            *common,
            "--phase",
            "measure=clean:240s",
        ],
        "saturation": [
            "--producer-rate",
            "50000",
            "--producer-mode",
            "depth-target",
            "--target-depth",
            "4000",
            "--worker-count",
            "64",
            *common,
            "--phase",
            "measure=clean:240s",
        ],
        "retry": [
            "--producer-rate",
            "200",
            "--worker-count",
            "16",
            *common,
            "--phase",
            "measure=retry-storm(transient_pct=30,poison_pct=1):240s",
            "--phase",
            "recovery=recovery:120s",
        ],
        "logical": [
            "--producer-rate",
            "400",
            "--worker-count",
            "16",
            *common,
            "--phase",
            "stream=logical-stream(publication=all):60s",
            "--phase",
            "measure=logical-stall:240s",
            "--phase",
            "catchup=logical-stream(publication=all):60s",
        ],
    }
    for cell in args.cells.split(","):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    run_cell, root, target, policy, cell, cpus, port, cells[cell]
                )
                for policy, cpus, port in policies
                if not (root / policy / f"{cell}.json").exists()
            ]
            for future in futures:
                future.result()


if __name__ == "__main__":
    main()

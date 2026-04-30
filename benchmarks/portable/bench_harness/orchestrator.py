"""Orchestrator: sequence phases for each system, manage PG lifecycle, tail
adapter stdout, drive the metrics daemon, write outputs, render plots.

The orchestrator is the only place that spans the full run. Everything else
is a module that does one job (DSL, metrics, writers, plots).
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import signal
import shutil
import subprocess
import sys
import threading
import time
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import psycopg

from . import adapters as adapters_mod
from . import writers as writers_mod
from .adapters import (
    ADAPTERS,
    DEFAULT_PG_IMAGE,
    DEFAULT_SYSTEMS,
    AdapterEntry,
    AdapterManifest,
    pg_url,
)
from .metrics import MetricsDaemon, PollTargets, parse_adapter_record
from .phases import (
    Phase,
    PhaseType,
    PhaseRuntime,
    default_registry,
    resolve_scenario,
)
from .plots import render_all
from .report import write_interactive_report
from .replica_pool import ReplicaPool
from .sample import Sample
from .versions import capture_adapter_revision
from .writers import (
    RawCsvWriter,
    build_manifest,
    compute_summary,
    write_manifest,
    write_run_readme,
    write_summary,
)

SCRIPT_DIR = Path(__file__).resolve().parent.parent
RESULTS_ROOT = SCRIPT_DIR / "results"
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"


# ────────────────────────────────────────────────────────────────────────
# Phase tracker: shared between metrics daemon and stdout tailer
# ────────────────────────────────────────────────────────────────────────


class PhaseTracker:
    def __init__(self) -> None:
        self._label = "pre-run"
        self._type = "warmup"
        self._lock = threading.Lock()

    def set(self, label: str, type_str: str) -> None:
        with self._lock:
            self._label = label
            self._type = type_str

    def get(self) -> tuple[str, str]:
        with self._lock:
            return self._label, self._type


# ────────────────────────────────────────────────────────────────────────
# Postgres lifecycle
# ────────────────────────────────────────────────────────────────────────


def _run_cmd(
    argv: list[str],
    *,
    cwd: Path | None = None,
    env: dict | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    print(f"[harness] $ {' '.join(argv)}", file=sys.stderr)
    return subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        env={**os.environ, **(env or {})},
        check=check,
    )


def _compose_env(pg_image: str) -> dict[str, str]:
    return {"POSTGRES_IMAGE": pg_image}


def start_postgres(pg_image: str) -> None:
    _run_cmd(
        ["docker", "compose", "up", "-d", "--wait"],
        cwd=SCRIPT_DIR,
        env=_compose_env(pg_image),
    )
    # Readiness probe
    for _ in range(30):
        r = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                "bench",
            ],
            cwd=str(SCRIPT_DIR),
            env={**os.environ, **_compose_env(pg_image)},
            capture_output=True,
        )
        if r.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError("Postgres did not become ready in time")

    last_error: Exception | None = None
    for _ in range(30):
        try:
            with psycopg.connect(pg_url("postgres"), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return
        except psycopg.Error as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(
        f"Postgres passed health checks but never became query-ready: {last_error}"
    )


def stop_postgres(pg_image: str) -> None:
    if os.environ.get("KEEP_DB"):
        print("[harness] KEEP_DB set — leaving postgres running for inspection")
        return
    _run_cmd(
        ["docker", "compose", "down", "-v"],
        cwd=SCRIPT_DIR,
        env=_compose_env(pg_image),
        check=False,
    )


def preflight_database(manifest: AdapterManifest, *, recreate: bool = False) -> None:
    """Create the per-system database if missing, install declared extensions,
    plus pgstattuple (required by the harness). Fail fast on errors."""
    admin_url = pg_url("postgres")
    with psycopg.connect(admin_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            if recreate:
                cur.execute(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                    """,
                    (manifest.db_name,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{manifest.db_name}"')
                cur.execute(f'CREATE DATABASE "{manifest.db_name}"')
            else:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (manifest.db_name,),
                )
                if cur.fetchone() is None:
                    # Database identifier can't be parameterised; this comes from
                    # the adapter manifest file (trusted) and matches [a-z_]+.
                    cur.execute(f'CREATE DATABASE "{manifest.db_name}"')

    target_url = pg_url(manifest.db_name)
    required_exts = list(manifest.extensions) + ["pgstattuple"]
    with psycopg.connect(target_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            for ext in required_exts:
                try:
                    cur.execute(f'CREATE EXTENSION IF NOT EXISTS "{ext}"')
                except psycopg.Error as exc:
                    raise RuntimeError(
                        f"Failed to CREATE EXTENSION {ext} in {manifest.db_name}: "
                        f"{exc}. Ensure the Postgres image provides this "
                        f"extension (e.g. via a custom Dockerfile)."
                    ) from exc


# ────────────────────────────────────────────────────────────────────────
# Adapter process management
# ────────────────────────────────────────────────────────────────────────


# Lifecycle state — subprocess handle / tailer / descriptor per replica —
# lives on `ReplicaSlot`. Lifecycle methods (start/stop/kill/restart) live
# on `ReplicaPool`. Orchestrator here builds the pool from a
# `_launch_one_replica` closure and exposes the pool on phase_state so
# destructive phase types can drive it.


def _tail_stdout(
    proc: subprocess.Popen,
    *,
    run_id: str,
    system: str,
    bench_start: float,
    get_phase,
    out_queue: "queue.Queue[Sample]",
    descriptor_holder: dict,
    stop_event: threading.Event,
) -> None:
    assert proc.stdout is not None
    for raw in iter(proc.stdout.readline, ""):
        if stop_event.is_set():
            break
        line = raw.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            # Non-JSON stdout — log and continue.
            print(f"[{system}] {line}", file=sys.stderr)
            continue
        if rec.get("kind") == "descriptor":
            descriptor_holder["descriptor"] = rec
            continue
        if rec.get("kind") != "adapter":
            continue
        s = parse_adapter_record(
            line,
            run_id=run_id,
            expected_system=system,
            bench_start=bench_start,
            get_phase=get_phase,
        )
        if s:
            out_queue.put(s)


def _launch_one_replica(
    system: str,
    entry: AdapterEntry,
    manifest: AdapterManifest,
    overrides: dict[str, str],
    *,
    instance_id: int,
    run_id: str,
    bench_start: float,
    tracker: PhaseTracker,
    out_queue: "queue.Queue[Sample]",
) -> tuple[subprocess.Popen, threading.Thread, threading.Event, dict]:
    """Launch one replica and wait for its startup descriptor."""
    # Per-instance env: the only variable is BENCH_INSTANCE_ID. Copy the
    # shared overrides first so the caller's producer-rate / worker-count
    # values carry through; stamp the id last so it can't be overridden.
    instance_overrides = dict(overrides)
    instance_overrides["BENCH_INSTANCE_ID"] = str(instance_id)
    spec = entry.launcher(manifest, instance_overrides)
    env = {**os.environ, **spec.env}
    print(
        f"[harness] launching {system} replica {instance_id}: "
        f"{' '.join(spec.argv[:2])}...",
        file=sys.stderr,
    )
    proc = subprocess.Popen(
        spec.argv,
        cwd=spec.cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
        bufsize=1,  # line-buffered
    )
    stop_event = threading.Event()
    descriptor_holder: dict = {}
    tailer = threading.Thread(
        target=_tail_stdout,
        args=(proc,),
        kwargs=dict(
            run_id=run_id,
            system=system,
            bench_start=bench_start,
            get_phase=tracker.get,
            out_queue=out_queue,
            descriptor_holder=descriptor_holder,
            stop_event=stop_event,
        ),
        name=f"tail-{system}-{instance_id}",
        daemon=True,
    )
    tailer.start()

    def _abort(message: str) -> "RuntimeError":
        # Tear down the child before raising so we don't leak a subprocess
        # when descriptor handshake fails for any reason.
        stop_event.set()
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        tailer.join(timeout=2.0)
        return RuntimeError(message)

    # Wait up to 60s for descriptor to arrive so we can cross-check against
    # the static manifest. Adapters are expected to emit it promptly.
    deadline = time.time() + 60
    while "descriptor" not in descriptor_holder and time.time() < deadline:
        if proc.poll() is not None:
            raise _abort(
                f"{system} replica {instance_id} exited during startup "
                f"(rc={proc.returncode}) before emitting a descriptor record."
            )
        time.sleep(0.1)
    descriptor = descriptor_holder.get("descriptor")
    if not descriptor:
        raise _abort(
            f"{system} replica {instance_id} did not emit a startup "
            "descriptor within 60s. The harness requires a descriptor "
            "record to cross-check the adapter against its static "
            "manifest; running blind would let drift slip through "
            "unnoticed."
        )
    try:
        _check_descriptor_drift(manifest, descriptor)
    except RuntimeError as exc:
        raise _abort(str(exc)) from exc
    return proc, tailer, stop_event, descriptor


def build_replica_pool(
    system: str,
    entry: AdapterEntry,
    manifest: AdapterManifest,
    overrides: dict[str, str],
    *,
    replicas: int,
    run_id: str,
    bench_start: float,
    tracker: PhaseTracker,
    out_queue: "queue.Queue[Sample]",
) -> ReplicaPool:
    """Construct a pool whose ``launch_fn`` captures this run's context.

    Start/restart operations on the pool will re-invoke the launcher
    with the same entry, manifest, overrides, and ingestion pipeline
    that the initial launch used — crucial for destructive scenarios
    (kill-worker then start-worker) to come back up identically.
    """

    def _launch_fn(instance_id: int):
        return _launch_one_replica(
            system,
            entry,
            manifest,
            overrides,
            instance_id=instance_id,
            run_id=run_id,
            bench_start=bench_start,
            tracker=tracker,
            out_queue=out_queue,
        )

    return ReplicaPool(system=system, capacity=replicas, launch_fn=_launch_fn)


def _check_cross_replica_drift(pool: ReplicaPool) -> None:
    """Warn when replicas of the same system disagree on static descriptor
    fields. Not fatal — mixed-version scenarios are a planned future
    iteration and the harness shouldn't pre-emptively ban them."""
    first = None
    first_tables: list[str] = []
    first_exts: list[str] = []
    for slot in pool.slots:
        if slot.descriptor is None:
            continue
        if first is None:
            first = slot
            first_tables = sorted(slot.descriptor.get("event_tables") or [])
            first_exts = sorted(slot.descriptor.get("extensions") or [])
            continue
        if sorted(slot.descriptor.get("event_tables") or []) != first_tables:
            print(
                f"[{pool.system}] WARNING: replica {slot.instance_id} "
                f"declared event_tables different from replica "
                f"{first.instance_id}",
                file=sys.stderr,
            )
        if sorted(slot.descriptor.get("extensions") or []) != first_exts:
            print(
                f"[{pool.system}] WARNING: replica {slot.instance_id} "
                f"declared extensions different from replica "
                f"{first.instance_id}",
                file=sys.stderr,
            )


def _check_descriptor_drift(manifest: AdapterManifest, descriptor: dict) -> None:
    rt_tables = set(descriptor.get("event_tables") or [])
    rt_exts = set(descriptor.get("extensions") or [])
    static_tables = set(manifest.event_tables)
    static_exts = set(manifest.extensions)
    if not rt_tables.issuperset(static_tables):
        missing = static_tables - rt_tables
        raise RuntimeError(
            f"{manifest.system}: runtime descriptor missing event tables "
            f"declared in adapter.json: {sorted(missing)}"
        )
    if not rt_exts.issuperset(static_exts):
        missing = static_exts - rt_exts
        raise RuntimeError(
            f"{manifest.system}: runtime descriptor missing extensions "
            f"declared in adapter.json: {sorted(missing)}"
        )


# Teardown lives on the pool now: `pool.stop_all(timeout_s=...)` walks
# every RUNNING replica and transitions it to STOPPED. Prior incarnations
# that were KILLED or CRASHED mid-run are left as-is (their handles are
# already cleared on the slot). See ReplicaPool.stop_all.


# ────────────────────────────────────────────────────────────────────────
# Drain thread: pulls samples off the queue, writes to raw.csv
# ────────────────────────────────────────────────────────────────────────


def _drain_loop(
    out_queue: "queue.Queue[Sample]", writer: RawCsvWriter, stop_event: threading.Event
) -> None:
    while not stop_event.is_set() or not out_queue.empty():
        try:
            s = out_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        writer.write(s)
    writer.flush()


# ────────────────────────────────────────────────────────────────────────
# Running a single system through the full phase list
# ────────────────────────────────────────────────────────────────────────


def run_one_system(
    system: str,
    *,
    phases: list[Phase],
    pg_image: str,
    fast: bool,
    run_id: str,
    out_queue: "queue.Queue[Sample]",
    tracker: PhaseTracker,
    sample_every_s: int,
    producer_rate: int,
    producer_mode: str,
    target_depth: int,
    worker_count: int,
    high_load_multiplier: float,
    replicas: int = 1,
) -> dict:
    entry = ADAPTERS[system]
    manifest = AdapterManifest.load(entry.bench_dir)
    print(f"\n=== [{system}] === ({manifest.db_name})", file=sys.stderr)

    # Re-run the builder right before launching. The pre-loop pre-build
    # in `drive` is the bulk of the work; this second call is
    # idempotent (cargo / docker build cache hit, <1s) and protects
    # against an adapter image being pruned during a multi-hour run
    # ahead of it (`pgboss-bench` was missing at hour 2 of a 4-system
    # consolidated run despite being built at startup, because docker
    # GC reclaimed it during the long pgque phase).
    entry.builder(False)

    # Sequential per-system fresh-PG isolation is the default.
    if not fast:
        stop_postgres(pg_image)
        start_postgres(pg_image)

    preflight_database(manifest, recreate=fast)

    overrides: dict[str, str] = {
        "SAMPLE_EVERY_S": str(sample_every_s),
        "PRODUCER_RATE": str(producer_rate),
        "PRODUCER_MODE": producer_mode,
        "TARGET_DEPTH": str(target_depth),
        "WORKER_COUNT": str(worker_count),
    }
    control_dir = Path(tempfile.mkdtemp(prefix=f"bench-control-{system}-"))
    control_file = control_dir / "producer_rate.txt"
    control_file.write_text(str(producer_rate))
    overrides["PRODUCER_RATE_CONTROL_FILE"] = str(control_file)
    overrides["PRODUCER_RATE_CONTROL_FILE_HOST"] = str(control_file)
    overrides["PRODUCER_RATE_CONTROL_FILE_CONTAINER"] = "/control/producer_rate.txt"

    bench_start = time.time()
    # Stamp the tracker to the first phase before tailers start ingesting.
    # Without this, early samples land under the *previous* system's final
    # phase (or "pre-run" for the first system), skewing per-phase aggregates
    # — especially visible with --replicas N where pool.start_all() takes
    # longer. tracker.set is called again at each phase boundary below.
    tracker.set(phases[0].label, phases[0].type.value)
    pool = build_replica_pool(
        system,
        entry,
        manifest,
        overrides,
        replicas=replicas,
        run_id=run_id,
        bench_start=bench_start,
        tracker=tracker,
        out_queue=out_queue,
    )
    pool.start_all()
    _check_cross_replica_drift(pool)
    runtime_descriptor = pool.descriptor or {}
    runtime_event_tables = list(
        runtime_descriptor.get("event_tables") or manifest.event_tables
    )
    runtime_event_indexes = list(
        runtime_descriptor.get("event_indexes") or manifest.event_indexes
    )

    # Register the metrics daemon now so it covers all phases including warmup.
    daemon = MetricsDaemon(
        run_id=run_id,
        system=system,
        database_url=pg_url(manifest.db_name),
        targets=PollTargets(
            event_tables=runtime_event_tables,
            event_indexes=runtime_event_indexes,
        ),
        output_queue=out_queue,
        bench_start=bench_start,
        get_phase=tracker.get,
        period_s=sample_every_s,
    )
    daemon.start()

    registry = default_registry()
    phase_state: dict[str, object] = {
        "producer_rate_control_file": str(control_file),
        "base_producer_rate": float(producer_rate),
        "high_load_multiplier": float(high_load_multiplier),
        # Exposed so the active-readers hook can scan this system's hot
        # table instead of the catalog. See hooks.enter_active_readers.
        "event_tables": runtime_event_tables,
        # Destructive / lifecycle phases (kill-worker, stop-worker,
        # rolling-replace — landing in #174 step 4+) act on this.
        # Pre-existing phase hooks ignore it.
        "replica_pool": pool,
    }
    try:
        for phase in phases:
            tracker.set(phase.label, phase.type.value)
            print(
                f"[{system}] phase {phase.label} ({phase.type.value}) "
                f"for {phase.duration_s}s",
                file=sys.stderr,
            )
            runtime = PhaseRuntime(
                database_url=pg_url(manifest.db_name),
                phase=phase,
                state=phase_state,
            )
            registry.enter(runtime)
            try:
                _sleep_or_abort(phase.duration_s, pool)
            finally:
                registry.exit(runtime)
                # Phase-boundary snapshot: pgstattuple / pgstatindex.
                daemon.phase_boundary_snapshot()
    finally:
        daemon.stop()
        daemon.join(timeout=5.0)
        pool.stop_all()
        shutil.rmtree(control_dir, ignore_errors=True)

    # Representative descriptor for manifest inclusion. Prefer replica 0's
    # if still available; otherwise any slot's (replica 0 may have been
    # killed mid-run by a destructive phase and not restarted).
    return pool.descriptor or {}


def _sleep_or_abort(seconds: float, pool: ReplicaPool) -> None:
    """Sleep in small increments so replica crash is noticed quickly.

    Only replicas in state RUNNING are watched. A replica that a phase
    deliberately stopped or killed (STOPPED / KILLED) is expected to
    have exited; the pool's `detect_crashes` skips it.

    The later destructive phase types (kill-worker, stop-worker) drive
    lifecycle changes through `state["replica_pool"]`, so the pool's
    intended state tracks whether any exit is a fault or expected.
    """
    end = time.time() + seconds
    while time.time() < end:
        remaining = end - time.time()
        time.sleep(min(1.0, max(0.05, remaining)))
        crashed = pool.detect_crashes()
        if crashed:
            slot = crashed[0]
            rc = slot.process.returncode if slot.process else "unknown"
            raise RuntimeError(
                f"{pool.system} replica {slot.instance_id} exited "
                f"during phase (rc={rc}) while state was RUNNING"
            )


# ────────────────────────────────────────────────────────────────────────
# Top-level driver
# ────────────────────────────────────────────────────────────────────────


def _new_run_dir(scenario: str | None) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_id = uuid.uuid4().hex[:6]
    name = f"{scenario or 'custom'}-{ts}-{short_id}"
    run_dir = RESULTS_ROOT / name
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def drive(
    *,
    systems: list[str],
    scenario: str | None,
    phases: list[Phase],
    pg_image: str,
    fast: bool,
    skip_build: bool,
    sample_every_s: int,
    producer_rate: int,
    producer_mode: str,
    target_depth: int,
    worker_count: int,
    high_load_multiplier: float,
    replicas: int,
    cli_args: list[str],
) -> Path:
    unknown = [s for s in systems if s not in ADAPTERS]
    if unknown:
        raise SystemExit(f"Unknown systems: {unknown}. Known: {sorted(ADAPTERS)}")

    run_dir = _new_run_dir(scenario)
    run_id = run_dir.name
    print(f"[harness] run_id = {run_id}", file=sys.stderr)
    raw_csv = run_dir / "raw.csv"
    writer = RawCsvWriter(raw_csv)
    out_queue: "queue.Queue[Sample]" = queue.Queue()
    tracker = PhaseTracker()

    drain_stop = threading.Event()
    drain_thread = threading.Thread(
        target=_drain_loop,
        args=(out_queue, writer, drain_stop),
        name="raw-csv-drain",
        daemon=True,
    )
    drain_thread.start()

    adapter_descriptors: dict[str, dict] = {}
    pg_env_snapshot: dict = {}

    try:
        # Start PG once upfront (needed for the initial build phase to connect;
        # also the --fast path keeps this same instance across systems).
        start_postgres(pg_image)

        if not skip_build:
            for system in systems:
                ADAPTERS[system].builder(False)

        # Capture PG env before the first system's fresh-PG teardown — this
        # way we see the initial config from the committed postgres.conf.
        if systems:
            first_manifest = AdapterManifest.load(ADAPTERS[systems[0]].bench_dir)
            # The database may not exist yet; connect to the admin database.
            pg_env_snapshot = writers_mod.capture_pg_env(pg_url("postgres"))

        def _checkpoint_outputs(completed_systems: list[str]) -> tuple[dict, dict]:
            """Write a manifest.json + summary.json reflecting the systems
            that have completed so far.

            Called after every successful system AND at the end. The
            point: a mid-run abort (pgque submodule missing, OOM, kill
            -9, power loss) leaves a durable partial result on disk
            covering the systems that did finish, instead of throwing
            away hours of completed work because the post-loop block
            never ran. Each call atomically replaces the previous
            checkpoint via `write_manifest` / `write_summary`'s
            tempfile + rename. The final post-loop write is then just
            the last checkpoint, naturally consistent with the loop.
            """
            ckpt_manifest = build_manifest(
                run_id=run_id,
                scenario=scenario,
                phases=phases,
                systems=completed_systems,
                database_url="",
                cli_args=cli_args,
                adapter_versions={
                    s: adapter_descriptors[s] for s in completed_systems
                },
                pg_image=pg_image,
            )
            if pg_env_snapshot:
                ckpt_manifest["postgres"] = pg_env_snapshot
            write_manifest(ckpt_manifest, run_dir / "manifest.json")
            ckpt_summary = compute_summary(
                raw_csv, run_id=run_id, scenario=scenario, phases=phases
            )
            write_summary(ckpt_summary, run_dir / "summary.json")
            return ckpt_manifest, ckpt_summary

        completed: list[str] = []
        for system in systems:
            descriptor = run_one_system(
                system,
                phases=phases,
                pg_image=pg_image,
                fast=fast,
                run_id=run_id,
                out_queue=out_queue,
                tracker=tracker,
                sample_every_s=sample_every_s,
                producer_rate=producer_rate,
                producer_mode=producer_mode,
                target_depth=target_depth,
                worker_count=worker_count,
                high_load_multiplier=high_load_multiplier,
                replicas=replicas,
            )
            # Merge the runtime descriptor the adapter emitted with the
            # harness-proven revision block (git SHA / submodule SHA /
            # pinned upstream version). The runtime half records what the
            # process claimed about itself; the harness half records what
            # we can prove by inspecting the source tree and pinned
            # manifests. Both ship in manifest.json so a reader can tell
            # *exactly* which code was under test without cross-referencing
            # anything outside the run directory.
            entry = dict(descriptor or {})
            entry["revision"] = capture_adapter_revision(system)
            adapter_descriptors[system] = entry
            completed.append(system)
            # Drain the in-flight sample queue before snapshotting so
            # `compute_summary` sees every row this system produced.
            # `_drain_loop` doesn't call task_done() so we can't use
            # out_queue.join(); poll for empty + flush the writer
            # explicitly. Bounded wall-clock so a stuck producer can't
            # hold up the next system's startup.
            drain_deadline = time.time() + 5.0
            while time.time() < drain_deadline and not out_queue.empty():
                time.sleep(0.05)
            writer.flush()
            _checkpoint_outputs(completed)
    finally:
        drain_stop.set()
        drain_thread.join(timeout=10)
        writer.close()
        stop_postgres(pg_image)

    # Final post-processing outputs. Recompute against the full
    # `completed` list to pick up any drain-loop samples that landed
    # after the last per-system checkpoint, and so the post-loop
    # `manifest` / `summary` locals are populated for the plot /
    # report rendering below.
    manifest, summary = _checkpoint_outputs(completed)
    write_run_readme(
        run_dir / "README.md",
        scenario=scenario,
        phases=phases,
        adapters=adapter_descriptors,
    )
    # Build system_meta so plots can group variants of the same family
    # (e.g. awa, awa-docker, awa-python all share the "awa" family colour).
    system_meta: dict[str, tuple[str, str]] = {}
    for system in systems:
        try:
            m = AdapterManifest.load(ADAPTERS[system].bench_dir)
            system_meta[system] = (m.family, m.display_name)
        except Exception:
            system_meta[system] = (system, system)
    render_all(
        raw_csv,
        systems=systems,
        phases=phases,
        out_dir=run_dir / "plots",
        system_meta=system_meta,
    )
    write_interactive_report(
        run_dir=run_dir,
        raw_csv=raw_csv,
        summary=summary,
        manifest=manifest,
        phases=phases,
        systems=systems,
    )

    print(f"\n[harness] results at: {run_dir}", file=sys.stderr)
    return run_dir


# ────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Run-mode arguments. Shared by `run` and any future runner subcommands.

    See `build_parser()` for the unified CLI surface (#174). This factor-out
    keeps the option list in one place so it doesn't drift between callers.
    """
    parser.add_argument(
        "--scenario",
        default=None,
        help="Named scenario, e.g. idle_in_tx_saturation, long_horizon. "
        "Combine with --phase to append extra phases.",
    )
    parser.add_argument(
        "--phase",
        action="append",
        default=[],
        help="Phase spec: label=type:duration (e.g. idle_1=idle-in-tx:60m). "
        "Repeatable. Can be used with or without --scenario.",
    )
    parser.add_argument(
        "--systems",
        default=",".join(DEFAULT_SYSTEMS),
        help="Comma-separated adapters to run. awa-docker is opt-in only "
        "because it duplicates the awa-native line in cross-system plots.",
    )
    parser.add_argument(
        "--pg-image",
        default=DEFAULT_PG_IMAGE,
        help="Pinned Postgres image. Do not track a moving major tag.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Developer fast path: keep one PG instance across systems "
        "(DB-only recreation). Non-canonical — use for iteration only.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip adapter build step; use cached binaries/images.",
    )
    parser.add_argument(
        "--sample-every",
        type=int,
        default=5,
        help="Sample cadence in seconds (default 5).",
    )
    parser.add_argument(
        "--producer-rate",
        type=int,
        default=800,
        help="Fixed-rate producer target jobs/s (default 800).",
    )
    parser.add_argument(
        "--producer-mode",
        choices=["fixed", "depth-target"],
        default="fixed",
        help="Producer mode: fixed-rate offered load or depth-target diagnostic mode.",
    )
    parser.add_argument(
        "--target-depth",
        type=int,
        default=1000,
        help="Target queue depth when --producer-mode=depth-target (default 1000).",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        default=32,
        help="Consumer concurrency (default 32).",
    )
    parser.add_argument(
        "--high-load-multiplier",
        type=float,
        default=1.5,
        help="Producer-rate multiplier applied during high-load phases (default 1.5).",
    )
    parser.add_argument(
        "--replicas",
        type=int,
        default=1,
        help="Replica count per system (default 1). Each replica runs the "
        "configured producer rate and worker count independently, so "
        "N replicas at --producer-rate=800 offer 800*N jobs/s in "
        "aggregate. Divide --producer-rate by --replicas to hold total "
        "offered load constant across replica counts.",
    )


def build_parser() -> argparse.ArgumentParser:
    """Top-level CLI for the portable benchmarking harness.

    Subcommand-shaped (#174) so the harness has one entry point even as
    new actions land. Today: `run` (drive a benchmark), `combine`
    (merge already-completed single-system runs into one consolidated
    report), and `compare` (render a markdown side-by-side from a
    combined run's summary.json). Once `chaos.py` folds in
    (UNIFIED_DRIVER_DESIGN.md), its scenarios become named phase
    sequences invoked through `run`; no new subcommand is expected to
    land at the CLI level.
    """
    from . import combine, compare

    parser = argparse.ArgumentParser(
        prog="long_horizon.py",
        description="Portable benchmarking harness: drive runs, combine reports, render comparisons.",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="{run,combine,compare}",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run a benchmark scenario or custom phase sequence.",
        description=(
            "Drive one or more adapters through a scenario or custom phase "
            "sequence. Pass every system you want to overlay in `index.html` "
            "as a comma-separated list to `--systems` *in this single "
            "invocation*; the cross-system overlays are produced from the "
            "merged samples in one `raw.csv`. To consolidate runs that were "
            "started separately, use the `combine` subcommand instead."
        ),
    )
    _add_run_arguments(run_parser)
    run_parser.set_defaults(func=_cmd_run)

    combine.add_subparser(subparsers)
    compare.add_subparser(subparsers)

    return parser


def _cmd_run(args: argparse.Namespace) -> int:
    from pydantic import ValidationError

    from .config import CliConfig, format_validation_error

    try:
        config = CliConfig.from_namespace(args)
        phases = config.resolve_phases()
    except ValidationError as exc:
        # All validation flows through one pydantic model so rules stay
        # in one place and bad combinations produce structured, readable
        # CLI errors. See bench_harness/config.py.
        raise SystemExit(format_validation_error(exc))
    except ValueError as exc:
        # resolve_scenario (and anything else that raises ValueError
        # downstream) is already CLI-friendly; surface it consistently.
        raise SystemExit(str(exc))

    drive(
        systems=config.systems,
        scenario=config.scenario,
        phases=phases,
        pg_image=config.pg_image,
        fast=config.fast,
        skip_build=config.skip_build,
        sample_every_s=config.sample_every,
        producer_rate=config.producer_rate,
        producer_mode=config.producer_mode,
        target_depth=config.target_depth,
        worker_count=config.worker_count,
        high_load_multiplier=config.high_load_multiplier,
        replicas=config.replicas,
        cli_args=list(sys.argv),
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

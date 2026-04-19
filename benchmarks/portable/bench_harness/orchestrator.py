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


@dataclass
class RunningAdapter:
    process: subprocess.Popen
    tailer: threading.Thread
    descriptor: dict


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


def launch_adapter(
    system: str,
    entry: AdapterEntry,
    manifest: AdapterManifest,
    overrides: dict[str, str],
    *,
    run_id: str,
    bench_start: float,
    tracker: PhaseTracker,
    out_queue: "queue.Queue[Sample]",
) -> RunningAdapter:
    spec = entry.launcher(manifest, overrides)
    env = {**os.environ, **spec.env}
    print(
        f"[harness] launching {system}: {' '.join(spec.argv[:2])}...", file=sys.stderr
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
        name=f"tail-{system}",
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
                f"{system} exited during startup (rc={proc.returncode}) "
                f"before emitting a descriptor record."
            )
        time.sleep(0.1)
    descriptor = descriptor_holder.get("descriptor")
    if not descriptor:
        raise _abort(
            f"{system} did not emit a startup descriptor within 60s. "
            "The harness requires a descriptor record to cross-check the "
            "adapter against its static manifest; running blind would let "
            "drift slip through unnoticed."
        )
    try:
        _check_descriptor_drift(manifest, descriptor)
    except RuntimeError as exc:
        raise _abort(str(exc)) from exc
    return RunningAdapter(process=proc, tailer=tailer, descriptor=descriptor)


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


def terminate_adapter(
    running: RunningAdapter, system: str, timeout_s: float = 10.0
) -> None:
    proc = running.process
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            print(
                f"[{system}] did not exit within {timeout_s}s, killing", file=sys.stderr
            )
            proc.kill()
            proc.wait()
    running.tailer.join(timeout=2.0)


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
) -> dict:
    entry = ADAPTERS[system]
    manifest = AdapterManifest.load(entry.bench_dir)
    print(f"\n=== [{system}] === ({manifest.db_name})", file=sys.stderr)

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
    adapter = launch_adapter(
        system,
        entry,
        manifest,
        overrides,
        run_id=run_id,
        bench_start=bench_start,
        tracker=tracker,
        out_queue=out_queue,
    )

    # Register the metrics daemon now so it covers all phases including warmup.
    daemon = MetricsDaemon(
        run_id=run_id,
        system=system,
        database_url=pg_url(manifest.db_name),
        targets=PollTargets(
            event_tables=manifest.event_tables,
            event_indexes=manifest.event_indexes,
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
        "event_tables": list(manifest.event_tables),
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
                _sleep_or_abort(phase.duration_s, adapter)
            finally:
                registry.exit(runtime)
                # Phase-boundary snapshot: pgstattuple / pgstatindex.
                daemon.phase_boundary_snapshot()
    finally:
        daemon.stop()
        daemon.join(timeout=5.0)
        terminate_adapter(adapter, system)
        shutil.rmtree(control_dir, ignore_errors=True)

    return adapter.descriptor


def _sleep_or_abort(seconds: float, adapter: RunningAdapter) -> None:
    """Sleep in small increments so adapter crash is noticed quickly."""
    end = time.time() + seconds
    while time.time() < end:
        remaining = end - time.time()
        time.sleep(min(1.0, max(0.05, remaining)))
        if adapter.process.poll() is not None:
            raise RuntimeError(
                f"adapter exited during phase (rc={adapter.process.returncode})"
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
    finally:
        drain_stop.set()
        drain_thread.join(timeout=10)
        writer.close()
        stop_postgres(pg_image)

    # Post-processing outputs.
    manifest = build_manifest(
        run_id=run_id,
        scenario=scenario,
        phases=phases,
        systems=systems,
        database_url="",  # captured pre-teardown into pg_env_snapshot
        cli_args=cli_args,
        adapter_versions=adapter_descriptors,
        pg_image=pg_image,
    )
    if pg_env_snapshot:
        manifest["postgres"] = pg_env_snapshot
    write_manifest(manifest, run_dir / "manifest.json")
    summary = compute_summary(raw_csv, run_id=run_id, scenario=scenario, phases=phases)
    write_summary(summary, run_dir / "summary.json")
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

    print(f"\n[harness] results at: {run_dir}", file=sys.stderr)
    return run_dir


# ────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Long-horizon benchmark runner for the portable queue suite",
    )
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
        default=10,
        help="Sample cadence in seconds (default 10).",
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
    return parser


def main(argv: list[str] | None = None) -> int:
    from pydantic import ValidationError

    from .config import CliConfig, format_validation_error

    parser = build_parser()
    args = parser.parse_args(argv)
    # All validation flows through one pydantic model so rules stay in
    # one place and bad combinations produce structured, readable CLI
    # errors. See bench_harness/config.py.
    try:
        config = CliConfig.from_namespace(args)
        phases = config.resolve_phases()
    except ValidationError as exc:
        parser.error(format_validation_error(exc))
    except ValueError as exc:
        # resolve_scenario (and anything else that raises ValueError
        # downstream) is already CLI-friendly; route it through the same
        # parser.error so the user sees consistent framing.
        parser.error(str(exc))

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
        cli_args=list(sys.argv) if argv is None else list(argv),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

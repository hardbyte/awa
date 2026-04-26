"""Adapter registry: how to build and launch each system as a long-horizon
process.

Each adapter has:
  - a static adapter.json manifest at benchmarks/portable/<name>-bench/
  - a Builder that produces the executable (cargo build, docker build, …)
  - a LaunchSpec: argv + env to start the long-horizon process

Adapters that aren't awa-native run inside Docker with --network host so they
can reach the shared Postgres on localhost:15555.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

SCRIPT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SCRIPT_DIR.parent.parent

PG_PORT = 15555
PG_USER = "bench"
PG_PASS = "bench"
DEFAULT_PG_IMAGE = "postgres:17.2-alpine"


def pg_url(dbname: str, host: str = "localhost") -> str:
    return f"postgres://{PG_USER}:{PG_PASS}@{host}:{PG_PORT}/{dbname}"


@dataclass
class AdapterManifest:
    system: str
    db_name: str
    event_tables: list[str]
    event_indexes: list[str]
    extensions: list[str]
    family: str = ""
    display_name: str = ""

    def __post_init__(self) -> None:
        # Sensible defaults: standalone systems are their own family and
        # render under their bare name. Variants opt-in by setting `family`.
        if not self.family:
            self.family = self.system
        if not self.display_name:
            self.display_name = self.system

    @classmethod
    def load(cls, bench_dir: Path) -> "AdapterManifest":
        path = bench_dir / "adapter.json"
        with path.open() as fh:
            data = json.load(fh)
        return cls(
            system=data["system"],
            db_name=data["db_name"],
            event_tables=list(data.get("event_tables", [])),
            event_indexes=list(data.get("event_indexes", [])),
            extensions=list(data.get("extensions", [])),
            family=data.get("family", "") or "",
            display_name=data.get("display_name", "") or "",
        )


@dataclass
class LaunchSpec:
    argv: list[str]
    env: dict[str, str]
    cwd: str | None = None
    mounts: list[tuple[str, str]] = field(default_factory=list)


Builder = Callable[[bool], None]
Launcher = Callable[[AdapterManifest, dict[str, str]], LaunchSpec]


def _cargo_target_dir(manifest_path: Path) -> Path:
    proc = subprocess.run(
        [
            "cargo",
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            str(manifest_path),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout)
    return Path(data["target_directory"])


# ─── Build functions ─────────────────────────────────────────────────────


def build_awa(skip: bool) -> None:
    if skip:
        return
    print("[harness] building awa-bench (native)...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "--release"],
        cwd=str(SCRIPT_DIR / "awa-bench"),
        env={**os.environ, "SQLX_OFFLINE": "true"},
        check=True,
    )


def _docker_build(image_tag: str, dockerfile: Path, context: Path, skip: bool) -> None:
    if skip:
        return
    print(f"[harness] docker build {image_tag}...", file=sys.stderr)
    subprocess.run(
        ["docker", "build", "-f", str(dockerfile), "-t", image_tag, "."],
        cwd=str(context),
        check=True,
    )


def build_awa_docker(skip: bool) -> None:
    _docker_build(
        "awa-bench-docker",
        SCRIPT_DIR / "awa-bench" / "Dockerfile",
        REPO_ROOT,
        skip,
    )


def build_awa_python(skip: bool) -> None:
    _docker_build(
        "awa-python-bench",
        SCRIPT_DIR / "awa-python-bench" / "Dockerfile",
        REPO_ROOT,
        skip,
    )


def build_procrastinate(skip: bool) -> None:
    _docker_build(
        "procrastinate-bench",
        SCRIPT_DIR / "procrastinate-bench" / "Dockerfile",
        REPO_ROOT,
        skip,
    )


def build_river(skip: bool) -> None:
    _docker_build(
        "river-bench",
        SCRIPT_DIR / "river-bench" / "Dockerfile",
        SCRIPT_DIR / "river-bench",
        skip,
    )


def build_oban(skip: bool) -> None:
    _docker_build(
        "oban-bench",
        SCRIPT_DIR / "oban-bench" / "Dockerfile",
        SCRIPT_DIR / "oban-bench",
        skip,
    )


def build_pgque(skip: bool) -> None:
    # Build context is REPO_ROOT so the Dockerfile can COPY the vendored
    # pgque submodule alongside the adapter source. Reminds developers to
    # `git submodule update --init` (the README documents this).
    _docker_build(
        "pgque-bench",
        SCRIPT_DIR / "pgque-bench" / "Dockerfile",
        REPO_ROOT,
        skip,
    )


def build_pgboss(skip: bool) -> None:
    _docker_build(
        "pgboss-bench",
        SCRIPT_DIR / "pgboss-bench" / "Dockerfile",
        REPO_ROOT,
        skip,
    )


def build_pgmq(skip: bool) -> None:
    _docker_build(
        "pgmq-bench",
        SCRIPT_DIR / "pgmq-bench" / "Dockerfile",
        REPO_ROOT,
        skip,
    )


# ─── Launch specs ────────────────────────────────────────────────────────


def _base_env(manifest: AdapterManifest, overrides: dict[str, str]) -> dict[str, str]:
    env = {
        "DATABASE_URL": pg_url(manifest.db_name),
        "SCENARIO": "long_horizon",
        "PRODUCER_MODE": "fixed",
        "PRODUCER_RATE": "800",
        "TARGET_DEPTH": "1000",
        "WORKER_COUNT": "32",
        "JOB_PAYLOAD_BYTES": "256",
        "JOB_WORK_MS": "1",
        "SAMPLE_EVERY_S": "5",
        # 0-indexed replica id. Defaults to 0 for single-replica runs
        # (the only mode today); the multi-replica spawner (follow-up
        # commit for #174) supplies distinct ids per subprocess.
        "BENCH_INSTANCE_ID": "0",
        # Cap glibc malloc arenas. Default is 8 × num_cores (= ~192 on
        # the 24-core dev box), and tokio's per-thread allocation
        # patterns spawn arenas that don't shrink, manifesting as
        # ~2.4 GB/h/replica anonymous-mapping growth in long runs.
        # Two arenas keeps fragmentation bounded with negligible
        # allocator-contention impact at our thread counts.
        "MALLOC_ARENA_MAX": "2",
    }
    env.update(
        {
            key: value
            for key, value in overrides.items()
            if not key.endswith("_HOST") and not key.endswith("_CONTAINER")
        }
    )
    return env


def launch_awa(manifest: AdapterManifest, overrides: dict[str, str]) -> LaunchSpec:
    target_dir = _cargo_target_dir(SCRIPT_DIR / "awa-bench" / "Cargo.toml")
    return LaunchSpec(
        argv=[str(target_dir / "release" / "awa-bench")],
        env=_base_env(manifest, overrides),
    )


def launch_awa_canonical(manifest: AdapterManifest, overrides: dict[str, str]) -> LaunchSpec:
    merged = {
        **overrides,
        "AWA_STORAGE_ENGINE": "canonical",
        "AWA_BENCH_SYSTEM": "awa-canonical",
    }
    return launch_awa(manifest, merged)


def _docker_launch(
    image: str,
    manifest: AdapterManifest,
    overrides: dict[str, str],
) -> LaunchSpec:
    env = _base_env(manifest, overrides)
    mounts: list[tuple[str, str]] = []
    host_control = overrides.get("PRODUCER_RATE_CONTROL_FILE_HOST")
    container_control = overrides.get("PRODUCER_RATE_CONTROL_FILE_CONTAINER")
    if host_control and container_control:
        host_dir = str(Path(host_control).parent)
        container_dir = str(Path(container_control).parent)
        mounts.append((host_dir, container_dir))
        env["PRODUCER_RATE_CONTROL_FILE"] = container_control
    # Docker containers reach PG via host networking.
    argv = ["docker", "run", "--rm", "--network", "host"]
    for host_path, container_path in mounts:
        argv.extend(["-v", f"{host_path}:{container_path}"])
    for k, v in env.items():
        argv.extend(["-e", f"{k}={v}"])
    argv.append(image)
    return LaunchSpec(argv=argv, env={}, mounts=mounts)


def launch_awa_docker(manifest, overrides):
    return _docker_launch("awa-bench-docker", manifest, overrides)


def launch_awa_python(manifest, overrides):
    return _docker_launch("awa-python-bench", manifest, overrides)


def launch_procrastinate(manifest, overrides):
    return _docker_launch("procrastinate-bench", manifest, overrides)


def launch_river(manifest, overrides):
    return _docker_launch("river-bench", manifest, overrides)


def launch_oban(manifest, overrides):
    return _docker_launch("oban-bench", manifest, overrides)


def launch_pgque(manifest, overrides):
    return _docker_launch("pgque-bench", manifest, overrides)


def launch_pgboss(manifest, overrides):
    return _docker_launch("pgboss-bench", manifest, overrides)


def launch_pgmq(manifest, overrides):
    return _docker_launch("pgmq-bench", manifest, overrides)


# ─── Registry ────────────────────────────────────────────────────────────


@dataclass
class AdapterEntry:
    bench_dir: Path
    builder: Builder
    launcher: Launcher


ADAPTERS: dict[str, AdapterEntry] = {
    "awa": AdapterEntry(
        bench_dir=SCRIPT_DIR / "awa-bench",
        builder=build_awa,
        launcher=launch_awa,
    ),
    "awa-canonical": AdapterEntry(
        bench_dir=SCRIPT_DIR / "awa-canonical-bench",
        builder=build_awa,
        launcher=launch_awa_canonical,
    ),
    "awa-docker": AdapterEntry(
        bench_dir=SCRIPT_DIR / "awa-docker-bench",
        builder=build_awa_docker,
        launcher=launch_awa_docker,
    ),
    "awa-python": AdapterEntry(
        bench_dir=SCRIPT_DIR / "awa-python-bench",
        builder=build_awa_python,
        launcher=launch_awa_python,
    ),
    "procrastinate": AdapterEntry(
        bench_dir=SCRIPT_DIR / "procrastinate-bench",
        builder=build_procrastinate,
        launcher=launch_procrastinate,
    ),
    "river": AdapterEntry(
        bench_dir=SCRIPT_DIR / "river-bench",
        builder=build_river,
        launcher=launch_river,
    ),
    "oban": AdapterEntry(
        bench_dir=SCRIPT_DIR / "oban-bench",
        builder=build_oban,
        launcher=launch_oban,
    ),
    "pgque": AdapterEntry(
        bench_dir=SCRIPT_DIR / "pgque-bench",
        builder=build_pgque,
        launcher=launch_pgque,
    ),
    "pgmq": AdapterEntry(
        bench_dir=SCRIPT_DIR / "pgmq-bench",
        builder=build_pgmq,
        launcher=launch_pgmq,
    ),
    "pgboss": AdapterEntry(
        bench_dir=SCRIPT_DIR / "pgboss-bench",
        builder=build_pgboss,
        launcher=launch_pgboss,
    ),
}

# Default --systems list for long-horizon. awa-docker is opt-in (it duplicates
# the awa-native line in cross-system plots — same Rust, same SQL).
DEFAULT_SYSTEMS = ["awa", "awa-python", "procrastinate", "river", "oban", "pgque", "pgboss"]

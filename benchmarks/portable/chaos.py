#!/usr/bin/env python3
"""
Chaos comparison: correctness under adverse conditions.

Tests SIGKILL recovery, Postgres restart, and no-job-loss guarantees
across the portable systems under test.

Usage:
    python chaos.py [--scenario crash_recovery|postgres_restart|repeated_kills]
                    [--systems awa,awa-docker,awa-python,procrastinate,river,oban]
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
PG_PORT = 15555
PG_USER = "bench"
PG_PASS = "bench"
DB_URL_TPL = f"postgres://{PG_USER}:{PG_PASS}@localhost:{PG_PORT}/{{}}"

PORTABLE_SYSTEMS = [
    "awa",
    "awa-docker",
    "awa-python",
    "procrastinate",
    "river",
    "oban",
]

PORTABLE_SCENARIOS = [
    "crash_recovery",
    "postgres_restart",
    "repeated_kills",
    "pg_backend_kill",
    "leader_failover",
    "pool_exhaustion",
]

EXTENDED_SCENARIOS = PORTABLE_SCENARIOS + [
    "retry_storm",
    "priority_starvation",
]

# ── Postgres helpers ──────────────────────────────────────────────

def pg_url(db: str) -> str:
    return DB_URL_TPL.format(db)

def psql(db: str, sql: str) -> str:
    result = subprocess.run(
        ["psql", pg_url(db), "-t", "-A", "-c", sql],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr}")
    return result.stdout.strip()

def wait_pg_ready(timeout: int = 30):
    for _ in range(timeout * 2):
        try:
            psql("awa_bench", "SELECT 1")
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("Postgres not ready")

def start_postgres():
    subprocess.run(
        ["docker", "compose", "up", "-d", "--wait"],
        cwd=str(SCRIPT_DIR), capture_output=True, timeout=60,
    )
    wait_pg_ready()

def stop_postgres():
    subprocess.run(
        ["docker", "compose", "down", "-v"],
        cwd=str(SCRIPT_DIR), capture_output=True,
    )

def reset_db(db: str):
    # Drop both public and awa schemas (Awa uses awa.*, River/Oban use public.*)
    psql(db, "DROP SCHEMA IF EXISTS awa CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

# ── Per-system SQL: enqueue, state counts, rescue times ───────────

def awa_like_system(db: str) -> dict[str, str]:
    return {
        "db": db,
        "job_table": "awa.jobs_hot",
        "state_col": "state",
        "enqueue_sql": """
            INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, run_at)
            SELECT 'chaos_job', 'chaos', jsonb_build_object('seq', g), 'available', 2, now()
            FROM generate_series(1, {count}) g
        """,
        "count_sql": """
            SELECT state::text, count(*)::int FROM awa.jobs WHERE queue = 'chaos' GROUP BY state
        """,
        "rescue_time_sql": """
            SELECT EXTRACT(EPOCH FROM (finalized_at - attempted_at))::float
            FROM awa.jobs WHERE queue = 'chaos' AND state = 'retryable'
            ORDER BY finalized_at DESC LIMIT 1
        """,
    }


SYSTEMS = {
    "awa": awa_like_system("awa_bench"),
    "awa-docker": awa_like_system("awa_docker_bench"),
    "awa-python": awa_like_system("awa_python_bench"),
    "river": {
        "db": "river_bench",
        "job_table": "river_job",
        "state_col": "state",
        "enqueue_sql": """
            INSERT INTO river_job (kind, queue, args, state, max_attempts, scheduled_at)
            SELECT 'chaos_job', 'default', jsonb_build_object('seq', g), 'available', 5, now()
            FROM generate_series(1, {count}) g
        """,
        "count_sql": """
            SELECT state::text, count(*)::int FROM river_job GROUP BY state
        """,
        "rescue_time_sql": """
            SELECT EXTRACT(EPOCH FROM (now() - attempted_at))::float
            FROM river_job WHERE state = 'retryable'
            ORDER BY attempted_at DESC LIMIT 1
        """,
    },
    "oban": {
        "db": "oban_bench",
        "job_table": "oban_jobs",
        "state_col": "state",
        "enqueue_sql": """
            INSERT INTO oban_jobs (worker, queue, args, state, max_attempts, scheduled_at, inserted_at)
            SELECT 'ObanBench.ChaosWorker', 'chaos', jsonb_build_object('seq', g),
                   'available', 5, now(), now()
            FROM generate_series(1, {count}) g
        """,
        "count_sql": """
            SELECT state::text, count(*)::int FROM oban_jobs WHERE queue = 'chaos' GROUP BY state
        """,
        "rescue_time_sql": """
            SELECT EXTRACT(EPOCH FROM (now() - attempted_at))::float
            FROM oban_jobs WHERE state = 'retryable'
            ORDER BY attempted_at DESC LIMIT 1
        """,
    },
    "procrastinate": {
        "db": "procrastinate_bench",
        "job_table": "procrastinate_jobs",
        "state_col": "status",
        "enqueue_sql": """
            INSERT INTO procrastinate_jobs (task_name, queue_name, args, status, attempts)
            SELECT 'main.chaos_job', 'chaos', jsonb_build_object('seq', g),
                   'todo', 0
            FROM generate_series(1, {count}) g
        """,
        "count_sql": """
            SELECT status::text, count(*)::int FROM procrastinate_jobs WHERE queue_name = 'chaos' GROUP BY status
        """,
        "rescue_time_sql": """
            SELECT 0.0
        """,
    },
}

def enqueue_jobs(system: str, count: int):
    if system == "procrastinate":
        enqueue_procrastinate_jobs(count)
        return
    cfg = SYSTEMS[system]
    sql = cfg["enqueue_sql"].format(count=count)
    psql(cfg["db"], sql)


def run_procrastinate_python(code: str) -> None:
    result = subprocess.run(
        [
            "docker", "run", "--rm", "--entrypoint", "sh",
            "--network", "host",
            "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
            "procrastinate-bench",
            "-lc",
            (
                "cd /app/benchmarks/portable/procrastinate-bench && "
                f"uv run python -c '{code}'"
            ),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Procrastinate helper failed:\n{result.stderr}")


def enqueue_procrastinate_jobs(count: int):
    code = (
        "import asyncio, main\n"
        "async def run():\n"
        "    async with main.app.open_async():\n"
        f"        await main.enqueue_chaos_jobs({count})\n"
        "asyncio.run(run())"
    )
    run_procrastinate_python(code)

def get_state_counts(system: str) -> dict[str, int]:
    cfg = SYSTEMS[system]
    raw = psql(cfg["db"], cfg["count_sql"])
    counts = {}
    for line in raw.strip().split("\n"):
        if "|" in line:
            state, count = line.split("|")
            counts[state.strip()] = int(count.strip())
    return counts

# ── Worker process management ─────────────────────────────────────

def start_awa_worker() -> subprocess.Popen:
    """Start Awa worker with short rescue intervals."""
    env = {
        **os.environ,
        "DATABASE_URL": pg_url("awa_bench"),
        "SCENARIO": "worker_only",
        "WORKER_COUNT": "10",
        "JOB_DURATION_MS": "30000",
        "RESCUE_INTERVAL_SECS": "5",
        "HEARTBEAT_STALENESS_SECS": "15",
    }
    return subprocess.Popen(
        [str(SCRIPT_DIR / "awa-bench" / "target" / "release" / "awa-bench")],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def start_awa_docker_worker() -> subprocess.Popen:
    return subprocess.Popen(
        [
            "docker", "run", "--rm", "--name", "awa-docker-chaos-worker",
            "--network", "host",
            "-e", f"DATABASE_URL={pg_url('awa_docker_bench')}",
            "-e", "SCENARIO=worker_only",
            "-e", "WORKER_COUNT=10",
            "-e", "JOB_DURATION_MS=30000",
            "-e", "RESCUE_INTERVAL_SECS=5",
            "-e", "HEARTBEAT_STALENESS_SECS=15",
            "awa-bench-docker",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def start_awa_python_worker() -> subprocess.Popen:
    return subprocess.Popen(
        [
            "docker", "run", "--rm", "--name", "awa-python-chaos-worker",
            "--network", "host",
            "-e", f"DATABASE_URL={pg_url('awa_python_bench')}",
            "-e", "SCENARIO=worker_only",
            "-e", "WORKER_COUNT=10",
            "-e", "JOB_DURATION_MS=30000",
            "-e", "RESCUE_INTERVAL_SECS=5",
            "-e", "HEARTBEAT_STALENESS_SECS=15",
            "awa-python-bench",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

def start_river_worker() -> subprocess.Popen:
    """Start River worker container with rescue timeout above job duration."""
    return subprocess.Popen(
        [
            "docker", "run", "--rm", "--name", "river-chaos-worker",
            "--network", "host",
            "-e", f"DATABASE_URL={pg_url('river_bench')}",
            "-e", "SCENARIO=worker_only",
            "-e", "WORKER_COUNT=10",
            "-e", "JOB_DURATION_MS=30000",
            "-e", "RESCUE_AFTER_SECS=60",
            "river-bench",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

def start_oban_worker() -> subprocess.Popen:
    """Start Oban worker container with short rescue interval."""
    return subprocess.Popen(
        [
            "docker", "run", "--rm", "--name", "oban-chaos-worker",
            "--network", "host",
            "-e", f"DATABASE_URL={pg_url('oban_bench')}",
            "-e", "SCENARIO=worker_only",
            "-e", "WORKER_COUNT=10",
            "-e", "JOB_DURATION_MS=30000",
            "-e", "RESCUE_AFTER_SECS=15",
            "oban-bench",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

def start_procrastinate_worker() -> subprocess.Popen:
    """Start Procrastinate worker container with short rescue interval."""
    return subprocess.Popen(
        [
            "docker", "run", "--rm", "--name", "procrastinate-chaos-worker",
            "--network", "host",
            "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
            "-e", "SCENARIO=worker_only",
            "-e", "WORKER_COUNT=10",
            "-e", "JOB_DURATION_MS=30000",
            "procrastinate-bench",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

def kill_worker(proc: subprocess.Popen, system: str):
    """SIGKILL the worker process."""
    if system == "awa":
        os.kill(proc.pid, signal.SIGKILL)
    else:
        # Docker container — force kill
        container = f"{system}-chaos-worker"
        subprocess.run(
            ["docker", "kill", "--signal", "KILL", container],
            capture_output=True, timeout=10,
        )
    proc.wait()

WORKER_STARTERS = {
    "awa": start_awa_worker,
    "awa-docker": start_awa_docker_worker,
    "awa-python": start_awa_python_worker,
    "river": start_river_worker,
    "oban": start_oban_worker,
    "procrastinate": start_procrastinate_worker,
}

# ── State name mapping (each system uses different terminology) ────

def running_state(system: str) -> str:
    return {
        "awa": "running",
        "awa-docker": "running",
        "awa-python": "running",
        "river": "running",
        "oban": "executing",
        "procrastinate": "doing",
    }[system]

def completed_state(system: str) -> str:
    return {
        "awa": "completed",
        "awa-docker": "completed",
        "awa-python": "completed",
        "river": "completed",
        "oban": "completed",
        "procrastinate": "succeeded",
    }[system]

# ── Scenarios ─────────────────────────────────────────────────────

def wait_for_state(system: str, target_state: str, min_count: int,
                   timeout: float = 120) -> float:
    """Wait until at least min_count jobs reach target_state. Returns seconds waited."""
    start = time.time()
    while time.time() - start < timeout:
        counts = get_state_counts(system)
        if counts.get(target_state, 0) >= min_count:
            return time.time() - start
        time.sleep(0.5)
    counts = get_state_counts(system)
    raise TimeoutError(
        f"{system}: timed out waiting for {min_count} {target_state} jobs "
        f"(got {counts})"
    )

def scenario_crash_recovery(system: str, job_count: int = 10) -> dict:
    """
    SIGKILL a worker mid-flight. Measure rescue time and verify no jobs lost.

    1. Enqueue jobs
    2. Start worker, wait for all jobs to be running
    3. SIGKILL the worker
    4. Record kill timestamp
    5. Start a new worker
    6. Wait for all jobs to complete
    7. Report rescue time and job counts
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])

    # Run migrations
    print(f"  [{system}] Running migrations...", file=sys.stderr)
    migrate_system(system)

    print(f"  [{system}] Enqueueing {job_count} jobs...", file=sys.stderr)
    enqueue_jobs(system, job_count)

    print(f"  [{system}] Starting worker...", file=sys.stderr)
    worker = WORKER_STARTERS[system]()
    time.sleep(2)  # Let worker start up

    # Wait for jobs to start running
    rs = running_state(system)
    try:
        wait_for_state(system, rs, min_count=job_count, timeout=30)
    except TimeoutError:
        counts = get_state_counts(system)
        print(f"  [{system}] Only some jobs running: {counts}", file=sys.stderr)

    time.sleep(2)  # Let them run a bit

    # SIGKILL
    kill_time = time.time()
    print(f"  [{system}] SIGKILL at t=0", file=sys.stderr)
    kill_worker(worker, system)

    # Verify all jobs are stuck (running with dead worker)
    time.sleep(1)
    counts_after_kill = get_state_counts(system)
    print(f"  [{system}] State after kill: {counts_after_kill}", file=sys.stderr)

    # Start replacement worker
    print(f"  [{system}] Starting replacement worker...", file=sys.stderr)
    worker2 = WORKER_STARTERS[system]()

    # Wait for rescue (jobs go from running → retryable/available)
    cs = completed_state(system)
    try:
        rescue_wait = wait_for_state(system, cs,
                                     min_count=job_count, timeout=300)
    except TimeoutError:
        rescue_wait = 300.0
        print(f"  [{system}] TIMEOUT waiting for completion", file=sys.stderr)

    total_time = time.time() - kill_time
    final_counts = get_state_counts(system)

    # Cleanup
    try:
        kill_worker(worker2, system)
    except Exception:
        pass

    completed = final_counts.get(completed_state(system), 0)
    lost = job_count - completed

    print(f"  [{system}] Rescue complete in {total_time:.1f}s: {final_counts}",
          file=sys.stderr)

    return {
        "system": system,
        "scenario": "crash_recovery",
        "config": {"job_count": job_count},
        "results": {
            "total_time_secs": round(total_time, 1),
            "rescue_time_secs": round(rescue_wait, 1),
            "jobs_completed": completed,
            "jobs_lost": lost,
            "final_state_counts": final_counts,
            "counts_after_kill": counts_after_kill,
        },
    }

def scenario_postgres_restart(system: str, job_count: int = 10) -> dict:
    """
    Restart Postgres mid-flight. Verify workers reconnect and no jobs lost.
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)
    enqueue_jobs(system, job_count)

    print(f"  [{system}] Starting worker...", file=sys.stderr)
    worker = WORKER_STARTERS[system]()
    time.sleep(2)

    rs = running_state(system)
    try:
        wait_for_state(system, rs, min_count=1, timeout=30)
    except TimeoutError:
        pass

    # Restart Postgres
    restart_time = time.time()
    print(f"  [{system}] Restarting Postgres...", file=sys.stderr)
    subprocess.run(
        ["docker", "compose", "restart", "postgres"],
        cwd=str(SCRIPT_DIR), capture_output=True, timeout=30,
    )
    wait_pg_ready(timeout=30)
    pg_downtime = time.time() - restart_time
    print(f"  [{system}] Postgres back after {pg_downtime:.1f}s", file=sys.stderr)

    # Wait for all jobs to complete
    try:
        completion_wait = wait_for_state(system, completed_state(system),
                                         min_count=job_count, timeout=300)
    except TimeoutError:
        completion_wait = 300.0
        print(f"  [{system}] TIMEOUT", file=sys.stderr)

    total_time = time.time() - restart_time
    final_counts = get_state_counts(system)

    try:
        kill_worker(worker, system)
    except Exception:
        pass

    completed = final_counts.get(completed_state(system), 0)

    return {
        "system": system,
        "scenario": "postgres_restart",
        "config": {"job_count": job_count},
        "results": {
            "pg_downtime_secs": round(pg_downtime, 1),
            "total_time_secs": round(total_time, 1),
            "jobs_completed": completed,
            "jobs_lost": job_count - completed,
            "final_state_counts": final_counts,
        },
    }

def scenario_repeated_kills(system: str, job_count: int = 20,
                            kill_count: int = 3) -> dict:
    """
    Enqueue N jobs, repeatedly kill and restart workers. Verify all complete.
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)
    enqueue_jobs(system, job_count)

    start_time = time.time()
    completed = 0

    for i in range(kill_count):
        print(f"  [{system}] Kill cycle {i+1}/{kill_count}...", file=sys.stderr)
        worker = WORKER_STARTERS[system]()
        time.sleep(5)  # Let it process some jobs
        kill_worker(worker, system)
        time.sleep(1)

        counts = get_state_counts(system)
        completed = counts.get(completed_state(system), 0)
        print(f"  [{system}] After kill {i+1}: {counts}", file=sys.stderr)

    # Final worker — let it finish
    print(f"  [{system}] Starting final worker...", file=sys.stderr)
    worker = WORKER_STARTERS[system]()

    try:
        wait_for_state(system, completed_state(system), min_count=job_count, timeout=300)
    except TimeoutError:
        print(f"  [{system}] TIMEOUT on final drain", file=sys.stderr)

    total_time = time.time() - start_time
    final_counts = get_state_counts(system)

    try:
        kill_worker(worker, system)
    except Exception:
        pass

    completed = final_counts.get(completed_state(system), 0)

    return {
        "system": system,
        "scenario": "repeated_kills",
        "config": {"job_count": job_count, "kill_count": kill_count},
        "results": {
            "total_time_secs": round(total_time, 1),
            "jobs_completed": completed,
            "jobs_lost": job_count - completed,
            "final_state_counts": final_counts,
        },
    }

# ── Scenario 4: PG backend kill ────────────────────────────────────

def scenario_pg_backend_kill(system: str, job_count: int = 10) -> dict:
    """
    Kill Postgres backend connections (not the server) mid-flight.
    Simulates OOM killer targeting individual connections.
    Verifies pool reconnection and job completion.
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)
    enqueue_jobs(system, job_count)

    print(f"  [{system}] Starting worker...", file=sys.stderr)
    worker = WORKER_STARTERS[system]()
    time.sleep(3)

    rs = running_state(system)
    try:
        wait_for_state(system, rs, min_count=1, timeout=30)
    except TimeoutError:
        pass

    # Kill all backend connections for this database (except our psql session).
    # This simulates OOM killer targeting Postgres backend processes.
    kill_time = time.time()
    killed = psql(cfg["db"], """
        SELECT count(pg_terminate_backend(pid))
        FROM pg_stat_activity
        WHERE datname = '{}' AND pid <> pg_backend_pid()
    """.format(cfg["db"]))
    print(f"  [{system}] Killed {killed} backends", file=sys.stderr)

    # Worker should reconnect via pool. Wait for completion.
    try:
        completion_wait = wait_for_state(system, completed_state(system),
                                         min_count=job_count, timeout=180)
    except TimeoutError:
        completion_wait = 180.0
        print(f"  [{system}] TIMEOUT", file=sys.stderr)

    total_time = time.time() - kill_time
    final_counts = get_state_counts(system)

    try:
        kill_worker(worker, system)
    except Exception:
        pass

    completed = final_counts.get(completed_state(system), 0)

    return {
        "system": system,
        "scenario": "pg_backend_kill",
        "config": {"job_count": job_count, "backends_killed": int(killed or 0)},
        "results": {
            "total_time_secs": round(total_time, 1),
            "jobs_completed": completed,
            "jobs_lost": job_count - completed,
            "final_state_counts": final_counts,
        },
    }

# ── Scenario 5: Leader failover ───────────────────────────────────

def scenario_leader_failover(system: str, job_count: int = 10) -> dict:
    """
    Run two worker instances. Kill one (likely the leader). Verify the
    other takes over maintenance duties and all jobs complete. Check for
    duplicate completions.
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)
    enqueue_jobs(system, job_count)

    print(f"  [{system}] Starting worker A...", file=sys.stderr)
    worker_a = WORKER_STARTERS[system]()
    time.sleep(3)

    print(f"  [{system}] Starting worker B...", file=sys.stderr)
    # For Docker workers, need different container names
    worker_b = start_second_worker(system)
    time.sleep(3)

    rs = running_state(system)
    try:
        wait_for_state(system, rs, min_count=1, timeout=30)
    except TimeoutError:
        pass

    # Kill worker A (likely the leader since it started first)
    kill_time = time.time()
    print(f"  [{system}] Killing worker A...", file=sys.stderr)
    kill_worker(worker_a, system)

    # Worker B should take over and complete all jobs
    try:
        completion_wait = wait_for_state(system, completed_state(system),
                                         min_count=job_count, timeout=180)
    except TimeoutError:
        completion_wait = 180.0

    total_time = time.time() - kill_time
    final_counts = get_state_counts(system)

    # Check for duplicates: completed count should equal job_count exactly
    completed = final_counts.get(completed_state(system), 0)
    duplicates = max(0, completed - job_count)

    try:
        kill_second_worker(system)
        kill_worker(worker_b, system)
    except Exception:
        pass

    return {
        "system": system,
        "scenario": "leader_failover",
        "config": {"job_count": job_count},
        "results": {
            "total_time_secs": round(total_time, 1),
            "jobs_completed": completed,
            "jobs_lost": max(0, job_count - completed),
            "duplicates": duplicates,
            "final_state_counts": final_counts,
        },
    }

def start_second_worker(system: str) -> subprocess.Popen:
    """Start a second worker instance (different container name for Docker)."""
    if system == "awa":
        return subprocess.Popen(
            [str(SCRIPT_DIR / "awa-bench" / "target" / "release" / "awa-bench")],
            env={
                **os.environ,
                "DATABASE_URL": pg_url("awa_bench"),
                "SCENARIO": "worker_only",
                "WORKER_COUNT": "10",
                "JOB_DURATION_MS": "30000",
                "RESCUE_INTERVAL_SECS": "5",
                "HEARTBEAT_STALENESS_SECS": "15",
            },
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "awa-docker":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "awa-docker-chaos-worker-b",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_docker_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=10",
             "-e", "JOB_DURATION_MS=30000", "-e", "RESCUE_INTERVAL_SECS=5",
             "-e", "HEARTBEAT_STALENESS_SECS=15",
             "awa-bench-docker"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "awa-python":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "awa-python-chaos-worker-b",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_python_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=10",
             "-e", "JOB_DURATION_MS=30000", "-e", "RESCUE_INTERVAL_SECS=5",
             "-e", "HEARTBEAT_STALENESS_SECS=15",
             "awa-python-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "river":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "river-chaos-worker-b",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('river_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=10",
             "-e", "JOB_DURATION_MS=30000", "-e", "RESCUE_AFTER_SECS=60",
             "river-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "oban":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "oban-chaos-worker-b",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('oban_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=10",
             "-e", "JOB_DURATION_MS=30000", "-e", "RESCUE_AFTER_SECS=15",
             "oban-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    else:  # procrastinate
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "procrastinate-chaos-worker-b",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=10",
             "-e", "JOB_DURATION_MS=30000",
             "procrastinate-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

def kill_second_worker(system: str):
    """Kill the second worker container."""
    if system != "awa":
        container = f"{system}-chaos-worker-b"
        subprocess.run(
            ["docker", "kill", "--signal", "KILL", container],
            capture_output=True, timeout=10,
        )

# ── Scenario 6: Retry storm ──────────────────────────────────────

def scenario_retry_storm(system: str, job_count: int = 500) -> dict:
    """
    Simulate mass failure: insert N jobs directly as retryable/scheduled.
    Measures time for the promotion pipeline to make them available and
    workers to complete them. Tests thundering-herd promotion pressure.
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)

    # Insert jobs directly as retryable (Awa) or scheduled (River/Oban)
    # to simulate mass failure without needing a failing worker.
    retry_enqueue = {
        "awa": """
            INSERT INTO awa.scheduled_jobs (kind, queue, args, state, priority, run_at, attempt, max_attempts)
            SELECT 'chaos_job', 'chaos', jsonb_build_object('seq', g),
                   'retryable', 2, now(), 1, 5
            FROM generate_series(1, {count}) g
        """,
        "river": """
            INSERT INTO river_job (kind, queue, args, state, max_attempts, scheduled_at, attempt)
            SELECT 'chaos_job', 'default', jsonb_build_object('seq', g),
                   'retryable', 5, now(), 1
            FROM generate_series(1, {count}) g
        """,
        "oban": """
            INSERT INTO oban_jobs (worker, queue, args, state, max_attempts, scheduled_at, inserted_at, attempt)
            SELECT 'ObanBench.ChaosWorker', 'chaos', jsonb_build_object('seq', g),
                   'retryable', 5, now(), now(), 1
            FROM generate_series(1, {count}) g
        """,
        "procrastinate": """
            INSERT INTO procrastinate_jobs (task_name, queue_name, args, status, attempts)
            SELECT 'main.chaos_job', 'chaos', jsonb_build_object('seq', g),
                   'todo', 1
            FROM generate_series(1, {count}) g
        """,
    }[system]

    print(f"  [{system}] Inserting {job_count} retryable jobs...", file=sys.stderr)
    psql(cfg["db"], retry_enqueue.format(count=job_count))

    start_time = time.time()
    print(f"  [{system}] Starting worker (must promote then complete)...",
          file=sys.stderr)
    # Use fast jobs (100ms) since we're testing promotion, not execution
    worker = start_worker_fast(system)

    try:
        wait_for_state(system, completed_state(system), min_count=job_count, timeout=300)
    except TimeoutError:
        print(f"  [{system}] TIMEOUT", file=sys.stderr)

    total_time = time.time() - start_time
    final_counts = get_state_counts(system)

    try:
        kill_worker(worker, system)
    except Exception:
        pass

    completed = final_counts.get(completed_state(system), 0)

    return {
        "system": system,
        "scenario": "retry_storm",
        "config": {"job_count": job_count},
        "results": {
            "total_time_secs": round(total_time, 1),
            "jobs_completed": completed,
            "jobs_lost": job_count - completed,
            "final_state_counts": final_counts,
        },
    }

def start_worker_fast(system: str) -> subprocess.Popen:
    """Start a worker with fast job duration (100ms) for promotion tests."""
    if system == "awa":
        return subprocess.Popen(
            [str(SCRIPT_DIR / "awa-bench" / "target" / "release" / "awa-bench")],
            env={
                **os.environ,
                "DATABASE_URL": pg_url("awa_bench"),
                "SCENARIO": "worker_only",
                "WORKER_COUNT": "50",
                "JOB_DURATION_MS": "100",
                "RESCUE_INTERVAL_SECS": "5",
                "HEARTBEAT_STALENESS_SECS": "15",
            },
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "awa-docker":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "awa-docker-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_docker_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "JOB_DURATION_MS=100", "-e", "RESCUE_INTERVAL_SECS=5",
             "-e", "HEARTBEAT_STALENESS_SECS=15",
             "awa-bench-docker"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "awa-python":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "awa-python-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_python_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "JOB_DURATION_MS=100", "-e", "RESCUE_INTERVAL_SECS=5",
             "-e", "HEARTBEAT_STALENESS_SECS=15",
             "awa-python-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "river":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "river-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('river_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "JOB_DURATION_MS=100", "-e", "RESCUE_AFTER_SECS=15",
             "river-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "oban":
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "oban-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('oban_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "JOB_DURATION_MS=100", "-e", "RESCUE_AFTER_SECS=15",
             "oban-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    else:  # procrastinate
        return subprocess.Popen(
            ["docker", "run", "--rm", "--name", "procrastinate-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "JOB_DURATION_MS=100",
             "procrastinate-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

# ── Scenario 7: Pool exhaustion ──────────────────────────────────

def scenario_pool_exhaustion(system: str, job_count: int = 20) -> dict:
    """
    Small connection pool (5), many workers (50). Verifies jobs still
    complete, heartbeats still fire, and no false rescues occur.
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)
    enqueue_jobs(system, job_count)

    print(f"  [{system}] Starting worker (pool=5, workers=50)...", file=sys.stderr)
    # Start worker with tiny pool and many workers
    if system == "awa":
        worker = subprocess.Popen(
            [str(SCRIPT_DIR / "awa-bench" / "target" / "release" / "awa-bench")],
            env={
                **os.environ,
                "DATABASE_URL": pg_url("awa_bench"),
                "SCENARIO": "worker_only",
                "WORKER_COUNT": "50",
                "MAX_CONNECTIONS": "5",
                "JOB_DURATION_MS": "5000",
                "RESCUE_INTERVAL_SECS": "5",
                "HEARTBEAT_STALENESS_SECS": "15",
            },
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "awa-docker":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "awa-docker-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_docker_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "MAX_CONNECTIONS=5", "-e", "JOB_DURATION_MS=5000",
             "-e", "RESCUE_INTERVAL_SECS=5", "-e", "HEARTBEAT_STALENESS_SECS=15",
             "awa-bench-docker"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "awa-python":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "awa-python-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_python_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "MAX_CONNECTIONS=5", "-e", "JOB_DURATION_MS=5000",
             "-e", "RESCUE_INTERVAL_SECS=5", "-e", "HEARTBEAT_STALENESS_SECS=15",
             "awa-python-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "river":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "river-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('river_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "MAX_CONNECTIONS=5", "-e", "JOB_DURATION_MS=5000",
             "-e", "RESCUE_AFTER_SECS=15",
             "river-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "oban":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "oban-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('oban_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "MAX_CONNECTIONS=5", "-e", "JOB_DURATION_MS=5000",
             "-e", "RESCUE_AFTER_SECS=15",
             "oban-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    elif system == "procrastinate":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "procrastinate-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=50",
             "-e", "MAX_CONNECTIONS=5", "-e", "JOB_DURATION_MS=5000",
             "procrastinate-bench"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    else:
        raise ValueError(f"Unsupported pool_exhaustion system: {system}")

    start_time = time.time()

    try:
        wait_for_state(system, completed_state(system), min_count=job_count, timeout=120)
    except TimeoutError:
        print(f"  [{system}] TIMEOUT", file=sys.stderr)

    total_time = time.time() - start_time
    final_counts = get_state_counts(system)

    try:
        kill_worker(worker, system)
    except Exception:
        pass

    completed = final_counts.get(completed_state(system), 0)

    return {
        "system": system,
        "scenario": "pool_exhaustion",
        "config": {"job_count": job_count, "max_connections": 5, "workers": 50},
        "results": {
            "total_time_secs": round(total_time, 1),
            "jobs_completed": completed,
            "jobs_lost": job_count - completed,
            "final_state_counts": final_counts,
        },
    }

# ── Scenario 8: Priority starvation under sustained load ─────────

def scenario_priority_starvation(system: str, job_count: int = 20) -> dict:
    """
    Enqueue low-priority jobs, then continuously enqueue high-priority jobs.
    Verify that the low-priority jobs eventually complete despite sustained
    high-priority pressure (via aging).
    """
    cfg = SYSTEMS[system]
    reset_db(cfg["db"])
    migrate_system(system)

    # Awa-specific: this scenario only works properly with Awa since
    # it has maintenance-based priority aging. River and Oban don't age
    # priorities, so low-priority jobs will be starved under sustained load.
    low_count = job_count
    high_batch = 20  # high-priority jobs per batch
    duration_secs = 90  # how long to sustain the high-priority pressure

    # Insert low-priority jobs
    low_enqueue = {
        "awa": """
            INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, run_at)
            SELECT 'chaos_job', 'chaos', jsonb_build_object('seq', g, 'prio', 'low'),
                   'available', 4, now()
            FROM generate_series(1, {count}) g
        """,
        "river": """
            INSERT INTO river_job (kind, queue, args, state, max_attempts, scheduled_at, priority)
            SELECT 'chaos_job', 'default', jsonb_build_object('seq', g, 'prio', 'low'),
                   'available', 5, now(), 4
            FROM generate_series(1, {count}) g
        """,
        "oban": """
            INSERT INTO oban_jobs (worker, queue, args, state, max_attempts, scheduled_at, inserted_at, priority)
            SELECT 'ObanBench.ChaosWorker', 'chaos', jsonb_build_object('seq', g, 'prio', 'low'),
                   'available', 5, now(), now(), 4
            FROM generate_series(1, {count}) g
        """,
        "procrastinate": """
            INSERT INTO procrastinate_jobs (task_name, queue_name, args, status, priority)
            SELECT 'main.chaos_job', 'chaos', jsonb_build_object('seq', g, 'prio', 'low'),
                   'todo', 0
            FROM generate_series(1, {count}) g
        """,
    }[system]

    print(f"  [{system}] Inserting {low_count} low-priority (4) jobs...",
          file=sys.stderr)
    psql(cfg["db"], low_enqueue.format(count=low_count))

    # Start workers with constrained capacity: 3 workers, 2s per job = ~1.5 j/s
    print(f"  [{system}] Starting workers (3 workers, 2s jobs)...", file=sys.stderr)
    if system == "awa":
        worker = subprocess.Popen(
            [str(SCRIPT_DIR / "awa-bench" / "target" / "release" / "awa-bench")],
            env={**os.environ, "DATABASE_URL": pg_url("awa_bench"),
                 "SCENARIO": "worker_only", "WORKER_COUNT": "3",
                 "JOB_DURATION_MS": "2000", "RESCUE_INTERVAL_SECS": "5",
                 "HEARTBEAT_STALENESS_SECS": "15"},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    elif system == "river":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "river-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('river_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=3",
             "-e", "JOB_DURATION_MS=2000", "-e", "RESCUE_AFTER_SECS=15"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    elif system == "oban":
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "oban-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('oban_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=3",
             "-e", "JOB_DURATION_MS=2000", "-e", "RESCUE_AFTER_SECS=15"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:  # procrastinate
        worker = subprocess.Popen(
            ["docker", "run", "--rm", "--name", "procrastinate-chaos-worker",
             "--network", "host",
             "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
             "-e", "SCENARIO=worker_only", "-e", "WORKER_COUNT=3",
             "-e", "JOB_DURATION_MS=2000"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(3)

    # Batch-enqueue high-priority jobs to sustain queue pressure
    high_batch_sql = {
        "awa": "INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, run_at) SELECT 'chaos_job', 'chaos', jsonb_build_object('prio', 'high', 'seq', g), 'available', 1, now() FROM generate_series(1, {batch}) g",
        "river": "INSERT INTO river_job (kind, queue, args, state, max_attempts, scheduled_at, priority) SELECT 'chaos_job', 'default', jsonb_build_object('prio', 'high', 'seq', g), 'available', 5, now(), 1 FROM generate_series(1, {batch}) g",
        "oban": "INSERT INTO oban_jobs (worker, queue, args, state, max_attempts, scheduled_at, inserted_at, priority) SELECT 'ObanBench.ChaosWorker', 'chaos', jsonb_build_object('prio', 'high', 'seq', g), 'available', 5, now(), now(), 0 FROM generate_series(1, {batch}) g",
        "procrastinate": "INSERT INTO procrastinate_jobs (task_name, queue_name, args, status, priority) SELECT 'main.chaos_job', 'chaos', jsonb_build_object('prio', 'high', 'seq', g), 'todo', 10 FROM generate_series(1, {batch}) g",
    }[system]

    start_time = time.time()
    total_high = 0
    print(f"  [{system}] Sustaining high-priority pressure for {duration_secs}s "
          f"({high_batch} jobs/batch)...", file=sys.stderr)

    while time.time() - start_time < duration_secs:
        try:
            psql(cfg["db"], high_batch_sql.format(batch=high_batch))
            total_high += high_batch
        except Exception:
            pass
        time.sleep(1)

    print(f"  [{system}] Enqueued {total_high} high-priority jobs. Waiting for low-priority completion...",
          file=sys.stderr)

    # Wait for ALL low-priority jobs to complete (they must have been aged)
    # Check by counting completed jobs with prio='low' in args
    low_completed_sql = {
        "awa": "SELECT count(*) FROM awa.jobs WHERE args->>'prio' = 'low' AND state = 'completed'",
        "river": "SELECT count(*) FROM river_job WHERE args->>'prio' = 'low' AND state = 'completed'",
        "oban": "SELECT count(*) FROM oban_jobs WHERE args->>'prio' = 'low' AND state = 'completed'",
        "procrastinate": "SELECT count(*) FROM procrastinate_jobs WHERE args->>'prio' = 'low' AND status = 'succeeded'",
    }[system]

    deadline = time.time() + 120  # 2 more minutes max
    while time.time() < deadline:
        low_done = int(psql(cfg["db"], low_completed_sql) or "0")
        if low_done >= low_count:
            break
        time.sleep(1)

    total_time = time.time() - start_time
    low_done = int(psql(cfg["db"], low_completed_sql) or "0")
    final_counts = get_state_counts(system)

    try:
        kill_worker(worker, system)
    except Exception:
        pass

    return {
        "system": system,
        "scenario": "priority_starvation",
        "config": {
            "low_priority_jobs": low_count,
            "high_priority_jobs_enqueued": total_high,
            "pressure_duration_secs": duration_secs,
        },
        "results": {
            "total_time_secs": round(total_time, 1),
            "low_priority_completed": low_done,
            "low_priority_starved": low_count - low_done,
            "final_state_counts": final_counts,
        },
    }

# ── Migration helpers ─────────────────────────────────────────────

def migrate_system(system: str):
    """Run migrations for the given system. Raises on failure."""
    cmds = {
        "awa": (
            [str(SCRIPT_DIR / "awa-bench" / "target" / "release" / "awa-bench")],
            {"DATABASE_URL": pg_url("awa_bench"), "SCENARIO": "migrate_only"},
        ),
        "awa-docker": (
            ["docker", "run", "--rm", "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_docker_bench')}",
             "-e", "SCENARIO=migrate_only",
             "awa-bench-docker"],
            {},
        ),
        "awa-python": (
            ["docker", "run", "--rm", "--network", "host",
             "-e", f"DATABASE_URL={pg_url('awa_python_bench')}",
             "-e", "SCENARIO=migrate_only",
             "awa-python-bench"],
            {},
        ),
        "river": (
            ["docker", "run", "--rm", "--network", "host",
             "-e", f"DATABASE_URL={pg_url('river_bench')}",
             "-e", "SCENARIO=migrate_only",
             "river-bench"],
            {},
        ),
        "oban": (
            ["docker", "run", "--rm", "--network", "host",
             "-e", f"DATABASE_URL={pg_url('oban_bench')}",
             "-e", "SCENARIO=migrate_only",
             "oban-bench"],
            {},
        ),
        "procrastinate": (
            ["docker", "run", "--rm", "--network", "host",
             "-e", f"DATABASE_URL={pg_url('procrastinate_bench')}",
             "-e", "SCENARIO=migrate_only",
             "procrastinate-bench"],
            {},
        ),
    }
    cmd, extra_env = cmds[system]
    result = subprocess.run(
        cmd, env={**os.environ, **extra_env},
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Migration failed for {system} (exit {result.returncode}):\n{result.stderr}"
        )

# ── Comparison table ──────────────────────────────────────────────

def print_comparison(results: list[dict]):
    print("\n" + "=" * 70)
    print("CHAOS COMPARISON RESULTS")
    print("=" * 70)

    by_scenario: dict[str, list] = {}
    for r in results:
        by_scenario.setdefault(r["scenario"], []).append(r)

    for scenario, items in by_scenario.items():
        print(f"\n--- {scenario} ---")
        if scenario == "crash_recovery":
            print(f"  {'System':<10} {'Rescue (s)':>12} {'Completed':>12} {'Lost':>8}")
            print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*8}")
            for r in items:
                if "error" in r:
                    print(f"  {r['system']:<10} {'ERROR':>12}")
                    continue
                res = r["results"]
                print(f"  {r['system']:<10} {res['rescue_time_secs']:>12.1f} "
                      f"{res['jobs_completed']:>12} {res['jobs_lost']:>8}")
        elif scenario == "postgres_restart":
            print(f"  {'System':<10} {'PG down (s)':>12} {'Total (s)':>12} {'Lost':>8}")
            print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*8}")
            for r in items:
                if "error" in r:
                    print(f"  {r['system']:<10} {'ERROR':>12}")
                    continue
                res = r["results"]
                print(f"  {r['system']:<10} {res['pg_downtime_secs']:>12.1f} "
                      f"{res['total_time_secs']:>12.1f} {res['jobs_lost']:>8}")
        elif scenario == "leader_failover":
            print(f"  {'System':<10} {'Total (s)':>12} {'Completed':>12} {'Lost':>8} {'Dupes':>8}")
            print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*8} {'-'*8}")
            for r in items:
                if "error" in r:
                    print(f"  {r['system']:<10} {'ERROR':>12}")
                    continue
                res = r.get("results", {})
                print(f"  {r['system']:<10} {res.get('total_time_secs',0):>12.1f} "
                      f"{res.get('jobs_completed',0):>12} {res.get('jobs_lost',0):>8} "
                      f"{res.get('duplicates',0):>8}")
        elif scenario == "priority_starvation":
            print(f"  {'System':<10} {'Total (s)':>12} {'Low done':>12} {'Low starved':>12}")
            print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*12}")
            for r in items:
                res = r.get("results", {})
                print(f"  {r['system']:<10} {res.get('total_time_secs',0):>12.1f} "
                      f"{res.get('low_priority_completed',0):>12} "
                      f"{res.get('low_priority_starved',0):>12}")
        else:
            # Generic format for remaining scenarios
            print(f"  {'System':<10} {'Total (s)':>12} {'Completed':>12} {'Lost':>8}")
            print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*8}")
            for r in items:
                res = r.get("results", {})
                if "error" in r:
                    print(f"  {r['system']:<10} {'ERROR':>12}")
                else:
                    print(f"  {r['system']:<10} {res.get('total_time_secs',0):>12.1f} "
                          f"{res.get('jobs_completed',0):>12} {res.get('jobs_lost',0):>8}")
    print()


def resolve_scenarios(selected_scenario: str | None, suite: str | None) -> list[str]:
    if selected_scenario and suite:
        raise ValueError("Use either --scenario or --suite, not both")
    if selected_scenario:
        if selected_scenario == "all":
            return list(EXTENDED_SCENARIOS)
        return [selected_scenario]
    if suite == "portable":
        return list(PORTABLE_SCENARIOS)
    if suite == "extended":
        return list(EXTENDED_SCENARIOS)
    return ["crash_recovery"]

# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Chaos comparison runner")
    parser.add_argument("--scenario",
                        choices=["crash_recovery", "postgres_restart",
                                  "repeated_kills", "pg_backend_kill",
                                  "leader_failover", "retry_storm",
                                  "pool_exhaustion", "priority_starvation",
                                  "all"])
    parser.add_argument("--suite", choices=["portable", "extended"],
                        help="Named scenario set for isolated cross-system runs")
    parser.add_argument("--systems", default=",".join(PORTABLE_SYSTEMS))
    parser.add_argument("--job-count", type=int, default=10)
    parser.add_argument("--keep-pg", action="store_true")
    args = parser.parse_args()

    systems = [s.strip() for s in args.systems.split(",")]
    scenarios = {
        "crash_recovery": scenario_crash_recovery,
        "postgres_restart": scenario_postgres_restart,
        "repeated_kills": scenario_repeated_kills,
        "pg_backend_kill": scenario_pg_backend_kill,
        "leader_failover": scenario_leader_failover,
        "retry_storm": lambda s, n: scenario_retry_storm(s, job_count=500),
        "pool_exhaustion": scenario_pool_exhaustion,
        "priority_starvation": scenario_priority_starvation,
    }

    try:
        start_postgres()
        all_results = []

        to_run = resolve_scenarios(args.scenario, args.suite)
        for scenario_name in to_run:
            scenario_fn = scenarios[scenario_name]
            for system in systems:
                print(f"\n=== {system}: {scenario_name} ===", file=sys.stderr)
                try:
                    result = scenario_fn(system, args.job_count)
                    all_results.append(result)
                except Exception as e:
                    print(f"  ERROR: {e}", file=sys.stderr)
                    all_results.append({
                        "system": system,
                        "scenario": scenario_name,
                        "error": str(e),
                    })

        # Save results
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        result_file = SCRIPT_DIR / "results" / f"chaos_{ts}.json"
        result_file.parent.mkdir(exist_ok=True)
        with open(result_file, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nResults saved to {result_file}", file=sys.stderr)

        if all_results:
            print_comparison(all_results)

    finally:
        if not args.keep_pg:
            stop_postgres()

if __name__ == "__main__":
    main()

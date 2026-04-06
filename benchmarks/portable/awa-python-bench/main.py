#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass

import awa


@dataclass
class BenchJob:
    seq: int


@dataclass
class ChaosJob:
    seq: int


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be set")
    return url


def env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    return int(value) if value is not None else default


def client() -> awa.AsyncClient:
    return awa.AsyncClient(database_url(), max_connections=env_int("MAX_CONNECTIONS", 20))


async def clean_queue(c: awa.AsyncClient, queue: str) -> None:
    tx = await c.transaction()
    try:
        await tx.execute("DELETE FROM awa.jobs WHERE queue = $1", queue)
        await tx.execute("DELETE FROM awa.queue_meta WHERE queue = $1", queue)
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise


async def count_by_state(c: awa.AsyncClient, queue: str) -> dict[str, int]:
    tx = await c.transaction()
    try:
        rows = await tx.fetch_all(
            """
            SELECT state::text AS state, count(*)::bigint AS count
            FROM awa.jobs
            WHERE queue = $1
            GROUP BY state
            """,
            queue,
        )
    finally:
        await tx.rollback()
    return {row["state"]: int(row["count"]) for row in rows}


async def wait_for_completion(
    c: awa.AsyncClient, queue: str, expected: int, timeout_secs: float
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_secs
    while True:
        tx = await c.transaction()
        try:
            row = await tx.fetch_one(
                """
                SELECT count(*)::bigint AS cnt
                FROM awa.jobs
                WHERE queue = $1 AND state = 'completed'
                """,
                queue,
            )
        finally:
            await tx.rollback()
        if int(row["cnt"]) >= expected:
            return
        if asyncio.get_running_loop().time() >= deadline:
            counts = await count_by_state(c, queue)
            raise TimeoutError(
                f"Timeout waiting for {expected} completions on {queue}: {counts}"
            )
        await asyncio.sleep(0.05)


async def enqueue_batch(c: awa.AsyncClient, queue: str, count: int) -> None:
    batch_size = 500
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        jobs = [BenchJob(seq=i) for i in range(batch_start, batch_end)]
        await c.insert_many_copy(jobs, queue=queue)


async def scenario_enqueue_throughput(job_count: int) -> dict:
    c = client()
    await c.migrate()
    queue = "awa_python_enqueue_bench"
    await clean_queue(c, queue)

    start = asyncio.get_running_loop().time()
    await enqueue_batch(c, queue, job_count)
    elapsed = asyncio.get_running_loop().time() - start

    await clean_queue(c, queue)
    await c.close()
    return {
        "system": "awa-python",
        "scenario": "enqueue_throughput",
        "config": {"job_count": job_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_worker_throughput(job_count: int, worker_count: int) -> dict:
    c = client()
    await c.migrate()
    queue = "awa_python_worker_bench"
    await clean_queue(c, queue)
    await enqueue_batch(c, queue, job_count)

    @c.task(BenchJob, queue=queue)
    async def handle(_job: awa.Job[BenchJob]) -> None:
        return None

    start = asyncio.get_running_loop().time()
    await c.start([(queue, worker_count)], poll_interval_ms=50)
    await wait_for_completion(c, queue, job_count, 120)
    elapsed = asyncio.get_running_loop().time() - start

    await c.shutdown(timeout_ms=5000)
    await clean_queue(c, queue)
    await c.close()
    return {
        "system": "awa-python",
        "scenario": "worker_throughput",
        "config": {"job_count": job_count, "worker_count": worker_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_pickup_latency(iterations: int, worker_count: int) -> dict:
    c = client()
    await c.migrate()
    queue = "awa_python_latency_bench"
    await clean_queue(c, queue)

    @c.task(BenchJob, queue=queue)
    async def handle(_job: awa.Job[BenchJob]) -> None:
        return None

    await c.start([(queue, worker_count)], poll_interval_ms=50)
    await asyncio.sleep(0.5)

    latencies_us: list[int] = []
    for i in range(iterations):
        start = asyncio.get_running_loop().time()
        await c.insert(BenchJob(seq=i), queue=queue)
        await wait_for_completion(c, queue, i + 1, 10)
        latencies_us.append(round((asyncio.get_running_loop().time() - start) * 1_000_000))

    await c.shutdown(timeout_ms=5000)
    await clean_queue(c, queue)
    await c.close()

    latencies_us.sort()
    n = len(latencies_us)
    return {
        "system": "awa-python",
        "scenario": "pickup_latency",
        "config": {"iterations": iterations, "worker_count": worker_count},
        "results": {
            "mean_us": sum(latencies_us) / n,
            "p50_us": latencies_us[n // 2],
            "p95_us": latencies_us[min(int(n * 0.95), n - 1)],
            "p99_us": latencies_us[min(int(n * 0.99), n - 1)],
        },
    }


async def scenario_migrate_only() -> None:
    c = client()
    await c.migrate()
    await c.close()
    print("[awa-python] Migrations complete.", file=sys.stderr)


async def scenario_worker_only() -> None:
    c = client()
    await c.migrate()

    worker_count = env_int("WORKER_COUNT", 10)
    job_duration_ms = env_int("JOB_DURATION_MS", 30000)
    rescue_interval_secs = env_int("RESCUE_INTERVAL_SECS", 5)
    heartbeat_staleness_secs = env_int("HEARTBEAT_STALENESS_SECS", 15)
    stop_event = asyncio.Event()

    @c.task(ChaosJob, queue="chaos")
    async def handle(_job: awa.Job[ChaosJob]) -> None:
        await asyncio.sleep(job_duration_ms / 1000.0)
        return None

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())

    await c.start(
        [("chaos", worker_count)],
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        heartbeat_rescue_interval_ms=rescue_interval_secs * 1000,
        heartbeat_staleness_ms=heartbeat_staleness_secs * 1000,
    )
    print(
        (
            "[awa-python] worker_only: started with "
            f"{worker_count} workers, job_duration={job_duration_ms}ms. "
            "Blocking until signal."
        ),
        file=sys.stderr,
    )
    await stop_event.wait()
    await c.shutdown(timeout_ms=5000)
    await c.close()
    print("[awa-python] worker_only: received signal, exiting.", file=sys.stderr)


async def main() -> None:
    scenario = os.environ.get("SCENARIO", "all")
    job_count = env_int("JOB_COUNT", 10000)
    worker_count = env_int("WORKER_COUNT", 50)
    latency_iterations = env_int("LATENCY_ITERATIONS", 100)

    if scenario == "migrate_only":
        await scenario_migrate_only()
        return
    if scenario == "worker_only":
        await scenario_worker_only()
        return

    results: list[dict] = []
    if scenario in ("all", "enqueue_throughput"):
        print("[awa-python] Running enqueue_throughput...", file=sys.stderr)
        results.append(await scenario_enqueue_throughput(job_count))
    if scenario in ("all", "worker_throughput"):
        print("[awa-python] Running worker_throughput...", file=sys.stderr)
        results.append(await scenario_worker_throughput(job_count, worker_count))
    if scenario in ("all", "pickup_latency"):
        print("[awa-python] Running pickup_latency...", file=sys.stderr)
        results.append(await scenario_pickup_latency(latency_iterations, worker_count))

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

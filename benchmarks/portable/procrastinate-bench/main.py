#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from time import monotonic

import procrastinate
from psycopg import AsyncConnection
from psycopg.rows import dict_row


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


app = procrastinate.App(
    connector=procrastinate.PsycopgConnector(conninfo=database_url()),
)


@app.task(queue="portable_default")
async def bench_job(seq: int) -> None:
    return None


@app.task(queue="chaos")
async def chaos_job(seq: int) -> None:
    await asyncio.sleep(env_int("JOB_DURATION_MS", 30000) / 1000.0)


async def connect() -> AsyncConnection:
    conn = await AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


async def clean_queue(queue: str) -> None:
    async with await connect() as conn:
        await conn.execute("DELETE FROM procrastinate_jobs WHERE queue_name = %s", (queue,))


async def count_completed(queue: str) -> int:
    async with await connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT count(*)::bigint AS cnt
                FROM procrastinate_jobs
                WHERE queue_name = %s AND status = 'succeeded'
                """,
                (queue,),
            )
            row = await cur.fetchone()
    return int(row["cnt"])


async def count_by_state(queue: str) -> dict[str, int]:
    async with await connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT status::text AS status, count(*)::bigint AS count
                FROM procrastinate_jobs
                WHERE queue_name = %s
                GROUP BY status
                """,
                (queue,),
            )
            rows = await cur.fetchall()
    return {row["status"]: int(row["count"]) for row in rows}


async def wait_for_completion(queue: str, expected: int, timeout_secs: float) -> None:
    deadline = monotonic() + timeout_secs
    while True:
        completed = await count_completed(queue)
        if completed >= expected:
            return
        if monotonic() >= deadline:
            counts = await count_by_state(queue)
            raise TimeoutError(
                f"Timeout waiting for {expected} completions on {queue}: {counts}"
            )
        await asyncio.sleep(0.05)


async def enqueue_batch(queue: str, count: int) -> None:
    batch_size = 500
    deferrer = bench_job.configure(queue=queue)
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        await deferrer.batch_defer_async(
            *({"seq": i} for i in range(batch_start, batch_end))
        )


async def enqueue_chaos_jobs(count: int, *, low_priority: bool = False) -> None:
    deferrer = chaos_job.configure(queue="chaos")
    for seq in range(1, count + 1):
        payload = {"seq": seq}
        if low_priority:
            payload["prio"] = "low"
        await deferrer.defer_async(**payload)


def build_worker(queue: str, worker_count: int, *, wait: bool):
    return app._worker(
        queues=[queue],
        concurrency=worker_count,
        wait=wait,
        fetch_job_polling_interval=0.05,
        abort_job_polling_interval=0.05,
        listen_notify=True,
        delete_jobs="never",
        install_signal_handlers=False,
        update_heartbeat_interval=5,
        stalled_worker_timeout=15,
    )


async def scenario_enqueue_throughput(job_count: int) -> dict:
    queue = "procrastinate_enqueue_bench"
    await clean_queue(queue)

    start = monotonic()
    await enqueue_batch(queue, job_count)
    elapsed = monotonic() - start

    await clean_queue(queue)
    return {
        "system": "procrastinate",
        "scenario": "enqueue_throughput",
        "config": {"job_count": job_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_worker_throughput(job_count: int, worker_count: int) -> dict:
    queue = "procrastinate_worker_bench"
    await clean_queue(queue)
    await enqueue_batch(queue, job_count)

    worker = build_worker(queue, worker_count, wait=False)
    start = monotonic()
    await worker.run()
    elapsed = monotonic() - start

    await wait_for_completion(queue, job_count, 10)
    await clean_queue(queue)
    return {
        "system": "procrastinate",
        "scenario": "worker_throughput",
        "config": {"job_count": job_count, "worker_count": worker_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_pickup_latency(iterations: int, worker_count: int) -> dict:
    queue = "procrastinate_latency_bench"
    await clean_queue(queue)

    worker = build_worker(queue, worker_count, wait=True)
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.5)

    deferrer = bench_job.configure(queue=queue)
    latencies_us: list[int] = []
    for i in range(iterations):
        start = monotonic()
        await deferrer.defer_async(seq=i)
        await wait_for_completion(queue, i + 1, 10)
        latencies_us.append(round((monotonic() - start) * 1_000_000))

    worker.stop()
    await worker_task

    await clean_queue(queue)
    latencies_us.sort()
    n = len(latencies_us)
    return {
        "system": "procrastinate",
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
    print("[procrastinate] Migrations complete.", file=sys.stderr)


async def scenario_worker_only() -> None:
    worker_count = env_int("WORKER_COUNT", 10)
    stop_event = asyncio.Event()
    worker = build_worker("chaos", worker_count, wait=True)
    worker_task = asyncio.create_task(worker.run())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())

    print(
        f"[procrastinate] worker_only: started with {worker_count} workers. Blocking until signal.",
        file=sys.stderr,
    )
    await stop_event.wait()
    worker.stop()
    await worker_task
    print("[procrastinate] worker_only: received signal, exiting.", file=sys.stderr)


async def main() -> None:
    scenario = os.environ.get("SCENARIO", "all")
    job_count = env_int("JOB_COUNT", 10000)
    worker_count = env_int("WORKER_COUNT", 50)
    latency_iterations = env_int("LATENCY_ITERATIONS", 100)

    async with app.open_async():
        await app.schema_manager.apply_schema_async()
        if scenario == "migrate_only":
            await scenario_migrate_only()
            return
        if scenario == "worker_only":
            await scenario_worker_only()
            return

        results: list[dict] = []
        if scenario in ("all", "enqueue_throughput"):
            print("[procrastinate] Running enqueue_throughput...", file=sys.stderr)
            results.append(await scenario_enqueue_throughput(job_count))
        if scenario in ("all", "worker_throughput"):
            print("[procrastinate] Running worker_throughput...", file=sys.stderr)
            results.append(await scenario_worker_throughput(job_count, worker_count))
        if scenario in ("all", "pickup_latency"):
            print("[procrastinate] Running pickup_latency...", file=sys.stderr)
            results.append(await scenario_pickup_latency(latency_iterations, worker_count))

        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

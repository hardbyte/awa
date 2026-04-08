#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from time import monotonic

from absurd_sdk import AsyncAbsurd
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row


ABSURD_SQL_PATH = Path(os.environ.get("ABSURD_SQL_PATH", "/opt/absurd.sql"))
POLL_INTERVAL_SECS = 0.05


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be set")
    return url


def env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    return int(value) if value is not None else default


def ensure_schema() -> None:
    result = subprocess.run(
        [
            "psql",
            database_url(),
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(ABSURD_SQL_PATH),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to apply {ABSURD_SQL_PATH}: {result.stderr or result.stdout}"
        )


def build_app(queue: str) -> AsyncAbsurd:
    app = AsyncAbsurd(database_url(), queue_name=queue)

    @app.register_task("bench_job")
    async def bench_job(_params: dict, _ctx) -> None:
        return None

    return app


async def connect() -> AsyncConnection:
    return await AsyncConnection.connect(
        database_url(),
        autocommit=True,
        row_factory=dict_row,
    )


async def recreate_queue(queue: str) -> None:
    app = build_app(queue)
    try:
        await app.drop_queue()
        await app.create_queue()
    finally:
        await app.close()


async def count_completed(conn: AsyncConnection, queue: str) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL("SELECT count(*)::bigint AS cnt FROM absurd.{} WHERE state = 'completed'").format(
                sql.Identifier(f"t_{queue}")
            )
        )
        row = await cur.fetchone()
    return int(row["cnt"])


async def count_by_state(conn: AsyncConnection, queue: str) -> dict[str, int]:
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL(
                "SELECT state::text AS state, count(*)::bigint AS count "
                "FROM absurd.{} GROUP BY state"
            ).format(sql.Identifier(f"t_{queue}"))
        )
        rows = await cur.fetchall()
    return {row["state"]: int(row["count"]) for row in rows}


async def wait_for_completion(
    conn: AsyncConnection,
    queue: str,
    expected: int,
    timeout_secs: float,
) -> None:
    deadline = monotonic() + timeout_secs
    while True:
        completed = await count_completed(conn, queue)
        if completed >= expected:
            return
        if monotonic() >= deadline:
            counts = await count_by_state(conn, queue)
            raise TimeoutError(
                f"Timeout waiting for {expected} completions on {queue}: {counts}"
            )
        await asyncio.sleep(POLL_INTERVAL_SECS)


async def wait_for_task_completion(
    app: AsyncAbsurd,
    task_id: str,
    timeout_secs: float,
) -> None:
    deadline = monotonic() + timeout_secs
    while True:
        snapshot = await app.fetch_task_result(task_id)
        if snapshot is None:
            raise RuntimeError(f"task {task_id} disappeared")
        if snapshot.state == "completed":
            return
        if snapshot.state in {"failed", "cancelled"}:
            raise RuntimeError(f"task {task_id} ended in state {snapshot.state}")
        if monotonic() >= deadline:
            raise TimeoutError(f"Timeout waiting for task {task_id} to complete")
        await asyncio.sleep(POLL_INTERVAL_SECS)


async def enqueue_batch(conn: AsyncConnection, queue: str, count: int) -> None:
    batch_size = 500
    query = (
        "SELECT task_id FROM absurd.spawn_task(%s, %s, %s::jsonb, %s::jsonb)"
    )
    async with conn.cursor() as cur:
        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            params = [
                (queue, "bench_job", json.dumps({"seq": i}), "{}")
                for i in range(batch_start, batch_end)
            ]
            await cur.executemany(query, params)


async def run_worker(queue: str, worker_count: int) -> tuple[AsyncAbsurd, asyncio.Task[None]]:
    worker_app = build_app(queue)
    worker_task = asyncio.create_task(
        worker_app.start_worker(
            concurrency=worker_count,
            poll_interval=POLL_INTERVAL_SECS,
        )
    )
    return worker_app, worker_task


async def stop_worker(worker_app: AsyncAbsurd, worker_task: asyncio.Task[None]) -> None:
    worker_app.stop_worker()
    await worker_task
    await worker_app.close()


async def scenario_enqueue_throughput(job_count: int) -> dict:
    queue = "absurd_enqueue_bench"
    await recreate_queue(queue)
    conn = await connect()
    try:
        start = monotonic()
        await enqueue_batch(conn, queue, job_count)
        elapsed = monotonic() - start
    finally:
        await conn.close()

    return {
        "system": "absurd",
        "scenario": "enqueue_throughput",
        "config": {"job_count": job_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_worker_throughput(job_count: int, worker_count: int) -> dict:
    queue = "absurd_worker_bench"
    await recreate_queue(queue)
    conn = await connect()
    worker_app: AsyncAbsurd | None = None
    worker_task: asyncio.Task[None] | None = None
    try:
        await enqueue_batch(conn, queue, job_count)
        start = monotonic()
        worker_app, worker_task = await run_worker(queue, worker_count)
        await wait_for_completion(conn, queue, job_count, 120)
        elapsed = monotonic() - start
    finally:
        if worker_app is not None and worker_task is not None:
            await stop_worker(worker_app, worker_task)
        await conn.close()

    return {
        "system": "absurd",
        "scenario": "worker_throughput",
        "config": {"job_count": job_count, "worker_count": worker_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_pickup_latency(iterations: int, worker_count: int) -> dict:
    queue = "absurd_latency_bench"
    await recreate_queue(queue)
    control_app = build_app(queue)
    worker_app, worker_task = await run_worker(queue, worker_count)

    try:
        await asyncio.sleep(0.5)
        latencies_us: list[int] = []
        for i in range(iterations):
            start = monotonic()
            result = await control_app.spawn("bench_job", {"seq": i})
            await wait_for_task_completion(control_app, result["task_id"], 10)
            latencies_us.append(round((monotonic() - start) * 1_000_000))
    finally:
        await stop_worker(worker_app, worker_task)
        await control_app.close()

    latencies_us.sort()
    n = len(latencies_us)
    return {
        "system": "absurd",
        "scenario": "pickup_latency",
        "config": {"iterations": iterations, "worker_count": worker_count},
        "results": {
            "mean_us": sum(latencies_us) / n,
            "p50_us": latencies_us[n // 2],
            "p95_us": latencies_us[min(int(n * 0.95), n - 1)],
            "p99_us": latencies_us[min(int(n * 0.99), n - 1)],
        },
    }


async def main() -> None:
    scenario = os.environ.get("SCENARIO", "all")
    job_count = env_int("JOB_COUNT", 10000)
    worker_count = env_int("WORKER_COUNT", 50)
    latency_iterations = env_int("LATENCY_ITERATIONS", 100)

    ensure_schema()

    results: list[dict] = []
    if scenario == "all" or scenario == "enqueue_throughput":
        print("[absurd] Running enqueue_throughput...", file=sys.stderr)
        results.append(await scenario_enqueue_throughput(job_count))
    if scenario == "all" or scenario == "worker_throughput":
        print("[absurd] Running worker_throughput...", file=sys.stderr)
        results.append(await scenario_worker_throughput(job_count, worker_count))
    if scenario == "all" or scenario == "pickup_latency":
        print("[absurd] Running pickup_latency...", file=sys.stderr)
        results.append(await scenario_pickup_latency(latency_iterations, worker_count))

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

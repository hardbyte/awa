#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from time import monotonic
from urllib.parse import urlparse

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


def env_str(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value is not None else default


def read_producer_rate(default: int) -> int:
    control_file = os.environ.get("PRODUCER_RATE_CONTROL_FILE")
    if not control_file:
        return default
    try:
        with open(control_file) as fh:
            return int(float(fh.read().strip()))
    except Exception:
        return default


app = procrastinate.App(
    connector=procrastinate.PsycopgConnector(conninfo=database_url()),
)


@app.task(queue="portable_default", name="bench_job")
async def bench_job(seq: int) -> None:
    return None


@app.task(queue="chaos", name="chaos_job")
async def chaos_job(seq: int) -> None:
    await asyncio.sleep(env_int("JOB_DURATION_MS", 30000) / 1000.0)


async def connect() -> AsyncConnection:
    conn = await AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


async def clean_queue(queue: str) -> None:
    async with await connect() as conn:
        await conn.execute(
            "DELETE FROM procrastinate_jobs WHERE queue_name = %s", (queue,)
        )


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


async def scenario_long_horizon() -> None:
    """Fixed-rate producer + steady consumer with JSONL telemetry every
    SAMPLE_EVERY_S. Contract: benchmarks/portable/CONTRIBUTING_ADAPTERS.md"""
    import collections
    import datetime as _dt
    import time

    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    work_ms = env_int("JOB_WORK_MS", 1)

    queue = "procrastinate_longhorizon_bench"
    # Parse the URL path so query params like ?sslmode=disable don't leak
    # into the descriptor's reported db_name.
    db_name = (urlparse(database_url()).path or "/").lstrip("/") or "procrastinate_bench"

    _emit(
        {
            "kind": "descriptor",
            "system": "procrastinate",
            "event_tables": [
                "public.procrastinate_jobs",
                "public.procrastinate_events",
            ],
            "extensions": [],
            "version": "0.1.0",
            "schema_version": os.environ.get("PROCRASTINATE_SCHEMA_VERSION", "current"),
            "db_name": db_name,
            "started_at": _now_iso(),
        }
    )

    latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    enqueued = 0
    completed = 0
    queue_depth = 0
    current_producer_target_rate = float(producer_rate)

    # Redefine the task so the handler captures `latencies_ms`/`completed`.
    # procrastinate discovers tasks by name, so we can shadow the module-level
    # `bench_job` with a queue-scoped handler. We use a fresh App to avoid
    # cross-scenario leakage.
    lh_app = procrastinate.App(
        connector=procrastinate.PsycopgConnector(conninfo=database_url()),
    )

    @lh_app.task(queue=queue, name="long_horizon_job", pass_context=True)
    async def long_horizon_task(
        context, seq: int, created_at_iso: str, padding: str = ""
    ) -> None:
        nonlocal completed
        try:
            created = _dt.datetime.fromisoformat(created_at_iso)
            now = _dt.datetime.now(_dt.timezone.utc)
            latency_ms = max(0.0, (now - created).total_seconds() * 1000.0)
        except Exception:
            latency_ms = 0.0
        latencies_ms.append((time.monotonic(), latency_ms))
        if work_ms:
            await asyncio.sleep(work_ms / 1000.0)
        completed += 1

    async with lh_app.open_async():
        worker = lh_app._worker(
            queues=[queue],
            concurrency=worker_count,
            wait=True,
            fetch_job_polling_interval=0.05,
            abort_job_polling_interval=0.05,
            listen_notify=True,
            delete_jobs="never",
            install_signal_handlers=False,
            update_heartbeat_interval=5,
            stalled_worker_timeout=15,
        )
        worker_task = asyncio.create_task(worker.run())

        shutdown = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_a: shutdown.set())

        async def producer() -> None:
            nonlocal enqueued, current_producer_target_rate
            seq = 0
            deferrer = long_horizon_task.configure(queue=queue)
            next_t = loop.time()
            while not shutdown.is_set():
                if producer_mode == "depth-target":
                    current_producer_target_rate = 0.0
                    if queue_depth >= target_depth:
                        await asyncio.sleep(0.05)
                        continue
                    next_t = loop.time()
                else:
                    effective_rate = read_producer_rate(producer_rate)
                    current_producer_target_rate = float(effective_rate)
                    if effective_rate <= 0:
                        next_t = loop.time()
                        await asyncio.sleep(0.1)
                        continue
                    next_t = max(next_t + (1.0 / effective_rate), loop.time())
                    sleep_for = next_t - loop.time()
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
                try:
                    await deferrer.defer_async(
                        seq=seq,
                        created_at_iso=_now_iso(),
                    )
                    enqueued += 1
                    seq += 1
                except Exception as exc:
                    print(
                        f"[procrastinate] producer insert failed: {exc}",
                        file=sys.stderr,
                    )

        async def depth_poller() -> None:
            nonlocal queue_depth
            if not _observer_enabled():
                # Non-zero replicas don't emit observer metrics; idle this
                # task instead of polling. Saves N-1 connections of polling
                # work on a multi-replica run.
                while not shutdown.is_set():
                    await asyncio.sleep(0.25)
                return
            while not shutdown.is_set():
                try:
                    async with await connect() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "SELECT count(*)::bigint AS cnt "
                                "FROM procrastinate_jobs "
                                "WHERE queue_name = %s AND status = 'todo'",
                                (queue,),
                            )
                            row = await cur.fetchone()
                            queue_depth = int(row["cnt"]) if row else 0
                except Exception:
                    pass
                await asyncio.sleep(1.0)

        async def sampler() -> None:
            now_epoch = int(time.time())
            sleep_for = sample_every_s - (now_epoch % sample_every_s)
            await asyncio.sleep(sleep_for)
            last_enq, last_cmp = 0, 0
            last_tick = loop.time()
            while not shutdown.is_set():
                deadline = loop.time() + sample_every_s
                while not shutdown.is_set() and loop.time() < deadline:
                    await asyncio.sleep(min(0.5, deadline - loop.time()))
                if shutdown.is_set():
                    break
                dt = max(0.001, loop.time() - last_tick)
                last_tick = loop.time()
                enq_rate = (enqueued - last_enq) / dt
                cmp_rate = (completed - last_cmp) / dt
                last_enq, last_cmp = enqueued, completed
                p50, p95, p99 = _percentiles(
                    latencies_ms, window_s=30.0, now=loop.time()
                )
                ts = _now_iso()
                for metric, value, window_s in [
                    ("claim_p50_ms", p50, 30.0),
                    ("claim_p95_ms", p95, 30.0),
                    ("claim_p99_ms", p99, 30.0),
                    ("enqueue_rate", enq_rate, float(sample_every_s)),
                    ("completion_rate", cmp_rate, float(sample_every_s)),
                    ("queue_depth", float(queue_depth), 0.0),
                    ("producer_target_rate", current_producer_target_rate, 0.0),
                ]:
                    if metric in _OBSERVER_METRICS and not _observer_enabled():
                        continue
                    _emit(
                        {
                            "t": ts,
                            "system": "procrastinate",
                            "kind": "adapter",
                            "subject_kind": "adapter",
                            "subject": "",
                            "metric": metric,
                            "value": value,
                            "window_s": window_s,
                        }
                    )

        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(producer(), name="producer"),
            asyncio.create_task(depth_poller(), name="depth"),
            asyncio.create_task(sampler(), name="sampler"),
        ]
        try:
            await shutdown.wait()
        finally:
            shutdown.set()
            for t in tasks:
                t.cancel()
            worker.stop()
            try:
                await asyncio.wait_for(
                    asyncio.gather(worker_task, *tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                worker_task.cancel()
                await asyncio.gather(worker_task, *tasks, return_exceptions=True)
            print(
                "[procrastinate] long_horizon: shutdown signal received",
                file=sys.stderr,
            )


def _instance_id() -> int:
    try:
        return int(os.environ.get("BENCH_INSTANCE_ID", "0"))
    except ValueError:
        return 0


def _observer_enabled() -> bool:
    """Mirror of awa-bench's `observer_enabled`. Only instance 0 emits
    cross-system observer metrics (queue depth, total backlog, producer
    target rate) so multi-replica runs report a single global observation
    instead of one per replica that the summary aggregator would have to
    de-duplicate later."""
    return _instance_id() == 0


# Adapter metrics that describe a *global* observation (queue depth,
# total backlog) rather than this replica's per-instance behaviour.
# Only instance 0 emits these.
_OBSERVER_METRICS = frozenset({
    "queue_depth",
    "running_depth",
    "retryable_depth",
    "scheduled_depth",
    "total_backlog",
    "producer_target_rate",
})


def _emit(record: dict) -> None:
    # Stamp the adapter's replica index onto every emission so the harness
    # tailer can attribute samples to the right row in raw.csv without
    # needing to track per-subprocess state. Descriptor and sample records
    # both carry it.
    record.setdefault("instance_id", _instance_id())
    print(json.dumps(record), flush=True)


def _now_iso() -> str:
    import datetime as _dt

    return (
        _dt.datetime.now(_dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _percentiles(events, *, window_s: float, now: float):
    cutoff = now - window_s
    values = [v for t, v in events if t >= cutoff]
    if not values:
        return 0.0, 0.0, 0.0
    values.sort()
    n = len(values)

    def q(p):
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return values[idx]

    return q(0.50), q(0.95), q(0.99)


async def ensure_schema_applied() -> None:
    """Apply procrastinate's schema only if not already present.

    procrastinate.SchemaManager.apply_schema_async() is not idempotent — it
    issues raw CREATE TYPE / CREATE TABLE without IF NOT EXISTS, so a second
    invocation crashes with DuplicateObject. We therefore probe for the core
    table first and skip if it exists. Without this guard a worker container
    started after the migrate_only container would die before processing
    any jobs (silent worker crash → 100% job loss in chaos scenarios).
    """
    async with await connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT to_regclass('public.procrastinate_jobs')")
            row = await cur.fetchone()
    if row and row[next(iter(row))] is not None:
        return
    await app.schema_manager.apply_schema_async()


async def main() -> None:
    scenario = os.environ.get("SCENARIO", "all")
    job_count = env_int("JOB_COUNT", 10000)
    worker_count = env_int("WORKER_COUNT", 50)
    latency_iterations = env_int("LATENCY_ITERATIONS", 100)

    async with app.open_async():
        await ensure_schema_applied()
        if scenario == "migrate_only":
            await scenario_migrate_only()
            return
        if scenario == "worker_only":
            await scenario_worker_only()
            return
        if scenario == "long_horizon":
            await scenario_long_horizon()
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
            results.append(
                await scenario_pickup_latency(latency_iterations, worker_count)
            )

        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

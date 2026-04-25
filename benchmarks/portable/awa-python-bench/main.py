#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from collections import deque
from dataclasses import dataclass
from urllib.parse import urlparse

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


async def migrate_with_retry(c, *, attempts: int = 4, base_delay: float = 0.5) -> None:
    """Run c.migrate() with retry on transient lock errors.

    The chaos harness can leave Postgres backends from previously SIGKILL'd
    worker containers holding session-scoped advisory locks (AWA_MIGR) and
    AccessExclusive locks on awa.* cache tables for a brief window. The next
    container's migrate() then races those leftovers and Postgres returns
    40P01 (deadlock detected) or a lock_timeout-style failure. The migration
    is idempotent, so retrying with backoff is safe.
    """
    last_exc: BaseException | None = None
    for attempt in range(attempts):
        try:
            await c.migrate()
            return
        except (
            Exception
        ) as exc:  # awa.DatabaseError surfaces as a generic Exception via PyO3
            msg = str(exc)
            if "deadlock detected" not in msg and "lock_not_available" not in msg:
                raise
            last_exc = exc
            delay = base_delay * (2**attempt)
            print(
                f"[awa-python] migrate() transient lock failure (attempt {attempt + 1}/{attempts}): {msg}; retrying in {delay:.1f}s",
                file=sys.stderr,
            )
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc


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


def queue_storage_schema() -> str:
    return os.environ.get("QUEUE_STORAGE_SCHEMA", "awa_exp")


def queue_slot_count() -> int:
    return env_int("QUEUE_SLOT_COUNT", 16)


def lease_slot_count() -> int:
    return env_int("LEASE_SLOT_COUNT", 8)


def queue_rotate_ms() -> int:
    return env_int("QUEUE_ROTATE_MS", 1000)


def lease_rotate_ms() -> int:
    return env_int("LEASE_ROTATE_MS", 50)


def client() -> awa.AsyncClient:
    return awa.AsyncClient(
        database_url(), max_connections=env_int("MAX_CONNECTIONS", 20)
    )


async def prepare_queue_storage(c: awa.AsyncClient) -> str:
    await migrate_with_retry(c)
    schema = queue_storage_schema()
    await c.install_queue_storage(
        schema=schema,
        queue_slot_count=queue_slot_count(),
        lease_slot_count=lease_slot_count(),
        reset=True,
    )
    return schema


async def clean_queue(c: awa.AsyncClient, queue: str) -> None:
    tx = await c.transaction()
    try:
        await tx.execute("DELETE FROM awa.jobs WHERE queue = $1", queue)
        await tx.execute("DELETE FROM awa.queue_meta WHERE queue = $1", queue)
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise


async def count_by_state(c: awa.AsyncClient, queue: str, schema: str) -> dict[str, int]:
    tx = await c.transaction()
    try:
        rows = await tx.fetch_all(
            f"""
            SELECT state, sum(count)::bigint AS count
            FROM (
                SELECT 'available'::text AS state, count(*)::bigint AS count
                FROM {schema}.ready_entries
                WHERE queue = $1

                UNION ALL

                SELECT state::text AS state, count(*)::bigint AS count
                FROM {schema}.leases
                WHERE queue = $1
                GROUP BY state

                UNION ALL

                SELECT state::text AS state, count(*)::bigint AS count
                FROM {schema}.deferred_jobs
                WHERE queue = $1
                GROUP BY state

                UNION ALL

                SELECT state::text AS state, count(*)::bigint AS count
                FROM {schema}.done_entries
                WHERE queue = $1
                GROUP BY state

                UNION ALL

                SELECT 'dlq'::text AS state, count(*)::bigint AS count
                FROM {schema}.dlq_entries
                WHERE queue = $1
            ) counts
            GROUP BY state
            """,
            queue,
        )
    finally:
        await tx.rollback()
    return {row["state"]: int(row["count"]) for row in rows}


async def wait_for_completion(
    c: awa.AsyncClient, queue: str, expected: int, timeout_secs: float, schema: str
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_secs
    while True:
        tx = await c.transaction()
        try:
            row = await tx.fetch_one(
                f"""
                WITH lane_counts AS (
                    SELECT COALESCE(sum(completed_count), 0)::bigint AS pruned_completed
                    FROM {schema}.queue_lanes
                    WHERE queue = $1
                ),
                live_terminal AS (
                    SELECT count(*)::bigint AS completed
                    FROM {schema}.done_entries
                    WHERE queue = $1
                )
                SELECT lane_counts.pruned_completed + live_terminal.completed AS cnt
                FROM lane_counts
                CROSS JOIN live_terminal
                """,
                queue,
            )
        finally:
            await tx.rollback()
        if int(row["cnt"]) >= expected:
            return
        if asyncio.get_running_loop().time() >= deadline:
            counts = await count_by_state(c, queue, schema)
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
    await prepare_queue_storage(c)
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
    schema = await prepare_queue_storage(c)
    queue = "awa_python_worker_bench"
    await clean_queue(c, queue)
    await enqueue_batch(c, queue, job_count)

    @c.task(BenchJob, queue=queue)
    async def handle(_job: awa.Job[BenchJob]) -> None:
        return None

    start = asyncio.get_running_loop().time()
    await c.start(
        [(queue, worker_count)],
        poll_interval_ms=50,
        queue_storage_schema=schema,
        queue_storage_queue_slot_count=queue_slot_count(),
        queue_storage_lease_slot_count=lease_slot_count(),
        queue_storage_queue_rotate_interval_ms=queue_rotate_ms(),
        queue_storage_lease_rotate_interval_ms=lease_rotate_ms(),
    )
    await wait_for_completion(c, queue, job_count, 120, schema)
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
    schema = await prepare_queue_storage(c)
    queue = "awa_python_latency_bench"
    await clean_queue(c, queue)

    @c.task(BenchJob, queue=queue)
    async def handle(_job: awa.Job[BenchJob]) -> None:
        return None

    await c.start(
        [(queue, worker_count)],
        poll_interval_ms=50,
        queue_storage_schema=schema,
        queue_storage_queue_slot_count=queue_slot_count(),
        queue_storage_lease_slot_count=lease_slot_count(),
        queue_storage_queue_rotate_interval_ms=queue_rotate_ms(),
        queue_storage_lease_rotate_interval_ms=lease_rotate_ms(),
    )
    await asyncio.sleep(0.5)

    latencies_us: list[int] = []
    for i in range(iterations):
        start = asyncio.get_running_loop().time()
        await c.insert(BenchJob(seq=i), queue=queue)
        # Queue-storage prune can rotate individual completed rows away;
        # waiting on the cumulative completion count keeps the benchmark
        # semantics stable while avoiding per-job races.
        await wait_for_completion(c, queue, i + 1, 10, schema)
        latencies_us.append(
            round((asyncio.get_running_loop().time() - start) * 1_000_000)
        )

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
    await prepare_queue_storage(c)
    await c.close()
    print("[awa-python] Migrations + queue_storage install complete.", file=sys.stderr)


async def scenario_worker_only() -> None:
    c = client()
    schema = await prepare_queue_storage(c)

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
        queue_storage_schema=schema,
        queue_storage_queue_slot_count=queue_slot_count(),
        queue_storage_lease_slot_count=lease_slot_count(),
        queue_storage_queue_rotate_interval_ms=queue_rotate_ms(),
        queue_storage_lease_rotate_interval_ms=lease_rotate_ms(),
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


async def scenario_long_horizon() -> None:
    """Fixed-rate producer + steady consumer, emits JSONL samples every
    SAMPLE_EVERY_S. Contract: benchmarks/portable/CONTRIBUTING_ADAPTERS.md"""
    import datetime
    import math
    import time

    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    # JOB_PAYLOAD_BYTES is part of the adapter contract but BenchJob has no
    # payload field today; intentionally unread until we plumb it through.
    _payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)
    work_ms = env_int("JOB_WORK_MS", 1)

    c = client()
    await migrate_with_retry(c)
    queue = "awa_python_longhorizon_bench"
    # rsplit would capture query params for URLs like postgres://.../db?sslmode=disable.
    # urlparse handles that cleanly and is robust against user:pass@host:port forms.
    db_name = (urlparse(database_url()).path or "/").lstrip("/") or "awa_python_bench"

    _emit(
        {
            "kind": "descriptor",
            "system": "awa-python",
            "event_tables": [
                "awa.jobs_hot",
                "awa.scheduled_jobs",
                "awa.jobs_dlq",
                "awa.job_unique_claims",
            ],
            "extensions": [],
            "version": "0.1.0",
            "schema_version": os.environ.get("AWA_SCHEMA_VERSION", "current"),
            "db_name": db_name,
            "started_at": _now_iso(),
        }
    )

    # Bounded ring for claim latency (30s @ producer_rate = ~24k, capped to 32k)
    latencies_ms: deque[tuple[float, float]] = deque(maxlen=32_768)
    enqueued = 0
    completed = 0
    queue_depth = 0
    current_producer_target_rate = float(producer_rate)

    @c.task(BenchJob, queue=queue)
    async def handle(job: awa.Job[BenchJob]) -> None:
        nonlocal completed
        now = time.monotonic()
        # `job.created_at` is a UTC datetime in the awa-python bridge.
        try:
            created = job.created_at
            now_wall = datetime.datetime.now(datetime.timezone.utc)
            latency_ms = max(0.0, (now_wall - created).total_seconds() * 1000.0)
        except Exception:
            latency_ms = 0.0
        latencies_ms.append((now, latency_ms))
        if work_ms:
            await asyncio.sleep(work_ms / 1000.0)
        completed += 1

    await c.start([(queue, worker_count)], poll_interval_ms=50)

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
            # Fire-and-forget insert; avoid blocking the producer loop on DB stalls.
            try:
                await c.insert(BenchJob(seq=seq), queue=queue)
                enqueued += 1
                seq += 1
            except Exception as exc:
                print(f"[awa-python] producer insert failed: {exc}", file=sys.stderr)

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
                tx = await c.transaction()
                try:
                    row = await tx.fetch_one(
                        "SELECT count(*)::bigint AS cnt FROM awa.jobs_hot "
                        "WHERE queue = $1 AND state = 'available'",
                        queue,
                    )
                finally:
                    await tx.rollback()
                queue_depth = int(row["cnt"])
            except Exception:
                pass
            await asyncio.sleep(1.0)

    async def sampler() -> None:
        # Align to wall-clock boundary so cross-system timebases match.
        now_epoch = int(time.time())
        sleep_for = sample_every_s - (now_epoch % sample_every_s)
        await asyncio.sleep(sleep_for)
        last_enqueued = enqueued
        last_completed = completed
        last_tick = loop.time()
        while not shutdown.is_set():
            tick_deadline = loop.time() + sample_every_s
            while not shutdown.is_set() and loop.time() < tick_deadline:
                await asyncio.sleep(min(0.5, tick_deadline - loop.time()))
            if shutdown.is_set():
                break
            dt = max(0.001, loop.time() - last_tick)
            last_tick = loop.time()
            enq_rate = (enqueued - last_enqueued) / dt
            cmp_rate = (completed - last_completed) / dt
            last_enqueued = enqueued
            last_completed = completed
            p50, p95, p99 = _percentiles(latencies_ms, window_s=30.0, now=loop.time())
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
                        "system": "awa-python",
                        "kind": "adapter",
                        "subject_kind": "adapter",
                        "subject": "",
                        "metric": metric,
                        "value": value,
                        "window_s": window_s,
                    }
                )

    tasks: list[asyncio.Task[None]] = []
    try:
        tasks = [
            asyncio.create_task(producer(), name="producer"),
            asyncio.create_task(depth_poller(), name="depth"),
            asyncio.create_task(sampler(), name="sampler"),
        ]
        await shutdown.wait()
    finally:
        shutdown.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await asyncio.wait_for(c.shutdown(timeout_ms=2000), timeout=5.0)
        except Exception as exc:
            print(
                f"[awa-python] shutdown did not complete cleanly: {exc}",
                file=sys.stderr,
            )
        try:
            await asyncio.wait_for(c.close(), timeout=2.0)
        except Exception as exc:
            print(
                f"[awa-python] close did not complete cleanly: {exc}", file=sys.stderr
            )
        print("[awa-python] long_horizon: shutdown signal received", file=sys.stderr)


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


def _percentiles(
    events: "deque[tuple[float, float]]",
    *,
    window_s: float,
    now: float,
) -> tuple[float, float, float]:
    # Filter to the window; keep the deque untouched (it's append-only elsewhere).
    cutoff = now - window_s
    values = [v for t, v in events if t >= cutoff]
    if not values:
        return 0.0, 0.0, 0.0
    values.sort()
    n = len(values)

    def q(p: float) -> float:
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return values[idx]

    return q(0.50), q(0.95), q(0.99)


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
    if scenario == "long_horizon":
        await scenario_long_horizon()
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

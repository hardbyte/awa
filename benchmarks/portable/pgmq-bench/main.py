#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import collections
import datetime as _dt
import json
import os
import signal
import time

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

QUEUE_NAME = "long_horizon_bench"
PGMQ_PG_IMAGE = "ghcr.io/pgmq/pg18-pgmq:v1.10.0"
PGMQ_UPSTREAM_VERSION = "v1.10.0"


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
    record.setdefault("instance_id", _instance_id())
    print(json.dumps(record), flush=True)


def _now_iso() -> str:
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

    def q(p: float) -> float:
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return float(values[idx])

    return q(0.50), q(0.95), q(0.99)


async def aconnect() -> psycopg.AsyncConnection:
    conn = await psycopg.AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


async def setup_queue(conn: psycopg.AsyncConnection) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT to_regclass(%s) AS reg", (f"pgmq.q_{QUEUE_NAME}",))
        row = await cur.fetchone()
        if row["reg"] is not None:
            await cur.execute("SELECT pgmq.drop_queue(%s)", (QUEUE_NAME,))
        await cur.execute("SELECT pgmq.create(%s)", (QUEUE_NAME,))


async def extension_version(conn: psycopg.AsyncConnection) -> str | None:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'pgmq'"
        )
        row = await cur.fetchone()
    return row["extversion"] if row else None


async def queue_depth(conn: psycopg.AsyncConnection) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT count(*)::bigint AS cnt
            FROM pgmq.q_{QUEUE_NAME}
            WHERE vt <= now()
            """
        )
        row = await cur.fetchone()
    return int(row["cnt"])


async def scenario_long_horizon() -> None:
    sample_every_s = env_int("SAMPLE_EVERY_S", 5)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)
    work_ms = env_int("JOB_WORK_MS", 1)
    producer_batch_ms = env_int("PRODUCER_BATCH_MS", 10)
    producer_batch_max = env_int("PRODUCER_BATCH_MAX", 128)
    poll_interval_ms = env_int("POLL_INTERVAL_MS", 50)
    visibility_timeout_s = env_int("VISIBILITY_TIMEOUT_S", 30)
    consumer_batch_size = env_int(
        "CONSUMER_BATCH_SIZE",
        max(1, min(64, round((producer_rate * 1.25) / max(worker_count * (1000 / max(poll_interval_ms, 1)), 1)))),
    )

    db_name = database_url().rsplit("/", 1)[-1]
    setup_conn = await aconnect()
    try:
        await setup_queue(setup_conn)
        extversion = await extension_version(setup_conn)
    finally:
        await setup_conn.close()

    _emit(
        {
            "kind": "descriptor",
            "system": "pgmq",
            "event_tables": [
                f"pgmq.q_{QUEUE_NAME}",
                f"pgmq.a_{QUEUE_NAME}",
            ],
            "extensions": ["pgmq"],
            "version": f"pgmq extension via {PGMQ_PG_IMAGE}",
            "schema_version": extversion,
            "db_name": db_name,
            "started_at": _now_iso(),
        }
    )

    producer_latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    subscriber_latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    end_to_end_latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    enqueued = 0
    completed = 0
    current_queue_depth = 0
    current_producer_target_rate = float(producer_rate)
    payload_padding = "x" * max(0, payload_bytes - 96)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_a: shutdown.set())

    producer_conn = await aconnect()
    depth_conn = await aconnect()

    async def producer() -> None:
        nonlocal enqueued, current_producer_target_rate, current_queue_depth
        seq = 0
        next_t = loop.time()
        while not shutdown.is_set():
            target_rate = read_producer_rate(producer_rate)
            current_producer_target_rate = float(target_rate)

            if producer_mode == "depth-target":
                current_queue_depth = await queue_depth(producer_conn)
                remaining = max(0, target_depth - current_queue_depth)
                batch_count = min(producer_batch_max, remaining)
                if batch_count <= 0:
                    await asyncio.sleep(producer_batch_ms / 1000.0)
                    continue
            else:
                credit = max(0.0, (loop.time() - next_t) * target_rate + 1.0)
                batch_count = max(1, min(producer_batch_max, int(credit)))

            batch = []
            for _ in range(batch_count):
                seq += 1
                batch.append(
                    Jsonb(
                        {
                            "seq": seq,
                            "enqueued_at_ms": int(time.time() * 1000),
                            "payload_padding": payload_padding,
                        }
                    )
                )

            started = time.monotonic()
            async with producer_conn.cursor() as cur:
                await cur.execute(
                    "SELECT * FROM pgmq.send_batch(%s, %s)",
                    (QUEUE_NAME, batch),
                )
                await cur.fetchall()
            elapsed_ms = (time.monotonic() - started) * 1000
            sample_ts = loop.time()
            per_msg_ms = elapsed_ms / max(len(batch), 1)
            for _ in batch:
                producer_latencies_ms.append((sample_ts, per_msg_ms))
            enqueued += len(batch)

            if producer_mode == "fixed":
                next_t += len(batch) / max(target_rate, 1)
                await asyncio.sleep(max(0.0, min(producer_batch_ms / 1000.0, next_t - loop.time())))

    async def consumer_task() -> None:
        nonlocal completed
        conn = await aconnect()
        try:
            while not shutdown.is_set():
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT * FROM pgmq.read(queue_name => %s, vt => %s, qty => %s)",
                        (QUEUE_NAME, visibility_timeout_s, consumer_batch_size),
                    )
                    rows = await cur.fetchall()
                if not rows:
                    await asyncio.sleep(poll_interval_ms / 1000.0)
                    continue

                started_at_ms = int(time.time() * 1000)
                for row in rows:
                    message = row["message"] or {}
                    if isinstance(message, dict) and "enqueued_at_ms" in message:
                        subscriber_latencies_ms.append(
                            (loop.time(), float(started_at_ms - int(message["enqueued_at_ms"])))
                        )

                if work_ms > 0:
                    await asyncio.sleep((work_ms * len(rows)) / 1000.0)

                msg_ids = [int(row["msg_id"]) for row in rows]
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT pgmq.archive(%s, %s)",
                        (QUEUE_NAME, msg_ids),
                    )
                    await cur.fetchall()

                completed_at_ms = int(time.time() * 1000)
                for row in rows:
                    message = row["message"] or {}
                    if isinstance(message, dict) and "enqueued_at_ms" in message:
                        end_to_end_latencies_ms.append(
                            (loop.time(), float(completed_at_ms - int(message["enqueued_at_ms"])))
                        )
                completed += len(rows)
        finally:
            await conn.close()

    async def depth_task() -> None:
        nonlocal current_queue_depth
        if not _observer_enabled():
            # Non-zero replicas don't emit observer metrics; idle this
            # task instead of polling. Saves N-1 connections of polling
            # work on a multi-replica run.
            while not shutdown.is_set():
                await asyncio.sleep(0.25)
            return
        while not shutdown.is_set():
            current_queue_depth = await queue_depth(depth_conn)
            await asyncio.sleep(0.25)

    async def sampler() -> None:
        await asyncio.sleep(
            sample_every_s - ((_dt.datetime.now(_dt.timezone.utc).timestamp()) % sample_every_s)
        )
        last_enqueued = enqueued
        last_completed = completed
        while not shutdown.is_set():
            now = loop.time()
            p50, p95, p99 = _percentiles(
                producer_latencies_ms, window_s=30, now=now
            )
            s50, s95, s99 = _percentiles(
                subscriber_latencies_ms, window_s=30, now=now
            )
            e50, e95, e99 = _percentiles(
                end_to_end_latencies_ms, window_s=30, now=now
            )
            enqueue_rate = (enqueued - last_enqueued) / max(sample_every_s, 1)
            completion_rate = (completed - last_completed) / max(sample_every_s, 1)
            last_enqueued = enqueued
            last_completed = completed
            ts = _now_iso()

            for metric, value, window in (
                ("producer_p50_ms", p50, 30),
                ("producer_p95_ms", p95, 30),
                ("producer_p99_ms", p99, 30),
                ("subscriber_p50_ms", s50, 30),
                ("subscriber_p95_ms", s95, 30),
                ("subscriber_p99_ms", s99, 30),
                ("claim_p50_ms", s50, 30),
                ("claim_p95_ms", s95, 30),
                ("claim_p99_ms", s99, 30),
                ("end_to_end_p50_ms", e50, 30),
                ("end_to_end_p95_ms", e95, 30),
                ("end_to_end_p99_ms", e99, 30),
                ("enqueue_rate", enqueue_rate, sample_every_s),
                ("completion_rate", completion_rate, sample_every_s),
                ("queue_depth", current_queue_depth, 0),
                ("producer_target_rate", current_producer_target_rate, 0),
            ):
                if metric in _OBSERVER_METRICS and not _observer_enabled():
                    continue
                _emit(
                    {
                        "t": ts,
                        "system": "pgmq",
                        "kind": "adapter",
                        "subject_kind": "adapter",
                        "subject": "",
                        "metric": metric,
                        "value": value,
                        "window_s": window,
                    }
                )
            await asyncio.sleep(sample_every_s)

    tasks = [
        asyncio.create_task(producer()),
        asyncio.create_task(depth_task()),
        asyncio.create_task(sampler()),
    ]
    tasks.extend(asyncio.create_task(consumer_task()) for _ in range(worker_count))

    await shutdown.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await producer_conn.close()
    await depth_conn.close()


async def main() -> None:
    scenario = env_str("SCENARIO", "long_horizon")
    if scenario != "long_horizon":
        raise RuntimeError(f"Unsupported scenario {scenario!r} for pgmq-bench")
    await scenario_long_horizon()


if __name__ == "__main__":
    asyncio.run(main())

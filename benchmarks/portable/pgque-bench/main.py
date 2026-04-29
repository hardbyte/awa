#!/usr/bin/env python3
"""Long-horizon benchmark adapter for pgque (NikolayS/pgque).

Contract: benchmarks/portable/CONTRIBUTING_ADAPTERS.md

pgque is pure SQL+PL/pgSQL, installed via ``\\i sql/pgque.sql`` (vendored as a
git submodule under ``vendor/pgque``). This adapter:

* loads pgque on startup (idempotent), creates the bench queue + consumer,
  tightens ticker config so latency is comparable to other adapters
* drives ``pgque.ticker(queue)`` and ``pgque.maint()`` in background tasks
  (no pg_cron in our test image)
* enqueues with ``pgque.send`` from a producer task honouring fixed-rate or
  depth-target modes
* consumes via a single consumer name (PgQ semantics) that loops
  receive -> parallel-process-batch -> ack; intra-batch parallelism uses
  ``WORKER_COUNT`` as a semaphore
* samples queue depth from ``pgque.get_consumer_info`` and emits the required
  long-horizon metrics every ``SAMPLE_EVERY_S``
* exits cleanly on SIGTERM in <= 5s.
"""

from __future__ import annotations

import asyncio
import collections
import datetime as _dt
import json
import os
import signal
import sys
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ADAPTER_DIR = Path(__file__).resolve().parent
PGQUE_SQL = ADAPTER_DIR / "vendor" / "pgque" / "sql" / "pgque.sql"

QUEUE_NAME = "long_horizon_bench"
CONSUMER_NAME = "bench_consumer"


# ─── env helpers ─────────────────────────────────────────────────────────


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


# ─── output helpers ──────────────────────────────────────────────────────


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


# ─── pgque install / setup ───────────────────────────────────────────────


async def aconnect() -> psycopg.AsyncConnection:
    conn = await psycopg.AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


def install_pgque_sync() -> None:
    """Run ``pgque.sql`` (idempotent) using a sync connection — psycopg
    handles the multi-statement script naturally with ``execute()``."""
    if not PGQUE_SQL.exists():
        # The vendored pgque SQL lives in a git submodule
        # (`benchmarks/portable/pgque-bench/vendor/pgque`) that has to
        # be initialised before this adapter runs. Surface a clear
        # diagnostic instead of the raw FileNotFoundError so a fresh
        # checkout doesn't leave operators chasing a path that doesn't
        # mean anything to them yet.
        raise RuntimeError(
            f"pgque vendor SQL not found at {PGQUE_SQL}. The pgque adapter "
            "uses a git submodule for the upstream vendor SQL; run "
            "`git submodule update --init --recursive` from the repo root "
            "before launching `long_horizon.py run --systems pgque,...`."
        )
    sql = PGQUE_SQL.read_text()
    with psycopg.connect(database_url(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            print(f"[pgque] installed pgque from {PGQUE_SQL}", file=sys.stderr)


async def setup_queue(conn: psycopg.AsyncConnection) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT pgque.create_queue(%s)", (QUEUE_NAME,))
        await cur.execute("SELECT pgque.subscribe(%s, %s)", (QUEUE_NAME, CONSUMER_NAME))
        # Tighten ticker so latency is comparable to other adapters. Defaults
        # are seconds-scale (3s lag, 60s idle) which would make this a wall-
        # clock test of the ticker, not the queue.
        for param, value in (
            ("ticker_max_count", "200"),
            ("ticker_max_lag", "100 milliseconds"),
            ("ticker_idle_period", "500 milliseconds"),
        ):
            await cur.execute(
                "SELECT pgque.set_queue_config(%s, %s, %s)",
                (QUEUE_NAME, param, value),
            )


async def discover_event_tables(conn: psycopg.AsyncConnection) -> list[str]:
    """Return the per-queue rotated child tables created by pgque.create_queue
    so the runtime descriptor can be a superset of adapter.json's static
    declarations."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT n.nspname || '.' || c.relname AS qname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'pgque'
              AND c.relkind = 'r'
              AND c.relname LIKE 'event_%'
            ORDER BY 1
            """
        )
        rows = await cur.fetchall()
    return [r["qname"] for r in rows]


# ─── scenario ────────────────────────────────────────────────────────────


async def scenario_long_horizon() -> None:
    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)
    work_ms = env_int("JOB_WORK_MS", 1)

    db_name = database_url().rsplit("/", 1)[-1]

    # Setup connection — used for create_queue/subscribe + descriptor query.
    setup_conn = await aconnect()
    try:
        await setup_queue(setup_conn)
        rotated_tables = await discover_event_tables(setup_conn)
    finally:
        await setup_conn.close()

    # Static manifest event_tables ∪ runtime-discovered child tables.
    static_tables = [
        "pgque.queue",
        "pgque.consumer",
        "pgque.subscription",
        "pgque.tick",
        "pgque.event_template",
        "pgque.retry_queue",
        "pgque.dead_letter",
        "pgque.config",
    ]
    descriptor_tables = sorted(set(static_tables) | set(rotated_tables))
    _emit(
        {
            "kind": "descriptor",
            "system": "pgque",
            "event_tables": descriptor_tables,
            "extensions": [],
            "version": "0.1.0",
            "schema_version": os.environ.get("PGQUE_SCHEMA_VERSION", "alpha3+3b75f58"),
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
    queue_depth = 0
    current_producer_target_rate = float(producer_rate)
    payload_padding = "x" * max(0, payload_bytes - 64)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_a: shutdown.set())

    # Per-task connections (psycopg AsyncConnection isn't safe to share).
    producer_conn = await aconnect()
    consumer_conn = await aconnect()
    ticker_conn = await aconnect()
    maint_conn = await aconnect()
    maint_step2_conn = await aconnect()
    depth_conn = await aconnect()

    work_sem = asyncio.Semaphore(worker_count)

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
            payload = json.dumps(
                {
                    "seq": seq,
                    "created_at": _now_iso(),
                    "padding": payload_padding,
                }
            )
            insert_started = time.monotonic()
            try:
                async with producer_conn.cursor() as cur:
                    await cur.execute(
                        "SELECT pgque.send(%s, %s::text)", (QUEUE_NAME, payload)
                    )
                producer_latencies_ms.append(
                    ((time.monotonic()), max(0.0, (time.monotonic() - insert_started) * 1000.0))
                )
                enqueued += 1
                seq += 1
            except Exception as exc:
                print(f"[pgque] producer send failed: {exc}", file=sys.stderr)
                await asyncio.sleep(0.05)

    async def ticker_task() -> None:
        # No pg_cron in the bench env; we drive the ticker ourselves. 50ms is
        # well below queue_ticker_max_lag (100ms) so we never pace the ticker.
        while not shutdown.is_set():
            try:
                async with ticker_conn.cursor() as cur:
                    await cur.execute("SELECT pgque.ticker(%s)", (QUEUE_NAME,))
            except Exception as exc:
                print(f"[pgque] ticker failed: {exc}", file=sys.stderr)
            await asyncio.sleep(0.05)

    async def maint_task() -> None:
        # Rotation step1, retry, vacuum. Step2 must be a separate transaction.
        while not shutdown.is_set():
            try:
                async with maint_conn.cursor() as cur:
                    await cur.execute("SELECT pgque.maint()")
            except Exception as exc:
                print(f"[pgque] maint failed: {exc}", file=sys.stderr)
            for _ in range(60):  # ~30s, but interruptible
                if shutdown.is_set():
                    return
                await asyncio.sleep(0.5)

    async def maint_step2_task() -> None:
        while not shutdown.is_set():
            try:
                async with maint_step2_conn.cursor() as cur:
                    await cur.execute("SELECT pgque.maint_rotate_tables_step2()")
            except Exception as exc:
                print(f"[pgque] maint_step2 failed: {exc}", file=sys.stderr)
            for _ in range(60):
                if shutdown.is_set():
                    return
                await asyncio.sleep(0.5)

    async def process_one(msg: dict) -> None:
        nonlocal completed
        async with work_sem:
            try:
                ev_time = msg["created_at"]
                if isinstance(ev_time, str):
                    ev_time = _dt.datetime.fromisoformat(ev_time)
                now = _dt.datetime.now(_dt.timezone.utc)
                if ev_time.tzinfo is None:
                    ev_time = ev_time.replace(tzinfo=_dt.timezone.utc)
                subscriber_latency_ms = max(0.0, (now - ev_time).total_seconds() * 1000.0)
            except Exception:
                subscriber_latency_ms = 0.0
            subscriber_latencies_ms.append((time.monotonic(), subscriber_latency_ms))
            if work_ms:
                await asyncio.sleep(work_ms / 1000.0)
            try:
                finished = _dt.datetime.now(_dt.timezone.utc)
                end_to_end_latency_ms = max(
                    0.0, (finished - ev_time).total_seconds() * 1000.0
                )
            except Exception:
                end_to_end_latency_ms = subscriber_latency_ms
            end_to_end_latencies_ms.append((time.monotonic(), end_to_end_latency_ms))
            completed += 1

    async def consumer_task() -> None:
        # Single consumer name; one batch in flight at a time. Intra-batch
        # parallelism is bounded by the semaphore. We LISTEN for ticker
        # notifications but also poll on a short timer so we recover if a
        # NOTIFY is missed (e.g. during reconnects).
        #
        # We drive next_batch + get_batch_events directly (rather than
        # pgque.receive) so we always know the batch_id even for empty
        # batches — PgQ opens a batch on next_batch regardless of event
        # count, and we MUST finish_batch to advance the consumer cursor.
        # Otherwise the consumer wedges on the first empty batch.
        listen_conn = await aconnect()
        try:
            async with listen_conn.cursor() as cur:
                await cur.execute(f'LISTEN "pgque_{QUEUE_NAME}"')
            while not shutdown.is_set():
                try:
                    async with consumer_conn.cursor() as cur:
                        await cur.execute(
                            "SELECT pgque.next_batch(%s, %s) AS batch_id",
                            (QUEUE_NAME, CONSUMER_NAME),
                        )
                        row = await cur.fetchone()
                        batch_id = row["batch_id"] if row else None
                except Exception as exc:
                    print(f"[pgque] next_batch failed: {exc}", file=sys.stderr)
                    await asyncio.sleep(0.1)
                    continue

                if batch_id is None:
                    # No tick boundary yet — wait for NOTIFY or up to 100ms.
                    try:
                        await asyncio.wait_for(_drain_notifies(listen_conn), 0.1)
                    except asyncio.TimeoutError:
                        pass
                    continue

                # Pull events (may be empty — still need to finish the batch).
                try:
                    async with consumer_conn.cursor() as cur:
                        await cur.execute(
                            "SELECT ev_data FROM pgque.get_batch_events(%s)",
                            (batch_id,),
                        )
                        rows = await cur.fetchall()
                except Exception as exc:
                    print(f"[pgque] get_batch_events failed: {exc}", file=sys.stderr)
                    rows = []

                if rows:
                    msgs = []
                    for r in rows:
                        try:
                            msgs.append(json.loads(r["ev_data"]))
                        except Exception:
                            msgs.append({"created_at": _now_iso()})
                    await asyncio.gather(*(process_one(m) for m in msgs))

                try:
                    async with consumer_conn.cursor() as cur:
                        await cur.execute("SELECT pgque.finish_batch(%s)", (batch_id,))
                except Exception as exc:
                    print(f"[pgque] finish_batch failed: {exc}", file=sys.stderr)
        finally:
            await listen_conn.close()

    async def _drain_notifies(listen_conn: psycopg.AsyncConnection) -> None:
        # AsyncConnection.notifies() yields forever; we read just one to
        # signal "wake up". A small sleep keeps us off the hot loop if a
        # NOTIFY storm arrives.
        async for _ in listen_conn.notifies():
            return

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
                async with depth_conn.cursor() as cur:
                    await cur.execute(
                        "SELECT pending_events FROM pgque.get_consumer_info(%s, %s)",
                        (QUEUE_NAME, CONSUMER_NAME),
                    )
                    row = await cur.fetchone()
                    queue_depth = int(row["pending_events"]) if row else 0
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
            producer_p50, producer_p95, producer_p99 = _percentiles(
                producer_latencies_ms, window_s=30.0, now=loop.time()
            )
            subscriber_p50, subscriber_p95, subscriber_p99 = _percentiles(
                subscriber_latencies_ms, window_s=30.0, now=loop.time()
            )
            end_to_end_p50, end_to_end_p95, end_to_end_p99 = _percentiles(
                end_to_end_latencies_ms, window_s=30.0, now=loop.time()
            )
            ts = _now_iso()
            for metric, value, window_s in [
                ("producer_call_p50_ms", producer_p50, 30.0),
                ("producer_call_p95_ms", producer_p95, 30.0),
                ("producer_call_p99_ms", producer_p99, 30.0),
                ("producer_p50_ms", producer_p50, 30.0),
                ("producer_p95_ms", producer_p95, 30.0),
                ("producer_p99_ms", producer_p99, 30.0),
                ("subscriber_p50_ms", subscriber_p50, 30.0),
                ("subscriber_p95_ms", subscriber_p95, 30.0),
                ("subscriber_p99_ms", subscriber_p99, 30.0),
                ("end_to_end_p50_ms", end_to_end_p50, 30.0),
                ("end_to_end_p95_ms", end_to_end_p95, 30.0),
                ("end_to_end_p99_ms", end_to_end_p99, 30.0),
                ("claim_p50_ms", subscriber_p50, 30.0),
                ("claim_p95_ms", subscriber_p95, 30.0),
                ("claim_p99_ms", subscriber_p99, 30.0),
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
                        "system": "pgque",
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
        asyncio.create_task(ticker_task(), name="ticker"),
        asyncio.create_task(maint_task(), name="maint"),
        asyncio.create_task(maint_step2_task(), name="maint_step2"),
        asyncio.create_task(consumer_task(), name="consumer"),
        asyncio.create_task(depth_poller(), name="depth"),
        asyncio.create_task(sampler(), name="sampler"),
    ]
    try:
        await shutdown.wait()
    finally:
        shutdown.set()
        for t in tasks:
            t.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=5.0
            )
        except asyncio.TimeoutError:
            pass
        for c in (
            producer_conn,
            consumer_conn,
            ticker_conn,
            maint_conn,
            maint_step2_conn,
            depth_conn,
        ):
            try:
                await c.close()
            except Exception:
                pass
        print("[pgque] long_horizon: shutdown signal received", file=sys.stderr)


# ─── entrypoint ──────────────────────────────────────────────────────────


async def main() -> None:
    install_pgque_sync()
    scenario = os.environ.get("SCENARIO", "long_horizon")
    if scenario == "migrate_only":
        print("[pgque] migrate_only: pgque.sql installed.", file=sys.stderr)
        return
    if scenario == "long_horizon":
        await scenario_long_horizon()
        return
    raise RuntimeError(f"[pgque] unsupported SCENARIO: {scenario}")


if __name__ == "__main__":
    asyncio.run(main())

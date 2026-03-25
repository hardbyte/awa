from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import quantiles
from typing import Any

import awa

from bench_output import (
    BenchLatency,
    BenchMetrics,
    BenchmarkResult,
    BenchRescue,
    BenchThroughput,
)


DEFAULT_DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class TimingJob:
    seq: int


@dataclass
class FailureJob:
    seq: int
    mode: str


async def scalar(client: awa.AsyncClient, query: str, *args: Any) -> int:
    tx = await client.transaction()
    try:
        row = await tx.fetch_one(query, *args)
    finally:
        await tx.rollback()
    return int(row["cnt"])


async def execute(client: awa.AsyncClient, query: str, *args: Any) -> int:
    tx = await client.transaction()
    try:
        affected = await tx.execute(query, *args)
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise
    return affected


async def state_counts(client: awa.AsyncClient, queue: str) -> dict[str, int]:
    """Query job state distribution for a queue across both tables."""
    counts: dict[str, int] = {}
    for table in ("awa.jobs_hot", "awa.scheduled_jobs"):
        tx = await client.transaction()
        try:
            rows = await tx.fetch_all(
                f"SELECT state::text AS st, count(*)::bigint AS cnt FROM {table} "
                f"WHERE queue = $1 GROUP BY state",
                queue,
            )
        finally:
            await tx.rollback()
        for row in rows:
            state = row["st"]
            counts[state] = counts.get(state, 0) + int(row["cnt"])
    return counts


async def reset_runtime_state(client: awa.AsyncClient) -> None:
    await execute(
        client,
        """
        TRUNCATE awa.jobs_hot, awa.scheduled_jobs, awa.queue_meta, awa.job_unique_claims
        RESTART IDENTITY CASCADE
        """,
    )


def in_flight_jobs(counts: dict[str, int]) -> int:
    return (
        counts.get("available", 0)
        + counts.get("running", 0)
        + counts.get("retryable", 0)
        + counts.get("scheduled", 0)
        + counts.get("waiting_external", 0)
    )


def pctl_ms(samples: list[float], pct: int) -> float:
    if not samples:
        return 0.0
    if len(samples) == 1:
        return samples[0]
    cuts = quantiles(samples, n=100, method="inclusive")
    return cuts[pct - 1]


# ═══════════════════════════════════════════════════════════════════════
# Existing scenarios: copy, hot, scheduled
# ═══════════════════════════════════════════════════════════════════════


async def run_copy_benchmark(
    client: awa.AsyncClient,
    total_jobs: int,
    chunk_size: int,
) -> None:
    queue = "py_bench_copy"
    await reset_runtime_state(client)

    started = asyncio.get_running_loop().time()
    inserted = 0
    for chunk_start in range(0, total_jobs, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_jobs)
        jobs = [TimingJob(seq=i) for i in range(chunk_start, chunk_end)]
        rows = await client.insert_many_copy(jobs, queue=queue)
        inserted += len(rows)
    elapsed = asyncio.get_running_loop().time() - started

    throughput = inserted / elapsed
    print(
        f"[py-copy] total_jobs={inserted} chunk_size={chunk_size} "
        f"elapsed={elapsed:.3f}s throughput={throughput:.0f}/s"
    )

    BenchmarkResult(
        scenario="copy",
        language="python",
        seeded=inserted,
        metrics=BenchMetrics(
            enqueue_per_s=throughput,
            drain_time_s=elapsed,
        ),
        outcomes={"inserted": inserted},
        metadata={"chunk_size": chunk_size},
    ).emit()


async def run_hot_benchmark(
    client: awa.AsyncClient,
    total_jobs: int,
    warmup_secs: int,
    window_secs: int,
    max_workers: int,
    poll_interval_ms: int,
) -> None:
    queue = "py_bench_hot"
    await reset_runtime_state(client)

    await execute(
        client,
        """
        INSERT INTO awa.jobs_hot
            (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'timing_job',
            $1,
            jsonb_build_object('seq', g),
            'available'::awa.job_state,
            2,
            25,
            now(),
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        """,
        queue,
        total_jobs,
    )

    handler_returned = 0

    @client.worker(TimingJob, queue=queue)
    async def handle(job: awa.Job) -> None:
        nonlocal handler_returned
        handler_returned += 1
        return None

    client.start([(queue, max_workers)], poll_interval_ms=poll_interval_ms)
    await asyncio.sleep(warmup_secs)

    handler_before = handler_returned
    completed_before = await scalar(
        client,
        """
        SELECT count(*)::bigint AS cnt
        FROM awa.jobs_hot
        WHERE queue = $1 AND state = 'completed'
        """,
        queue,
    )

    await asyncio.sleep(window_secs)

    completed_after = await scalar(
        client,
        """
        SELECT count(*)::bigint AS cnt
        FROM awa.jobs_hot
        WHERE queue = $1 AND state = 'completed'
        """,
        queue,
    )
    remaining_hot = await scalar(
        client,
        """
        SELECT count(*)::bigint AS cnt
        FROM awa.jobs_hot
        WHERE queue = $1 AND state IN ('available', 'running')
        """,
        queue,
    )

    await client.shutdown(timeout_ms=5000)

    handler_delta = handler_returned - handler_before
    completed_delta = completed_after - completed_before
    handler_per_s = handler_delta / window_secs
    db_per_s = completed_delta / window_secs

    print(
        f"[py-steady-hot] warmup={warmup_secs}s window={window_secs}s "
        f"handler_returned={handler_delta} ({handler_per_s:.0f}/s) "
        f"db_completed_delta={completed_delta} ({db_per_s:.0f}/s) "
        f"remaining_hot={remaining_hot}"
    )

    BenchmarkResult(
        scenario="hot",
        language="python",
        seeded=total_jobs,
        metrics=BenchMetrics(
            throughput=BenchThroughput(
                handler_per_s=handler_per_s,
                db_finalized_per_s=db_per_s,
            ),
            enqueue_per_s=None,
        ),
        outcomes={"completed": completed_delta, "remaining": remaining_hot},
        metadata={
            "warmup_secs": warmup_secs,
            "window_secs": window_secs,
            "max_workers": max_workers,
            "poll_interval_ms": poll_interval_ms,
        },
    ).emit()


async def run_scheduled_benchmark(
    client: awa.AsyncClient,
    total_jobs: int,
    due_rate: int,
    window_secs: int,
    max_workers: int,
    poll_interval_ms: int,
) -> None:
    queue = "py_bench_scheduled"
    await reset_runtime_state(client)

    await execute(
        client,
        """
        INSERT INTO awa.scheduled_jobs
            (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'timing_job',
            $1,
            jsonb_build_object('seq', g),
            'scheduled'::awa.job_state,
            2,
            25,
            now() + interval '365 days' + make_interval(secs => g),
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        """,
        queue,
        total_jobs,
    )

    due_rows = due_rate * window_secs
    handler_returned = 0
    pickup_lateness_ms: list[float] = []
    schedule_started_at: datetime | None = None

    @client.worker(TimingJob, queue=queue)
    async def handle(job: awa.Job) -> None:
        nonlocal handler_returned
        handler_returned += 1

        slot = job.metadata.get("steady_slot")
        if slot is not None and schedule_started_at is not None:
            scheduled_for = schedule_started_at + timedelta(seconds=int(slot))
            lateness_ms = max(
                0.0,
                (datetime.now(timezone.utc) - scheduled_for).total_seconds() * 1000.0,
            )
            pickup_lateness_ms.append(lateness_ms)
        return None

    client.start([(queue, max_workers)], poll_interval_ms=poll_interval_ms)
    await asyncio.sleep(1)

    await execute(
        client,
        """
        WITH target AS (
            SELECT id, row_number() OVER (ORDER BY id) AS rn
            FROM (
                SELECT id
                FROM awa.scheduled_jobs
                WHERE queue = $1
                  AND state = 'scheduled'
                ORDER BY id ASC
                LIMIT $2
            ) picked
        )
        UPDATE awa.scheduled_jobs AS jobs
        SET run_at = now() + make_interval(secs => (((target.rn - 1) / $3) + 1)),
            metadata = jsonb_set(
                COALESCE(jobs.metadata, '{}'::jsonb),
                '{steady_slot}',
                to_jsonb((((target.rn - 1) / $3) + 1)),
                true
            )
        FROM target
        WHERE jobs.id = target.id
        """,
        queue,
        due_rows,
        due_rate,
    )

    schedule_started_at = datetime.now(timezone.utc)
    completed_prev = 0
    completed_by_second: list[tuple[int, int]] = []

    for second in range(1, window_secs + 1):
        await asyncio.sleep(1)
        completed = await scalar(
            client,
            """
            SELECT count(*)::bigint AS cnt
            FROM awa.jobs_hot
            WHERE queue = $1 AND state = 'completed'
            """,
            queue,
        )
        completed_by_second.append((second, completed - completed_prev))
        completed_prev = completed

    deadline = asyncio.get_running_loop().time() + window_secs + 10
    while handler_returned < due_rows and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.2)

    completed_total = await scalar(
        client,
        """
        SELECT count(*)::bigint AS cnt
        FROM awa.jobs_hot
        WHERE queue = $1 AND state = 'completed'
        """,
        queue,
    )
    scheduled_remaining = await scalar(
        client,
        """
        SELECT count(*)::bigint AS cnt
        FROM awa.scheduled_jobs
        WHERE queue = $1 AND state = 'scheduled'
        """,
        queue,
    )

    await client.shutdown(timeout_ms=5000)

    print(
        f"[py-steady-scheduled] seeded={total_jobs} due_rate={due_rate}/s "
        f"window={window_secs}s picked_total={handler_returned} "
        f"completed_total={completed_total} scheduled_remaining={scheduled_remaining}"
    )
    print(
        "[py-steady-scheduled] per-second completions="
        + ", ".join(f"{second}:{count}" for second, count in completed_by_second)
    )
    if pickup_lateness_ms:
        print(
            "[py-steady-scheduled] pickup_lateness_ms "
            f"p50={pctl_ms(pickup_lateness_ms, 50):.0f} "
            f"p95={pctl_ms(pickup_lateness_ms, 95):.0f} "
            f"p99={pctl_ms(pickup_lateness_ms, 99):.0f}"
        )

    latency = None
    if pickup_lateness_ms:
        latency = BenchLatency(
            p50=pctl_ms(pickup_lateness_ms, 50),
            p95=pctl_ms(pickup_lateness_ms, 95),
            p99=pctl_ms(pickup_lateness_ms, 99),
        )

    BenchmarkResult(
        scenario="scheduled",
        language="python",
        seeded=total_jobs,
        metrics=BenchMetrics(
            throughput=BenchThroughput(
                handler_per_s=handler_returned / window_secs if window_secs else 0,
                db_finalized_per_s=completed_total / window_secs if window_secs else 0,
            ),
            enqueue_per_s=None,
            latency_ms=latency,
        ),
        outcomes={
            "completed": completed_total,
            "scheduled_remaining": scheduled_remaining,
        },
        metadata={"due_rate": due_rate, "window_secs": window_secs},
    ).emit()


# ═══════════════════════════════════════════════════════════════════════
# Worker concurrency sweep
# ═══════════════════════════════════════════════════════════════════════


async def run_worker_sweep(
    database_url: str,
    total_jobs: int,
    worker_counts: list[int],
    poll_interval_ms: int,
) -> None:
    """Measure throughput at different worker concurrency levels."""
    for worker_count in worker_counts:
        client = awa.AsyncClient(
            database_url,
            max_connections=max(worker_count + 10, 50),
        )
        await client.migrate()
        queue = f"py_bench_sweep_{worker_count}"
        await reset_runtime_state(client)

        await execute(
            client,
            """
            INSERT INTO awa.jobs_hot
                (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
            SELECT
                'timing_job', $1, jsonb_build_object('seq', g),
                'available'::awa.job_state, 2, 25, now(), '{}'::jsonb, '{}'::text[]
            FROM generate_series(1, $2) AS g
            """,
            queue,
            total_jobs,
        )

        handler_returned = 0

        @client.worker(TimingJob, queue=queue)
        async def handle(job: awa.Job) -> None:
            nonlocal handler_returned
            handler_returned += 1
            return None

        client.start([(queue, worker_count)], poll_interval_ms=poll_interval_ms)
        await asyncio.sleep(2)  # warmup

        handler_before = handler_returned
        completed_before = await scalar(
            client,
            "SELECT count(*)::bigint AS cnt FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
            queue,
        )
        await asyncio.sleep(10)  # measurement window

        completed_after = await scalar(
            client,
            "SELECT count(*)::bigint AS cnt FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
            queue,
        )
        await client.shutdown(timeout_ms=5000)

        handler_delta = handler_returned - handler_before
        completed_delta = completed_after - completed_before
        handler_per_s = handler_delta / 10
        db_per_s = completed_delta / 10

        print(
            f"[py-sweep] workers={worker_count} "
            f"handler={handler_per_s:.0f}/s db={db_per_s:.0f}/s"
        )

        BenchmarkResult(
            scenario=f"sweep_{worker_count}w",
            language="python",
            seeded=total_jobs,
            metrics=BenchMetrics(
                throughput=BenchThroughput(
                    handler_per_s=handler_per_s,
                    db_finalized_per_s=db_per_s,
                ),
            ),
            outcomes={"completed": completed_delta},
            metadata={"workers": worker_count, "window_secs": 10},
        ).emit()


# ═══════════════════════════════════════════════════════════════════════
# Scheduling latency jitter
# ═══════════════════════════════════════════════════════════════════════


async def run_latency_jitter(
    client: awa.AsyncClient,
    total_due: int,
    spread_secs: int,
    max_workers: int,
    poll_interval_ms: int,
) -> None:
    """Measure pickup latency jitter for scheduled jobs becoming due."""
    queue = "py_bench_jitter"
    await reset_runtime_state(client)

    # Seed jobs that become due uniformly over spread_secs
    await execute(
        client,
        """
        INSERT INTO awa.scheduled_jobs
            (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'timing_job', $1, jsonb_build_object('seq', g),
            'scheduled'::awa.job_state, 2, 25,
            now() + make_interval(secs => (g::double precision / $2) * $3),
            jsonb_build_object('due_offset_ms', ((g::double precision / $2) * $3 * 1000)::bigint),
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        """,
        queue,
        total_due,
        spread_secs,
    )

    pickup_lateness_ms: list[float] = []
    start_time = datetime.now(timezone.utc)

    @client.worker(TimingJob, queue=queue)
    async def handle(job: awa.Job) -> None:
        due_offset_ms = job.metadata.get("due_offset_ms")
        if due_offset_ms is not None:
            scheduled_for = start_time + timedelta(milliseconds=int(due_offset_ms))
            lateness = max(
                0.0,
                (datetime.now(timezone.utc) - scheduled_for).total_seconds() * 1000.0,
            )
            pickup_lateness_ms.append(lateness)
        return None

    client.start([(queue, max_workers)], poll_interval_ms=poll_interval_ms)

    # Wait for all jobs to be processed
    deadline = asyncio.get_running_loop().time() + spread_secs + 30
    while len(pickup_lateness_ms) < total_due and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.5)

    await client.shutdown(timeout_ms=5000)

    completed = len(pickup_lateness_ms)
    latency = None
    if pickup_lateness_ms:
        latency = BenchLatency(
            p50=pctl_ms(pickup_lateness_ms, 50),
            p95=pctl_ms(pickup_lateness_ms, 95),
            p99=pctl_ms(pickup_lateness_ms, 99),
        )
        print(
            f"[py-jitter] total_due={total_due} spread={spread_secs}s completed={completed} "
            f"p50={latency.p50:.0f}ms p95={latency.p95:.0f}ms p99={latency.p99:.0f}ms"
        )

    BenchmarkResult(
        scenario="latency_jitter",
        language="python",
        seeded=total_due,
        metrics=BenchMetrics(
            throughput=BenchThroughput(
                handler_per_s=completed / spread_secs if spread_secs else 0,
                db_finalized_per_s=completed / spread_secs if spread_secs else 0,
            ),
            latency_ms=latency,
        ),
        outcomes={"completed": completed, "missed": total_due - completed},
        metadata={
            "spread_secs": spread_secs,
            "max_workers": max_workers,
        },
    ).emit()


# ═══════════════════════════════════════════════════════════════════════
# Stale heartbeat rescue
# ═══════════════════════════════════════════════════════════════════════


async def run_heartbeat_rescue(
    client: awa.AsyncClient,
    total_jobs: int,
    max_workers: int,
    poll_interval_ms: int,
) -> None:
    """Measure rescue + re-processing of jobs stuck in running state."""
    queue = "py_bench_rescue"
    await reset_runtime_state(client)

    # Seed jobs directly in running state with expired heartbeat
    await execute(
        client,
        """
        INSERT INTO awa.jobs_hot
            (kind, queue, args, state, priority, max_attempts, attempt,
             run_at, metadata, tags, heartbeat_at, deadline_at)
        SELECT
            'timing_job', $1, jsonb_build_object('seq', g),
            'running'::awa.job_state, 2, 25, 1,
            now() - interval '10 seconds', '{}'::jsonb, '{}'::text[],
            now() - interval '60 seconds',
            now() - interval '5 seconds'
        FROM generate_series(1, $2) AS g
        """,
        queue,
        total_jobs,
    )

    handler_returned = 0

    @client.worker(TimingJob, queue=queue)
    async def handle(job: awa.Job) -> None:
        nonlocal handler_returned
        handler_returned += 1
        return None

    started = asyncio.get_running_loop().time()
    client.start(
        [(queue, max_workers)],
        poll_interval_ms=poll_interval_ms,
        heartbeat_interval_ms=50,
        deadline_rescue_interval_ms=100,
    )

    deadline = asyncio.get_running_loop().time() + 30
    while handler_returned < total_jobs and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.5)

    drain_time = asyncio.get_running_loop().time() - started
    await client.shutdown(timeout_ms=5000)

    final_counts = await state_counts(client, queue)
    completed = final_counts.get("completed", 0)
    handler_per_s = handler_returned / drain_time if drain_time > 0 else 0
    db_per_s = completed / drain_time if drain_time > 0 else 0

    print(
        f"[py-rescue] total={total_jobs} drain={drain_time:.2f}s "
        f"handler={handler_per_s:.0f}/s rescued_and_completed={completed}"
    )

    BenchmarkResult(
        scenario="heartbeat_rescue",
        language="python",
        seeded=total_jobs,
        metrics=BenchMetrics(
            throughput=BenchThroughput(
                handler_per_s=handler_per_s,
                db_finalized_per_s=db_per_s,
            ),
            drain_time_s=drain_time,
            rescue=BenchRescue(deadline_rescued=total_jobs),
        ),
        outcomes={k: v for k, v in final_counts.items() if v > 0},
        metadata={"max_workers": max_workers},
    ).emit()


# ═══════════════════════════════════════════════════════════════════════
# Mixed failure modes
# ═══════════════════════════════════════════════════════════════════════


async def run_failure_benchmark(
    client: awa.AsyncClient,
    scenario_name: str,
    total_jobs: int,
    failure_pct: int,
    failure_mode: str,
    max_workers: int,
    poll_interval_ms: int,
) -> None:
    """Run a benchmark with a configurable percentage of jobs failing.

    failure_mode is one of: terminal, retryable, callback_timeout, mixed.
    """
    queue = f"py_bench_{scenario_name}"
    await reset_runtime_state(client)

    # Determine mode per job
    success_count = total_jobs - int(total_jobs * failure_pct / 100)
    failure_count = total_jobs - success_count

    # Seed via SQL for speed — use 'failure_job' kind with mode in args.
    # Terminal failures get max_attempts=1 so the first exception is final
    # (Python has no JobError::terminal equivalent — exceptions are retryable).
    # Mixed mode uses Cancel() for terminal sub-jobs, so max_attempts=5 is fine.
    # Retryable and callback modes need max_attempts=5 for retry cycles.
    failure_max_attempts = 1 if failure_mode == "terminal" else 5

    await execute(
        client,
        """
        INSERT INTO awa.jobs_hot
            (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'failure_job',
            $1,
            jsonb_build_object('seq', g, 'mode', 'complete'),
            'available'::awa.job_state,
            2,
            5,
            now(),
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        """,
        queue,
        success_count,
    )
    if failure_count > 0:
        await execute(
            client,
            """
            INSERT INTO awa.jobs_hot
                (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
            SELECT
                'failure_job',
                $1,
                jsonb_build_object('seq', $2 + g, 'mode', $3),
                'available'::awa.job_state,
                2,
                $5::smallint,
                now(),
                '{}'::jsonb,
                '{}'::text[]
            FROM generate_series(1, $4) AS g
            """,
            queue,
            success_count,
            failure_mode,
            failure_count,
            failure_max_attempts,
        )

    handler_returned = 0
    callback_timeouts = 0

    @client.worker(FailureJob, queue=queue)
    async def handle_failure(job: awa.Job) -> Any:
        nonlocal handler_returned, callback_timeouts
        handler_returned += 1
        mode = job.args.mode

        if mode == "complete":
            return None
        elif mode == "terminal":
            # max_attempts=1 ensures first exception is terminal
            raise Exception("intentional terminal failure")
        elif mode == "retryable":
            if job.attempt == 1:
                return awa.RetryAfter(0.05)
            return None
        elif mode == "callback_timeout":
            if job.attempt == 1:
                callback_timeouts += 1
                token = await job.register_callback(timeout_seconds=0.3)
                return awa.WaitForCallback(token)
            return None
        elif mode == "mixed":
            # Rotate through modes based on seq
            sub_mode = ["complete", "terminal", "retryable"][job.args.seq % 3]
            if sub_mode == "complete":
                return None
            elif sub_mode == "terminal":
                return awa.Cancel("intentional mixed terminal failure")
            elif sub_mode == "retryable":
                if job.attempt == 1:
                    return awa.RetryAfter(0.05)
                return None
        return None

    started = asyncio.get_running_loop().time()
    final_counts: dict[str, int] = {}
    drain_time = 0.0
    client.start(
        [(queue, max_workers)],
        poll_interval_ms=poll_interval_ms,
        heartbeat_interval_ms=50,
        deadline_rescue_interval_ms=100,
        callback_rescue_interval_ms=100,
    )
    try:
        # Wait for all jobs to reach a terminal state
        timeout_at = asyncio.get_running_loop().time() + 60
        while asyncio.get_running_loop().time() < timeout_at:
            await asyncio.sleep(0.5)
            final_counts = await state_counts(client, queue)
            if in_flight_jobs(final_counts) == 0:
                break
        drain_time = asyncio.get_running_loop().time() - started
    finally:
        await client.shutdown(timeout_ms=5000)

    timed_out = in_flight_jobs(final_counts) > 0
    if timed_out:
        raise RuntimeError(
            f"Failure benchmark timed out after {drain_time:.2f}s with unfinished jobs: "
            f"{final_counts}"
        )

    completed = final_counts.get("completed", 0)
    failed = final_counts.get("failed", 0)
    cancelled = final_counts.get("cancelled", 0)
    handler_per_s = handler_returned / drain_time if drain_time > 0 else 0
    db_per_s = (completed + failed + cancelled) / drain_time if drain_time > 0 else 0

    print(
        f"[py-{scenario_name}] total={total_jobs} failure_pct={failure_pct}% "
        f"mode={failure_mode} drain_time={drain_time:.2f}s "
        f"handler={handler_per_s:.0f}/s db_finalized={db_per_s:.0f}/s "
        f"completed={completed} failed={failed}"
    )

    rescue = None
    if callback_timeouts > 0:
        rescue = BenchRescue(callback_timeouts=callback_timeouts)

    BenchmarkResult(
        scenario=scenario_name,
        language="python",
        seeded=total_jobs,
        metrics=BenchMetrics(
            throughput=BenchThroughput(
                handler_per_s=handler_per_s,
                db_finalized_per_s=db_per_s,
            ),
            enqueue_per_s=None,
            drain_time_s=drain_time,
            rescue=rescue,
        ),
        outcomes={k: v for k, v in final_counts.items() if v > 0},
        metadata={
            "failure_pct": failure_pct,
            "failure_mode": failure_mode,
            "max_workers": max_workers,
        },
    ).emit()


# ═══════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════


async def make_client(args: argparse.Namespace) -> awa.AsyncClient:
    client = awa.AsyncClient(args.database_url, max_connections=args.max_connections)
    await client.migrate()
    return client


FAILURE_SCENARIOS = [
    ("terminal_1pct", 1, "terminal"),
    ("terminal_10pct", 10, "terminal"),
    ("terminal_50pct", 50, "terminal"),
    ("retryable_1pct", 1, "retryable"),
    ("retryable_10pct", 10, "retryable"),
    ("retryable_50pct", 50, "retryable"),
    ("callback_timeout_10pct", 10, "callback_timeout"),
    ("mixed_50pct", 50, "mixed"),
]


async def async_main(args: argparse.Namespace) -> None:
    if args.scenario in {"copy", "all", "baseline"}:
        client = await make_client(args)
        await run_copy_benchmark(client, args.copy_total_jobs, args.copy_chunk_size)
    if args.scenario in {"hot", "all", "baseline"}:
        client = await make_client(args)
        await run_hot_benchmark(
            client,
            total_jobs=args.hot_total_jobs,
            warmup_secs=args.warmup_secs,
            window_secs=args.window_secs,
            max_workers=args.max_workers,
            poll_interval_ms=args.poll_interval_ms,
        )
    if args.scenario in {"scheduled", "all", "baseline"}:
        client = await make_client(args)
        await run_scheduled_benchmark(
            client,
            total_jobs=args.scheduled_total_jobs,
            due_rate=args.due_rate,
            window_secs=args.window_secs,
            max_workers=args.max_workers,
            poll_interval_ms=args.poll_interval_ms,
        )
    if args.scenario in {"sweep", "workers", "all"}:
        await run_worker_sweep(
            database_url=args.database_url,
            total_jobs=args.hot_total_jobs,
            worker_counts=[1, 4, 16, 64, 256],
            poll_interval_ms=args.poll_interval_ms,
        )
    if args.scenario in {"jitter", "workers", "all"}:
        client = await make_client(args)
        await run_latency_jitter(
            client,
            total_due=args.jitter_total_due,
            spread_secs=args.jitter_spread_secs,
            max_workers=args.max_workers,
            poll_interval_ms=args.poll_interval_ms,
        )
    if args.scenario in {"rescue", "workers", "all"}:
        client = await make_client(args)
        await run_heartbeat_rescue(
            client,
            total_jobs=args.rescue_total_jobs,
            max_workers=args.max_workers,
            poll_interval_ms=args.poll_interval_ms,
        )
    if args.scenario in {"failures", "all"}:
        for scenario_name, failure_pct, failure_mode in FAILURE_SCENARIOS:
            client = await make_client(args)
            await run_failure_benchmark(
                client,
                scenario_name=scenario_name,
                total_jobs=args.failure_total_jobs,
                failure_pct=failure_pct,
                failure_mode=failure_mode,
                max_workers=args.max_workers,
                poll_interval_ms=args.poll_interval_ms,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local Python worker benchmarks for Awa")
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="PostgreSQL connection URL",
    )
    parser.add_argument(
        "--scenario",
        choices=["copy", "hot", "scheduled", "baseline", "sweep", "jitter", "rescue", "workers", "failures", "all"],
        default="all",
    )
    parser.add_argument("--max-connections", type=int, default=50)
    parser.add_argument("--max-workers", type=int, default=256)
    parser.add_argument("--poll-interval-ms", type=int, default=50)
    parser.add_argument("--warmup-secs", type=int, default=2)
    parser.add_argument("--window-secs", type=int, default=10)
    parser.add_argument("--copy-total-jobs", type=int, default=50_000)
    parser.add_argument("--copy-chunk-size", type=int, default=10_000)
    parser.add_argument("--hot-total-jobs", type=int, default=200_000)
    parser.add_argument("--scheduled-total-jobs", type=int, default=2_000_000)
    parser.add_argument("--due-rate", type=int, default=4_000)
    parser.add_argument("--failure-total-jobs", type=int, default=5_000)
    parser.add_argument("--jitter-total-due", type=int, default=2_000)
    parser.add_argument("--jitter-spread-secs", type=int, default=10)
    parser.add_argument("--rescue-total-jobs", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    main()

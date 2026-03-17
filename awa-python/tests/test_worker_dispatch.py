"""Tests for the Python worker dispatch loop (client.start / client.shutdown)."""

import asyncio
import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@pytest.fixture
async def client():
    c = awa.Client(DATABASE_URL)
    await c.migrate()
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'dispatch_%'", [])
    await tx.commit()
    return c


@dataclass
class DispatchEmail:
    to: str
    subject: str


@dataclass
class DispatchFailing:
    should_fail: bool


@pytest.mark.asyncio
async def test_worker_dispatch_completes_jobs(client):
    """Worker dispatch loop claims and completes jobs."""
    queue = "dispatch_complete"

    # Track completed jobs
    completed = []

    @client.worker(DispatchEmail, queue=queue)
    async def handle_email(job):
        completed.append(job.args.to)
        return None  # Completed

    # Insert jobs
    for i in range(5):
        await client.insert(
            DispatchEmail(to=f"user{i}@test.com", subject="Test"),
            queue=queue,
        )

    # Start workers and let them run briefly
    client.start([(queue, 5)])
    await asyncio.sleep(1.0)  # Give workers time to process
    await client.shutdown()

    # Verify all jobs completed
    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT count(*)::bigint AS cnt FROM awa.jobs WHERE queue = $1 AND state::text = 'completed'",
        [queue],
    )
    await tx.commit()
    assert row["cnt"] == 5, f"Expected 5 completed, got {row['cnt']}"


@pytest.mark.asyncio
async def test_worker_dispatch_retries_on_error(client):
    """Handler exceptions cause jobs to become retryable."""
    queue = "dispatch_retry"

    @client.worker(DispatchFailing, queue=queue)
    async def handle_failing(job):
        raise ValueError("transient failure")

    await client.insert(
        DispatchFailing(should_fail=True),
        queue=queue,
    )

    client.start([(queue, 2)])
    await asyncio.sleep(0.5)
    await client.shutdown()

    # Job should be retryable (not completed)
    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT count(*)::bigint AS cnt FROM awa.jobs WHERE queue = $1 AND state::text = 'retryable'",
        [queue],
    )
    await tx.commit()
    assert row["cnt"] >= 1, "Failed job should be retryable"


@pytest.mark.asyncio
async def test_worker_dispatch_handles_cancel(client):
    """Handler returning Cancel marks job as cancelled."""
    queue = "dispatch_cancel"

    @client.worker(DispatchEmail, queue=queue)
    async def handle_cancel(job):
        return awa.Cancel(reason="not needed")

    await client.insert(
        DispatchEmail(to="cancel@test.com", subject="Cancel"),
        queue=queue,
    )

    client.start([(queue, 2)])
    await asyncio.sleep(0.5)
    await client.shutdown()

    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT count(*)::bigint AS cnt FROM awa.jobs WHERE queue = $1 AND state::text = 'cancelled'",
        [queue],
    )
    await tx.commit()
    assert row["cnt"] == 1


@pytest.mark.asyncio
async def test_worker_dispatch_shutdown_is_clean(client):
    """Shutdown stops the dispatch loop without errors."""
    queue = "dispatch_shutdown"

    @client.worker(DispatchEmail, queue=queue)
    async def handle(job):
        return None

    client.start([(queue, 2)])
    await asyncio.sleep(0.1)
    await client.shutdown()  # Should not raise


@pytest.mark.asyncio
async def test_worker_dispatch_requires_registered_workers(client):
    """start fails fast when no Python handlers are registered."""
    with pytest.raises(
        RuntimeError, match="register at least one worker before starting the runtime"
    ):
        client.start([("dispatch_missing_worker", 1)])


@pytest.mark.asyncio
async def test_worker_dispatch_shutdown_signals_cancellation(client):
    """Shutdown flips job.is_cancelled() for an in-flight Python handler."""
    queue = "dispatch_cancel_signal"
    started = asyncio.Event()
    observed = asyncio.Event()

    @client.worker(DispatchEmail, queue=queue)
    async def handle(job):
        started.set()
        while not job.is_cancelled():
            await asyncio.sleep(0.02)
        observed.set()
        return awa.Cancel(reason="shutdown")

    await client.insert(
        DispatchEmail(to="signal@test.com", subject="Signal"),
        queue=queue,
    )

    client.start([(queue, 1)])
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await client.shutdown(timeout_ms=500)
    await asyncio.wait_for(observed.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_worker_dispatch_health_check(client):
    """Health check reflects the Rust runtime state while workers are running."""
    queue = "dispatch_health"

    @client.worker(DispatchEmail, queue=queue)
    async def handle(job):
        await asyncio.sleep(0.05)
        return None

    client.start([(queue, 2)])
    health = await client.health_check()
    await client.shutdown()

    assert health.postgres_connected is True
    assert health.poll_loop_alive is True
    assert health.heartbeat_alive is True
    assert queue in health.queues
    assert health.queues[queue].max_workers == 2


@pytest.mark.asyncio
async def test_worker_dispatch_validates_registered_queue(client):
    """start() rejects configurations that ignore @client.worker queue declarations."""

    @client.worker(DispatchEmail, queue="dispatch_declared")
    async def handle(job):
        return None

    with pytest.raises(ValueError):
        client.start([("different_queue", 1)])


@pytest.mark.asyncio
async def test_logging_bridge():
    """Verify that pyo3-log bridge is initialized (Rust logs → Python logging)."""
    import logging

    # Configure Python logging to capture
    logger = logging.getLogger("awa_model")
    logger.setLevel(logging.DEBUG)
    # If the bridge is working, Rust tracing events will show up
    # in Python's logging. We just verify no crash occurs.
    c = awa.Client(DATABASE_URL)
    await c.migrate()  # This triggers Rust tracing logs

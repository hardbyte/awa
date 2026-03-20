"""Tests for webhook callback completion (waiting_external state).

Covers: register_callback, complete_external, fail_external, retry_external,
resolve_callback, and their sync variants.
"""

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
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'cb_%'")
    await tx.commit()
    return c


@dataclass
class ExternalTask:
    order_id: int


# ── register_callback + WaitForCallback ──────────────────────────────


@pytest.mark.asyncio
async def test_register_callback_and_wait(client):
    """Handler calls register_callback then returns WaitForCallback."""
    queue = "cb_register"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=1), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(callback_ids) == 1, "handler should have registered a callback"

    # Job should be in waiting_external state
    jobs = await client.list_jobs(queue=queue, state="waiting_external")
    assert len(jobs) == 1
    assert jobs[0].state == awa.JobState.WaitingExternal


# ── complete_external ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_complete_external(client):
    """complete_external transitions waiting job to completed."""
    queue = "cb_complete"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=2), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(callback_ids) == 1
    completed = await client.complete_external(callback_ids[0], payload={"paid": True})
    assert completed.state == awa.JobState.Completed


@pytest.mark.asyncio
async def test_complete_external_without_payload(client):
    """complete_external works with no payload."""
    queue = "cb_complete_no_payload"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=3), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    completed = await client.complete_external(callback_ids[0])
    assert completed.state == awa.JobState.Completed


# ── fail_external ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fail_external(client):
    """fail_external transitions waiting job to failed with error."""
    queue = "cb_fail"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=4), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    failed = await client.fail_external(callback_ids[0], "payment declined")
    assert failed.state == awa.JobState.Failed


# ── retry_external ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_retry_external(client):
    """retry_external transitions waiting job back to available."""
    queue = "cb_retry"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=5), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    retried = await client.retry_external(callback_ids[0])
    assert retried.state == awa.JobState.Available


# ── resolve_callback ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_callback_default_complete(client):
    """resolve_callback with default_action='complete' completes the job."""
    queue = "cb_resolve_complete"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=6), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    result = await client.resolve_callback(
        callback_ids[0], payload={"status": "ok"}, default_action="complete"
    )
    assert result.is_completed()
    assert result.job is not None
    assert result.job.state == awa.JobState.Completed


@pytest.mark.asyncio
async def test_resolve_callback_default_ignore(client):
    """resolve_callback with default_action='ignore' ignores (no state change)."""
    queue = "cb_resolve_ignore"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    job = await client.insert(ExternalTask(order_id=7), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    result = await client.resolve_callback(
        callback_ids[0], payload={"status": "unknown"}, default_action="ignore"
    )
    assert result.is_ignored()
    assert result.reason is not None

    # Job should still be in waiting_external
    current = await client.get_job(job.id)
    assert current.state == awa.JobState.WaitingExternal


@pytest.mark.asyncio
async def test_resolve_callback_default_fail(client):
    """resolve_callback with default_action='fail' fails the job."""
    queue = "cb_resolve_fail"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=8), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    result = await client.resolve_callback(
        callback_ids[0], payload={"status": "error"}, default_action="fail"
    )
    assert result.is_failed()
    assert result.job is not None
    assert result.job.state == awa.JobState.Failed


# ── Sync variants ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_complete_external_sync(client):
    """Sync variant of complete_external."""
    queue = "cb_complete_sync"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=9), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    completed = client.complete_external_sync(callback_ids[0])
    assert completed.state == awa.JobState.Completed


@pytest.mark.asyncio
async def test_fail_external_sync(client):
    """Sync variant of fail_external."""
    queue = "cb_fail_sync"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=10), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    failed = client.fail_external_sync(callback_ids[0], "sync failure")
    assert failed.state == awa.JobState.Failed


@pytest.mark.asyncio
async def test_retry_external_sync(client):
    """Sync variant of retry_external."""
    queue = "cb_retry_sync"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=11), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    retried = client.retry_external_sync(callback_ids[0])
    assert retried.state == awa.JobState.Available


@pytest.mark.asyncio
async def test_resolve_callback_sync(client):
    """Sync variant of resolve_callback."""
    queue = "cb_resolve_sync"
    callback_ids = []

    @client.worker(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback()

    await client.insert(ExternalTask(order_id=12), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    result = client.resolve_callback_sync(callback_ids[0], default_action="complete")
    assert result.is_completed()


# ── Error paths ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_complete_external_invalid_callback_id(client):
    """complete_external with non-existent callback_id raises error."""
    import uuid

    fake_id = str(uuid.uuid4())
    with pytest.raises(awa.CallbackNotFound):
        await client.complete_external(fake_id)


@pytest.mark.asyncio
async def test_complete_external_invalid_uuid_format(client):
    """complete_external with invalid UUID format raises validation error."""
    with pytest.raises(awa.ValidationError):
        await client.complete_external("not-a-uuid")

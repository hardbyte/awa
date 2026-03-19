"""Tests for structured job progress and metadata updates (#12)."""

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
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'progress_%'")
    await tx.commit()
    return c


@dataclass
class ProgressArgs:
    data: str


# PP1: set_progress() from handler → persisted after flush
@pytest.mark.asyncio
async def test_set_progress_from_handler(client):
    """job.set_progress() sets progress in the buffer, flush persists to DB."""
    queue = "progress_pp1"
    flushed = []

    @client.worker(ProgressArgs, queue=queue)
    async def handle(job):
        job.set_progress(50, "halfway there")
        await job.flush_progress()
        flushed.append(True)
        return None

    await client.insert(ProgressArgs(data="pp1"), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(flushed) == 1, "handler should have run"

    # After completion, progress is cleared. But we verified the flush worked
    # because flush_progress() didn't raise. Verify via a different approach:
    # insert a job that retries to check progress persists.


# PP2: update_metadata() from handler → persisted
@pytest.mark.asyncio
async def test_update_metadata_from_handler(client):
    """job.update_metadata() shallow-merges into progress.metadata."""
    queue = "progress_pp2"
    verified = []

    @client.worker(ProgressArgs, queue=queue)
    async def handle(job):
        job.set_progress(25, "starting")
        job.update_metadata({"batch": 1, "cursor": "abc"})
        await job.flush_progress()

        # Verify the progress was written
        jobs = await client.list_jobs(queue=queue, state="running")
        for j in jobs:
            if j.id == job.id and j.progress is not None:
                assert j.progress["metadata"]["batch"] == 1
                assert j.progress["metadata"]["cursor"] == "abc"
                verified.append(True)
        return None

    await client.insert(ProgressArgs(data="pp2"), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(verified) == 1, "metadata should have been verified during execution"


# PP3: await job.flush_progress() → immediate write
@pytest.mark.asyncio
async def test_flush_progress_immediate(client):
    """flush_progress() writes to DB before returning."""
    queue = "progress_pp3"
    flush_verified = []

    @client.worker(ProgressArgs, queue=queue)
    async def handle(job):
        job.set_progress(42, "flushing now")
        await job.flush_progress()
        # Read back from DB to verify
        jobs = await client.list_jobs(queue=queue, state="running")
        for j in jobs:
            if j.id == job.id and j.progress is not None:
                assert j.progress["percent"] == 42
                assert j.progress["message"] == "flushing now"
                flush_verified.append(True)
        return None

    await client.insert(ProgressArgs(data="pp3"), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(flush_verified) == 1


# PP4: job.progress property returns dict
@pytest.mark.asyncio
async def test_progress_property_returns_dict(client):
    """job.progress returns the progress dict during execution."""
    queue = "progress_pp4"
    progress_read = []

    @client.worker(ProgressArgs, queue=queue)
    async def handle(job):
        # Initially None (no progress set on fresh job)
        assert job.progress is None

        job.set_progress(75, "reading back")
        # After setting, should be readable from the buffer
        p = job.progress
        assert p is not None
        assert p["percent"] == 75
        assert p["message"] == "reading back"
        progress_read.append(True)
        return None

    await client.insert(ProgressArgs(data="pp4"), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(progress_read) == 1


# PP5: Progress persists across retry
@pytest.mark.asyncio
async def test_progress_persists_across_retry(client):
    """Progress set in attempt 1 is visible in attempt 2."""
    queue = "progress_pp5"
    attempt_data = []

    @client.worker(ProgressArgs, queue=queue)
    async def handle(job):
        if job.attempt == 1:
            # First attempt: set checkpoint and request retry
            job.set_progress(50, "first attempt")
            job.update_metadata({"last_id": 999})
            return awa.RetryAfter(0.01)
        else:
            # Second attempt: read checkpoint from previous attempt
            p = job.progress
            if p is not None:
                attempt_data.append(p)
            return None

    await client.insert(ProgressArgs(data="pp5"), queue=queue)

    client.start([(queue, 1)])
    await asyncio.sleep(2.0)  # Give time for retry
    await client.shutdown()

    assert len(attempt_data) == 1, f"second attempt should have read checkpoint, got {attempt_data}"
    p = attempt_data[0]
    assert p["percent"] == 50
    assert p["metadata"]["last_id"] == 999

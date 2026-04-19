"""Tests for Job properties: timestamps, progress, metadata, and field accuracy.

Covers the full Job property surface that was previously untested:
run_at, created_at, finalized_at, deadline, progress on queried jobs,
and progress lifecycle across state transitions.
"""

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
TIMESTAMP_SKEW_TOLERANCE = timedelta(seconds=1)
RUNTIME_START_KWARGS = {
    "leader_election_interval_ms": 100,
    "queue_storage_queue_rotate_interval_ms": 60_000,
}


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    await c.install_queue_storage(reset=True)
    try:
        yield c
    finally:
        try:
            await c.shutdown()
        except Exception:
            pass
        await c.close()


@dataclass
class PropsJob:
    value: str


# ── Timestamps on insert ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_created_at_set_on_insert(client):
    """created_at is an RFC3339 timestamp set at insert time."""
    before = datetime.now(timezone.utc)
    job = await client.insert(PropsJob(value="ts"), queue="props_created")
    after = datetime.now(timezone.utc)

    assert job.created_at is not None
    created = datetime.fromisoformat(job.created_at)
    assert before - TIMESTAMP_SKEW_TOLERANCE <= created <= after + TIMESTAMP_SKEW_TOLERANCE


@pytest.mark.asyncio
async def test_run_at_defaults_to_now(client):
    """run_at defaults to approximately now for immediately-available jobs."""
    before = datetime.now(timezone.utc)
    job = await client.insert(PropsJob(value="ts"), queue="props_run_at")
    after = datetime.now(timezone.utc)

    run_at = datetime.fromisoformat(job.run_at)
    assert before - TIMESTAMP_SKEW_TOLERANCE <= run_at <= after + TIMESTAMP_SKEW_TOLERANCE


@pytest.mark.asyncio
async def test_run_at_with_future_schedule(client):
    """run_at reflects the scheduled time when inserting with a future run_at."""
    future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    job = await client.insert(
        PropsJob(value="future"),
        queue="props_run_at_future",
        run_at=future.isoformat(),
    )

    run_at = datetime.fromisoformat(job.run_at)
    assert run_at.year == 2099
    assert job.state == awa.JobState.Scheduled


@pytest.mark.asyncio
async def test_finalized_at_none_on_insert(client):
    """finalized_at is None for a freshly inserted job."""
    job = await client.insert(PropsJob(value="ts"), queue="props_finalized")
    assert job.finalized_at is None


@pytest.mark.asyncio
async def test_finalized_at_set_after_cancel(client):
    """finalized_at is set when a job reaches a terminal state."""
    job = await client.insert(PropsJob(value="ts"), queue="props_finalized_cancel")
    cancelled = await client.cancel(job.id)

    assert cancelled.finalized_at is not None
    finalized = datetime.fromisoformat(cancelled.finalized_at)
    created = datetime.fromisoformat(cancelled.created_at)
    assert finalized >= created


@pytest.mark.asyncio
async def test_deadline_none_on_insert(client):
    """deadline is None on a freshly inserted job (set at claim time)."""
    job = await client.insert(PropsJob(value="ts"), queue="props_deadline")
    assert job.deadline is None


@pytest.mark.asyncio
async def test_deadline_set_during_execution(client):
    """deadline is set when a job is claimed and running."""
    queue = "props_deadline_running"
    observed_deadline = []

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        # During execution, the job should have a deadline
        fetched = await client.get_job(job.id)
        observed_deadline.append(fetched.deadline)
        return None

    await client.insert(PropsJob(value="dl"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(observed_deadline) == 1
    assert observed_deadline[0] is not None
    deadline = datetime.fromisoformat(observed_deadline[0])
    assert deadline > datetime.now(timezone.utc)  # deadline should be in the future


# ── Progress on queried jobs ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_progress_none_on_fresh_job(client):
    """Freshly inserted job has no progress."""
    job = await client.insert(PropsJob(value="p"), queue="props_progress_none")
    assert job.progress is None


@pytest.mark.asyncio
async def test_progress_readable_on_retrying_job(client):
    """Progress set during execution is readable via get_job on a retrying job."""
    queue = "props_progress_retry"

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        job.set_progress(65, "two thirds done")
        job.update_metadata({"batch": 42})
        return awa.RetryAfter(600)  # long retry delay

    job = await client.insert(PropsJob(value="p"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert fetched.state == awa.JobState.Retryable
    assert fetched.progress is not None
    assert fetched.progress["percent"] == 65
    assert fetched.progress["message"] == "two thirds done"
    assert fetched.progress["metadata"]["batch"] == 42


@pytest.mark.asyncio
async def test_progress_cleared_on_completed_job(client):
    """Completed jobs have progress cleared to None."""
    queue = "props_progress_complete"

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        job.set_progress(100, "done")
        return None

    job = await client.insert(PropsJob(value="p"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert fetched.state == awa.JobState.Completed
    assert fetched.progress is None


@pytest.mark.asyncio
async def test_progress_preserved_on_failed_job(client):
    """Failed jobs retain their progress for operator inspection."""
    queue = "props_progress_fail"

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        job.set_progress(10, "about to fail")
        raise awa.TerminalError("fatal")

    job = await client.insert(PropsJob(value="p"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert fetched.state == awa.JobState.Failed
    assert fetched.progress is not None
    assert fetched.progress["percent"] == 10


@pytest.mark.asyncio
async def test_progress_preserved_on_cancelled_job(client):
    """Cancelled jobs retain their progress."""
    queue = "props_progress_cancel"

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        job.set_progress(50, "cancelling")
        return awa.Cancel("user request")

    job = await client.insert(PropsJob(value="p"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert fetched.state == awa.JobState.Cancelled
    assert fetched.progress is not None
    assert fetched.progress["percent"] == 50


# ── Progress during execution (handler-side) ──────────────────────────


@pytest.mark.asyncio
async def test_progress_readable_during_execution(client):
    """job.progress reads from the live buffer during handler execution."""
    queue = "props_progress_live"
    observations = []

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        assert job.progress is None  # no prior progress

        job.set_progress(25, "quarter")
        p = job.progress
        assert p is not None
        observations.append(("after_set", p["percent"], p.get("message")))

        job.update_metadata({"step": 1})
        p2 = job.progress
        observations.append(("after_meta", p2.get("metadata", {}).get("step")))

        job.set_progress(75, "three quarters")
        p3 = job.progress
        observations.append(("overwrite", p3["percent"]))

        return None

    await client.insert(PropsJob(value="live"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(observations) == 3
    assert observations[0] == ("after_set", 25, "quarter")
    assert observations[1] == ("after_meta", 1)
    assert observations[2] == ("overwrite", 75)


@pytest.mark.asyncio
async def test_progress_clamped_to_100(client):
    """set_progress clamps percent > 100 to 100."""
    queue = "props_progress_clamp"
    values = []

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        job.set_progress(200, "over")
        values.append(job.progress["percent"])
        return None

    await client.insert(PropsJob(value="clamp"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert values == [100]


# ── Metadata shallow merge ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_metadata_shallow_merge(client):
    """update_metadata does shallow merge: new keys added, existing overwritten."""
    queue = "props_meta_merge"
    results = []

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        job.update_metadata({"a": 1, "b": 2})
        job.update_metadata({"b": 99, "c": 3})  # overwrites b, adds c
        meta = job.progress["metadata"]
        results.append(dict(meta))
        return None

    await client.insert(PropsJob(value="merge"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(results) == 1
    assert results[0] == {"a": 1, "b": 99, "c": 3}


@pytest.mark.asyncio
async def test_update_metadata_rejects_non_dict(client):
    """update_metadata with a non-dict argument raises an error."""
    queue = "props_meta_bad"
    errors = []

    @client.task(PropsJob, queue=queue)
    async def handle(job):
        try:
            job.update_metadata("not a dict")
        except (ValueError, TypeError) as e:
            errors.append(str(e))
        return None

    await client.insert(PropsJob(value="bad"), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(errors) == 1, f"should have caught an error, got {errors}"


# ── get_job ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_job_returns_all_fields(client):
    """get_job returns a Job with all expected properties populated."""
    job = await client.insert(
        PropsJob(value="full"),
        queue="props_get_job",
        priority=1,
        max_attempts=5,
        tags=["test", "props"],
        metadata={"source": "test"},
    )

    fetched = await client.get_job(job.id)
    assert fetched.id == job.id
    assert fetched.kind == "props_job"
    assert fetched.queue == "props_get_job"
    assert fetched.state == awa.JobState.Available
    assert fetched.priority == 1
    assert fetched.attempt == 0
    assert fetched.max_attempts == 5
    assert fetched.tags == ["test", "props"]
    assert fetched.metadata == {"source": "test"}
    assert fetched.args == {"value": "full"}
    assert fetched.run_at is not None
    assert fetched.created_at is not None
    assert fetched.finalized_at is None
    assert fetched.deadline is None
    assert fetched.progress is None


@pytest.mark.asyncio
async def test_get_job_not_found(client):
    """get_job with a non-existent ID raises an error."""
    with pytest.raises(awa.AwaError):
        await client.get_job(999999999)

"""Edge-case callback tests ported from Rust external_wait_test.rs.

Covers scenarios that were previously only tested in Rust:
  - E5:  Callback timeout rescue (retryable + failed paths)
  - E6:  Double completion protection
  - E8:  Admin cancel while waiting_external
  - E9:  Admin retry while waiting_external
  - E10: Drain queue includes waiting_external jobs
  - E11: Race: complete_external during running (before WaitForCallback)
  - E16: Stale callback rejected by run_lease
"""

import asyncio
import os
import uuid as uuid_mod
from dataclasses import dataclass

import pytest

import awa


def _timeout_multiplier() -> float:
    """CI runners are slower and loaded — scale generously so timeouts
    reflect the real budget for the work, not the runner's throughput.
    Matches the pattern used in test_chaos_recovery.py."""
    raw = os.environ.get("AWA_CHAOS_TIMEOUT_MULTIPLIER")
    if raw is not None:
        try:
            return max(float(raw), 1.0)
        except ValueError:
            pass
    return 3.0 if os.environ.get("CI") else 1.0


def _scaled(timeout: float) -> float:
    return timeout * _timeout_multiplier()

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    await c.install_queue_storage(reset=True)
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.queue_meta WHERE queue LIKE 'cbe_%'")
    await tx.commit()
    try:
        yield c
    finally:
        await c.shutdown()
        await c.close()


@dataclass
class ExternalTask:
    order_id: int


async def _wait_for_job_state(
    client, job_id: int, expected: tuple[awa.JobState, ...], timeout: float
):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await client.get_job(job_id)
        if any(job.state == state for state in expected):
            return job
        await asyncio.sleep(0.05)
    job = await client.get_job(job_id)
    raise AssertionError(f"Timed out waiting for job {job_id} to reach {expected}, last={job.state}")


async def _setup_waiting_job(
    client, queue: str, order_id: int, timeout_seconds: float = 3600, **insert_kwargs
):
    """Insert a job, run it through a handler that parks in waiting_external.

    Returns (job, callback_id).
    """
    callback_ids = []

    @client.task(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=timeout_seconds)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    job = await client.insert(
        ExternalTask(order_id=order_id), queue=queue, **insert_kwargs
    )

    await client.start(
        [(queue, 1)],
        poll_interval_ms=50,
        heartbeat_interval_ms=50,
        leader_election_interval_ms=100,
    )
    # Wait for handler to register callback
    deadline = asyncio.get_event_loop().time() + _scaled(5)
    while not callback_ids and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.1)
    assert len(callback_ids) == 1, f"handler should have registered a callback for {queue}"
    waiting_job = await _wait_for_job_state(
        client,
        job.id,
        (awa.JobState.WaitingExternal,),
        _scaled(5),
    )
    await client.shutdown()
    return waiting_job, callback_ids[0]


# ── E5: Callback timeout rescue ──────────────────────────────────────


@pytest.mark.asyncio
async def test_callback_timeout_rescue_retryable(client):
    """Callback timeout with remaining attempts transitions to retryable."""
    queue = "cbe_timeout_retry"
    job, _callback_id = await _setup_waiting_job(
        client, queue, order_id=100, max_attempts=3, timeout_seconds=0.1
    )
    await asyncio.sleep(_scaled(0.3))
    await client.start(
        [(queue, 1)],
        poll_interval_ms=50,
        heartbeat_interval_ms=50,
        callback_rescue_interval_ms=25,
        leader_election_interval_ms=100,
    )
    updated = await _wait_for_job_state(
        client, job.id, (awa.JobState.Retryable,), _scaled(5)
    )
    await client.shutdown()
    assert updated.state == awa.JobState.Retryable


@pytest.mark.asyncio
async def test_callback_timeout_rescue_exhausted(client):
    """Callback timeout with max_attempts exhausted transitions to failed."""
    queue = "cbe_timeout_fail"
    job, _callback_id = await _setup_waiting_job(
        client, queue, order_id=101, max_attempts=1, timeout_seconds=0.1
    )
    await asyncio.sleep(_scaled(0.3))
    await client.start(
        [(queue, 1)],
        poll_interval_ms=50,
        heartbeat_interval_ms=50,
        callback_rescue_interval_ms=25,
        leader_election_interval_ms=100,
    )
    updated = await _wait_for_job_state(client, job.id, (awa.JobState.Failed,), _scaled(5))
    await client.shutdown()
    assert updated.state == awa.JobState.Failed


# ── E6: Double completion protection ─────────────────────────────────


@pytest.mark.asyncio
async def test_double_completion_rejected(client):
    """Second complete_external on same callback raises CallbackNotFound."""
    queue = "cbe_double_complete"
    _, callback_id = await _setup_waiting_job(client, queue, order_id=102)

    # First completion succeeds
    completed = await client.complete_external(callback_id, payload={"ok": True})
    assert completed.state == awa.JobState.Completed

    # Second completion must fail
    with pytest.raises(awa.CallbackNotFound):
        await client.complete_external(callback_id, payload={"ok": True})


# ── E8: Admin cancel while waiting_external ──────────────────────────


@pytest.mark.asyncio
async def test_admin_cancel_waiting_external(client):
    """Admin cancel on a waiting_external job transitions to cancelled."""
    queue = "cbe_admin_cancel"
    job, _ = await _setup_waiting_job(client, queue, order_id=103)

    cancelled = await client.cancel(job.id)
    assert cancelled is not None
    assert cancelled.state == awa.JobState.Cancelled


# ── E9: Admin retry while waiting_external ───────────────────────────


@pytest.mark.asyncio
async def test_admin_retry_waiting_external(client):
    """Admin retry on a waiting_external job transitions to available."""
    queue = "cbe_admin_retry"
    job, _ = await _setup_waiting_job(client, queue, order_id=104)

    retried = await client.retry(job.id)
    assert retried is not None
    assert retried.state == awa.JobState.Available


# ── E10: Drain queue includes waiting_external ───────────────────────


@pytest.mark.asyncio
async def test_drain_queue_cancels_waiting_external(client):
    """drain_queue includes waiting_external jobs."""
    queue = "cbe_drain"
    job, _ = await _setup_waiting_job(client, queue, order_id=105)

    drained = await client.drain_queue(queue)
    assert drained >= 1

    updated = await client.get_job(job.id)
    assert updated.state == awa.JobState.Cancelled


# ── E11: Race: complete during running state ─────────────────────────


@pytest.mark.asyncio
async def test_complete_external_during_running(client):
    """complete_external works even while job is still in running state.

    This tests the race where the external system calls back before the
    executor has transitioned the job to waiting_external. The SQL WHERE
    clause accepts both 'running' and 'waiting_external'.
    """
    queue = "cbe_race_running"
    callback_ids = []
    registered = asyncio.Event()
    release = asyncio.Event()

    @client.task(ExternalTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=3600)
        callback_ids.append(token.id)
        registered.set()
        await release.wait()
        return awa.WaitForCallback(token)

    job = await client.insert(ExternalTask(order_id=106), queue=queue)
    await client.start([(queue, 1)], poll_interval_ms=50, leader_election_interval_ms=100)
    await asyncio.wait_for(registered.wait(), timeout=_scaled(5))
    callback_id = callback_ids[0]

    completed = await client.complete_external(callback_id)
    release.set()
    await client.shutdown()
    assert completed.state == awa.JobState.Completed


# ── E16: Stale callback rejected by run_lease ────────────────────────


@pytest.mark.asyncio
async def test_stale_callback_rejected_after_rescue(client):
    """After a job is rescued and re-claimed, the old callback_id is stale.

    complete_external on the old callback_id raises CallbackNotFound because
    the rescue clears callback fields.
    """
    queue = "cbe_stale_lease"
    callback_ids = []
    attempts = []

    @client.task(ExternalTask, queue=queue)
    async def handle(job):
        attempts.append(job.attempt)
        if job.attempt == 1:
            token = await job.register_callback(timeout_seconds=3600)
            callback_ids.append(token.id)
            return awa.WaitForCallback(token)
        # Second attempt: complete normally
        return None

    job = await client.insert(
        ExternalTask(order_id=107), queue=queue, max_attempts=3
    )

    # Run first attempt → parks in waiting_external
    await client.start([(queue, 1)], poll_interval_ms=50, leader_election_interval_ms=100)
    deadline = asyncio.get_event_loop().time() + _scaled(5)
    while not callback_ids and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.1)
    await client.shutdown()

    assert len(callback_ids) == 1
    old_callback_id = callback_ids[0]

    # Simulate rescue: admin retry moves job back to available
    await client.retry(job.id)

    # Run second attempt → completes normally
    await client.start(
        [(queue, 1)],
        poll_interval_ms=50,
        promote_interval_ms=100,
        leader_election_interval_ms=100,
    )
    deadline = asyncio.get_event_loop().time() + _scaled(5)
    while len(attempts) < 2 and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.1)
    await client.shutdown()

    assert len(attempts) >= 2

    # The old callback_id should now be rejected
    with pytest.raises(awa.CallbackNotFound):
        await client.complete_external(old_callback_id)


# ── Callback timeout with live runtime ───────────────────────────────


@pytest.mark.asyncio
async def test_callback_timeout_rescued_by_runtime(client):
    """A short callback timeout is rescued by the live runtime's maintenance loop.

    This is an end-to-end test: insert a job, register a callback with a
    very short timeout, and let the runtime's maintenance leader rescue it.
    """
    queue = "cbe_timeout_live"
    attempts = []

    @client.task(ExternalTask, queue=queue)
    async def handle(job):
        attempts.append(job.attempt)
        if job.attempt == 1:
            # First attempt: register callback with very short timeout
            token = await job.register_callback(timeout_seconds=1)
            return awa.WaitForCallback(token)
        else:
            # Second attempt (after rescue): complete normally
            return None

    await client.insert(
        ExternalTask(order_id=109), queue=queue, max_attempts=3
    )

    await client.start(
        [(queue, 1)],
        poll_interval_ms=50,
        heartbeat_interval_ms=50,
        callback_rescue_interval_ms=200,
        promote_interval_ms=200,
        leader_election_interval_ms=100,
    )

    # Wait for the job to go through: attempt 1 → timeout → rescue → attempt 2 → complete
    deadline = asyncio.get_event_loop().time() + _scaled(10)
    while len(attempts) < 2 and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.2)
    await client.shutdown()

    assert len(attempts) >= 2, f"Expected 2 attempts, got {attempts}"
    assert attempts[0] == 1
    assert attempts[1] == 2

    # Verify job is completed
    jobs = await client.list_jobs(queue=queue, state="completed")
    assert len(jobs) == 1

"""Tests for sequential callbacks (resume_external, wait_for_callback, heartbeat).

Covers: resume_external (async + sync), wait_for_callback on Job,
heartbeat_callback during sequential flow, error paths.
"""

import asyncio
import os
import uuid
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
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
class SeqTask:
    order_id: int


# ── resume_external ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resume_external_transitions_to_running(client):
    """resume_external transitions waiting_external job back to running."""
    queue = "seq_resume_basic"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    await client.insert(SeqTask(order_id=1), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    assert len(callback_ids) == 1
    resumed = await client.resume_external(
        callback_ids[0], payload={"payment_id": "pay_123"}
    )
    assert resumed.state == awa.JobState.Running


@pytest.mark.asyncio
async def test_resume_external_stores_payload_in_metadata(client):
    """resume_external stores the callback payload in job metadata."""
    queue = "seq_resume_payload"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    job = await client.insert(SeqTask(order_id=2), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    await client.resume_external(
        callback_ids[0], payload={"amount": 42.50, "currency": "NZD"}
    )

    updated = await client.get_job(job.id)
    assert updated.metadata["_awa_callback_result"]["amount"] == 42.50
    assert updated.metadata["_awa_callback_result"]["currency"] == "NZD"


@pytest.mark.asyncio
async def test_resume_then_stale_callback_rejected(client):
    """After resume, the old callback_id is consumed and cannot be reused."""
    queue = "seq_resume_complete"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    job = await client.insert(SeqTask(order_id=3), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    # Resume first callback
    resumed = await client.resume_external(callback_ids[0], payload={"step": "payment"})
    assert resumed.state == awa.JobState.Running

    # The old callback is consumed — resume again should fail
    with pytest.raises(awa.CallbackNotFound):
        await client.resume_external(callback_ids[0])

    # Complete with stale callback should also fail
    with pytest.raises(awa.CallbackNotFound):
        await client.complete_external(callback_ids[0])

    # Job is still running (handler ended, but no new callback registered)
    updated = await client.get_job(job.id)
    assert updated.state == awa.JobState.Running


@pytest.mark.asyncio
async def test_resume_external_without_payload(client):
    """resume_external works with no payload (null stored)."""
    queue = "seq_resume_no_payload"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    job = await client.insert(SeqTask(order_id=4), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    resumed = await client.resume_external(callback_ids[0])
    assert resumed.state == awa.JobState.Running

    updated = await client.get_job(job.id)
    assert (
        updated.metadata.get("_awa_callback_result") is None
    )  # null in JSON = None in Python


# ── resume_external error paths ──────────────────────────────────────


@pytest.mark.asyncio
async def test_resume_external_invalid_callback_id(client):
    """resume_external with non-existent callback_id raises error."""
    fake_id = str(uuid.uuid4())
    with pytest.raises(awa.CallbackNotFound):
        await client.resume_external(fake_id)


@pytest.mark.asyncio
async def test_resume_external_invalid_uuid(client):
    """resume_external with invalid UUID raises validation error."""
    with pytest.raises(awa.ValidationError):
        await client.resume_external("not-a-uuid")


@pytest.mark.asyncio
async def test_resume_after_complete_fails(client):
    """resume_external on an already-completed job fails."""
    queue = "seq_resume_after_complete"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    await client.insert(SeqTask(order_id=5), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    # Complete first
    await client.complete_external(callback_ids[0])

    # Resume should fail
    with pytest.raises(awa.CallbackNotFound):
        await client.resume_external(callback_ids[0])


@pytest.mark.asyncio
async def test_double_resume_fails(client):
    """Only one resume_external succeeds; second gets CallbackNotFound."""
    queue = "seq_double_resume"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    await client.insert(SeqTask(order_id=6), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    # First resume succeeds
    await client.resume_external(callback_ids[0], payload={"first": True})

    # Second resume should fail
    with pytest.raises(awa.CallbackNotFound):
        await client.resume_external(callback_ids[0], payload={"second": True})


# ── resume_external sync variant ─────────────────────────────────────


@pytest.mark.asyncio
async def test_resume_external_sync(client):
    """Sync variant of resume_external."""
    queue = "seq_resume_sync"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    await client.insert(SeqTask(order_id=7), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    resumed = client._raw.resume_external_sync(callback_ids[0], payload={"sync": True})
    assert resumed.state == awa.JobState.Running


# ── heartbeat_callback in sequential flow ────────────────────────────


@pytest.mark.asyncio
async def test_heartbeat_during_wait(client):
    """heartbeat_callback extends timeout while job is waiting_external."""
    queue = "seq_heartbeat"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=10)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    await client.insert(SeqTask(order_id=8), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    # Heartbeat extends the timeout
    updated = await client.heartbeat_callback(callback_ids[0], timeout_seconds=7200)
    assert updated.state == awa.JobState.WaitingExternal

    # Complete after heartbeat
    completed = await client.complete_external(callback_ids[0])
    assert completed.state == awa.JobState.Completed


@pytest.mark.asyncio
async def test_heartbeat_after_resume_fails(client):
    """heartbeat_callback fails after the callback has been resumed."""
    queue = "seq_heartbeat_after_resume"
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    await client.insert(SeqTask(order_id=9), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await asyncio.sleep(1.0)
    await client.shutdown()

    # Resume first
    await client.resume_external(callback_ids[0])

    # Heartbeat on consumed callback should fail
    with pytest.raises(awa.CallbackNotFound):
        await client.heartbeat_callback(callback_ids[0])


@pytest.mark.asyncio
async def test_heartbeat_invalid_timeout_rejected(client):
    """heartbeat_callback rejects negative timeout values."""
    with pytest.raises(awa.ValidationError):
        await client.heartbeat_callback(str(uuid.uuid4()), timeout_seconds=-1.0)


@pytest.mark.asyncio
async def test_heartbeat_invalid_timeout_sync_rejected(client):
    """Sync heartbeat_callback rejects invalid timeout values."""
    sync_client = awa.Client(DATABASE_URL)
    with pytest.raises(awa.ValidationError):
        sync_client.heartbeat_callback(str(uuid.uuid4()), timeout_seconds=float("nan"))


# ── wait_for_callback (in-handler sequential) ────────────────────────


@pytest.mark.asyncio
async def test_wait_for_callback_happy_path(client):
    """Handler uses wait_for_callback, external system resumes it."""
    queue = "seq_wait_happy"
    payloads_received = []
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        payload = await job.wait_for_callback(token)
        payloads_received.append(payload)
        # Handler completes after receiving the payload

    job = await client.insert(SeqTask(order_id=10), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)

    # Wait for the handler to register the callback and enter waiting
    for _ in range(20):
        await asyncio.sleep(0.3)
        if callback_ids:
            break
    assert callback_ids, "handler should have registered a callback"

    # Wait a bit more for the handler to transition to waiting_external
    await asyncio.sleep(0.5)

    # Resume with payload
    await client.resume_external(callback_ids[0], payload={"answer": 42})

    # Wait for the handler to process the payload and complete
    await asyncio.sleep(1.5)
    await client.shutdown()

    assert len(payloads_received) == 1
    assert payloads_received[0]["answer"] == 42

    # Job should be completed
    final_job = await client.get_job(job.id)
    assert final_job.state == awa.JobState.Completed


@pytest.mark.asyncio
async def test_wait_for_callback_two_sequential(client):
    """Handler does two sequential wait_for_callback calls."""
    queue = "seq_wait_two"
    all_payloads = []
    callback_ids = []

    @client.task(SeqTask, queue=queue)
    async def handle(job):
        # First wait
        token1 = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token1.id)
        payload1 = await job.wait_for_callback(token1)
        all_payloads.append(payload1)

        # Second wait
        token2 = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token2.id)
        payload2 = await job.wait_for_callback(token2)
        all_payloads.append(payload2)

    job = await client.insert(SeqTask(order_id=11), queue=queue)

    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)

    # Wait for first callback registration
    for _ in range(20):
        await asyncio.sleep(0.3)
        if callback_ids:
            break
    assert len(callback_ids) >= 1

    await asyncio.sleep(0.5)

    # Resume first callback
    await client.resume_external(
        callback_ids[0], payload={"step": 1, "data": "payment"}
    )

    # Wait for second callback registration
    for _ in range(20):
        await asyncio.sleep(0.3)
        if len(callback_ids) >= 2:
            break
    assert len(callback_ids) >= 2, f"expected 2 callbacks, got {len(callback_ids)}"

    await asyncio.sleep(0.5)

    # Resume second callback
    await client.resume_external(
        callback_ids[1], payload={"step": 2, "data": "shipping"}
    )

    await asyncio.sleep(1.5)
    await client.shutdown()

    assert len(all_payloads) == 2
    assert all_payloads[0]["step"] == 1
    assert all_payloads[0]["data"] == "payment"
    assert all_payloads[1]["step"] == 2
    assert all_payloads[1]["data"] == "shipping"

    final_job = await client.get_job(job.id)
    assert final_job.state == awa.JobState.Completed

"""Tests for handler-staged follow-up jobs (ADR-046)."""

import asyncio
import os
from dataclasses import dataclass

import pytest

import awa
from awa.testing import AwaTestClient, HandlerJob

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
RUNTIME_START_KWARGS = {
    "leader_election_interval_ms": 100,
    "queue_storage_queue_rotate_interval_ms": 60_000,
}


@dataclass
class StageResolve:
    source: int
    item_id: int


@dataclass
class StageProcess:
    item_id: int


@dataclass
class StagePoisoned:
    id: int


@dataclass
class StageCancelled:
    id: int


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    await c.install_queue_storage(reset=True)
    try:
        yield c
    finally:
        await c.shutdown()
        await c.close()


async def count_kind(client, kind: str) -> int:
    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT count(*)::bigint AS n FROM awa.jobs WHERE kind = $1", kind
    )
    await tx.commit()
    return row["n"]


async def wait_for_state(client, job_id: int, state: str, timeout: float = 10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        job = await client.get_job(job_id)
        if str(job.state) == state:
            return job
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"job {job_id} is {job.state}, expected {state}")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_staged_follow_ups_commit_and_deduplicate(client):
    queue = "staged_py_dedup"

    @client.task(StageResolve, queue=queue)
    async def resolve(job):
        job.enqueue_on_completion(
            StageProcess(item_id=job.args.item_id * 10),
            queue="staged_py_process",
            unique_opts={},
            metadata={"source": job.args.source},
        )

    ids = []
    for source, item_id in [(1, 7), (2, 7), (3, 8)]:
        job = await client.insert(
            StageResolve(source=source, item_id=item_id), queue=queue
        )
        ids.append(job.id)
    await client.start([(queue, 5)], **RUNTIME_START_KWARGS)
    for job_id in ids:
        await wait_for_state(client, job_id, "completed")
    await client.shutdown()

    tx = await client.transaction()
    rows = await tx.fetch_all(
        "SELECT (args->>'item_id')::bigint AS item_id FROM awa.jobs WHERE kind = $1 ORDER BY 1",
        "stage_process",
    )
    await tx.commit()
    assert [row["item_id"] for row in rows] == [70, 80]


@pytest.mark.asyncio
async def test_failed_stage_poisons_and_other_outcomes_discard(client):
    queue = "staged_py_poison"

    @client.task(StagePoisoned, queue=queue)
    async def poisoned(job):
        try:
            job.enqueue_on_completion(
                StageProcess(item_id=-1), metadata={"pad": "x" * awa.MAX_STAGED_BYTES}
            )
        except awa.StagingError:
            pass  # swallowed: the attempt must still fail
        return None

    @client.task(StageCancelled, queue=queue)
    async def cancelled(job):
        job.enqueue_on_completion(StageProcess(item_id=-2))
        return awa.Cancel("not needed")

    poisoned_job = await client.insert(
        StagePoisoned(id=1), queue=queue, max_attempts=1
    )
    cancelled_job = await client.insert(StageCancelled(id=1), queue=queue)
    await client.start([(queue, 2)], **RUNTIME_START_KWARGS)
    failed = await wait_for_state(client, poisoned_job.id, "failed")
    await wait_for_state(client, cancelled_job.id, "cancelled")
    await client.shutdown()

    assert "failed to stage" in str(failed.errors[-1]["error"])
    assert await count_kind(client, "stage_process") == 0


@pytest.mark.asyncio
async def test_staging_error_is_terminal():
    assert issubclass(awa.StagingError, awa.TerminalError)


def test_handler_job_records_and_limits_staging():
    job = HandlerJob(
        args=StageResolve(source=1, item_id=1),
        id=1,
        attempt=1,
        max_attempts=25,
        queue="default",
        priority=2,
        deadline=None,
        metadata={},
        tags=[],
    )
    job.enqueue_on_completion(StageProcess(item_id=1), unique_opts={})
    assert len(job.staged) == 1
    assert job.staged[0].args == StageProcess(item_id=1)
    assert job.staged[0].kwargs == {"unique_opts": {}}

    for item_id in range(2, awa.MAX_STAGED_FOLLOW_UPS + 1):
        job.enqueue_on_completion(StageProcess(item_id=item_id))
    with pytest.raises(awa.StagingError):
        job.enqueue_on_completion(StageProcess(item_id=0))
    assert job.staging_error is not None
    assert len(job.staged) == awa.MAX_STAGED_FOLLOW_UPS


@pytest.fixture
async def tc():
    client = awa.AsyncClient(DATABASE_URL)
    test_client = AwaTestClient(client)
    await test_client.migrate()
    await test_client.clean()
    try:
        yield test_client
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_test_client_inserts_staged_follow_ups(tc):
    async def resolve(job):
        job.enqueue_on_completion(
            StageProcess(item_id=job.args.item_id), unique_opts={}
        )

    for source in (1, 2):
        await tc.insert(StageResolve(source=source, item_id=5))
        result = await tc.work_one(StageResolve, handler=resolve)
        assert result.is_completed()
    assert await count_kind(tc.client, "stage_process") == 1


@pytest.mark.asyncio
async def test_test_client_fails_a_poisoned_completion(tc):
    async def poisoned(job):
        try:
            job.enqueue_on_completion(
                StageProcess(item_id=1), metadata={"pad": "x" * awa.MAX_STAGED_BYTES}
            )
        except awa.StagingError:
            pass

    await tc.insert(StagePoisoned(id=1))
    result = await tc.work_one(StagePoisoned, handler=poisoned)
    assert result.is_failed()
    assert "failed to stage" in (result.error or "")
    assert await count_kind(tc.client, "stage_process") == 0

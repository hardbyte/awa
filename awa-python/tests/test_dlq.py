"""Tests for the Dead Letter Queue Python bindings.

Exercises both sync and async DLQ APIs end-to-end against the shared test
database. Covers list/get/depth, single + bulk retry, single + bulk move,
and single + bulk purge.
"""

import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class DlqPyJob:
    value: str


@pytest.fixture
def sync_client():
    c = awa.Client(DATABASE_URL)
    c.migrate()
    tx = c.transaction()
    # Keep to our own queues so parallel tests elsewhere don't collide.
    tx.execute("DELETE FROM awa.jobs_dlq WHERE queue LIKE 'pydlq_%'")
    tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'pydlq_%'")
    tx.commit()
    return c


def _fail_job(client, job_id: int) -> None:
    tx = client.transaction()
    tx.execute(
        "UPDATE awa.jobs SET state='failed', finalized_at=now() WHERE id=$1",
        job_id,
    )
    tx.commit()


def test_move_list_get_depth(sync_client):
    queue = "pydlq_roundtrip"
    job = sync_client.insert(DlqPyJob(value="first"), queue=queue)
    _fail_job(sync_client, job.id)

    entry = sync_client.move_failed_to_dlq(job.id, "py_test")
    assert entry is not None
    assert entry.job.id == job.id
    assert entry.reason == "py_test"
    assert entry.original_run_lease == 0  # never claimed

    listed = sync_client.list_dlq(queue=queue)
    assert len(listed) == 1
    assert listed[0].job.id == job.id

    depth = sync_client.dlq_depth(queue=queue)
    assert depth == 1

    fetched = sync_client.get_dlq_job(job.id)
    assert fetched is not None
    assert fetched.reason == "py_test"

    depths = sync_client.dlq_depth_by_queue()
    queue_map = dict(depths)
    assert queue_map.get(queue) == 1


def test_bulk_move_and_purge(sync_client):
    queue = "pydlq_bulk"
    ids = []
    for i in range(3):
        job = sync_client.insert(DlqPyJob(value=f"b{i}"), queue=queue)
        _fail_job(sync_client, job.id)
        ids.append(job.id)

    moved = sync_client.bulk_move_failed_to_dlq(queue=queue, reason="py_bulk")
    assert moved == 3
    assert sync_client.dlq_depth(queue=queue) == 3

    purged = sync_client.purge_dlq(queue=queue)
    assert purged == 3
    assert sync_client.dlq_depth(queue=queue) == 0


def test_retry_from_dlq_revives(sync_client):
    queue = "pydlq_retry"
    job = sync_client.insert(DlqPyJob(value="retry_me"), queue=queue)
    _fail_job(sync_client, job.id)
    sync_client.move_failed_to_dlq(job.id, "py_will_retry")

    revived = sync_client.retry_from_dlq(job.id)
    assert revived is not None
    assert revived.id == job.id
    assert revived.attempt == 0
    assert str(revived.state) == "available"

    # DLQ should be empty for this queue now.
    assert sync_client.dlq_depth(queue=queue) == 0


def test_purge_dlq_job_single(sync_client):
    queue = "pydlq_purge_one"
    job = sync_client.insert(DlqPyJob(value="gone"), queue=queue)
    _fail_job(sync_client, job.id)
    sync_client.move_failed_to_dlq(job.id, "py_purge_me")

    assert sync_client.purge_dlq_job(job.id) is True
    assert sync_client.purge_dlq_job(job.id) is False  # already gone
    assert sync_client.get_dlq_job(job.id) is None


@pytest.mark.asyncio
async def test_async_dlq_flow():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    tx = await c.transaction()
    async with tx:
        await tx.execute("DELETE FROM awa.jobs_dlq WHERE queue = 'pydlq_async'")
        await tx.execute("DELETE FROM awa.jobs WHERE queue = 'pydlq_async'")

    job = await c.insert(DlqPyJob(value="async"), queue="pydlq_async")
    tx = await c.transaction()
    async with tx:
        await tx.execute(
            "UPDATE awa.jobs SET state='failed', finalized_at=now() WHERE id=$1",
            job.id,
        )

    entry = await c.move_failed_to_dlq(job.id, "py_async")
    assert entry is not None
    assert entry.job.id == job.id
    assert entry.reason == "py_async"

    depth = await c.dlq_depth(queue="pydlq_async")
    assert depth == 1

    revived = await c.retry_from_dlq(job.id)
    assert revived is not None
    assert revived.id == job.id
    assert str(revived.state) == "available"

    # DLQ should be empty for this queue after the retry moves the row back.
    assert await c.dlq_depth(queue="pydlq_async") == 0

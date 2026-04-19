"""Tests for the Dead Letter Queue Python bindings on queue_storage.

The Python client does not yet expose the full worker-side queue_storage
configuration, so these tests bootstrap a dedicated queue_storage schema and
materialize failed rows directly inside that schema before exercising the
public DLQ APIs.
"""

import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
SCHEMA = "awa_py_dlq"


@dataclass
class DlqPyJob:
    value: str


def _move_ready_to_failed_done(client: awa.Client, job_id: int) -> None:
    tx = client.transaction()
    tx.execute(
        f"""
        WITH moved AS (
            DELETE FROM {SCHEMA}.ready_entries
            WHERE job_id = $1
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                payload
        )
        INSERT INTO {SCHEMA}.done_entries (
            ready_slot,
            ready_generation,
            job_id,
            kind,
            queue,
            args,
            state,
            priority,
            attempt,
            run_lease,
            max_attempts,
            lane_seq,
            run_at,
            attempted_at,
            finalized_at,
            created_at,
            unique_key,
            unique_states,
            payload
        )
        SELECT
            ready_slot,
            ready_generation,
            job_id,
            kind,
            queue,
            args,
            'failed'::awa.job_state,
            priority,
            GREATEST(attempt, 1),
            run_lease,
            max_attempts,
            lane_seq,
            run_at,
            COALESCE(attempted_at, now()),
            now(),
            created_at,
            unique_key,
            unique_states,
            payload
        FROM moved
        """,
        job_id,
    )
    tx.commit()


async def _move_ready_to_failed_done_async(client: awa.AsyncClient, job_id: int) -> None:
    tx = await client.transaction()
    async with tx:
        await tx.execute(
            f"""
            WITH moved AS (
                DELETE FROM {SCHEMA}.ready_entries
                WHERE job_id = $1
                RETURNING
                    ready_slot,
                    ready_generation,
                    job_id,
                    kind,
                    queue,
                    args,
                    priority,
                    attempt,
                    run_lease,
                    max_attempts,
                    lane_seq,
                    run_at,
                    attempted_at,
                    created_at,
                    unique_key,
                    unique_states,
                    payload
            )
            INSERT INTO {SCHEMA}.done_entries (
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
            )
            SELECT
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                'failed'::awa.job_state,
                priority,
                GREATEST(attempt, 1),
                run_lease,
                max_attempts,
                lane_seq,
                run_at,
                COALESCE(attempted_at, now()),
                now(),
                created_at,
                unique_key,
                unique_states,
                payload
            FROM moved
            """,
            job_id,
        )


@pytest.fixture
def sync_client():
    client = awa.Client(DATABASE_URL)
    client.migrate()
    client.install_queue_storage(schema=SCHEMA, reset=True)
    try:
        yield client
    finally:
        client.close()


def test_move_list_get_depth(sync_client):
    queue = "pydlq_roundtrip"
    job = sync_client.insert(DlqPyJob(value="first"), queue=queue)
    _move_ready_to_failed_done(sync_client, job.id)

    entry = sync_client.move_failed_to_dlq(job.id, "py_test")
    assert entry is not None
    assert entry.job.id == job.id
    assert entry.reason == "py_test"
    assert entry.original_run_lease == 0

    listed = sync_client.list_dlq(queue=queue)
    assert len(listed) == 1
    assert listed[0].job.id == job.id

    fetched = sync_client.get_dlq_job(job.id)
    assert fetched is not None
    assert fetched.reason == "py_test"

    depth = sync_client.dlq_depth(queue=queue)
    assert depth == 1
    queue_map = dict(sync_client.dlq_depth_by_queue())
    assert queue_map.get(queue) == 1


def test_bulk_move_and_purge(sync_client):
    queue = "pydlq_bulk"
    for i in range(3):
        job = sync_client.insert(DlqPyJob(value=f"b{i}"), queue=queue)
        _move_ready_to_failed_done(sync_client, job.id)

    moved = sync_client.bulk_move_failed_to_dlq(queue=queue, reason="py_bulk")
    assert moved == 3
    assert sync_client.dlq_depth(queue=queue) == 3

    purged = sync_client.purge_dlq(queue=queue)
    assert purged == 3
    assert sync_client.dlq_depth(queue=queue) == 0


def test_retry_from_dlq_revives(sync_client):
    queue = "pydlq_retry"
    job = sync_client.insert(DlqPyJob(value="retry_me"), queue=queue)
    _move_ready_to_failed_done(sync_client, job.id)
    sync_client.move_failed_to_dlq(job.id, "py_will_retry")

    revived = sync_client.retry_from_dlq(job.id)
    assert revived is not None
    assert revived.id == job.id
    assert revived.attempt == 0
    assert str(revived.state) == "available"
    assert sync_client.dlq_depth(queue=queue) == 0


def test_purge_dlq_job_single(sync_client):
    queue = "pydlq_purge_one"
    job = sync_client.insert(DlqPyJob(value="gone"), queue=queue)
    _move_ready_to_failed_done(sync_client, job.id)
    sync_client.move_failed_to_dlq(job.id, "py_purge_me")

    assert sync_client.purge_dlq_job(job.id) is True
    assert sync_client.purge_dlq_job(job.id) is False
    assert sync_client.get_dlq_job(job.id) is None


@pytest.mark.asyncio
async def test_async_dlq_flow():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    await client.install_queue_storage(schema=SCHEMA, reset=True)
    try:
        job = await client.insert(DlqPyJob(value="async"), queue="pydlq_async")
        await _move_ready_to_failed_done_async(client, job.id)

        entry = await client.move_failed_to_dlq(job.id, "py_async")
        assert entry is not None
        assert entry.job.id == job.id
        assert entry.reason == "py_async"

        depth = await client.dlq_depth(queue="pydlq_async")
        assert depth == 1

        revived = await client.retry_from_dlq(job.id)
        assert revived is not None
        assert revived.id == job.id
        assert str(revived.state) == "available"
        assert await client.dlq_depth(queue="pydlq_async") == 0
    finally:
        await client.close()

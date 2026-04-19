"""Tests for the Dead Letter Queue Python bindings on queue_storage.

These tests exercise the public DLQ APIs directly, so they bootstrap a
dedicated queue_storage schema and materialize failed rows inside that schema
before exercising list/retry/purge behavior.
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
        ),
        released AS (
            SELECT awa.release_queue_storage_unique_claim(
                job_id,
                unique_key,
                unique_states,
                'available'::awa.job_state
            )
            FROM moved
        ),
        lane_fix AS (
            UPDATE {SCHEMA}.queue_lanes AS lanes
            SET available_count = GREATEST(0, lanes.available_count - delta.moved_count)
            FROM (
                SELECT queue, priority, count(*)::bigint AS moved_count
                FROM moved
                GROUP BY queue, priority
            ) AS delta
            WHERE lanes.queue = delta.queue
              AND lanes.priority = delta.priority
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
            ),
            released AS (
                SELECT awa.release_queue_storage_unique_claim(
                    job_id,
                    unique_key,
                    unique_states,
                    'available'::awa.job_state
                )
                FROM moved
            ),
            lane_fix AS (
                UPDATE {SCHEMA}.queue_lanes AS lanes
                SET available_count = GREATEST(0, lanes.available_count - delta.moved_count)
                FROM (
                    SELECT queue, priority, count(*)::bigint AS moved_count
                    FROM moved
                    GROUP BY queue, priority
                ) AS delta
                WHERE lanes.queue = delta.queue
                  AND lanes.priority = delta.priority
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
    tx = client.transaction()
    tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    tx.commit()
    client.install_queue_storage(schema=SCHEMA, reset=True)
    tx = client.transaction()
    tx.execute("DELETE FROM awa.job_unique_claims")
    tx.commit()
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

    moved = sync_client.bulk_move_failed_to_dlq(reason="py_bulk", allow_all=True)
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


def test_bulk_retry_and_purge_require_scope_unless_allow_all(sync_client):
    retry_job = sync_client.insert(DlqPyJob(value="retry_all"), queue="pydlq_retry_all")
    _move_ready_to_failed_done(sync_client, retry_job.id)
    sync_client.move_failed_to_dlq(retry_job.id, "py_retry_all")

    with pytest.raises(awa.ValidationError):
        sync_client.bulk_retry_from_dlq()

    retried = sync_client.bulk_retry_from_dlq(allow_all=True)
    assert retried == 1
    assert sync_client.dlq_depth(queue="pydlq_retry_all") == 0

    purge_job = sync_client.insert(DlqPyJob(value="purge_all"), queue="pydlq_purge_all")
    _move_ready_to_failed_done(sync_client, purge_job.id)
    sync_client.move_failed_to_dlq(purge_job.id, "py_purge_all")

    with pytest.raises(awa.ValidationError):
        sync_client.purge_dlq()

    purged = sync_client.purge_dlq(allow_all=True)
    assert purged == 1
    assert sync_client.dlq_depth(queue="pydlq_purge_all") == 0


def test_list_and_purge_with_before_dlq_at_only(sync_client):
    queue = "pydlq_before_cursor"
    older = sync_client.insert(DlqPyJob(value="older"), queue=queue)
    newer = sync_client.insert(DlqPyJob(value="newer"), queue=queue)
    _move_ready_to_failed_done(sync_client, older.id)
    _move_ready_to_failed_done(sync_client, newer.id)
    sync_client.move_failed_to_dlq(older.id, "older")
    sync_client.move_failed_to_dlq(newer.id, "newer")

    tx = sync_client.transaction()
    tx.execute(
        f"UPDATE {SCHEMA}.dlq_entries SET dlq_at = now() - interval '1 day' WHERE job_id = $1",
        older.id,
    )
    tx.commit()

    listed = sync_client.list_dlq(queue=queue)
    assert [entry.job.id for entry in listed] == [newer.id, older.id]

    paged = sync_client.list_dlq(queue=queue, before_dlq_at=listed[0].dlq_at)
    assert [entry.job.id for entry in paged] == [older.id]

    purged = sync_client.purge_dlq(queue=queue, before_dlq_at=listed[0].dlq_at)
    assert purged == 1
    remaining = sync_client.list_dlq(queue=queue)
    assert [entry.job.id for entry in remaining] == [newer.id]


def test_retry_from_dlq_surfaces_unique_conflict(sync_client):
    queue = "pydlq_unique"
    unique_opts = {"by_queue": True, "by_args": True}

    original = sync_client.insert(
        DlqPyJob(value="same"),
        queue=queue,
        unique_opts=unique_opts,
    )
    _move_ready_to_failed_done(sync_client, original.id)
    sync_client.move_failed_to_dlq(original.id, "py_unique")
    tx = sync_client.transaction()
    tx.execute("DELETE FROM awa.job_unique_claims WHERE job_id = $1", original.id)
    tx.commit()

    replacement = sync_client.insert(
        DlqPyJob(value="same"),
        queue=queue,
        unique_opts=unique_opts,
    )
    assert replacement.id != original.id

    with pytest.raises(awa.UniqueConflict):
        sync_client.retry_from_dlq(original.id)

    dlq_entry = sync_client.get_dlq_job(original.id)
    assert dlq_entry is not None
    assert dlq_entry.reason == "py_unique"


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

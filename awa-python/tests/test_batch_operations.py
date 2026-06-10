"""Python bindings for durable batch operations."""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class BatchPyJob:
    value: str


def _reset_control_plane(client: awa.Client) -> None:
    tx = client.transaction()
    tx.execute(
        """
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        """
    )
    tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    tx.execute("DELETE FROM awa.batch_operation_items")
    tx.execute("DELETE FROM awa.batch_operations")
    tx.execute("DELETE FROM awa.jobs_hot WHERE queue LIKE 'pybatch_%'")
    tx.commit()


@pytest.fixture
def sync_client():
    client = awa.Client(DATABASE_URL)
    client.migrate()
    _reset_control_plane(client)
    try:
        yield client
    finally:
        _reset_control_plane(client)
        client.close()


@pytest.fixture
async def async_client():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    reset = awa.Client(DATABASE_URL)
    try:
        _reset_control_plane(reset)
    finally:
        reset.close()
    try:
        yield client
    finally:
        reset = awa.Client(DATABASE_URL)
        try:
            _reset_control_plane(reset)
        finally:
            reset.close()
            await client.close()


def test_sync_batch_operation_helpers_preview_submit_list_and_cancel(sync_client):
    queue = "pybatch_sync"
    job = sync_client.insert(BatchPyJob("one"), queue=queue, priority=4)
    second = sync_client.insert(BatchPyJob("two"), queue=queue, priority=4)

    preview = sync_client.preview_set_priority(
        1,
        filter={"queue": queue, "state": "available"},
    )
    assert preview["total_matched"] == 2
    assert {row["id"] for row in preview["sample"]} == {job.id, second.id}

    operation = sync_client.set_priority(
        1,
        filter={"queue": queue},
        submitted_by="pytest",
    )
    assert operation["op_kind"] == "set_priority"
    assert operation["state"] == "pending"
    assert operation["submitted_by"] == "pytest"
    assert operation["spec"]["priority"] == 1

    listed = sync_client.list_batch_operations(state="pending")
    assert [row["id"] for row in listed] == [operation["id"]]

    fetched = sync_client.get_batch_operation(operation["id"])
    assert fetched["id"] == operation["id"]

    cancelled = sync_client.cancel_batch_operation(operation["id"])
    assert cancelled["state"] == "cancelling"


def test_sync_batch_operation_move_queue_and_validation(sync_client):
    queue = "pybatch_move"
    job = sync_client.insert(BatchPyJob("move"), queue=queue, priority=3)

    preview = sync_client.preview_move_queue(
        "pybatch_dest",
        priority=2,
        filter={"ids": [job.id]},
    )
    assert preview["total_matched"] == 1

    operation = sync_client.move_queue(
        "pybatch_dest",
        priority=2,
        filter={"ids": [job.id]},
    )
    assert operation["op_kind"] == "move_queue"
    assert operation["spec"]["queue"] == "pybatch_dest"
    assert operation["spec"]["priority"] == 2

    with pytest.raises(awa.ValidationError, match="requires a filter"):
        sync_client.set_priority(1)


@pytest.mark.asyncio
async def test_async_batch_operation_helpers_accept_datetime_filters(async_client):
    queue = "pybatch_async"
    job = await async_client.insert(BatchPyJob("async"), queue=queue, priority=4)
    created_before = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1)

    preview = await async_client.preview_batch_operation(
        "set_priority",
        spec={"priority": 1},
        filter={
            "queue": queue,
            "created_at_lt": created_before,
        },
    )
    assert preview["total_matched"] == 1

    operation = await async_client.submit_batch_operation(
        "move_queue",
        spec={"queue": "pybatch_async_dest"},
        filter={"ids": [job.id]},
        submitted_by="pytest-async",
    )
    assert operation["op_kind"] == "move_queue"
    assert operation["submitted_by"] == "pytest-async"

    cancelled = await async_client.cancel_batch_operation(operation["id"])
    assert cancelled["state"] == "cancelling"

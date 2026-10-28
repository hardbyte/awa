"""Tests for retained job output (ADR-048)."""

import asyncio
import math
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
class Summarize:
    n: int


@dataclass
class Report:
    updated: int
    tags: tuple


@dataclass
class OutputCallback:
    n: int


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


async def wait_for_state(client, job_id: int, state: str, timeout: float = 10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        job = await client.get_job(job_id)
        if str(job.state) == state:
            return job
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"job {job_id} is {job.state}, expected {state}")
        await asyncio.sleep(0.05)


def handler_job(**overrides) -> HandlerJob:
    fields = {
        "args": Summarize(n=1),
        "id": 1,
        "attempt": 1,
        "max_attempts": 25,
        "queue": "default",
        "priority": 2,
        "deadline": None,
        "metadata": {},
        "tags": [],
    }
    fields.update(overrides)
    return HandlerJob(**fields)


@pytest.mark.asyncio
async def test_completion_stores_the_output(client):
    queue = "output_py_completed"
    seen_during_dispatch = []

    @client.task(Summarize, queue=queue)
    async def summarize(job):
        job.set_output(Report(updated=job.args.n, tags=("a", "b")))
        seen_during_dispatch.append(job.output)

    job = await client.insert(Summarize(n=3), queue=queue)
    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    done = await wait_for_state(client, job.id, "completed")
    await client.shutdown()

    assert seen_during_dispatch == [{"updated": 3, "tags": ["a", "b"]}]
    assert done.output == {"updated": 3, "tags": ["a", "b"]}


@pytest.mark.asyncio
async def test_a_failed_output_is_kept_in_the_error_entry_and_does_not_poison(client):
    queue = "output_py_failed"
    errors = []

    @client.task(Summarize, queue=queue)
    async def summarize(job):
        job.set_output({"step": "fetched"})
        try:
            job.set_output({"blob": "x" * awa.MAX_OUTPUT_BYTES})
        except awa.OutputError as e:
            errors.append(e)
        job.set_output({"step": "failed"})
        raise awa.TerminalError("gave up")

    job = await client.insert(Summarize(n=1), queue=queue, max_attempts=1)
    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    failed = await wait_for_state(client, job.id, "failed")
    await client.shutdown()

    assert len(errors) == 1 and isinstance(errors[0], awa.TerminalError)
    assert failed.output is None
    assert [entry.get("output") for entry in failed.errors] == [{"step": "failed"}]


@pytest.mark.asyncio
async def test_a_failed_set_clears_and_the_job_still_completes(client):
    queue = "output_py_cleared"

    @client.task(Summarize, queue=queue)
    async def summarize(job):
        job.set_output({"step": "started"})
        with pytest.raises(awa.OutputError):
            job.set_output({1, 2})
        assert job.output is None
        job.set_output({"step": "started"})
        job.set_output(None)

    job = await client.insert(Summarize(n=1), queue=queue)
    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    done = await wait_for_state(client, job.id, "completed")
    await client.shutdown()
    assert done.output is None


@pytest.mark.asyncio
async def test_external_completion_stores_an_output_when_asked(client):
    queue = "output_py_external"
    callback_ids = []

    @client.task(OutputCallback, queue=queue)
    async def wait(job):
        token = await job.register_callback(timeout_seconds=60)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    job = await client.insert(OutputCallback(n=1), queue=queue)
    await client.start([(queue, 1)], **RUNTIME_START_KWARGS)
    await wait_for_state(client, job.id, "waiting_external")
    with pytest.raises(awa.OutputError):
        await client.complete_external(callback_ids[0], output={"bad": math.inf})
    assert str((await client.get_job(job.id)).state) == "waiting_external"
    completed = await client.complete_external(
        callback_ids[0], {"ignored": True}, output={"approved": True}
    )
    await client.shutdown()
    assert completed.output == {"approved": True}
    assert (await client.get_job(job.id)).output == {"approved": True}


def test_output_error_is_terminal():
    assert issubclass(awa.OutputError, awa.TerminalError)
    assert awa.MAX_OUTPUT_BYTES == 4096


def test_handler_job_records_output_like_the_runtime():
    job = handler_job()
    assert job.output is None
    job.set_output(Report(updated=2, tags=("x",)))
    assert job.output == {"updated": 2, "tags": ["x"]}
    job.set_output(None)
    assert job.output is None

    job.set_output({"ok": True})
    for bad in (
        {1, 2},
        {"nan": math.nan},
        {1: "int key"},
        {"big": 2**70},
        {"nul": "a\x00b"},
        "x" * awa.MAX_OUTPUT_BYTES,
        object(),
    ):
        with pytest.raises(awa.OutputError):
            job.set_output(bad)
        assert job.output is None, bad
        job.set_output({"ok": True})

    job.clear_output()
    assert job.output is None
    with pytest.raises(AttributeError):
        job.output = {"direct": True}


def test_handler_job_output_is_sealed_with_the_handler():
    job = handler_job()
    job.set_output("done")
    job._sealed = True
    with pytest.raises(awa.OutputError):
        job.set_output("late")
    assert job.output == "done"


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
async def test_test_client_reports_the_sealed_output(tc):
    async def summarize(job):
        job.set_output({"n": job.args.n})

    async def snooze(job):
        job.set_output({"n": job.args.n})
        return awa.Snooze(seconds=3600)

    await tc.insert(Summarize(n=4))
    result = await tc.work_one(Summarize, handler=summarize)
    assert result.is_completed()
    assert result.output == {"n": 4}

    await tc.insert(Summarize(n=5))
    snoozed = await tc.work_one(Summarize, handler=snooze)
    assert snoozed.outcome == "snoozed"
    assert snoozed.output is None

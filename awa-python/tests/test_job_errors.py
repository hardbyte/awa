"""
Tests for Job.errors field — the error history exposed to Python.

Covers: empty errors, single error, multiple retries, terminal errors,
sync client path, and error shape validation.
"""
import asyncio
import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class ErrorTestJob:
    action: str


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    return c


@pytest.fixture
def sync_client():
    c = awa.Client(DATABASE_URL)
    c.migrate()
    return c


async def clean(client, queue):
    await client.drain_queue(queue)


# ── Zero errors ───────────────────────────────────────────────────


async def test_available_job_has_empty_errors(client):
    """A freshly inserted job has an empty errors list."""
    await clean(client, "err_empty")
    job = await client.insert(ErrorTestJob(action="succeed"), queue="err_empty")
    fetched = await client.get_job(job.id)

    assert isinstance(fetched.errors, list)
    assert len(fetched.errors) == 0


async def test_completed_job_has_empty_errors(client):
    """A successfully completed job has no errors."""
    await clean(client, "err_completed")

    @client.task(ErrorTestJob, queue="err_completed")
    async def handle(job):
        return None

    job = await client.insert(ErrorTestJob(action="succeed"), queue="err_completed")
    client.start([("err_completed", 1)], poll_interval_ms=10)
    await asyncio.sleep(1)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert str(fetched.state) == "completed"
    assert isinstance(fetched.errors, list)
    assert len(fetched.errors) == 0


# ── Single error (terminal) ──────────────────────────────────────


async def test_terminal_error_appears_in_errors(client):
    """A terminal error produces exactly one error entry."""
    await clean(client, "err_terminal")

    @client.task(ErrorTestJob, queue="err_terminal")
    async def handle(job):
        raise awa.TerminalError("permanent failure")

    job = await client.insert(
        ErrorTestJob(action="terminal"),
        queue="err_terminal",
        max_attempts=5,  # lots of retries — but terminal skips them
    )
    client.start([("err_terminal", 1)], poll_interval_ms=10)
    await asyncio.sleep(2)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert str(fetched.state) == "failed"
    assert len(fetched.errors) == 1

    err = fetched.errors[0]
    assert "error" in err, f"Missing 'error' key in {err}"
    assert "attempt" in err, f"Missing 'attempt' key in {err}"
    assert "at" in err, f"Missing 'at' key in {err}"
    assert "permanent failure" in err["error"]
    assert err["attempt"] == 1


# ── Multiple errors (retries exhausted) ───────────────────────────


async def test_multiple_retries_accumulate_errors(client):
    """Each failed attempt adds an entry to the errors list."""
    await clean(client, "err_multi")

    call_count = 0

    @client.task(ErrorTestJob, queue="err_multi")
    async def handle(job):
        nonlocal call_count
        call_count += 1
        raise ValueError(f"attempt {call_count}")

    job = await client.insert(
        ErrorTestJob(action="retry_exhaust"),
        queue="err_multi",
        max_attempts=3,
    )
    client.start(
        [("err_multi", 1)],
        poll_interval_ms=10,
        promote_interval_ms=100,
    )
    # Wait for all 3 attempts to run
    for _ in range(60):
        await asyncio.sleep(0.5)
        fetched = await client.get_job(job.id)
        if str(fetched.state) == "failed":
            break
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert str(fetched.state) == "failed"
    assert len(fetched.errors) == 3, f"Expected 3 errors, got {len(fetched.errors)}: {fetched.errors}"

    # Each error should have increasing attempt numbers
    attempts = [e["attempt"] for e in fetched.errors]
    assert attempts == [1, 2, 3], f"Expected [1, 2, 3], got {attempts}"

    # Each error message should contain "attempt N"
    for i, err in enumerate(fetched.errors):
        assert f"attempt {i + 1}" in err["error"], f"Error {i} missing expected message: {err['error']}"


# ── Error shape validation ────────────────────────────────────────


async def test_error_entry_shape(client):
    """Each error entry is a dict with specific keys and types."""
    await clean(client, "err_shape")

    @client.task(ErrorTestJob, queue="err_shape")
    async def handle(job):
        raise RuntimeError("shape test error")

    job = await client.insert(
        ErrorTestJob(action="shape"),
        queue="err_shape",
        max_attempts=1,
    )
    client.start([("err_shape", 1)], poll_interval_ms=10)
    await asyncio.sleep(2)
    await client.shutdown()

    fetched = await client.get_job(job.id)
    assert len(fetched.errors) >= 1

    err = fetched.errors[0]

    # Required keys
    assert isinstance(err["error"], str), f"'error' should be str, got {type(err['error'])}"
    assert isinstance(err["attempt"], int), f"'attempt' should be int, got {type(err['attempt'])}"
    assert isinstance(err["at"], str), f"'at' should be str (ISO timestamp), got {type(err['at'])}"

    # Error contains exception type
    assert "RuntimeError" in err["error"]
    assert "shape test error" in err["error"]

    # Timestamp is ISO format
    from datetime import datetime
    datetime.fromisoformat(err["at"].replace("+00:00", "+00:00"))  # should not raise


# ── Sync Client path ─────────────────────────────────────────────


async def test_sync_client_exposes_errors(client, sync_client):
    """Sync Client.get_job() also exposes the errors field."""
    await clean(client, "err_sync")

    @client.task(ErrorTestJob, queue="err_sync")
    async def handle(job):
        raise ValueError("sync error test")

    job = await client.insert(
        ErrorTestJob(action="sync"),
        queue="err_sync",
        max_attempts=1,
    )
    client.start([("err_sync", 1)], poll_interval_ms=10)
    await asyncio.sleep(2)
    await client.shutdown()

    fetched = sync_client.get_job(job.id)
    assert str(fetched.state) == "failed"
    assert isinstance(fetched.errors, list)
    assert len(fetched.errors) >= 1
    assert "sync error test" in fetched.errors[0]["error"]

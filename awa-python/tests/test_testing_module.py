"""Tests for awa.testing module — pytest fixtures and TestClient."""

import os
from dataclasses import dataclass

import pytest

import awa
from awa.testing import AwaTestClient, JobRow, NoJob, WorkResult

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class SendEmail:
    to: str
    subject: str


@dataclass
class FailingJob:
    should_fail: bool = True


@pytest.fixture
async def tc():
    """Set up a TestClient."""
    client = awa.Client(DATABASE_URL)
    test_client = AwaTestClient(client)
    await test_client.migrate()
    await test_client.clean()
    return test_client


@pytest.mark.asyncio
async def test_work_one_completed(tc):
    """work_one returns completed when handler returns None."""
    await tc.insert(SendEmail(to="work@test.com", subject="Work"))

    async def handler(job):
        assert job.args.to == "work@test.com"
        assert job.queue == "default"
        return None  # Completed

    result = await tc.work_one(SendEmail, handler=handler)
    assert result.is_completed()
    assert result.outcome == "completed"
    # Verify the job is a real JobRow, not a placeholder
    assert isinstance(result.job, JobRow)
    assert result.job.kind == "send_email"
    assert result.job.queue == "default"
    assert result.job.state == "completed"


@pytest.mark.asyncio
async def test_work_one_retryable_error(tc):
    """work_one returns retryable when handler raises Exception."""
    await tc.insert(SendEmail(to="retry@test.com", subject="Retry"))

    async def handler(job):
        raise ValueError("temporary failure")

    result = await tc.work_one(SendEmail, handler=handler)
    assert result.is_retryable()
    assert "temporary failure" in result.error


@pytest.mark.asyncio
async def test_work_one_cancel(tc):
    """work_one returns cancelled when handler returns Cancel."""
    await tc.insert(SendEmail(to="cancel@test.com", subject="Cancel"))

    async def handler(job):
        return awa.Cancel(reason="not needed")

    result = await tc.work_one(SendEmail, handler=handler)
    assert result.is_cancelled()
    assert result.error == "not needed"


@pytest.mark.asyncio
async def test_work_one_retry_after(tc):
    """work_one returns retryable when handler returns RetryAfter."""
    await tc.insert(SendEmail(to="later@test.com", subject="Later"))

    async def handler(job):
        return awa.RetryAfter(seconds=60)

    result = await tc.work_one(SendEmail, handler=handler)
    assert result.is_retryable()


@pytest.mark.asyncio
async def test_work_one_snooze(tc):
    """work_one returns snoozed when handler returns Snooze."""
    await tc.insert(SendEmail(to="snooze@test.com", subject="Snooze"))

    async def handler(job):
        return awa.Snooze(seconds=300)

    result = await tc.work_one(SendEmail, handler=handler)
    assert result.is_snoozed()


@pytest.mark.asyncio
async def test_work_one_no_job(tc):
    """work_one returns no_job result when no matching job exists."""
    # Don't insert anything — the queue is empty after clean()
    async def handler(job):
        return None

    result = await tc.work_one(SendEmail, handler=handler)
    assert result.is_no_job()
    assert result.outcome == "no_job"
    assert result.job is NoJob
    assert not result.job  # NoJob is falsy


@pytest.mark.asyncio
async def test_work_one_filters_by_queue(tc):
    """work_one only claims jobs from the specified queue."""
    # Insert a job into a non-default queue
    await tc.insert(SendEmail(to="other@test.com", subject="Other"), queue="emails")

    async def handler(job):
        return None

    # Trying to work from the default queue should find nothing
    result_default = await tc.work_one(SendEmail, handler=handler, queue="default")
    assert result_default.is_no_job()

    # Working from the correct queue should find the job
    result_emails = await tc.work_one(SendEmail, handler=handler, queue="emails")
    assert result_emails.is_completed()
    assert isinstance(result_emails.job, JobRow)
    assert result_emails.job.queue == "emails"


@pytest.mark.asyncio
async def test_test_client_clean_isolates(tc):
    """clean() removes all jobs for test isolation."""
    await tc.insert(SendEmail(to="clean@test.com", subject="Clean"))
    await tc.clean()

    # Should have no jobs
    tx = await tc.client.transaction()
    row = await tx.fetch_one("SELECT count(*)::bigint as cnt FROM awa.jobs")
    await tx.commit()
    assert row["cnt"] == 0

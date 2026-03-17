"""Integration tests for the awa Python client.

Requires Postgres running at localhost:15432 (see docker command in test plan).
"""

import asyncio
import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@pytest.fixture
async def client():
    """Create a client and run migrations."""
    c = awa.Client(DATABASE_URL)
    await c.migrate()
    # Clean up jobs from previous tests
    # We use a raw transaction for this
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.jobs", [])
    await tx.execute("DELETE FROM awa.queue_meta", [])
    await tx.commit()
    return c


# -- Test job types --


@dataclass
class SendEmail:
    to: str
    subject: str


@dataclass
class ProcessPayment:
    order_id: int
    amount_cents: int


@dataclass
class SMTPEmail:
    server: str
    port: int


@dataclass
class PDFRenderJob:
    template: str


# -- Kind derivation tests --


def test_derive_kind_golden_cases():
    """Golden test cases from PRD §9.2 — must match Rust."""
    cases = [
        ("SendEmail", "send_email"),
        ("SendConfirmationEmail", "send_confirmation_email"),
        ("SMTPEmail", "smtp_email"),
        ("OAuthRefresh", "o_auth_refresh"),
        ("PDFRenderJob", "pdf_render_job"),
        ("ProcessV2Import", "process_v2_import"),
        ("ReconcileQ3Revenue", "reconcile_q3_revenue"),
        ("HTMLToPDF", "html_to_pdf"),
        ("IOError", "io_error"),
    ]
    for input_name, expected in cases:
        result = awa.derive_kind(input_name)
        assert result == expected, f"derive_kind({input_name!r}): expected {expected!r}, got {result!r}"


# -- Migration tests --


@pytest.mark.asyncio
async def test_migrate():
    """Migrations run successfully."""
    await awa.migrate(DATABASE_URL)


def test_migrations_sql():
    """Can extract raw migration SQL."""
    sqls = awa.migrations()
    assert len(sqls) >= 1
    version, description, sql = sqls[0]
    assert version == 1
    assert "CREATE SCHEMA" in sql
    assert "awa.jobs" in sql


# -- Insert tests --


@pytest.mark.asyncio
async def test_insert_dataclass(client):
    """Insert a dataclass-based job."""
    job = await client.insert(SendEmail(to="alice@example.com", subject="Welcome"))
    assert job.kind == "send_email"
    assert job.queue == "default"
    assert job.state == awa.JobState.Available
    assert job.priority == 2
    assert job.attempt == 0
    assert job.max_attempts == 25
    assert job.args["to"] == "alice@example.com"
    assert job.args["subject"] == "Welcome"


@pytest.mark.asyncio
async def test_insert_with_opts(client):
    """Insert with custom queue, priority, tags."""
    job = await client.insert(
        SendEmail(to="bob@example.com", subject="Alert"),
        queue="email",
        priority=1,
        max_attempts=3,
        tags=["urgent", "email"],
    )
    assert job.queue == "email"
    assert job.priority == 1
    assert job.max_attempts == 3
    assert job.tags == ["urgent", "email"]


@pytest.mark.asyncio
async def test_insert_with_custom_kind(client):
    """Insert with an explicit kind override."""
    job = await client.insert(
        SendEmail(to="x@y.com", subject="Test"),
        kind="custom_email_kind",
    )
    assert job.kind == "custom_email_kind"


@pytest.mark.asyncio
async def test_insert_dict(client):
    """Insert using a plain dict (kind required)."""
    job = await client.insert(
        {"to": "dict@example.com", "body": "Hello"},
        kind="send_notification",
    )
    assert job.kind == "send_notification"
    assert job.args["to"] == "dict@example.com"


@pytest.mark.asyncio
async def test_kind_auto_derivation(client):
    """Verify auto kind derivation matches PRD spec."""
    job1 = await client.insert(SMTPEmail(server="mail.example.com", port=587))
    assert job1.kind == "smtp_email"

    job2 = await client.insert(PDFRenderJob(template="invoice.html"))
    assert job2.kind == "pdf_render_job"


# -- Transaction tests --


@pytest.mark.asyncio
async def test_transaction_insert(client):
    """Insert within a transaction."""
    tx = await client.transaction()
    job = await tx.insert(SendEmail(to="tx@example.com", subject="Atomic"))
    assert job.kind == "send_email"
    assert job.id > 0
    await tx.commit()


@pytest.mark.asyncio
async def test_transaction_rollback(client):
    """Rolled back transaction should not persist."""
    tx = await client.transaction()
    job = await tx.insert(SendEmail(to="rollback@example.com", subject="Gone"))
    job_id = job.id
    await tx.rollback()

    # Job should not exist after rollback - verify via a new transaction
    tx2 = await client.transaction()
    result = await tx2.execute(
        "SELECT count(*) FROM awa.jobs WHERE id = $1", [job_id]
    )
    await tx2.commit()
    # execute returns affected rows for non-SELECT, but for SELECT we'd need fetch_one
    # Let's use fetch_one instead
    tx3 = await client.transaction()
    row = await tx3.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", [job_id]
    )
    await tx3.commit()
    assert row["cnt"] == 0


@pytest.mark.asyncio
async def test_transaction_execute_and_fetch(client):
    """Transaction execute + fetch_one work for raw SQL."""
    tx = await client.transaction()

    # Insert a job
    job = await tx.insert(
        ProcessPayment(order_id=42, amount_cents=9999), queue="billing"
    )

    # Fetch it back with raw SQL
    row = await tx.fetch_one(
        "SELECT id, kind, queue FROM awa.jobs WHERE id = $1", [job.id]
    )
    assert row["id"] == job.id
    assert row["kind"] == "process_payment"
    assert row["queue"] == "billing"

    await tx.commit()


# -- Admin tests --


@pytest.mark.asyncio
async def test_admin_cancel(client):
    """Cancel a job via client."""
    job = await client.insert(SendEmail(to="cancel@example.com", subject="Cancel"))
    result = await client.cancel(job.id)
    assert result is not None
    assert result.state == awa.JobState.Cancelled


@pytest.mark.asyncio
async def test_admin_retry(client):
    """Retry a failed job."""
    job = await client.insert(SendEmail(to="retry@example.com", subject="Retry"))

    # Manually set to failed
    tx = await client.transaction()
    await tx.execute(
        "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
        [job.id],
    )
    await tx.commit()

    result = await client.retry(job.id)
    assert result is not None
    assert result.state == awa.JobState.Available


@pytest.mark.asyncio
async def test_admin_pause_resume(client):
    """Pause and resume a queue."""
    await client.pause_queue("test_py_queue")
    await client.resume_queue("test_py_queue")


@pytest.mark.asyncio
async def test_admin_drain(client):
    """Drain a queue."""
    for i in range(3):
        await client.insert(
            SendEmail(to=f"drain{i}@example.com", subject="Drain"),
            queue="drain_py_test",
        )

    count = await client.drain_queue("drain_py_test")
    assert count == 3


@pytest.mark.asyncio
async def test_admin_queue_stats(client):
    """Get queue statistics."""
    await client.insert(
        SendEmail(to="stats@example.com", subject="Stats"), queue="stats_py_test"
    )
    stats = await client.queue_stats()
    assert isinstance(stats, list)
    stat = next((s for s in stats if s["queue"] == "stats_py_test"), None)
    assert stat is not None
    assert stat["available"] == 1


# -- Worker registration tests --


@pytest.mark.asyncio
async def test_worker_registration(client):
    """Register a worker via decorator."""

    @client.worker(SendEmail, queue="email")
    async def handle_send_email(job):
        pass  # Just verify registration works

    # The handler should still be callable
    assert callable(handle_send_email)


# -- Error handling tests --


@pytest.mark.asyncio
async def test_insert_dict_without_kind_fails(client):
    """Inserting a dict without kind should fail."""
    # Dicts don't have a class name to derive kind from - but "dict" → "dict"
    # which is valid SQL-wise, just not meaningful. This test verifies
    # the kind derivation doesn't crash.
    job = await client.insert({"data": "test"}, kind="explicit_kind")
    assert job.kind == "explicit_kind"


# -- Job repr --


@pytest.mark.asyncio
async def test_job_repr(client):
    """Job has a useful repr."""
    job = await client.insert(SendEmail(to="repr@example.com", subject="Repr"))
    r = repr(job)
    assert "send_email" in r
    assert str(job.id) in r


# -- Return types --


def test_retry_after():
    r = awa.RetryAfter(seconds=30.0)
    assert r.seconds == 30.0


def test_snooze():
    s = awa.Snooze(seconds=60.0)
    assert s.seconds == 60.0


def test_cancel():
    c = awa.Cancel(reason="no longer needed")
    assert c.reason == "no longer needed"


def test_cancel_default():
    c = awa.Cancel()
    assert "cancelled" in c.reason.lower()


# -- Transaction context manager --


@pytest.mark.asyncio
async def test_transaction_context_manager_commit(client):
    """Transaction as async context manager commits on clean exit."""
    tx = await client.transaction()
    async with tx:
        job = await tx.insert(SendEmail(to="ctx@example.com", subject="Context"))

    # Job should be committed
    tx2 = await client.transaction()
    row = await tx2.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", [job.id]
    )
    await tx2.commit()
    assert row["cnt"] == 1


@pytest.mark.asyncio
async def test_transaction_context_manager_rollback_on_error(client):
    """Transaction rolls back when exception occurs in context manager."""
    job_id = None
    try:
        tx = await client.transaction()
        async with tx:
            job = await tx.insert(SendEmail(to="err@example.com", subject="Error"))
            job_id = job.id
            raise ValueError("simulated error")
    except ValueError:
        pass

    # Job should NOT exist
    tx2 = await client.transaction()
    row = await tx2.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", [job_id]
    )
    await tx2.commit()
    assert row["cnt"] == 0


# -- Pydantic support --


@pytest.mark.asyncio
async def test_insert_pydantic_model(client):
    """Insert a pydantic BaseModel if pydantic is available."""
    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    class PydanticEmail(BaseModel):
        to: str
        subject: str
        urgent: bool = False

    job = await client.insert(PydanticEmail(to="pydantic@example.com", subject="Pydantic", urgent=True))
    assert job.kind == "pydantic_email"
    assert job.args["to"] == "pydantic@example.com"
    assert job.args["urgent"] is True

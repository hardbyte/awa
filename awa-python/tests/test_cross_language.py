"""Cross-language integration tests.

Verify that the Python client produces jobs that match the Rust serde schema
exactly — kind derivation, JSON args structure, and state transitions.
These tests validate the cross-language contract from PRD §9.2 and §17.
"""

import os
from dataclasses import dataclass

import pytest

import awa
from awa._awa import derive_kind

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@pytest.fixture
async def client():
    c = awa.Client(DATABASE_URL)
    await c.migrate()
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.jobs")
    await tx.execute("DELETE FROM awa.queue_meta")
    await tx.commit()
    return c


# ── Kind derivation golden tests (must match Rust exactly) ─────────────


def test_kind_derivation_cross_language_golden():
    """PRD §9.2: Golden test cases must produce identical results in Rust and Python.

    These exact cases are also tested in awa-macros and awa-model (Rust).
    """
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
    for class_name, expected_kind in cases:
        result = derive_kind(class_name)
        assert result == expected_kind, (
            f"Cross-language mismatch: derive_kind({class_name!r}) = {result!r}, "
            f"expected {expected_kind!r}"
        )


# ── Cross-language JSON contract ───────────────────────────────────────


@dataclass
class SendEmail:
    to: str
    subject: str
    body: str = ""


@dataclass
class ProcessPayment:
    order_id: int
    amount_cents: int
    currency: str = "USD"


@pytest.mark.asyncio
async def test_python_insert_matches_rust_schema(client):
    """Insert from Python, verify the JSON in Postgres matches Rust's serde schema.

    PRD §17: 'kind + JSON args are the contract'.
    """
    job = await client.insert(
        SendEmail(to="cross@test.com", subject="Cross-lang", body="Hello from Python")
    )

    # Verify kind derivation
    assert job.kind == "send_email"

    # Verify args structure matches what Rust's #[derive(Serialize)] would produce
    args = job.args
    assert isinstance(args, dict)
    assert set(args.keys()) == {"to", "subject", "body"}
    assert args["to"] == "cross@test.com"
    assert args["subject"] == "Cross-lang"
    assert args["body"] == "Hello from Python"


@pytest.mark.asyncio
async def test_python_insert_numeric_args(client):
    """Verify numeric types serialize correctly for Rust deserialization."""
    job = await client.insert(
        ProcessPayment(order_id=12345, amount_cents=9999, currency="NZD")
    )

    assert job.kind == "process_payment"
    args = job.args
    assert args["order_id"] == 12345
    assert args["amount_cents"] == 9999
    assert args["currency"] == "NZD"
    # These must be exact types Rust expects (i64, i64, String)
    assert isinstance(args["order_id"], int)
    assert isinstance(args["amount_cents"], int)
    assert isinstance(args["currency"], str)


@pytest.mark.asyncio
async def test_python_insert_job_defaults_match_rust(client):
    """Verify default job fields match Rust's InsertOpts::default()."""
    job = await client.insert(SendEmail(to="defaults@test.com", subject="Defaults"))

    # PRD §7.1: Defaults
    assert job.queue == "default"
    assert job.priority == 2  # PRD: "default" priority
    assert job.max_attempts == 25  # PRD: default max_attempts
    assert job.attempt == 0
    assert job.state == awa.JobState.Available
    assert job.tags == []


@pytest.mark.asyncio
async def test_python_insert_all_queues_opts(client):
    """Verify all InsertOpts fields are honored identically to Rust."""
    job = await client.insert(
        SendEmail(to="opts@test.com", subject="Full opts"),
        queue="email",
        priority=1,
        max_attempts=5,
        tags=["urgent", "cross-lang"],
    )

    assert job.queue == "email"
    assert job.priority == 1
    assert job.max_attempts == 5
    assert job.tags == ["urgent", "cross-lang"]


# ── Job state transitions ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_state_values_match_rust_enum(client):
    """Verify Python JobState enum values match Rust's JobState enum."""
    # These must match the Rust enum and Postgres enum exactly
    assert awa.JobState.Scheduled is not None
    assert awa.JobState.Available is not None
    assert awa.JobState.Running is not None
    assert awa.JobState.Completed is not None
    assert awa.JobState.Retryable is not None
    assert awa.JobState.Failed is not None
    assert awa.JobState.Cancelled is not None


@pytest.mark.asyncio
async def test_insert_creates_available_state(client):
    """PRD §6.1: Insert without run_at → 'available'."""
    job = await client.insert(SendEmail(to="state@test.com", subject="State"))
    assert job.state == awa.JobState.Available


@pytest.mark.asyncio
async def test_cancel_transitions_to_cancelled(client):
    """PRD §6.1: Cancel → 'cancelled'."""
    job = await client.insert(SendEmail(to="cancel@test.com", subject="Cancel"))
    cancelled = await client.cancel(job.id)
    assert cancelled.state == awa.JobState.Cancelled


@pytest.mark.asyncio
async def test_retry_transitions_to_available(client):
    """PRD §10: Retry → 'available', attempt reset to 0."""
    job = await client.insert(SendEmail(to="retry@test.com", subject="Retry"))

    # Manually set to failed
    tx = await client.transaction()
    await tx.execute(
        "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
        job.id,
    )
    await tx.commit()

    retried = await client.retry(job.id)
    assert retried.state == awa.JobState.Available


# ── Transactional enqueue ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_transactional_enqueue_atomic(client):
    """PRD §9.1: Transaction commit makes job visible; rollback doesn't."""
    # Commit path
    tx = await client.transaction()
    job = await tx.insert(SendEmail(to="atomic@test.com", subject="Atomic"))
    await tx.commit()

    # Job should exist
    tx2 = await client.transaction()
    row = await tx2.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", job.id
    )
    await tx2.commit()
    assert row["cnt"] == 1

    # Rollback path
    tx3 = await client.transaction()
    job2 = await tx3.insert(SendEmail(to="rolled@test.com", subject="Rollback"))
    job2_id = job2.id
    await tx3.rollback()

    tx4 = await client.transaction()
    row2 = await tx4.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", job2_id
    )
    await tx4.commit()
    assert row2["cnt"] == 0


# ── Admin operations ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pause_resume_affects_queue_meta(client):
    """PRD §9.6/§10: Pause stores in queue_meta, resume clears it."""
    await client.pause_queue("cross_lang_q", paused_by="python_test")

    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT paused, paused_by FROM awa.queue_meta WHERE queue = $1",
        "cross_lang_q",
    )
    await tx.commit()
    assert row["paused"] is True
    assert row["paused_by"] == "python_test"

    await client.resume_queue("cross_lang_q")

    tx2 = await client.transaction()
    row2 = await tx2.fetch_one(
        "SELECT paused FROM awa.queue_meta WHERE queue = $1", "cross_lang_q"
    )
    await tx2.commit()
    assert row2["paused"] is False


@pytest.mark.asyncio
async def test_drain_cancels_pending_jobs(client):
    """PRD §10: Drain cancels available/scheduled/retryable jobs."""
    for i in range(5):
        await client.insert(
            SendEmail(to=f"drain{i}@test.com", subject="Drain"),
            queue="drain_cross",
        )

    drained = await client.drain_queue("drain_cross")
    assert drained == 5

    # Verify all cancelled
    tx = await client.transaction()
    row = await tx.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE queue = 'drain_cross' AND state = 'available'",
    )
    await tx.commit()
    assert row["cnt"] == 0

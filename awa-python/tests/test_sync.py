"""Synchronous API tests for the awa Python client.

These tests use NO async — they verify the _sync methods work from
plain synchronous Python code (Django/Flask web handlers).

Requires Postgres running at localhost:15432.
"""

import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class SyncEmail:
    to: str
    subject: str


@dataclass
class SyncPayment:
    order_id: int
    amount: int


@pytest.fixture
def client():
    """Create a client and run migrations synchronously."""
    c = awa.Client(DATABASE_URL)
    c.migrate()
    # Clean up jobs from previous tests
    tx = c.transaction()
    tx.execute("DELETE FROM awa.jobs")
    tx.execute("DELETE FROM awa.queue_meta")
    tx.commit()
    return c


# -- Test 12: insert_sync returns Job directly --


def test_insert_sync(client):
    job = client.insert(SyncEmail(to="sync@example.com", subject="Hello"))
    assert job.kind == "sync_email"
    assert job.queue == "default"
    assert job.state == awa.JobState.Available
    assert job.args["to"] == "sync@example.com"


# -- Test 13: migrate_sync is idempotent --


def test_migrate_sync_idempotent(client):
    # Should not raise on repeated calls
    client.migrate()
    client.migrate()


# -- Test 14: cancel_sync / retry_sync --


def test_cancel_and_retry_sync(client):
    job = client.insert(SyncEmail(to="cancel@example.com", subject="Cancel"))
    result = client.cancel(job.id)
    assert result is not None
    assert result.state == awa.JobState.Cancelled

    # Manually set to failed for retry
    tx = client.transaction()
    tx.execute(
        "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
        job.id,
    )
    tx.commit()

    retried = client.retry(job.id)
    assert retried is not None
    assert retried.state == awa.JobState.Available


# -- Test 15: retry_failed_sync --


def test_retry_failed_sync(client):
    job = client.insert(
        SyncEmail(to="fail@example.com", subject="Fail"), queue="sync_retry"
    )
    tx = client.transaction()
    tx.execute(
        "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
        job.id,
    )
    tx.commit()

    retried = client.retry_failed(queue="sync_retry")
    assert len(retried) >= 1


# -- Test 16: discard_failed_sync --


def test_discard_failed_sync(client):
    job = client.insert(
        SyncEmail(to="discard@example.com", subject="Discard"), queue="sync_discard"
    )
    tx = client.transaction()
    tx.execute(
        "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
        job.id,
    )
    tx.commit()

    count = client.discard_failed("sync_email")
    assert count >= 1


# -- Test 17: pause_queue_sync / resume_queue_sync / drain_queue_sync --


def test_queue_management_sync(client):
    for i in range(3):
        client.insert(
            SyncEmail(to=f"drain{i}@example.com", subject="Drain"),
            queue="sync_drain",
        )

    client.pause_queue("sync_drain")
    client.resume_queue("sync_drain")
    count = client.drain_queue("sync_drain")
    assert count == 3


# -- Test 18: list_jobs_sync with filters --


def test_list_jobs_sync(client):
    client.insert(
        SyncEmail(to="list@example.com", subject="List"), queue="sync_list"
    )
    jobs = client.list_jobs(queue="sync_list", state="available")
    assert len(jobs) == 1
    assert jobs[0].queue == "sync_list"


# -- Test 19: queue_stats_sync --


def test_queue_stats_sync(client):
    client.insert(
        SyncEmail(to="stats@example.com", subject="Stats"), queue="sync_stats"
    )
    client.flush_admin_metadata()
    stats = client.queue_stats()
    assert isinstance(stats, list)
    stat = next((s for s in stats if s.queue == "sync_stats"), None)
    assert stat is not None
    assert stat.available >= 1


# -- Test 20: health_check_sync --


def test_health_check_sync(client):
    health = client.health_check()
    assert health.postgres_connected is True
    assert health.poll_loop_alive is False


# -- Test 21: transaction_sync with context manager: commit on clean exit --


def test_transaction_sync_context_manager_commit(client):
    with client.transaction() as tx:
        job = tx.insert(SyncEmail(to="ctx@example.com", subject="Context"))

    # Job should be committed
    tx2 = client.transaction()
    row = tx2.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", job.id
    )
    tx2.commit()
    assert row["cnt"] == 1


# -- Test 22: transaction_sync with context manager: rollback on exception --


def test_transaction_sync_context_manager_rollback(client):
    job_id = None
    try:
        with client.transaction() as tx:
            job = tx.insert(SyncEmail(to="err@example.com", subject="Error"))
            job_id = job.id
            raise ValueError("simulated error")
    except ValueError:
        pass

    # Job should NOT exist
    tx2 = client.transaction()
    row = tx2.fetch_one(
        "SELECT count(*)::bigint as cnt FROM awa.jobs WHERE id = $1", job_id
    )
    tx2.commit()
    assert row["cnt"] == 0


# -- Test 23: Sync methods work from non-async context (no event loop) --
# (All tests in this file are already non-async, proving this by their existence)


def test_no_event_loop_required(client):
    """Verify we're NOT in an async context."""
    import asyncio

    # Should raise RuntimeError since there's no running event loop
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()

    # But sync methods still work
    job = client.insert(SyncEmail(to="no-loop@example.com", subject="NoLoop"))
    assert job.id > 0


# -- Test 24: insert_many_copy_sync --


def test_insert_many_copy_sync(client):
    jobs_data = [SyncEmail(to=f"copy{i}@example.com", subject=f"Copy {i}") for i in range(10)]
    results = client.insert_many_copy(jobs_data, queue="sync_copy")
    assert len(results) == 10
    for i, job in enumerate(results):
        assert job.kind == "sync_email"
        assert job.queue == "sync_copy"
        assert job.args["to"] == f"copy{i}@example.com"


# -- Test 25: JobState.__str__ returns lowercase --


def test_job_state_str_lowercase():
    """JobState.__str__ returns lowercase string for all variants."""
    assert str(awa.JobState.Scheduled) == "scheduled"
    assert str(awa.JobState.Available) == "available"
    assert str(awa.JobState.Running) == "running"
    assert str(awa.JobState.Completed) == "completed"
    assert str(awa.JobState.Retryable) == "retryable"
    assert str(awa.JobState.Failed) == "failed"
    assert str(awa.JobState.Cancelled) == "cancelled"
    assert str(awa.JobState.WaitingExternal) == "waiting_external"


# -- Test 26: queue_stats returns typed QueueStat objects --


def test_queue_stats_returns_typed_objects(client):
    """queue_stats returns QueueStat objects with correct types and values."""
    client.insert(SyncEmail(to="typed@example.com", subject="Typed"), queue="sync_typed_stats")
    client.flush_admin_metadata()
    stats = client.queue_stats()
    assert len(stats) > 0
    stat = next(s for s in stats if s.queue == "sync_typed_stats")
    assert isinstance(stat, awa.QueueStat)
    assert stat.queue == "sync_typed_stats"
    assert isinstance(stat.available, int)
    assert stat.available >= 1
    assert isinstance(stat.running, int)
    assert isinstance(stat.failed, int)
    assert isinstance(stat.paused, bool)
    assert stat.lag_seconds is None or isinstance(stat.lag_seconds, float)


# -- Test 27: RawClient backwards-compatibility alias --


def test_raw_client_alias():
    """RawClient alias exists and points to the underlying PyO3 class."""
    assert hasattr(awa, "RawClient")
    assert awa.RawClient is awa._awa.Client


async def test_worker_decorator_deprecated():
    """client.worker() still works but emits DeprecationWarning."""
    import warnings
    # Use AsyncClient since worker/task is on that class
    c = awa.AsyncClient(DATABASE_URL)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        @c.worker(SyncEmail, queue="deprecated_test")
        async def handle(job):
            return None

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "client.task()" in str(w[0].message)

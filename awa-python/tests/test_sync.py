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


def reset_storage_transition_state(client: awa.Client) -> None:
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
    tx.execute("DELETE FROM awa.runtime_instances")
    tx.commit()


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
    reset_storage_transition_state(c)
    # Clean up jobs from previous tests
    tx = c.transaction()
    tx.execute("DELETE FROM awa.jobs")
    tx.execute("DELETE FROM awa.queue_meta")
    tx.commit()
    try:
        yield c
    finally:
        reset_storage_transition_state(c)
        c.close()


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

    result = client.retry_failed(queue="sync_retry")
    assert len(result.jobs) >= 1
    assert result.matched >= 1
    assert result.matched >= len(result.jobs)
    # Canonical backend has no per-queue rollups, so pruned_failed_count is None.
    assert result.pruned_failed_count is None


def test_retry_failed_sync_queue_storage_reports_pruned_count(client):
    """On queue_storage, retry_failed(queue=...) surfaces the rollup count.

    With no failed rows the result is deterministic: nothing matched, and the
    per-queue pruned-failed rollup sum is Some(0) (clamped >= 0) — no prune
    race, no sleeps.
    """
    schema = "awa_py_sync_retry_pruned"
    client.install_queue_storage(schema=schema, reset=True)
    try:
        result = client.retry_failed(queue="sync_retry_pruned")
        assert result.jobs == []
        assert result.matched == 0
        assert result.pruned_failed_count == 0
    finally:
        tx = client.transaction()
        tx.execute(
            "DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
        )
        tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        tx.commit()


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


def test_insert_many_copy_sync_per_job_opts(client):
    jobs_data = [
        SyncEmail(to="copy-a@example.com", subject="Copy A"),
        SyncEmail(to="copy-b@example.com", subject="Copy B"),
    ]
    results = client.insert_many_copy(
        jobs_data,
        queue="unused_default",
        opts=[
            {"queue": "sync_copy_a", "ordering_key": "customer-a"},
            {"queue": "sync_copy_b", "ordering_key": None},
        ],
    )

    assert [job.queue for job in results] == ["sync_copy_a", "sync_copy_b"]


def test_enqueue_many_copy_sync_queue_storage(client):
    schema = "awa_py_sync_enqueue_many_copy"
    queue = "sync_enqueue_many_copy"
    client.install_queue_storage(schema=schema, reset=True)

    count = client.enqueue_many_copy(
        [SyncEmail(to=f"qs-copy{i}@example.com", subject=f"Copy {i}") for i in range(3)],
        queue=queue,
        priority=1,
        metadata={"source": "python-sync"},
        tags=["bulk"],
    )
    assert count == 3

    tx = client.transaction()
    row = tx.fetch_one(
        f"""
        SELECT
            count(*)::bigint AS ready_count,
            min(payload->'metadata'->>'source') AS source,
            min(payload->'tags'->>0) AS tag
        FROM {schema}.ready_entries
        WHERE queue = $1
        """,
        queue,
    )
    # Available count derives from sequence-backed lane cursors; aliased to
    # keep the historical `available_count` field name on the returned row.
    counts = tx.fetch_one(
        f"""
        SELECT GREATEST(
            {schema}.sequence_next_value(qe.seq_name)
              - {schema}.sequence_next_value(qc.seq_name),
            0
        ) AS available_count
        FROM {schema}.queue_enqueue_heads AS qe
        JOIN {schema}.queue_claim_heads AS qc
          ON qc.queue = qe.queue
         AND qc.priority = qe.priority
         AND qc.enqueue_shard = qe.enqueue_shard
        WHERE qe.queue = $1 AND qe.priority = 1
        """,
        queue,
    )
    tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    tx.commit()

    assert row["ready_count"] == 3
    assert row["source"] == "python-sync"
    assert row["tag"] == "bulk"
    assert counts["available_count"] == 3


def test_ordering_key_routes_all_sync_python_insert_paths(client):
    schema = "awa_py_sync_ordering_key"
    queue = "sync_ordering_key"
    client.install_queue_storage(schema=schema, reset=True)

    tx = client.transaction()
    tx.execute(
        """
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 4)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        """,
        queue,
    )
    tx.commit()

    key = b"account-42"
    expected_jobs = [
        client.insert(
            SyncEmail(to="key-client@example.com", subject="Client"),
            queue=queue,
            ordering_key=key,
        )
    ]

    tx = client.transaction()
    expected_jobs.append(
        tx.insert(
            SyncEmail(to="key-tx@example.com", subject="Tx"),
            queue=queue,
            ordering_key="account-42",
        )
    )
    tx.commit()

    expected_jobs.extend(
        client.insert_many_copy(
            [
                SyncEmail(to="key-copy-1@example.com", subject="Copy 1"),
                SyncEmail(to="key-copy-2@example.com", subject="Copy 2"),
            ],
            queue=queue,
            ordering_key=key,
        )
    )
    count = client.enqueue_many_copy(
        [
            SyncEmail(to="key-enqueue-1@example.com", subject="Enqueue 1"),
            SyncEmail(to="key-enqueue-2@example.com", subject="Enqueue 2"),
        ],
        queue=queue,
        ordering_key="account-42",
    )
    assert count == 2

    tx = client.transaction()
    rows = tx.fetch_all(
        f"""
        SELECT enqueue_shard
        FROM {schema}.ready_entries
        WHERE queue = $1
        ORDER BY job_id
        """,
        queue,
    )
    tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    tx.commit()

    assert len(rows) == len(expected_jobs) + count
    assert len({row["enqueue_shard"] for row in rows}) == 1


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

    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @c.worker(SyncEmail, queue="deprecated_test")
            async def handle(job):
                return None

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "client.task()" in str(w[0].message)
    finally:
        await c.close()

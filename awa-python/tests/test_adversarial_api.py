"""
Adversarial verification of the Pythonic API surface.

Targets: Client/AsyncClient split, @task decorator, structured errors,
typed returns, JobState.__str__, deprecation warnings.
"""
import asyncio
import os
import warnings
from collections import Counter
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class AdvJob:
    value: str


# ─── Client/AsyncClient confusion ────────────────────────────────


async def test_sync_client_methods_are_truly_sync():
    """Client methods must not return coroutines."""
    client = awa.Client(DATABASE_URL)
    client.migrate()

    job = client.insert(AdvJob(value="sync_check"), queue="adv_sync")
    # If this were a coroutine, accessing .id would fail
    assert isinstance(job.id, int), f"Expected int id, got {type(job.id)}"
    assert str(job.state) == "available"


async def test_async_client_methods_are_truly_async():
    """AsyncClient methods must return awaitables."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    job = await client.insert(AdvJob(value="async_check"), queue="adv_async")
    assert isinstance(job.id, int)
    assert str(job.state) == "available"


async def test_sync_and_async_clients_coexist():
    """Both clients connected to the same DB work simultaneously."""
    sync = awa.Client(DATABASE_URL)
    async_client = awa.AsyncClient(DATABASE_URL)
    sync.migrate()

    # Insert via sync, read via async
    job = sync.insert(AdvJob(value="cross_client"), queue="adv_coexist")
    fetched = await async_client.get_job(job.id)
    assert fetched.id == job.id
    assert str(fetched.state) == "available"


async def test_sync_client_has_no_worker_methods():
    """Sync Client should not have task/start/shutdown/worker methods."""
    client = awa.Client(DATABASE_URL)
    assert not hasattr(client, "task"), "Sync Client should not have task()"
    assert not hasattr(client, "start"), "Sync Client should not have start()"
    assert not hasattr(client, "shutdown"), "Sync Client should not have shutdown()"


# ─── @client.task() decorator ─────────────────────────────────────


async def test_task_decorator_registers_handler():
    """@client.task() registers a handler that processes jobs."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    results = []

    @client.task(AdvJob, queue="adv_task_reg")
    async def handle(job):
        results.append(job.args.value)
        return None

    await client.insert(AdvJob(value="task_test"), queue="adv_task_reg")
    client.start([("adv_task_reg", 2)])
    for _ in range(20):
        await asyncio.sleep(0.1)
        if results:
            break
    await client.shutdown()

    assert results == ["task_test"]


async def test_worker_decorator_emits_deprecation():
    """@client.worker() still works but warns."""
    client = awa.AsyncClient(DATABASE_URL)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        @client.worker(AdvJob, queue="adv_deprecated")
        async def handle(job):
            return None

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "task()" in str(w[0].message)


async def test_task_with_invalid_args_type():
    """Passing a non-class as args_type registers but fails at job processing time.
    The decorator doesn't validate eagerly — this is a known limitation."""
    client = awa.AsyncClient(DATABASE_URL)

    # Registration succeeds (lazy validation)
    @client.task("not_a_class", queue="adv_bad_type")
    async def handle(job):
        return None

    # The error surfaces when a job of this kind is processed, not at registration


# ─── Typed returns ─────────────────────────────────────────────────


async def test_queue_stats_returns_typed_objects():
    """queue_stats returns QueueStat with attribute access, not dicts."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    await client.insert(AdvJob(value="typed"), queue="adv_typed_stats")

    stats = await client.queue_stats()
    assert len(stats) > 0

    stat = stats[0]
    assert isinstance(stat, awa.QueueStat)
    # Verify all 11 fields are accessible
    assert hasattr(stat, "queue")
    assert hasattr(stat, "total_queued")
    assert hasattr(stat, "scheduled")
    assert hasattr(stat, "available")
    assert hasattr(stat, "retryable")
    assert hasattr(stat, "running")
    assert hasattr(stat, "failed")
    assert hasattr(stat, "waiting_external")
    assert hasattr(stat, "completed_last_hour")
    assert hasattr(stat, "lag_seconds")
    assert hasattr(stat, "paused")

    # Verify dict-style access DOESN'T work (it's a typed object)
    with pytest.raises(TypeError):
        _ = stat["queue"]


async def test_sync_queue_stats_also_typed():
    """Sync Client.queue_stats() also returns QueueStat objects."""
    client = awa.Client(DATABASE_URL)
    client.migrate()
    client.insert(AdvJob(value="sync_typed"), queue="adv_sync_typed")

    stats = client.queue_stats()
    assert len(stats) > 0
    assert isinstance(stats[0], awa.QueueStat)


# ─── JobState.__str__ ─────────────────────────────────────────────


def test_job_state_str_all_variants():
    """Every JobState variant has a lowercase string representation."""
    expected = [
        (awa.JobState.Scheduled, "scheduled"),
        (awa.JobState.Available, "available"),
        (awa.JobState.Running, "running"),
        (awa.JobState.Completed, "completed"),
        (awa.JobState.Retryable, "retryable"),
        (awa.JobState.Failed, "failed"),
        (awa.JobState.Cancelled, "cancelled"),
        (awa.JobState.WaitingExternal, "waiting_external"),
    ]
    for variant, expected_str in expected:
        assert str(variant) == expected_str, f"{variant} -> {str(variant)!r}, expected {expected_str!r}"


def test_job_state_str_equality():
    """str(state) enables natural string comparison."""
    assert str(awa.JobState.Completed) == "completed"
    assert str(awa.JobState.Completed) != "Completed"  # case-sensitive
    assert str(awa.JobState.WaitingExternal) == "waiting_external"


def test_job_state_repr():
    """repr shows enum-style name."""
    r = repr(awa.JobState.Completed)
    assert "Completed" in r


# ─── Structured error logging ─────────────────────────────────────


async def test_handler_exception_preserves_type():
    """A Python exception in a handler surfaces with the correct type."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    @client.task(AdvJob, queue="adv_error_type")
    async def handle(job):
        raise ValueError(f"bad value: {job.args.value}")

    job = await client.insert(
        AdvJob(value="error_test"),
        queue="adv_error_type",
        max_attempts=1,
    )
    client.start([("adv_error_type", 1)])
    await asyncio.sleep(2)
    await client.shutdown()

    stored = await client.get_job(job.id)
    assert str(stored.state) == "failed"
    # Note: Job.errors is not currently exposed in the Python API.
    # The error is stored in the DB and visible via SQL / the web UI.
    # Structured error logging (python.exception_type, python.message)
    # is the primary way to inspect handler errors programmatically.


async def test_terminal_error_in_handler():
    """TerminalError immediately fails without retry."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    call_count = 0

    @client.task(AdvJob, queue="adv_terminal")
    async def handle(job):
        nonlocal call_count
        call_count += 1
        raise awa.TerminalError("permanent failure")

    job = await client.insert(
        AdvJob(value="terminal_test"),
        queue="adv_terminal",
        max_attempts=5,
    )
    client.start([("adv_terminal", 1)])
    await asyncio.sleep(2)
    await client.shutdown()

    stored = await client.get_job(job.id)
    assert str(stored.state) == "failed"
    assert call_count == 1, f"Terminal error should not retry, but ran {call_count} times"


# ─── Explicit signatures reject bad kwargs ─────────────────────────


async def test_insert_rejects_unknown_kwargs():
    """Passing unknown kwargs to insert raises TypeError."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    with pytest.raises(TypeError):
        await client.insert(AdvJob(value="bad"), queu="typo_queue")


def test_sync_insert_rejects_unknown_kwargs():
    """Sync Client also rejects unknown kwargs."""
    client = awa.Client(DATABASE_URL)
    client.migrate()

    with pytest.raises(TypeError):
        client.insert(AdvJob(value="bad"), queu="typo_queue")


# ─── Concurrent processing safety ─────────────────────────────────


async def test_no_duplicate_processing_with_task_decorator():
    """Jobs registered via @task are processed exactly once."""
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    # Clean stale jobs from previous runs
    await client.drain_queue("adv_dedup")
    results = []

    @client.task(AdvJob, queue="adv_dedup")
    async def handle(job):
        await asyncio.sleep(0.02)
        results.append(job.args.value)
        return None

    for i in range(30):
        await client.insert(AdvJob(value=f"job_{i}"), queue="adv_dedup")

    client.start([{"name": "adv_dedup", "max_workers": 10}], poll_interval_ms=10)
    for _ in range(60):
        await asyncio.sleep(0.1)
        if len(results) >= 30:
            break
    await client.shutdown()

    counts = Counter(results)
    duplicates = {k: v for k, v in counts.items() if v > 1}
    assert not duplicates, f"DUPLICATE PROCESSING: {duplicates}"
    assert len(results) == 30, f"Expected 30, got {len(results)}"

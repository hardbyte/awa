"""Tests for start() queue config validation (dict form, global_max_workers, rate_limit).

Requires Postgres running at localhost:15432.
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
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    return c


@dataclass
class ConfigTestJob:
    value: str


# -- Test 14: Backward compat tuple form --


@pytest.mark.asyncio
async def test_tuple_form_backward_compat(client):
    """start([("q", 10)]) still works."""
    queue = "cfg_tuple_compat"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    # Should not raise
    await client.start([(queue, 10)])
    await client.shutdown()


# -- Test 12: Dict queue config with rate_limit --


@pytest.mark.asyncio
async def test_dict_config_with_rate_limit(client):
    """Dict form with rate_limit starts successfully."""
    queue = "cfg_dict_rl"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    await client.start(
        [{"name": queue, "max_workers": 10, "rate_limit": (100.0, 100)}]
    )
    await client.shutdown()


# -- Test 13: global_max_workers kwarg --


@pytest.mark.asyncio
async def test_global_max_workers(client):
    """start() with global_max_workers enters weighted mode."""
    queue = "cfg_weighted"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    await client.start(
        [{"name": queue, "min_workers": 5, "weight": 2}],
        global_max_workers=20,
    )
    await client.shutdown()


# -- Test 15: Invalid: tuple + global_max_workers --


@pytest.mark.asyncio
async def test_tuple_plus_global_max_workers_raises(client):
    """Tuple form is not supported with global_max_workers."""
    queue = "cfg_tuple_global"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="tuple queue config is not supported"):
        await client.start([(queue, 10)], global_max_workers=20)


# -- Test 16: Invalid: both max_workers and min_workers --


@pytest.mark.asyncio
async def test_both_max_and_min_workers_raises(client):
    """Cannot specify both max_workers and min_workers."""
    queue = "cfg_both"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="max_workers.*min_workers"):
        await client.start(
            [{"name": queue, "max_workers": 10, "min_workers": 5}]
        )


# -- Additional validation: weight <= 0 --


@pytest.mark.asyncio
async def test_zero_weight_raises(client):
    """weight=0 raises ValueError."""
    queue = "cfg_zero_weight"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="weight must be > 0"):
        await client.start(
            [{"name": queue, "max_workers": 10, "weight": 0}]
        )


# -- Additional validation: global_max_workers + queues=None --


@pytest.mark.asyncio
async def test_global_max_workers_requires_explicit_queues(client):
    """global_max_workers without explicit queues raises ValueError."""
    queue = "cfg_global_no_queues"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="weighted mode requires explicit queue configs"):
        await client.start(global_max_workers=20)


# -- Additional validation: dict missing name --


@pytest.mark.asyncio
async def test_dict_missing_name_raises(client):
    """Dict without 'name' key raises ValueError."""
    queue = "cfg_no_name"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="name"):
        await client.start([{"max_workers": 10}])


# -- Additional validation: rate_limit wrong type --


@pytest.mark.asyncio
async def test_rate_limit_wrong_type_raises(client):
    """rate_limit that's not a (float, int) tuple raises TypeError."""
    queue = "cfg_bad_rl"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(TypeError, match="rate_limit must be a"):
        await client.start(
            [{"name": queue, "max_workers": 10, "rate_limit": "fast"}]
        )


# -- Retention kwargs --


@pytest.mark.asyncio
async def test_retention_kwargs_accepted(client):
    """start() with retention kwargs starts successfully."""
    queue = "cfg_retention"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    await client.start(
        [(queue, 10)],
        completed_retention_hours=1.0,
        failed_retention_hours=168.0,
        cleanup_batch_size=500,
    )
    await client.shutdown()


@pytest.mark.asyncio
async def test_maintenance_interval_kwargs_accepted(client):
    """start() accepts all maintenance interval kwargs including heartbeat_staleness_ms."""
    queue = "cfg_maintenance_intervals"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    await client.start(
        [(queue, 4)],
        heartbeat_interval_ms=100,
        heartbeat_staleness_ms=5_000,
        heartbeat_rescue_interval_ms=500,
        deadline_rescue_interval_ms=1_000,
        callback_rescue_interval_ms=1_000,
        leader_election_interval_ms=200,
        promote_interval_ms=500,
    )
    await client.shutdown()


@pytest.mark.asyncio
async def test_per_queue_retention_in_dict_config(client):
    """Dict form with per-queue retention config starts successfully."""
    queue = "cfg_per_queue_retention"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    await client.start(
        [
            {
                "name": queue,
                "max_workers": 5,
                "retention": {"completed_hours": 1, "failed_hours": 168},
            }
        ]
    )
    await client.shutdown()


@pytest.mark.asyncio
async def test_default_start_uses_queue_storage_backend(client):
    """start() uses queue storage by default and records the active schema."""
    queue = "cfg_default_queue_storage_runtime"
    seen = asyncio.Event()

    tx = await client.transaction()
    await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    await tx.commit()

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        seen.set()
        return None

    try:
        await client.start([(queue, 1)], poll_interval_ms=25)
        job = await client.insert(ConfigTestJob(value="default-queue-storage"), queue=queue)
        await asyncio.wait_for(seen.wait(), timeout=2.0)
        await client.shutdown()

        fetched = await client.get_job(job.id)
        assert fetched.state == awa.JobState.Completed

        tx = await client.transaction()
        backend = await tx.fetch_one(
            "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
        )
        await tx.commit()
        assert backend["schema_name"] == "awa_exp"
    finally:
        await client.shutdown()
        tx = await client.transaction()
        await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        await tx.commit()


@pytest.mark.asyncio
async def test_queue_storage_start_selects_runtime_backend(client):
    """start() can explicitly run the Python worker on queue storage."""
    queue = "cfg_queue_storage_runtime"
    schema = "awa_py_cfg_runtime"
    seen = asyncio.Event()

    tx = await client.transaction()
    await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    await tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await tx.commit()

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        seen.set()
        return None

    try:
        await client.start(
            [(queue, 1)],
            poll_interval_ms=25,
            queue_storage_schema=schema,
            queue_storage_queue_rotate_interval_ms=1000,
            queue_storage_lease_rotate_interval_ms=50,
        )
        job = await client.insert(ConfigTestJob(value="queue-storage"), queue=queue)
        await asyncio.wait_for(seen.wait(), timeout=2.0)
        await client.shutdown()

        fetched = await client.get_job(job.id)
        assert fetched.state == awa.JobState.Completed

        tx = await client.transaction()
        backend = await tx.fetch_one(
            "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
        )
        await tx.commit()
        assert backend["schema_name"] == schema
    finally:
        await client.shutdown()
        tx = await client.transaction()
        await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        await tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await tx.commit()

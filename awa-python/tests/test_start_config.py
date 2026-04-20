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


async def wait_for_job_state(
    client: awa.AsyncClient,
    job_id: int,
    expected_state: awa.JobState,
    timeout: float = 2.0,
) -> awa.Job:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = await client.get_job(job_id)
        if job.state == expected_state:
            return job
        await asyncio.sleep(0.05)
    return await client.get_job(job_id)


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    tx = await c.transaction()
    await tx.execute("DROP SCHEMA IF EXISTS awa CASCADE")
    await tx.execute("DROP SCHEMA IF EXISTS awa_exp CASCADE")
    await tx.commit()
    await c.migrate()
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    await tx.execute("DELETE FROM awa.runtime_instances")
    await tx.commit()
    try:
        yield c
    finally:
        await c.close()


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
async def test_default_start_stays_canonical_until_transition_active(client):
    """Auto role stays on canonical until the transition becomes active."""
    queue = "cfg_default_canonical_runtime"
    seen = asyncio.Event()

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        seen.set()
        return None

    try:
        await client.start([(queue, 1)], poll_interval_ms=25)
        job = await client.insert(ConfigTestJob(value="default-canonical"), queue=queue)
        await asyncio.wait_for(seen.wait(), timeout=2.0)
        fetched = await wait_for_job_state(client, job.id, awa.JobState.Completed)
        assert fetched.state == awa.JobState.Completed

        tx = await client.transaction()
        status = await tx.fetch_one(
            """
            SELECT
                awa.active_queue_storage_schema() AS active_schema,
                (SELECT state FROM awa.storage_status()) AS state,
                (SELECT active_engine FROM awa.storage_status()) AS active_engine
            """
        )
        await tx.commit()
        assert status["active_schema"] is None
        assert status["state"] == "canonical"
        assert status["active_engine"] == "canonical"
    finally:
        await client.shutdown()


@pytest.mark.asyncio
async def test_invalid_storage_transition_role_raises(client):
    """Unknown storage_transition_role values should be rejected early."""
    queue = "cfg_bad_transition_role"

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="storage_transition_role must be one of"):
        await client.start(
            [(queue, 1)],
            poll_interval_ms=25,
            storage_transition_role="queue-storage-now",
        )


@pytest.mark.asyncio
async def test_queue_storage_target_role_executes_after_mixed_transition(client):
    """A queue-storage target worker can start before the routing flip and pick up new work immediately."""
    queue = "cfg_queue_storage_target"
    schema = "awa_py_cfg_target"
    auto_count = 0
    target_count = 0
    auto_seen = asyncio.Event()
    target_seen = asyncio.Event()
    target_client = awa.AsyncClient(DATABASE_URL)

    @client.task(ConfigTestJob, queue=queue)
    async def handle_auto(job):
        nonlocal auto_count
        auto_count += 1
        auto_seen.set()
        return None

    @target_client.task(ConfigTestJob, queue=queue)
    async def handle_target(job):
        nonlocal target_count
        target_count += 1
        target_seen.set()
        return None

    try:
        await target_client.migrate()

        tx = await client.transaction()
        await tx.execute("SELECT * FROM awa.storage_abort()")
        await tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await tx.execute(
            "SELECT * FROM awa.storage_prepare('queue_storage', jsonb_build_object('schema', $1::text))",
            schema,
        )
        await tx.commit()

        await client.start(
            [(queue, 1)],
            poll_interval_ms=25,
            queue_storage_schema=schema,
        )
        await target_client.start(
            [(queue, 1)],
            poll_interval_ms=25,
            queue_storage_schema=schema,
            storage_transition_role="queue_storage_target",
        )

        preflip_job = await client.insert(ConfigTestJob(value="before-flip"), queue=queue)
        await asyncio.wait_for(auto_seen.wait(), timeout=2.0)
        fetched = await wait_for_job_state(client, preflip_job.id, awa.JobState.Completed)
        assert fetched.state == awa.JobState.Completed
        assert auto_count == 1
        assert target_count == 0

        tx = await client.transaction()
        await tx.execute("SELECT * FROM awa.storage_enter_mixed_transition()")
        status = await tx.fetch_one(
            """
            SELECT
                (SELECT state FROM awa.storage_status()) AS state,
                (SELECT active_engine FROM awa.storage_status()) AS active_engine,
                awa.active_queue_storage_schema() AS active_schema
            """
        )
        await tx.commit()
        assert status["state"] == "mixed_transition"
        assert status["active_engine"] == "queue_storage"
        assert status["active_schema"] == schema

        postflip_job = await client.insert(ConfigTestJob(value="after-flip"), queue=queue)
        await asyncio.wait_for(target_seen.wait(), timeout=2.0)
        fetched = await wait_for_job_state(client, postflip_job.id, awa.JobState.Completed)
        assert fetched.state == awa.JobState.Completed
        assert auto_count == 1
        assert target_count == 1
    finally:
        await target_client.shutdown()
        await target_client.close()
        await client.shutdown()
        await client.close()
        reset = awa.AsyncClient(DATABASE_URL)
        try:
            tx = await reset.transaction()
            await tx.execute("DELETE FROM awa.runtime_instances")
            await tx.execute("SELECT * FROM awa.storage_abort()")
            await tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await tx.commit()
        finally:
            await reset.close()


@pytest.mark.asyncio
async def test_install_queue_storage_rejected_while_runtime_is_running(client):
    """install_queue_storage() is blocked once the worker runtime has started."""
    queue = "cfg_install_queue_storage_guard"
    schema = "awa_py_cfg_runtime_guard"

    tx = await client.transaction()
    await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    await tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await tx.commit()

    @client.task(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    try:
        await client.start([(queue, 1)], poll_interval_ms=25, queue_storage_schema=schema)
        with pytest.raises(awa.AwaError, match="cannot install queue storage while the worker runtime is running"):
            await client.install_queue_storage(schema=f"{schema}_other")
    finally:
        await client.shutdown()
        tx = await client.transaction()
        await tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        await tx.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await tx.execute(f"DROP SCHEMA IF EXISTS {schema}_other CASCADE")
        await tx.commit()

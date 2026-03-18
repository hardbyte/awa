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
    c = awa.Client(DATABASE_URL)
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

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    # Should not raise
    client.start([(queue, 10)])
    await client.shutdown()


# -- Test 12: Dict queue config with rate_limit --


@pytest.mark.asyncio
async def test_dict_config_with_rate_limit(client):
    """Dict form with rate_limit starts successfully."""
    queue = "cfg_dict_rl"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    client.start(
        [{"name": queue, "max_workers": 10, "rate_limit": (100.0, 100)}]
    )
    await client.shutdown()


# -- Test 13: global_max_workers kwarg --


@pytest.mark.asyncio
async def test_global_max_workers(client):
    """start() with global_max_workers enters weighted mode."""
    queue = "cfg_weighted"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    client.start(
        [{"name": queue, "min_workers": 5, "weight": 2}],
        global_max_workers=20,
    )
    await client.shutdown()


# -- Test 15: Invalid: tuple + global_max_workers --


@pytest.mark.asyncio
async def test_tuple_plus_global_max_workers_raises(client):
    """Tuple form is not supported with global_max_workers."""
    queue = "cfg_tuple_global"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="tuple queue config is not supported"):
        client.start([(queue, 10)], global_max_workers=20)


# -- Test 16: Invalid: both max_workers and min_workers --


@pytest.mark.asyncio
async def test_both_max_and_min_workers_raises(client):
    """Cannot specify both max_workers and min_workers."""
    queue = "cfg_both"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="max_workers.*min_workers"):
        client.start(
            [{"name": queue, "max_workers": 10, "min_workers": 5}]
        )


# -- Additional validation: weight <= 0 --


@pytest.mark.asyncio
async def test_zero_weight_raises(client):
    """weight=0 raises ValueError."""
    queue = "cfg_zero_weight"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="weight must be > 0"):
        client.start(
            [{"name": queue, "max_workers": 10, "weight": 0}]
        )


# -- Additional validation: global_max_workers + queues=None --


@pytest.mark.asyncio
async def test_global_max_workers_requires_explicit_queues(client):
    """global_max_workers without explicit queues raises ValueError."""
    queue = "cfg_global_no_queues"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="weighted mode requires explicit queue configs"):
        client.start(global_max_workers=20)


# -- Additional validation: dict missing name --


@pytest.mark.asyncio
async def test_dict_missing_name_raises(client):
    """Dict without 'name' key raises ValueError."""
    queue = "cfg_no_name"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(ValueError, match="name"):
        client.start([{"max_workers": 10}])


# -- Additional validation: rate_limit wrong type --


@pytest.mark.asyncio
async def test_rate_limit_wrong_type_raises(client):
    """rate_limit that's not a (float, int) tuple raises TypeError."""
    queue = "cfg_bad_rl"

    @client.worker(ConfigTestJob, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(TypeError, match="rate_limit must be a"):
        client.start(
            [{"name": queue, "max_workers": 10, "rate_limit": "fast"}]
        )

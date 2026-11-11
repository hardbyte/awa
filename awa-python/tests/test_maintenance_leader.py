"""client.start(maintenance_leader=False): a runtime that never contends for
the maintenance advisory lock, even when it is the only runtime."""

import asyncio
import os
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
QUEUE = "maintenance_leader_optout"
RUNTIME_START_KWARGS = {
    "leader_election_interval_ms": 50,
    "queue_storage_queue_rotate_interval_ms": 60_000,
}
# Many election intervals: long enough that an opted-out runtime that did
# contend would have taken the free lock.
VACANT_WINDOW_S = 1.5
LEADER_LOCK_HOLDERS = """
    SELECT count(*)::bigint AS holders
    FROM pg_locks
    WHERE locktype = 'advisory' AND granted
      AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND ((classid::bigint << 32) | objid::bigint) = 4708303813012965710
"""


@dataclass
class LeaderOptOutProbe:
    n: int


async def new_client():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    @client.task(LeaderOptOutProbe, queue=QUEUE)
    async def handle(job):
        return None

    return client


@pytest.fixture
async def clients():
    created = []

    async def make():
        client = await new_client()
        created.append(client)
        return client

    try:
        yield make
    finally:
        for client in created:
            await client.shutdown()
            await client.close()


async def lock_holders(client) -> int:
    tx = await client.transaction()
    row = await tx.fetch_one(LEADER_LOCK_HOLDERS)
    await tx.commit()
    return row["holders"]


async def wait_until(predicate, timeout_s=10.0):
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not await predicate():
        assert asyncio.get_running_loop().time() < deadline, "timed out"
        await asyncio.sleep(0.02)


async def assert_lock_stays_free(opted_out):
    deadline = asyncio.get_running_loop().time() + VACANT_WINDOW_S
    while asyncio.get_running_loop().time() < deadline:
        assert await lock_holders(opted_out) == 0, "the leader lock was taken"
        assert (await opted_out.health_check()).leader is False
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_opted_out_runtime_never_leads_alone_and_still_runs_jobs(clients):
    opted_out = await clients()
    # Another test's runtime may still hold the lock for a moment.
    await wait_until(lambda: _is_zero(lock_holders(opted_out)))
    await opted_out.start(
        [(QUEUE, 2)], maintenance_leader=False, **RUNTIME_START_KWARGS
    )

    await assert_lock_stays_free(opted_out)
    health = await opted_out.health_check()
    assert health.healthy is True
    assert health.heartbeat_alive is True

    job = await opted_out.insert(LeaderOptOutProbe(n=1), queue=QUEUE)

    async def completed():
        return (await opted_out.get_job(job.id)).state == awa.JobState.Completed

    await wait_until(completed)


@pytest.mark.asyncio
async def test_eligible_runtime_leads_and_leadership_waits_for_its_return(clients):
    opted_out = await clients()
    await wait_until(lambda: _is_zero(lock_holders(opted_out)))
    await opted_out.start(
        [(QUEUE, 1)], maintenance_leader=False, **RUNTIME_START_KWARGS
    )
    eligible = await clients()
    await eligible.start([(QUEUE, 1)], **RUNTIME_START_KWARGS)

    async def eligible_leads():
        return (await eligible.health_check()).leader

    await wait_until(eligible_leads)
    assert (await opted_out.health_check()).leader is False

    await eligible.shutdown()
    await wait_until(lambda: _is_zero(lock_holders(opted_out)))
    await assert_lock_stays_free(opted_out)

    returning = await clients()
    await returning.start([(QUEUE, 1)], **RUNTIME_START_KWARGS)

    async def returning_leads():
        return (await returning.health_check()).leader

    await wait_until(returning_leads)
    assert (await opted_out.health_check()).leader is False


async def _is_zero(awaitable) -> bool:
    return await awaitable == 0

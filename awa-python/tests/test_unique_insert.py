"""Tests for unique job insert from Python.

Covers: unique_opts parameter on insert(), by_queue, by_args, by_period,
duplicate rejection, cross-language hash consistency, and edge cases with
various hashable arg types (unicode, nested dicts, empty args, large args).
"""

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
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'uniq_%'")
    await tx.commit()
    return c


@dataclass
class SendEmail:
    to: str
    subject: str


@dataclass
class SimpleJob:
    value: int


# ── Happy path: first insert succeeds ────────────────────────────────


@pytest.mark.asyncio
async def test_unique_insert_first_succeeds(client):
    """First insert with unique_opts succeeds and returns a job."""
    job = await client.insert(
        SendEmail(to="alice@example.com", subject="Hello"),
        queue="uniq_happy",
        unique_opts={"by_args": True},
    )
    assert job.id > 0
    assert job.state == awa.JobState.Available


# ── Duplicate rejected ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unique_duplicate_rejected(client):
    """Second insert with same unique key raises UniqueConflict."""
    await client.insert(
        SendEmail(to="bob@example.com", subject="Hi"),
        queue="uniq_dup",
        unique_opts={"by_args": True},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            SendEmail(to="bob@example.com", subject="Hi"),
            queue="uniq_dup",
            unique_opts={"by_args": True},
        )


# ── by_queue: same args, different queue ─────────────────────────────


@pytest.mark.asyncio
async def test_unique_by_queue_different_queues_allowed(client):
    """With by_queue=True, same args in different queues are distinct."""
    await client.insert(
        SendEmail(to="carol@example.com", subject="A"),
        queue="uniq_q1",
        unique_opts={"by_queue": True, "by_args": True},
    )

    # Same args, different queue — should succeed
    job2 = await client.insert(
        SendEmail(to="carol@example.com", subject="A"),
        queue="uniq_q2",
        unique_opts={"by_queue": True, "by_args": True},
    )
    assert job2.id > 0


@pytest.mark.asyncio
async def test_unique_by_queue_same_queue_rejected(client):
    """With by_queue=True, same args in same queue are rejected."""
    await client.insert(
        SendEmail(to="dave@example.com", subject="B"),
        queue="uniq_q_same",
        unique_opts={"by_queue": True, "by_args": True},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            SendEmail(to="dave@example.com", subject="B"),
            queue="uniq_q_same",
            unique_opts={"by_queue": True, "by_args": True},
        )


# ── by_args: different args allowed ──────────────────────────────────


@pytest.mark.asyncio
async def test_unique_by_args_different_args_allowed(client):
    """With by_args=True, different args in same queue are distinct."""
    await client.insert(
        SendEmail(to="eve@example.com", subject="X"),
        queue="uniq_args",
        unique_opts={"by_args": True},
    )

    job2 = await client.insert(
        SendEmail(to="frank@example.com", subject="Y"),
        queue="uniq_args",
        unique_opts={"by_args": True},
    )
    assert job2.id > 0


# ── by_period: time-bucketed uniqueness ──────────────────────────────


@pytest.mark.asyncio
async def test_unique_by_period_same_bucket_rejected(client):
    """With by_period, same args in same time bucket are rejected."""
    import time

    # Use a large bucket so both inserts land in the same period
    period = 3600  # 1 hour
    bucket = int(time.time()) // period

    await client.insert(
        SimpleJob(value=1),
        queue="uniq_period",
        unique_opts={"by_args": True, "by_period": bucket},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            SimpleJob(value=1),
            queue="uniq_period",
            unique_opts={"by_args": True, "by_period": bucket},
        )


@pytest.mark.asyncio
async def test_unique_by_period_different_bucket_allowed(client):
    """With by_period, same args in different buckets are allowed."""
    await client.insert(
        SimpleJob(value=2),
        queue="uniq_period_diff",
        unique_opts={"by_args": True, "by_period": 1000},
    )

    job2 = await client.insert(
        SimpleJob(value=2),
        queue="uniq_period_diff",
        unique_opts={"by_args": True, "by_period": 1001},
    )
    assert job2.id > 0


# ── Args with various hashable types ─────────────────────────────────


@dataclass
class UnicodeJob:
    emoji: str
    cjk: str


@pytest.mark.asyncio
async def test_unique_unicode_args(client):
    """Unicode args hash consistently — duplicate detected."""
    await client.insert(
        UnicodeJob(emoji="🎉🚀", cjk="日本語テスト"),
        queue="uniq_unicode",
        unique_opts={"by_args": True},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            UnicodeJob(emoji="🎉🚀", cjk="日本語テスト"),
            queue="uniq_unicode",
            unique_opts={"by_args": True},
        )


@dataclass
class NestedJob:
    config: dict


@pytest.mark.asyncio
async def test_unique_nested_dict_args(client):
    """Nested dict args hash consistently — duplicate detected."""
    nested = {"level1": {"level2": {"key": "value", "list": [1, 2, 3]}}}

    await client.insert(
        NestedJob(config=nested),
        queue="uniq_nested",
        unique_opts={"by_args": True},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            NestedJob(config=nested),
            queue="uniq_nested",
            unique_opts={"by_args": True},
        )


@dataclass
class EmptyJob:
    pass


@pytest.mark.asyncio
async def test_unique_empty_args(client):
    """Empty args hash consistently — duplicate detected."""
    await client.insert(
        EmptyJob(),
        queue="uniq_empty",
        unique_opts={"by_args": True},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            EmptyJob(),
            queue="uniq_empty",
            unique_opts={"by_args": True},
        )


@dataclass
class LargePayloadJob:
    data: str


@pytest.mark.asyncio
async def test_unique_large_args(client):
    """Large args (~100KB) hash consistently without error."""
    large_data = "x" * 100_000

    await client.insert(
        LargePayloadJob(data=large_data),
        queue="uniq_large",
        unique_opts={"by_args": True},
    )

    with pytest.raises(awa.UniqueConflict):
        await client.insert(
            LargePayloadJob(data=large_data),
            queue="uniq_large",
            unique_opts={"by_args": True},
        )


# ── Without unique_opts: no dedup ───────────────────────────────────


@pytest.mark.asyncio
async def test_no_unique_opts_allows_duplicates(client):
    """Without unique_opts, duplicate args are allowed."""
    job1 = await client.insert(
        SendEmail(to="grace@example.com", subject="Z"),
        queue="uniq_none",
    )
    job2 = await client.insert(
        SendEmail(to="grace@example.com", subject="Z"),
        queue="uniq_none",
    )
    assert job1.id != job2.id


# ── Sync variant ─────────────────────────────────────────────────────


def test_unique_insert_sync():
    """Sync insert with unique_opts works."""
    c = awa.Client(DATABASE_URL)
    c.migrate()
    tx = c.transaction()
    tx.execute("DELETE FROM awa.jobs WHERE queue = 'uniq_sync'")
    tx.commit()

    job = c.insert(
        SendEmail(to="heidi@example.com", subject="Sync"),
        queue="uniq_sync",
        unique_opts={"by_args": True},
    )
    assert job.id > 0

    with pytest.raises(awa.UniqueConflict):
        c.insert(
            SendEmail(to="heidi@example.com", subject="Sync"),
            queue="uniq_sync",
            unique_opts={"by_args": True},
        )

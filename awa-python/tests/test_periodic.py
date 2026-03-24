"""Tests for periodic (cron) job registration and execution from Python."""

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
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'periodic_%'")
    await tx.execute("DELETE FROM awa.cron_jobs WHERE name LIKE 'test_%'")
    await tx.commit()
    return c


@dataclass
class DailyReport:
    format: str


@dataclass
class HourlySync:
    source: str


# ── Registration ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_periodic_registration(client):
    """periodic() registers a cron schedule that appears in cron_jobs table."""
    queue = "periodic_reg"

    @client.worker(DailyReport, queue=queue)
    async def handle(job):
        return None

    client.periodic(
        name="test_daily_report",
        cron_expr="0 9 * * *",
        args_type=DailyReport,
        args=DailyReport(format="pdf"),
        timezone="Pacific/Auckland",
        queue=queue,
    )

    # Start briefly to trigger cron sync
    client.start([(queue, 1)])
    await asyncio.sleep(2.0)
    await client.shutdown()

    # Verify cron_jobs table has the schedule
    tx = await client.transaction()
    row = await tx.fetch_optional(
        "SELECT name, cron_expr, timezone, kind, queue, args FROM awa.cron_jobs WHERE name = $1",
        "test_daily_report",
    )
    await tx.commit()

    assert row is not None, "cron job should be synced to database"
    assert row["cron_expr"] == "0 9 * * *"
    assert row["timezone"] == "Pacific/Auckland"
    assert row["kind"] == "daily_report"
    assert row["queue"] == queue


@pytest.mark.asyncio
async def test_periodic_with_metadata_and_tags(client):
    """periodic() propagates metadata and tags to the cron_jobs table."""
    queue = "periodic_meta"

    @client.worker(HourlySync, queue=queue)
    async def handle(job):
        return None

    client.periodic(
        name="test_hourly_sync",
        cron_expr="0 * * * *",
        args_type=HourlySync,
        args=HourlySync(source="api"),
        queue=queue,
        priority=1,
        max_attempts=3,
        tags=["sync", "hourly"],
        metadata={"team": "platform"},
    )

    client.start([(queue, 1)])
    await asyncio.sleep(2.0)
    await client.shutdown()

    tx = await client.transaction()
    row = await tx.fetch_optional(
        "SELECT priority, max_attempts, tags, metadata FROM awa.cron_jobs WHERE name = $1",
        "test_hourly_sync",
    )
    await tx.commit()

    assert row is not None
    assert row["priority"] == 1
    assert row["max_attempts"] == 3
    assert row["tags"] == ["sync", "hourly"]
    assert row["metadata"]["team"] == "platform"


@pytest.mark.asyncio
async def test_periodic_invalid_cron_expr(client):
    """periodic() with invalid cron expression raises an error."""
    queue = "periodic_invalid"

    @client.worker(DailyReport, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(awa.ValidationError):
        client.periodic(
            name="test_bad_cron",
            cron_expr="not a cron expression",
            args_type=DailyReport,
            args=DailyReport(format="csv"),
            queue=queue,
        )


@pytest.mark.asyncio
async def test_periodic_invalid_timezone(client):
    """periodic() with invalid timezone raises an error."""
    queue = "periodic_bad_tz"

    @client.worker(DailyReport, queue=queue)
    async def handle(job):
        return None

    with pytest.raises(awa.ValidationError):
        client.periodic(
            name="test_bad_tz",
            cron_expr="0 9 * * *",
            args_type=DailyReport,
            args=DailyReport(format="csv"),
            timezone="Not/A/Timezone",
            queue=queue,
        )


# ── Execution ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.slow
async def test_periodic_fires_and_executes(client):
    """A periodic job with a per-minute schedule fires and the handler runs.

    Uses '* * * * *' (every minute). The cron evaluator finds the most
    recent past fire time and enqueues immediately once the leader is
    elected. Leader election takes up to 10s (default interval).
    """
    queue = "periodic_fire"
    executed = []

    @client.worker(DailyReport, queue=queue)
    async def handle(job):
        executed.append(job.args)
        return None

    client.periodic(
        name="test_every_minute",
        cron_expr="* * * * *",
        args_type=DailyReport,
        args=DailyReport(format="html"),
        queue=queue,
    )

    # Use fast leader election so the cron job fires promptly.
    # Default 10s election interval makes this test timing-dependent.
    client.start([(queue, 1)], leader_election_interval_ms=100, heartbeat_interval_ms=100)

    # Poll until the job fires or timeout.
    for _ in range(60):
        await asyncio.sleep(0.5)
        if len(executed) >= 1:
            break

    await client.shutdown()

    assert len(executed) >= 1, f"periodic job should have fired at least once, got {len(executed)}"
    # args comes through as the deserialized dataclass from the dispatch bridge
    args = executed[0]
    if isinstance(args, dict):
        assert args["format"] == "html"
    else:
        assert args.format == "html"

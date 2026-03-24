"""Integration tests for ORM/driver transaction bridging.

Tests insert_job() with asyncpg and psycopg3 (async + sync),
verifying that jobs inserted via external connections are visible
to Awa and that transactional atomicity is preserved.
"""

import os
from dataclasses import dataclass

import pytest

import awa
from awa.bridge import insert_job, insert_job_sync

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

# Parse components for asyncpg (which doesn't accept full URLs with ?params)
_DSN = DATABASE_URL.split("?")[0]


@dataclass
class BridgeEmail:
    to: str
    subject: str


@dataclass
class BridgePayment:
    order_id: str
    amount_cents: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def awa_client():
    """Awa sync client for verifying job insertion."""
    c = awa.Client(DATABASE_URL)
    c.migrate()
    return c


# ---------------------------------------------------------------------------
# asyncpg tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asyncpg_insert_job(awa_client):
    """Insert a job via asyncpg connection and verify with Awa client."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    try:
        async with conn.transaction():
            row = await insert_job(
                conn,
                BridgeEmail(to="asyncpg@example.com", subject="Hello"),
                queue="bridge_asyncpg",
                tags=["bridge", "asyncpg"],
                metadata={"source": "test"},
            )

        assert row["id"] > 0
        assert row["kind"] == "bridge_email"
        assert row["queue"] == "bridge_asyncpg"
        assert row["state"] == "available"

        # Verify via Awa client
        job = awa_client.get_job(row["id"])
        assert job.kind == "bridge_email"
        assert job.queue == "bridge_asyncpg"
        assert job.state == awa.JobState.Available
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_asyncpg_transaction_rollback(awa_client):
    """Jobs inserted in a rolled-back asyncpg transaction should not exist."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    job_id = None
    try:
        try:
            async with conn.transaction():
                row = await insert_job(
                    conn,
                    BridgeEmail(to="rollback@example.com", subject="Gone"),
                    queue="bridge_rollback",
                )
                job_id = row["id"]
                raise ValueError("force rollback")
        except ValueError:
            pass

        # Job should NOT exist
        assert job_id is not None
        count = await conn.fetchval(
            "SELECT count(*) FROM awa.jobs WHERE id = $1", job_id
        )
        assert count == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_asyncpg_insert_with_custom_kind(awa_client):
    """Explicit kind parameter overrides auto-derivation."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    try:
        async with conn.transaction():
            row = await insert_job(
                conn,
                BridgeEmail(to="custom@example.com", subject="Kind"),
                kind="custom_email_kind",
            )

        assert row["kind"] == "custom_email_kind"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_asyncpg_insert_dict_args(awa_client):
    """Dict args work with explicit kind."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    try:
        async with conn.transaction():
            row = await insert_job(
                conn,
                {"to": "dict@example.com", "urgent": True},
                kind="dict_email",
                queue="bridge_dict",
            )

        assert row["kind"] == "dict_email"
        assert row["queue"] == "bridge_dict"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_asyncpg_mixed_with_app_sql(awa_client):
    """Insert app data and Awa job in the same asyncpg transaction."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    try:
        # Create a temp table for app data
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS bridge_test_orders "
            "(id SERIAL PRIMARY KEY, email TEXT NOT NULL)"
        )

        async with conn.transaction():
            # App insert
            order_id = await conn.fetchval(
                "INSERT INTO bridge_test_orders (email) VALUES ($1) RETURNING id",
                "mixed@example.com",
            )
            # Awa job insert in same transaction
            row = await insert_job(
                conn,
                BridgePayment(order_id=str(order_id), amount_cents=4999),
                queue="bridge_mixed",
                metadata={"order_id": order_id},
            )

        assert row["kind"] == "bridge_payment"
        job = awa_client.get_job(row["id"])
        assert job.state == awa.JobState.Available
    finally:
        await conn.execute("DROP TABLE IF EXISTS bridge_test_orders")
        await conn.close()


# ---------------------------------------------------------------------------
# psycopg3 async tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_psycopg_async_insert_job(awa_client):
    """Insert a job via psycopg3 AsyncConnection."""
    import psycopg

    async with await psycopg.AsyncConnection.connect(
        _DSN, row_factory=psycopg.rows.dict_row
    ) as conn:
        async with conn.transaction():
            row = await insert_job(
                conn,
                BridgeEmail(to="psycopg-async@example.com", subject="Hello"),
                queue="bridge_psycopg_async",
                tags=["bridge", "psycopg"],
            )

    assert row["id"] > 0
    assert row["kind"] == "bridge_email"
    assert row["queue"] == "bridge_psycopg_async"
    assert row["state"] == "available"

    job = awa_client.get_job(row["id"])
    assert job.kind == "bridge_email"


@pytest.mark.asyncio
async def test_psycopg_async_rollback(awa_client):
    """Jobs inserted in a rolled-back psycopg async transaction should not exist."""
    import psycopg

    job_id = None
    async with await psycopg.AsyncConnection.connect(
        _DSN, row_factory=psycopg.rows.dict_row
    ) as conn:
        try:
            async with conn.transaction():
                row = await insert_job(
                    conn,
                    BridgeEmail(to="rollback@example.com", subject="Gone"),
                    queue="bridge_psycopg_rollback",
                )
                job_id = row["id"]
                raise ValueError("force rollback")
        except ValueError:
            pass

        # Verify rolled back
        async with conn.transaction():
            cur = await conn.execute(
                "SELECT count(*)::bigint AS cnt FROM awa.jobs WHERE id = %s", (job_id,)
            )
            row = await cur.fetchone()
            assert row["cnt"] == 0


# ---------------------------------------------------------------------------
# psycopg3 sync tests
# ---------------------------------------------------------------------------


def test_psycopg_sync_insert_job(awa_client):
    """Insert a job via psycopg3 sync Connection."""
    import psycopg

    with psycopg.Connection.connect(
        _DSN, row_factory=psycopg.rows.dict_row
    ) as conn:
        with conn.transaction():
            row = insert_job_sync(
                conn,
                BridgeEmail(to="psycopg-sync@example.com", subject="Hello"),
                queue="bridge_psycopg_sync",
                tags=["bridge", "psycopg-sync"],
            )

    assert row["id"] > 0
    assert row["kind"] == "bridge_email"
    assert row["queue"] == "bridge_psycopg_sync"
    assert row["state"] == "available"

    job = awa_client.get_job(row["id"])
    assert job.kind == "bridge_email"


def test_psycopg_sync_rollback(awa_client):
    """Jobs inserted in a rolled-back psycopg sync transaction should not exist."""
    import psycopg

    job_id = None
    with psycopg.Connection.connect(
        _DSN, row_factory=psycopg.rows.dict_row
    ) as conn:
        try:
            with conn.transaction():
                row = insert_job_sync(
                    conn,
                    BridgeEmail(to="rollback@example.com", subject="Gone"),
                    queue="bridge_sync_rollback",
                )
                job_id = row["id"]
                raise ValueError("force rollback")
        except ValueError:
            pass

        # Verify rolled back
        with conn.transaction():
            cur = conn.execute(
                "SELECT count(*)::bigint AS cnt FROM awa.jobs WHERE id = %s", (job_id,)
            )
            assert cur.fetchone()["cnt"] == 0


def test_psycopg_sync_mixed_with_app_sql(awa_client):
    """Insert app data and Awa job in the same psycopg3 sync transaction."""
    import psycopg

    with psycopg.Connection.connect(
        _DSN, row_factory=psycopg.rows.dict_row
    ) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bridge_test_sync_orders "
            "(id SERIAL PRIMARY KEY, email TEXT NOT NULL)"
        )

        with conn.transaction():
            cur = conn.execute(
                "INSERT INTO bridge_test_sync_orders (email) VALUES (%s) RETURNING id",
                ("sync-mixed@example.com",),
            )
            order_row = cur.fetchone()
            order_id = order_row["id"]

            row = insert_job_sync(
                conn,
                BridgePayment(order_id=str(order_id), amount_cents=1299),
                queue="bridge_sync_mixed",
                metadata={"order_id": order_id},
            )

        assert row["kind"] == "bridge_payment"

        # Commit so the Awa client (separate connection) can see the row
        conn.commit()

        job = awa.Client(DATABASE_URL).get_job(row["id"])
        assert job.state == awa.JobState.Available

        conn.execute("DROP TABLE IF EXISTS bridge_test_sync_orders")
        conn.commit()


# ---------------------------------------------------------------------------
# Edge cases / validation
# ---------------------------------------------------------------------------


def test_unsupported_connection_type():
    """Passing an unsupported connection type raises TypeError."""
    with pytest.raises(TypeError, match="Unsupported connection type"):
        insert_job_sync("not a connection", BridgeEmail(to="x", subject="y"))


def test_dict_args_without_kind():
    """Dict args without a kind raises TypeError."""
    with pytest.raises(TypeError, match="Dict args require"):
        awa.bridge._derive_kind({"foo": "bar"}, None)


def test_serialize_null_bytes():
    """Null bytes in args are rejected."""
    with pytest.raises(ValueError, match="null bytes"):
        awa.bridge._serialize_args({"key": "val\x00ue"})


@pytest.mark.asyncio
async def test_asyncpg_insert_scheduled_job():
    """A job with future run_at should be in 'scheduled' state."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    try:
        async with conn.transaction():
            row = await insert_job(
                conn,
                BridgeEmail(to="sched@example.com", subject="Later"),
                queue="bridge_scheduled",
                run_at="2099-01-01T00:00:00+00:00",
            )

        assert row["state"] == "scheduled"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_asyncpg_insert_priority_and_max_attempts():
    """Custom priority and max_attempts are respected."""
    import asyncpg

    conn = await asyncpg.connect(_DSN)
    try:
        async with conn.transaction():
            row = await insert_job(
                conn,
                BridgeEmail(to="prio@example.com", subject="Priority"),
                priority=1,
                max_attempts=5,
            )

        assert row["priority"] == 1
        assert row["max_attempts"] == 5
    finally:
        await conn.close()

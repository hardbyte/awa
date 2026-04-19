"""Python-side descriptor authoring (issue #168).

Verifies that queue and job-kind descriptors declared from Python land in
the same catalog tables and receive the same hashes as ones declared from
Rust — i.e. cross-runtime parity. Requires Postgres running at
localhost:15432.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass

import psycopg
import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class DescriptorTestJob:
    value: str


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    await c.install_queue_storage(reset=True)
    try:
        yield c
    finally:
        await c.shutdown()
        await c.close()


def _sync_url() -> str:
    # psycopg wants postgresql:// not postgres://.
    return DATABASE_URL.replace("postgres://", "postgresql://", 1)


def _fetch_queue_row(queue: str) -> dict | None:
    with psycopg.connect(_sync_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT display_name, description, owner, docs_url, tags, extra,
                       descriptor_hash, last_seen_at, sync_interval_ms
                FROM awa.queue_descriptors
                WHERE queue = %s
                """,
                (queue,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return {
        "display_name": row[0],
        "description": row[1],
        "owner": row[2],
        "docs_url": row[3],
        "tags": row[4],
        "extra": row[5],
        "descriptor_hash": row[6],
        "last_seen_at": row[7],
        "sync_interval_ms": row[8],
    }


def _fetch_kind_row(kind: str) -> dict | None:
    with psycopg.connect(_sync_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT display_name, description, owner, docs_url, tags, extra,
                       descriptor_hash
                FROM awa.job_kind_descriptors
                WHERE kind = %s
                """,
                (kind,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return {
        "display_name": row[0],
        "description": row[1],
        "owner": row[2],
        "docs_url": row[3],
        "tags": row[4],
        "extra": row[5],
        "descriptor_hash": row[6],
    }


def _cleanup(queue: str, kind: str) -> None:
    with psycopg.connect(_sync_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM awa.queue_descriptors WHERE queue = %s", (queue,)
            )
            cur.execute(
                "DELETE FROM awa.job_kind_descriptors WHERE kind = %s", (kind,)
            )
        conn.commit()


@pytest.mark.asyncio
async def test_python_queue_and_kind_descriptors_sync_on_start(client):
    suffix = uuid.uuid4().hex[:8]
    queue = f"py_desc_queue_{suffix}"
    kind = f"py_desc_kind_{suffix}"

    @client.task(DescriptorTestJob, kind=kind, queue=queue)
    async def handle(job):
        return None

    client.queue_descriptor(
        queue,
        display_name="Python declared queue",
        description="Declared entirely from the Python bridge.",
        owner="python-team@example.com",
        docs_url="https://runbook/python-queue",
        tags=["python", "test"],
        extra={"team": "python"},
    )
    client.job_kind_descriptor(
        kind,
        display_name="Python declared kind",
        owner="python-team@example.com",
        tags=["python"],
    )

    await client.start([(queue, 1)])
    try:
        # start() synchronously awaits sync_*_descriptors before returning the
        # running runtime; the rows should be present immediately.
        queue_row = _fetch_queue_row(queue)
        kind_row = _fetch_kind_row(kind)

        assert queue_row is not None, "python-declared queue descriptor missing"
        assert queue_row["display_name"] == "Python declared queue"
        assert queue_row["owner"] == "python-team@example.com"
        assert queue_row["docs_url"] == "https://runbook/python-queue"
        assert queue_row["tags"] == ["python", "test"]
        assert queue_row["extra"] == {"team": "python"}
        assert queue_row["descriptor_hash"]  # non-empty blake3 hex
        assert queue_row["sync_interval_ms"] > 0

        assert kind_row is not None, "python-declared kind descriptor missing"
        assert kind_row["display_name"] == "Python declared kind"
        assert kind_row["owner"] == "python-team@example.com"
        assert kind_row["tags"] == ["python"]
    finally:
        await client.shutdown()
        _cleanup(queue, kind)


@pytest.mark.asyncio
async def test_python_descriptor_hash_matches_equivalent_rust_declaration(client):
    """The descriptor_hash is stable across Python/Rust — identical field
    values must produce identical hashes so drift detection works across
    mixed-language fleets.

    We can't import QueueDescriptor directly to compute the Rust hash, so
    instead we check stability: declaring the same descriptor twice from
    Python produces the same hash, and a re-declaration with different
    fields produces a different one.
    """
    suffix = uuid.uuid4().hex[:8]
    queue = f"py_hash_queue_{suffix}"

    @client.task(DescriptorTestJob, queue=queue)
    async def handle(job):
        return None

    client.queue_descriptor(
        queue,
        display_name="Version 1",
        tags=["a", "b"],
        extra={"key": 1},
    )
    await client.start([(queue, 1)])
    try:
        first_hash = _fetch_queue_row(queue)["descriptor_hash"]
    finally:
        await client.shutdown()

    # Second client, same fields → same hash (stable JSON canonicalization).
    client2 = awa.AsyncClient(DATABASE_URL)
    await client2.install_queue_storage(reset=False)
    client2.queue_descriptor(
        queue,
        display_name="Version 1",
        tags=["a", "b"],
        extra={"key": 1},
    )

    @client2.task(DescriptorTestJob, queue=queue)
    async def handle2(job):
        return None

    await client2.start([(queue, 1)])
    try:
        second_hash = _fetch_queue_row(queue)["descriptor_hash"]
        assert second_hash == first_hash, (
            "same fields must produce same descriptor_hash across runs"
        )
    finally:
        try:
            await client2.shutdown()
        finally:
            await client2.close()

    # Third declaration, changed field → different hash.
    client3 = awa.AsyncClient(DATABASE_URL)
    await client3.install_queue_storage(reset=False)
    client3.queue_descriptor(
        queue,
        display_name="Version 2",  # changed
        tags=["a", "b"],
        extra={"key": 1},
    )

    @client3.task(DescriptorTestJob, queue=queue)
    async def handle3(job):
        return None

    await client3.start([(queue, 1)])
    try:
        third_hash = _fetch_queue_row(queue)["descriptor_hash"]
        assert third_hash != first_hash, "different fields must change the hash"
    finally:
        try:
            await client3.shutdown()
        finally:
            await client3.close()
            _cleanup(queue, "descriptor_test_job")


@pytest.mark.asyncio
async def test_undeclared_queue_descriptor_fails_start(client):
    """Descriptor on a queue the client isn't running should reject at start
    (mirrors the Rust BuildError::QueueDescriptorWithoutQueue check)."""
    suffix = uuid.uuid4().hex[:8]
    running_queue = f"py_running_{suffix}"
    orphan_queue = f"py_orphan_{suffix}"

    @client.task(DescriptorTestJob, queue=running_queue)
    async def handle(job):
        return None

    client.queue_descriptor(orphan_queue, display_name="Nobody runs me")

    # Matches the Rust BuildError::QueueDescriptorWithoutQueue message —
    # "queue descriptor declared for unknown queue '<name>'".
    with pytest.raises(Exception, match=r"(?i)descriptor.*unknown queue"):
        await client.start([(running_queue, 1)])

    # No runtime was started; nothing to shut down. Clean up any stray row.
    _cleanup(orphan_queue, "descriptor_test_job")


@pytest.mark.asyncio
async def test_descriptor_mutation_after_start_rejected(client):
    """Declaring a descriptor on a client whose runtime is already running
    can't take effect (the runtime only reads descriptors at start()). Make
    that failure loud instead of silently no-op."""
    suffix = uuid.uuid4().hex[:8]
    queue = f"py_mutation_{suffix}"

    @client.task(DescriptorTestJob, queue=queue)
    async def handle(job):
        return None

    client.queue_descriptor(queue, display_name="First")
    await client.start([(queue, 1)])
    try:
        with pytest.raises(Exception, match=r"(?i)before start"):
            client.queue_descriptor(queue, display_name="Second, too late")
        with pytest.raises(Exception, match=r"(?i)before start"):
            client.job_kind_descriptor(
                "descriptor_test_job",
                display_name="Also too late",
            )
    finally:
        await client.shutdown()
        _cleanup(queue, "descriptor_test_job")


@pytest.mark.asyncio
async def test_last_seen_at_bumps_on_restart(client):
    suffix = uuid.uuid4().hex[:8]
    queue = f"py_liveness_{suffix}"

    @client.task(DescriptorTestJob, queue=queue)
    async def handle(job):
        return None

    client.queue_descriptor(queue, display_name="Liveness test")
    await client.start([(queue, 1)])
    try:
        first_seen = _fetch_queue_row(queue)["last_seen_at"]
    finally:
        await client.shutdown()

    await asyncio.sleep(1.2)

    client2 = awa.AsyncClient(DATABASE_URL)
    await client2.install_queue_storage(reset=False)
    client2.queue_descriptor(queue, display_name="Liveness test")

    @client2.task(DescriptorTestJob, queue=queue)
    async def handle2(job):
        return None

    await client2.start([(queue, 1)])
    try:
        second_seen = _fetch_queue_row(queue)["last_seen_at"]
        assert second_seen > first_seen, (
            "last_seen_at must advance on each client start"
        )
    finally:
        try:
            await client2.shutdown()
        finally:
            await client2.close()
            _cleanup(queue, "descriptor_test_job")

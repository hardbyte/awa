"""awa.testing — pytest fixtures and helpers for testing Awa job handlers.

Usage:
    import pytest
    from awa.testing import awa_client

    @pytest.fixture
    async def client(awa_client):
        return awa_client

    async def test_send_email(awa_client):
        await awa_client.insert(SendEmail(to="a@b.com", subject="Hi"))
        result = await awa_client.work_one(SendEmail, handler=handle_send_email)
        assert result.is_completed()
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Awaitable

import pytest_asyncio

import awa
from awa._awa import derive_kind

# Default test database URL
DEFAULT_DATABASE_URL = "postgres://postgres:test@localhost:5432/awa_test"


def get_database_url() -> str:
    """Get the test database URL from environment or use default."""
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


@dataclass
class JobRow:
    """Lightweight representation of a job row from the database.

    Used instead of awa.Job (a Rust/PyO3 struct that cannot be constructed
    from Python) to carry job metadata through WorkResult.
    """

    id: int
    kind: str
    queue: str
    args: dict[str, Any]
    attempt: int
    max_attempts: int
    priority: int
    tags: list[str]
    state: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> JobRow:
        return cls(
            id=row["id"],
            kind=row["kind"],
            queue=row["queue"],
            args=row["args"],
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            priority=row["priority"],
            tags=row.get("tags") or [],
            state=row.get("state"),
        )


class _NoJob:
    """Sentinel indicating no job was available to claim."""

    def __repr__(self) -> str:
        return "NoJob"

    def __bool__(self) -> bool:
        return False


NoJob = _NoJob()
"""Singleton sentinel used as WorkResult.job when no job was found."""


@dataclass
class WorkResult:
    """Result of executing a single job via work_one.

    When no job is available, outcome is "no_job" and job is the NoJob sentinel.
    """

    job: JobRow | _NoJob
    outcome: str  # "completed", "retryable", "failed", "cancelled", "snoozed", "no_job"
    error: str | None = None

    def is_completed(self) -> bool:
        return self.outcome == "completed"

    def is_failed(self) -> bool:
        return self.outcome == "failed"

    def is_retryable(self) -> bool:
        return self.outcome == "retryable"

    def is_cancelled(self) -> bool:
        return self.outcome == "cancelled"

    def is_snoozed(self) -> bool:
        return self.outcome == "snoozed"

    def is_no_job(self) -> bool:
        return self.outcome == "no_job"


class AwaTestClient:
    """Test client for Awa job queue integration testing.

    Wraps a real awa.Client with helpers for claiming and executing
    individual jobs synchronously in tests.
    """

    def __init__(self, client: awa.Client):
        self._client = client

    @property
    def client(self) -> awa.Client:
        return self._client

    async def insert(self, args: Any, **kwargs: Any) -> awa.Job:
        """Insert a job (delegates to client.insert)."""
        return await self._client.insert(args, **kwargs)

    async def migrate(self) -> None:
        """Run migrations."""
        await self._client.migrate()

    async def clean(self) -> None:
        """Delete all jobs and queue metadata (for test isolation)."""
        tx = await self._client.transaction()
        await tx.execute("DELETE FROM awa.jobs", [])
        await tx.execute("DELETE FROM awa.queue_meta", [])
        await tx.commit()

    async def work_one(
        self,
        args_type: type,
        handler: Callable[[Any], Awaitable[Any]],
        *,
        kind: str | None = None,
        queue: str = "default",
    ) -> WorkResult:
        """Claim and execute a single job of the given type.

        Claims one available job matching the kind and queue, deserializes
        args, calls the handler, and updates the job state.

        Returns a WorkResult with outcome="no_job" if no matching job is
        available, rather than raising.
        """
        kind_str = kind or derive_kind(args_type.__name__)

        # Claim one job within a transaction.
        # Use try/except to handle the case where no rows match (fetch_one
        # raises when zero rows are returned).
        tx = await self._client.transaction()
        try:
            row = await tx.fetch_one(
                """
                WITH claimed AS (
                    SELECT id FROM awa.jobs
                    WHERE state = 'available' AND kind = $1 AND queue = $2
                    ORDER BY run_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE awa.jobs
                SET state = 'running',
                    attempt = attempt + 1,
                    attempted_at = now(),
                    heartbeat_at = now(),
                    deadline_at = now() + interval '5 minutes'
                FROM claimed
                WHERE awa.jobs.id = claimed.id
                RETURNING awa.jobs.id, awa.jobs.kind, awa.jobs.queue,
                          awa.jobs.args, awa.jobs.attempt, awa.jobs.max_attempts,
                          awa.jobs.priority, awa.jobs.tags, awa.jobs.state::text AS state
                """,
                [kind_str, queue],
            )
        except Exception:
            # No matching job available — fetch_one raises when 0 rows
            await tx.rollback()
            return WorkResult(job=NoJob, outcome="no_job")
        await tx.commit()

        # Capture the claimed job row so we can pass it through to WorkResult
        # without re-fetching (avoids the old placeholder bug).
        claimed_job = JobRow.from_row(row)
        job_id = claimed_job.id

        # Deserialize args
        args_data = row["args"]
        if hasattr(args_type, "model_validate"):
            # Pydantic
            args_instance = args_type.model_validate(args_data)
        elif hasattr(args_type, "__dataclass_fields__"):
            # Dataclass
            args_instance = args_type(**args_data)
        else:
            args_instance = args_data

        # Execute handler
        try:
            result = await handler(args_instance)
        except awa.TerminalError as e:
            # Terminal error -> failed
            tx3 = await self._client.transaction()
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
                [job_id],
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="failed", error=str(e))
        except Exception as e:
            # Retryable error
            tx3 = await self._client.transaction()
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'retryable', finalized_at = now() WHERE id = $1",
                [job_id],
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="retryable", error=str(e))

        # Handle return value
        tx3 = await self._client.transaction()
        if result is None:
            # Completed
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1",
                [job_id],
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="completed")
        elif isinstance(result, awa.RetryAfter):
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'retryable', finalized_at = now() WHERE id = $1",
                [job_id],
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="retryable")
        elif isinstance(result, awa.Snooze):
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'available', attempt = attempt - 1 WHERE id = $1",
                [job_id],
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="snoozed")
        elif isinstance(result, awa.Cancel):
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'cancelled', finalized_at = now() WHERE id = $1",
                [job_id],
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="cancelled", error=result.reason)
        else:
            await tx3.rollback()
            raise TypeError(f"Unexpected handler return type: {type(result)}")


async def _get_job(client: awa.Client, job_id: int) -> JobRow:
    """Fetch a job by ID and return a JobRow with its current state."""
    tx = await client.transaction()
    row = await tx.fetch_one(
        """
        SELECT id, kind, queue, args, attempt, max_attempts,
               priority, tags, state::text AS state
        FROM awa.jobs WHERE id = $1
        """,
        [job_id],
    )
    await tx.commit()
    return JobRow.from_row(row)


@pytest_asyncio.fixture
async def awa_client():
    """Pytest fixture providing a connected and migrated TestClient.

    Cleans up jobs between tests for isolation.
    """
    url = get_database_url()
    client = awa.Client(url)
    tc = AwaTestClient(client)
    await tc.migrate()
    await tc.clean()
    yield tc
    # Cleanup after test
    await tc.clean()

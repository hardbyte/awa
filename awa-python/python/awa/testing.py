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

import copy
import dataclasses
import json
import operator
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Generic, TypeVar

import pytest_asyncio

import awa
from awa._awa import _decode_output, _validate_output, derive_kind

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

T = TypeVar("T")
O = TypeVar("O")


_STAGING_KWARGS = frozenset(
    {
        "kind",
        "queue",
        "priority",
        "max_attempts",
        "tags",
        "metadata",
        "run_at",
        "unique_opts",
        "ordering_key",
    }
)


def _compact_len(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())


def _str_option(kwargs: dict[str, Any], name: str, default: str) -> str:
    value = kwargs.get(name)
    if value is None:
        return default
    if not isinstance(value, str):
        raise awa.StagingError(f"{name} must be a str, not {type(value).__name__}")
    return value


def _int_option(kwargs: dict[str, Any], name: str, default: int) -> int:
    # Like the runtime's conversion: anything with __index__, but no floats.
    value = kwargs.get(name)
    if value is None:
        return default
    try:
        value = operator.index(value)
    except TypeError:
        raise awa.StagingError(
            f"{name} must be an int, not {type(value).__name__}"
        ) from None
    if not -(2**15) <= value < 2**15:
        raise awa.StagingError(f"{name} {value} does not fit in 16 bits")
    return value


def _tags_option(kwargs: dict[str, Any]) -> list[str]:
    # Like the runtime's conversion: a sequence of str, but not a str itself.
    value = kwargs.get("tags")
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise awa.StagingError(
            f"tags must be a list of str, not {type(value).__name__}"
        )
    if not all(isinstance(tag, str) for tag in value):
        raise awa.StagingError("tags must be a list of str")
    return list(value)


def _check_json(value: Any, name: str) -> None:
    """Reject what the runtime's JSON conversion or jsonb would reject."""
    if isinstance(value, str):
        if "\x00" in value:
            raise awa.StagingError(f"{name} must not contain NUL characters")
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise awa.StagingError(f"{name} keys must be str, not {type(key).__name__}")
            _check_json(key, name)
            _check_json(item, name)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_json(item, name)


def _validated_size(args: Any, kwargs: dict[str, Any]) -> int:
    """Apply the runtime's staging rules and return the request's size."""
    kind = _str_option(kwargs, "kind", "") or derive_kind(type(args).__name__)
    queue = _str_option(kwargs, "queue", "default")
    priority = _int_option(kwargs, "priority", 2)
    max_attempts = _int_option(kwargs, "max_attempts", 25)
    tags = _tags_option(kwargs)
    args_json = _args_to_json(args)
    metadata = kwargs.get("metadata") or {}
    _check_json(args_json, "args")
    _check_json(metadata, "metadata")
    if not 1 <= priority <= 4:
        raise awa.StagingError(f"priority {priority} is outside 1..=4")
    if not 1 <= max_attempts <= 1000:
        raise awa.StagingError(f"max_attempts {max_attempts} is outside 1..=1000")
    for name, value in (("kind", kind), ("queue", queue)):
        if len(value) > 200:
            raise awa.StagingError(f"{name} is longer than 200 characters")
    if len(tags) > 20:
        raise awa.StagingError("more than 20 tags")
    if any("\x00" in value for value in (kind, queue, *tags)):
        raise awa.StagingError("kind, queue and tags must not contain NUL bytes")
    ordering_key = kwargs.get("ordering_key") or b""
    if isinstance(ordering_key, str):
        ordering_key = ordering_key.encode()
    if not isinstance(ordering_key, (bytes, bytearray, memoryview)):
        raise awa.StagingError("ordering_key must be bytes-like or str")
    _check_unique_opts(kwargs.get("unique_opts"))
    _check_run_at(kwargs.get("run_at"))
    return (
        len(kind.encode())
        + len(queue.encode())
        + _compact_len(args_json)
        + _compact_len(metadata)
        + sum(len(tag.encode()) for tag in tags)
        + len(ordering_key)
    )


def _check_unique_opts(unique_opts: Any) -> None:
    """Mirror the runtime's ``unique_opts`` parsing."""
    if unique_opts is None:
        return
    if not isinstance(unique_opts, dict):
        raise awa.StagingError("unique_opts must be a dict")
    for key in ("by_queue", "by_args"):
        if key in unique_opts and not isinstance(unique_opts[key], bool):
            raise awa.StagingError(f"unique_opts[{key!r}] must be a bool")
    by_period = unique_opts.get("by_period")
    if by_period is not None and not (
        isinstance(by_period, int) and -(2**63) <= by_period < 2**63
    ):
        raise awa.StagingError("unique_opts['by_period'] must be a 64-bit int")
    states = unique_opts.get("states")
    if states is not None and not (isinstance(states, int) and 0 <= states <= 255):
        raise awa.StagingError("unique_opts['states'] must be an int in 0..=255")


def _check_run_at(run_at: Any) -> None:
    """Mirror the runtime's ``run_at`` parsing: a datetime or an RFC 3339 string."""
    if run_at is None:
        return
    text = run_at if isinstance(run_at, str) else getattr(run_at, "isoformat", None)
    if callable(text):
        text = text()
    if not isinstance(text, str):
        raise awa.StagingError("run_at must be a datetime or RFC3339 string")
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as e:
        raise awa.StagingError(f"run_at is not a valid timestamp: {e}") from e


def _args_to_json(args: Any) -> Any:
    if hasattr(args, "model_dump"):
        return args.model_dump(mode="json")
    if dataclasses.is_dataclass(args) and not isinstance(args, type):
        return dataclasses.asdict(args)
    return args


@dataclass
class StagedFollowUp:
    """A follow-up staged with ``HandlerJob.enqueue_on_completion``."""

    args: Any
    kwargs: dict[str, Any]


@dataclass
class HandlerJob(Generic[T]):
    """Python-side stand-in for the runtime job wrapper used by workers.

    ``enqueue_on_completion`` applies the runtime's validation and limits and
    records each request in ``staged``, so handler unit tests can assert on
    what a handler staged without a database. ``set_output`` validates with
    the runtime's own converter, and ``output`` reads back what was recorded.
    """

    args: T
    id: int
    attempt: int
    max_attempts: int
    queue: str
    priority: int
    deadline: datetime | None
    metadata: dict[str, Any]
    tags: list[str]
    staged: list[StagedFollowUp] = field(default_factory=list)
    staging_error: awa.StagingError | None = None
    _staged_bytes: int = 0
    _sealed: bool = False
    _output: Any = field(default=None, repr=False)
    # The type given to `task(..., output=...)`, which `set_output` checks.
    output_type: Any = field(default=None, repr=False)

    def is_cancelled(self) -> bool:
        return False

    @property
    def output(self) -> Any:
        """The output recorded so far (ADR-048), as JSON-compatible data.
        Each read returns a fresh copy, as on a real job."""
        return copy.deepcopy(self._output)

    def set_output(self, value: Any) -> None:
        """Record this attempt's output, as ``Job.set_output`` does: the same
        conversion and limit, ``None`` clears it, and a failure raises
        ``awa.OutputError`` and clears it."""
        if self._sealed:
            raise awa.OutputError(
                "the handler already returned; its output can no longer be recorded"
            )
        try:
            self._output = _validate_output(value, self.output_type)
        except awa.OutputError:
            self._output = None
            raise

    def clear_output(self) -> None:
        if not self._sealed:
            self._output = None

    def output_as(self, tp: type[O]) -> O | None:
        """The recorded output decoded as ``tp``, as ``Job.output_as`` does."""
        return _decode_output(tp, self._output)

    def enqueue_on_completion(self, args: Any, **kwargs: Any) -> None:
        unknown = set(kwargs) - _STAGING_KWARGS
        if unknown:
            # As at runtime, a bad keyword is a plain TypeError from the call.
            raise TypeError(f"unexpected keyword arguments: {sorted(unknown)}")
        if self._sealed:
            raise awa.StagingError(
                "the handler already returned; follow-ups can no longer be staged"
            )
        try:
            size = _validated_size(args, kwargs)
            if len(self.staged) >= awa.MAX_STAGED_FOLLOW_UPS:
                raise awa.StagingError(
                    f"too many staged follow-ups (limit {awa.MAX_STAGED_FOLLOW_UPS})"
                )
            if self._staged_bytes + size > awa.MAX_STAGED_BYTES:
                raise awa.StagingError(
                    f"staged follow-ups exceed {awa.MAX_STAGED_BYTES} bytes"
                )
        except awa.StagingError as e:
            self.staging_error = self.staging_error or e
            raise
        except Exception as e:
            error = awa.StagingError(f"follow-up could not be prepared: {e}")
            self.staging_error = self.staging_error or error
            raise error from e
        self._staged_bytes += size
        self.staged.append(StagedFollowUp(args=args, kwargs=dict(kwargs)))


@dataclass
class WorkResult:
    """Result of executing a single job via work_one.

    When no job is available, outcome is "no_job" and job is the NoJob sentinel.
    """

    job: JobRow | _NoJob
    outcome: str  # "completed", "retryable", "failed", "cancelled", "snoozed", "no_job"
    error: str | None = None
    # The output the handler recorded (ADR-048). This harness finalizes
    # canonical rows, which store no outputs, so it is reported here only.
    output: Any = None

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
        await tx.execute(
            """
            UPDATE awa.storage_transition_state
            SET current_engine = 'canonical',
                prepared_engine = NULL,
                state = 'canonical',
                transition_epoch = transition_epoch + 1,
                details = '{}'::jsonb,
                updated_at = now(),
                finalized_at = NULL
            WHERE singleton
            """
        )
        await tx.execute(
            "DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
        )
        await tx.execute("DELETE FROM awa.runtime_instances")
        await tx.execute("DELETE FROM awa.jobs")
        await tx.execute("DELETE FROM awa.queue_meta")
        await tx.commit()

    async def work_one(
        self,
        args_type: type,
        handler: Callable[[HandlerJob[Any]], Awaitable[Any]],
        *,
        kind: str | None = None,
        queue: str = "default",
        output: Any = None,
    ) -> WorkResult:
        """Claim and execute a single job of the given type.

        ``output`` is the type the handler's task declares, which
        ``set_output`` checks as the runtime does.

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
                -- Cast state to text: awa.job_state is a PG custom enum.
                -- sqlx's row_to_py_dict can't deserialize custom enums
                -- without explicit registration, so we cast to text.
                RETURNING awa.jobs.id, awa.jobs.kind, awa.jobs.queue,
                          awa.jobs.args, awa.jobs.attempt, awa.jobs.max_attempts,
                          awa.jobs.priority, awa.jobs.tags, awa.jobs.metadata,
                          awa.jobs.deadline_at AS deadline, awa.jobs.state::text AS state
                """,
                kind_str,
                queue,
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

        # Deserialize args and mirror the runtime job wrapper shape.
        try:
            args_data = row["args"]
            if hasattr(args_type, "model_validate"):
                args_instance = args_type.model_validate(args_data)
            elif hasattr(args_type, "__dataclass_fields__"):
                args_instance = args_type(**args_data)
            else:
                args_instance = args_data
        except Exception as e:
            tx3 = await self._client.transaction()
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="failed", error=str(e))

        deadline_raw = row.get("deadline")
        deadline = (
            datetime.fromisoformat(deadline_raw)
            if isinstance(deadline_raw, str)
            else deadline_raw
        )
        handler_job = HandlerJob(
            args=args_instance,
            id=job_id,
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            queue=row["queue"],
            priority=row["priority"],
            deadline=deadline,
            metadata=row.get("metadata") or {},
            tags=row.get("tags") or [],
            output_type=output,
        )

        # Execute handler
        try:
            result = await handler(handler_job)
        except awa.TerminalError as e:
            # Terminal error -> failed
            handler_job._sealed = True
            tx3 = await self._client.transaction()
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(
                job=job, outcome="failed", error=str(e), output=handler_job.output
            )
        except Exception as e:
            # Retryable error; the final attempt exhausts the job, mirroring
            # the real executor.
            handler_job._sealed = True
            new_state = (
                "failed" if row["attempt"] >= row["max_attempts"] else "retryable"
            )
            tx3 = await self._client.transaction()
            await tx3.execute(
                f"UPDATE awa.jobs SET state = '{new_state}', finalized_at = now() WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(
                job=job, outcome=new_state, error=str(e), output=handler_job.output
            )

        # ADR-046, as in the runtime: staging closes when the handler
        # returns, staged follow-ups ride only on completion, and a
        # completion after a failed stage fails the attempt terminally.
        handler_job._sealed = True
        if result is None and handler_job.staging_error is not None:
            error = f"a follow-up failed to stage: {handler_job.staging_error}"
            tx3 = await self._client.transaction()
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(
                job=job, outcome="failed", error=error, output=handler_job.output
            )

        # Handle return value
        tx3 = await self._client.transaction()
        if result is None:
            # Completed
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1",
                job_id,
            )
            for staged in handler_job.staged:
                # A savepoint per insert, so a duplicate unique key is
                # skipped without aborting the completion.
                await tx3.execute("SAVEPOINT awa_staged_follow_up")
                try:
                    await tx3.insert(staged.args, **staged.kwargs)
                except awa.UniqueConflict as e:
                    # Only the deduplication index means "already enqueued".
                    if "idx_awa_jobs_unique" not in str(e):
                        raise
                    await tx3.execute("ROLLBACK TO SAVEPOINT awa_staged_follow_up")
                else:
                    await tx3.execute("RELEASE SAVEPOINT awa_staged_follow_up")
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="completed", output=handler_job.output)
        elif isinstance(result, awa.RetryAfter):
            # RetryAfter reports a failed attempt; the final attempt exhausts
            # the job, mirroring the real executor.
            new_state = (
                "failed" if row["attempt"] >= row["max_attempts"] else "retryable"
            )
            await tx3.execute(
                f"UPDATE awa.jobs SET state = '{new_state}', finalized_at = now() WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome=new_state, output=handler_job.output)
        elif isinstance(result, awa.Snooze):
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'available', attempt = attempt - 1 WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(job=job, outcome="snoozed")
        elif isinstance(result, awa.Cancel):
            await tx3.execute(
                "UPDATE awa.jobs SET state = 'cancelled', finalized_at = now() WHERE id = $1",
                job_id,
            )
            await tx3.commit()
            job = await _get_job(self._client, job_id)
            return WorkResult(
                job=job,
                outcome="cancelled",
                error=result.reason,
                output=handler_job.output,
            )
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
        job_id,
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

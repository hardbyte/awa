"""
Pythonic client wrappers for awa.

Provides ``Client`` (sync, for producers) and ``AsyncClient`` (async, for
workers and async applications). Both delegate to the underlying PyO3
``_awa.Client`` but present a clean, un-suffixed API.

The raw ``_awa.Client`` remains available as ``RawClient`` for advanced use
or migration from the 0.4.x ``_sync`` pattern.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Awaitable, Callable, Literal, TypeVar

from awa._awa import (
    CallbackToken,
    Client as RawClient,
    DlqEntry,
    HealthCheck,
    Job,
    QueueStat,
    ResolveResult,
    SyncTransaction,
    Transaction,
)

T = TypeVar("T")

DEFAULT_QUEUE_STORAGE_SCHEMA = "awa"
DEFAULT_QUEUE_STORAGE_QUEUE_SLOT_COUNT = 16
DEFAULT_QUEUE_STORAGE_LEASE_SLOT_COUNT = 8
DEFAULT_QUEUE_STORAGE_CLAIM_SLOT_COUNT = 8


class AsyncClient:
    """Async client for workers and async applications.

    Use this when your application runs in an async context (FastAPI,
    Starlette, standalone async scripts, worker processes).

    Supports ``async with`` for automatic pool cleanup::

        async with awa.AsyncClient("postgres://localhost/mydb") as client:
            await client.migrate()
            await client.insert(SendEmail(to="alice@example.com", subject="Hi"))
            # pool is closed automatically on exit

    For worker processes::

        client = awa.AsyncClient("postgres://localhost/mydb")
        await client.migrate()

        @client.task(SendEmail, queue="email")
        async def handle(job):
            print(f"Sending to {job.args.to}")

        await client.start([("email", 2)])
        ...
        await client.shutdown()
        await client.close()
    """

    def __init__(self, database_url: str, max_connections: int = 10) -> None:
        self._raw = RawClient(database_url, max_connections)

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ── Job operations (async) ──────────────────────────────────

    async def insert(
        self,
        args: Any,
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
        unique_opts: dict[str, Any] | None = None,
    ) -> Job:
        """Insert a job. Returns a ``Job`` object."""
        return await self._raw.insert(
            args,
            kind=kind,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            tags=tags if tags is not None else [],
            metadata=metadata,
            run_at=run_at,
            unique_opts=unique_opts,
        )

    async def insert_many_copy(
        self,
        jobs: list[Any],
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
    ) -> list[Job]:
        """Bulk insert jobs using COPY for high throughput."""
        return await self._raw.insert_many_copy(
            jobs,
            kind=kind,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            tags=tags if tags is not None else [],
            metadata=metadata,
            run_at=run_at,
        )

    async def migrate(self) -> None:
        """Run database migrations."""
        return await self._raw.migrate()

    async def install_queue_storage(
        self,
        *,
        schema: str = DEFAULT_QUEUE_STORAGE_SCHEMA,
        queue_slot_count: int = DEFAULT_QUEUE_STORAGE_QUEUE_SLOT_COUNT,
        lease_slot_count: int = DEFAULT_QUEUE_STORAGE_LEASE_SLOT_COUNT,
        reset: bool = False,
    ) -> None:
        """Install and activate a queue-storage backend schema.

        This is a low-level helper for tests and explicit queue-storage-only
        setups. It prepares the schema and selects it as the active backend
        immediately. For the staged `0.5 -> 0.6` storage transition, use the
        dedicated storage-transition commands instead.
        """
        return await self._raw.install_queue_storage(
            schema=schema,
            queue_slot_count=queue_slot_count,
            lease_slot_count=lease_slot_count,
            reset=reset,
        )

    async def prepare_queue_storage_schema(
        self,
        *,
        schema: str = DEFAULT_QUEUE_STORAGE_SCHEMA,
        queue_slot_count: int = DEFAULT_QUEUE_STORAGE_QUEUE_SLOT_COUNT,
        lease_slot_count: int = DEFAULT_QUEUE_STORAGE_LEASE_SLOT_COUNT,
    ) -> None:
        """Materialize the queue-storage schema's tables / indexes /
        functions without changing the storage transition state.

        Mirrors ``awa storage prepare-queue-storage-schema`` on the CLI.
        Pairs with ``awa.storage_prepare(...)`` for a staged
        ``0.5 → 0.6`` transition: call ``storage_prepare`` to flip the
        transition state to ``prepared``, call this method to create
        the actual tables, then start workers. For the all-in-one
        "install and activate" path, see :meth:`install_queue_storage`.
        """
        return await self._raw.prepare_queue_storage_schema(
            schema=schema,
            queue_slot_count=queue_slot_count,
            lease_slot_count=lease_slot_count,
        )

    async def transaction(self) -> Transaction:
        """Start an async transaction (use as async context manager)."""
        return await self._raw.transaction()

    async def get_job(self, job_id: int) -> Job:
        """Fetch a job by ID."""
        return await self._raw.get_job(job_id)

    async def retry(self, job_id: int) -> Job | None:
        """Retry a failed/cancelled job."""
        return await self._raw.retry(job_id)

    async def cancel(self, job_id: int) -> Job | None:
        """Cancel a job.

        Pending/waiting jobs transition to ``cancelled`` immediately.
        Running jobs are also cancelled in storage, but handler-visible
        ``job.is_cancelled()`` is primarily driven by shutdown/rescue signals,
        not by admin cancel alone.
        """
        return await self._raw.cancel(job_id)

    async def cancel_by_unique_key(
        self,
        kind: str,
        *,
        queue: str | None = None,
        args: Any | None = None,
        period_bucket: int | None = None,
    ) -> Job | None:
        """Cancel a job by its unique key components."""
        return await self._raw.cancel_by_unique_key(
            kind, queue=queue, args=args, period_bucket=period_bucket
        )

    async def retry_failed(
        self, *, kind: str | None = None, queue: str | None = None
    ) -> list[Job]:
        """Retry all failed jobs matching the filter."""
        return await self._raw.retry_failed(kind=kind, queue=queue)

    async def discard_failed(self, kind: str) -> int:
        """Delete all failed jobs of a given kind."""
        return await self._raw.discard_failed(kind)

    async def pause_queue(self, queue: str, paused_by: str | None = None) -> None:
        """Pause a queue."""
        return await self._raw.pause_queue(queue, paused_by)

    async def resume_queue(self, queue: str) -> None:
        """Resume a paused queue."""
        return await self._raw.resume_queue(queue)

    async def drain_queue(self, queue: str) -> int:
        """Cancel all pending jobs in a queue."""
        return await self._raw.drain_queue(queue)

    async def flush_admin_metadata(self) -> None:
        """Drain dirty keys and recompute cached admin counters.

        Call before ``queue_stats()`` in tests without a running
        maintenance leader to ensure the cache is fresh.
        """
        return await self._raw.flush_admin_metadata()

    async def dump_job(self, job_id: int) -> str:
        """Return a pretty JSON snapshot of a job and its lifecycle metadata.

        Identical shape to ``awa job dump <id>`` in the Rust CLI.
        """
        return await self._raw.dump_job(job_id)

    async def dump_run(self, job_id: int, attempt: int | None = None) -> str:
        """Return a pretty JSON snapshot of a single attempt run.

        Identical shape to ``awa job dump-run <id> [--attempt N]``.
        """
        return await self._raw.dump_run(job_id, attempt)

    async def storage_status(self) -> str:
        """Return a pretty JSON storage-transition status report."""
        return await self._raw.storage_status()

    async def list_cron_jobs(self) -> list[dict[str, Any]]:
        """Return the registered cron/periodic schedules."""
        import json
        return json.loads(await self._raw.list_cron_jobs())

    async def delete_cron_job(self, name: str) -> bool:
        """Delete a cron schedule by name. Returns ``True`` if it existed."""
        return await self._raw.delete_cron_job(name)

    async def queue_stats(self) -> list[QueueStat]:
        """Per-queue statistics."""
        return await self._raw.queue_stats()

    async def list_jobs(
        self,
        *,
        state: str | None = None,
        kind: str | None = None,
        queue: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """List jobs with optional filters."""
        return await self._raw.list_jobs(
            state=state, kind=kind, queue=queue, limit=limit
        )

    # --- Dead Letter Queue -------------------------------------------------
    #
    # DLQ rows live in queue_storage `dlq_entries`, outside the hot claim
    # path. These methods expose the operator-level admin surface: inspect,
    # retry, move failed jobs in, and purge. See `awa-model/src/dlq.rs` for
    # semantics.

    async def list_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: dt.datetime | None = None,
        limit: int = 100,
    ) -> list["DlqEntry"]:
        """List DLQ entries, newest first. Use `(before_dlq_at, before_id)` as the cursor."""
        return await self._raw.list_dlq(
            kind=kind,
            queue=queue,
            tag=tag,
            before_id=before_id,
            before_dlq_at=before_dlq_at,
            limit=limit,
        )

    async def get_dlq_job(self, job_id: int) -> "DlqEntry | None":
        """Fetch a DLQ entry by id. Returns None if not in the DLQ."""
        return await self._raw.get_dlq_job(job_id)

    async def dlq_depth(self, *, queue: str | None = None) -> int:
        """Total DLQ row count, optionally filtered by queue."""
        return await self._raw.dlq_depth(queue=queue)

    async def dlq_depth_by_queue(self) -> list[tuple[str, int]]:
        """DLQ row counts grouped by queue (descending)."""
        return await self._raw.dlq_depth_by_queue()

    async def retry_from_dlq(
        self,
        job_id: int,
        *,
        run_at: dt.datetime | None = None,
        priority: int | None = None,
        queue: str | None = None,
    ) -> Job | None:
        """Revive a DLQ'd job back to `available` (or `scheduled` if run_at is future)."""
        return await self._raw.retry_from_dlq(
            job_id, run_at=run_at, priority=priority, queue=queue
        )

    async def bulk_retry_from_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        allow_all: bool = False,
    ) -> int:
        """Bulk-retry DLQ rows matching the filter. Returns revived count."""
        return await self._raw.bulk_retry_from_dlq(
            kind=kind, queue=queue, tag=tag, allow_all=allow_all
        )

    async def move_failed_to_dlq(
        self, job_id: int, reason: str
    ) -> "DlqEntry | None":
        """Move an already-failed job into the DLQ."""
        return await self._raw.move_failed_to_dlq(job_id, reason)

    async def bulk_move_failed_to_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        reason: str = "manual",
        allow_all: bool = False,
    ) -> int:
        """Bulk-move failed jobs into the DLQ."""
        return await self._raw.bulk_move_failed_to_dlq(
            kind=kind, queue=queue, reason=reason, allow_all=allow_all
        )

    async def purge_dlq_job(self, job_id: int) -> bool:
        """Delete a single DLQ row. Returns True if it was deleted."""
        return await self._raw.purge_dlq_job(job_id)

    async def purge_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: dt.datetime | None = None,
        allow_all: bool = False,
    ) -> int:
        """Bulk-delete DLQ rows matching the filter."""
        return await self._raw.purge_dlq(
            kind=kind,
            queue=queue,
            tag=tag,
            before_id=before_id,
            before_dlq_at=before_dlq_at,
            allow_all=allow_all,
        )

    async def health_check(self) -> HealthCheck:
        """Runtime health check."""
        return await self._raw.health_check()

    async def shutdown(self, timeout_ms: int = 2000) -> None:
        """Graceful shutdown: stop workers and drain in-flight jobs.

        The connection pool remains open so you can still query the
        database after shutdown (e.g. to verify final state). Call
        :meth:`close` when you're done to release all connections.
        """
        return await self._raw.shutdown(timeout_ms)

    async def close(self) -> None:
        """Close the connection pool, releasing all database connections.

        After ``close()`` any attempt to use the client raises
        ``DatabaseError``. Call this when you're done with the client
        to avoid leaking connections.
        """
        return await self._raw.close()

    # ── External callbacks (async) ──────────────────────────────

    async def complete_external(
        self, callback_id: str, payload: dict[str, Any] | None = None
    ) -> Job:
        """Complete a waiting job via external callback."""
        return await self._raw.complete_external(callback_id, payload)

    async def fail_external(self, callback_id: str, error: str) -> Job:
        """Fail a waiting job via external callback."""
        return await self._raw.fail_external(callback_id, error)

    async def retry_external(self, callback_id: str) -> Job:
        """Retry a waiting job via external callback."""
        return await self._raw.retry_external(callback_id)

    async def resume_external(
        self, callback_id: str, payload: dict[str, Any] | None = None
    ) -> Job:
        """Resume a waiting job via external callback, returning it to running.

        Use this for sequential callback patterns where the handler needs to
        continue execution with the callback payload.
        """
        return await self._raw.resume_external(callback_id, payload)

    async def heartbeat_callback(
        self, callback_id: str, timeout_seconds: float = 3600.0
    ) -> Job:
        """Reset the callback timeout for a long-running external operation.

        Call periodically to signal "still working" without completing the job.
        Resets the timeout deadline to ``now() + timeout_seconds``.
        """
        return await self._raw.heartbeat_callback(callback_id, timeout_seconds)

    async def resolve_callback(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
        default_action: str = "ignore",
    ) -> ResolveResult:
        """Resolve a callback with optional CEL expression evaluation."""
        return await self._raw.resolve_callback(
            callback_id, payload=payload, default_action=default_action
        )

    # ── Worker lifecycle (sync — called from async context) ─────

    def task(
        self,
        args_type: type[T],
        *,
        kind: str | None = None,
        queue: str = "default",
    ) -> Callable[[Callable[[Job[T]], Awaitable[Any]]], Callable[[Job[T]], Awaitable[Any]]]:
        """Register a task handler (decorator).

        Example::

            @client.task(SendEmail, queue="email")
            async def handle(job):
                send_email(job.args.to, job.args.subject)
        """
        return self._raw.worker(args_type, kind=kind, queue=queue)

    def worker(
        self,
        args_type: type[T],
        *,
        kind: str | None = None,
        queue: str = "default",
    ) -> Callable[[Callable[[Job[T]], Awaitable[Any]]], Callable[[Job[T]], Awaitable[Any]]]:
        """Deprecated: use ``task()`` instead."""
        import warnings
        warnings.warn(
            "client.worker() is deprecated, use client.task() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.task(args_type, kind=kind, queue=queue)

    def periodic(
        self,
        name: str,
        cron_expr: str,
        args_type: type[T],
        args: T,
        *,
        timezone: str = "UTC",
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a periodic (cron) job schedule."""
        return self._raw.periodic(
            name,
            cron_expr,
            args_type,
            args,
            timezone=timezone,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            tags=tags if tags is not None else [],
            metadata=metadata,
        )

    def queue_descriptor(
        self,
        queue: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        docs_url: str | None = None,
        tags: list[str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Attach descriptive metadata to a queue.

        The display name, description, owner, docs URL, tags, and any extra
        JSON you provide show up on the admin UI's queue views and in the
        admin API. This is how operators understand what a queue is for
        without reading the worker source.

        Call before :py:meth:`start`. The queue must also appear in the
        ``queues=`` argument of :py:meth:`start` — you can't describe a
        queue this client doesn't run.

        Example::

            client.queue_descriptor(
                "email",
                display_name="Outbound email",
                owner="growth@company.com",
                docs_url="https://runbook/email",
                tags=["user-facing"],
            )
        """
        return self._raw.queue_descriptor(
            queue,
            display_name=display_name,
            description=description,
            owner=owner,
            docs_url=docs_url,
            tags=tags,
            extra=extra,
        )

    def job_kind_descriptor(
        self,
        kind: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        docs_url: str | None = None,
        tags: list[str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Attach descriptive metadata to a job kind.

        Same shape as :py:meth:`queue_descriptor`, but for a job kind (the
        string ``@client.task(...)`` publishes — by default the snake_cased
        class name of the args type).

        Call before :py:meth:`start`. The kind must be registered with
        :py:meth:`task` or referenced from a periodic schedule.

        Example::

            @client.task(SendEmail, queue="email")
            async def send(job): ...

            client.job_kind_descriptor(
                "send_email",
                display_name="Send user email",
                description="Renders a template and hands off to SES.",
            )
        """
        return self._raw.job_kind_descriptor(
            kind,
            display_name=display_name,
            description=description,
            owner=owner,
            docs_url=docs_url,
            tags=tags,
            extra=extra,
        )

    async def start(
        self,
        queues: list[tuple[str, int]] | list[dict[str, Any]] | None = None,
        *,
        poll_interval_ms: int = 200,
        global_max_workers: int | None = None,
        completed_retention_hours: float | None = None,
        failed_retention_hours: float | None = None,
        descriptor_retention_days: float | None = None,
        cleanup_batch_size: int | None = None,
        leader_election_interval_ms: int | None = None,
        heartbeat_interval_ms: int | None = None,
        promote_interval_ms: int | None = None,
        heartbeat_rescue_interval_ms: int | None = None,
        heartbeat_staleness_ms: int | None = None,
        deadline_rescue_interval_ms: int | None = None,
        callback_rescue_interval_ms: int | None = None,
        queue_storage_schema: str | None = None,
        queue_storage_queue_slot_count: int = DEFAULT_QUEUE_STORAGE_QUEUE_SLOT_COUNT,
        queue_storage_lease_slot_count: int = DEFAULT_QUEUE_STORAGE_LEASE_SLOT_COUNT,
        queue_storage_claim_slot_count: int = DEFAULT_QUEUE_STORAGE_CLAIM_SLOT_COUNT,
        queue_storage_queue_rotate_interval_ms: int = 1000,
        queue_storage_lease_rotate_interval_ms: int = 50,
        queue_storage_claim_rotate_interval_ms: int | None = None,
        storage_transition_role: Literal[
            "auto", "canonical_drain", "queue_storage_target"
        ] = "auto",
    ) -> None:
        """Start the worker runtime.

        ``descriptor_retention_days`` controls how long a declared queue
        or job-kind descriptor can sit un-refreshed before the maintenance
        leader deletes it from ``awa.queue_descriptors`` /
        ``awa.job_kind_descriptors``. Defaults to 30 days; pass ``0`` to
        disable retention (useful if you manage the catalog externally).

        Workers are queue-storage-capable by default, but during a storage
        transition ``storage_transition_role`` controls how this runtime
        participates:

        - ``"auto"`` follows ``awa.storage_status()`` and stays canonical
          until queue-storage routing becomes active
        - ``"canonical_drain"`` keeps draining canonical backlog even after
          the routing flip
        - ``"queue_storage_target"`` prepares and runs the queue-storage
          executor immediately so new work can start executing as soon as the
          cluster enters mixed transition

        Set ``queue_storage_schema`` to override the schema name; all workers
        participating in the same storage transition should agree on that
        target schema.
        """
        return await self._raw.start(
            queues,
            poll_interval_ms=poll_interval_ms,
            global_max_workers=global_max_workers,
            completed_retention_hours=completed_retention_hours,
            failed_retention_hours=failed_retention_hours,
            descriptor_retention_days=descriptor_retention_days,
            cleanup_batch_size=cleanup_batch_size,
            leader_election_interval_ms=leader_election_interval_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            promote_interval_ms=promote_interval_ms,
            heartbeat_rescue_interval_ms=heartbeat_rescue_interval_ms,
            heartbeat_staleness_ms=heartbeat_staleness_ms,
            deadline_rescue_interval_ms=deadline_rescue_interval_ms,
            callback_rescue_interval_ms=callback_rescue_interval_ms,
            queue_storage_schema=queue_storage_schema,
            queue_storage_queue_slot_count=queue_storage_queue_slot_count,
            queue_storage_lease_slot_count=queue_storage_lease_slot_count,
            queue_storage_claim_slot_count=queue_storage_claim_slot_count,
            queue_storage_queue_rotate_interval_ms=queue_storage_queue_rotate_interval_ms,
            queue_storage_lease_rotate_interval_ms=queue_storage_lease_rotate_interval_ms,
            queue_storage_claim_rotate_interval_ms=queue_storage_claim_rotate_interval_ms,
            storage_transition_role=storage_transition_role,
        )


class Client:
    """Synchronous client for producers (Django, Flask, scripts).

    Use this when your application is synchronous — web frameworks
    like Django/Flask, management commands, data pipelines.

    Example::

        client = awa.Client("postgres://localhost/mydb")
        client.migrate()
        job = client.insert(SendEmail(to="alice@example.com", subject="Hi"))
        print(f"Enqueued job {job.id}")
    """

    def __init__(self, database_url: str, max_connections: int = 10) -> None:
        self._raw = RawClient(database_url, max_connections)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection pool, releasing all database connections."""
        return self._raw.close_sync()

    def insert(
        self,
        args: Any,
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
        unique_opts: dict[str, Any] | None = None,
    ) -> Job:
        """Insert a job. Returns a ``Job`` object."""
        return self._raw.insert_sync(
            args,
            kind=kind,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            tags=tags if tags is not None else [],
            metadata=metadata,
            run_at=run_at,
            unique_opts=unique_opts,
        )

    def insert_many_copy(
        self,
        jobs: list[Any],
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
    ) -> list[Job]:
        """Bulk insert jobs using COPY for high throughput."""
        return self._raw.insert_many_copy_sync(
            jobs,
            kind=kind,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            tags=tags if tags is not None else [],
            metadata=metadata,
            run_at=run_at,
        )

    def migrate(self) -> None:
        """Run database migrations."""
        return self._raw.migrate_sync()

    def install_queue_storage(
        self,
        *,
        schema: str = DEFAULT_QUEUE_STORAGE_SCHEMA,
        queue_slot_count: int = DEFAULT_QUEUE_STORAGE_QUEUE_SLOT_COUNT,
        lease_slot_count: int = DEFAULT_QUEUE_STORAGE_LEASE_SLOT_COUNT,
        reset: bool = False,
    ) -> None:
        """Install and activate a queue-storage backend schema.

        This is a low-level helper for tests and explicit queue-storage-only
        setups. It prepares the schema and selects it as the active backend
        immediately. For the staged `0.5 -> 0.6` storage transition, use the
        dedicated storage-transition commands instead.
        """
        return self._raw.install_queue_storage_sync(
            schema=schema,
            queue_slot_count=queue_slot_count,
            lease_slot_count=lease_slot_count,
            reset=reset,
        )

    def transaction(self) -> SyncTransaction:
        """Start a sync transaction (use as context manager)."""
        return self._raw.transaction_sync()

    def get_job(self, job_id: int) -> Job:
        """Fetch a job by ID."""
        return self._raw.get_job_sync(job_id)

    def retry(self, job_id: int) -> Job | None:
        """Retry a failed/cancelled job."""
        return self._raw.retry_sync(job_id)

    def cancel(self, job_id: int) -> Job | None:
        """Cancel a job.

        Pending/waiting jobs transition to ``cancelled`` immediately.
        Running jobs are also cancelled in storage, but handler-visible
        ``job.is_cancelled()`` is primarily driven by shutdown/rescue signals,
        not by admin cancel alone.
        """
        return self._raw.cancel_sync(job_id)

    def cancel_by_unique_key(
        self,
        kind: str,
        *,
        queue: str | None = None,
        args: Any | None = None,
        period_bucket: int | None = None,
    ) -> Job | None:
        """Cancel a job by its unique key components."""
        return self._raw.cancel_by_unique_key_sync(
            kind, queue=queue, args=args, period_bucket=period_bucket
        )

    def retry_failed(
        self, *, kind: str | None = None, queue: str | None = None
    ) -> list[Job]:
        """Retry all failed jobs matching the filter."""
        return self._raw.retry_failed_sync(kind=kind, queue=queue)

    def discard_failed(self, kind: str) -> int:
        """Delete all failed jobs of a given kind."""
        return self._raw.discard_failed_sync(kind)

    def pause_queue(self, queue: str, paused_by: str | None = None) -> None:
        """Pause a queue."""
        return self._raw.pause_queue_sync(queue, paused_by)

    def resume_queue(self, queue: str) -> None:
        """Resume a paused queue."""
        return self._raw.resume_queue_sync(queue)

    def drain_queue(self, queue: str) -> int:
        """Cancel all pending jobs in a queue."""
        return self._raw.drain_queue_sync(queue)

    def flush_admin_metadata(self) -> None:
        """Drain dirty keys and recompute cached admin counters."""
        return self._raw.flush_admin_metadata_sync()

    def dump_job(self, job_id: int) -> str:
        """Return a pretty JSON snapshot of a job and its lifecycle metadata."""
        return self._raw.dump_job_sync(job_id)

    def dump_run(self, job_id: int, attempt: int | None = None) -> str:
        """Return a pretty JSON snapshot of a single attempt run."""
        return self._raw.dump_run_sync(job_id, attempt)

    def storage_status(self) -> str:
        """Return a pretty JSON storage-transition status report."""
        return self._raw.storage_status_sync()

    def list_cron_jobs(self) -> list[dict[str, Any]]:
        """Return the registered cron/periodic schedules."""
        import json
        return json.loads(self._raw.list_cron_jobs_sync())

    def delete_cron_job(self, name: str) -> bool:
        """Delete a cron schedule by name. Returns ``True`` if it existed."""
        return self._raw.delete_cron_job_sync(name)

    def queue_stats(self) -> list[QueueStat]:
        """Per-queue statistics."""
        return self._raw.queue_stats_sync()

    def list_jobs(
        self,
        *,
        state: str | None = None,
        kind: str | None = None,
        queue: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """List jobs with optional filters."""
        return self._raw.list_jobs_sync(
            state=state, kind=kind, queue=queue, limit=limit
        )

    def health_check(self) -> HealthCheck:
        """Runtime health check."""
        return self._raw.health_check_sync()

    # ── External callbacks (sync) ───────────────────────────────

    def complete_external(
        self, callback_id: str, payload: dict[str, Any] | None = None
    ) -> Job:
        """Complete a waiting job via external callback."""
        return self._raw.complete_external_sync(callback_id, payload)

    def fail_external(self, callback_id: str, error: str) -> Job:
        """Fail a waiting job via external callback."""
        return self._raw.fail_external_sync(callback_id, error)

    def retry_external(self, callback_id: str) -> Job:
        """Retry a waiting job via external callback."""
        return self._raw.retry_external_sync(callback_id)

    def resume_external(
        self, callback_id: str, payload: dict[str, Any] | None = None
    ) -> Job:
        """Resume a waiting job via external callback, returning it to running."""
        return self._raw.resume_external_sync(callback_id, payload)

    def heartbeat_callback(
        self, callback_id: str, timeout_seconds: float = 3600.0
    ) -> Job:
        """Reset the callback timeout for a long-running external operation."""
        return self._raw.heartbeat_callback_sync(callback_id, timeout_seconds)

    def resolve_callback(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
        default_action: str = "ignore",
    ) -> ResolveResult:
        """Resolve a callback with optional CEL expression evaluation."""
        return self._raw.resolve_callback_sync(
            callback_id, payload=payload, default_action=default_action
        )

    # --- Dead Letter Queue (sync) -----------------------------------------

    def list_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: dt.datetime | None = None,
        limit: int = 100,
    ) -> list[DlqEntry]:
        """List DLQ entries, newest first. Use `(before_dlq_at, before_id)` as the cursor."""
        return self._raw.list_dlq_sync(
            kind=kind,
            queue=queue,
            tag=tag,
            before_id=before_id,
            before_dlq_at=before_dlq_at,
            limit=limit,
        )

    def get_dlq_job(self, job_id: int) -> DlqEntry | None:
        """Fetch a DLQ entry by id."""
        return self._raw.get_dlq_job_sync(job_id)

    def dlq_depth(self, *, queue: str | None = None) -> int:
        """Total DLQ depth, optionally filtered by queue."""
        return self._raw.dlq_depth_sync(queue=queue)

    def dlq_depth_by_queue(self) -> list[tuple[str, int]]:
        """DLQ depth grouped by queue."""
        return self._raw.dlq_depth_by_queue_sync()

    def retry_from_dlq(
        self,
        job_id: int,
        *,
        run_at: dt.datetime | None = None,
        priority: int | None = None,
        queue: str | None = None,
    ) -> Job | None:
        """Revive a DLQ'd job."""
        return self._raw.retry_from_dlq_sync(
            job_id, run_at=run_at, priority=priority, queue=queue
        )

    def bulk_retry_from_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        allow_all: bool = False,
    ) -> int:
        """Bulk-retry DLQ rows matching the filter."""
        return self._raw.bulk_retry_from_dlq_sync(
            kind=kind, queue=queue, tag=tag, allow_all=allow_all
        )

    def move_failed_to_dlq(self, job_id: int, reason: str) -> DlqEntry | None:
        """Move an already-failed job into the DLQ."""
        return self._raw.move_failed_to_dlq_sync(job_id, reason)

    def bulk_move_failed_to_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        reason: str = "manual",
        allow_all: bool = False,
    ) -> int:
        """Bulk-move failed jobs into the DLQ."""
        return self._raw.bulk_move_failed_to_dlq_sync(
            kind=kind, queue=queue, reason=reason, allow_all=allow_all
        )

    def purge_dlq_job(self, job_id: int) -> bool:
        """Delete a single DLQ row."""
        return self._raw.purge_dlq_job_sync(job_id)

    def purge_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: dt.datetime | None = None,
        allow_all: bool = False,
    ) -> int:
        """Bulk-delete DLQ rows matching the filter."""
        return self._raw.purge_dlq_sync(
            kind=kind,
            queue=queue,
            tag=tag,
            before_id=before_id,
            before_dlq_at=before_dlq_at,
            allow_all=allow_all,
        )

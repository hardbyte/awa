"""Type stubs for awa._awa (PyO3 extension module)."""

from __future__ import annotations

import datetime
from enum import IntEnum
from typing import Any, Awaitable, Callable, Generic, TypeVar

T = TypeVar("T")

class JobState(IntEnum):
    Scheduled = ...
    Available = ...
    Running = ...
    Completed = ...
    Retryable = ...
    Failed = ...
    Cancelled = ...
    WaitingExternal = ...
    def __str__(self) -> str: ...

class QueueStat:
    @property
    def queue(self) -> str: ...
    @property
    def total_queued(self) -> int: ...
    @property
    def scheduled(self) -> int: ...
    @property
    def available(self) -> int: ...
    @property
    def retryable(self) -> int: ...
    @property
    def running(self) -> int: ...
    @property
    def failed(self) -> int: ...
    @property
    def waiting_external(self) -> int: ...
    @property
    def completed_last_hour(self) -> int: ...
    @property
    def lag_seconds(self) -> float | None: ...
    @property
    def paused(self) -> bool: ...

class CallbackToken:
    @property
    def id(self) -> str: ...

class DlqEntry:
    """An entry in the Dead Letter Queue.

    Combines the original `Job` snapshot with DLQ-specific metadata (reason
    for moving, timestamp, and the run_lease of the final attempt).
    """

    @property
    def job(self) -> Job[dict[str, Any]]: ...
    @property
    def reason(self) -> str: ...
    @property
    def dlq_at(self) -> datetime.datetime: ...
    @property
    def original_run_lease(self) -> int: ...

class Job(Generic[T]):
    @property
    def id(self) -> int: ...
    @property
    def kind(self) -> str: ...
    @property
    def queue(self) -> str: ...
    @property
    def state(self) -> JobState: ...
    @property
    def priority(self) -> int: ...
    @property
    def attempt(self) -> int: ...
    @property
    def max_attempts(self) -> int: ...
    @property
    def tags(self) -> list[str]: ...
    @property
    def args(self) -> T: ...
    @property
    def metadata(self) -> dict[str, Any]: ...
    @property
    def errors(self) -> list[dict[str, Any]]: ...
    @property
    def run_at(self) -> str: ...
    @property
    def deadline(self) -> str | None: ...
    @property
    def created_at(self) -> str: ...
    @property
    def finalized_at(self) -> str | None: ...
    @property
    def progress(self) -> dict[str, Any] | None: ...
    def is_cancelled(self) -> bool: ...
    def set_progress(self, percent: int, message: str | None = None) -> None: ...
    def update_metadata(self, updates: dict[str, Any]) -> None: ...
    async def flush_progress(self) -> None: ...
    def flush_progress_sync(self) -> None: ...
    async def register_callback(
        self,
        timeout_seconds: float = 3600,
        filter: str | None = None,
        on_complete: str | None = None,
        on_fail: str | None = None,
        transform: str | None = None,
    ) -> CallbackToken: ...
    async def wait_for_callback(self, token: CallbackToken) -> Any: ...

class ResolveResult:
    @property
    def outcome(self) -> str: ...
    @property
    def job(self) -> Job[dict[str, Any]] | None: ...
    @property
    def payload(self) -> Any: ...
    @property
    def reason(self) -> str | None: ...
    def is_completed(self) -> bool: ...
    def is_failed(self) -> bool: ...
    def is_ignored(self) -> bool: ...

class QueueHealth:
    @property
    def in_flight(self) -> int: ...
    @property
    def available(self) -> int: ...
    @property
    def max_workers(self) -> int | None: ...
    @property
    def min_workers(self) -> int | None: ...
    @property
    def weight(self) -> int | None: ...
    @property
    def overflow_held(self) -> int | None: ...

class HealthCheck:
    @property
    def healthy(self) -> bool: ...
    @property
    def postgres_connected(self) -> bool: ...
    @property
    def poll_loop_alive(self) -> bool: ...
    @property
    def heartbeat_alive(self) -> bool: ...
    @property
    def shutting_down(self) -> bool: ...
    @property
    def leader(self) -> bool: ...
    @property
    def queues(self) -> dict[str, QueueHealth]: ...

class RetryAfter:
    seconds: float
    def __init__(self, seconds: float) -> None: ...

class Snooze:
    seconds: float
    def __init__(self, seconds: float) -> None: ...

class Cancel:
    reason: str
    def __init__(self, reason: str = "cancelled by handler") -> None: ...

class WaitForCallback:
    callback_id: str
    def __init__(self, token: CallbackToken) -> None: ...

class Transaction:
    async def execute(self, query: str, *args: Any) -> int: ...
    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any]: ...
    async def fetch_optional(self, query: str, *args: Any) -> dict[str, Any] | None: ...
    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]: ...
    async def insert(
        self,
        args: Any,
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
        unique_opts: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    async def insert_many(
        self,
        jobs: list[Any],
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
    ) -> list[Job[dict[str, Any]]]: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def __aenter__(self) -> Transaction: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> bool: ...

class SyncTransaction:
    def execute(self, query: str, *args: Any) -> int: ...
    def fetch_one(self, query: str, *args: Any) -> dict[str, Any]: ...
    def fetch_optional(self, query: str, *args: Any) -> dict[str, Any] | None: ...
    def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]: ...
    def insert(
        self,
        args: Any,
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
        unique_opts: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    def insert_many(
        self,
        jobs: list[Any],
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
    ) -> list[Job[dict[str, Any]]]: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def __enter__(self) -> SyncTransaction: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> bool: ...

class Client:
    def __init__(
        self, database_url: str, max_connections: int = 10
    ) -> None: ...
    # Async methods
    async def insert(
        self,
        args: Any,
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
        unique_opts: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    async def migrate(self) -> None: ...
    async def install_queue_storage(
        self,
        *,
        schema: str = "awa_exp",
        queue_slot_count: int = 4,
        lease_slot_count: int = 2,
        reset: bool = False,
    ) -> None: ...
    async def transaction(self) -> Transaction: ...
    def worker(
        self,
        args_type: type[T],
        *,
        kind: str | None = None,
        queue: str = "default",
    ) -> Callable[[Callable[[Job[T]], Awaitable[Any]]], Callable[[Job[T]], Awaitable[Any]]]: ...
    async def retry(self, job_id: int) -> Job[dict[str, Any]] | None: ...
    async def cancel(self, job_id: int) -> Job[dict[str, Any]] | None: ...
    async def cancel_by_unique_key(
        self,
        kind: str,
        *,
        queue: str | None = None,
        args: Any | None = None,
        period_bucket: int | None = None,
    ) -> Job[dict[str, Any]] | None: ...
    async def retry_failed(
        self, *, kind: str | None = None, queue: str | None = None
    ) -> list[Job[dict[str, Any]]]: ...
    async def discard_failed(self, kind: str) -> int: ...
    async def pause_queue(
        self, queue: str, paused_by: str | None = None
    ) -> None: ...
    async def resume_queue(self, queue: str) -> None: ...
    async def drain_queue(self, queue: str) -> int: ...
    async def flush_admin_metadata(self) -> None: ...
    async def queue_stats(self) -> list[QueueStat]: ...
    async def list_jobs(
        self,
        *,
        state: str | None = None,
        kind: str | None = None,
        queue: str | None = None,
        limit: int = 100,
    ) -> list[Job[dict[str, Any]]]: ...
    async def get_job(self, job_id: int) -> Job[dict[str, Any]]: ...
    async def list_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: datetime.datetime | None = None,
        limit: int = 100,
    ) -> list[DlqEntry]: ...
    def list_dlq_sync(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: datetime.datetime | None = None,
        limit: int = 100,
    ) -> list[DlqEntry]: ...
    async def get_dlq_job(self, job_id: int) -> DlqEntry | None: ...
    def get_dlq_job_sync(self, job_id: int) -> DlqEntry | None: ...
    async def dlq_depth(self, *, queue: str | None = None) -> int: ...
    def dlq_depth_sync(self, *, queue: str | None = None) -> int: ...
    async def dlq_depth_by_queue(self) -> list[tuple[str, int]]: ...
    def dlq_depth_by_queue_sync(self) -> list[tuple[str, int]]: ...
    async def retry_from_dlq(
        self,
        job_id: int,
        *,
        run_at: datetime.datetime | None = None,
        priority: int | None = None,
        queue: str | None = None,
    ) -> Job[dict[str, Any]] | None: ...
    def retry_from_dlq_sync(
        self,
        job_id: int,
        *,
        run_at: datetime.datetime | None = None,
        priority: int | None = None,
        queue: str | None = None,
    ) -> Job[dict[str, Any]] | None: ...
    async def bulk_retry_from_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        allow_all: bool = False,
    ) -> int: ...
    def bulk_retry_from_dlq_sync(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        allow_all: bool = False,
    ) -> int: ...
    async def move_failed_to_dlq(self, job_id: int, reason: str) -> DlqEntry | None: ...
    def move_failed_to_dlq_sync(self, job_id: int, reason: str) -> DlqEntry | None: ...
    async def bulk_move_failed_to_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        reason: str = "manual",
        allow_all: bool = False,
    ) -> int: ...
    def bulk_move_failed_to_dlq_sync(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        reason: str = "manual",
        allow_all: bool = False,
    ) -> int: ...
    async def purge_dlq_job(self, job_id: int) -> bool: ...
    def purge_dlq_job_sync(self, job_id: int) -> bool: ...
    async def purge_dlq(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: datetime.datetime | None = None,
        allow_all: bool = False,
    ) -> int: ...
    def purge_dlq_sync(
        self,
        *,
        kind: str | None = None,
        queue: str | None = None,
        tag: str | None = None,
        before_id: int | None = None,
        before_dlq_at: datetime.datetime | None = None,
        allow_all: bool = False,
    ) -> int: ...
    async def health_check(self) -> HealthCheck: ...
    async def insert_many_copy(
        self,
        jobs: list[Any],
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
    ) -> list[Job[dict[str, Any]]]: ...
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
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
    ) -> None: ...
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
        queue_storage_queue_slot_count: int = 16,
        queue_storage_lease_slot_count: int = 8,
        queue_storage_queue_rotate_interval_ms: int = 1000,
        queue_storage_lease_rotate_interval_ms: int = 50,
    ) -> None: ...
    async def shutdown(self, timeout_ms: int = 2000) -> None: ...
    async def close(self) -> None: ...
    # External callback completion (async)
    async def complete_external(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    async def fail_external(
        self, callback_id: str, error: str
    ) -> Job[dict[str, Any]]: ...
    async def retry_external(
        self, callback_id: str
    ) -> Job[dict[str, Any]]: ...
    async def resume_external(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    async def heartbeat_callback(
        self,
        callback_id: str,
        timeout_seconds: float = 3600.0,
    ) -> Job[dict[str, Any]]: ...
    async def resolve_callback(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
        default_action: str = "ignore",
    ) -> ResolveResult: ...
    # Sync methods
    def insert_sync(
        self,
        args: Any,
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
        unique_opts: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    def close_sync(self) -> None: ...
    def migrate_sync(self) -> None: ...
    def install_queue_storage_sync(
        self,
        *,
        schema: str = "awa_exp",
        queue_slot_count: int = 4,
        lease_slot_count: int = 2,
        reset: bool = False,
    ) -> None: ...
    def transaction_sync(self) -> SyncTransaction: ...
    def retry_sync(self, job_id: int) -> Job[dict[str, Any]] | None: ...
    def cancel_sync(self, job_id: int) -> Job[dict[str, Any]] | None: ...
    def cancel_by_unique_key_sync(
        self,
        kind: str,
        *,
        queue: str | None = None,
        args: Any | None = None,
        period_bucket: int | None = None,
    ) -> Job[dict[str, Any]] | None: ...
    def retry_failed_sync(
        self, *, kind: str | None = None, queue: str | None = None
    ) -> list[Job[dict[str, Any]]]: ...
    def discard_failed_sync(self, kind: str) -> int: ...
    def pause_queue_sync(
        self, queue: str, paused_by: str | None = None
    ) -> None: ...
    def resume_queue_sync(self, queue: str) -> None: ...
    def drain_queue_sync(self, queue: str) -> int: ...
    def flush_admin_metadata_sync(self) -> None: ...
    def queue_stats_sync(self) -> list[QueueStat]: ...
    def list_jobs_sync(
        self,
        *,
        state: str | None = None,
        kind: str | None = None,
        queue: str | None = None,
        limit: int = 100,
    ) -> list[Job[dict[str, Any]]]: ...
    def get_job_sync(self, job_id: int) -> Job[dict[str, Any]]: ...
    def health_check_sync(self) -> HealthCheck: ...
    def insert_many_copy_sync(
        self,
        jobs: list[Any],
        *,
        kind: str | None = None,
        queue: str = "default",
        priority: int = 2,
        max_attempts: int = 25,
        tags: list[str] = [],
        metadata: dict[str, Any] | None = None,
        run_at: Any | None = None,
    ) -> list[Job[dict[str, Any]]]: ...
    # External callback completion (sync)
    def complete_external_sync(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    def fail_external_sync(
        self, callback_id: str, error: str
    ) -> Job[dict[str, Any]]: ...
    def retry_external_sync(
        self, callback_id: str
    ) -> Job[dict[str, Any]]: ...
    def resume_external_sync(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
    ) -> Job[dict[str, Any]]: ...
    def heartbeat_callback_sync(
        self,
        callback_id: str,
        timeout_seconds: float = 3600.0,
    ) -> Job[dict[str, Any]]: ...
    def resolve_callback_sync(
        self,
        callback_id: str,
        payload: dict[str, Any] | None = None,
        default_action: str = "ignore",
    ) -> ResolveResult: ...

# Functions
def derive_kind(name: str) -> str: ...
async def migrate(database_url: str) -> None: ...
def migrations() -> list[tuple[int, str, str]]: ...
def migrations_range(from_version: int, to_version: int) -> list[tuple[int, str, str]]: ...
def current_migration_version() -> int: ...

# Exceptions
class AwaError(Exception): ...
class UniqueConflict(AwaError): ...
class SchemaNotMigrated(AwaError): ...
class UnknownJobKind(AwaError): ...
class SerializationError(AwaError): ...
class ValidationError(AwaError): ...
class TerminalError(AwaError): ...
class DatabaseError(AwaError): ...
class CallbackNotFound(AwaError): ...

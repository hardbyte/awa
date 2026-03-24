"""Awa — Postgres-native background job queue for Rust and Python."""

from awa._awa import (
    # Raw PyO3 client (kept for backwards compat, use Client/AsyncClient instead)
    Client as RawClient,
    # Job types
    Job,
    JobState,
    HealthCheck,
    QueueHealth,
    QueueStat,
    # Callback
    CallbackToken,
    WaitForCallback,
    ResolveResult,
    # Transaction
    Transaction,
    SyncTransaction,
    # Handler return types
    RetryAfter,
    Snooze,
    Cancel,
    # Functions
    derive_kind,
    migrate,
    migrations,
    migrations_range,
    current_migration_version,
    # Exceptions
    AwaError,
    UniqueConflict,
    SchemaNotMigrated,
    UnknownJobKind,
    SerializationError,
    ValidationError,
    TerminalError,
    DatabaseError,
    CallbackNotFound,
)

from awa.bridge import insert_job, insert_job_sync
from awa.client import AsyncClient, Client

__all__ = [
    # Clients
    "Client",
    "AsyncClient",
    "RawClient",
    # Job types
    "Job",
    "JobState",
    "HealthCheck",
    "QueueHealth",
    "QueueStat",
    # Callback
    "CallbackToken",
    "WaitForCallback",
    "ResolveResult",
    # Transaction
    "Transaction",
    "SyncTransaction",
    # Handler return types
    "RetryAfter",
    "Snooze",
    "Cancel",
    # Bridge (ORM/driver transaction bridging)
    "insert_job",
    "insert_job_sync",
    # Functions
    "derive_kind",
    "migrate",
    "migrations",
    "migrations_range",
    "current_migration_version",
    # Exceptions
    "AwaError",
    "UniqueConflict",
    "SchemaNotMigrated",
    "UnknownJobKind",
    "SerializationError",
    "ValidationError",
    "TerminalError",
    "DatabaseError",
    "CallbackNotFound",
]

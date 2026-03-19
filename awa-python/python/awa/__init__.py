"""Awa — Postgres-native background job queue for Rust and Python."""

from awa._awa import (
    # Client
    Client,
    # Job types
    Job,
    JobState,
    HealthCheck,
    QueueHealth,
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

__all__ = [
    "Client",
    "Job",
    "JobState",
    "HealthCheck",
    "QueueHealth",
    "CallbackToken",
    "WaitForCallback",
    "ResolveResult",
    "Transaction",
    "SyncTransaction",
    "RetryAfter",
    "Snooze",
    "Cancel",
    "derive_kind",
    "migrate",
    "migrations",
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

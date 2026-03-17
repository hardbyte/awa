"""Awa — Postgres-native background job queue for Rust and Python."""

from awa._awa import (
    # Client
    Client,
    # Job types
    Job,
    JobState,
    HealthCheck,
    QueueHealth,
    # Transaction
    Transaction,
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
    TerminalError,
    DatabaseError,
)

__all__ = [
    "Client",
    "Job",
    "JobState",
    "HealthCheck",
    "QueueHealth",
    "Transaction",
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
    "TerminalError",
    "DatabaseError",
]

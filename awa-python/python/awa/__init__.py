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
    # Dead Letter Queue
    DlqEntry,
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
    init_telemetry,
    shutdown_telemetry,
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

from awa.client import AsyncClient, Client

# `serve` is gated by the Rust `ui` feature. Source builds compiled with
# `--no-default-features --features cel` omit it, so the import has to be
# optional or `import awa` blows up before the worker SDK is reachable.
try:
    from awa._awa import serve  # noqa: F401
except ImportError:
    serve = None  # type: ignore[assignment]

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
    # Dead Letter Queue
    "DlqEntry",
    # Transaction
    "Transaction",
    "SyncTransaction",
    # Handler return types
    "RetryAfter",
    "Snooze",
    "Cancel",
    # Functions
    "derive_kind",
    "migrate",
    "migrations",
    "migrations_range",
    "current_migration_version",
    "init_telemetry",
    "shutdown_telemetry",
    # Web UI server
    "serve",
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

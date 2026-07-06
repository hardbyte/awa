"""Awa — Postgres-native background job queue for Rust and Python."""

from awa._awa import (
    # Raw PyO3 client (kept for backwards compat, use Client/AsyncClient instead)
    Client as RawClient,
    # Job types
    Job,
    JobState,
    PartitionedQueue,
    HealthCheck,
    QueueHealth,
    QueueStat,
    # Callback
    CallbackToken,
    WaitForCallback,
    ResolveResult,
    # Admin
    RetryFailedResult,
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
    StorageNotFinalized,
    UnknownJobKind,
    SerializationError,
    ValidationError,
    TerminalError,
    DatabaseError,
    CallbackNotFound,
)

from awa.client import (
    AsyncClient,
    BatchOperation,
    BatchOperationFilter,
    BatchOperationKind,
    BatchOperationPreview,
    BatchOperationState,
    Client,
)
from awa import callback_contract

__all__ = [
    # Clients
    "Client",
    "AsyncClient",
    "RawClient",
    "BatchOperation",
    "BatchOperationFilter",
    "BatchOperationKind",
    "BatchOperationPreview",
    "BatchOperationState",
    # Job types
    "Job",
    "JobState",
    "PartitionedQueue",
    "HealthCheck",
    "QueueHealth",
    "QueueStat",
    # Callback
    "CallbackToken",
    "WaitForCallback",
    "ResolveResult",
    # Admin
    "RetryFailedResult",
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
    # Exceptions
    "AwaError",
    "UniqueConflict",
    "SchemaNotMigrated",
    "StorageNotFinalized",
    "UnknownJobKind",
    "SerializationError",
    "ValidationError",
    "TerminalError",
    "DatabaseError",
    "CallbackNotFound",
    # Callback receiver contract (for user-owned API layers)
    "callback_contract",
]

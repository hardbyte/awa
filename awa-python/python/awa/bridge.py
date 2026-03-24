"""ORM/driver transaction bridging for Awa.

Insert Awa jobs within existing psycopg3 or asyncpg transactions — no separate
Awa connection required.

Usage with psycopg (async)::

    import psycopg
    import awa
    from awa.bridge import insert_job

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        async with conn.transaction():
            await conn.execute("INSERT INTO orders ...")
            job = await insert_job(conn, SendEmail(to="a@b.com", subject="Hi"))

Usage with asyncpg::

    import asyncpg
    from awa.bridge import insert_job

    conn = await asyncpg.connect(dsn)
    async with conn.transaction():
        await conn.execute("INSERT INTO orders ...")
        job = await insert_job(conn, SendEmail(to="a@b.com", subject="Hi"))

Usage with psycopg (sync)::

    import psycopg
    from awa.bridge import insert_job_sync

    with psycopg.Connection.connect(dsn) as conn:
        with conn.transaction():
            conn.execute("INSERT INTO orders ...")
            job = insert_job_sync(conn, SendEmail(to="a@b.com", subject="Hi"))
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from typing import Any

from awa._awa import derive_kind

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# $-style placeholders (asyncpg, native PostgreSQL)
_INSERT_SQL_DOLLAR = """\
INSERT INTO awa.jobs
    (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
VALUES
    ($1, $2, $3::jsonb, $4, $5, $6, COALESCE($7, now()), $8::jsonb, $9)
RETURNING id, kind, queue, args, state, priority, attempt, max_attempts,
          run_at, created_at, metadata, tags
"""

# %s-style placeholders (psycopg3)
_INSERT_SQL_PERCENT = """\
INSERT INTO awa.jobs
    (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
VALUES
    (%s, %s, %s::jsonb, %s, %s, %s, COALESCE(%s, now()), %s::jsonb, %s)
RETURNING id, kind, queue, args, state, priority, attempt, max_attempts,
          run_at, created_at, metadata, tags
"""


# ---------------------------------------------------------------------------
# Arg serialization (pure Python, no PyO3 dependency)
# ---------------------------------------------------------------------------

def _reject_null_bytes(obj: Any) -> None:
    """Recursively check for null bytes in string values."""
    if isinstance(obj, str):
        if "\x00" in obj:
            raise ValueError(
                "Job args must not contain null bytes (\\u0000): "
                "Postgres JSONB does not support them"
            )
    elif isinstance(obj, dict):
        for key, value in obj.items():
            _reject_null_bytes(key)
            _reject_null_bytes(value)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            _reject_null_bytes(item)


def _serialize_args(args: Any) -> str:
    """Serialize job args to a JSON string.

    Supports dataclasses, pydantic BaseModel, and plain dicts.
    """
    if dataclasses.is_dataclass(args) and not isinstance(args, type):
        data = dataclasses.asdict(args)
    elif hasattr(args, "model_dump"):
        # Pydantic BaseModel
        data = args.model_dump(mode="json")
    elif isinstance(args, dict):
        data = args
    else:
        raise TypeError(
            f"Job args must be a dataclass, pydantic BaseModel, or dict, "
            f"got {type(args).__name__}"
        )

    # Check for null bytes in raw string values before JSON encoding
    # (json.dumps escapes \x00 to \u0000, so check the source data)
    _reject_null_bytes(data)
    return json.dumps(data, default=str)


def _derive_kind(args: Any, kind: str | None) -> str:
    """Derive the job kind from args type or use the explicit kind."""
    if kind is not None:
        return kind
    if isinstance(args, dict):
        if "kind" in args:
            return args["kind"]
        raise TypeError(
            "Dict args require an explicit 'kind' parameter or a 'kind' key"
        )
    class_name = type(args).__name__
    return derive_kind(class_name)


def _determine_state(run_at: str | datetime | None) -> str:
    """Return 'scheduled' if run_at is in the future, else 'available'."""
    if run_at is None:
        return "available"
    if isinstance(run_at, str):
        run_at = datetime.fromisoformat(run_at)
    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=timezone.utc)
    if run_at > datetime.now(timezone.utc):
        return "scheduled"
    return "available"


def _normalize_run_at(run_at: str | datetime | None) -> datetime | None:
    """Convert run_at to a datetime object, or None.

    asyncpg requires native datetime objects for timestamptz params.
    """
    if run_at is None:
        return None
    if isinstance(run_at, str):
        dt = datetime.fromisoformat(run_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    if isinstance(run_at, datetime):
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=timezone.utc)
        return run_at
    raise TypeError(f"run_at must be a string or datetime, got {type(run_at).__name__}")


def _prepare_params(
    args: Any,
    *,
    kind: str | None = None,
    queue: str = "default",
    priority: int = 2,
    max_attempts: int = 25,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    run_at: str | datetime | None = None,
) -> tuple[str, str, str, str, int, int, datetime | None, str, list[str]]:
    """Prepare the 9 bind parameters for the INSERT statement.

    Returns (kind, queue, args_json, state, priority, max_attempts,
             run_at_dt, metadata_json, tags).
    """
    resolved_kind = _derive_kind(args, kind)
    args_json = _serialize_args(args)
    state = _determine_state(run_at)
    run_at_dt = _normalize_run_at(run_at)
    metadata_json = json.dumps(metadata or {})
    resolved_tags = tags if tags is not None else []

    return (
        resolved_kind,
        queue,
        args_json,
        state,
        priority,
        max_attempts,
        run_at_dt,
        metadata_json,
        resolved_tags,
    )


# ---------------------------------------------------------------------------
# Row → dict helper
# ---------------------------------------------------------------------------

def _row_to_dict(row: Any, driver: str) -> dict[str, Any]:
    """Convert a database row to a plain dict."""
    if driver == "asyncpg":
        # asyncpg.Record supports dict()
        return dict(row)
    # psycopg3 returns rows with column access via index or .column_name
    if hasattr(row, "_asdict"):
        return row._asdict()
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}
    # Fallback: already a dict
    return dict(row)


# ---------------------------------------------------------------------------
# Driver detection
# ---------------------------------------------------------------------------

def _detect_driver(conn: Any) -> str:
    """Detect the database driver from a connection object.

    Returns 'asyncpg', 'psycopg_async', or 'psycopg_sync'.
    """
    module = type(conn).__module__ or ""

    if module.startswith("asyncpg"):
        return "asyncpg"

    if module.startswith("psycopg"):
        # Distinguish async vs sync psycopg connections
        cls_name = type(conn).__name__
        if "Async" in cls_name:
            return "psycopg_async"
        return "psycopg_sync"

    raise TypeError(
        f"Unsupported connection type: {type(conn).__qualname__} "
        f"(module={module}). Supported drivers: asyncpg, psycopg3."
    )


# ---------------------------------------------------------------------------
# Public API: async
# ---------------------------------------------------------------------------

async def insert_job(
    conn: Any,
    args: Any,
    *,
    kind: str | None = None,
    queue: str = "default",
    priority: int = 2,
    max_attempts: int = 25,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    run_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Insert an Awa job using an existing database connection.

    Works with asyncpg connections/transactions and psycopg3
    AsyncConnection objects. The caller is responsible for transaction
    management — call this within your existing transaction block.

    Returns a dict with the inserted job's columns (id, kind, queue,
    state, args, etc.).
    """
    params = _prepare_params(
        args,
        kind=kind,
        queue=queue,
        priority=priority,
        max_attempts=max_attempts,
        tags=tags,
        metadata=metadata,
        run_at=run_at,
    )

    driver = _detect_driver(conn)

    if driver == "asyncpg":
        row = await conn.fetchrow(_INSERT_SQL_DOLLAR, *params)
        return _row_to_dict(row, driver)

    if driver == "psycopg_async":
        cursor = await conn.execute(_INSERT_SQL_PERCENT, params)
        row = await cursor.fetchone()
        return _row_to_dict(row, driver)

    raise TypeError(
        f"insert_job() requires an async connection. "
        f"For sync psycopg3 connections, use insert_job_sync(). "
        f"Got: {type(conn).__qualname__}"
    )


# ---------------------------------------------------------------------------
# Public API: sync
# ---------------------------------------------------------------------------

def insert_job_sync(
    conn: Any,
    args: Any,
    *,
    kind: str | None = None,
    queue: str = "default",
    priority: int = 2,
    max_attempts: int = 25,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    run_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Insert an Awa job using a synchronous psycopg3 connection.

    The caller is responsible for transaction management — call this
    within your existing ``with conn.transaction():`` block.

    Returns a dict with the inserted job's columns.
    """
    params = _prepare_params(
        args,
        kind=kind,
        queue=queue,
        priority=priority,
        max_attempts=max_attempts,
        tags=tags,
        metadata=metadata,
        run_at=run_at,
    )

    driver = _detect_driver(conn)

    if driver == "psycopg_sync":
        cursor = conn.execute(_INSERT_SQL_PERCENT, params)
        row = cursor.fetchone()
        return _row_to_dict(row, driver)

    raise TypeError(
        f"insert_job_sync() requires a sync psycopg3 connection. "
        f"For async connections, use insert_job(). "
        f"Got: {type(conn).__qualname__}"
    )

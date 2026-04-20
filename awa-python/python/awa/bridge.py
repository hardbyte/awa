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

Usage with SQLAlchemy::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from awa.bridge import insert_job_sync

    engine = create_engine("postgresql+psycopg://...")
    with Session(engine) as session, session.begin():
        session.execute(...)
        job = insert_job_sync(session, SendEmail(to="a@b.com", subject="Hi"))

Usage with Django::

    from django.db import connection, transaction
    from awa.bridge import insert_job_sync

    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO orders ...")
        job = insert_job_sync(connection, SendEmail(to="a@b.com", subject="Hi"))
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
SELECT id, kind, queue, args, state, priority, attempt, max_attempts,
       run_at, created_at, metadata, tags
FROM awa.insert_job_compat(
    $1,
    $2,
    $3::jsonb,
    $4::awa.job_state,
    $5::smallint,
    $6::smallint,
    $7::timestamptz,
    $8::jsonb,
    $9::text[]
)
"""

# %s-style placeholders (psycopg3)
_INSERT_SQL_PERCENT = """\
SELECT id, kind, queue, args, state, priority, attempt, max_attempts,
       run_at, created_at, metadata, tags
FROM awa.insert_job_compat(
    %s,
    %s,
    %s::jsonb,
    %s::awa.job_state,
    %s::smallint,
    %s::smallint,
    %s::timestamptz,
    %s::jsonb,
    %s::text[]
)
"""

# Named parameters for SQLAlchemy.
_INSERT_SQL_NAMED = """\
SELECT id, kind, queue, args, state, priority, attempt, max_attempts,
       run_at, created_at, metadata, tags
FROM awa.insert_job_compat(
    :kind,
    :queue,
    CAST(:args AS jsonb),
    CAST(:state AS awa.job_state),
    CAST(:priority AS smallint),
    CAST(:max_attempts AS smallint),
    CAST(:run_at AS timestamptz),
    CAST(:metadata AS jsonb),
    :tags
)
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


def _coerce_job_data(args: Any) -> Any:
    """Convert supported job args to plain JSON-compatible Python data."""
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

    return data


def _serialize_args(args: Any) -> str:
    """Serialize job args to a JSON string."""
    data = _coerce_job_data(args)

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
    """Match Awa insert semantics: any explicit run_at starts as scheduled."""
    return "scheduled" if run_at is not None else "available"


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


def _prepare_values(
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
    """Prepare shared insert values for all bridge backends."""
    args_data = _coerce_job_data(args)
    metadata_data = metadata or {}
    _reject_null_bytes(args_data)
    _reject_null_bytes(metadata_data)

    resolved_kind = _derive_kind(args, kind)
    state = _determine_state(run_at)
    run_at_dt = _normalize_run_at(run_at)
    resolved_tags = tags if tags is not None else []

    return {
        "kind": resolved_kind,
        "queue": queue,
        "args_data": args_data,
        "args_json": json.dumps(args_data, default=str),
        "state": state,
        "priority": priority,
        "max_attempts": max_attempts,
        "run_at": run_at_dt,
        "metadata_data": metadata_data,
        "metadata_json": json.dumps(metadata_data, default=str),
        "tags": resolved_tags,
    }


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


def _row_from_cursor(cursor: Any, row: Any) -> dict[str, Any]:
    """Convert a DB-API/psycopg row using cursor metadata when needed."""
    if hasattr(row, "_asdict"):
        return row._asdict()
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}
    if cursor.description is not None:
        columns = [getattr(col, "name", col[0]) for col in cursor.description]
        return dict(zip(columns, row, strict=False))
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

    if module.startswith("sqlalchemy"):
        cls_name = type(conn).__name__
        if "Async" in cls_name:
            return "sqlalchemy_async"
        return "sqlalchemy_sync"

    if module.startswith("django") or (
        hasattr(conn, "cursor")
        and hasattr(conn, "vendor")
        and getattr(conn, "vendor", None) == "postgresql"
    ):
        return "django_sync"

    raise TypeError(
        f"Unsupported connection type: {type(conn).__qualname__} "
        f"(module={module}). Supported drivers: asyncpg, psycopg3, "
        f"SQLAlchemy, and Django."
    )


def _sqlalchemy_text() -> Any:
    """Build the typed SQLAlchemy text statement lazily."""
    from sqlalchemy import bindparam, text
    from sqlalchemy.dialects.postgresql import ARRAY, JSONB
    from sqlalchemy.types import DateTime, String

    return text(_INSERT_SQL_NAMED).bindparams(
        bindparam("args", type_=JSONB),
        bindparam("metadata", type_=JSONB),
        bindparam("tags", type_=ARRAY(String())),
        bindparam("run_at", type_=DateTime(timezone=True)),
    )


def _sqlalchemy_params(values: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": values["kind"],
        "queue": values["queue"],
        "args": values["args_data"],
        "state": values["state"],
        "priority": values["priority"],
        "max_attempts": values["max_attempts"],
        "run_at": values["run_at"],
        "metadata": values["metadata_data"],
        "tags": values["tags"],
    }


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
    values = _prepare_values(
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
        row = await conn.fetchrow(
            _INSERT_SQL_DOLLAR,
            values["kind"],
            values["queue"],
            values["args_json"],
            values["state"],
            values["priority"],
            values["max_attempts"],
            values["run_at"],
            values["metadata_json"],
            values["tags"],
        )
        return _row_to_dict(row, driver)

    if driver == "psycopg_async":
        cursor = await conn.execute(
            _INSERT_SQL_PERCENT,
            (
                values["kind"],
                values["queue"],
                values["args_json"],
                values["state"],
                values["priority"],
                values["max_attempts"],
                values["run_at"],
                values["metadata_json"],
                values["tags"],
            ),
        )
        row = await cursor.fetchone()
        return _row_from_cursor(cursor, row)

    if driver == "sqlalchemy_async":
        result = await conn.execute(_sqlalchemy_text(), _sqlalchemy_params(values))
        return dict(result.mappings().one())

    raise TypeError(
        f"insert_job() requires an async connection. "
        f"For sync psycopg3/SQLAlchemy/Django connections, use "
        f"insert_job_sync(). "
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
    values = _prepare_values(
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
        cursor = conn.execute(
            _INSERT_SQL_PERCENT,
            (
                values["kind"],
                values["queue"],
                values["args_json"],
                values["state"],
                values["priority"],
                values["max_attempts"],
                values["run_at"],
                values["metadata_json"],
                values["tags"],
            ),
        )
        row = cursor.fetchone()
        return _row_from_cursor(cursor, row)

    if driver == "sqlalchemy_sync":
        result = conn.execute(_sqlalchemy_text(), _sqlalchemy_params(values))
        return dict(result.mappings().one())

    if driver == "django_sync":
        with conn.cursor() as cursor:
            cursor.execute(
                _INSERT_SQL_PERCENT,
                (
                    values["kind"],
                    values["queue"],
                    values["args_json"],
                    values["state"],
                    values["priority"],
                    values["max_attempts"],
                    values["run_at"],
                    values["metadata_json"],
                    values["tags"],
                ),
            )
            row = cursor.fetchone()
            return _row_from_cursor(cursor, row)

    raise TypeError(
        f"insert_job_sync() requires a sync psycopg3/SQLAlchemy/Django "
        f"connection. For async connections, use insert_job(). "
        f"Got: {type(conn).__qualname__}"
    )

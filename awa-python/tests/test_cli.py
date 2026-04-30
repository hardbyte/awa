"""Smoke tests for the `python -m awa` entry point.

We spawn the CLI as a subprocess so argparse wiring, subparser dispatch,
and the Python→Rust admin paths all exercise end-to-end. These tests
complement test_dlq.py (which covers the underlying PyO3 methods) by
catching regressions in the CLI glue itself.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


def _cli(*args: str, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "awa", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_help_lists_all_subcommands():
    """Regression: argparse wiring for every subcommand stays intact."""
    result = _cli("--help")
    assert result.returncode == 0, result.stderr
    for cmd in ("migrate", "job", "queue", "dlq", "cron", "storage"):
        assert cmd in result.stdout, f"top-level {cmd} missing from --help"


@pytest.mark.parametrize(
    "group,expected_subs",
    [
        ("job", ("dump", "dump-run", "retry", "cancel", "retry-failed", "discard", "list")),
        ("queue", ("pause", "resume", "drain", "stats")),
        ("dlq", ("list", "depth", "retry", "retry-all", "purge")),
        ("cron", ("list", "remove")),
        ("storage", ("status",)),
    ],
)
def test_subcommand_help(group: str, expected_subs: tuple[str, ...]):
    result = _cli(group, "--help")
    assert result.returncode == 0, result.stderr
    for sub in expected_subs:
        assert sub in result.stdout, f"{group} {sub} missing from help"


def test_migrate_sql_without_db_url(tmp_path):
    """--sql --version should print migration SQL without needing a DB."""
    result = _cli("migrate", "--version", "1")
    assert result.returncode == 0, result.stderr
    assert "Migration V1" in result.stdout


def test_queue_stats_without_db_url_errors_cleanly():
    result = _cli("queue", "stats")
    assert result.returncode == 1
    assert "--database-url is required" in result.stderr


def test_queue_stats_alias_backcompat():
    """'queue-stats' is a legacy alias that still resolves to the new impl."""
    result = _cli("queue-stats")
    assert result.returncode == 1
    assert "--database-url is required" in result.stderr


def test_dlq_list_parses_before_dlq_at_argument_type():
    """Regression: --before-dlq-at must be parsed as datetime before hitting
    the client, which expects datetime not str."""
    # Invalid datetime — must fail with our own explicit error, not an opaque
    # type-conversion traceback from deep in the Rust bindings.
    result = _cli(
        "--database-url",
        "postgres://127.0.0.1:1/nonexistent",
        "dlq",
        "list",
        "--before-dlq-at",
        "not-a-date",
    )
    # argparse rejects the bad value before any DB call — exit code is 2
    # (argparse usage error) rather than 1 (our own sys.exit). Either is fine
    # for a hard failure; what matters is that the message mentions the flag.
    assert result.returncode != 0
    assert "before-dlq-at" in result.stderr.lower()


def test_job_retry_failed_requires_filter():
    result = _cli(
        "--database-url",
        "postgres://127.0.0.1:1/nonexistent",
        "job",
        "retry-failed",
    )
    assert result.returncode == 1
    assert "--kind" in result.stderr and "--queue" in result.stderr


# ── live integration ────────────────────────────────────────────────────
#
# These require a live Postgres at DATABASE_URL. On canonical backends the
# DLQ APIs are refused, so we bootstrap a throwaway queue_storage schema
# the same way test_dlq.py does.

SCHEMA = "awa_py_cli_dlq"


@dataclass
class CliDlqJob:
    value: str


@pytest.fixture
def sync_client_qs():
    client = awa.Client(DATABASE_URL)
    client.migrate()
    tx = client.transaction()
    tx.execute("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
    tx.commit()
    client.install_queue_storage(schema=SCHEMA, reset=True)
    tx = client.transaction()
    tx.execute("DELETE FROM awa.job_unique_claims")
    tx.commit()
    try:
        yield client
    finally:
        client.close()


def _move_ready_to_failed_done(client: awa.Client, job_id: int) -> None:
    # Mirror test_dlq.py's helper so we can seed a failed row inside the
    # queue_storage schema without booting a real worker. Kept byte-for-byte
    # in sync with tests/test_dlq.py::_move_ready_to_failed_done.
    tx = client.transaction()
    tx.execute(
        f"""
        WITH moved AS (
            DELETE FROM {SCHEMA}.ready_entries
            WHERE job_id = $1
            RETURNING ready_slot, ready_generation, job_id, kind, queue, args,
                      priority, attempt, run_lease, max_attempts, lane_seq,
                      run_at, attempted_at, created_at, unique_key,
                      unique_states, payload
        ),
        released AS (
            SELECT awa.release_queue_storage_unique_claim(
                job_id, unique_key, unique_states, 'available'::awa.job_state
            )
            FROM moved
        ),
        lane_fix AS (
            UPDATE {SCHEMA}.queue_lanes AS lanes
            SET available_count = GREATEST(0, lanes.available_count - delta.moved_count)
            FROM (
                SELECT queue, priority, count(*)::bigint AS moved_count
                FROM moved
                GROUP BY queue, priority
            ) AS delta
            WHERE lanes.queue = delta.queue
              AND lanes.priority = delta.priority
        )
        INSERT INTO {SCHEMA}.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, args, state,
            priority, attempt, run_lease, max_attempts, lane_seq,
            run_at, attempted_at, finalized_at, created_at,
            unique_key, unique_states, payload
        )
        SELECT ready_slot, ready_generation, job_id, kind, queue, args,
               'failed'::awa.job_state, priority, GREATEST(attempt, 1),
               run_lease, max_attempts, lane_seq, run_at,
               COALESCE(attempted_at, now()), now(), created_at,
               unique_key, unique_states, payload
        FROM moved
        """,
        job_id,
    )
    tx.commit()


def test_dlq_list_renders_nested_job_fields(sync_client_qs):
    """Regression for Codex review on PR #188.

    ``client.list_dlq()`` returns ``DlqEntry`` objects whose id/kind/queue
    fields live under ``.job``. A previous draft of __main__.py reached for
    ``row.id`` / ``row.kind`` / ``row.queue`` directly, which raised
    AttributeError as soon as the DLQ was non-empty. This test seeds a
    DLQ row and invokes ``awa dlq list`` to verify the formatter reads the
    nested fields.
    """
    queue = "pydlq_cli"
    job = sync_client_qs.insert(CliDlqJob(value="cli"), queue=queue)
    _move_ready_to_failed_done(sync_client_qs, job.id)
    entry = sync_client_qs.move_failed_to_dlq(job.id, "cli_test")
    assert entry is not None

    result = _cli("--database-url", DATABASE_URL, "dlq", "list", "--queue", queue)
    assert result.returncode == 0, result.stderr
    assert str(job.id) in result.stdout, result.stdout
    assert queue in result.stdout
    # "Next page" hint uses last.dlq_at.isoformat(); bare repr would leak
    # the "datetime.datetime(" prefix and the pagination round-trip would
    # break. Assert the isoformat shape.
    assert "datetime.datetime" not in result.stdout

    # Cleanup so reruns stay idempotent.
    sync_client_qs.purge_dlq(queue=queue)

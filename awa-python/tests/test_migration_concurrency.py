"""Concurrent-migration safety for the Python client.

Races several ``client.migrate()`` calls from separate OS threads — each with
its own client and connection pool — against a freshly dropped schema. This is
the Python mirror of the Rust ``test_concurrent_runs_apply_once_and_converge``:
the migration advisory lock must serialize the runners so every call succeeds
and the schema converges on the build's version, with each migration recorded
exactly once.

``migrate()`` maps to ``migrate_sync`` on the raw client, which releases the
GIL for the duration of the migration, so the threads contend for real on the
Postgres advisory lock rather than serializing behind the interpreter.

Requires Postgres running at localhost:15432.
"""

import concurrent.futures
import os

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

RACERS = 8


def _drop_schema() -> None:
    client = awa.Client(DATABASE_URL, max_connections=2)
    tx = client.transaction()
    tx.execute("DROP SCHEMA IF EXISTS awa CASCADE")
    tx.commit()
    client.close()


def test_concurrent_migrate_converges():
    _drop_schema()

    def migrate_once() -> None:
        # A distinct client per thread => a distinct connection pool => the
        # racers genuinely contend on the Postgres advisory lock.
        client = awa.Client(DATABASE_URL, max_connections=2)
        try:
            client.migrate()
        finally:
            client.close()

    errors: list[BaseException] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=RACERS) as pool:
        futures = [pool.submit(migrate_once) for _ in range(RACERS)]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

    assert not errors, f"concurrent migrate() calls failed: {errors}"

    # Converged: schema at the build's version, no torn/duplicate application.
    client = awa.Client(DATABASE_URL, max_connections=2)
    tx = client.transaction()
    max_version = tx.fetch_one("SELECT max(version) AS v FROM awa.schema_version")["v"]
    recorded = tx.fetch_one("SELECT count(*) AS n FROM awa.schema_version")["n"]
    duplicates = tx.fetch_one(
        "SELECT count(*) AS n FROM ("
        "  SELECT version FROM awa.schema_version GROUP BY version HAVING count(*) > 1"
        ") AS d"
    )["n"]
    tx.commit()
    client.close()

    assert max_version == awa.current_migration_version()
    assert duplicates == 0, "schema_version has duplicate rows"
    # One row per known migration; len(migrations()) accounts for the reserved
    # v008 gap, so it is the migration count rather than CURRENT_VERSION.
    assert recorded == len(awa.migrations())

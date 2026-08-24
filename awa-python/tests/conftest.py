"""Suite-wide fixtures and guards for the Python test session.

#420: late in a full run, one or two tests ERROR at *fixture setup* with
`pool timed out while waiting for an open connection`, always ~15 minutes
in, and a rerun always passes. Two things feed it, both addressed here.

**Peak connection demand.** `AsyncClient`/`Client` default to a
10-connection pool — a sensible application default, and far more than any
test needs. Re-running the suite against a `max_connections=30` server
makes the mechanism visible: 19 of 30 backends sit `idle`, held by pools
from tests that finished minutes earlier (sqlx keeps an idle connection for
its idle timeout, which defaults to 10 minutes). Each new fixture then
wants up to 10 more. When the server has none left, sqlx cannot grow the
pool and its 30s acquire timeout expires — which is the reported error, and
why the victim is whichever test came next rather than the test at fault.
`_TEST_POOL_MAX_CONNECTIONS` below caps what tests ask for. With the cap, the
same 312-test run on the same 30-connection server holds 3 backends where it
previously held 11-19, and passes. Callers that pass `max_connections`
explicitly are untouched.

**Non-deterministic pool teardown.** Two fixtures built a client and
`return`ed it instead of yielding and closing it, leaving teardown to
whenever CPython collected the object. Those are fixed;
`connection_leak_guard` keeps them fixed.
"""

import os

import pytest

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

# Enough for a client running a small worker fleet: the LISTEN/NOTIFY
# connection is held for the client's lifetime, and the dispatcher,
# heartbeat, and maintenance loops each want one alongside the handler's
# own queries. The library default of 10 is right for applications and
# more than any test here needs.
_TEST_POOL_MAX_CONNECTIONS = 5


def _cap_pool_size(cls: type) -> None:
    """Default this client class's pool to a test-sized one.

    Wraps `__init__` rather than editing the 27 test modules that
    construct clients. An explicit `max_connections` argument still wins,
    so a test that deliberately exercises pool sizing is unaffected.
    """
    original = cls.__init__

    def __init__(self, database_url, max_connections=None, **kwargs):
        if max_connections is None:
            max_connections = _TEST_POOL_MAX_CONNECTIONS
        original(self, database_url, max_connections, **kwargs)

    __init__.__wrapped__ = original
    cls.__init__ = __init__


for _cls in (awa.AsyncClient, awa.Client):
    _cap_pool_size(_cls)


# Slack over the baseline. Backend teardown is asynchronous on the server
# side, so a just-closed pool can still be visible for a moment. Kept below
# `_TEST_POOL_MAX_CONNECTIONS` so one leaked pool cannot hide under it.
CONNECTION_LEAK_SLACK = 3


def _backend_count() -> int | None:
    """Backends on this database, or None if we cannot ask."""
    try:
        client = awa.Client(DATABASE_URL, max_connections=1)
    except Exception:
        return None
    try:
        tx = client.transaction()
        row = tx.fetch_one(
            """
            SELECT count(*) AS n
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
            """
        )
        tx.commit()
        return int(row["n"])
    except Exception:
        return None
    finally:
        client.close()


@pytest.fixture(scope="session", autouse=True)
def connection_leak_guard():
    baseline = _backend_count()
    yield
    if baseline is None:
        return

    final = _backend_count()
    if final is None:
        return

    growth = final - baseline
    print(
        f"\n[connection-guard] backends at session start={baseline} "
        f"end={final} growth={growth}"
    )
    assert growth <= CONNECTION_LEAK_SLACK, (
        f"Connection leak: {growth} more backends at session end than at "
        f"start (baseline={baseline}, final={final}, slack="
        f"{CONNECTION_LEAK_SLACK}). A fixture is building an awa client and "
        f"returning it without closing it — use `yield c` with "
        f"`await c.close()` / `c.close()` in a finally block."
    )

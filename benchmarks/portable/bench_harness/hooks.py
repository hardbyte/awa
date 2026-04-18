"""Phase-type enter/exit runtime hooks.

Each hook receives a PhaseRuntime and may stash state in runtime.state that
its paired exit hook retrieves to clean up.

Hooks run synchronously on the orchestrator thread — they should complete
fast. Long-running side effects (held transactions, background readers) get
forked into threads that block on an event the exit hook signals.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

import psycopg

from .phases import PhaseRuntime


# ─── idle-in-tx ──────────────────────────────────────────────────────────
#
# Open a connection, BEGIN, SELECT txid_current(), sleep until the exit hook
# closes the connection. The transaction pins the cluster xmin horizon for
# the whole phase.


def enter_idle_in_tx(runtime: PhaseRuntime) -> None:
    stop = threading.Event()
    holder: dict[str, Any] = {"stop": stop}

    def _hold() -> None:
        # autocommit off: the transaction stays open until close().
        with psycopg.connect(runtime.database_url, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT txid_current()")
                xid = cur.fetchone()[0]
                holder["xid"] = xid
            # Block until the exit hook signals us. Don't commit/rollback
            # until then — autoexit via `with` rollback fires on stop.
            stop.wait()

    thread = threading.Thread(target=_hold, name="idle-in-tx-holder", daemon=True)
    thread.start()
    holder["thread"] = thread
    runtime.state["idle-in-tx"] = holder


def exit_idle_in_tx(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("idle-in-tx", None)
    if not holder:
        return
    holder["stop"].set()
    thread: threading.Thread = holder["thread"]
    thread.join(timeout=5.0)


# ─── active-readers ──────────────────────────────────────────────────────
#
# Open N overlapping REPEATABLE READ connections running a repeating scan
# query. Parity with awa's Rust MVCC bench `active_scan` mode.


def _reader_loop(
    database_url: str,
    stop: threading.Event,
    scan_sql: str,
) -> None:
    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            while not stop.is_set():
                try:
                    cur.execute(scan_sql)
                    cur.fetchall()
                except psycopg.Error:
                    # Swallow transient errors; keep the reader active.
                    pass
                time.sleep(0.1)


def enter_active_readers(runtime: PhaseRuntime) -> None:
    # A portable scan that hits each adapter's event tables lightly. The
    # adapter can override this via env.
    scan_sql = os.environ.get(
        "ACTIVE_READER_SQL",
        "SELECT count(*) FROM pg_stat_user_tables",
    )
    reader_count = int(os.environ.get("ACTIVE_READER_COUNT", "4"))
    stop = threading.Event()
    threads: list[threading.Thread] = []
    for i in range(reader_count):
        t = threading.Thread(
            target=_reader_loop,
            args=(runtime.database_url, stop, scan_sql),
            name=f"active-reader-{i}",
            daemon=True,
        )
        t.start()
        threads.append(t)
    runtime.state["active-readers"] = {"stop": stop, "threads": threads}


def exit_active_readers(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("active-readers", None)
    if not holder:
        return
    holder["stop"].set()
    for t in holder["threads"]:
        t.join(timeout=5.0)


# ─── high-load ───────────────────────────────────────────────────────────
#
# Signal the adapter to raise its producer rate for the duration of the
# phase. We write `PRODUCER_TARGET_RATE=<new>` to a well-known path that
# the adapter re-reads on each producer tick. Adapters that don't support
# dynamic rate changes simply ignore the file — the phase still runs the
# clean workload, which is strictly worse data but not a failure.


def enter_high_load(runtime: PhaseRuntime) -> None:
    multiplier = float(runtime.state.get("high_load_multiplier", 1.5))
    base = float(runtime.state.get("base_producer_rate", 800.0))
    control_file = runtime.state.get("producer_rate_control_file")
    if not control_file:
        return
    with Path(control_file).open("w") as fh:
        fh.write(str(base * multiplier))


def exit_high_load(runtime: PhaseRuntime) -> None:
    base = str(runtime.state.get("base_producer_rate", 800))
    control_file = runtime.state.get("producer_rate_control_file")
    if not control_file:
        return
    with Path(control_file).open("w") as fh:
        fh.write(base)

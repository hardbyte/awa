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
    ready = threading.Event()
    holder: dict[str, Any] = {"stop": stop, "ready": ready}

    def _hold() -> None:
        try:
            # autocommit off: the transaction stays open until close().
            with psycopg.connect(runtime.database_url, autocommit=False) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT txid_current()")
                    xid = cur.fetchone()[0]
                    holder["xid"] = xid
                # Signal success only after xid is captured: if BEGIN/SELECT
                # raised, the ready event is still set below with an error
                # so the caller can surface it instead of silently running a
                # no-op idle-in-tx phase.
                ready.set()
                # Block until the exit hook signals us. Don't commit/rollback
                # until then: autoexit via `with` rollback fires on stop.
                stop.wait()
        except Exception as exc:
            holder["error"] = exc
            ready.set()

    thread = threading.Thread(target=_hold, name="idle-in-tx-holder", daemon=True)
    thread.start()
    holder["thread"] = thread
    runtime.state["idle-in-tx"] = holder

    def _abort_holder() -> None:
        # The registry's exit hook won't run if we raise, so tear the
        # holder down here. Otherwise the thread could finish its connect
        # later and pin a transaction across subsequent phases.
        stop.set()
        runtime.state.pop("idle-in-tx", None)
        thread.join(timeout=1.0)

    # Wait until the holder either opens its transaction or errors out. The
    # phase is measuring "what happens when the MVCC horizon is pinned," so
    # silently running without a held transaction would make the measurement
    # meaningless.
    if not ready.wait(timeout=5.0):
        _abort_holder()
        raise RuntimeError(
            "idle-in-tx holder thread did not open a transaction within 5s"
        )
    if "error" in holder:
        err = holder["error"]
        _abort_holder()
        raise RuntimeError(
            f"idle-in-tx holder thread failed to open transaction: {err}"
        ) from err


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
    # The PlanetScale failure mode is about analytics readers hitting the
    # queue's hot tables while the primary churns them. Default to scanning
    # the first event table from the adapter's manifest so the reader
    # actually exercises that path; fall back to a catalog scan only if
    # the manifest didn't declare any event tables. Callers can still
    # override via env for bespoke analytics shapes.
    event_tables = runtime.state.get("event_tables") or []
    default_sql: str
    if event_tables:
        first = str(event_tables[0])
        default_sql = f"SELECT count(*) FROM {first}"
    else:
        default_sql = "SELECT count(*) FROM pg_stat_user_tables"
    scan_sql = os.environ.get("ACTIVE_READER_SQL", default_sql)
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


# ─── kill-worker / start-worker ──────────────────────────────────────────
#
# Destructive / lifecycle phase types. Both act on the replica pool stashed
# on `runtime.state["replica_pool"]` by `orchestrator.run_one_system`. The
# `instance` param (default 0) selects which replica; phase parsing lets
# a scenario write `kill=kill-worker(instance=2):60s`.
#
# Neither has an exit hook — the enter-side action is the whole story.
# Restart-on-exit would conflate "kill" with "kill then restart" and make
# the named-scenario composition (crash_recovery, rolling-replace) harder
# to reason about. Scenarios that need restart follow a kill-worker phase
# with a start-worker phase.
#
# A ValueError from the pool (out-of-range instance_id, already running for
# start-worker, etc.) propagates up through the phase loop as a hard abort;
# that's appropriate — a scenario targeting a non-existent replica is
# misconfigured, not a chaos data point.


def enter_kill_worker(runtime: PhaseRuntime) -> None:
    pool = runtime.state.get("replica_pool")
    if pool is None:
        # Importing the type here would create a circular phases→replica_pool
        # dependency; duck-type instead.
        raise RuntimeError(
            "kill-worker requires state['replica_pool']. "
            "The orchestrator sets this in run_one_system; running a "
            "kill-worker phase outside that context is a misconfiguration."
        )
    instance = runtime.phase.int_param("instance", default=0)
    pool.kill_worker(instance)


def enter_start_worker(runtime: PhaseRuntime) -> None:
    pool = runtime.state.get("replica_pool")
    if pool is None:
        raise RuntimeError(
            "start-worker requires state['replica_pool']. "
            "The orchestrator sets this in run_one_system; running a "
            "start-worker phase outside that context is a misconfiguration."
        )
    instance = runtime.phase.int_param("instance", default=0)
    pool.start_worker(instance)

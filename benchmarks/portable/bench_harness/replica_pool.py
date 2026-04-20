"""Replica lifecycle: start / stop / restart / kill by instance_id.

The pool is the harness-side handle that destructive phase types
(kill-worker, stop-worker, rolling-replace in #174 step 4+) act on.
Today only `run_one_system` touches it — it starts N replicas up front,
watches them during phases, and terminates them at the end. The pool
keeps the *intended* state per instance (running vs. deliberately
stopped) so a surprise exit during a phase can be distinguished from an
exit the scenario itself asked for:

    pool.stop_worker(0, signal=signal.SIGTERM)   # graceful
    pool.kill_worker(1)                          # SIGKILL
    pool.start_worker(0)                         # restart replica 0

The launcher-level machinery (`_launch_one_replica`) stays in
``orchestrator.py``; this module holds only the pool logic and owns the
closure the pool uses to create new replicas on demand. That indirection
is what makes the unit tests possible: a test can pass in a fake
``launch_fn`` and assert the state-machine transitions without needing
Docker or a Postgres.
"""

from __future__ import annotations

import enum
import signal as _signal
import subprocess
import sys
import threading
from dataclasses import dataclass
from typing import Callable

# The `LaunchFn` callable is structural — the pool doesn't care about
# the caller's ingestion pipeline (phase tracker, sample queue, etc.),
# only that `launch_fn(instance_id)` returns the four handles below.
# That indirection keeps the pool trivially fakeable in unit tests.


class ReplicaState(str, enum.Enum):
    """Harness-observable lifecycle states for a replica slot.

    The pool tracks the *intended* state, not just the process's actual
    state. `_sleep_or_abort` compares both: if a replica is intended to
    be RUNNING but its process has exited, the scenario has hit a real
    fault. If it's STOPPED or KILLED, exit is expected.
    """

    # Never launched in this run.
    UNSTARTED = "unstarted"
    # Launched, descriptor handshake done, intended to be running.
    RUNNING = "running"
    # Deliberately stopped (SIGTERM via stop_worker). Not considered a fault.
    STOPPED = "stopped"
    # Deliberately killed (SIGKILL via kill_worker). Not considered a fault.
    KILLED = "killed"
    # Exited unexpectedly while intended state was RUNNING. Fault.
    CRASHED = "crashed"


@dataclass
class ReplicaSlot:
    """One instance slot. Tracks handle + intended state.

    The handle (subprocess + tailer + descriptor) is populated after
    `start_worker` completes. It's cleared on stop/kill so that checks
    against poll() don't mistake a terminated prior incarnation for a
    live replica.
    """

    instance_id: int
    state: ReplicaState = ReplicaState.UNSTARTED
    # Populated by start_worker. Cleared by stop/kill. Set to the replica's
    # subprocess handle when RUNNING or CRASHED.
    process: subprocess.Popen | None = None
    # Set alongside process; joined on stop/kill.
    tailer: threading.Thread | None = None
    # Per-replica tailer stop signal.
    stop_event: threading.Event | None = None
    # Most recent descriptor record the replica emitted on startup.
    descriptor: dict | None = None


# Signature of the launcher callback the pool invokes to create a replica.
# Returns (process, tailer, stop_event, descriptor) — matching what
# `orchestrator._launch_one_replica` produces.
LaunchFn = Callable[
    [int],
    tuple[subprocess.Popen, threading.Thread, threading.Event, dict],
]


class ReplicaPool:
    """Lifecycle for N replicas of a single system.

    Construction is *capacity* only — nothing is launched until
    ``start_all`` or ``start_worker(i)`` is called. That lets test code
    inspect the slots in their pre-launch state without racing a real
    subprocess spawn.
    """

    def __init__(
        self,
        *,
        system: str,
        capacity: int,
        launch_fn: LaunchFn,
    ) -> None:
        if capacity < 1:
            raise ValueError(f"ReplicaPool capacity must be >=1, got {capacity}")
        self.system = system
        self.capacity = capacity
        self._launch_fn = launch_fn
        self._slots: list[ReplicaSlot] = [
            ReplicaSlot(instance_id=i) for i in range(capacity)
        ]

    # ── Introspection ──────────────────────────────────────────────────

    def slot(self, instance_id: int) -> ReplicaSlot:
        self._check_id(instance_id)
        return self._slots[instance_id]

    @property
    def slots(self) -> list[ReplicaSlot]:
        return list(self._slots)

    @property
    def running_replicas(self) -> list[ReplicaSlot]:
        return [s for s in self._slots if s.state is ReplicaState.RUNNING]

    @property
    def descriptor(self) -> dict | None:
        """First non-None descriptor across the pool, for manifest inclusion.

        Mirrors the legacy one-descriptor-per-system shape in
        manifest.json. The drift check in orchestrator has already logged
        any warning about cross-replica disagreement.
        """
        for slot in self._slots:
            if slot.descriptor is not None:
                return slot.descriptor
        return None

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start_all(self) -> None:
        """Launch every slot. On failure, tear down whatever started."""
        started: list[int] = []
        try:
            for i in range(self.capacity):
                self.start_worker(i)
                started.append(i)
        except Exception:
            for i in started:
                self._terminate_slot(
                    self._slots[i], signal_type=_signal.SIGKILL, timeout_s=5.0
                )
            raise

    def start_worker(self, instance_id: int) -> None:
        """Launch (or relaunch) a specific slot.

        Valid from UNSTARTED, STOPPED, KILLED, or CRASHED states — any
        non-RUNNING state can transition back. Re-launching a RUNNING
        slot is a bug in the caller, not a scenario we try to paper over.
        """
        slot = self.slot(instance_id)
        if slot.state is ReplicaState.RUNNING:
            raise RuntimeError(
                f"{self.system} replica {instance_id} is already RUNNING; "
                f"stop it before starting again"
            )
        proc, tailer, stop_event, descriptor = self._launch_fn(instance_id)
        slot.process = proc
        slot.tailer = tailer
        slot.stop_event = stop_event
        slot.descriptor = descriptor
        slot.state = ReplicaState.RUNNING

    def stop_worker(
        self, instance_id: int, *, timeout_s: float = 10.0
    ) -> None:
        """Graceful shutdown (SIGTERM). Escalates to SIGKILL on timeout."""
        slot = self.slot(instance_id)
        if slot.state is not ReplicaState.RUNNING:
            # Idempotent: stopping an already-stopped slot is a no-op.
            return
        self._terminate_slot(slot, signal_type=_signal.SIGTERM, timeout_s=timeout_s)
        slot.state = ReplicaState.STOPPED

    def kill_worker(
        self, instance_id: int, *, timeout_s: float = 2.0
    ) -> None:
        """Immediate hard kill (SIGKILL). No graceful window."""
        slot = self.slot(instance_id)
        if slot.state is not ReplicaState.RUNNING:
            return
        self._terminate_slot(slot, signal_type=_signal.SIGKILL, timeout_s=timeout_s)
        slot.state = ReplicaState.KILLED

    def restart_worker(
        self,
        instance_id: int,
        *,
        stop_signal: int = _signal.SIGTERM,
        stop_timeout_s: float = 10.0,
    ) -> None:
        """Stop then start. Handy for rolling-replace phase sequences."""
        slot = self.slot(instance_id)
        if slot.state is ReplicaState.RUNNING:
            if stop_signal == _signal.SIGKILL:
                self.kill_worker(instance_id, timeout_s=stop_timeout_s)
            else:
                self.stop_worker(instance_id, timeout_s=stop_timeout_s)
        self.start_worker(instance_id)

    def stop_all(self, *, timeout_s: float = 10.0) -> None:
        """Stop every RUNNING replica. Best-effort terminal teardown used
        at the end of run_one_system — idempotent and safe to call even
        when some replicas were already KILLED or CRASHED mid-run."""
        for slot in self._slots:
            if slot.state is ReplicaState.RUNNING:
                self._terminate_slot(
                    slot, signal_type=_signal.SIGTERM, timeout_s=timeout_s
                )
                slot.state = ReplicaState.STOPPED

    # ── Health watch ───────────────────────────────────────────────────

    def detect_crashes(self) -> list[ReplicaSlot]:
        """Return any slots whose process died while state was RUNNING.

        Also flips their state to CRASHED so the caller can decide
        whether to abort or treat it as a data point. Deliberately-
        stopped (STOPPED / KILLED) slots are never reported here — the
        phase registry asked for their exit.
        """
        crashed: list[ReplicaSlot] = []
        for slot in self._slots:
            if slot.state is not ReplicaState.RUNNING:
                continue
            if slot.process is not None and slot.process.poll() is not None:
                slot.state = ReplicaState.CRASHED
                crashed.append(slot)
        return crashed

    # ── Internals ──────────────────────────────────────────────────────

    def _check_id(self, instance_id: int) -> None:
        if instance_id < 0 or instance_id >= self.capacity:
            raise IndexError(
                f"{self.system} replica {instance_id} out of range "
                f"(capacity={self.capacity})"
            )

    def _terminate_slot(
        self,
        slot: ReplicaSlot,
        *,
        signal_type: int,
        timeout_s: float,
    ) -> None:
        proc = slot.process
        if proc is not None and proc.poll() is None:
            try:
                proc.send_signal(signal_type)
            except ProcessLookupError:
                # Already gone between our poll() check and the signal —
                # not a fault, just racy teardown.
                pass
            try:
                proc.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                # Escalate if graceful window expired.
                print(
                    f"[{self.system}] replica {slot.instance_id} did not exit "
                    f"within {timeout_s}s on {signal_type}; escalating to SIGKILL",
                    file=sys.stderr,
                )
                proc.kill()
                proc.wait()
        if slot.stop_event is not None:
            slot.stop_event.set()
        if slot.tailer is not None:
            slot.tailer.join(timeout=2.0)
        # Clear the handle so subsequent checks don't see a stale pid.
        slot.process = None
        slot.tailer = None
        slot.stop_event = None

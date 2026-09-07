# Python wheel exit crash: diagnosis and regression

The 0.6.2 wheel crashes **after** lifecycle PASS, independently of the schema or
job execution path. Its Rust completion callback can still own Python objects
when the asyncio future is already done. CPython 3.12.3 begins finalization on
the main thread, force-exits the completion thread when it tries to reacquire
the GIL, and unwinds Rust destructors without a Python thread state:

```
main:   Py_FinalizeEx -> finalize_modules -> GC
worker: PyEval_RestoreThread -> take_gil -> PyThread_exit_thread
        [forced unwind] -> pyo3_async_runtimes::generic::set_result
        -> PyObject_GC_Del -> _PyInterpreterState_GET -> SIGSEGV
```

The CI core came from [run 34064725352, job 101571373986](https://github.com/hardbyte/awa/actions/runs/34064725352/job/101571373986).
It failed on the first 0.6.2 iteration, after the 0.6.6 iteration passed.
`released-wheel.json` records CPython 3.12.3, Ubuntu 24.04/glibc 2.39, and the
native extension SHA256. The exact same wheel hash was verified in the Ubuntu
container used to symbolize the core. GDB reported a libz build-ID mismatch;
the CPython, libc, and Awa frames determining this diagnosis resolved. The raw
core is retained in the workflow artifact, not in git. GDB was absent from the
original runner, so this PR also installs it and preserves the signal exit
code if backtrace collection itself fails.

## Reproduction

`scripts/compat/shutdown_probe.py` only constructs a client (opening its pool)
and awaits `close()`. It needs a reachable Postgres but performs no migrations
or job processing. With the published 0.6.2 wheel on Ubuntu's CPython 3.12.3:

- 100 full lifecycle replays under host GDB passed.
- 100 full lifecycle replays under Ubuntu GDB passed.
- 1,000 shorter lifecycle replays under Ubuntu GDB passed.
- The untraced shorter lifecycle crashed at iteration 1,828 (zero-based).
- The untraced close-only probe crashed at iteration 325 (zero-based).

These passes did not disprove the race: GDB perturbs its timing.

A controlled schedule makes the same native stack deterministic. In an Ubuntu
24.04 container with two CPUs, GDB and `python3-dbg`, run the 0.6.2 probe with
`--delay-wakeup`. Break at `finalize_modules`, enable `set scheduler-locking on`,
select the Rust completion thread sleeping in `clock_nanosleep` (thread 4 in the
recorded two-CPU run), break at `PyThread_exit_thread`, and continue. The first
backtrace proves CPython's forced thread exit; continue again to capture the
SIGSEGV. `cpython-312-controlled.txt` records both stops. The artificial delay
widens the existing `call_soon_threadsafe` wakeup/GIL-reacquisition window.

With the identical wheel on CPython **3.13.12**, the same schedule stops in
`PyThread_hang_thread` instead. Releasing scheduler locking lets the main thread
exit normally. See `cpython-313-controlled.txt`; 1,000 untraced close-only
iterations also passed. This matches CPython's fix for
[python/cpython#87135](https://github.com/python/cpython/issues/87135): parking a
late thread avoids the unsafe forced unwind. It does not join the native
completion callback, so changing interpreters alone is not Awa's library fix.

## Library fix and durable check

`awa-python/src/async_bridge.rs` adapts the upstream generic runtime, counting
native async tasks and Python-facing completion callbacks **before** they enter
Tokio. An atexit hook fences new work, cancels outstanding async tasks, and waits
with the GIL released until every accepted task and callback is dropped. Task
futures release their owned Python handles before reporting completion. This
also covers argument conversion and result construction inside async bodies,
which can acquire the GIL before the completion callback exists.
All Awa Rust-to-Python futures use this adapter. Workers and pools still require
their ordinary explicit shutdown/close lifecycle.

`tests/test_interpreter_shutdown.py` holds the completion callback after it has
woken asyncio, then checks atexit ordering. The released wheel prints
`EARLY_EXIT`; the patched wheel passes on the same CPython 3.12.3 environment.
A second subprocess checks the shutdown fence and idempotence. A third holds
argument conversion inside an abandoned native insert until atexit; it prints
`EARLY_EXIT` with completion-only tracking, proving async bodies need joining
too. These checks exercise
the boundary directly without relying on a rare process crash. CI builds and
installs an actual wheel on 3.12.3 for this regression, alongside the normal
full Python 3.13/3.14 suites.

Pinned historical-wheel schema checks now explicitly use CPython 3.13.12,
whose thread-finalization behavior is verified above. Their wheel bytes remain
unchanged; every process must exit successfully. `COMPAT_PYTHON` can explicitly
select 3.12.3 for diagnosis, and `COMPAT_REPETITIONS` controls stress repetitions.
Do not remove the native completion regression or claim older published wheels
contain this fix.

## Stalled shutdown boundary

Cancellation releases async socket/database waits; a regression holds a query
behind a PostgreSQL advisory lock and retains the blocking transaction through
atexit, proving the native waiter is cancelled rather than joined indefinitely.
The migration helper uses an untracked Tokio blocking task containing Rust/SQLx
work. Cancelling its tracked waiter detaches that task; it does not join it at
interpreter exit or permit it to attach to Python later.

Synchronous Python code inside a task or completion callback cannot be safely
aborted. After five seconds the bridge writes a native stderr diagnostic with
the outstanding task/callback counts and continues waiting. Returning from the
hook on timeout would reintroduce access to a finalizing interpreter; forcibly
exiting the process is not a library cleanup policy. A controlled regression
holds a completion callback until the parent observes the warning, verifies the
child remains alive, then releases it and verifies safe finalization.

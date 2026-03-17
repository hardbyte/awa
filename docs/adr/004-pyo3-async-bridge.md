# ADR-004: PyO3 for Python Integration Instead of Subprocess/gRPC/REST

## Status

Accepted

## Context

Awa provides first-class Python workers -- not just insert-only clients (like River's Python client) but full job execution with the same reliability guarantees as Rust workers. This requires calling Python handler functions from the Rust runtime, managing the async lifecycle, and ensuring heartbeats continue during Python execution.

Several integration approaches were evaluated:

1. **Subprocess:** Spawn a Python process per job. Communication via stdin/stdout or files. Simple but heavyweight -- process startup cost dominates for short jobs, and heartbeating requires an additional IPC channel.
2. **gRPC/REST:** Run a Python worker as a separate service. The Rust runtime dispatches jobs via RPC. Adds a network hop, requires a separate deployment, and complicates transactional enqueue.
3. **WASM:** Compile Python to WebAssembly. Not mature enough for real workloads -- most Python libraries (especially anything with C extensions) don't compile to WASM.
4. **PyO3 (in-process):** Embed CPython in the Rust process via PyO3. Python handlers run as callbacks in the same process, with zero-copy data passing and direct access to the Rust runtime's heartbeat and cancellation machinery.

## Decision

Use PyO3 with `pyo3-async-runtimes` for in-process Python integration.

### Phase 0 Spike Results

A proof-of-concept spike (`spike/pyo3-async/`) validated four critical requirements before committing to this approach:

1. **Rust tokio can call a Python `async def` and await its result.**
   `pyo3_async_runtimes::tokio::future_into_py` converts Rust futures to Python awaitables. `pyo3_async_runtimes::tokio::into_future` converts Python coroutines to Rust futures. Round-trip latency is sub-microsecond.

2. **A Rust background task can heartbeat while a Python handler runs.**
   The heartbeat task runs as a separate tokio task that never acquires the GIL. The spike proved that heartbeat increments continue accumulating while a Python handler is blocked in `asyncio.sleep`.

3. **Python exceptions propagate to Rust as structured errors.**
   PyO3 converts Python exceptions to `PyErr`, which carries the exception type, message, and traceback. The spike confirmed that type name, message, and traceback are all accessible from Rust.

4. **`ctx.is_cancelled()` works from Python when Rust signals shutdown.**
   A shared `AtomicBool` (wrapped in a `#[pyclass]`) is checked by Python and set by Rust. The spike confirmed that cancellation signalled from a Rust background task is visible to the Python handler within milliseconds.

### How Heartbeats Survive GIL Blocks

This is the key architectural insight that makes PyO3 viable for job processing:

```
┌─────────────────────────────────────────────────┐
│ Rust Process                                     │
│                                                  │
│  tokio runtime                                   │
│  ├── Dispatcher task (claims jobs)               │
│  ├── HeartbeatService task ◄── never touches GIL │
│  ├── Maintenance task                            │
│  └── Job execution task                          │
│       └── Acquires GIL → calls Python handler    │
│           └── Python handler runs                │
│               └── GIL held here                  │
│                                                  │
│  HeartbeatService runs on tokio, not on GIL.     │
│  Even if Python blocks the GIL, heartbeats       │
│  continue because they only do:                  │
│    UPDATE awa.jobs SET heartbeat_at = now()       │
│    WHERE id = ANY($1)                            │
│  ...which is pure Rust + sqlx, no GIL needed.    │
└─────────────────────────────────────────────────┘
```

The `HeartbeatService` reads the in-flight job ID set (a Rust `HashSet<i64>` behind `RwLock`) and writes to Postgres via `sqlx`. Neither operation requires the GIL. Even if a Python handler does CPU-bound work that holds the GIL for seconds, heartbeats are unaffected.

The one scenario where heartbeats would fail is if the entire tokio runtime is blocked (e.g., a Rust-side bug that blocks the executor). The deadline recovery mechanism (ADR-003) covers this case.

### Architecture of the Python Bridge

The `awa-python` crate (`awa-python/src/`) exposes:

- `PyClient`: Connection pool management, job insertion, worker registration, admin operations.
- `PyTransaction`: Async context manager wrapping a Postgres transaction for atomic enqueue.
- `PyJob`: Read-only view of a job row with Python-native getters (args as dict, timestamps as ISO strings).
- `PyJobState`: Enum matching Rust's `JobState`.
- `PyRetryAfter`, `PySnooze`, `PyCancel`: Return types for worker handlers.

Worker registration uses a decorator pattern:

```python
@client.worker(SendEmail, queue="email")
async def handle(job):
    # job.args is a dict
    send_email(job.args["to"], job.args["subject"])
```

The decorator stores the handler and args type in a `HashMap<String, WorkerEntry>`. `awa-python` then builds a normal `awa-worker::Client` and registers a thin adapter `Worker` per Python kind. When the Rust runtime dispatches a job, that adapter looks up the Python handler by kind and calls it via `pyo3_async_runtimes::tokio::into_future`.

### Type Bridging

Python args objects (dataclasses, pydantic BaseModels, or plain dicts) are serialized to `serde_json::Value` at the boundary:

- **Pydantic:** `obj.model_dump(mode="json")` -> Python dict -> `py_to_json()` -> `serde_json::Value`
- **Dataclass:** `dataclasses.asdict(obj)` -> Python dict -> `py_to_json()` -> `serde_json::Value`
- **Dict:** Direct `py_to_json()` conversion

The same CamelCase-to-snake_case kind derivation runs in the Python bridge (`awa-python/src/args.rs`), matching the Rust proc-macro (`awa-macros`) and library (`awa-model/src/kind.rs`) implementations character-for-character. Golden tests verify cross-language consistency (PRD section 9.2).

## Consequences

### Positive

- **Zero-copy data path:** Job args pass from Postgres (sqlx) through Rust to Python without serialization round-trips to a subprocess or network.
- **Shared heartbeat and crash recovery:** Python jobs benefit from the exact same dispatcher, LISTEN/NOTIFY wakeup, heartbeat, deadline, and maintenance mechanisms as Rust jobs, with no duplicate queue loop.
- **Single deployment artifact:** One process runs both Rust and Python workers. No sidecar, no RPC service, no process manager.
- **Transactional enqueue from Python:** `PyTransaction` wraps a real Postgres transaction, enabling atomic enqueue from Python with the same guarantees as Rust.
- **Sub-millisecond dispatch overhead:** PyO3 function call overhead is measured in microseconds, negligible compared to I/O-bound job handlers.

### Negative

- **CPython version coupling:** The `awa-python` binary is compiled against a specific CPython version (currently 3.12). Different Python versions require separate builds.
- **Single GIL:** Only one Python thread can execute at a time. CPU-bound Python handlers limit parallelism. Mitigation: use `asyncio.to_thread()` for CPU-bound work, or run CPU-heavy jobs in Rust.
- **Debugging complexity:** Stack traces span Rust and Python. PyO3 exceptions carry traceback information, but mixed-language debugging is inherently harder than single-language.
- **Build complexity:** `awa-python` requires maturin, a Python virtual environment, and a compatible CPython installation in the build environment. It is excluded from the main Cargo workspace for this reason.

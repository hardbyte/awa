# Benchmarking Notes

This document captures the current local benchmark setup and a few reference
results from local runs.

## Test Environment

- Machine: Apple M5 MacBook Air
- Runtime: local PostgreSQL 17 in OrbStack
- Database URL used for local tests:
  `postgres://postgres:test@localhost:15432/awa_test`
- Benchmarks live in:
  `awa/tests/benchmark_test.rs`
  `awa/tests/scheduling_benchmark_test.rs`
  `awa/tests/failure_benchmark_test.rs`
- Python worker benchmarks live in:
  `awa-python/scripts/benchmark_runtime.py`
- Shared output schema:
  `awa/tests/bench_output.rs` (Rust)
  `awa-python/scripts/bench_output.py` (Python)

These are local engineering benchmarks, not published vendor-style numbers. The
main goal is to compare shapes, validate architecture changes, and catch
regressions.

## Methodology Notes

The most important lesson from this round of work is that benchmark isolation
matters.

The maintenance service is global to the worker instance. If a benchmark leaves
behind millions of deferred rows in `awa.scheduled_jobs`, later "hot path"
benchmarks may accidentally measure background promotion work as well. The
sustained hot/deferred benchmarks now reset runtime state first to avoid that
pollution.

Two benchmark shapes are used:

- **Burst/frontier benchmarks**: seed a backlog, then drain it
- **Steady/sustained benchmarks**: warm up, then measure a fixed time window

Steady numbers are the better indicator of sustained runtime behavior.

## Python Runtime Benchmarks

The Python benchmark script exercises the real `awa-python` worker path while
reusing the same database-facing benchmark shapes as the Rust runtime:

**Baseline scenarios** (`--scenario baseline`):

- `copy`: Python client `insert_many_copy` throughput
- `hot`: sustained worker throughput over pre-seeded `awa.jobs_hot`
- `scheduled`: sustained deferred promotion over pre-seeded `awa.scheduled_jobs`

**Failure scenarios** (`--scenario failures`):

- `terminal_1pct` / `10pct` / `50pct`: terminal failures
- `retryable_1pct` / `10pct` / `50pct`: retry-once failures
- `callback_timeout_10pct`: callback registration with short timeout
- `mixed_50pct`: rotating through terminal, retryable, and success modes

The worker-focused scenarios seed with SQL directly so the reported number is
about Python handler dispatch and runtime behavior, not enqueue serialization.

## Current Headline Results

### Sustained Hot Path

Measured with `test_runtime_sustained_hot_path` after resetting runtime state:

- warmup: 2s
- measurement window: 10s
- queue size seeded: 200,000 immediately-available jobs

Latest observed result:

- handler returns: about `8.1k jobs/s`
- DB `completed` transitions: about `8.1k jobs/s`

This benchmark always enables the in-memory OpenTelemetry exporter so the
runtime metrics path is exercised while measuring.

### Python Runtime Baseline

Measured with `awa-python/scripts/benchmark_runtime.py` on the same local
database:

- `insert_many_copy`: about `15.6k jobs/s` (`50,000` jobs in `3.20s`)
- sustained hot path:
  - handler returns: about `4.9k jobs/s`
  - DB `completed` transitions: about `4.8k jobs/s`
- sustained deferred frontier, `2,000,000` deferred rows with `4,000/s` due:
  - all `40,000` due jobs were eventually picked and completed
  - about `36.6k` completed within the 10-second measurement window
  - pickup lateness:
    - `p50`: about `243 ms`
    - `p95`: about `700 ms`
    - `p99`: about `794 ms`

These runs isolate the Python worker path. Seed data is inserted with SQL so
the runtime number is not dominated by Python-side enqueue serialization.

### Large Deferred Frontier

Measured with `test_scheduled_steady_10m_due_1k_per_sec`:

- total deferred backlog: `10,000,000` rows
- due rate target: `1,000` jobs/s
- measurement window: 10s

Isolated 4-thread Tokio runtime result:

- all `10,000` due jobs were picked and completed
- pickup lateness:
  - `p50`: `0 ms`
  - `p95`: about `489 ms`
  - `p99`: about `787 ms`

This is good enough to show that the hot/deferred split plus indexed promotion
can handle a very large deferred frontier locally, but the release pattern is
still burstier than ideal.

### Moderate Deferred Frontier — Higher Due Rate

Measured with `test_scheduled_steady_2m_due_4k_per_sec`:

- total deferred backlog: `2,000,000` rows
- due rate target: `4,000` jobs/s
- measurement window: 10s

Result:

- all `40,000` due jobs were picked
- `37,696` completed within the 10-second window
- pickup lateness:
  - `p50`: about `204 ms`
  - `p95`: about `501 ms`
  - `p99`: about `589 ms`
- promotion batches averaged `196` jobs at `54 ms` per batch
- claim latency: `4.8 ms` mean

This validates the architecture at a realistic production scale: 2M deferred
rows with 4k/s throughput and sub-second tail pickup latency.

### Scaling Limit: 10M Deferred at 6k/s

Measured with `test_scheduled_steady_10m_due_6k_per_sec`:

- total deferred backlog: `10,000,000` rows
- due rate target: `6,000` jobs/s
- measurement window: 10s

Result:

- only `~20k–57k` of the `60,000` target jobs promoted in the window
- promotion batches: `10–21` batches at `1.7–3.6s` mean (`5–8s` max)
- pickup lateness: `p50 ~12s`, `p99 ~15–21s`
- claim and completion latency remained healthy (`11–20 ms` mean)

The bottleneck is promotion, not dispatch or completion. The promotion query
(`DELETE FROM scheduled_jobs` + `INSERT INTO jobs_hot` in a single CTE) runs in
`~50 ms` in isolation but degrades to multi-second latency under concurrent load.

Investigation showed this is caused by WAL/IO pressure from operating on the
2.5 GB `scheduled_jobs` table (heap + 566 MB partial index) under concurrent
dispatch and completion activity. Increasing `shared_buffers` from `128 MB` to
`2 GB` did not help — the bottleneck is write-path contention, not cache misses.

**Key tuning parameters for promotion throughput:**

- `PROMOTE_BATCH_SIZE` (default `4,096`): rows per promotion batch
- `PROMOTE_MAX_BATCHES_PER_TICK` (default `32`): max batches per maintenance tick
- `promote_interval` (default `250 ms`): how often promotion runs
- `COMPLETION_FLUSH_INTERVAL` (default `1 ms`): completion batcher flush interval

The current architecture handles 2M deferred / 4k/s comfortably. For 10M+ at
higher due rates, promotion would need to be parallelized (e.g., by queue or
ID range) or the deferred table partitioned.

### Progress Feature Overhead

The structured progress feature (ADR-014) adds a `progress JSONB` column
and a two-tier heartbeat flush. Performance impact was validated:

- **Zero overhead when no progress is set.** The heartbeat service
  partitions jobs by pending progress — jobs without mutations use the
  original heartbeat-only query. `snapshot_pending_progress` returns empty
  when no generation has been bumped.
- **Completion batcher** adds `progress = NULL` to the batch UPDATE. This
  is a constant-time write to a nullable column with no measurable impact
  (batcher throughput remains ~78k/s in unit benchmarks).
- **Sustained hot-path throughput** unchanged at ~8.1k/s after the feature
  was added.

## Failure-Mode Benchmarks

The failure-mode benchmark suite measures throughput, drain time, and recovery
behaviour when a configurable percentage of jobs fail, retry, hang, or trigger
rescue paths. This answers a question the happy-path benchmarks cannot: **how
does failure impact healthy-job throughput?**

### Benchmark matrix

| Scenario | Description |
|----------|-------------|
| `terminal_1pct` / `10pct` / `50pct` | N% of jobs fail terminally |
| `retryable_1pct` / `10pct` / `50pct` | N% of jobs fail once then succeed on retry |
| `callback_timeout_10pct` | 10% register a callback that times out, then succeed on retry |
| `deadline_hang_10pct` | 10% hang until deadline rescue fires, then succeed on retry |
| `snooze_once_10pct` | 10% snooze once, then succeed |
| `mixed_all_modes` | 50% success, 10% each of terminal/retryable/callback/deadline/snooze |
| `stale_heartbeat_rescue` | All jobs seeded as "running" with stale heartbeat — measures rescue-to-completion time |

### Rust harness

Tests live in `awa/tests/failure_benchmark_test.rs`. Each scenario seeds jobs
deterministically by mode, starts a Client with aggressive rescue intervals,
drains to terminal states, and emits both human-readable output and a JSONL
record. The full matrix command covers the 10 failure scenarios; the
stale-heartbeat rescue benchmark is a separate test.

### Python harness

The Python benchmark (`awa-python/scripts/benchmark_runtime.py`) supports a
failure-mode subset via `--scenario failures`:

- `terminal_1pct` / `10pct` / `50pct`
- `retryable_1pct` / `10pct` / `50pct`
- `callback_timeout_10pct`
- `mixed_50pct`

It does not currently include the Rust-only `deadline_hang`, `snooze_once`, or
`stale_heartbeat_rescue` scenarios. The worker returns `RetryAfter`,
`WaitForCallback`, `Cancel`, or raises exceptions based on the job's `mode`
field.

### Structured output

Both Rust and Python benchmarks emit one JSONL record per scenario, prefixed
with `@@BENCH_JSON@@` for extraction. Schema version 2:

```json
{
  "schema_version": 2,
  "scenario": "terminal_10pct",
  "language": "rust",
  "seeded": 5000,
  "metrics": {
    "throughput": {
      "handler_per_s": 4200.0,
      "db_finalized_per_s": 4100.0
    },
    "drain_time_s": 1.22,
    "rescue": {
      "deadline_rescued": 0,
      "callback_timeouts": 0
    }
  },
  "outcomes": {
    "completed": 4500,
    "failed": 500
  }
}
```

Extract JSONL from mixed stdout: `grep '@@BENCH_JSON@@' output.txt | sed 's/^@@BENCH_JSON@@//'`

For enqueue-only benchmarks (`insert_only_single`, `copy_single`, and the
contention matrix), `metrics.enqueue_per_s` is emitted instead of
`metrics.throughput`. Those records still include `"measurement": "enqueue"` in
`metadata`, plus optional Postgres-side deltas such as `wal_bytes`,
`temp_bytes_delta`, and `xact_commit_delta`.

## Interpreting The Results

Some practical guidelines:

- Compare like with like. Burst/frontier benchmarks and steady-state benchmarks
  answer different questions.
- Reset runtime state before sustained measurements if you want to isolate one
  path. Global background work can distort results.
- Prefer the `db_completed_delta` view when you care about end-to-end queue
  completion, not just handler return rate.
- Treat the numbers here as machine-local reference points, not portable
  guarantees.

## How To Run

### Happy-path benchmarks

```bash
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test scheduling_benchmark_test \
  test_runtime_sustained_hot_path -- --exact --ignored --nocapture

DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test scheduling_benchmark_test \
  test_scheduled_steady_2m_due_4k_per_sec -- --exact --ignored --nocapture
```

### Enqueue contention benchmarks

These are the most useful benchmarks when you want to compare single-producer
enqueue against multi-producer contention, or compare chunked `INSERT` with the
COPY staging path under concurrent writers.

```bash
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_BENCH_CONTENTION_PRODUCERS=4 \
  AWA_BENCH_CONTENTION_JOBS_PER_PRODUCER=3000 \
  AWA_BENCH_INSERT_BATCH_SIZE=1000 \
  AWA_BENCH_COPY_CHUNK_SIZE=1000 \
  cargo test --package awa --test benchmark_test \
  test_enqueue_contention_matrix -- --exact --ignored --nocapture
```

This emits six JSONL records:

- `insert_single`
- `copy_single`
- `insert_contention_distinct`
- `copy_contention_distinct`
- `insert_contention_same_queue`
- `copy_contention_same_queue`

The matrix hard-resets Awa runtime tables before each scenario so later cases
do not inherit a larger or dirtier jobs table from earlier ones.

By default, `AWA_BENCH_COPY_CHUNK_SIZE` should match
`AWA_BENCH_INSERT_BATCH_SIZE` if you want the closest apples-to-apples
comparison between chunked `INSERT` and chunked COPY staging. If you want to
test the current "one bulk COPY per producer" shape instead, set
`AWA_BENCH_COPY_CHUNK_SIZE` to `AWA_BENCH_CONTENTION_JOBS_PER_PRODUCER`.

The optional Postgres profile block in `metadata.db_profile` is meant to make
server runs easier to interpret. In particular:

- `wal_bytes` shows how much WAL the scenario generated
- `temp_bytes_delta` and `temp_files_delta` show temp-file pressure
- `xact_commit_delta` helps explain why many small commits degrade throughput
- `tup_inserted_delta` shows how much table churn the database observed

### Failure-mode benchmarks (Rust)

```bash
# Full matrix (10 failure scenarios)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test failure_benchmark_test \
  test_failure_bench_full_matrix -- --exact --ignored --nocapture

# Single scenario
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test failure_benchmark_test \
  test_failure_bench_terminal_10pct -- --exact --ignored --nocapture

# Stale heartbeat rescue
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test failure_benchmark_test \
  test_failure_bench_stale_heartbeat_rescue -- --exact --ignored --nocapture
```

### Python benchmarks

```bash
cd awa-python
uv run maturin develop

# Baseline scenarios (copy, hot, scheduled)
PYTHONPATH=scripts DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  uv run python scripts/benchmark_runtime.py --scenario baseline

# Failure scenarios
PYTHONPATH=scripts DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  uv run python scripts/benchmark_runtime.py --scenario failures

# Everything
PYTHONPATH=scripts DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  uv run python scripts/benchmark_runtime.py --scenario all
```

## Caveats

- These numbers are from one local machine and one local Postgres setup.
- Ignored benchmark tests are not part of the normal unit/integration test pass.
- The current focus is relative behavior and architectural validation, not
  cross-machine leaderboard comparisons.

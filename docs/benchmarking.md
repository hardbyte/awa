# Benchmarking Notes

This document captures the benchmark harnesses used in the repo and a few
reference results from local runs and dedicated-server enqueue comparisons.

## Test Environment

- Local machine: Apple M5 MacBook Air (16 GB)
- Local runtime: PostgreSQL 17 in Docker (OrbStack)
- Local database URL used for the example commands:
  `postgres://postgres:test@localhost:15432/awa_test`
- The portable harness in `benchmarks/portable/` defaults to PostgreSQL 17 but
  accepts `--pg-image postgres:18-alpine` for PG18 runs.
- Dedicated-server enqueue comparisons were also run against PostgreSQL 17 on
  a separate Linux host. Those numbers are used only for shape comparison and
  should not be treated as published throughput claims.
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
sustained hot/deferred benchmarks reset runtime state first to avoid that
pollution.

Two benchmark shapes are used:

- **Burst/frontier benchmarks**: seed a backlog, then drain it
- **Steady/sustained benchmarks**: warm up, then measure a fixed time window

Steady numbers are the better indicator of sustained runtime behavior.

## MVCC Horizon Benchmark

`awa/tests/scheduling_benchmark_test.rs` also includes an ignored
`test_mvcc_horizon_overlap_benchmark` scenario for the failure mode described in
PlanetScale's "Keeping a Postgres queue healthy" post: overlapping long-lived
transactions pin the MVCC horizon while queue churn continues.

The benchmark:

- runs a steady producer feeding `awa.jobs_hot`
- enables aggressive completed-job cleanup to generate update/delete churn
- starts overlapping `REPEATABLE READ` reader transactions on separate
  connections to hold old snapshots open
- samples per-second throughput, queue depth, and `pg_stat_user_tables`
  (`n_dead_tup`, vacuum counters) for `awa.jobs_hot`

Example:

```bash
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --release --package awa --test scheduling_benchmark_test \
  test_mvcc_horizon_overlap_benchmark -- --ignored --exact --nocapture
```

This scenario is also wired into `.github/workflows/nightly-chaos.yml` and runs
on PostgreSQL 18 in the Rust nightly benchmark lane.

The nightly lane uses a shorter CI profile so the benchmark stays cheap on
shared runners while still exercising overlap readers and cleanup pressure:

- `AWA_MVCC_JOB_RATE=400`
- `AWA_MVCC_BASELINE_SECS=5`
- `AWA_MVCC_OVERLAP_SECS=12`
- `AWA_MVCC_COOLDOWN_SECS=5`
- `AWA_MVCC_OVERLAP_READERS=2`
- `AWA_MVCC_OVERLAP_HOLD_SECS=12`
- `AWA_MVCC_OVERLAP_STAGGER_SECS=4`

Nightly regression checks use per-run ratios (`overlap_handler_per_s` and
`cooldown_handler_per_s` relative to the same run's baseline window) plus
guardrails on `dead_tup_delta` and `max_available`.

Useful knobs:

- `AWA_MVCC_JOB_RATE` — steady producer rate in jobs/sec
- `AWA_MVCC_BASELINE_SECS` / `AWA_MVCC_OVERLAP_SECS` / `AWA_MVCC_COOLDOWN_SECS`
- `AWA_MVCC_OVERLAP_READERS` / `AWA_MVCC_OVERLAP_HOLD_SECS` /
  `AWA_MVCC_OVERLAP_STAGGER_SECS`
- `AWA_MVCC_CLEANUP_INTERVAL_MS` / `AWA_MVCC_CLEANUP_BATCH_SIZE`

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

## Reference Results

### Immediate Enqueue Throughput

The enqueue path that is being measured here has a few important architectural
properties:

- homogeneous inserts now route directly to `awa.jobs_hot` or
  `awa.scheduled_jobs` instead of going through the compatibility view
- admin metadata maintenance moved from row-level triggers to statement-level
  trigger batches (`v005`)
- COPY staging now reuses a session-local temp table and stages typed values
  instead of reparsing text on the final `INSERT ... SELECT`

Example reference results from one local laptop run and one dedicated server
run:

- local laptop (`Apple M5`, release build, v0.5.0-alpha.0):
  - `insert_only_single`: about `30k inserts/s`
  - `copy_single`: about `43k inserts/s`
  - `insert_contention_distinct` (4 producers x 3k): about `46k inserts/s`
  - `copy_contention_distinct` (4 producers x 3k, chunk 1000): about `100k inserts/s`
  - `insert_contention_same_queue` (4 producers x 3k): about `95k inserts/s`
  - `copy_contention_same_queue` (4 producers x 3k, chunk 1000): about `67k inserts/s`

These are engineering comparisons, not product guarantees. Their main value is
showing where the architecture bottlenecks move as the implementation changes.

### Sustained Hot Path

Measured with `test_runtime_sustained_hot_path` after resetting runtime state:

- warmup: 2s
- measurement window: 10s
- queue size seeded: 200,000 immediately-available jobs

Example reference result from one local run (release mode, v0.5.0-alpha.0):

- handler returns: about `5.6k jobs/s`
- DB `completed` transitions: about `5.6k jobs/s`

This benchmark enables the in-memory OpenTelemetry exporter and the
production alerting metrics path (queue depth, lag, wait-duration histogram).
Back-to-back A/B testing shows v0.5.0 is ~44% faster than v0.4.1 on the
same hardware (5.9k vs 4.1k/s) — the promotion query optimizations more
than offset the metrics instrumentation cost.

### Python Runtime Baseline

Measured with `awa-python/scripts/benchmark_runtime.py` on the same local
database:

- `insert_many_copy`: about `16.2k jobs/s` (`50,000` jobs in `3.09s`)
- sustained hot path:
  - handler returns: about `3.2k jobs/s`
  - DB `completed` transitions: about `3.1k jobs/s`

These runs isolate the Python worker path. Seed data is inserted with SQL so
the runtime number is not dominated by Python-side enqueue serialization.

### Large Deferred Frontier

Measured with `test_scheduled_steady_10m_due_1k_per_sec`:

- total deferred backlog: `10,000,000` rows
- due rate target: `1,000` jobs/s
- measurement window: 10s

Isolated 4-thread Tokio runtime result (release mode, v0.5.0-alpha.0):

- `9,000` of `10,000` due jobs completed within the window
- per-second completions: `0, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000`
  — perfectly steady after the first-tick startup delay
- pickup lateness:
  - `p50`: `229 ms`
  - `p95`: `332 ms`
  - `p99`: `343 ms`
- promotion: `354` batches, mean `4.3 ms`, max `59 ms`
- claim latency: mean `5.0 ms`

This demonstrates that the hot/deferred split with literal-state promotion
queries handles a 10M-row deferred frontier with steady, predictable throughput.

**Key optimization (v0.5.0):** Promotion queries use literal state values
(e.g., `WHERE state = 'scheduled'`) instead of parameterized (`WHERE state = $1`).
This allows the Postgres planner to match the partial index
`idx_awa_scheduled_jobs_run_at_scheduled` at plan time. With a parameterized
query, the planner falls back to a full bitmap scan on multi-million-row tables,
degrading promotion from ~4ms to ~400ms per batch (100x slower).

### Moderate Deferred Frontier — Higher Due Rate

Measured with `test_scheduled_steady_2m_due_4k_per_sec`:

- total deferred backlog: `2,000,000` rows
- due rate target: `4,000` jobs/s
- measurement window: 10s

Result (v0.5.0-alpha.0):

- all `40,000` due jobs were picked and completed
- pickup lateness: `p50`: `0 ms`, `p95`: `0 ms`, `p99`: `57 ms`
- promotion: `214` batches, mean `10.0 ms`, max `176 ms`
- claim latency: mean `12.4 ms`

This validates the architecture at a realistic production scale: 2M deferred
rows with 4k/s throughput and reliable promotion.

### High-Rate Deferred Frontier: 10M at 6k/s

Measured with `test_scheduled_steady_10m_due_6k_per_sec`:

- total deferred backlog: `10,000,000` rows
- due rate target: `6,000` jobs/s
- measurement window: 10s

Result:

- `58,686` of `60,000` target jobs completed within the window (98%)
- per-second completions: `2942, 6834, 6907, 4758, 5915, 5662, 7246, 6446, 5305, 6671`
- pickup lateness: `p50`: `0 ms`, `p95`: `310 ms`, `p99`: `476 ms`
- promotion: `242` batches, mean `6.4 ms`, max `99 ms`

This rate was previously a documented scaling limit (only 20-57k of 60k
promoted, promotion at 1.7-3.6s per batch). The literal-state promotion
fix (v0.5.0) eliminated the bottleneck entirely.

**Key tuning parameters for promotion throughput:**

- `PROMOTE_BATCH_SIZE` (default `4,096`): rows per promotion batch
- `PROMOTE_MAX_BATCHES_PER_TICK` (default `32`): max batches per maintenance tick
- `promote_interval` (default `250 ms`): how often promotion runs
- `COMPLETION_FLUSH_INTERVAL` (default `1 ms`): completion batcher flush interval

### Concurrent Multi-Queue Lifecycle

Measured with `concurrent_lifecycle_test.rs`. Four queues (`email:32`,
`payments:16`, `analytics:64`, `webhooks:16` workers), 128 total workers,
full insert → claim → execute → complete lifecycle.

Queue-count sweep with 128 total workers, pool=50, 20k jobs (release mode):

| Config | Workers/queue | Throughput | Per-worker |
|--------|--------------|------------|------------|
| 1 queue × 128 | 128 | `~1.9k/s` | `~14/s` |
| 2 queues × 64 | 64 | `~2.0k/s` | `~15/s` |
| 4 queues × 32 | 32 | `~350/s` | `~2.7/s` |

**Key finding**: 1-queue and 2-queue throughput is essentially identical,
but 4 queues drops 5-6x. The cliff between 2 and 4 queues indicates
that the per-queue dispatcher overhead (separate claim query, PgListener,
semaphore) compounds non-linearly when many small queues share a pool.

For comparison, the hot-path benchmark (200k pre-seeded into `jobs_hot`
via SQL) reaches `~10k/s` because it bypasses insert triggers and has a
fully warmed dispatch pipeline. The lifecycle benchmarks exercise the
complete path including job-state triggers and notification.

**Tuning guidelines**:

- Size the connection pool to at least `num_queues * 4 + 20`. With
  4 queues, that's 36+ connections.
- Prefer fewer queues with larger worker pools over many small queues.
  2 queues × 64 workers performs the same as 1 × 128.
- Use multi-queue for isolation, priority, or rate limiting — not for
  throughput.

A unified cross-queue claim query (one SQL round-trip claiming across all
queues via `LATERAL JOIN`) could eliminate the per-queue dispatch overhead.
Prototyping shows 16ms for 48 jobs across 2 queues — comparable to
single-queue performance. This is tracked as a future optimization.

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
- **Sustained hot-path throughput** was unchanged when the progress feature
  was added. Current hot-path throughput (~5.6k/s) reflects the additional
  v0.5.0 OTel metrics instrumentation, not progress overhead.

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

It does not yet include the Rust-only `deadline_hang`, `snooze_once`, or
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

## Cross-System Comparison (Portable Benchmarks)

The `benchmarks/portable/` harness runs comparable scenarios against Awa
(native Rust), `awa-docker`, `awa-python`, River (Go), and Oban (Elixir)
sharing the same Postgres instance. See `benchmarks/portable/README.md` for
setup and usage.

A recent fair comparison used:

- 5 isolated repetitions per system
- 10,000 benchmark jobs
- 50 workers
- 50 pickup-latency iterations
- portable chaos suite with 10 long-running jobs
- systems included: `awa`, `awa-docker`, `awa-python`, `river`, `oban`

`procrastinate` was excluded from the fair 5x comparison because its portable
chaos adapter/schema path was not yet trustworthy enough to compare fairly.

### Benchmark Summary (5 isolated repetitions)

#### Enqueue Throughput (jobs/sec)

| System | Mean | Stdev | Min | Max |
|--------|-----:|------:|----:|----:|
| `awa` | 30,798 | 8,293 | 20,599 | 43,522 |
| `awa-docker` | 28,777 | 2,676 | 24,328 | 30,822 |
| `awa-python` | 24,357 | 2,276 | 21,012 | 26,823 |
| `river` | 39,792 | 15,912 | 31,461 | 68,195 |
| `oban` | 12,129 | 452 | 11,641 | 12,739 |

#### Worker Throughput — no-op jobs (jobs/sec)

| System | Mean | Stdev | Min | Max |
|--------|-----:|------:|----:|----:|
| `awa` | 851 | 15 | 843 | 879 |
| `awa-docker` | 847 | 3 | 843 | 849 |
| `awa-python` | 818 | 3 | 816 | 823 |
| `river` | 968 | 1 | 967 | 969 |
| `oban` | 2,243 | 1,211 | 1,641 | 4,407 |

#### Pickup Latency — single job to idle queue (p50)

| System | Mean p50 | Min p50 | Max p50 |
|--------|---------:|--------:|--------:|
| `awa` | 56,023 us | 55,738 us | 56,697 us |
| `awa-docker` | 55,768 us | 55,511 us | 56,041 us |
| `awa-python` | 56,933 us | 56,662 us | 57,193 us |
| `river` | 263,434 us | 262,101 us | 264,067 us |
| `oban` | 55,430 us | 53,145 us | 56,045 us |

### Benchmark Takeaways

- The three Awa variants were tightly clustered and stable. Native Awa had the
  fastest mean enqueue throughput, while `awa-docker` and `awa-python` stayed
  close on pickup latency and worker throughput.
- River had strong worker throughput and the fastest mean enqueue throughput in
  this run set, but its enqueue numbers were much more variable than the Awa
  family.
- River remained a clear outlier on pickup latency at roughly `263 ms` p50,
  versus roughly `55-57 ms` for the other systems.
- Oban had the lowest enqueue throughput in this suite, but the highest mean
  worker throughput. That mean is noisy: the 5-run range was `1.6k/s` to
  `4.4k/s`, so the mean should not be treated as a stable single-number result.

### Comparability Notes

- These are still local engineering numbers on one machine, not published
  vendor-style claims.
- No-op workload measures queue/runtime overhead. Real jobs with meaningful work
  would narrow many of these gaps.
- River uses `InsertManyFast` (COPY protocol) for enqueue. Awa uses
  `insert_many_copy_from_pool`.
- Oban uses the open-source Basic engine; Oban Pro's SmartEngine would likely
  differ.
- Single-node only; no multi-process cluster benchmark is implied by these
  results.

### Correctness Under Adverse Conditions

The portable chaos runner (`benchmarks/portable/chaos.py`) exercises crash
recovery, Postgres restart, repeated worker kills, backend termination, leader
failover, and connection-pool exhaustion.

An early River configuration used a stuck-job timeout shorter than the 30s
chaos job duration, which could falsely rescue healthy work. After correcting
River to use a rescue timeout above the job duration, a focused rerun completed
the portable chaos scenarios with zero lost jobs.

#### Chaos Summary

| System | Crash Recovery | Repeated Kills | Leader Failover | Postgres Restart |
|--------|----------------|----------------|-----------------|------------------|
| `awa` | mean `41.7s`, lost `0` | mean `53.4s`, lost `0` | mean `47.5s`, lost `0`, dupes `0` | mean `28.3s`, lost `0` |
| `awa-docker` | mean `41.8s`, lost `0` | mean `54.3s`, lost `0` | mean `47.7s`, lost `0`, dupes `0` | mean `28.5s`, lost `0` |
| `awa-python` | mean `42.2s`, lost `0` | mean `54.4s`, lost `0` | mean `47.9s`, lost `0`, dupes `0` | mean `28.9s`, lost `0` |
| `river` | corrected rerun `103.6s`, lost `0` | corrected rerun `114.6s`, lost `0` | corrected rerun `110.6s`, lost `0`, dupes `0` | corrected rerun `28.4s`, lost `0` |
| `oban` | mean `93.1s`, lost `0` | mean `110.4s`, lost `0` | mean `89.0s`, lost `0`, dupes `0` | mean `29.8s`, lost `0` |

#### Chaos Takeaways

- The Awa family was the most consistent result in the suite: all five runs of
  all portable chaos scenarios completed with zero loss and zero duplicates.
- Oban was also correct in this run set, but materially slower to rescue and
  fail over than the Awa variants.
- River's first run highlighted an important fairness issue in the harness:
  stuck-job detection has to be configured relative to each system's recovery
  semantics and the test job duration, not just matched numerically.
- With the corrected River rescue timeout, the focused rerun completed the
  portable chaos scenarios without job loss or duplicate completions.
- All systems handled Postgres restart and backend-kill scenarios without job
  loss in this suite.

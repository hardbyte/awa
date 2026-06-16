# Benchmarking Notes

This document captures the benchmark harnesses used in the repo and a few reference results from named development and comparison environments.

## Reference Environments

- **Developer laptop reference**: Apple M5 MacBook Air (16 GB), PostgreSQL 17 in Docker (OrbStack). Several awa-only regression examples below come from this environment.
- **Dedicated enqueue reference**: PostgreSQL 17 on a separate Linux host. These runs are useful for producer-path shape comparisons and are not published throughput claims.
- **Cross-system comparison reference**: [postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking). That repo owns fair-comparison hardware, adapter configuration, raw output, and result summaries. Its default Postgres image is 17.2; `--pg-image postgres:18-alpine` selects PG18.
- Example commands assume a developer database URL: `postgres://postgres:test@localhost:15432/awa_test`
- Benchmarks live in: `awa/tests/benchmark_test.rs` `awa/tests/scheduling_benchmark_test.rs` `awa/tests/failure_benchmark_test.rs`
- Python worker benchmarks live in: `awa-python/scripts/benchmark_runtime.py`
- Shared output schema: `awa/tests/bench_output.rs` (Rust) `awa-python/scripts/bench_output.py` (Python)

These are engineering benchmarks, not published vendor-style numbers. The main goal is to compare shapes, validate architecture changes, and catch regressions. Treat each result as tied to the environment named beside it; do not compare numbers from different sections as though they came from one machine.

## North-Star Metrics

Awa's performance target is sustainable end-to-end queue work, not the largest enqueue-only number. A benchmark table should lead with:

- **completed jobs/sec** — durable terminal transitions, not just handler returns
- **p99 end-to-end latency** — producer enqueue to durable completion
- **queue depth / backlog growth** — whether offered load is being absorbed or stored as future latency
- **WAL bytes per completed job** — the main Postgres durability budget for the hot path
- **transaction commits per completed job** — connection and commit pressure
- **dead tuples and prune lag** — whether churn stays bounded under overlap readers, retries, and terminal bursts

Producer throughput is still measured, especially for `INSERT` vs `COPY` and `enqueue_shards` sweeps, but it is a secondary producer-path diagnostic. A configuration that can enqueue `50k/s` and complete `5k/s` is a backlog and latency problem unless the workload is intentionally bursty and has enough headroom to drain before the SLA window closes.

## Positioning Proof Checklist

Queue storage changes Awa's comparison set.

The interesting public question is no longer "can Postgres run jobs?" It is "can a Postgres job queue keep dispatch latency low while also keeping hot-path dead tuples bounded?"

That means the benchmark burden for positioning is different from the benchmark burden for internal regressions. Before making the strongest public claim, the comparison needs to be rerun cleanly on the same hardware against the right reference points:

- Awa queue storage
- PgQue
- River
- optionally Oban Pro as a paid partitioned reference

The benchmark set should include:

- idle pickup latency
- sustained runtime throughput and durable completion throughput
- overlap readers / MVCC horizon pressure
- mixed workload soak
- terminal-failure burst
- WAL bytes/job, commit pressure, queue depth, and dead-tuple pressure for every headline table

Operational differences should be called out, not hidden:

- Awa does not require `pg_cron`; the worker runtime owns dispatch, rescue, rotation, and prune.
- PgQue is designed around a periodic ticker and recommends `pg_cron`.
- River and Oban are job frameworks rather than shared-log event queues, so they remain the more comparable references for worker semantics.

Commands and raw output for any public-facing table should live in-repo under `docs/adr/bench/` or another stable location so the resulting claim is auditable.

## Methodology Notes

The most important lesson from this round of work is that benchmark isolation matters.

The maintenance service is global to the worker instance. If a benchmark leaves behind millions of deferred rows in `awa.scheduled_jobs`, later "hot path" benchmarks may accidentally measure background promotion work as well. The sustained hot/deferred benchmarks reset runtime state first to avoid that pollution.

Two benchmark shapes are used:

- **Burst/frontier benchmarks**: seed a backlog, then drain it
- **Steady/sustained benchmarks**: warm up, then measure a fixed time window

Steady numbers are the better indicator of sustained runtime behavior.

## MVCC Horizon Benchmark

`awa/tests/scheduling_benchmark_test.rs` also includes an ignored `test_mvcc_horizon_overlap_benchmark` scenario for the failure mode described in PlanetScale's "Keeping a Postgres queue healthy" post: overlapping long-lived transactions pin the MVCC horizon while queue churn continues.

The benchmark:

- runs a steady producer feeding `awa.jobs_hot`
- enables aggressive completed-job cleanup to generate update/delete churn
- starts overlapping `REPEATABLE READ` reader transactions on separate connections to hold old snapshots open
- samples per-second throughput, queue depth, and `pg_stat_user_tables` (`n_dead_tup`, vacuum counters) for `awa.jobs_hot`

Reader modes:

- `idle_snapshot`: pins a snapshot and waits inside the transaction; useful for the pure MVCC-horizon case
- `active_scan`: runs repeated queue scans inside the open transaction so the reader behaves more like overlapping analytics work on the primary

Example:

```bash
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --release --package awa --test scheduling_benchmark_test \
  test_mvcc_horizon_overlap_benchmark -- --ignored --exact --nocapture
```

This scenario is also wired into `.github/workflows/nightly-chaos.yml` and runs on PostgreSQL 18 in the Rust nightly benchmark lane.

The nightly lane uses a shorter CI profile so the benchmark stays cheap on shared runners while still exercising overlap readers and cleanup pressure:

- `AWA_MVCC_JOB_RATE=400`
- `AWA_MVCC_BASELINE_SECS=5`
- `AWA_MVCC_OVERLAP_SECS=12`
- `AWA_MVCC_COOLDOWN_SECS=5`
- `AWA_MVCC_OVERLAP_READERS=2`
- `AWA_MVCC_OVERLAP_HOLD_SECS=12`
- `AWA_MVCC_OVERLAP_STAGGER_SECS=4`
- `AWA_MVCC_READER_MODE=active_scan`
- `AWA_MVCC_ANALYTICS_TICK_MS=500`

Nightly regression checks use per-run ratios (`overlap_handler_per_s` and `cooldown_handler_per_s` relative to the same run's baseline window) plus guardrails on `dead_tup_delta` and `max_available`.

That nightly profile is intentionally short. It checks that Awa still behaves reasonably under overlapping analytical readers, but it is not the same thing as the 15-minute mixed-workload soak discussed in the PlanetScale post. For a closer reproduction, increase duration and reader hold times and keep the readers in `active_scan` mode so queries overlap continuously rather than simulating only `idle in transaction`.

A second ignored benchmark target now exists for that longer profile: `test_mvcc_horizon_planetscale_soak`. Its default shape is closer to the blog's mixed-workload setup:

- `800` jobs/sec producer rate
- `3` overlapping analytics readers
- `120s` reader hold time
- `20s` stagger between readers
- `15m` overlap window with `active_scan`

That soak benchmark is wired into CI as a weekly/manual run, while the shorter `test_mvcc_horizon_overlap_benchmark` remains the daily nightly smoke.

### MVCC bench knobs

- `AWA_MVCC_JOB_RATE` — steady producer rate in jobs/sec
- `AWA_MVCC_BASELINE_SECS` / `AWA_MVCC_OVERLAP_SECS` / `AWA_MVCC_COOLDOWN_SECS`
- `AWA_MVCC_OVERLAP_READERS` / `AWA_MVCC_OVERLAP_HOLD_SECS` / `AWA_MVCC_OVERLAP_STAGGER_SECS`
- `AWA_MVCC_READER_MODE` — `idle_snapshot` or `active_scan`
- `AWA_MVCC_ANALYTICS_TICK_MS` — cadence for repeated analytics scans in `active_scan` mode
- `AWA_MVCC_CLEANUP_INTERVAL_MS` / `AWA_MVCC_CLEANUP_BATCH_SIZE`

## Cross-system comparison

The awa benches in this file are the awa-only regression detection track — fast, precise, hard thresholds, awa code only. They are not designed for fair comparison with other systems.

For cross-system comparison, the long-horizon harness lives in a separate repository so each system-under-test can be benchmarked through its public API on equal footing:

[**hardbyte/postgresql-job-queue-benchmarking**](https://github.com/hardbyte/postgresql-job-queue-benchmarking)

The companion repo currently benchmarks awa against pgque, procrastinate, pg-boss, river, oban, and pgmq using a shared subprocess contract: each adapter is a self-contained binary that emits one JSON sample per line. Headline numbers, per-system architectural notes, and reproducible run instructions are in that repo's [`SYSTEM_COMPARISONS.md`](https://github.com/hardbyte/postgresql-job-queue-benchmarking/blob/main/SYSTEM_COMPARISONS.md) and [`results/2026-04-28/SUMMARY.md`](https://github.com/hardbyte/postgresql-job-queue-benchmarking/blob/main/results/2026-04-28/SUMMARY.md).

The two tracks are deliberately separate. If they ever diverge on workload shape or thresholds, the awa-only benches in this file are canonical for awa's own numbers and the cross-system runner defers.

### 2026-05-03 cross-system reference run

An overnight run of the companion benchmark repo compared `awa` 0.6 alpha builds with the same phase-driven harness. Treat these as reference results for shape and regression tracking, not universal product guarantees.

Key observations:

- `awa` peak throughput improved by `49%` from 0.6.0-alpha.2 to 0.6.0-alpha.3 at 128 workers and one replica: `4,576` to `6,834` jobs/s. The same run showed a roughly `1.5x` to `1.7x` improvement across the worker-count matrix.
- Phase A matrix shape for `awa`: `296` -> `1,115` -> `3,961` -> `6,834` jobs/s as worker count increased.
- Phase B `pgmq` peaked at `13,290` jobs/s at 16 workers and collapsed at 128 workers, matching the previously observed high-worker behaviour.
- Phase C multi-replica runs exposed the remaining `awa` topology sensitivity: at fixed total workers, throughput fell from `3,560` to `2,491` to `1,503` jobs/s across `1x64`, `2x32`, and `4x16`. That shape is consistent with fleet-wide completion flusher contention (`processes x AWA_COMPLETION_SHARDS`). `pgque` moved in the opposite direction in the same run (`18,660` -> `34,388` -> `38,810`), so this remains a useful comparison target rather than noise to smooth over.
- Phase D 60-minute `awa` soak sustained a median `5,369` jobs/s with median dead tuples around `396`, validating the ADR-019 / ADR-023 hot-path dead-tuple promise under sustained churn.

Queue-storage e2e sweeps separate tuning from storage design. For the hot single-queue shape, `enqueue_shards = 4` plus larger completion batches sustained `7.9k` completed jobs/s with `200ms` p99 end-to-end latency and bounded depth. Increasing `claimers` did not materially improve that shape.

When the application can accept partitioned ordering at the logical workload level, routing through several physical queues with `PartitionedQueue` is the preferred throughput lever: it creates independent claim and completion coordination streams rather than adding more claimers to one queue head.

ADR-026 terminal-count deltas remove hot-path `queue_terminal_live_counts` updates. Completion and terminal-delete paths append signed deltas, exact reads include pending deltas, and maintenance folds sealed slots into compact counters only when the MVCC horizon is not pinned by another backend snapshot or idle transaction id. Benchmark runs should therefore sample `queue_terminal_count_deltas_*` alongside `queue_terminal_live_counts` so regressions distinguish pending append-only rows from mutable-counter dead tuples.

The offered-rate benchmark exercises absorption directly and samples WAL. With one claimer, `claim_batch_size = 512`, `AWA_COMPLETION_BATCH_SIZE = 512`, the fused receipt completion path, and `max_workers = 1024`, a 10-second no-op run at `10k/s` offered load keeps durable completions at the offered rate, drains to zero backlog, and writes `1.80 KiB` WAL per completed job. `max_workers = 512` is close but does not consistently meet the 10k offered target, because no-op handlers hold permits while waiting for durable completion acknowledgement.

First-principles WAL accounting then compared the production path, a narrow `done_entries` row, and a deliberately non-production path that skipped `done_entries` entirely:

| Shape                          | Completed jobs/s |  p99 e2e |   WAL/job |
| ------------------------------ | ---------------: | -------: | --------: |
| Tuned production queue storage |        `7,885/s` | `203 ms` | `2,241 B` |
| Narrow terminal history        |        `8,337/s` | `205 ms` | `1,932 B` |
| Skip `done_entries` entirely   |        `8,402/s` | `230 ms` | `1,441 B` |

The shipped design is the middle row: keep the durable terminal fact, but avoid duplicating immutable ready-body fields on ready-backed terminal rows. The skip-`done_entries` experiment remains rejected because it weakens the terminal-history contract, and its small throughput gain over narrow terminal history points at durable WAL write/sync pressure rather than a hidden per-job query loop as the next ceiling.

### Queue-storage striping reference

Queue striping is a contention-control knob for workloads dominated by one hot logical queue. The companion benchmark repo includes an awa-only sweep of `queue_storage_queue_stripe_count` over `1`, `2`, and `4` at `64`, `128`, `256`, and `512` workers. The source artifact lives in companion benchmark PR [#21](https://github.com/hardbyte/postgresql-job-queue-benchmarking/pull/21).

Reference result:

| Stripes | 64 workers | 128 workers | 256 workers | 512 workers |
| ------: | ---------: | ----------: | ----------: | ----------: |
|       1 |  `3,408/s` |   `4,741/s` |   `9,530/s` |  `11,188/s` |
|       2 |  `4,654/s` |   `7,667/s` |  `11,975/s` |  `11,173/s` |
|       4 |  `4,378/s` |   `7,596/s` |  `11,418/s` |  `11,443/s` |

The clearest gain was the `1 -> 2` stripe step: at `128` workers throughput rose by `62%`, and at `256` workers throughput rose by `26%` while end-to-end p99 fell from `1,802 ms` to `1,027 ms`. `4` stripes mostly matched `2` stripes in this shape, with the only clear advantage at the `512` worker tail.

Treat this as tuning guidance, not an out-of-the-box setting. The default stays `queue_storage_queue_stripe_count=1`; hot single-queue deployments should consider `2` after measuring their own worker/replica shape.

## Python Runtime Benchmarks

The Python benchmark script exercises the real `awa-python` worker path while reusing the same database-facing benchmark shapes as the Rust runtime:

**Baseline scenarios** (`--scenario baseline`):

- `copy`: Python client `enqueue_many_copy` direct queue-storage COPY throughput
- `hot`: sustained worker throughput over pre-seeded `awa.jobs_hot`
- `scheduled`: sustained deferred promotion over pre-seeded `awa.scheduled_jobs`

**Failure scenarios** (`--scenario failures`):

- `terminal_1pct` / `10pct` / `50pct`: terminal failures
- `retryable_1pct` / `10pct` / `50pct`: retry-once failures
- `callback_timeout_10pct`: callback registration with short timeout
- `mixed_50pct`: rotating through terminal, retryable, and success modes

The worker-focused scenarios seed with SQL directly so the reported number is about Python handler dispatch and runtime behavior, not enqueue serialization.

## Reference Results

### Immediate Enqueue Throughput

The enqueue path that is being measured here has a few important architectural properties:

- homogeneous inserts now route directly to `awa.jobs_hot` or `awa.scheduled_jobs` instead of going through the compatibility view
- admin metadata maintenance moved from row-level triggers to statement-level trigger batches
- COPY staging now reuses a session-local temp table and stages typed values instead of reparsing text on the final `INSERT ... SELECT`

Example reference result from the developer laptop reference:

- developer laptop reference (`Apple M5`, release build, v0.5.0-alpha.0):
  - `insert_only_single`: about `30k inserts/s`
  - `copy_single`: about `43k inserts/s`
  - `insert_contention_distinct` (4 producers x 3k): about `46k inserts/s`
  - `copy_contention_distinct` (4 producers x 3k, chunk 1000): about `100k inserts/s`
  - `insert_contention_same_queue` (4 producers x 3k): about `95k inserts/s`
  - `copy_contention_same_queue` (4 producers x 3k, chunk 1000): about `67k inserts/s`

These are engineering comparisons, not product guarantees. Their main value is showing where the architecture bottlenecks move as the implementation changes.

### Sustained Hot Path

Measured with `test_runtime_sustained_hot_path` after resetting runtime state:

- warmup: 2s
- measurement window: 10s
- queue size seeded: 200,000 immediately-available jobs

Example reference result from the developer laptop reference (release mode, v0.5.0-alpha.0):

- handler returns: about `5.6k jobs/s`
- DB `completed` transitions: about `5.6k jobs/s`

This benchmark enables the in-memory OpenTelemetry exporter and the production alerting metrics path (queue depth, lag, wait-duration histogram). Back-to-back A/B testing in the same reference environment shows v0.5.0 is ~44% faster than v0.4.1 (5.9k vs 4.1k/s) — the promotion query optimizations more than offset the metrics instrumentation cost.

### Python Runtime Baseline

Measured with `awa-python/scripts/benchmark_runtime.py` on the developer laptop reference database:

- `enqueue_many_copy`: about `16.2k jobs/s` (`50,000` jobs in `3.09s`)
- sustained hot path:
  - handler returns: about `3.2k jobs/s`
  - DB `completed` transitions: about `3.1k jobs/s`

The `copy` scenario measures producer serialization and direct queue-storage COPY. The worker-focused scenarios seed with SQL so the runtime number is not dominated by Python-side enqueue serialization.

### Deep Backlog Drain

`test_queue_storage_deep_backlog_drain_benchmark` is an ignored regression benchmark for the spike shape where a large ready backlog drains more slowly than the same fleet processes steady offered load. It seeds with direct queue-storage COPY, optionally analyzes `ready_entries`, then measures claim/complete throughput and claim latency while draining a fixed backlog.

```bash
DATABASE_URL_QUEUE_STORAGE=postgres://postgres:test@localhost:15432/awa_test_queue_storage \
AWA_QS_DEEP_BACKLOG_JOBS=1000000 \
AWA_QS_DEEP_BACKLOG_SECONDS=120 \
AWA_QS_DEEP_BACKLOG_CONSUMERS=16 \
AWA_QS_DEEP_BACKLOG_SHARDS=16 \
cargo test --package awa --test queue_storage_benchmark_test \
  test_queue_storage_deep_backlog_drain_benchmark -- --exact --ignored --nocapture
```

Useful knobs:

- `AWA_QS_DEEP_BACKLOG_JOBS` — ready rows to seed (default `100000`)
- `AWA_QS_DEEP_BACKLOG_COPY_BATCH` — direct COPY chunk size (default `1000`)
- `AWA_QS_DEEP_BACKLOG_CONSUMERS` — concurrent claim/complete loops (default `8`)
- `AWA_QS_DEEP_BACKLOG_CLAIM_BATCH_SIZE` — claim batch size (default `512`)
- `AWA_QS_DEEP_BACKLOG_SHARDS` — `awa.queue_meta.enqueue_shards` upserted before seeding (default `16`)
- `AWA_QS_DEEP_BACKLOG_ANALYZE` — run `ANALYZE` after seeding (default `1`)

### Large Deferred Frontier

Measured with `test_scheduled_steady_10m_due_1k_per_sec`:

- total deferred backlog: `10,000,000` rows
- due rate target: `1,000` jobs/s
- measurement window: 10s

Isolated 4-thread Tokio runtime result (release mode, v0.5.0-alpha.0):

- `9,000` of `10,000` due jobs completed within the window
- per-second completions: `0, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000` — perfectly steady after the first-tick startup delay
- pickup lateness:
  - `p50`: `229 ms`
  - `p95`: `332 ms`
  - `p99`: `343 ms`
- promotion: `354` batches, mean `4.3 ms`, max `59 ms`
- claim latency: mean `5.0 ms`

This demonstrates that the hot/deferred split with literal-state promotion queries handles a 10M-row deferred frontier with steady, predictable throughput.

**Key optimization (v0.5.0):** Promotion queries use literal state values (e.g., `WHERE state = 'scheduled'`) instead of parameterized (`WHERE state = $1`). This allows the Postgres planner to match the partial index `idx_awa_scheduled_jobs_run_at_scheduled` at plan time. With a parameterized query, the planner falls back to a full bitmap scan on multi-million-row tables, degrading promotion from ~4ms to ~400ms per batch (100x slower).

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

This validates the architecture at a realistic production scale: 2M deferred rows with 4k/s throughput and reliable promotion.

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

This rate was previously a documented scaling limit (only 20-57k of 60k promoted, promotion at 1.7-3.6s per batch). The literal-state promotion fix (v0.5.0) eliminated the bottleneck entirely.

**Promotion and completion throughput knobs:**

- `promote_interval` (default `250 ms`): how often promotion runs
- `AWA_COMPLETION_FLUSH_MS` (default `1 ms`): completion batcher flush interval
- `AWA_COMPLETION_BATCH_SIZE` (default `512`): max rows per completion-batcher flush. Queue-storage short-job completion uses a fused receipt-claim lock, terminal-insert, and count-delta statement, so the default keeps finalization latency low while still amortising durable completion work under load.
- `AWA_COMPLETION_SHARDS`: number of parallel completion flushers. The runtime default is storage-dependent: `8` for canonical storage and `1` for queue storage. Queue storage starts conservative because the terminal / receipt path already batches heavily and the effective fleet-wide flusher count is `processes × AWA_COMPLETION_SHARDS`. Increase only after measuring end-to-end throughput, p99 latency, WAL bytes/job, and dead tuples for the target worker-process topology.

Internal promotion constants are fixed in the runtime: `PROMOTE_BATCH_SIZE = 4,096` rows per promotion batch and `PROMOTE_MAX_BATCHES_PER_TICK = 32` batches per maintenance tick. They are not environment variables or builder options.

### Concurrent Multi-Queue Lifecycle

Measured with `concurrent_lifecycle_test.rs`. Four queues (`email:32`, `payments:16`, `analytics:64`, `webhooks:16` workers), 128 total workers, full insert → claim → execute → complete lifecycle.

Queue-count sweep with 128 total workers, pool=50, 20k jobs (release mode):

| Config        | Workers/queue | Throughput | Per-worker |
| ------------- | ------------- | ---------- | ---------- |
| 1 queue × 128 | 128           | `~1.9k/s`  | `~14/s`    |
| 2 queues × 64 | 64            | `~2.0k/s`  | `~15/s`    |
| 4 queues × 32 | 32            | `~350/s`   | `~2.7/s`   |

**Key finding**: 1-queue and 2-queue throughput is essentially identical, but 4 queues drops 5-6x. The cliff between 2 and 4 queues indicates that the per-queue dispatcher overhead (separate claim query, PgListener, semaphore) compounds non-linearly when many small queues share a pool.

For comparison, the hot-path benchmark (200k pre-seeded into `jobs_hot` via SQL) reaches `~10k/s` because it bypasses insert triggers and has a fully warmed dispatch pipeline. The lifecycle benchmarks exercise the complete path including job-state triggers and notification.

**Tuning guidelines**:

- Size the connection pool to at least `num_queues * 4 + 20`. With 4 queues, that's 36+ connections.
- Prefer fewer queues with larger worker pools over many small queues. 2 queues × 64 workers performs the same as 1 × 128.
- Use multi-queue for isolation, priority, or rate limiting — not for throughput.

A unified cross-queue claim query (one SQL round-trip claiming across all queues via `LATERAL JOIN`) could eliminate the per-queue dispatch overhead. Prototyping shows 16ms for 48 jobs across 2 queues — comparable to single-queue performance. This is tracked as a future optimization.

### Progress Feature Overhead

ADR-014 introduced the user-facing structured progress API and a two-tier heartbeat flush. In the current queue-storage engine, mutable progress lives in `{schema}.attempt_state` only after an attempt first needs per-attempt mutable state; short jobs that never report progress do not allocate an `attempt_state` row. Older benchmark notes refer to `progress` columns on `jobs_hot` / `scheduled_jobs`, which was the canonical-storage implementation.

Performance impact was validated:

- **Zero overhead when no progress is set.** The heartbeat service partitions jobs by pending progress; jobs without mutations use the heartbeat-only path, and `snapshot_pending_progress` returns empty when no generation has been bumped.
- **Completion remains narrow.** Successful completion clears the progress snapshot while closing the attempt. On queue storage this is handled through the attempt-state / terminal-snapshot path rather than rewriting the ready row.
- **Sustained hot-path throughput** was unchanged when the progress feature was added. Current hot-path throughput (~5.6k/s) reflects the additional v0.5.0 OTel metrics instrumentation, not progress overhead.

## Failure-Mode Benchmarks

The failure-mode benchmark suite measures throughput, drain time, and recovery behaviour when a configurable percentage of jobs fail, retry, hang, or trigger rescue paths. This answers a question the happy-path benchmarks cannot: **how does failure impact healthy-job throughput?**

### Benchmark matrix

| Scenario | Description |
| --- | --- |
| `terminal_1pct` / `10pct` / `50pct` | N% of jobs fail terminally |
| `retryable_1pct` / `10pct` / `50pct` | N% of jobs fail once then succeed on retry |
| `callback_timeout_10pct` | 10% register a callback that times out, then succeed on retry |
| `deadline_hang_10pct` | 10% hang until deadline rescue fires, then succeed on retry |
| `snooze_once_10pct` | 10% snooze once, then succeed |
| `mixed_all_modes` | 50% success, 10% each of terminal/retryable/callback/deadline/snooze |
| `stale_heartbeat_rescue` | All jobs seeded as "running" with stale heartbeat — measures rescue-to-completion time |

### Rust harness

Tests live in `awa/tests/failure_benchmark_test.rs`. Each scenario seeds jobs deterministically by mode, starts a Client with aggressive rescue intervals, drains to terminal states, and emits both human-readable output and a JSONL record. The full matrix command covers the 10 failure scenarios; the stale-heartbeat rescue benchmark is a separate test.

### Python harness

The Python benchmark (`awa-python/scripts/benchmark_runtime.py`) supports a failure-mode subset via `--scenario failures`:

- `terminal_1pct` / `10pct` / `50pct`
- `retryable_1pct` / `10pct` / `50pct`
- `callback_timeout_10pct`
- `mixed_50pct`

Python also includes heartbeat rescue via `--scenario rescue`. It still does not include the Rust-only `deadline_hang` and `snooze_once` scenarios. The worker returns `RetryAfter`, `WaitForCallback`, `Cancel`, or raises exceptions based on the job's `mode` field.

### Structured output

Both Rust and Python benchmarks emit one JSONL record per scenario, prefixed with `@@BENCH_JSON@@` for extraction. Schema version 2:

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

For enqueue-only benchmarks (`insert_only_single`, `copy_single`, and the contention matrix), `metrics.enqueue_per_s` is emitted instead of `metrics.throughput`. Those records still include `"measurement": "enqueue"` in `metadata`, plus optional Postgres-side deltas such as `wal_bytes`, `temp_bytes_delta`, and `xact_commit_delta`.

## Interpreting The Results

Some practical guidelines:

- Compare like with like. Burst/frontier benchmarks and steady-state benchmarks answer different questions.
- Reset runtime state before sustained measurements if you want to isolate one path. Global background work can distort results.
- Prefer the `db_completed_delta` view when you care about end-to-end queue completion, not just handler return rate.
- Treat the numbers here as environment-specific reference points, not portable guarantees. For cross-system claims, use the companion comparison repo and its recorded environment.

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

These are the most useful benchmarks when you want to compare single-producer enqueue against multi-producer contention, or compare chunked `INSERT` with the COPY staging path under concurrent writers.

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

The matrix hard-resets Awa runtime tables before each scenario so later cases do not inherit a larger or dirtier jobs table from earlier ones.

By default, `AWA_BENCH_COPY_CHUNK_SIZE` should match `AWA_BENCH_INSERT_BATCH_SIZE` if you want the closest apples-to-apples comparison between chunked `INSERT` and chunked COPY staging. If you want to test the current "one bulk COPY per producer" shape instead, set `AWA_BENCH_COPY_CHUNK_SIZE` to `AWA_BENCH_CONTENTION_JOBS_PER_PRODUCER`.

The optional Postgres profile block in `metadata.db_profile` is meant to make server runs easier to interpret. In particular:

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

- Each number is tied to the environment stated near the result. The developer commands use a localhost database URL for convenience, but that URL is not a claim about where every result was produced.
- Ignored benchmark tests are not part of the normal unit/integration test pass.
- The current focus is relative behavior and architectural validation, not cross-machine leaderboard comparisons.

## Cross-system numbers

Cross-system comparison (awa vs pgque / procrastinate / pg-boss / river / oban / pgmq) lives in [hardbyte/postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking). That repo is the source of truth for fair-comparison numbers, chaos results across systems, and the long-horizon harness itself.

The reasoning for the split: cross-system benches need to evolve at the pace of the systems being compared (each gets new releases, new public APIs, new adapter caveats), and the awa-only regression track in this file needs to evolve at awa's pace. Keeping them in one repo coupled their schedules and conflated "did awa regress?" with "is awa still faster than X?". They're separate questions; they're now in separate places.

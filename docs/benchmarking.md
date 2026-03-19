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

Examples:

```bash
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test scheduling_benchmark_test \
  test_runtime_sustained_hot_path -- --exact --ignored --nocapture

DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test --package awa --test scheduling_benchmark_test \
  test_scheduled_steady_10m_due_1k_per_sec -- --exact --ignored --nocapture
```

## Caveats

- These numbers are from one local machine and one local Postgres setup.
- Ignored benchmark tests are not part of the normal unit/integration test pass.
- The current focus is relative behavior and architectural validation, not
  cross-machine leaderboard comparisons.

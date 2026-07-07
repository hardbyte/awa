# AWA — Validation Test Plan

Tests run against real Postgres 15+ (not managed services) using a dedicated test database. Most Rust/Python rows below are automated in CI; heavyweight soak, failover, benchmark, TLA+, and managed-service checks are run manually or in scheduled lanes when they need special infrastructure.

## Test Matrix

**Rust** = Rust integration test, **Py** = Python test, **TLA+** = TLC model check, **Both** = cross-language.

### Correctness & Crash Recovery

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T1 | No duplicate processing (100k jobs) | ✓ |  |
| T4 | `kill -9` crash recovery | ✓ |  |
| T5 | Deadline rescue (hung job) | ✓ |  |
| T6 | Transactional atomicity | ✓ |  |
| T7 | Transactional atomicity (Python) |  | ✓ |
| T8 | Uniqueness under contention | ✓ | ✓ |
| T9 | Hash cross-language consistency | ✓ |  |
| T10 | Priority ordering | ✓ |  |
| T12 | Queue isolation | ✓ |  |
| T18 | Backoff timing | ✓ |  |
| T19 | Snooze semantics | ✓ |  |
| T20 | Terminal error semantics | ✓ |  |
| T21 | Deserialization failure | ✓ |  |
| B1 | Late completion after rescue is no-op | ✓ |  |
| B2 | Late completion after cancel is no-op | ✓ |  |
| B3 | Shutdown waits for in-flight jobs | ✓ |  |
| B4 | Heartbeat alive during shutdown drain | ✓ |  |
| B5 | Deadline rescue signals ctx.is_cancelled() | ✓ |  |
| B6 | UniqueConflict.constraint field | ✓ |  |
| B7 | Admin cancel signals in-flight handler cancellation | ✓ | ✓ |
| T75 | Priority aging maintenance task promotes long-waiting low-priority jobs | ✓ |  |

### Uniqueness (Python)

| #    | Test                                                  | Rust | Py  |
| ---- | ----------------------------------------------------- | ---- | --- |
| T8   | Uniqueness under contention (10 concurrent producers) | ✓    |     |
| PU1  | Unique insert happy path                              |      | ✓   |
| PU2  | Duplicate rejected (UniqueConflict)                   |      | ✓   |
| PU3  | by_queue: different queues allowed                    |      | ✓   |
| PU4  | by_queue: same queue rejected                         |      | ✓   |
| PU5  | by_args: different args allowed                       |      | ✓   |
| PU6  | by_period: same bucket rejected                       |      | ✓   |
| PU7  | by_period: different bucket allowed                   |      | ✓   |
| PU8  | Unicode args hash consistently                        |      | ✓   |
| PU9  | Nested dict args hash consistently                    |      | ✓   |
| PU10 | Empty args hash consistently                          |      | ✓   |
| PU11 | Large args (~100KB) hash consistently                 |      | ✓   |
| PU12 | No unique_opts allows duplicates                      |      | ✓   |
| PU13 | Sync insert with unique_opts                          |      | ✓   |

### Admin & Resilience

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T13 | Queue pause/resume | ✓ |  |
| T22 | Pool exhaustion resilience | ✓ |  |
| T26 | Migration idempotency | ✓ |  |
| T27 | Admin ops under load | ✓ |  |
| T72 | Runtime recovers after terminating Postgres backends | ✓ |  |
| T71 | Mixed Rust/Python workers share queue | Both | Both |
| T73 | Sustained mixed workload survives node failure (see below) | Both | Both |
| T74 | Hot-standby promotion | ✓ |  |
| HA1 | Postgres failover smoke | ✓ |  |

**T73 ordering invariant:** Jobs are inserted _before_ Rust clients start so that the Python worker exclusively claims `simple_chaos_job` rows. This avoids a race where Rust's instant `CompleteWorker` steals all simple jobs before the slower Python worker (400 ms/job) can claim any. The test confirms Python is mid-execution via its `START` stdout line, then kills it. Heartbeat backdating triggers rescue, and the `max_simple_attempt >= 2` assertion verifies re-processing by a surviving Rust worker.

### External Callbacks

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| E1 | register_callback + WaitForCallback → waiting_external | ✓ | ✓ |
| E2 | complete_external → completed | ✓ | ✓ |
| E3 | fail_external → failed | ✓ | ✓ |
| E4 | retry_external → available | ✓ | ✓ |
| E5/PE1-2 | Callback timeout rescue (retryable + failed) | ✓ | ✓ |
| E6/PE3 | Double completion → CallbackNotFound | ✓ | ✓ |
| E7 | Wrong callback_id → CallbackNotFound | ✓ | ✓ |
| E8/PE4 | Admin cancel while waiting_external | ✓ | ✓ |
| E9/PE5 | Admin retry while waiting_external | ✓ | ✓ |
| E10/PE6 | drain_queue includes waiting_external | ✓ | ✓ |
| E11/PE7 | Race: complete during running (before WaitForCallback) | ✓ | ✓ |
| E12 | Crash clears stale callback | ✓ |  |
| E13 | Uniqueness during waiting_external | ✓ |  |
| E15 | resolve_callback accepts running state | ✓ |  |
| E16/PE8 | Stale callback rejected after rescue | ✓ | ✓ |
| E17 | cancel_callback clears fields | ✓ |  |
| E18 | cancel_callback wrong lease is noop | ✓ |  |
| PE9 | Callback timeout rescued end-to-end by live runtime |  | ✓ |

### CEL Callback Expressions

| #       | Test                                               | Rust | Py  |
| ------- | -------------------------------------------------- | ---- | --- |
| C1-C2   | resolve_callback default actions (complete/ignore) | ✓    | ✓   |
| C3-C4   | Filter expressions (pass/fail)                     | ✓    |     |
| C5-C6   | on_fail takes precedence over on_complete          | ✓    |     |
| C7      | Transform payload                                  | ✓    |     |
| C8-C9   | Invalid CEL fail-open                              | ✓    |     |
| C10     | Fallthrough to default action                      | ✓    |     |
| C11     | Invalid CEL at registration → validation error     | ✓    |     |
| C12     | Double resolve → CallbackNotFound                  | ✓    |     |
| C14-C15 | Deeply nested / missing field (fail-open)          | ✓    |     |
| C16     | resolve_callback accepts running state             | ✓    |     |
| C17     | Concurrent resolve (FOR UPDATE prevents race)      | ✓    |     |
| C18-C19 | CEL disabled: registration + resolution errors     | ✓    |     |

### Sequential Callbacks

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| SC1 | wait_for_callback: suspend + resume | ✓ | ✓ (SC14) |
| SC2 | Two sequential callbacks via admin API | ✓ | ✓ (SC15) |
| SC3 | Timeout during second callback → retryable | ✓ |  |
| SC4/SC16 | Heartbeat extends timeout during wait | ✓ | ✓ |
| SC5 | Concurrent resume: exactly one succeeds | ✓ |  |
| SC6 | Resume with wrong run_lease rejected | ✓ |  |
| SC7 | Crash/rescue after resume | ✓ |  |
| SC8 | Admin cancel after resume | ✓ |  |
| SC9 | Resume preserves metadata | ✓ | ✓ |
| SC10-SC12 | fail/resolve/retry on second callback | ✓ |  |
| SC13 | resume_external transitions to running with payload |  | ✓ |
| SC17 | Double resume fails with CallbackNotFound |  | ✓ |

### HTTP Worker

| #   | Test                               | Rust | Py  |
| --- | ---------------------------------- | ---- | --- |
| HW1 | Sync mode 200 → completed          | ✓    | —   |
| HW2 | Sync mode 500 → retryable          | ✓    | —   |
| HW3 | Sync mode 400 → terminal fail      | ✓    | —   |
| HW4 | Async mode 202 → callback complete | ✓    | —   |
| HW5 | Async mode 503 → retryable         | ✓    | —   |
| HW6 | Async unreachable → retryable      | ✓    | —   |
| HW7 | Custom headers                     | ✓    | —   |
| HW8 | BLAKE3 callback signature          | ✓    | —   |
| HW9 | Callback URL construction          | ✓    | —   |

_HTTPWorker is a Rust-only feature (ADR-018: serverless function dispatch). Not exposed to Python._

### Python SDK

| #       | Test                                                         | Py  |
| ------- | ------------------------------------------------------------ | --- |
| T58-T70 | Sync API variants (insert, cancel, retry, drain, list, etc.) | ✓   |
| P12-P16 | start() config validation (dict, tuple, weighted, errors)    | ✓   |
| P17     | start() accepts all maintenance interval kwargs              | ✓   |
| PP1-PP5 | Progress tracking (set, merge, flush, checkpoint)            | ✓   |
| BR4     | Bridge: asyncpg/psycopg/SQLAlchemy/Django insert             | ✓   |

### Cron / Periodic Jobs

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T34-T46 | Cron: migration, upsert, atomic CTE, dedup, backfill, DST | ✓ |  |
| T40-T41 | End-to-end periodic + metadata propagation | ✓ |  |
| CRP1-CRP9 | Cron pause / resume: state transitions, CTE guard, upsert preserves pause, manual trigger bypass, queue interaction | ✓ |  |

### COPY Batch Ingestion

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T47-T57a | COPY: empty, single, 1000, special chars, tags, unique, atomic | ✓ | ✓ |
| QSC1-QSC2 | Queue storage COPY producer: ready/deferred direct COPY, unique-conflict rollback | ✓ | ✓ |

### Rate Limiting & Weighted Concurrency

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| RL1-RL6 | Rate limit: fast, throttle, burst, bottleneck, invalid | ✓ |  |
| W5-W13 | Weighted: backward compat, overflow, floor, cap, proportionality | ✓ |  |

### Observability

| #       | Test                         | Rust |
| ------- | ---------------------------- | ---- |
| T28-T30 | Tracing spans + OTel metrics | ✓    |
| OT1-OT4 | OTLP export end-to-end       | ✓    |

### Bridge Adapters

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| BR1-BR3 | tokio-postgres bridge: atomicity, lease guard, unique conflict | ✓ |  |
| BR4 | Python bridge: asyncpg/psycopg/SQLAlchemy/Django |  | ✓ |

### UI (Admin)

| # | Test | Rust |
| --- | --- | --- |
| RO1-RO3 | Read-only serve: capabilities, mutations blocked, UI disables | ✓ |
| UIB1-UIB3 | Batch operations page: set-priority preview/submit, move-queue payload, cancellation request | ✓ |

### Admin Metadata Guardrails

| #   | Test                                                         | Rust |
| --- | ------------------------------------------------------------ | ---- |
| AM1 | Heartbeat/progress-only UPDATEs do not dirty queues or kinds | ✓    |
| AM2 | flush_dirty_admin_metadata() drains backlog > 100 keys       | ✓    |

### Benchmarks

| # | Test | Rust | Py |
| --- | --- | --- | --- |
| T31-T33 | Throughput + latency + insert speed | ✓ |  |
| CL1-CL2 | Concurrent multi-queue lifecycle | ✓ |  |
| SP1-SP2 | Scheduled promotion at scale | ✓ |  |
| FB1-FB7 | Failure modes: terminal, retryable, callback, deadline, mixed | ✓ |  |
| FB8 | Failure modes (Python) |  | ✓ |
| PB1 | Portable cross-system enqueue throughput (Awa vs River vs Oban, 10k and 50k jobs) |  |  |
| PB2 | Portable cross-system worker throughput (50/100/200 workers, no-op jobs) |  |  |
| PB3 | Portable cross-system pickup latency (single job to idle queue) |  |  |

### Dirty-key trigger overhead (measured v0.5.1, debug build, Docker PG 17)

Concurrent lifecycle benchmark (1 queue × 128 workers, 20K jobs):

| Config                  | Throughput | Overhead |
| ----------------------- | ---------- | -------- |
| No triggers (baseline)  | 1963/s     | —        |
| Noop PL/pgSQL trigger   | 1855/s     | 5.5%     |
| Full dirty-key triggers | 1832/s     | **~7%**  |

### Formal Models (TLA+)

| # | Model | Invariants |
| --- | --- | --- |
| TLA1 | AwaCore | Lease-guarded finalization, stale completion rejected |
| TLA2 | AwaExtended | Shutdown/rescue/permit/fairness protocol |
| TLA3 | AwaCbk | At-most-once callback resolution, sequential resume |
| TLA4 | AwaCron | No duplicate fire under leader failover; paused schedules block enqueue across a snapshot-pause-CAS race; liveness preserved under fair Resume |
| TLA5 | AwaBatcher | At-most-once completion, DirectCompleteFail recovery |
| TLA6 | AwaDispatchClaim with NewClaim config | Dispatch claim safety |
| TLA7 | AwaSegmentedStorage | Segmented storage safety, waiting flow, optional attempt-state, prune safety |
| TLA8 | AwaSegmentedStorageInterleavings | Two-worker segmented-storage interleavings |
| TLA9 | AwaShardedPrune | Cross-shard ready/terminal prune matching by `enqueue_shard` |
| TLA10 | AwaSegmentedStorageRaces | Claim-vs-rotate/prune race exposure and checked-commit safety |
| TLA11 | AwaSegmentedStorageTrace | Runtime trace acceptance for snooze, receipt rescue, cancel, callback wait, DLQ retry, and DLQ purge paths |
| TLA12 | AwaStorageLockOrder | Postgres lock ordering across claim, complete, cancel, rescue, rotate, and prune |
| TLA13 | AwaStorageTransition | Storage-transition prepare, mixed-entry, finalize, and abort gates |
| TLA14 | AwaDeadTupleContract | Hot-table reclaim-kind and partition-truncate contract |

The formal suite includes passing configs and expected-counterexample configs. The expected-counterexample configs keep historical bugs executable: old dispatch claim, old view trigger, naive segment race, old storage-transition gate, shard-ignorant prune, and deliberate lock-order cycles. Trace configs use a `TraceIncomplete` invariant as a positive witness: a valid trace violates that invariant after TLC consumes every event.

## 0.7 Planned Validation

Planned test matrix for the 0.7 cycle, mapped to the roadmap ([`0.7-roadmap.md`](0.7-roadmap.md)) and the release gates on the [#383 tracker](https://github.com/hardbyte/awa/issues/383). Rows move into the matrix above as they are implemented.

### Harness & compatibility

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V1 | Broad integration suite green under `AWA_TEST_ENGINE=queue_storage` | ✓ | ✓ | #360 |
| V2 | Engine guard rejects canonical-only raw-SQL helpers under queue_storage | ✓ |  | #360 |
| V3 | Pinned 0.6.0 binary: enqueue→claim→complete→cancel against 0.7 schema | ✓ | ✓ | #367 |
| V4 | 0.7 binary against pre-migrate 0.6 schema fails loudly (message + exit code asserted) | ✓ |  | #367 |
| V5 | `awa migrate` refuses non-`active` storage state, names finalize steps | ✓ |  | #370 |

### Deployment & operations

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V6 | `/readyz` flips 503 on DB-down / schema-mismatch / stalled claim loop | ✓ |  | #368 |
| V7 | Maintenance-only role: promotes/rescues/rotates, never claims or dispatches | ✓ | ✓ | #282 |
| V8 | Chaos topology with zero general workers (maintenance-only + callback ingress + HttpWorker) | ✓ |  | #282, #372 |
| V9 | Callback contract parity: embedded router vs `awa callbacks serve` (shared contract tests) | ✓ | ✓ | #372 |
| V10 | `tick()` bounded steps; scheduler-driven demo promotes/rescues with no resident worker | ✓ | ✓ | #118 |
| V11 | Unauthenticated non-loopback serve → read-only + banner; token auth constant-time; mutation audit line | ✓ |  | #343 |
| V12 | `awa doctor` checks cover every troubleshooting.md scenario; `--json` schema asserted | ✓ |  | #373 |
| V13 | Integration suite through pgbouncer session mode; transaction mode engages NOTIFY fallback + warning | ✓ | ✓ | #374 |
| V14 | Helm chart kind smoke: four-surface topology installs, probes pass, migration hook runs | ✓ |  | #344 |

### Observability

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V15 | Trace context: enqueue→claim→finalize one connected trace; retries/callbacks/cron linked | ✓ |  | #110 |
| V16 | Trace propagation across the PyO3 boundary (Python handler is a child span) |  | ✓ | #110 |
| V17 | Tracing overhead A/B within the E8 gate (<2% sampled enqueue cost) | ✓ |  | #110 |
| V18 | Attempt-timeline API assembles ordered events across retries incl. callback park/resume | ✓ |  | #375 |
| V19 | Alert pack rules lint/import clean | ✓ |  | #376 |

### Flow & tenancy

| # | Test | Rust | Py | Ref |
| --- | --- | --- | --- | --- |
| V20 | Per-key Tier 1: worker-local cap holds exactly; fleet approximation matches documented formula | ✓ | ✓ | #340 |
| V21 | Per-key fairness under Zipf-skewed tenants (Jain index bound, no starvation) | ✓ |  | #340 |
| V22 | A→B promotion is transactional with parent finalize; `on_parent_failure` policies (TLA+ witness + integration) | ✓ | ✓ | #14 |
| V23 | Backpressure: `Signal` surfaces pressure, `Reject` returns typed error, transactional enqueue unaffected by default | ✓ | ✓ | #341 |
| V24 | SQL contract conformance script green; BLAKE3 unique-key + shard-hash cross-language vectors | ✓ | ✓ | #342 |

### Storage & performance (gate evidence, benchmark harness)

| # | Test | Ref |
| --- | --- | --- |
| V25 | 0.6 pinned-MVCC long-horizon shape still passes on 0.7 main (798/s through 60-min pin, bounded depth, full reclaim) | #383 Gate 2 |
| V26 | #246 shape: ≤5% rescue-ON overhead at 1×256, or named mechanism + mitigation | #246 / E2 |
| V27 | Ring-state dead tuples reduced to noise on the long-horizon idle phase | #371 |
| V28 | Partitioned-queue preset ≥9k jobs/s e2e on the reference 24-CPU harness | #383 Gate 2 |
| V29 | Gate A evidence: allocator bake-off (E1) + WAL/job decomposition (E3) run ids recorded on #295 | #295 |

## Running Tests

CI runs the Rust suite as a sharded matrix (`scripts/ci-test-shard.sh`): the
migration replay binary is partitioned per-test with `cargo-nextest`, the
other slow binaries form a `heavy` shard, and every remaining test target —
including newly added files, automatically — runs in `rest`. Locally,
`cargo test --workspace` remains the equivalent single command; individual
shards can be reproduced with e.g. `./scripts/ci-test-shard.sh heavy`.


```bash
# Start Postgres
docker run -d --name awa-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=awa_test -p 15432:5432 postgres:18-alpine

# Rust tests (unit + integration + scale + validation)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --workspace

# Python tests
cd awa-python && .venv/bin/pytest tests/ -v

# Chaos recovery only (same test CI runs as a dedicated step)
cd awa-python && .venv/bin/pytest tests/test_chaos_recovery.py -v -m chaos

# Nightly chaos + benchmark lane
# GitHub Actions: .github/workflows/nightly-chaos.yml

# Rust chaos suite (mixed workload soak, sustained node-failure soak, leader failover, leader connection loss, mixed Rust/Python fleet, transient DB disconnect recovery)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_PYTHON_BIN="$PWD/awa-python/.venv/bin/python" \
  cargo test --package awa --test chaos_suite_test \
  -- --ignored --test-threads=1 --nocapture

# Postgres hot-standby promotion smoke (boots primary + replica + stable proxy endpoint via Docker Compose)
cargo test --package awa --test postgres_failover_smoke_test -- --ignored --nocapture

# COPY integration tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test copy_test -- --nocapture

# Queue-storage COPY producer tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa-model --test queue_storage_copy_test -- --nocapture

# Python sync tests
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test .venv/bin/pytest tests/test_sync.py -v

# Benchmark tests (throughput + latency, requires Postgres)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test -- --ignored --nocapture

# COPY benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test test_throughput_copy_insert -- --ignored --nocapture

# Failure-mode benchmark matrix (Rust)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test failure_benchmark_test test_failure_bench_full_matrix -- --exact --ignored --nocapture

# Stale-heartbeat rescue benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test failure_benchmark_test test_failure_bench_stale_heartbeat_rescue -- --exact --ignored --nocapture

# MVCC horizon overlap benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --release --package awa --test scheduling_benchmark_test test_mvcc_horizon_overlap_benchmark -- --exact --ignored --nocapture

# Python failure-mode benchmarks
cd awa-python && PYTHONPATH=scripts DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run python scripts/benchmark_runtime.py --scenario failures

# Sequential callback tests (Rust)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test external_wait_test -- --test-threads=1 --nocapture

# Sequential callback tests (Python)
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/test_sequential_callbacks.py -v

# TLA+ correctness models (requires Docker)
./correctness/run-tlc.sh core/AwaCore.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageInterleavings.cfg
./correctness/run-tlc.sh protocol/AwaExtended.tla
./correctness/run-tlc.sh core/AwaBatcher.tla
./correctness/run-tlc.sh core/AwaBatcher.tla core/AwaBatcherLiveness.cfg
./correctness/run-tlc.sh races/AwaCbk.tla
./correctness/run-tlc.sh races/AwaCbk.tla races/AwaCbkLiveness.cfg
./correctness/run-tlc.sh races/AwaCron.tla races/AwaCronLiveness.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimOld.cfg
./correctness/run-tlc.sh races/AwaDispatchClaim.tla races/AwaDispatchClaimNew.cfg
./correctness/run-tlc.sh races/AwaViewTrigger.tla
./correctness/run-tlc.sh races/AwaViewTrigger.tla races/AwaViewTriggerOld.cfg

# Concurrent multi-queue lifecycle benchmarks
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --release --package awa --test concurrent_lifecycle_test -- --ignored --nocapture

# tokio-postgres bridge tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa-model --test bridge_tokio_pg_test -- --ignored --nocapture

# Bug fix tests (state guard, unique conflict field)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test executor_guard_test -- --nocapture

# Rate limiting tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test rate_limit_test -- --nocapture

# Weighted concurrency tests
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test weighted_test -- --nocapture

# Python start() config validation tests
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/test_start_config.py -v

# Progress tests (Rust)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test progress_test -- --test-threads=1 --nocapture

# Progress tests (Python)
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/test_progress.py -v

# Telemetry OTLP integration test (requires otel-lgtm + postgres)
# Start an OTLP collector (receives metrics via gRPC, exposes Prometheus API):
docker run -d --name otel-lgtm -p 4317:4317 -p 9090:9090 grafana/otel-lgtm:0.22.0
# Wait for ready:
until curl -sf http://localhost:9090/api/v1/status/runtimeinfo; do sleep 2; done
# Run the test:
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  cargo test -p awa --test telemetry_test -- --ignored --nocapture
# Cleanup:
docker rm -f otel-lgtm

# Repeat 20 times to detect flakes
for i in $(seq 1 20); do echo "=== Run $i ===" && cargo test --workspace 2>&1 | tail -1; done
```

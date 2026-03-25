# AWA — Validation Test Plan

Run after Phase 2 (Rust core + Python client complete). Pass all = ship.

Tests run against real Postgres 15+ (not managed services). Dedicated test database.
All tests are automated and run in CI.

See [the full test plan](../prd.md) for detailed descriptions of each test case.

## Test Matrix

| # | Test | Category | Status |
|---|---|---|---|
| T1 | No duplicate processing (100k jobs) | Correctness | Implemented |
| T4 | `kill -9` crash recovery | Crash recovery | Implemented |
| T5 | Deadline rescue (hung job) | Crash recovery | Implemented |
| T6 | Transactional atomicity (Rust) | Correctness | Implemented |
| T7 | Transactional atomicity (Python) | Correctness | Implemented |
| T8 | Uniqueness under contention | Correctness | Implemented |
| T9 | Hash cross-language consistency | Correctness | Implemented |
| T10 | Priority ordering | Correctness | Implemented |
| T12 | Queue isolation | Isolation | Implemented |
| T13 | Queue pause/resume | Admin | Implemented |
| T18 | Backoff timing | Correctness | Implemented |
| T19 | Snooze semantics | Correctness | Implemented |
| T20 | Terminal error semantics | Correctness | Implemented |
| T21 | Deserialization failure | Correctness | Implemented |
| T22 | Pool exhaustion resilience | Resilience | Implemented |
| T26 | Migration idempotency | Migration | Implemented |
| T27 | Admin ops under load | Admin | Implemented |
| T28 | Tracing spans emitted on job execution | Observability | Implemented |
| T29 | OTel metrics emitted (completed, duration, in_flight) | Observability | Implemented |
| T30 | OTel failure metrics emitted | Observability | Implemented |
| T31 | Throughput >= 3,000 jobs/sec (Rust workers, debug build) | Benchmark | Implemented |
| T32 | Pickup latency p50 < 50ms (LISTEN/NOTIFY) | Benchmark | Implemented |
| T33 | Insert throughput >= 10,000 inserts/sec | Benchmark | Implemented |
| T34 | V2 migration creates `awa.cron_jobs` table | Migration | Implemented |
| T35 | UPSERT sync: inserts new, updates changed, does NOT delete others | Cron | Implemented |
| T36 | Atomic CTE: mark + insert succeeds, returns job row | Cron | Implemented |
| T37 | Atomic CTE: second call returns 0 rows (dedup) | Cron | Implemented |
| T38 | Multi-deployment: disjoint schedules coexist (no orphan deletion) | Cron | Implemented |
| T39 | No backfill: only latest missed fire enqueued | Cron | Implemented |
| T40 | End-to-end: register periodic + start → job appears with cron metadata | Cron | Implemented |
| T41 | Tags and metadata propagate from schedule to enqueued job | Cron | Implemented |
| T42 | Cron expression validation at build time | Cron (unit) | Implemented |
| T43 | Timezone validation at build time | Cron (unit) | Implemented |
| T44 | DST spring-forward: at most one fire | Cron (unit) | Implemented |
| T45 | DST fall-back: exactly one fire | Cron (unit) | Implemented |
| T46 | First registration (NULL last_enqueued_at): enqueues most recent past fire | Cron (unit) | Implemented |
| T47 | COPY: empty input returns empty vec | COPY | Implemented |
| T48 | COPY: single job matches single insert | COPY | Implemented |
| T49 | COPY: 1000 jobs all inserted with correct kind/queue/state | COPY | Implemented |
| T50 | COPY: args with special chars (JSON quotes, newlines, commas, tabs, backslashes, unicode) round-trip | COPY | Implemented |
| T51 | COPY: tags with pathological values (commas, quotes, braces, backslashes, whitespace, NULL, empty) round-trip | COPY | Implemented |
| T52 | COPY: unique constraint handled via DO NOTHING | COPY | Implemented |
| T53 | COPY: mixed run_at (NULL=available, future=scheduled) | COPY | Implemented |
| T54 | COPY: metadata with special chars round-trips | COPY | Implemented |
| T55 | COPY: atomic (transaction rollback discards all) | COPY | Implemented |
| T56 | COPY: within caller-managed transaction | COPY | Implemented |
| T57 | COPY benchmark: 10K jobs vs chunked INSERT | COPY (bench) | Implemented |
| T57a | COPY: multiple calls within one caller-managed transaction reuse staging safely | COPY | Implemented |
| T58 | Python: insert_sync returns Job directly | Sync | Implemented |
| T59 | Python: migrate_sync idempotent | Sync | Implemented |
| T60 | Python: cancel_sync / retry_sync | Sync | Implemented |
| T61 | Python: retry_failed_sync | Sync | Implemented |
| T62 | Python: discard_failed_sync | Sync | Implemented |
| T63 | Python: pause/resume/drain_queue_sync | Sync | Implemented |
| T64 | Python: list_jobs_sync with filters | Sync | Implemented |
| T65 | Python: queue_stats_sync | Sync | Implemented |
| T66 | Python: health_check_sync | Sync | Implemented |
| T67 | Python: transaction_sync context manager commit | Sync | Implemented |
| T68 | Python: transaction_sync context manager rollback on exception | Sync | Implemented |
| T69 | Python: sync methods work from non-async context | Sync | Implemented |
| T70 | Python: insert_many_copy_sync | Sync | Implemented |
| T71 | Mixed Rust/Python workers share the same queue correctly | Cross-language resilience | Implemented |
| T72 | Runtime recovers after terminating Postgres worker backends | Resilience | Implemented |
| T73 | Sustained mixed workload survives Python node death and Rust node reconnect under load | Resilience | Implemented |
| T74 | Hot-standby promotion via stable endpoint: reconnect, insert, and scheduled promotion still work after cutover | HA failover | Implemented |
| B1 | Late completion after rescue is no-op (state guard) | Bug fix | Implemented |
| B2 | Late completion after cancel is no-op (state guard) | Bug fix | Implemented |
| B3 | Shutdown waits for in-flight jobs | Bug fix | Implemented |
| B4 | Heartbeat alive during shutdown drain | Bug fix | Implemented |
| B5 | Deadline rescue signals ctx.is_cancelled() | Bug fix | Implemented |
| B6 | UniqueConflict.constraint field has constraint name | Bug fix | Implemented |
| RL1 | No rate limit — fast dispatch | Rate limit | Implemented |
| RL2 | Rate limit throttles dispatch (10/sec, 30 jobs) | Rate limit | Implemented |
| RL3 | Burst 20, rate 5/sec — burst then throttle | Rate limit | Implemented |
| RL4 | Rate limit + low max_workers — concurrency is bottleneck | Rate limit | Implemented |
| RL5 | Invalid rate limit (max_rate <= 0) rejected | Rate limit | Implemented |
| RL6 | Zero weight rejected | Rate limit | Implemented |
| W5 | Hard-reserved backward compat | Weighted | Implemented |
| W6 | Idle overflow to loaded queue | Weighted | Implemented |
| W7 | Floor guarantee under load (min_workers) | Weighted | Implemented |
| W8 | Global cap not exceeded (global_max_workers) | Weighted | Implemented |
| W9 | min_workers sum > global rejected (BuildError) | Weighted | Implemented |
| W10 | Weight proportionality (3:1 ratio) | Weighted | Implemented |
| W11 | Permit-before-claim — no orphan running jobs | Weighted | Implemented |
| W12 | Health check reports weighted capacity | Weighted | Implemented |
| W13 | Health check reports hard-reserved capacity | Weighted | Implemented |
| P12 | Python: Dict config with rate_limit starts | Python config | Implemented |
| P13 | Python: global_max_workers weighted mode | Python config | Implemented |
| P14 | Python: Backward compat tuple form | Python config | Implemented |
| P15 | Python: tuple + global_max_workers raises | Python config | Implemented |
| P16 | Python: both max_workers and min_workers raises | Python config | Implemented |
| PR1 | set_progress + flush → percent persisted | Progress | Implemented |
| PR2 | update_metadata shallow-merge | Progress | Implemented |
| PR3 | Multiple set_progress → only last value | Progress | Implemented |
| PR4 | flush_progress immediate DB write (verified inside handler) | Progress | Implemented |
| PR5 | Progress survives rescue (stale heartbeat) | Progress | Implemented |
| PR6 | Completed job → progress = NULL | Progress | Implemented |
| PR7 | RetryAfter → next attempt sees previous progress | Progress | Implemented |
| PR8 | No progress set → no overhead | Progress | Implemented |
| PR9 | set_progress(101) → clamped to 100 (verified via DB) | Progress | Implemented |
| PR10 | WaitForCallback preserves progress | Progress | Implemented |
| PR11 | complete_external clears progress | Progress | Implemented |
| PR12 | fail_external preserves progress | Progress | Implemented |
| PR13 | Callback timeout rescue preserves progress | Progress | Implemented |
| PR14 | Terminal failure preserves progress | Progress | Implemented |
| PR15 | Cancel preserves progress | Progress | Implemented |
| PR16 | Full lifecycle (real Client): complete clears, retry preserves checkpoint | Progress | Implemented |
| PP1 | Python: set_progress from handler persists after flush | Progress (Py) | Implemented |
| PP2 | Python: update_metadata shallow-merges into progress.metadata | Progress (Py) | Implemented |
| PP3 | Python: flush_progress immediate DB write | Progress (Py) | Implemented |
| PP4 | Python: job.progress property returns dict during execution | Progress (Py) | Implemented |
| PP5 | Python: progress persists across retry (checkpoint) | Progress (Py) | Implemented |
| OT1 | OTLP export: awa.job.completed reaches collector | Telemetry (E2E) | Implemented |
| OT2 | OTLP export: awa.job.claimed reaches collector | Telemetry (E2E) | Implemented |
| OT3 | OTLP export: awa.dispatch.claim_batches reaches collector | Telemetry (E2E) | Implemented |
| OT4 | OTLP export: awa.job.duration histogram reaches collector | Telemetry (E2E) | Implemented |

## Running Tests

```bash
# Start Postgres
docker run -d --name awa-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=awa_test -p 15432:5432 postgres:17-alpine

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

# Python sync tests
cd awa-python && DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test .venv/bin/pytest tests/test_sync.py -v

# Benchmark tests (throughput + latency, requires Postgres)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test -- --ignored --nocapture

# COPY benchmark
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test test_throughput_copy_insert -- --ignored --nocapture

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

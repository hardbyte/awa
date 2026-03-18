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

# Benchmark tests (throughput + latency, requires Postgres)
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test benchmark_test -- --ignored --nocapture

# Repeat 20 times to detect flakes
for i in $(seq 1 20); do echo "=== Run $i ===" && cargo test --workspace 2>&1 | tail -1; done
```

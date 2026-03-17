# AWA — Validation Test Plan

Run after Phase 2 (Rust core + Python client complete). Pass all = ship v0.1.

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

# Repeat 20 times to detect flakes
for i in $(seq 1 20); do echo "=== Run $i ===" && cargo test --workspace 2>&1 | tail -1; done
```

# Portable Cross-System Benchmarks

Comparable benchmark scenarios for Awa (native Rust and Docker), Awa-Python,
Procrastinate (Python), River (Go), and Oban (Elixir) running against a shared
Postgres instance.

## Prerequisites

- Docker and Docker Compose
- Rust toolchain (for the Awa adapter)
- No Go or Elixir installation required — River and Oban build inside Docker

## Quick Start

```bash
# Start Postgres, build all adapters, run all scenarios, stop Postgres
uv run python benchmarks/portable/run.py

# Run specific systems
uv run python benchmarks/portable/run.py --systems awa,awa-docker,awa-python,procrastinate,river

# Run a single scenario
uv run python benchmarks/portable/run.py --scenario worker_throughput --job-count 50000 --worker-count 200

# Skip rebuild (use cached images/binaries)
uv run python benchmarks/portable/run.py --skip-build

# Keep Postgres running after benchmarks (for debugging)
uv run python benchmarks/portable/run.py --keep-pg

# Run isolated 3x repetitions at the higher-throughput scale point
uv run python benchmarks/portable/isolated.py --skip-build

# Run the benchmark suite plus the portable chaos suite per system in isolation
uv run python benchmarks/portable/full_suite.py --skip-build

# Run the same harness against Postgres 18
uv run python benchmarks/portable/run.py --pg-image postgres:18-alpine
```

## Scenarios

### enqueue_throughput

Insert N jobs as fast as possible. Measures bulk insert speed.

- Awa uses `insert_many_copy_from_pool` (COPY protocol)
- River uses `InsertManyFast` (COPY protocol)
- Oban uses `Oban.insert_all` (Ecto changesets)

### worker_throughput

Pre-enqueue N no-op jobs, start workers, measure time to drain the queue.
Exercises the full dispatch-execute-complete cycle.

### pickup_latency

Enqueue one job at a time to an idle queue with workers running. Measures
the time from insert to completion — reflects LISTEN/NOTIFY responsiveness
and dispatch poll interval.

## Architecture

```
benchmarks/portable/
├── run.py                 # Orchestrator — builds, runs, collects results
├── isolated.py            # Repeats one-system-per-run isolated benchmarks
├── docker-compose.yml     # Shared Postgres service (PG17 by default, PG18 via --pg-image)
├── init-databases.sql     # Creates awa_bench, awa_docker_bench, awa_python_bench, procrastinate_bench, river_bench, oban_bench
├── awa-bench/             # Rust binary (built locally or in Docker from workspace)
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/main.rs
├── awa-python-bench/      # Python runtime variant (Docker)
│   ├── Dockerfile
│   └── main.py
├── procrastinate-bench/   # Python Procrastinate adapter (Docker)
│   ├── Dockerfile
│   ├── main.py
│   └── pyproject.toml
├── river-bench/           # Go binary (built in Docker)
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
├── oban-bench/            # Elixir app (built in Docker)
│   ├── Dockerfile
│   ├── mix.exs
│   ├── config/
│   ├── lib/
│   └── priv/repo/migrations/
└── results/               # JSON output from benchmark runs
```

Each adapter:
- Accepts configuration via environment variables (`DATABASE_URL`, `SCENARIO`,
  `JOB_COUNT`, `WORKER_COUNT`, `LATENCY_ITERATIONS`)
- Outputs JSON results to stdout, logs to stderr
- Manages its own schema migration and cleanup

`awa` runs natively from the local workspace. `awa-docker`, `awa-python`,
`procrastinate`, River, and Oban run in Docker containers with `--network host`
to connect to the shared Postgres.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `--scenario` | `all` | `enqueue_throughput`, `worker_throughput`, `pickup_latency`, or `all` |
| `--job-count` | `10000` | Number of jobs per scenario |
| `--worker-count` | `50` | Concurrent workers |
| `--latency-iterations` | `100` | Iterations for pickup latency test |
| `--systems` | `awa,awa-docker,awa-python,procrastinate,river,oban` | Comma-separated list of systems to run |
| `--pg-image` | `postgres:17-alpine` | Docker image for the shared Postgres service, e.g. `postgres:18-alpine` |

## Fairness Constraints

- Same Postgres version and configuration for all systems
- Same job count, batch size, and worker concurrency
- Same result schema (JSON with jobs_per_sec, duration_ms, latency percentiles)
- Each system uses its own database to avoid schema conflicts
- `awa`, `awa-docker`, `awa-python`, and `procrastinate` use separate databases
  so variant comparisons do not inherit warmed tables or queue metadata from the prior run
- Aligned poll intervals: all systems use 50ms poll/fetch interval
- Aligned rescue intent: Awa uses 15s heartbeat staleness, Oban uses 15s
  rescue-after, and River uses a rescue timeout above the 30s chaos job
  duration so healthy jobs are not falsely rescued
- Awa reuses one DB session across COPY batches so its temp-table reuse
  optimization is exercised in the portable harness
- Pickup latency uses each system's normal single-job insert API rather than a
  batch insert helper
- Chaos enqueue via direct SQL INSERT — all three systems have INSERT triggers
  that fire NOTIFY, so workers discover jobs at the same speed
- River uses the upstream `rivermigrate` package so its benchmark schema tracks
  the real River release instead of a local approximation
- The comparable chaos subset excludes feature-specific probes like priority
  aging and synthetic retry-promotion shortcuts; those remain available via the
  `extended` suite for diagnostics rather than apples-to-apples comparisons

## Chaos / Correctness Scenarios

```bash
# SIGKILL recovery: kill worker mid-flight, measure rescue time
uv run python benchmarks/portable/chaos.py --scenario crash_recovery

# Postgres restart: restart PG with jobs in flight
uv run python benchmarks/portable/chaos.py --scenario postgres_restart

# Repeated kills: 3 kill cycles, verify all jobs eventually complete
uv run python benchmarks/portable/chaos.py --scenario repeated_kills --job-count 20

# All chaos scenarios
uv run python benchmarks/portable/chaos.py --scenario all

# Comparable cross-system chaos subset
uv run python benchmarks/portable/chaos.py --suite portable

# Benchmark suite + portable chaos suite, one isolated run per system
uv run python benchmarks/portable/full_suite.py --skip-build

# Same, but repeated 5x and with CSV/Markdown summaries emitted beside the JSON
uv run python benchmarks/portable/full_suite.py --repetitions 5 --skip-build
```

### pg_backend_kill

Kill Postgres backend connections (not the server) using
`pg_terminate_backend()` while jobs are in flight. Simulates the OOM killer
targeting individual Postgres backends. Verifies pool reconnection and
job completion.

### leader_failover

Run two worker instances for each system. Kill one (likely the leader).
Verify the second takes over maintenance duties (rescue, promotion), all
jobs complete, and no duplicate completions occur.

### retry_storm

Insert 500 jobs directly as `retryable` (simulating mass failure). Measures
time for each system's promotion pipeline to move them back to `available`
and workers to complete them. Tests thundering-herd promotion pressure.

### pool_exhaustion

Start workers with a very small connection pool (5 connections) but many
workers (50). Verifies jobs still complete, heartbeats still fire, and no
false rescues occur from pool starvation.

### priority_starvation

Enqueue low-priority (4) jobs, then continuously enqueue high-priority (1)
jobs at a rate exceeding worker capacity. Tests whether each system's
priority mechanism prevents indefinite starvation of low-priority work.

Awa has maintenance-based priority aging (ADR-005); River and Oban use
strict priority ordering with no aging.

### crash_recovery

Start workers, enqueue jobs (30s sleep each), wait until all are running,
SIGKILL the worker, start a replacement. Measures time from kill to all jobs
completed, and verifies zero job loss.

Each system is configured with short rescue intervals:
- Awa: heartbeat staleness 15s, rescue poll 5s
- River: `RescueStuckJobsAfter` 60s for the 30s chaos jobs, so crash recovery
  is measured without false-rescuing healthy long-running work
- Oban: Lifeline `rescue_after` 15s

### postgres_restart

Start workers with jobs in flight, restart the Postgres container, verify
workers reconnect and all jobs complete with zero loss.

### repeated_kills

Enqueue N jobs, repeatedly SIGKILL and restart workers (3 cycles), then
let a final worker finish. Verifies all N jobs eventually complete despite
repeated crashes.

## Result Schema

```json
{
  "system": "awa",
  "scenario": "worker_throughput",
  "config": { "job_count": 50000, "worker_count": 200 },
  "results": { "duration_ms": 12766, "jobs_per_sec": 3916 }
}
```

Results are saved to `results/results_<timestamp>.json` with full configuration
metadata.

`full_suite.py` also writes:
- `benchmark_summary_<timestamp>.md`
- `benchmark_summary_<timestamp>.csv`
- `chaos_summary_<timestamp>.md`
- `chaos_summary_<timestamp>.csv`
- `full_suite_<timestamp>.log`
- `full_suite_<timestamp>.status.json`

The status file is updated during execution so long runs can be inspected even
if the terminal session is interrupted.

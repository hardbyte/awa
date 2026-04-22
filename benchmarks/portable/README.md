# Portable Cross-System Benchmarks

Comparable benchmark scenarios for Awa (native Rust and Docker), Awa-Python,
Procrastinate (Python), River (Go), Oban (Elixir), PgQue (SQL/PL-pgSQL; the
adapter is a thin Python driver around the native database API),
PGMQ, and pg-boss (Node.js) running against a shared Postgres instance.

## Prerequisites

- Docker and Docker Compose
- Rust toolchain (for the Awa adapter)
- No Go or Elixir installation required — River and Oban build inside Docker
- Initialize git submodules so the PgQue adapter can install its SQL:
  `git submodule update --init --recursive`

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
├── init-databases.sql     # Creates awa_bench, awa_docker_bench, awa_python_bench, procrastinate_bench, river_bench, oban_bench, pgque_bench, pgmq_bench, pgboss_bench
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
├── pgque-bench/           # PgQue (SQL/PL-pgSQL) adapter (Docker, Python driver)
│   ├── Dockerfile
│   ├── adapter.json
│   ├── main.py
│   ├── pyproject.toml
│   └── vendor/pgque/      # Git submodule pinned to upstream NikolayS/pgque
├── pgmq-bench/            # PGMQ adapter (Docker, Python driver)
│   ├── Dockerfile
│   ├── adapter.json
│   ├── main.py
│   └── pyproject.toml
├── pgboss-bench/          # pg-boss adapter (Docker, Node.js)
│   ├── Dockerfile
│   ├── adapter.json
│   ├── main.js
│   └── package.json
└── results/               # JSON output from benchmark runs
```

Each adapter:
- Accepts configuration via environment variables (`DATABASE_URL`, `SCENARIO`,
  `JOB_COUNT`, `WORKER_COUNT`, `LATENCY_ITERATIONS`)
- Outputs JSON results to stdout, logs to stderr
- Manages its own schema migration and cleanup

The Awa adapters (`awa` and `awa-python`) now benchmark the queue-storage
engine rather than the legacy canonical worker path. They also accept:

- `QUEUE_STORAGE_SCHEMA` (default `awa_exp`)
- `QUEUE_SLOT_COUNT` / `LEASE_SLOT_COUNT` (defaults `16` / `8`)
- `QUEUE_ROTATE_MS` / `LEASE_ROTATE_MS` (defaults `1000` / `50`)
- `PRIORITY_AGING_MS` (default `60000` for the long-horizon adapter; must be
  greater than `0` on the current branch)
- `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_SERVICE_NAME` / `OTEL_EXPORT_INTERVAL_MS`
  for optional OTLP metrics export from the native `awa-bench` adapter during
  focused profiling runs (for example against Grafana LGTM)
- `PRODUCER_ONLY_INSTANCE_ZERO=1` to make only replica `0` produce in
  long-horizon multi-replica runs. This is useful when you want to model
  "many worker pods, external producers" and separate enqueue-head contention
  from claim/start coordination.

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

## Long-horizon scenario runner

`long_horizon.py` drives multi-hour workloads against each system, collects
Postgres-side and adapter-side telemetry on a shared timebase, and renders
publication-quality plots. Use this to compare awa to peer systems on slow
failure modes (idle-in-tx bloat, sustained high load, soak drift) that the
short-horizon suite above cannot see.

```bash
# Named scenario (idle_in_tx_saturation ≈ 2h40m; long_horizon ≈ 6h10m):
uv run python benchmarks/portable/long_horizon.py --scenario idle_in_tx_saturation

# Event/message delivery comparison profile:
uv run python benchmarks/portable/long_horizon.py \
    --scenario event_delivery_matrix \
    --systems awa,pgque,pgboss,pgmq \
    --pg-image ghcr.io/pgmq/pg18-pgmq:v1.10.0

# Custom phases (label=type:duration):
uv run python benchmarks/portable/long_horizon.py \
    --phase warmup=warmup:10m \
    --phase clean_1=clean:60m \
    --phase idle_1=idle-in-tx:60m \
    --phase recovery_1=recovery:30m \
    --systems awa,river

# Developer fast path — single PG, DB-only recreation, short phases:
uv run python benchmarks/portable/long_horizon.py \
    --scenario idle_in_tx_saturation --fast
```

Phase types: `warmup`, `clean`, `idle-in-tx`, `recovery`, `active-readers`,
`high-load`. `warmup` samples are kept in `raw.csv` but excluded from
`summary.json` and hidden by default in the interactive report. New phase
types plug in via `bench_harness.phases` + `bench_harness.hooks`.

### Recommended comparison scenarios

- `event_delivery_matrix`
  - Balanced event/message-queue comparison.
  - Sequence: steady-state, active subscriber/read pressure, bursty offered
    load, then clean tail.
  - Best default when comparing throughput, producer latency, subscriber
    latency, end-to-end latency, and dead tuples together.

- `event_delivery_burst`
  - Backlog-growth and catch-up profile.
  - Best when you want to see queue depth, end-to-end delivery inflation, and
    recovery behaviour after sustained oversupply.

- `fleet_steady_state`
  - Multi-replica steady-state profile.
  - Intended for `--replicas >= 2` so systems are compared under real replica
    contention instead of only single-worker-process pressure.

- `crash_recovery_under_load`
  - Multi-replica worker-crash profile under sustained backlog pressure.
  - Best when you want to track rescue/recovery behaviour without dropping out
    of the phase-driven report/summary flow.

### Scaling and semantics helpers

```bash
# Sweep the same long-horizon profile across worker counts.
uv run python benchmarks/portable/worker_scale.py \
  --systems awa,pgque \
  --worker-counts 1,4,8,16,32,64

# Awa-only retry / priority semantics run.
uv run python benchmarks/portable/awa_semantics.py \
  --scenario retry_priority_mix

# Awa-only crash recovery under load (2 replicas).
uv run python benchmarks/portable/awa_semantics.py \
  --scenario crash_recovery_under_load

# Override with explicit short phases when you want a quicker dev loop.
uv run python benchmarks/portable/awa_semantics.py \
  --scenario retry_priority_mix \
  --phase warmup=warmup:5s \
  --phase clean_1=clean:10s \
  --phase pressure_1=high-load:15s \
  --phase recovery_1=clean:10s
```

`worker_scale.py` answers throughput/latency vs worker-count directly instead
of relying on one fixed `WORKER_COUNT` long-horizon run. By default it uses a
short `warmup -> clean -> high-load -> recovery` profile so the sweep answers
scaling under load, not active-reader interference. Use `--scenario` or
repeated `--phase` flags when you want a different shape. It also defaults to
the harness `--fast` mode so repeated sweeps reuse one Postgres instance while
still recreating benchmark databases between runs; pass `--no-fast` for full
cold-start runs.

`awa_semantics.py` is intentionally Awa-only for now. It exercises retryable
failures, mixed priorities, and crash recovery using the same phase-driven
runner, but it does not pretend those semantics are apples-to-apples across
systems like `pgque` or `pgmq` that are not full job queues.
Like `worker_scale.py`, it defaults to harness `--fast` mode for shorter,
more reliable iteration loops; pass `--no-fast` when you specifically want
full container restart behaviour.

The `retry_priority_mix` scenario now sets an explicit `PRIORITY_AGING_MS`
override so the benchmark measures Awa's priority-aging behavior instead of a
pure starvation case. Its adapter metrics also distinguish:
- current effective priority completion rates
- original enqueue priority completion rates
- `aged_completion_rate` for work that only completed after aging promoted it

The shipped scenario lengths are meant to be tractable on a developer machine.
For deeper pressure studies, extend them with explicit `--phase` overrides
rather than treating the defaults as "long enough" by definition.

### Outputs

Per run: `benchmarks/portable/results/<scenario>-<timestamp>-<id>/`
- `index.html` — self-contained interactive report with a two-metric timeline
  explorer and sample-quality / outlier panel
- `raw.csv` — tidy long-form, one row per (system, subject, metric, sample)
- `summary.json` — per-system per-phase aggregates + recovery metrics
- `manifest.json` — PG version, config, host info, adapter versions, CLI args
- `plots/` — `dead_tuples`, `dead_tuples_faceted`, `claim_p99`, `throughput`,
  `producer_p99`, `producer_call_p99`, `subscriber_p99`, `end_to_end_p99`, `table_size`,
  `queue_depth` (PNG 300 DPI + SVG)

For event-delivery scenarios the report now splits producer latency into:
- `producer_p99`: effective per-message enqueue latency
- `producer_call_p99`: producer call / batch-commit latency

Single-row systems report the same value for both. Awa's queue-storage adapter
microbatches producer writes by default in the long-horizon runner, so the two
views make the producer trade-off visible without conflating per-message cost
with batch commit cost.

### Default system matrix differs from the steady-state suite

| System | Steady-state suite (`run.py`) | Long-horizon runner (`long_horizon.py`) |
|---|:---:|:---:|
| awa (native) | ✓ | ✓ |
| awa-docker | ✓ | opt-in only |
| awa-python | ✓ | ✓ |
| procrastinate | ✓ | ✓ |
| river | ✓ | ✓ |
| oban | ✓ | ✓ |
| pgque | — | ✓ |
| pgmq | — | opt-in only |
| pgboss | — | ✓ |

`awa-docker` is excluded by default from the long-horizon runner because its
line on any multi-hour dead-tuple / latency plot is the awa-native line —
same Rust binary, same SQL, same DB-observable behaviour. Keeping it in the
default would double the runtime and clutter plots with an overlapping
series. Run it explicitly via `--systems awa-docker` if you're validating
Docker packaging under long-horizon pressure.

`pgmq` is also opt-in because it requires a pgmq-enabled Postgres image rather
than the stock pinned `postgres:17.2-alpine` image. Use:

```bash
uv run python benchmarks/portable/long_horizon.py \
  --systems pgmq \
  --pg-image ghcr.io/pgmq/pg18-pgmq:v1.10.0
```

### Reproducibility

- PG image pinned to a specific minor (`postgres:17.2-alpine` by default).
- `postgres.conf` committed alongside `docker-compose.yml` with explicit
  autovacuum settings (they dominate recovery curves).
- Per-system PG container rebuilt fresh between systems — no warmed shared
  buffers, no lingering autovacuum state, no cross-system competition for
  locks. `--fast` opts into DB-only recreation for developer iteration.
- `manifest.json` captures PG version, every relevant setting, host CPU/RAM,
  Docker version, per-adapter git sha / schema version, and the exact CLI
  used.

### Relationship to the Rust MVCC benches

The long-horizon runner is the **cross-system visualization** track: slower,
multi-system, plot-producing, no regression gates. The awa-only MVCC
benches (`test_mvcc_horizon_overlap_benchmark` nightly,
`test_mvcc_horizon_planetscale_soak` weekly) are the **awa regression
detection** track: fast, precise, Rust harness, hard thresholds on
`overlap_handler_per_s` / `dead_tup_delta`. Both stay. See
`docs/benchmarking.md` for the full split.

### Adding a new system

See [`CONTRIBUTING_ADAPTERS.md`](CONTRIBUTING_ADAPTERS.md) for the full
adapter contract. Summary: ship `adapter.json` declaring `system`,
`db_name`, `event_tables`, `extensions`; add a builder + launcher in
`bench_harness/adapters.py`; implement the JSONL protocol (descriptor +
samples, SIGTERM handling) in your adapter's own language.

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

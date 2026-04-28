# Contributing a new adapter to the long-horizon portable bench

The long-horizon runner is deliberately language-agnostic. An adapter is any
long-running process that exposes a Postgres-backed job queue and speaks the
JSONL protocol documented below. This guide walks through adding a new
system end-to-end, then gives a skeleton per language already represented
in the existing adapters (Rust, Python, Go, Elixir).

## 1. Directory layout

Create a self-contained subdirectory:

```text
benchmarks/portable/<system>-bench/
    adapter.json                # static manifest (authoritative for preflight)
    Dockerfile                  # unless the adapter runs natively on the host
    README.md                   # anything the orchestrator CI won't surface
    <language-specific build files>
```

Pin your language runtime and dependencies the same way the existing
adapters do — the bench answers "how did these systems behave on this
pinned Postgres, on this day", not "how does the bleeding edge of each
behave today."

## 2. Static manifest: `adapter.json`

```json
{
  "system": "<name>",
  "db_name": "<name>_bench",
  "event_tables": ["<schema>.<table>", "..."],
  "event_indexes": ["<schema>.<indexname>", "..."],
  "extensions": ["<ext>", "..."],
  "family": "<optional, defaults to system>",
  "display_name": "<optional, defaults to system>"
}
```

This is the **single source of truth for preflight**. The harness reads it
**before** launching your adapter and:

1. creates the `db_name` database if missing,
2. runs `CREATE EXTENSION IF NOT EXISTS <ext>` for every declared extension
   (plus `pgstattuple`, which the harness always requires),
3. seeds the metrics daemon's poll targets (tables + indexes).

Runtime drift is caught: when your adapter emits its descriptor record on
startup (below), the harness checks the runtime descriptor's
`event_tables` / `extensions` are a **superset** of the static manifest.
Mismatch fails the run loudly.

### 2a. SDK variants of the same system

Some systems ship multiple SDKs over the same on-disk schema (e.g. Awa
has a Rust core, a Rust binary running in Docker, and a Python SDK). Each
variant gets its own `<system>-bench/` directory, its own `system` key
(used as the unique row identifier in `raw.csv`), but shares a `family`
so plots colour them with one base hue and distinguish variants by
linestyle (`-`, `--`, `:`, `-.` cycled in registration order):

```json
// awa-bench/adapter.json
{ "system": "awa",        "family": "awa", "display_name": "Awa (Rust, native)", ... }
// awa-docker-bench/adapter.json
{ "system": "awa-docker", "family": "awa", "display_name": "Awa (Rust, Docker)", ... }
// awa-python-bench/adapter.json
{ "system": "awa-python", "family": "awa", "display_name": "Awa (Python SDK)",  ... }
```

Both fields are optional. Standalone systems can omit `family` (it
defaults to the system name) and `display_name` (defaults to the system
name); they will plot exactly as before. If you only want a friendlier
plot label without grouping, set `display_name` and leave `family`
unset.

## 3. Runtime contract

One long-running process. stdout is telemetry, stderr is logs.

### Env in

The harness sets these before launch. Your adapter reads them:

| Var | Meaning |
|---|---|
| `DATABASE_URL` | per-system database; harness has already created it and run extensions |
| `SCENARIO` | lifecycle mode; new adapters implement at least `long_horizon` |
| `PRODUCER_MODE` | `fixed` (primary) or `depth-target` (diagnostic only) |
| `PRODUCER_RATE` | target jobs/s when `PRODUCER_MODE=fixed` |
| `TARGET_DEPTH` | target queue depth when `PRODUCER_MODE=depth-target` |
| `WORKER_COUNT` | consumer concurrency |
| `JOB_PAYLOAD_BYTES` | rough payload size (default 256) |
| `JOB_WORK_MS` | synthetic job work time (default 1 ms) |
| `SAMPLE_EVERY_S` | emission cadence in seconds (default 5) |

### JSONL out

#### Startup descriptor (one line, at startup)

```json
{"kind":"descriptor","system":"<name>","event_tables":["<schema>.<table>","..."],"extensions":["<ext>","..."],"version":"<sha-or-tag>","schema_version":"<adapter-reported>","db_name":"<name>_bench","started_at":"2026-04-17T09:00:10.000Z"}
```

Not used for preflight — that's the static manifest's job. Used for
forensics (recorded in `manifest.json`) and for drift detection.

#### Samples (every `SAMPLE_EVERY_S`, one record per metric)

```json
{"t":"2026-04-17T09:00:10.000Z","system":"<name>","kind":"adapter","subject_kind":"adapter","subject":"","metric":"claim_p99_ms","value":4.2,"window_s":30}
```

Required metrics, emitted every tick:

| Metric | Meaning | Window |
|---|---|---|
| `claim_p50_ms`, `claim_p95_ms`, `claim_p99_ms` | pickup latency (insert → worker start), rolling 30s | 30s |
| `enqueue_rate` | jobs inserted per second | `SAMPLE_EVERY_S` |
| `completion_rate` | jobs completed per second | `SAMPLE_EVERY_S` |
| `queue_depth` | available jobs not yet claimed | 0 (instantaneous) |

Percentiles must be computed with **bounded memory** (HDR histogram or
t-digest, 3 significant figures). Naïve sort-every-window will not keep up
over multi-hour runs.

Cadence is clock-aligned: align your first tick to the next wall-clock
`SAMPLE_EVERY_S` boundary so cross-system samples line up on the plot
timebase regardless of each adapter's start time within that second.

### Shutdown

On SIGTERM, flush any pending samples and exit 0 within 5 seconds. The
harness sends SIGTERM at each phase boundary is **not true** — it sends
SIGTERM only when all phases are complete or the run is cancelled. During
the run your adapter stays in a steady producer+consumer loop; the
metrics daemon and harness tailer do the per-phase bookkeeping.

## 4. Migrations — use the system's own install path

The harness does **not** maintain migration SQL per adapter. Each adapter
owns its schema evolution: `CREATE EXTENSION pgmq`,
`new PgBoss().start()`, `rivermigrate`, `awa migrate`, `ecto.migrate`, etc.

Adapters that ship Postgres extensions not bundled in the stock image
(`pgmq`, `pgq`, …) need a custom Dockerfile for the PG service that
installs the extension binaries. The harness calls `CREATE EXTENSION`
from the static manifest; your Dockerfile only needs to provide the
binaries.

### 4a. Vendoring SUTs (optional submodule pattern)

The existing adapters all pin their SUT through the language's package
manager (`go.sum`, `uv.lock`, `mix.lock`, `Cargo.lock`) and let the
adapter run the SUT's standard `migrate` entry point at startup. That
keeps the bench reproducible without copying any DDL into this repo.

If your SUT requires bringing schema or migration files into the bench
directory (rare — most projects publish migrations as part of their
distributed package), prefer a git submodule over copy-pasting:

```bash
# Pin the upstream repo as a submodule
git submodule add https://github.com/<org>/<sut>.git \
  benchmarks/portable/<system>-bench/vendor/<sut>

# Use a local path in your manifest of choice:
# Go (go.mod):       replace github.com/<org>/<sut> => ./vendor/<sut>
# Python (pyproject): [tool.uv.sources]  <sut> = { path = "vendor/<sut>" }
# Elixir (mix.exs):   {:<sut>, path: "vendor/<sut>"}
# Rust (Cargo.toml):  <sut> = { path = "vendor/<sut>" }
```

Pin the submodule to a tagged release SHA, never a moving branch. Have
your adapter's startup descriptor include the submodule SHA in its
`version` field (e.g. read it from `git -C vendor/<sut> rev-parse HEAD`
in the Dockerfile and bake it in at build time) so it's captured in
`manifest.json` for every run.

## 5. Registration — four touchpoints

1. `benchmarks/portable/<system>-bench/adapter.json` — the static manifest (above).
2. `benchmarks/portable/init-databases.sql` — `CREATE DATABASE <name>_bench;` (and the stock `CREATE EXTENSION IF NOT EXISTS pgstattuple;` pattern).
3. `benchmarks/portable/bench_harness/adapters.py` — register your system in
   `ADAPTERS` with a builder (how to compile / `docker build`) and a launcher
   (argv + env for `docker run --network host` or native).
4. `benchmarks/portable/README.md` systems table — one new row.

## 6. Testing before opening a PR

```bash
# Standalone run against a throwaway DB
docker compose -f benchmarks/portable/docker-compose.yml up -d postgres
DATABASE_URL=postgres://bench:bench@localhost:15555/<name>_bench \
  SCENARIO=long_horizon PRODUCER_MODE=fixed PRODUCER_RATE=100 \
  WORKER_COUNT=8 SAMPLE_EVERY_S=2 \
  ./your-adapter 2>&1 | head -50

# Integrate with the harness at reduced scale
uv run python benchmarks/portable/long_horizon.py run \
  --phase warmup=warmup:30s \
  --phase clean_1=clean:30s \
  --phase idle_1=idle-in-tx:30s \
  --phase recovery_1=recovery:30s \
  --systems <system> --fast
```

After the run, the PR checklist:

- [ ] Your system's line appears in every generated plot.
- [ ] Declared `event_tables` show up as rows in `raw.csv` with
      `subject=<schema>.<table>`.
- [ ] Your adapter's JSONL samples show up as `subject_kind=adapter` rows
      (`metric` in `{claim_p50_ms, claim_p95_ms, claim_p99_ms, enqueue_rate,
      completion_rate, queue_depth}`).
- [ ] `descriptor` record appears in `manifest.json -> adapters[<system>]`.
- [ ] No drift errors from the manifest-vs-descriptor check.

---

## Skeletons per language

The existing adapters are the reference implementations — treat each as a
working example of the contract in its language:

### Rust
`benchmarks/portable/awa-bench/src/long_horizon.rs` — uses
`hdrhistogram` for bounded-memory percentiles, `tokio::signal` for clean
SIGTERM handling, and aligns the first sample tick to the next wall-clock
`SAMPLE_EVERY_S` boundary via `SystemTime::now()`.

### Python
`benchmarks/portable/awa-python-bench/main.py::scenario_long_horizon` and
`benchmarks/portable/procrastinate-bench/main.py::scenario_long_horizon`
— asyncio producer/sampler/depth tasks, `deque` with a capped `maxlen`
as a bounded latency ring, `loop.add_signal_handler` for SIGTERM.

### Go
`benchmarks/portable/river-bench/main.go::runLongHorizon` — goroutines per
concern (producer / depth poller / sampler), `sync/atomic` for counters,
`signal.Notify` for SIGTERM.

### Elixir
`benchmarks/portable/oban-bench/lib/oban_bench/long_horizon.ex` — three
`spawn_link`'d loops; ETS tables for shared counters and latency ring.
Relies on Docker SIGTERM → BEAM shutdown (stdout is line-buffered, so
emitted samples survive).

Copy the closest skeleton, swap in your system's insert / worker / depth
APIs, and ship.

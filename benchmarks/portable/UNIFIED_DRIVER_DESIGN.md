# Unified phase-driven driver — design notes

Tracking #174. This document is the plan for folding `chaos.py` into
`long_horizon.py`'s phase-driven harness. It lives in-tree so reviewers
can react to direction before implementation commits land, and so the
final design stays discoverable after the refactor.

The target is deletion of `chaos.py` as a standalone driver: one
entry point, one adapter contract, one output format. Scenario types
that today live in `chaos.py` (SIGKILL-and-respawn, Postgres restart,
pool exhaustion, etc.) become phase types on the existing DSL alongside
`warmup`, `clean`, `idle-in-tx`, `active-readers`, and `high-load`.

## 1. Current state summary

**Shared already** (landed in #170):

- Adapter registry (`ADAPTERS`, `AdapterManifest`, `adapter.json`).
- Launcher/builder split with docker vs. native distinction.
- PG container lifecycle.
- Pydantic-validated CLI config.
- `raw.csv` + `summary.json` output pipeline.

**Duplicated by `chaos.py`** (~2010 lines, ~142 per-system refs):

- Per-system SQL templates + state-column dict parallel to `adapter.json`.
- Per-system `start_<system>_worker` and `start_second_worker`
  functions with inline docker argv.
- Per-scenario `if system == "awa": … elif …` ladders inside every
  chaos scenario function.
- Separate scenario-level JSON output format.

**What chaos actually does that long-horizon doesn't:**

- Destructive operations on the adapter process (SIGKILL, then
  relaunch and verify recovery).
- Destructive operations on Postgres (container restart,
  `pg_terminate_backend`, pool exhaustion).
- Pass/fail scenario framing rather than continuous measurement.

All three are expressible as phase types in the existing DSL.

## 2. New phase types

Grouped by target subsystem. Each phase type's name encodes its fault
semantics rather than taking a signal parameter — "`kill-*`" is
SIGKILL, "`stop-*`" is SIGTERM, "`isolate-*`" / "`partition-*`" are
network-level. That way a scenario reads honestly: modelling a
rolling replica restart picks `stop-worker`, modelling a crash picks
`kill-worker`, modelling a split brain picks `isolate-worker`.

### Worker-directed (harsh: SIGKILL)

- **`kill-worker`** — SIGKILL a specific replica of the running
  adapter. Takes an `instance: int` parameter (default `0`). Harness
  records the exact kill timestamp as a phase boundary; the process
  stays down until the phase ends, then the relauncher restarts it.
  Replaces `scenario_crash_recovery`.
- **`repeated-kills`** — same but with a `period_s: float` parameter
  that kills the instance repeatedly across the phase duration.
  Replaces `scenario_repeated_kills`.
- **`kill-leader`** — a multi-replica variant: identifies the current
  leader (via a simple "whoever holds the maintenance advisory lock"
  SQL probe for Awa; per-adapter probe functions for River/Oban) and
  kills it. Replaces `scenario_leader_failover`.

### Worker-directed (graceful: SIGTERM)

- **`stop-worker`** — SIGTERM a specific replica. Waits for graceful
  shutdown within a bounded window before escalating to SIGKILL.
  Models the "rolling replica replace" operator workflow that today
  has no coverage in chaos.py — a proper answer to "does the fleet
  stay correct during a rolling deploy?"
- **`rolling-replace`** — composable macro: `stop-worker(i)` →
  `start-worker(i)` for each `i` in sequence, with configurable
  inter-step dwell. Expressible as a phase sequence rather than a
  new primitive, so it lives as a named `SCENARIOS` entry, not a
  phase type.

### Worker-directed (isolation)

- **`isolate-worker`** — drop network packets between a replica and
  Postgres via `iptables` / `docker network disconnect` for the
  phase duration, then restore. Models a partial-partition scenario
  (replica still alive, can't reach PG). The lease + heartbeat
  semantics should push its in-flight jobs onto other replicas;
  this phase verifies that.

### Postgres-directed

- **`pg-restart`** — `docker restart portable-postgres-1`. Entire
  fleet experiences a PG drop. Replaces `scenario_postgres_restart`.
- **`pg-backend-kill`** — `pg_terminate_backend(...)` against all
  adapter connections. Replaces `scenario_pg_backend_kill`.
- **`pool-exhaustion`** — drop `max_connections` on the running PG
  container for the phase duration. Replaces
  `scenario_pool_exhaustion`; note that existing adapters configure
  pool size per process, so this hits all replicas uniformly.
- **`partition-pg`** — block network traffic to Postgres from *all*
  replicas. Harsher than `pg-backend-kill` (connections can't
  reconnect until restored). Models a PG-side partition rather than
  a restart.

### Postgres-directed

- **`pg-restart`** — `docker restart portable-postgres-1`. Entire
  fleet experiences a PG drop. Replaces `scenario_postgres_restart`.
- **`pg-backend-kill`** — `pg_terminate_backend(...)` against all
  adapter connections. Replaces `scenario_pg_backend_kill`.
- **`pool-exhaustion`** — drop `max_connections` on the running PG
  container for the phase duration. Replaces
  `scenario_pool_exhaustion`; note that existing adapters configure
  pool size per process, so this hits all replicas uniformly.

### Load-directed (phase-composed, not strictly new)

- **`retry-storm`** — insert N jobs in `retryable` state directly
  via SQL. Existing `high-load` phase already simulates load; the
  current `scenario_retry_storm` is a specific kind of direct SQL
  injection that today lives in chaos.py. Reshape it as a
  `setup-sql` hook on a regular `high-load` phase.
- **`priority-starvation`** — similar. Becomes a hook on a
  `high-load` phase that seeds the queue with inversely-skewed
  priorities.

## 3. Launcher / relauncher contract

Biggest change. Today `launch_<system>` starts the adapter exactly
once per run, returns a subprocess handle, the orchestrator tails it
until shutdown.

New shape: the launcher is a factory. The orchestrator owns a
`ReplicaPool` (list of running subprocess handles, one per replica)
and the ability to:

1. **Start** — spawn replica `i`. Returns handle + tailing thread.
2. **Stop** — SIGTERM (graceful) or SIGKILL (destructive). Deregister
   tailer. Per-replica `raw.csv` rows stop landing; a phase-boundary
   marker records which instance exited at what time.
3. **Restart** — stop then start, re-attaching a new tailer. Samples
   from the restarted replica carry the same `instance_id` so the
   time series is continuous from the analysis side.

Each launcher returns a `LaunchSpec` as today; the harness knows how
to produce a distinct `--container-name` suffix per instance so two
Docker replicas don't collide.

## 4. `replicas: int` as a first-class dimension

Referenced in my #174 comment: every scenario — long-horizon steady
state and chaos — runs at `replicas >= 1` (default 1). `LaunchSpec`
exposes it. Every phase type that acts on "the adapter" takes an
optional `instance: int` selector (default 0).

Implications:

- `raw.csv` gains an `instance_id` column. Legacy 1-replica runs
  write `instance_id=0` and behave identically to today's output at
  the plot layer.
- Default `long_horizon.py` run at `replicas=3` becomes a new named
  scenario (`fleet_steady_state` or similar) measuring contention
  amplification that single-replica runs can't see.
- DB name stays per-system, not per-replica. All replicas of an
  adapter share `<system>_bench`; that's the production shape.

## 5. Output: one `raw.csv`, pass/fail derived

From #174:

> pass/fail-style chaos answers are computed from the time series
> (for example "recovery time" is
> `time-of-first-completed-sample minus time-of-kill-phase-boundary`)

Concrete mapping of existing chaos stats → time-series derivations:

| Today's field | Derived from |
|---|---|
| `successes / runs` | Set at scenario-suite level; in the unified driver it becomes "did the phase sequence complete without the adapter dropping out" — already capturable in `summary.json`. |
| `jobs_lost` | `n_completed_samples_before_kill − n_completed_samples_after_final_phase` (with job-id accounting via the adapter's descriptor). |
| `max_jobs_lost` | Max across replicas in multi-replica runs. |
| `mean_total_time_secs` | `phase_end − phase_start` for the destructive phase. Already in `summary.json`. |
| `mean_rescue_time_secs` | `first-completion-sample-after-kill − kill-boundary`. |
| `low_priority_starved` | Samples in the high-load phase where low-priority depth is monotonically non-decreasing. Derivable from `raw.csv` without a new field. |

`summary.json` gains a `scenario_outcomes: dict[str, dict]` block
where each phase sequence's derived pass/fail metrics land under its
name. Readers of today's `chaos_summary_*.csv` get the same columns;
their source is the unified time series, not a scenario-specific
emitter.

## 6. Migration plan (order of commits)

Sized so each commit compiles, passes tests, and is useful on its
own.

1. **Design doc** (this file). Lets reviewers react before code moves.
1a. **Subcommand-shaped CLI surface.** *Landed.* `long_horizon.py`
   becomes the umbrella entry point with two subcommands: `run`
   (drive a benchmark, the existing flow) and `combine` (merge
   already-completed single-system runs into one report). When
   `chaos.py` folds in below, its scenarios become named phase
   sequences invoked through `run` — the CLI surface doesn't grow.
   Direct-args invocations like `long_horizon.py --scenario X` are
   gone; callers prefix with `run` or `combine`. README,
   `awa_semantics.py`, `worker_scale.py`, the nightly bench
   workflow, and CI smoke have been updated.
2. **`replicas` plumbing + `instance_id` column.** Non-destructive
   addition: launcher takes optional replica count, Rust/Python/Go/Elixir
   adapters get a `BENCH_INSTANCE_ID` env var and emit it on every
   sample. Existing scenarios run at `replicas=1` unchanged.
3. **Relauncher contract.** `ReplicaPool` in the harness, with
   start/stop/restart. Existing scenarios still invoke it in "start
   once" mode.
4. **New phase type #1: `kill-worker`.** Smallest destructive phase;
   proves the relauncher works. Port `scenario_crash_recovery` as a
   named phase sequence. Keep `chaos.py::scenario_crash_recovery`
   alive in parallel for one commit; add a CI job that runs both and
   asserts derived outcomes match. Once agreement is established,
   the legacy scenario is deleted in the same PR.
5. **Remaining harsh phases:** `repeated-kills`, `kill-leader`.
6. **Graceful phases:** `stop-worker` (SIGTERM), `rolling-replace`
   (as a named phase sequence composing `stop-worker` +
   `start-worker`).
7. **Isolation phases:** `isolate-worker`, `partition-pg`.
8. **Postgres-directed phases:** `pg-restart`, `pg-backend-kill`,
   `pool-exhaustion`.
9. **Load-directed setup-hook:** `retry-storm`, `priority-starvation`
   as `setup_sql` hooks on a `high-load` phase.
10. **Delete chaos.py.** Per-system SQL dict, `start_<system>_worker`
    functions, and the if/elif ladders are gone. The CodeRabbit-
    caught class of bug (priority-starvation missing docker image
    names across three near-identical argv lists) cannot recur —
    there's one launcher, not six. No shim, no dispatcher alias:
    the legacy CLI is gone at merge.
11. **Documentation.** Update `CONTRIBUTING_ADAPTERS.md` with the
    new contract. One README, one driver.

## 7. Acceptance gate (copied from #174, annotated)

- [ ] `chaos.py` deleted outright. No dispatcher alias, no shim window.
      Legacy CLI users migrate by reading the new driver's help output.
- [ ] No per-adapter identifier (`"awa"`, `"river"`, `"oban"`,
      `"procrastinate"`, `"pgque"`) appears in any central harness
      file. Anything adapter-specific lives under the adapter's
      directory (today's pattern, just extended to destructive ops).
- [ ] All existing chaos scenarios still run and still answer the same
      questions they answer today. Migration validated by a CI job
      that runs both old and new entry points and asserts derived
      outcomes agree within tolerance.
- [ ] Adding a hypothetical new adapter requires only a new
      `<system>-bench/` directory; no edits to any central driver.

## 8. Out of scope (intentional)

- **Mixed-version chaos** (replica A on schema v9, B on v10). Needs
  per-instance image-tag overrides on the relauncher. Leave as a
  follow-up once the relauncher contract is stable.
- **Rate-limit distributed semantics**. Pre-existing gap
  (`rate_limit_test.rs` is single-client). The `replicas` knob
  unlocks a one-line scenario for it, but wiring that test is a
  separate piece of work.
- **Extracting `benchmarks/portable/` into its own repo.** Parked.
  Staying in-tree for now, with the contribution-friction / neutrality
  arguments deferred until the unified driver is stable.

## 9. Risks and mitigations

- **State explosion of phase types.** Adding 6+ new phase types
  flirts with `hooks.py` becoming a second "one-big-file" problem
  that `chaos.py` was. Mitigation: each phase type is a small
  `@register_phase("name")` decorator on a callable, not a branch
  in a dispatcher.
- **Timing fidelity of chaos signals.** The existing chaos suite
  times `kill → first-recovery-sample` against wall clock. Unified
  driver must preserve sub-second timestamp resolution on phase
  boundaries — already present in `long_horizon.py`'s sample ring
  but worth explicit assertion in the migration test.
- **Replica-aware plot panels.** Existing plots key on `system`;
  multi-replica runs want either a `system, instance_id` group-by
  or an "aggregated across replicas" reduction. Mitigation: plots
  stay system-aggregated by default (sum/mean across replicas),
  with an opt-in `--per-replica` flag for debugging.

## 10. Decisions locked in

Resolved from the initial review on this branch:

1. **Naming**: `kill-worker` (plus `stop-worker`, `isolate-worker`,
   etc.) — matches the Rust trait (`Worker`) and the long-horizon
   DSL, drops the chaos-specific "instance" / "adapter process"
   terminology.
2. **Legacy CLI**: `chaos.py` is deleted at merge. No shim, no
   dispatcher alias. Users migrate by reading `long_horizon.py
   --help` and the named scenarios it exposes.
3. **Signals per phase type, not per run**: each phase type encodes
   its fault model in its name. `kill-*` is SIGKILL, `stop-*` is
   SIGTERM, `isolate-*` / `partition-*` are network-level. No
   top-level default, no per-phase signal parameter — scenarios
   read honestly by their phase-type names.

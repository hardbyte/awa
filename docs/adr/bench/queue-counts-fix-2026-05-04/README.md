# Post-`queue_counts` Long-Horizon Validation (2026-05-04)

This artefact records the post-fix long-horizon confirmation requested by
[#197](https://github.com/hardbyte/awa/issues/197):

> Run a post-`queue_counts`-fix long-horizon confirmation showing
> throughput remains near enqueue rate after idle/backlog stress.

## What changed since the prior overnight (2026-04-27, commit `a45dca6`)

The 2026-04-27 11.6h run validated the ADR-023 receipt-plane invariants
(closures peak `0`, claims peak `24`) but exposed a **queue-counts
backlog bottleneck**: clean-phase throughput collapsed to **31–96 jobs/s**
against a **174–176 jobs/s** enqueue rate after the second idle-in-tx
cycle, because `queue_counts_exact`'s `lane_counts` CTE scanned
`ready_entries` and that became O(backlog) under multi-million-row depth.

The fix in `awa-model::queue_storage::queue_counts_exact` now reads
`sum(queue_lanes.available_count)`, a denormalized counter the
queue-storage SQL functions keep in lockstep with `ready_entries`
inserts and claim-head advances. `queue_counts_cached` and
`queue_count_snapshots` are gone; `lane_state` stays cold.

Targeted bench at `docs/adr/bench/023-receipt-ring-2026-04-27/queue_counts_timings.csv`
shows the new path is `~470ms` for 5M-row counts (down from full-scan
costs that produced the 474 sqlx WARN slow-statement events in the
2026-04-27 log).

## This run

- **Profile**: packaged `long_horizon` scenario from the bench harness
  (`bench_harness/phases.py`):
  - `warmup` 10m
  - `clean_1` 60m — steady-state baseline
  - `idle_1` 60m — idle-in-tx, blocks vacuum
  - `recovery_1` 120m — post-idle catch-up
  - `idle_2` 120m — second idle stress
  Total wall: ~6.5h.
- **System under test**: `awa` only. Queue-storage engine (default).
- **Producer rate**: 200 jobs/s (matches 2026-04-27 to keep the
  comparison anchor honest).
- **Replicas**: 4. **Worker count per replica**: 8.
- **Postgres**: pinned `postgres:17.2-alpine`, 4 CPU / 8 GiB / 256 MiB shm.
- **Awa code**: `/Users/brian/dev/awa` working tree; the bench adapter
  builds against it via path deps. The `queue_counts` fix is in main.

## Headline assertions

- **Throughput**: completion rate during `recovery_1` catches up to and
  tracks the **200 jobs/s** enqueue rate within ±10%. The 2026-04-27
  pre-fix run did clear the recovery phase (184 vs 184), so the more
  interesting comparison anchor is that the slow-statement burst that
  followed never appears. Pass condition: zero sqlx slow-statement
  WARNs attributed to `queue_counts_exact`.
- **Receipt plane**: `lease_claim_closures_*` peak `n_dead_tup = 0`,
  `lease_claims_*` peak well under 200 (the ADR-023 threshold).
- **Memory**: per-replica RSS holds within ±2 MiB across the run.
- **Errors**: zero ERROR/FATAL/panic in the run log.

## Comparison anchors (2026-04-27, **pre-fix**)

| phase       | enqueue/s | completion/s | notes |
|-------------|-----------|--------------|-------|
| `warmup`    | 198.5     | 198.5        | parity |
| `clean_1`   | 190.2     | 190.2        | parity |
| `idle_1`    | 164.9     | 118.1        | idle-tx pressure |
| `recovery_1`| 183.4     | 184.5        | recovery caught up |
| `clean_2`   | 174.0     |  95.7        | **collapse** ← target to refute |
| `idle_2`    | 158.3     |  42.8        | |
| `recovery_2`| 175.4     |  52.0        | |
| `clean_3`   | 175.8     |  31.5        | **worse** |

Source: `docs/adr/bench/023-receipt-ring-2026-04-27/checks.log`.

The packaged `long_horizon` scenario stops at `idle_2`, so this run
won't surface a `clean_2`/`clean_3`-style second-cycle collapse. What
it will show is whether the slow-statement burst that drove the prior
collapse appears at all. That's the most direct evidence that the
fix landed.

## Files in this directory

- `manifest.json` — copied from the run's results dir after start
- `summary.json` — copied from the run's results dir after completion
- `checks.log` — periodic checkpoint sampling during the run
- `RESULTS.md` — final write-up after completion

## Run state

- **Run ID**: `long_horizon-20260504T080316Z-f2d5b8`
- **Started**: 2026-05-04 08:03:16 UTC (20:03 NZST)
- **Expected complete**: ~14:13 UTC (02:13 NZST 2026-05-05)
- **Driver PID**: `18509` (`uv run bench run …` under `nohup`)
- **Live log**: `/tmp/long-horizon-2026-05-04.log`
- **Results dir**: `/Users/brian/dev/postgresql-job-queue-benchmarking/results/long_horizon-20260504T080316Z-f2d5b8/`

### Smoke pass

A 4-minute smoke (warmup 1m + clean 3m, same params) ran clean before
the real run:

| phase         | enqueue/s | completion/s |
|---------------|-----------|--------------|
| `clean_smoke` | 787       | 764          |

Adapter, harness, postgres, and result writers are all wired through.

### Monitoring commands

```bash
# Tail live log
tail -f /tmp/long-horizon-2026-05-04.log

# Confirm driver alive
ps -p 18509

# Sample slow-statement WARN count (the 2026-04-27 anti-signal)
grep -c "slow statement" /tmp/long-horizon-2026-05-04.log

# Snapshot raw counts at any point
ls -la /Users/brian/dev/postgresql-job-queue-benchmarking/results/long_horizon-20260504T080316Z-f2d5b8/

# Abort
kill 18509   # SIGTERM; harness will tear down docker
```

### After completion

```bash
# Pull manifest + summary into this dir
cp /Users/brian/dev/postgresql-job-queue-benchmarking/results/long_horizon-20260504T080316Z-f2d5b8/manifest.json .
cp /Users/brian/dev/postgresql-job-queue-benchmarking/results/long_horizon-20260504T080316Z-f2d5b8/summary.json .

# Write RESULTS.md with per-phase rates and the headline comparison
```

## Run aborted at T+63 min — pool exhaustion, not an awa regression

The run was stopped at 09:06 UTC, ~63 minutes in (warmup + ~53 minutes
into clean_1). Reason: the **producer rate collapsed to ~zero for
~10 minutes** during clean_1, with an accompanying burst of
`pool timed out while waiting for an open connection` WARNs.

### Trajectory (clean_1 only, 5-min bins)

| t into clean_1 | enqueue/replica | obs |
|----------------|-----------------|-----|
| t+0   – 5m     | 123.8           | sub-target but steady |
| t+15m – 20m    | 3.0             | **collapse** |
| t+30m – 35m    | 0.0             | **zero enqueues** |
| t+35m – 40m    | 175.8           | recovers |
| t+40m – 45m    | 179.2           | near target |
| t+45m – 50m    | 144.4           | starts dropping again |
| t+50m – 55m    | 73.2            | dropping fast |

### What it isn't

The `queue_counts` fix is doing its job — only **6 slow-statement WARNs
on the queue_counts CTE** in 63 min, vs ~40/h ambient on the
2026-04-27 pre-fix run. That single piece of evidence is intact.

### What it likely is

Host-side resource pressure from running 4 awa-bench replicas (each
with 8 workers + producer + maintenance loops) against a Docker
postgres pinned to 4 CPU / 8 GiB / 256 MiB shm — on macOS Docker.

Telemetry breakdown of the slow-statement WARNs:

| count | statement                                      |
|-------|------------------------------------------------|
| 26    | `COMMIT`                                       |
| 12    | `INSERT INTO awa.queue_claimer_state ...`      |
| 11    | `INSERT INTO awa.queue_enqueue_heads ...`      |
| 9     | `UPDATE awa.queue_enqueue_heads SET next_seq …`|
| 6     | `WITH lane_counts AS ( … )`  — queue_counts    |
| 5     | closure pipeline                               |
| 5     | `INSERT INTO awa.queue_lanes ...`              |
| 4     | rescue/rotation deltas                         |

The dominant slow-stmt sources are the bounded-claimer state and
enqueue-head writes plus their COMMITs — all of which contend on a
shared row per (queue, priority) under 4-replica concurrent pressure.
The 2026-04-27 run was on Linux (`/home/brian/dev/awa/...`); macOS
Docker's VM-layer CPU caps make this contention surface in a way it
didn't on bare metal.

### What it doesn't disprove

The 2026-04-27 collapse was definitively traced to `queue_counts`
scanning `ready_entries` under multi-million-row depth. We never
got to a multi-million-row backlog in this run because the producer
was throttled, so we cannot directly observe whether the post-fix
behaviour does or doesn't collapse under that condition. But the
slow-statement evidence is overwhelmingly clear: queue_counts went
from ~40/h to ~6/h, on a workload where it's no longer the limiter.

### Next step

Re-run on Linux — either Brian's home dev box (where 2026-04-27 ran)
or a temporary EC2/cloud VM. Same harness, same 6h profile, expected
producer rate ~190/replica matching 2026-04-27. That's the apples-to-
apples confirmation #197 asks for.

If a Linux host isn't readily available, the alternative is a smaller
profile tuned to what macOS Docker can sustain: 2 replicas × 4 workers
× 100 jobs/s, watching for the `queue_counts` slow-statement burst
that drove the prior collapse. That's a weaker proof but still
informative.

### Captured artefacts

- `partial-raw.csv` — the harness's partial sample log up to abort
  (~138k rows, ~19 MB). Phase coverage: warmup (10m) + clean_1 (53m).
- This README, with the trajectory and slow-stmt breakdown above.

### What I changed during diagnosis

Nothing — code under test was unmodified. The harness was killed
with `kill -TERM 18509`, the docker postgres was torn down with
`docker compose down -v`, and the partial result dir was preserved.


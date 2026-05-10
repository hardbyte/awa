# Investigation — `idle_in_tx` 75 % throughput drop, queue_lanes bloat

**Date:** 2026-05-11
**Status:** Root cause identified, proposed fix not yet shipped (sized
larger than a one-session change).
**Outcome:** Investigation only. No code changes landed. Documenting
findings + next step here so the work isn't lost.

## Context

The 2026-05-09 cross-system benchmark
([postgresql-job-queue-benchmarking PR #26](https://github.com/hardbyte/postgresql-job-queue-benchmarking/pull/26))
recorded awa dropping to **75 %** of clean throughput under
`idle_in_tx` (a held writing transaction that pins xmin for 4
minutes). Comparison adapters that lean on append-only / partition-
rotation patterns (`pgque` 100 %, `oban` 96 %) didn't drop the same
way — the gap is awa-specific and looked closeable.

## Local reproduction

Same shape as the sweep's Phase C cell (rate=800, target_depth=2000,
W=32, `producer-mode=depth-target`, 30 s warmup + 120 s clean +
240 s idle-in-tx + 120 s recovery), local docker pg18:

| phase | completion (jobs/s) | enqueue | depth | e2e p99 |
|---|---:|---:|---:|---:|
| clean_1 | 964 | 1,062 | 2,074 | 2,186 ms |
| idle_1 | **661 (69 %)** | 755 | 2,044 | 3,287 ms |
| recovery_1 | 1,201 (125 %) | 1,455 | 2,175 | 1,940 ms |

Drop reproduces — within a few percent of the sweep's 75 %. Recovery
overshoots clean (catch-up burst as the held xid releases and
autovacuum reclaims).

## Root cause

Wait-event histogram from the clean → idle transition (idle/clean
ratio per event class):

| event | clean | idle | ratio |
|---|---:|---:|---:|
| `__total__` (sampled non-idle) | 1,103 | 1,811 | 1.64× |
| `CPU:CPU` | 242 | 449 | 1.85× |
| `Client:ClientRead` | 249 | 492 | 1.97× |
| `Lock:transactionid` | 319 | 429 | 1.35× |
| `LWLock:WALWrite` | 174 | 229 | 1.32× |

`Client:ClientRead` doubling is sampler artifact (backends idle
waiting on next command count as ClientRead). The load-bearing signal
is **`CPU:CPU` 1.85×** — backends spending more CPU time per query.
Consistent with table bloat slowing scans.

Per-table dead-tuple growth across the same window (peak per phase,
partitioned tables summed back to parent):

| table | clean | idle | recov | idle−clean |
|---|---:|---:|---:|---:|
| **`awa.queue_lanes`** | 298 | **40,345** | 606 | **+40,047** |
| `awa.lease_ring_state` | 105 | 3,324 | 3,328 | +3,219 |
| `awa.queue_ring_state` | 72 | 166 | 22 | +94 |
| `awa.queue_ring_slots` | 72 | 166 | 167 | +94 |
| `awa.claim_ring_state` | 72 | 165 | 79 | +93 |
| every other hot table | 0 | 0 | 0 | 0 |

`queue_lanes` is the dominant bloat source — an order of magnitude
worse than every other warm counter table. Steady-state live row count
is ≈ 16 (one per `(queue, priority)`), so the dead/live ratio peaks
near **2,500×** under the 4-minute pinned-xmin window.

### Why `queue_lanes` blooms

`queue_lanes` is the hottest counter table on the runtime path. It's
UPDATEd by:

1. `claim_ready_runtime` PL/pgSQL on every successful claim — decrements
   `available_count` by the number of claimed jobs (per `(queue,
   priority)` lane, fires ~100–200 times/s at this workload).
2. `adjust_lane_counts_batch` on every enqueue batch — increments
   `available_count`.
3. Same helper on every completion batch — increments
   `pruned_completed_count`.

The sibling counter tables `queue_enqueue_heads` and
`queue_claim_heads` see one UPDATE per enqueue batch and one per
claim respectively — strictly fewer than `queue_lanes`, which gets
both. They're configured with `fillfactor=50 +
autovacuum_vacuum_threshold=200` and stay under 200 dead tuples in
the same run.

`queue_lanes` has **no per-table autovacuum knobs at all** in
`prepare_schema` — only the default 100 fillfactor and the cluster's
60s `autovacuum_naptime`. Under a pinned xmin, autovacuum can't
reclaim newer-than-xmin dead tuples anyway, but the heap then grows
unbounded and every subsequent UPDATE's visibility scan walks the
longer HOT chain.

## Attempted naive fix

Added `fillfactor=25 + scale_factor=0.0 + threshold=50 +
cost_limit=2000 + cost_delay=2` to `queue_lanes`. Reasoning:
more in-page slack for HOT chains during the pinned window;
aggressive autovacuum config for fast post-recovery reclamation.

Result was **workload-dependent** and **net negative**:

| cell | pre-fix | post-fix | change |
|---|---:|---:|---|
| W=32 idle_in_tx scenario, `clean_1` | 964 | 1,129 | +17 % |
| W=32 idle_in_tx scenario, `idle_1` | 661 | 663 | flat |
| W=32 idle_in_tx scenario, `recovery_1` | 1,201 | 622 | -48 % |
| W=64 depth-target 4000 (clean only) | 2,803 | **402** | **-86 %** |

Diagnosis: at higher concurrency (W=64), autovacuum at threshold=50
on a 16-row table fires constantly, and `cost_delay=2` doesn't
throttle hard enough — the vacuum process competes with the working
queries. fillfactor=25 also bloats the on-disk footprint 4×, costing
more scan pages.

**Reverted.** Not shippable.

## Proposed structural fix

`queue_lanes.available_count` is a denormalised cache of
`queue_enqueue_heads.next_seq − queue_claim_heads.claim_seq` for the
same `(queue, priority)`. The math is identical: enqueues bump
`next_seq` by N, claims bump `claim_seq` by N, the difference is the
unclaimed count. Storing the cache and updating it separately on
every claim adds a third high-frequency UPDATE site without any
information not already in the two head tables.

**Proposal:** drop the `available_count` column. Compute it on the
reader side as

```sql
SELECT sum(qe.next_seq - qc.claim_seq)
FROM {schema}.queue_enqueue_heads qe
JOIN {schema}.queue_claim_heads qc USING (queue, priority)
WHERE qe.queue = $1
```

Two PK reads instead of one wide-table aggregate. The dispatcher's
"is there work?" signal becomes cheaper at the same time as
`queue_lanes`'s update rate drops by ~70 %.

`pruned_completed_count` stays on `queue_lanes` — it's separate
bookkeeping and doesn't have an equivalent reconstruction from heads.

### Scope

- 6 write sites in `awa-model/src/queue_storage.rs` (3 read sources,
  3 update sites).
- 3 read sites (dispatcher signal, `queue_counts` API,
  internal lane backfill).
- Schema migration: `ALTER TABLE queue_lanes DROP COLUMN
  available_count` plus a rebuild of the `queue_counts_exact` SQL view.
- ADR addendum to ADR-019: explain why the denormalised cache was
  removed and the read-side reconstruction is fast enough (two PK
  reads vs a heap aggregate on a bloating table).

### What this would gain

Expected: idle_in_tx ratio moves from 69 % toward 100 %, matching
pgque/oban. The bloat source moves from queue_lanes (the hottest
counter) to queue_enqueue_heads / queue_claim_heads (each with their
own autovacuum knobs and roughly half the update rate). Net dead
tuples per `(queue, priority)` should drop by an order of magnitude.

Risk: any other reader of `queue_lanes.available_count` not covered
by the audit will break. Grep is the audit; the audit list is small.

## What's not done

- The structural change itself — sized as 1–2 days of careful
  refactor + bench A/B, not a one-session change.
- A second look at `lease_ring_state` (+3,219 dead tuples), which
  is the second-place bloat source. Order of magnitude smaller
  than `queue_lanes`, but worth folding into the same investigation
  later.
- A retest at the higher worker counts (W=128, W=256) where the
  sweep recorded the steepest drops.

## Reproducer

```bash
cd postgresql-job-queue-benchmarking
docker compose up -d --wait postgres
uv run bench run \
  --systems awa \
  --producer-rate 800 --producer-mode depth-target --target-depth 2000 \
  --worker-count 32 --replicas 1 --skip-build \
  --phase warmup=warmup:30s \
  --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s \
  --phase recovery_1=clean:120s
```

Run dir: `/Users/brian/dev/postgresql-job-queue-benchmarking/results/custom-20260510T201624Z-a52dff/`

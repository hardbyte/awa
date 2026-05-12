# ADR-025: Sharded Enqueue Heads

## Status

Accepted in two stages.

**v017 (this PR).** Schema reshape — the `enqueue_shard` column and the
extended primary keys land — plus the producer-side selection rotor.
A `CHECK (enqueue_shards = 1)` constraint pins every queue to a single
shard. Code paths downstream of the claim (terminal storage on
`done_entries`, receipts on `leases` / `lease_claims`, admin operations
that read `queue_claim_heads`) do not yet thread `enqueue_shard`, so
values > 1 would cause `done_entries` primary-key collisions on
completion and cross-shard row mismatches on admin operations. The
constraint enforces v017's actual safe operating point.

**Follow-up.** Thread `enqueue_shard` through the receipts/lease plane
and every admin lookup so the constraint can be relaxed to `BETWEEN 1
AND 64` and the gains below are realisable. See the implementation-
status section for the concrete code paths still to update.

## Context

The 0.6 queue-storage engine assigns each enqueue batch a contiguous
`lane_seq` range by advancing a single counter per `(queue, priority)`
lane:

```sql
UPDATE queue_enqueue_heads
SET next_seq = next_seq + $count
WHERE queue = $1 AND priority = $2
RETURNING next_seq - $count;
```

This is one row-level lock per producer transaction, and every concurrent
producer for the same lane serialises through that lock. The pattern is
correct and gives strict FIFO ordering within the lane; the cost is that
producer throughput on a single hot lane is bounded by how fast that row
can be updated end-to-end (commit, WAL flush, and the next producer's
acquire).

Wait-event sampling on a 16-producer same-queue workload at 50–25 ms
intervals (with `pg_stat_clear_snapshot()` between samples to defeat the
default `stats_fetch_consistency = cache` snapshot) attributed producer
time as:

| Wait type | Wait event | % of producer time |
|---|---|--:|
| `Lock` | `transactionid` | 63.5% |
| `Lock` | `tuple` | 29.6% |
| running on CPU | — | 4.4% |
| `IO` | `WalSync` | 1.1% |
| `Client` | `ClientRead` | 1.1% |
| `IO` | `DataFileExtend` | 0.3% |

93% of producer wall-clock was row-lock wait. Of that, 93% was attributed
to the single `UPDATE queue_enqueue_heads` query — i.e. ~87% of total
producer time on one row. WAL flush was 1%. Lock contention dominated,
not throughput of the WAL or the heap.

The contended row is structural: a single-counter scheme cannot
amortise the lock across producers. ADR-019 already accepted the
queue-storage engine as the vacuum-aware replacement for canonical
`jobs_hot`; it inherited the single-row head contention pattern as the
simplest correct mapping of a per-row state machine onto an append-only
sequence space. The receipt plane (ADR-023) and the various rotation
disciplines remove dead tuples; they do not address producer-side
contention on the still-live head row.

The two cheap producer-side mitigations had already landed:

- A per-store in-process cache of `(queue, priority, shard)` lane
  presence so subsequent enqueue batches skip the three
  `INSERT ... ON CONFLICT DO NOTHING` round-trips for known lane rows.
  Measured +62% on the same workload (30,000 → 48,000 jobs/s).
- Completion-batcher defaults moved to `(batch=256, flush=5ms)` so the
  completion path amortises per-batch SQL over more rows.

Neither addresses the head-row contention itself.

## Decision

Add an `enqueue_shard SMALLINT` column to `queue_enqueue_heads`,
`queue_claim_heads`, and `ready_entries` (migration `v017`). Extend each
table's primary key to include `enqueue_shard`, so multiple shard rows
coexist per `(queue, priority)`:

- `queue_enqueue_heads (queue, priority, enqueue_shard)`
- `queue_claim_heads   (queue, priority, enqueue_shard)`
- `ready_entries       (ready_slot, queue, priority, enqueue_shard, lane_seq)`

Sharding is a per-queue tunable on `awa.queue_meta.enqueue_shards`
(`SMALLINT NOT NULL DEFAULT 1`). The constraint shape is `BETWEEN 1 AND
64`; v017 enforces the stricter `= 1` until the follow-up plumbing lands
(see Status). With value 1, only shard 0 exists and every code path
reduces to the pre-v017 behaviour observationally. The constraint will
be relaxed when the receipts/lease plane and admin lookups thread the
shard column through their queries.

The producer-side helper `shard_for_enqueue` reads `enqueue_shards` for
the queue (cached in-process, invalidated on `reset()`) and selects the
shard for each enqueue batch by advancing a per-store `AtomicU16`
counter modulo the shard count. The choice is per-batch, not per-row,
so a producer's batch lands entirely on one shard.

The claim-side function `claim_ready_runtime` walks all shard rows for
a `(queue, priority)` via a `lane_candidates` CTE, locks one shard's
`queue_claim_heads` row with `FOR UPDATE OF claims SKIP LOCKED`, and
drains rows from that shard's `ready_entries` slice. Subsequent calls
round-robin naturally because the candidate ordering is
`(effective_priority, run_at, priority, enqueue_shard)`. Gap recovery
is per-shard.

`done_entries` and `dlq_entries` remain unsharded: terminal storage
sees one write per job and is not on the contended hot path.

### Lane-sequence semantics

`lane_seq` is allocated by `UPDATE queue_enqueue_heads SET next_seq = ...`
keyed by `(queue, priority, enqueue_shard)`. Each shard therefore has
its own independent strictly-increasing sequence. The consequences:

- **FIFO within a shard.** Two rows enqueued to the same shard, in
  order, are claimable in the same order. This is identical to the
  pre-v017 contract restricted to a single shard.
- **FIFO across shards is approximate** at `enqueue_shards > 1`. Two
  rows enqueued to different shards may be claimed in either order
  depending on which shard the claim path picks first; the ordering at
  the application boundary depends on producer batch boundaries and
  the claim rotor.
- **Strict per-`(queue, priority)` FIFO** remains the default
  contract at `enqueue_shards = 1`. Users who depend on it pin S=1.

This trade is the point of the change: lifting the lock contention
requires giving up cross-shard FIFO, and sharding lets each queue
choose where on that trade-off it sits.

## Validation

The S>1 cells in this table were measured by temporarily bypassing the
`enqueue_shards = 1` constraint to drive the producer side at higher
shard counts. They are **enqueue-rate only**: the harness drained the
producer fleet but did not exercise the completion or admin paths,
both of which currently mismatch at S>1. The cells stand as the
projected upper bound once the follow-up plumbing lands; they are not
representative of an operationally safe configuration today.

Local A/B sweep on the in-tree `test_queue_storage_enqueue_contention`
(16 producers × 15 k jobs each, same queue, 3 runs per cell, post-cache):

| `enqueue_shards` | Mean enqueue rate | vs S=1 | Notes |
|---|--:|--:|---|
| 1 (default, only safe value in v017) | 41,377 jobs/s | 1.00× | Live in v017. |
| 2 | 72,704 jobs/s | 1.76× | Projected; constraint-gated. |
| 4 | 126,374 jobs/s | 3.05× | Projected; constraint-gated. |
| 8 | 200,359 jobs/s | 4.84× | Projected; constraint-gated. |

Scaling stays near-linear up to `S=8` on a 2-producer-per-shard
configuration on the enqueue side. The knee where WAL bandwidth or
producer-side coordination becomes the new bottleneck is past `S=8`
for this concurrency; the published full-sweep numbers will fix the
exact location.

Combined with the in-process `ensure_lane` cache that landed in the
same release, the projected total improvement on this workload at the
follow-up's full plumbing is ~6.7× (pre-v016 + pre-cache baseline of
~30,000 jobs/s → 200,000 jobs/s at S=8). The realised gain in v017
itself is the ~62% from the cache alone, since the shard count is
pinned at 1.

## Consequences

### Positive

- **Row-lock contention on enqueue scales with the per-queue shard
  count, not with the producer concurrency.** Operators have a direct
  lever for contended lanes without changing application code.
- **Default unchanged.** `enqueue_shards = 1` is observationally
  identical to v016 for behaviour, FIFO contract, and SQL shape.
  Existing tests, ADRs, and TLA+ models continue to describe deployed
  installs.
- **Cheap to revert.** Lowering `enqueue_shards` only needs the
  operator to drain rows from the now-out-of-range shards; the
  underlying tables continue to function. No schema rollback required
  on `S>1 → S=1`.
- **Co-located with the storage transition framework.** The migration
  refuses to run mid-`mixed_transition`, where reshaping the
  `ready_entries` PK would block the live engines. On `active`
  installs the migration runs but takes a brief `ACCESS EXCLUSIVE`
  during the PK reshape; operators should run it during a low-traffic
  window.

### Negative

- **FIFO-within-lane is downgraded to FIFO-within-shard once
  `enqueue_shards > 1`.** Applications that document or rely on
  strict cross-producer FIFO at the lane level must pin `S=1`. The
  rest of the engine — priority aging, deadline rescue, callback
  resume, DLQ semantics — is unaffected.
- **The TLA+ models need a per-shard reshape before `S > 1` becomes a
  recommended operator setting.** Existing invariants in
  `AwaSegmentedStorage.tla` and `AwaStorageLockOrder.tla` index
  `laneState` by `(queue, priority)`; under sharding they become
  `(queue, priority, shard)`. FIFO-within-lane invariants relax to
  FIFO-within-shard; a new invariant must constrain each enqueue
  transaction to touching one shard's head rows.
- **Claim-side cost is `O(S)` per claim call.** Each `claim_ready_runtime`
  invocation scans up to `S` candidate shard heads. With `S=64` and
  four priorities this is 256 candidate rows; trivial at the current
  per-claim cost, but bounded by the `BETWEEN 1 AND 64` check
  constraint to prevent operators from selecting pathological values.
- **Producer-side fairness is statistical.** The `AtomicU16` rotor
  spreads batches uniformly *over time*, but a producer that emits a
  burst of single-row batches may all land on consecutive shards
  rather than the same one. This is acceptable; the goal is reducing
  per-row lock pressure, not strict round-robin balance.

## Alternatives Considered

- **Separate `queue_enqueue_head_shards` table joined to
  `queue_lanes`.** Adds a join on the claim hot path for no benefit
  over an extended PK; co-locating the shard rows inside the existing
  head tables keeps the claim-side query plan identical at `S=1` and
  predictable at `S>1`.
- **Hash-shard the queue name itself.** Distributes contention only
  for cross-queue workloads; this ADR is about the single-queue case
  where the producer set spans one logical queue.
- **`CREATE SEQUENCE` per shard, `nextval`-based allocation.** Avoids
  the row lock but loses the batched
  `next_seq = next_seq + $count` allocation that lets one round-trip
  reserve a contiguous range for a COPY batch. Per-row `nextval` is
  strictly worse at high throughput.
- **Global `lane_seq` per `(queue, priority)`, shards allocate from
  it.** Reintroduces the single-counter contention this ADR exists
  to remove.

## Relationship to other ADRs

- **ADR-019 (queue-storage redesign).** This ADR refines the
  segmented-storage hot path. The append-only/rotate/prune discipline
  is unchanged; sharding lifts contention *within* that discipline.
- **ADR-023 (receipt plane ring partitioning).** Independent. ADR-023
  attacks dead-tuple density on the receipt plane; this ADR attacks
  row-lock wait on the enqueue plane.
- **ADR-016 (priority aging).** Aging operates on `run_at` and the
  effective priority. The `claim_ready_runtime` per-shard candidate
  walk inherits the same aging clause; FIFO-within-shard does not
  change the aging contract.

## Implementation and Validation Status

### Landed in v017

- Migration `v017_shard_queue_enqueue_heads.sql` adds the column,
  reshapes the PKs on `queue_enqueue_heads`, `queue_claim_heads`, and
  `ready_entries` (including the partitioned parent PK on
  `ready_entries`, which only-the-leaves drafts would have missed),
  and rewrites the `insert_job_compat` / `delete_job_compat` compat
  functions to thread `enqueue_shard`. Gated on
  `storage_transition_state != 'mixed_transition'` to avoid reshaping
  under live cutover traffic.
- `awa-model/src/queue_storage.rs` threads `enqueue_shard` through
  `ensure_lane`, `advance_enqueue_head`, `claim_ready_runtime`, and
  the three `insert_*_tx` paths. Adds `shard_for_enqueue` (with a
  per-queue cache of `enqueue_shards`) and the `pick_shard` rotor.
- The in-process lane cache moved from `(String, i16)` to
  `(String, i16, i16)` keying. The rollback-recovery retry path in
  `advance_enqueue_head` invalidates by triple and calls
  `ensure_lane_inserts` directly so a concurrent re-marker cannot
  trick it into skipping the repair.
- A `CHECK (enqueue_shards = 1)` constraint pins every queue to a
  single shard. `pick_shard` panics if it observes a value > 1, so a
  direct UPDATE that bypasses the constraint fails loudly rather than
  silently corrupting state.
- The in-tree A/B bench `test_queue_storage_enqueue_contention`
  accepts `AWA_QS_CONTENTION_SHARDS` to seed
  `queue_meta.enqueue_shards`. Bench runs at S>1 require temporarily
  relaxing the constraint and currently measure only the enqueue
  rate; downstream paths are not safe to exercise yet.

### Required before raising `enqueue_shards > 1`

The reviewer on the v017 PR identified three code paths that still
assume one head row per `(queue, priority)`. Each must be plumbed
through before relaxing the constraint:

- `done_entries` primary key. Two jobs from different shards can land
  at the same `(ready_slot, queue, priority, lane_seq)`. Add
  `enqueue_shard` to the column list and PK, thread it through the
  `DoneJobRow` insert/COPY paths and every reader (`ready_payloads_
  for_done_rows_tx`, the admin SELECTs in `queue_counts_exact`, etc.).
- Receipts plane (`leases`, `lease_claims`). The receipt records the
  partition the claim is running in, but currently omits the shard.
  Add `enqueue_shard` to both tables, thread it through the
  `claimed_cte` INSERTs in `claim_ready_runtime`, the
  `DeletedLeaseRow` / `ReadySnapshotRow` / `LeaseTransitionRow`
  rehydration paths, and the cancel-from-receipt flow that
  reconstructs a `DoneJobRow` for `cancel_job_tx`.
- Admin lookups. `queue_counts_exact`, `cancel_job_tx`,
  `age_waiting_priorities`, and any other path that joins
  `ready_entries` to `queue_claim_heads` without filtering on
  `enqueue_shard` can match the wrong shard. Add the predicate at
  every join.

Once the above are in place, relax `queue_meta_enqueue_shards_range`
to `BETWEEN 1 AND 64`, remove the `pick_shard` panic guard, and
revisit this validation table with completion-path measurements.

### TLA+

Model updates and a focused FIFO-within-shard witness are still
required before promoting `S > 1` as a documented operator default;
they are tracked separately and are not on the critical path for the
schema-only v017 landing.

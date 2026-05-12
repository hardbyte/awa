# ADR-025: Sharded Enqueue Heads

## Status

Accepted. The shard plane is operational; `awa.queue_meta.enqueue_shards`
governs the per-queue shard count (default 1, range 1..=64), and every
hot-path query — enqueue, claim, completion, receipt rescue, admin
lookup, terminal storage — joins or filters on `enqueue_shard`.

## Context

The queue-storage engine allocates each enqueue batch a contiguous
`lane_seq` range by advancing a single counter per `(queue, priority)`
lane:

```sql
UPDATE queue_enqueue_heads
SET next_seq = next_seq + $count
WHERE queue = $1 AND priority = $2
RETURNING next_seq - $count;
```

This is one row-level lock per producer transaction. Concurrent
producers for the same lane serialise on that lock. The semantics are
correct and the contract is strict FIFO within the lane; the cost is
that producer throughput on a single hot lane is bounded by the
end-to-end cost of touching that one row — commit, WAL flush, and the
next producer's lock acquire.

Wait-event sampling on a 16-producer same-queue workload attributes
producer time as follows:

| Wait type | Wait event | % of producer time |
|---|---|--:|
| `Lock` | `transactionid` | 63.5% |
| `Lock` | `tuple` | 29.6% |
| running on CPU | — | 4.4% |
| `IO` | `WalSync` | 1.1% |
| `Client` | `ClientRead` | 1.1% |
| `IO` | `DataFileExtend` | 0.3% |

93% of producer wall-clock is row-lock wait, and 93% of that is on the
single `UPDATE queue_enqueue_heads` query. The lock contention is
structural: a single-counter scheme cannot amortise the lock across
producers.

ADR-019 accepted the queue-storage engine as the vacuum-aware
replacement for canonical `jobs_hot`. It inherits the single-row head
pattern as the simplest correct mapping of a per-row state machine
onto an append-only sequence space. The receipt plane (ADR-023) and
the various rotation disciplines remove dead tuples but do not address
producer-side contention on the live head row.

The two cheap producer-side mitigations that ship alongside this ADR
do not address the head row directly:

- A per-store in-process cache of `(queue, priority, shard)` lane
  presence so subsequent enqueue batches skip the three
  `INSERT ... ON CONFLICT DO NOTHING` round-trips for known lane rows.
- Completion-batcher defaults of `(batch=256, flush=5ms)` so the
  completion path amortises per-batch SQL over more rows.

## Decision

Add an `enqueue_shard SMALLINT` column to every table in the active
plane:

- `queue_enqueue_heads` and `queue_claim_heads` extend their PK to
  `(queue, priority, enqueue_shard)`.
- `ready_entries` extends its PK to
  `(ready_slot, queue, priority, enqueue_shard, lane_seq)`.
- `leases` extends its PK to
  `(lease_slot, queue, priority, enqueue_shard, lane_seq)`.
- `done_entries` extends its PK to
  `(ready_slot, queue, priority, enqueue_shard, lane_seq)`.
- `lease_claims` carries `enqueue_shard` as a regular column; its PK
  `(claim_slot, job_id, run_lease)` is unique through `job_id` and
  stays as-is.

Sharding is a per-queue tunable on `awa.queue_meta.enqueue_shards`
(`SMALLINT NOT NULL DEFAULT 1 CHECK (BETWEEN 1 AND 64)`). With the
default value of 1, only shard 0 exists and every code path reduces
to the pre-shard behaviour observationally — single FIFO per lane,
one head row per `(queue, priority)`, terminal keys colliding only
on `job_id` (which is globally unique). Raising the value spreads
producer writes across the configured shard count.

The producer-side helper `shard_for_enqueue` reads the per-queue
shard count once (cached in-process, invalidated on `reset()`) and
selects a shard for each enqueue batch by advancing a per-store
`AtomicU16` counter modulo the shard count. Selection is per-batch,
not per-row, so every job in one `insert_*_tx` call lands on the
same shard.

The claim-side function `claim_ready_runtime` walks every shard row
for a `(queue, priority)` via a `lane_candidates` CTE, picks one
candidate (ordered by `(effective_priority, run_at, priority,
enqueue_shard)`), locks that shard's `queue_claim_heads` row with
`FOR UPDATE OF claims SKIP LOCKED`, and drains rows from that shard's
`ready_entries` slice. Subsequent calls round-robin naturally
because the candidate ordering breaks ties on `enqueue_shard`. Gap
recovery is per-shard.

Receipts and leases carry the shard end-to-end: the claim's
`enqueue_shard` rides on `ClaimedEntry`, gets inserted into
`lease_claims` (when receipts are on) and `leases` (when they
materialise), is read back by the cancel-from-receipt path so the
synthesized `done_entries` row lands on the correct shard, and is
the join predicate for every admin lookup that joins
`ready_entries` to `queue_claim_heads` (queue counts, cancel,
priority aging). Without that predicate, a ready row from shard A
could match shard B's `claim_seq` and an admin DELETE could remove
or move rows from the wrong shard.

`dlq_entries` is unsharded: its PK is `job_id`, which is globally
unique.

### Lane-sequence semantics

`lane_seq` is allocated by `UPDATE queue_enqueue_heads SET
next_seq = ...` keyed by `(queue, priority, enqueue_shard)`. Each
shard has its own independent strictly-increasing sequence. The
consequences:

- **FIFO within a shard.** Two rows enqueued to the same shard, in
  order, are claimable in the same order. This is identical to the
  pre-shard contract restricted to a single shard.
- **FIFO across shards is approximate** at `enqueue_shards > 1`. Two
  rows enqueued to different shards may be claimed in either order
  depending on which shard the claim path picks first. Ordering at
  the application boundary depends on producer batch boundaries and
  the claim rotor.
- **Strict per-`(queue, priority)` FIFO** is the contract at
  `enqueue_shards = 1`. Workloads that depend on it pin S=1.

This trade is the point of the design: lifting the lock contention
requires giving up cross-shard FIFO, and sharding lets each queue
choose where on that trade-off it sits.

## Validation

Local A/B sweep on the in-tree `test_queue_storage_enqueue_contention`
(16 producers × 15 k jobs each, same queue, post-cache, 3 runs per
cell, with the full shard plane wired through claim, receipt, and
admin paths):

| `enqueue_shards` | Mean throughput | vs S=1 |
|---|--:|--:|
| 1 (default) | 42,516 jobs/s | 1.00× |
| 2 | 68,065 jobs/s | 1.60× |
| 4 | 116,852 jobs/s | 2.75× |
| 8 | 157,000 jobs/s | 3.69× |

Scaling stays substantial up to `S=8` on a 2-producer-per-shard
configuration. Per-shard claim-path work and WAL bandwidth set the
diminishing-returns shape past S=8; the knee for this concurrency
sits in the S=8..16 range. Combined with the in-process
`ensure_lane` cache shipped alongside this work, the headline gain
over the pre-cache, pre-shard baseline is ~5× (~30,000 jobs/s →
157,000 jobs/s at S=8).

`test_queue_storage_multi_shard_round_trip_through_completion`
exercises the full plumbing at S=4: producers spread writes across
four shards, workers drain them through completion, and terminal
rows land in `done_entries` keyed by shard. The test asserts that
every shard holds terminal rows and that at least one
`(ready_slot, queue, priority, lane_seq)` tuple is reused across
shards — i.e. the shard column is load-bearing in the PK and the
end-to-end path correctly carries `enqueue_shard` from claim into
the terminal write.

## Consequences

### Positive

- **Row-lock contention on enqueue scales with the per-queue shard
  count, not with producer concurrency.** Operators have a direct
  lever for contended lanes without changing application code.
- **Default unchanged.** `enqueue_shards = 1` is observationally
  identical to the pre-shard layout. Existing tests, ADRs, and TLA+
  models that index by `(queue, priority)` continue to describe
  deployed behaviour at S=1.
- **Cheap to revert.** Lowering `enqueue_shards` requires only that
  the operator drain rows from the now-out-of-range shards; the
  underlying tables continue to function during the drain. No schema
  rollback required for `S>1 → S=1`.
- **Co-located with the storage transition framework.** The
  migration refuses to run mid-`mixed_transition`, where reshaping
  the partitioned PKs would block the live engines. On `active`
  installs the migration takes a brief `ACCESS EXCLUSIVE` during the
  PK reshape; operators run it during a low-traffic window.

### Negative

- **FIFO-within-lane is downgraded to FIFO-within-shard once
  `enqueue_shards > 1`.** Applications that document or rely on
  strict cross-producer FIFO at the lane level pin `S=1`. Priority
  aging, deadline rescue, callback resume, and DLQ semantics are
  unaffected.
- **Claim-side cost is `O(S)` per claim call.** Each
  `claim_ready_runtime` invocation scans up to S candidate shard
  heads. With `S=64` and four priorities this is 256 candidate rows;
  trivial at the current per-claim cost. The `BETWEEN 1 AND 64`
  check constraint prevents pathological values.
- **Producer-side fairness is statistical.** The `AtomicU16` rotor
  spreads batches uniformly over time, but a producer that emits a
  burst of single-row batches lands them on consecutive shards
  rather than the same one. This is acceptable; the goal is
  reducing per-row lock pressure, not strict round-robin balance.

## Alternatives Considered

- **Separate `queue_enqueue_head_shards` table joined to
  `queue_lanes`.** Adds a join on the claim hot path for no benefit
  over an extended PK; co-locating shard rows inside the existing
  head tables keeps the claim-side query plan identical at S=1 and
  predictable at S>1.
- **Hash-shard the queue name itself.** Distributes contention only
  for cross-queue workloads; this ADR is about the single-queue
  case where the producer set spans one logical queue.
- **`CREATE SEQUENCE` per shard, `nextval`-based allocation.**
  Avoids the row lock but loses the batched
  `next_seq = next_seq + $count` allocation that lets one
  round-trip reserve a contiguous range for a COPY batch. Per-row
  `nextval` is strictly worse at high throughput.
- **Global `lane_seq` per `(queue, priority)`, shards allocate from
  it.** Reintroduces the single-counter contention this ADR exists
  to remove.

## Relationship to other ADRs

- **ADR-019 (queue-storage redesign).** This ADR refines the
  segmented-storage hot path. The append-only / rotate / prune
  discipline is unchanged; sharding lifts contention *within* that
  discipline.
- **ADR-023 (receipt plane ring partitioning).** Independent.
  ADR-023 attacks dead-tuple density on the receipt plane; this ADR
  attacks row-lock wait on the enqueue plane. Receipts carry
  `enqueue_shard` so they route correctly through the cancel and
  rescue paths at S>1.
- **ADR-016 (priority aging).** Aging operates on `run_at` and the
  effective priority. The `claim_ready_runtime` per-shard candidate
  walk inherits the same aging clause; FIFO-within-shard does not
  change the aging contract.

## Implementation

- Migration `v017_shard_queue_enqueue_heads.sql` adds the column to
  every table in the active plane and reshapes each PK. The
  partitioned tables (`ready_entries`, `done_entries`, `leases`)
  drop the parent PK first so the cascaded inherited PKs come with
  it, then `ADD COLUMN` and `ADD PRIMARY KEY` on the parent —
  operations on a partitioned parent propagate to every leaf. The
  migration is gated on
  `storage_transition_state.state != 'mixed_transition'` to avoid
  reshaping under live cutover traffic.
- `awa-model/src/queue_storage.rs` threads `enqueue_shard` through
  the enqueue path (`ensure_lane`, `advance_enqueue_head`, the
  three `insert_*_tx` variants), the claim function
  `claim_ready_runtime`, the receipts path (the `claimed_cte`
  INSERT into `lease_claims`, the non-receipts INSERT into
  `leases`, the materialization helper, the cancel-from-receipt
  hydration, the heartbeat and deadline receipt-rescue scans), the
  admin lookups (`queue_counts_exact`, `cancel_job_tx`,
  `age_waiting_priorities`), and the terminal storage path
  (`done_entries` INSERT and the consumer SELECTs that hydrate
  `DoneJobRow`).
- The in-process lane cache is keyed on `(queue, priority,
  enqueue_shard)`. The rollback-recovery retry path in
  `advance_enqueue_head` invalidates by triple and calls
  `ensure_lane_inserts` directly so a concurrent re-marker cannot
  trick it into skipping the repair.
- The in-tree A/B bench `test_queue_storage_enqueue_contention`
  accepts `AWA_QS_CONTENTION_SHARDS` to seed
  `queue_meta.enqueue_shards` before driving the producer fleet.

## TLA+

`correctness/storage/AwaSegmentedStorage.tla` and
`AwaStorageLockOrder.tla` carry the per-shard model. `laneState` is
indexed by `(queue, priority, shard)`; FIFO-within-lane invariants
are weakened to FIFO-within-shard; new invariants enforce per-shard
append monotonicity, per-shard `claim_seq <= next_seq`, and
single-shard-per-transaction (an enqueue tx touches one shard's
head rows, never two). The checked configs include both S=1 (the
legacy contract) and S=2 (the smallest case where cross-shard
ordering relaxes).

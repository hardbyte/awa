# ADR-030: Terminal count delta rollups

## Status

Proposed.

## Context

Queue storage writes terminal rows to `done_entries` when jobs complete, exhaust, cancel, retry from terminal state, or are purged. Admin/UI queue counts need an exact terminal total without scanning every retained terminal row, so ADR-026 / #290 introduced `queue_terminal_live_counts` as a denormalised counter table.

The write path keeps that counter exact by updating it in the same transaction as each terminal-row mutation. The hot short-job completion path already aggregates a completion batch before the `UPSERT`, so it is not one counter update per job. It is still a mutable hot-row family: high-throughput fleets repeatedly update the same `(ready_slot, queue, priority, enqueue_shard, counter_bucket)` rows, which creates WAL and dead tuples even after ready rows became append-only.

Blindly removing the counter update is not acceptable. It makes `queue_counts_exact` either scan `done_entries` or undercount terminal rows, and it weakens the operational contract that queue counts are exact when the trust marker is set.

## Decision

Do not remove `queue_terminal_live_counts` from the completion transaction unless a replacement preserves exact counts. The safe replacement shape is an append-only terminal-count delta ledger plus maintenance rollup:

1. Terminal mutations append a narrow delta row in the same transaction as the `done_entries` insert/delete:
   - completion / terminal insertion writes a positive delta,
   - retry / purge / compatibility delete writes a negative delta,
   - the delta key is the same counter key used today: `(ready_slot, queue, priority, enqueue_shard, counter_bucket)`.
2. Completion batches keep the existing grouping step, but `INSERT` grouped deltas instead of `UPSERT`ing the live counter. A 512-job batch that touches one queue/priority/shard/bucket group should append one delta row, not 512.
3. `queue_counts_exact` reads: `queue_terminal_live_counts + SUM(unrolled terminal_count_deltas)`. The exact read remains honest while maintenance is behind.
4. Maintenance rolls closed delta segments into `queue_terminal_live_counts` in deterministic key order, then truncates those delta segments in the same transaction.
5. The trust marker remains meaningful: it means the base counter plus all unrolled deltas is complete for the active schema. Rebuild recomputes the base counter from `done_entries` and clears/truncates the delta ledger.

The ledger should be ring-partitioned. The hot path appends to the current delta segment; maintenance only rolls and truncates closed segments. That keeps the common lifecycle append-only and avoids replacing one dead-tuple source with another.

## Correctness Requirements

- The delta append must commit atomically with the terminal mutation it describes. If the state transition rolls back, the delta rolls back.
- Exact reads must include every committed terminal mutation exactly once: either in the rolled-up counter or in the unrolled delta sum.
- Rollup must be crash-safe and idempotent. Applying deltas and advancing the segment/rollup marker must commit together.
- Rollup must acquire counter rows in a deterministic order, matching the current deadlock-avoidance rule for direct counter updates.
- Segment prune must not truncate delta rows that have not been rolled up or included by the exact-read path.
- TLA+ storage models must gain explicit `TerminalDelta` and `RollupDelta` actions before this design can move from proposed to accepted.

## Consequences

Positive:

- Removes the remaining mutable terminal-counter update from the short-job completion hot path.
- Preserves exact queue counts without scanning `done_entries` on every admin read.
- Keeps the durability and at-least-once job safety contract unchanged: the ledger describes terminal rows, it does not decide whether a job completed.
- Gives maintenance a clear place to trade freshness for lower WAL/dead-tuple pressure.

Negative:

- Adds another ring-partitioned storage family and maintenance branch.
- Exact count reads must sum pending deltas; if maintenance is badly delayed, that sum can grow. The table is narrow and batched, but the read path still needs metrics and tests around delta backlog.
- Rebuild and upgrade paths become more complex because they must coordinate the base counter and pending deltas.

## Alternatives Considered

### Drop the counter and scan `done_entries`

Rejected for the normal path. It is simple and exact, but it makes a common admin/UI read proportional to retained terminal history. That moves cost from completion to observability rather than removing it.

### Make terminal counts eventually consistent

Rejected for the exact-count path. Awa exposes exact queue counts and uses the trust marker to make that contract explicit. Eventual counts may be useful for a cheaper overview endpoint, but they should not replace `queue_counts_exact`.

### Keep striped live counters only

Counter bucketing reduces contention, but it still updates mutable rows in the completion path. It remains the conservative implementation until the delta ledger is implemented and validated.

## Relationship To Other ADRs

- Refines ADR-019 queue storage by moving one more hot-path structure toward append-only segment rotation.
- Refines ADR-026 narrow terminal history. The terminal row remains the source of truth; the delta ledger is derived count evidence.
- Complements ADR-023 receipt rings and the ready tombstone ledger: all three prefer append-only hot paths and maintenance-time truncation.

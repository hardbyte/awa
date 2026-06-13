# ADR-032: Failed terminal retention floor

## Status

Accepted.

## Context

Queue-storage terminal rows live in the rotating `done_entries_*` ring and are reclaimed by queue-ring prune once the matching ready segment is no longer live. Prune is segment-coarse: when the oldest sealed queue slot is reclaimable, the prune transaction `TRUNCATE`s the ready, tombstone, terminal, and delta children for that slot in one shot. ADR-026 made this cheap and correct for ordinary terminal history by treating ready and terminal rows as one retention unit.

That coarseness creates a silent-loss cliff for `failed` terminal rows. A job that exhausts its handler with retries left, or a queue that does not opt into the DLQ, lands a `failed` terminal fact in `done_entries`. An operator who wants to re-drive those failures — `retry_failed_by_queue`, `retry_failed_by_kind`, or a manual scan — has only until the slot rotates out and prune truncates it. There is no per-row retention applied to a `failed` row before the segment is reclaimed: the row is dropped on the floor of the prune `TRUNCATE` with no warning and no record that it ever existed past the terminal counters. The window is whatever `queue_slot_count` × rotation cadence happens to be, which is a churn-control knob, not a retention policy. Operators who reasonably expect "I can retry recent failures" get a window that silently shrinks as throughput rises.

The mechanism that drops them is the prune `TRUNCATE` itself. From the prune path:

> `FOR UPDATE` on `queue_ring_state` and `queue_ring_slots[slot]`, then `LOCK TABLE ... ACCESS EXCLUSIVE`, recheck active rows, then `TRUNCATE`; ready-backed terminal rows, ready tombstones, and pending terminal-count deltas are reclaimed with their retained ready bodies.

A `failed` row in the truncated slot is reclaimed alongside the rest of the segment. Nothing carries it forward, and nothing tells the operator it is gone.

## Decision

Queue-storage prune gains a **failed-retention floor**: a `failed` terminal row is retryable for at least `failed_retention` past its `finalized_at`, regardless of how fast its queue slot rotates. The floor is enforced by **carry-forward inside the prune transaction**, not by blocking the slot.

### Carry-forward

`QueueStorage::prune_oldest` takes a `failed_retention: Duration` parameter (`Duration::ZERO` disables the floor and reproduces the old behavior; granularity is whole seconds). Inside the single prune transaction, before the `TRUNCATE`:

1. **Identify carried rows.** In the slot being pruned, select `done_entries` rows with `state = 'failed' AND finalized_at >= now() - failed_retention`. These are inside the floor and must survive.
2. **Widen them.** ADR-026 made ready-backed `failed` rows narrow — `args`, `max_attempts`, `run_at`, `created_at`, `unique_key`, and `unique_states` are `NULL` and hydrate from the retained `ready_entries` row in the same segment. The carry hydrates those fields from the ready child and produces self-contained **wide** rows, the same shape as a synthetic (non-ready-backed) terminal row, so the carried fact no longer depends on any ready row.
3. **Re-insert into the live segment.** The widened rows are inserted through the `{schema}.done_entries` parent with `ready_slot = current_slot` and `ready_generation = ` the current slot's generation, both read in-transaction from the `queue_ring_state` row already locked `FOR UPDATE` (which also serializes against rotate — see Guarantees). LIST partitioning routes them to the live done child, which is not in the `TRUNCATE` list. The PK cannot collide because `lane_seq` comes from never-resetting global sequences; there is deliberately no `ON CONFLICT` clause so a violation aborts the transaction loudly rather than silently dropping a terminal fact.
4. **Re-append the terminal-count deltas.** The carried rows' positive terminal-count deltas are re-appended under the current slot, mirroring the completion path's delta write exactly (state dimension and `job_id` counter bucketing included), so exact counts stay consistent across the move.

The carried rows are then **excluded from the rollup fold** that the prune transaction writes for the truncated slot. The fold splits the truncated terminal rows into two count dimensions: non-`failed` terminal rows go to `pruned_completed_count`, and `failed` rows whose `finalized_at < cutoff` (older than the floor, genuinely being dropped) go to `pruned_failed_count`. Carried `failed` rows are in neither — they remain live in the new slot.

### Count split and operator visibility

Migration v032 adds `pruned_failed_count` to `{schema}.queue_terminal_rollups`. `pruned_completed_count` keeps its name (additive-only migration policy) and now means "non-`failed` pruned terminal rows". The sum of the two columns is always the correct total of terminal rows that have left the live ring — see Consequences for the mixed-version honesty argument.

`QueueCounts` gains a `pruned_failed: i64` field, sourced from `SUM(queue_terminal_rollups.pruned_failed_count)` over the queue. It is cumulative and monotonic: rollups never decrease, so this is a standing, queryable record of how many `failed` rows have aged past the floor and are no longer retryable. Operators get visibility of the loss that the old design hid.

`retry_failed_by_kind` and `retry_failed_by_queue` return a `RetryFailedOutcome { retried, matched, pruned_failed_count }`. `matched` is the scan count before retry, so `matched - retried.len()` reports ids that raced or were pruned out from under the retry rather than silently dropping them. `pruned_failed_count` is `Some(cumulative rollup sum for the queue)` on the by-queue path and `None` on the by-kind path, because rollups have no kind dimension and faking a global number there would be dishonest. The CLI surfaces both: it reports the matched/raced split when they differ and, when `pruned_failed_count` is `Some(n > 0)`, appends that `N` failed rows have been pruned past retention and are no longer retryable.

### Reuse of `failed_retention`

The floor reuses the existing `failed_retention` knob (default `72h`) rather than introducing a new one. On the canonical compatibility path `failed_retention` already bounds how long `failed` rows stay queryable; in queue storage it now bounds how long they stay **retryable** before carry-forward stops carrying them. One knob, one intent — failed terminal rows survive for at least `failed_retention` — implemented differently per engine. `MaintenanceService::rotate_queue_storage_queue` passes `self.failed_retention` to `prune_oldest`; direct callers pass an explicit `Duration`.

### `cancelled` is excluded

The floor applies to `state = 'failed'` only. `cancelled` rows are always prunable when their segment is. Cancellation is an explicit operator (or application) action that already had its decision moment; re-driving a cancelled job is not the recovery path the floor protects. Holding cancelled rows past their segment would carry retention cost for a class nobody asked to keep retryable.

## Guarantees

### Crash safety

Carry-forward, the rollup fold, the `TRUNCATE`, and the delta re-append all run inside the **single existing prune transaction**. There is no intermediate committed state where a carried `failed` row has been truncated from the old slot but not yet re-inserted into the live slot, or where its count delta has been re-appended but the source row not yet truncated. The transaction either commits as a whole — old slot truncated, carried rows wide in the live slot, rollup reflecting only the genuinely-dropped rows, deltas consistent — or rolls back to the pre-prune state. A crash mid-prune leaves the carried rows in the old slot, retryable, and a later prune tick retries the whole operation. No terminal fact is observable in zero or two places.

### Rotate-vs-carry serialization

The destination of the carry is the *current* slot. If a rotate advanced `current_slot` between the carry's read of the destination and the carry's insert, the rows could land in a slot that is itself about to be sealed and pruned — re-creating the cliff one slot over. This cannot happen because both prune and rotate take `queue_ring_state` `FOR UPDATE`, and prune holds that lock for the whole transaction:

- Prune acquires `queue_ring_state FOR UPDATE`, reads `current_slot` / generation, carries the rows into that slot, folds, truncates the old slot, re-appends deltas, and commits — all under the held lock.
- A concurrent `rotate_ready` that wants to advance `current_slot` must take the same `queue_ring_state FOR UPDATE` and therefore blocks until the prune transaction commits or aborts.

So the destination slot the carry reads is stable for the life of the prune transaction: a rotate can only advance `current_slot` *after* prune commits, by which point the carried rows are already durably in what was the live slot, and that slot is now simply one slot back in the ring with a full retention window ahead of it. The interleaving "carry reads slot N, rotate flips to N+1, prune truncates N+1's predecessor" is unreachable because the rotate cannot proceed while prune holds the lock.

## Relationship to ADR-019, ADR-020, ADR-023, and ADR-026

- **Amends ADR-026.** ADR-026's "Queue-prune logic must continue treating ready and terminal rows as one retention unit" consequence no longer holds without qualification for `failed` rows inside the floor. A carried `failed` row is deliberately decoupled from its retained ready body: the carry widens it into a self-contained terminal row (the synthetic / non-ready-backed shape ADR-026 already defines for unclaimed-cancellation and scheduled-cancellation rows) and re-inserts it into the live segment, leaving the original ready body to be reclaimed with the truncated slot. The "one retention unit" property still holds for ordinary terminal history; the floor is the documented exception, and it is safe precisely because ADR-026 already has a wide synthetic terminal shape that does not depend on a ready row.
- **Refines ADR-019** the same way ADR-026 does — it touches the queue-storage terminal-family prune path, not the lifecycle or lease/claim planes. Prune's lock discipline (ADR-019 / the lock-order model) is unchanged; carry-forward adds work inside the same locked transaction without taking new locks.
- **ADR-020 (DLQ) is unaffected.** The DLQ is a separate opt-in hold table with its own `dlq_retention`. The floor is for non-DLQ `failed` terminal rows — the failures a queue without DLQ leaves in `done_entries`. A queue that routes terminal failures to the DLQ never produces the `failed` `done_entries` rows the floor protects.
- **ADR-023 (receipt plane) is unaffected.** `carried_failed_rows` is always `0` for the lease and claim rings; the floor lives only on the queue-storage terminal ring.

## Consequences

### Positive

- Failed terminal rows are retryable for at least `failed_retention` regardless of queue rotation speed. The retention window is a policy, not a side effect of churn-control sizing.
- The loss that does happen — failures aged past the floor — is recorded in `pruned_failed_count` and surfaced through `QueueCounts.pruned_failed`, so it is observable instead of silent.
- Retry outcomes report `matched` vs `retried` and a pruned-past-retention signal, so an operator re-driving failures knows when ids raced or were dropped rather than assuming a clean retry.
- No new locks, no new background pass, no second terminal family: the floor is extra work inside the prune transaction that already exists.

### Negative

- `pruned_completed_count` is misnamed for what it now holds. The additive-only migration policy forbids renaming or retyping a column, so it keeps its name and now means "non-`failed` pruned terminal rows". Code and SQL reading rollups must read both columns to get the terminal total. (Documented as-is; the rename cost is not worth a breaking migration.)
- **Pre-v032 mixed-version honesty.** A binary that predates v032 has no `pruned_failed_count` column to write, so it folds *all* pruned terminal rows — failed and non-failed — into `pruned_completed_count`. During a rolling upgrade the failed/non-failed split is therefore best-effort: rows pruned by an old binary are counted as completed. The total is never wrong — `pruned_completed_count + pruned_failed_count` is always the correct count of terminal rows that have left the ring — but `pruned_failed_count` undercounts for failures an old binary pruned. Once every binary is on v032 the split is exact going forward.
- Carrying rows forward keeps `failed` history in the live ring longer, costing some ring space proportional to failed-row volume × `failed_retention`. For queues with high failed volume and a long floor this is a real cost; lower `failed_retention` or route failures to the DLQ if it matters.
- Prune does slightly more work per tick when carrying rows: a select, a wide insert, and a delta append on top of the fold and truncate. It stays one transaction and one slot.

## Alternatives Considered

### Block the slot until the youngest failed row ages out

Refuse to prune a sealed slot while it holds a `failed` row inside the floor. Rejected: prune scans the single coldest slot per tick by design, and a slot can carry an in-floor `failed` row for the full `failed_retention` window. Blocking it stalls reclamation of *everything* in that slot — ready bodies, tombstones, completed terminal rows, deltas — behind one young failure, for up to `failed_retention`. That re-introduces exactly the dead-tuple / unprunable-segment pressure ADR-026 and the #169 work removed. Carry-forward keeps the coldest-slot scan free to reclaim on schedule.

### Row-level `DELETE` of the prunable rows

Delete just the genuinely-prunable rows from the sealed slot and leave the in-floor `failed` rows in place. Rejected: it violates the `AwaDeadTupleContract` `PartitionTruncate` reclaim pin and ADR-019's contract that these partitions are reclaimed by `TRUNCATE`, not row vacuum. Row-level deletes turn an append-only / truncate-reclaimed partition into a row-vacuum table, which is the dead-tuple shape the storage redesign exists to avoid.

### A separate failed-hold table

Carry in-floor `failed` rows to a dedicated retention table outside the queue ring. Rejected: it adds a second terminal family with its own read path, prune, counts, and retry surface — exactly the cost ADR-026 rejected for the success-receipt-table alternative ("another terminal source for counts, admin reads, retry/replay tools, and prune"). The carry-forward design keeps one durable terminal family by reusing the wide synthetic terminal shape that already exists.

## Validation

- Runtime tests assert that a `failed` row inside `failed_retention` survives a prune of its original slot as a wide, self-contained terminal row in the live slot, hydrates correctly, and remains retryable; a `failed` row past the floor is pruned and folded into `pruned_failed_count`.
- The terminal-count consistency oracle (`test_queue_terminal_live_counts_rebuild_restores_invariant`) and the terminal-counter trust-marker test cover that carry-forward keeps exact counts consistent: carried rows' deltas re-appended under the new slot, dropped rows' counts folded into rollups.
- `correctness/storage/AwaShardedPrune.tla` models ready-backed (narrow) terminal rows whose done rows come from a ready row in the same segment. Carried `failed` rows enter the synthetic / wide terminal class, which that model does not represent; `correctness/storage/MAPPING.md` documents the boundary.

## Relationship to Other ADRs

- Amends ADR-026's one-retention-unit consequence for in-floor `failed` rows (see above).
- Refines ADR-019's queue-storage terminal-family prune path.
- Independent of ADR-020 (DLQ) and ADR-023 (receipt plane).

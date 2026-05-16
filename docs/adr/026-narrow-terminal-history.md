# ADR-026: Narrow Terminal History

## Status

Accepted.

## Context

The queue-storage runtime intentionally keeps the runnable row immutable in
`ready_entries` and writes a durable terminal fact to `done_entries` when an
attempt completes, fails, or is cancelled. That shape is safe and easy to
inspect, but it duplicates the immutable job body on the hottest successful
completion path: `args`, `max_attempts`, `run_at`, `created_at`, uniqueness
metadata, and often an unchanged runtime payload are written once in
`ready_entries` and then again in `done_entries`.

The duplication is visible in the WAL and row-size budget. Local Awa-only
experiments on 2026-05 measured:

| Shape | Completed jobs/s | p99 e2e | WAL/job |
|---|---:|---:|---:|
| Tuned production queue storage | ~7,885/s | ~203 ms | ~2,241 B |
| Narrow terminal history prototype | ~8,337/s | ~205 ms | ~1,932 B |
| Skip `done_entries` entirely | ~8,402/s | ~230 ms | ~1,441 B |

Skipping `done_entries` is not acceptable: it weakens the durable terminal
history contract and removes the operator/API source of truth. The useful
signal is that most of the safe gain comes from avoiding duplicated terminal
body bytes, not from deleting the terminal fact.

## Decision

For terminal rows whose source attempt still has a retained ready row, write a
narrow `done_entries` row:

- keep terminal identity and ordering fields:
  `ready_slot`, `ready_generation`, `job_id`, `kind`, `queue`, `state`,
  `priority`, `attempt`, `run_lease`, `lane_seq`, `enqueue_shard`,
  `attempted_at`, and `finalized_at`
- keep `payload` only when the terminal runtime payload differs from the
  ready-row payload
- leave duplicated immutable body fields nullable and normally `NULL`:
  `args`, `max_attempts`, `run_at`, `created_at`, `unique_key`,
  and `unique_states`

Reads that materialize a `JobRow` or move a terminal row to another storage
family hydrate through a left join from `done_entries` to the retained
`ready_entries` row using the ready segment and lane key:

```text
(ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq)
```

Schema preparation also creates a read-only `{schema}.terminal_jobs` view with
the hydrated terminal shape. It is the preferred SQL surface for inspection,
reporting, and external read-only tooling. The physical `done_entries` table
remains the write/transition surface because retry, DLQ move, and discard
paths must delete the terminal fact and its retained ready backing row in one
transaction.

Terminal rows are narrow only when the source state keeps the ready row:
`running` and `waiting_external` attempts. Paths that delete the ready row
before terminalizing, such as cancelling an unclaimed `available` job, still
write a wide `done_entries` row. Scheduled/deferred cancellation also remains
wide because there is no ready backing row.

When a terminal row is deleted, the retained ready backing row must be deleted
in the same transaction before retry, DLQ move, or discard continues. Otherwise
the backing row would become live again as soon as the terminal fact disappears.

Queue-ring prune reclaims the ready body and the terminal fact together. The
TLA+ model records this as `TerminalHasRetainedReadyBody`: every modelled
terminal row has a retained ready body until queue prune removes both.

## Consequences

### Positive

- Keeps the durable terminal fact and all existing public API semantics.
- Reduces WAL and row bytes on the dominant completion path.
- Preserves `attempted_at` and `finalized_at` in `done_entries`, so terminal
  attempt timing remains directly inspectable without reconstructing it from a
  ready row.
- Keeps the ring-prune safety story unchanged: `ready_entries` and
  `done_entries` are still reclaimed by queue-slot truncation.

### Negative

- Direct SQL against `done_entries.args`, `max_attempts`, `run_at`,
  `created_at`, `unique_key`, `unique_states`, or `payload` must tolerate
  `NULL` on ready-backed terminal rows. Use `{schema}.terminal_jobs` unless
  code intentionally needs the physical storage representation.
- Terminal delete paths have one more responsibility: remove the retained
  ready backing row before re-enqueueing, moving to DLQ, or discarding.
- Queue-prune logic must continue treating ready and terminal rows as one
  retention unit.

## Alternatives Considered

### Keep fully materialized terminal rows

This is the pre-ADR behavior. It is simple for direct SQL users but spends
extra WAL and heap bytes on every terminal transition.

### Remove `done_entries` for successful completions

This produced the lowest WAL/job result in experiment, but it removes the
durable terminal fact that queue counts, `load_job`, admin inspection, and
retention currently rely on. It is rejected.

### Store a separate success-receipt table

A success-receipt table would make successful completions even narrower, but it
would add another terminal source for counts, admin reads, retry/replay tools,
and prune. The retained-ready design gets most of the safe benefit while
keeping one durable terminal family.

## Defaults

This ADR does not change user-facing defaults:

- `enqueue_shards = 1` remains the strict-FIFO default; use more shards only
  when partitioned FIFO is acceptable.
- `claimers = 1` and `claim_batch_size = 128` remain the queue defaults.
- `AWA_COMPLETION_BATCH_SIZE = 256`, `AWA_COMPLETION_FLUSH_MS = 5`, and
  queue-storage `AWA_COMPLETION_SHARDS = 1` remain the completion defaults.
- `queue_slot_count = 16`, `lease_slot_count = 8`, `claim_slot_count = 8`,
  and `lease_claim_receipts = true` remain the queue-storage defaults.

The first tuning move for overload remains semantic enqueue sharding when the
workload can accept partitioned FIFO. This ADR lowers the durable completion
write budget underneath those defaults.

## Validation

- Runtime tests assert that completed and failed ready-backed terminal rows are
  narrow, hydrate correctly through `load_job`, and can retry without
  resurrecting the retained ready backing row.
- Runtime tests assert that the `{schema}.terminal_jobs` compatibility view
  hydrates ready-backed terminal rows while the physical `done_entries` row
  remains narrow.
- Runtime tests assert that cancelling an unclaimed available job writes a wide
  terminal row because no ready backing row survives that transition.
- Existing DLQ move, bulk move, retry, and discard flows hydrate terminal rows
  before moving them and delete retained backing rows when the terminal fact is
  removed.
- `correctness/storage/AwaSegmentedStorage.tla` models retained terminal ready
  bodies, terminal-to-DLQ backing-row deletion, and queue prune reclaiming the
  ready body and terminal fact together.

## Relationship to Other ADRs

- Refines ADR-019's queue-storage terminal family.
- Preserves ADR-023's receipt-plane safety: completion still closes the exact
  claim and writes a durable terminal fact.
- Complements ADR-025: enqueue sharding attacks producer head-row contention;
  this ADR reduces completion-side WAL/row pressure.

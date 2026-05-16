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

The duplication is visible in the WAL and row-size budget. Reference
queue-storage runs showed this shape:

| Shape | Completed jobs/s | p99 e2e | WAL/job |
|---|---:|---:|---:|
| Tuned production queue storage | `7,885/s` | `203 ms` | `2,241 B` |
| Narrow terminal history | `8,337/s` | `205 ms` | `1,932 B` |
| Skip `done_entries` entirely | `8,402/s` | `230 ms` | `1,441 B` |

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
retention rely on. It is rejected.

### Store a separate success-receipt table

A success-receipt table would make successful completions even narrower, but it
would add another terminal source for counts, admin reads, retry/replay tools,
and prune. The retained-ready design gets most of the safe benefit while
keeping one durable terminal family.

## Defaults

This ADR changes the batching defaults that determine whether lower WAL turns
into durable throughput:

- `enqueue_shards = 1` remains the strict-FIFO default; use more shards only
  when partitioned FIFO is acceptable.
- `claimers = 1` and `claim_batch_size = 512` remain the queue defaults.
- `AWA_COMPLETION_BATCH_SIZE = 512`, `AWA_COMPLETION_FLUSH_MS = 1`, and
  queue-storage `AWA_COMPLETION_SHARDS = 1` remain the completion defaults.
- `queue_slot_count = 16`, `lease_slot_count = 8`, `claim_slot_count = 8`,
  and `lease_claim_receipts = true` remain the queue-storage defaults.

The queue-storage short-job completion path also uses one fused statement for
the common receipt-backed, payload-empty terminal transition: close the receipt
in `lease_claim_closures` and insert the narrow `done_entries` fact from the
claimed runtime snapshot in the same SQL statement. Jobs with unique-key
transitions, terminal payload metadata, materialized heartbeat/progress state,
or missed receipt closures keep the general transaction path.

The offered-rate benchmark turns that lower write budget into an explicit
capacity check. With one queue shard, one claimer, `claim_batch_size = 512`,
queue-storage completion shards at `1`, and `max_workers = 1024`, a 10-second
`10k/s` no-op workload keeps durable completions at the offered rate, drains to
zero backlog, and writes `1.8 KiB` WAL per completed job. A `12k/s` offered
workload exceeds that reference configuration and accumulates backlog during
the offer window before draining afterward.

## Relationship to Rejected ADR-024

The relevant historical ADR-024 was **Deferred `done_entries`
Materialisation**. It removed the `done_entries` insert from receipt completion
entirely, treated
`lease_claim_closures` as the immediate completion source of truth, and added a
background materializer plus read-side synthetic projections and prune guards.
Its A/B benchmark was worse than the synchronous path: `1,839/s` vs `2,803/s`,
with higher p99 latency. It also added new moving
parts: a materializer cadence, materializer lag monitoring, rotation catch-up,
and temporary closure-without-terminal read semantics.

ADR-026 deliberately keeps the opposite contract:

- completion still writes the durable terminal fact synchronously;
- `done_entries` remains the terminal source of truth for counts, admin reads,
  retries, DLQ moves, discard, and retention;
- there is no materializer, no lag window, and no extra prune precondition;
- the fast path is an implementation detail of the same logical transition:
  only claims that are successfully closed in `lease_claim_closures` are
  inserted into `done_entries`.

The overlap with ADR-024 is the insight that duplicating ready-body JSONB is
wasteful. The difference is where the system draws the safety boundary:
ADR-024 deferred the terminal fact; ADR-026 keeps the terminal fact durable and
only narrows its payload, then fuses the receipt-close and terminal-insert SQL
so the synchronous contract is cheap enough to hit the throughput target.

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
- Supersedes the useful part of the rejected ADR-024 deferred-materialisation
  experiment without adopting its asynchronous terminal-history contract.

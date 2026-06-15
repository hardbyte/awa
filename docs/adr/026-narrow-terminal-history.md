# ADR-026: Narrow Terminal History

## Status

Accepted. The "Queue-prune logic must continue treating ready and terminal rows as one retention unit" consequence below is **amended by [ADR-032: Failed terminal retention floor](032-failed-terminal-retention.md)** for `failed` terminal rows inside the `failed_retention` floor: queue prune carries those rows forward into the live segment as wide self-contained terminal rows (the synthetic shape this ADR already defines for unclaimed/scheduled cancellation), decoupling them from the retained ready body so they stay retryable past their original segment. Ordinary terminal history is still one retention unit.

## Context

The queue-storage runtime intentionally keeps the runnable row immutable in `ready_entries` and records durable terminal history when an attempt completes, fails, or is cancelled. The first queue-storage shape wrote every terminal fact to `done_entries`. That shape was safe and easy to inspect, but it duplicated the immutable job body on the hottest successful completion path: `args`, `max_attempts`, `run_at`, `created_at`, uniqueness metadata, and often an unchanged runtime payload were written once in `ready_entries` and then again in `done_entries`.

The duplication is visible in the WAL and row-size budget. Reference queue-storage runs showed this shape:

| Shape                          | Completed jobs/s |  p99 e2e |   WAL/job |
| ------------------------------ | ---------------: | -------: | --------: |
| Tuned production queue storage |        `7,885/s` | `203 ms` | `2,241 B` |
| Narrow terminal history        |        `8,337/s` | `205 ms` | `1,932 B` |
| Skip `done_entries` entirely   |        `8,402/s` | `230 ms` | `1,441 B` |

Skipping durable terminal history is not acceptable: it weakens the operator/API source of truth. The useful signal is that most of the safe gain comes from avoiding duplicated terminal body bytes, not from deleting the terminal fact.

## Decision

For terminal rows whose source attempt still has a retained ready row, avoid copying immutable ready-body fields into terminal history.

For failed, cancelled, non-receipt, and wide terminal rows, write a narrow `done_entries` row:

- keep terminal identity and ordering fields: `ready_slot`, `ready_generation`, `job_id`, `kind`, `queue`, `state`, `priority`, `attempt`, `run_lease`, `lane_seq`, `enqueue_shard`, `attempted_at`, and `finalized_at`
- keep `payload` only when the terminal runtime payload differs from the ready-row payload
- leave duplicated immutable body fields nullable and normally `NULL`: `args`, `max_attempts`, `run_at`, `created_at`, `unique_key`, and `unique_states`

Reads that materialize a `JobRow` or move a terminal row to another storage family hydrate through a left join from `done_entries` to the retained `ready_entries` row using the ready segment and lane key:

```text
(ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq)
```

For successful receipt-backed completions that do not need wide terminal body fields, write compact rows to `receipt_completion_batches` instead of one `done_entries` row per completed job. Each compact terminal batch stores arrays of `(job_id, run_lease, lane_seq, attempt, attempted_at)` plus the shared ready segment, queue, priority, shard, finalized timestamp, and originating claim slot. The same completion transaction also writes a compact claim-local row to `lease_claim_closure_batches`, keyed by immutable, globally allocated `lease_claims.receipt_id` values and a derived `int8multirange` for indexed membership checks. The claim transaction writes compact `ready_claim_attempt_batches` range evidence in the ready row's queue slot before writing receipt claims, so stale claim-cursor recovery can prove that covered lanes already emitted attempts without reading claim-ring parent tables. The runtime routes claim lookup and compact closure writes to the originating `claim_slot` child partitions, while compact terminal history, terminal-count deltas, and claim-attempt evidence remain queue-slot routed. The compact closure batch carries the ready slot and generation for the completed group, so queue and claim prune can first compare exact row counts for a sealed segment and only fall back to per-claim membership checks when counts do not prove closure. Terminal history and claim-closure proof are therefore separate append-only ledgers, so successful completions do not need per-job `lease_claim_closures` rows and stale receipt scans do not need to unnest terminal-history arrays.

Receipt stale-rescue and deadline-rescue cursors scan only claims that are old or expired enough to be rescue candidates, so long-lived retained claim partitions do not make each maintenance tick re-prove fresh claims.

The compact path is only for receipt-backed jobs with no unique key, no tags, no terminal errors, and no custom metadata. Awa-owned provenance metadata (`_awa_original_priority` and `_awa_original_queue`) remains compact-safe: it is either already present on the retained ready payload, or it is claim-time priority-aging provenance that does not require duplicating the whole terminal body in `done_entries`.

Schema preparation creates a read-only `{schema}.terminal_jobs` view with the hydrated terminal shape. It is the preferred SQL surface for inspection, reporting, and external read-only tooling. The view expands compact completion batches and joins both physical terminal families back to retained `ready_entries`. The physical `done_entries` table remains the write/transition surface for terminal rows that can be retried, moved to DLQ, discarded, or carried forward by failed-retention pruning. Compact completed rows are not retryable; SQL compatibility delete hides them by writing `receipt_completion_tombstones`. That compatibility delete expands retained compact batches to find the target job; Awa does not maintain a `job_ids` GIN index because that would add WAL to every successful compact completion for a cold administrative path.

The retained ready backing row remains immutable and inert until queue prune reclaims the segment.

Terminal rows are narrow only for claimed attempts whose immutable body is already represented by a retained ready row: `running` and `waiting_external` attempts. Cancelling an unclaimed `available` job tombstones the ready lane and writes a wide `done_entries` row because the job never had a claimed attempt snapshot. Scheduled/deferred cancellation also remains wide because there is no ready backing row in the ready ring.

When a terminal row is deleted, the retained ready backing row is not deleted and does not become live again. A ready-backed terminal row was already claimed, so the lane is behind the claim cursor; retry or queue-move paths append a fresh ready row at a new lane position. Ready-row cleanup remains segment-level work owned by queue prune.

Queue-ring prune reclaims retained ready bodies, ready tombstones, compact completion batches, compact completion tombstones, pending terminal-count deltas, and any remaining terminal facts for the segment together. The TLA+ model records this as `TerminalHasRetainedReadyBody`: every modelled ready-backed public terminal row has a retained ready body until the terminal fact is moved/deleted or queue prune removes the segment.

Exact terminal counts are part of this terminal-history contract. Awa keeps them exact with three derived stores:

- `queue_terminal_live_counts`, keyed by `(ready_slot, queue, priority, enqueue_shard, counter_bucket)`, stores folded counts for retained queue segments.
- `queue_terminal_count_deltas_*`, partitioned with the queue ring, stores pending signed deltas for terminal mutations that have not been folded yet.
- `queue_terminal_rollups` stores permanent counts for pruned queue segments.

Terminal mutations append a narrow delta row in the same transaction as the physical terminal mutation:

- completion / terminal insertion writes a positive delta, whether the physical row is a `done_entries` row or a compact receipt completion batch;
- retry, purge, discard, DLQ move, and compatibility delete write a negative delta;
- the delta key includes `ready_generation`, so reused `ready_slot` partitions cannot inherit stale deltas.

Compact receipt completion batches keep the existing grouping step, but append grouped deltas instead of `UPSERT`ing the live counter. A 512-job compact batch that touches one queue/priority/shard group appends one positive delta row, not 512. Wide `done_entries` terminal rows keep job-id counter bucketing so later terminal deletes cancel their original positive bucket exactly.

`queue_counts_exact` computes the live terminal component as `queue_terminal_live_counts + SUM(queue_terminal_count_deltas)`, then adds pruned rollups from `queue_terminal_rollups` / `queue_lanes`. The exact read remains honest while maintenance is behind. If the terminal-counter trust marker is not set, the read path falls back to scanning `{schema}.terminal_jobs` so rolling upgrades and recovery windows stay honest.

The maintenance leader rolls sealed delta segments into `queue_terminal_live_counts` in deterministic key order, then truncates the matching delta child in the same transaction. Candidate selection is driven by sealed slots that actually contain pending deltas, so an empty old prefix cannot hide a later sealed slot that needs rollup. Rollup skips the current queue slot and any slot with active leases, retained ready rows at or beyond their lane claim cursors, or open receipt claims. The retained-ready check runs before the exclusive delta-child lock and before the receipt-closure proof, because a slot with pending ready rows cannot be rolled up or pruned yet. Once the delta child is locked, rollup proves receipt claims are closed with the same exact-count proof queue prune uses and only falls back to per-claim membership checks when counts do not prove closure.

Rollup is also MVCC-horizon aware. If PostgreSQL reports another backend with an open snapshot in the current database, or an idle transaction with an assigned transaction id, maintenance returns before mutating `queue_terminal_live_counts` or locking the delta child. The pending deltas remain append-only and exact reads remain honest because they include the delta sum. When the horizon clears, a later maintenance tick folds the sealed deltas. Queue prune also truncates the delta child after folding terminal history into permanent rollups, so a lagging counter rollup cannot block retention.

The trust marker remains meaningful: it means the folded counter plus all unrolled deltas is complete for the active schema. Rebuild recomputes the folded counter from `{schema}.terminal_jobs` and clears the delta ledger.

Correctness requirements for the delta ledger:

- The delta append must commit atomically with the terminal mutation it describes. If the state transition rolls back, the delta rolls back.
- Exact reads must include every committed terminal mutation exactly once: either in the rolled-up counter or in the unrolled delta sum.
- Rollup must be crash-safe and idempotent. Applying deltas and truncating the delta segment must commit together.
- Rollup must acquire counter rows in deterministic order.
- Rollup may defer while the MVCC horizon is pinned, but the deferral must happen before mutating folded counters or truncating pending deltas.
- Segment prune may truncate pending delta rows only because it first folds terminal history from `terminal_jobs` into permanent rollups; exact reads no longer need those pending deltas after the terminal segment is pruned.
- The storage TLA+ models track terminal deltas as append-only, partition-truncated derived state; they do not make job safety depend on counter rollup timing.

## Consequences

### Positive

- Keeps the durable terminal fact and all existing public API semantics.
- Reduces WAL and row bytes on the dominant completion path.
- Preserves `attempted_at` and `finalized_at` in terminal history, so terminal attempt timing remains inspectable through `{schema}.terminal_jobs`.
- Keeps the ring-prune safety story unchanged: ready rows, terminal rows, compact completion batches, compact tombstones, and terminal deltas are still reclaimed by queue-slot truncation after the queue slot is proven inactive.

### Negative

- Direct SQL against `done_entries.args`, `max_attempts`, `run_at`, `created_at`, `unique_key`, `unique_states`, or `payload` must tolerate `NULL` on ready-backed terminal rows. Direct SQL against `done_entries` must also tolerate successful receipt-backed completed jobs being absent from that table. Use `{schema}.terminal_jobs` unless code intentionally needs the physical storage representation.
- Terminal delete paths have one more responsibility: append the matching negative terminal-count delta before re-enqueuing, moving to DLQ, or discarding.
- Queue-prune logic must continue treating ready and terminal rows as one retention unit. ([ADR-032](032-failed-terminal-retention.md) amends this for `failed` rows inside the `failed_retention` floor, which prune carries forward as wide self-contained terminal rows.)
- Exact terminal-count reads must include both folded counters and pending deltas. Operational SQL that reads only `queue_terminal_live_counts` sees folded state, not the full exact count.
- Pending delta rows can accumulate while long reader transactions pin the MVCC horizon. That is intentional: it trades a larger append-only ledger for near-zero dead tuples in `queue_terminal_live_counts` during the pin.

## Alternatives Considered

### Keep fully materialized terminal rows

This is the pre-ADR behavior. It is simple for direct SQL users but spends extra WAL and heap bytes on every terminal transition.

### Remove durable terminal history for successful completions

This produced the lowest WAL/job result in experiment, but it removes the durable terminal fact that queue counts, `load_job`, admin inspection, and retention rely on. It is rejected.

### Store compact successful receipt completions synchronously

This is the chosen design. It adds another physical terminal family, but keeps one public terminal surface (`terminal_jobs`), one exact-count contract, and one queue-ring prune boundary. It deliberately does not add a background materializer or make completion visible before durable terminal history commits.

### Drop terminal counters and scan `done_entries`

Rejected for the normal exact-count path. It is simple and exact, but makes a common admin/UI read proportional to retained terminal history. That moves cost from completion to observability rather than removing it.

### Make terminal counts eventually consistent

Rejected for `queue_counts_exact`. Awa exposes exact queue counts and uses the trust marker to make that contract explicit. Eventual counts may be useful for a cheaper overview endpoint, but they should not replace the exact path.

### Keep striped live counters only

Counter bucketing reduces contention, but it still updates mutable rows in the terminal path. This is rejected for the high-throughput queue-storage path because benchmark evidence showed the live-counter table becoming the dominant dead-tuple source under pinned readers after ready rows became append-only.

## Defaults

This ADR changes the batching defaults that determine whether lower WAL turns into durable throughput:

- `enqueue_shards = 1` remains the strict-FIFO default; use more shards only when partitioned FIFO is acceptable.
- `claimers = 1` and `claim_batch_size = 512` remain the queue defaults.
- `AWA_COMPLETION_BATCH_SIZE = 512`, `AWA_COMPLETION_FLUSH_MS = 1`, and queue-storage `AWA_COMPLETION_SHARDS = 1` remain the completion defaults.
- `queue_slot_count = 16`, `lease_slot_count = 8`, `claim_slot_count = 8`, and `lease_claim_receipts = true` remain the queue-storage defaults.
- `terminal_count_rollup_interval = 30s` folds pending terminal-count deltas for sealed queue slots. Each tick processes at most four sealed slots. Exact reads include pending deltas, so this cadence affects compaction pressure, not correctness.

The queue-storage short-job completion path also uses one fused statement for the common receipt-backed, payload-empty terminal transition: insert compact completion batch rows, delete any matching `attempt_state`, and append matching terminal-count deltas from the claimed runtime snapshot. Before that statement, the transaction takes per-`(job_id, run_lease)` advisory locks in deterministic key order so duplicate completion attempts and known-key materialization or close paths serialize and observe the winning evidence. The fused statement skips claims that already have a materialized `leases` row; those attempts keep the general lease-deleting transaction path. A materialized receipt can be in a newer lease-ring slot than the original claim carried, so the general receipt fallback matches the lease by stable ready-lane identity and `(job_id, run_lease)`. Jobs with unique-key transitions, terminal payload metadata, materialized lease state, or missed receipt evidence also keep the general transaction path.

If a later retry, DLQ move, discard, or SQL compatibility delete removes a `done_entries` terminal row before claim prune has reclaimed the originating receipt, the terminal-delete path first materializes an explicit `lease_claim_closures` row. Compact successful completions keep their closure evidence in `lease_claim_closure_batches`; SQL compatibility delete hides them from public terminal reads by writing `receipt_completion_tombstones` and does not reopen the receipt.

The offered-rate benchmark turns that lower write budget into an explicit capacity check. With one queue shard, one claimer, `claim_batch_size = 512`, queue-storage completion shards at `1`, and `max_workers = 1024`, a 10-second `10k/s` no-op workload keeps durable completions at the offered rate, drains to zero backlog, and writes `1.8 KiB` WAL per completed job. A `12k/s` offered workload exceeds that reference configuration and accumulates backlog during the offer window before draining afterward.

## Relationship to Rejected ADR-024

The relevant historical ADR-024 was **Deferred `done_entries` Materialisation**. It removed the `done_entries` insert from receipt completion entirely, treated `lease_claim_closures` as the immediate completion source of truth, and added a background materializer plus read-side synthetic projections and prune guards. Its A/B benchmark was worse than the synchronous path: `1,839/s` vs `2,803/s`, with higher p99 latency. It also added new moving parts: a materializer cadence, materializer lag monitoring, rotation catch-up, and temporary closure-without-terminal read semantics.

ADR-026 deliberately keeps the opposite contract:

- completion still writes durable terminal history synchronously;
- `{schema}.terminal_jobs` is the terminal source of truth for counts and admin reads; `done_entries` remains the transition surface for retryable/movable terminal rows;
- there is no materializer, no lag window, and no extra prune precondition;
- the fast path is an implementation detail of the same logical transition: successful completion writes compact terminal history plus compact claim-local closure evidence atomically, while non-success and cold terminal-delete paths use `lease_claim_closures` with `done_entries`.

The overlap with ADR-024 is the insight that duplicating ready-body JSONB is wasteful. The difference is where the system draws the safety boundary: ADR-024 deferred the terminal fact; ADR-026 keeps the terminal fact durable and only narrows its payload, then fuses receipt-claim locking, terminal insert, and count-delta append so the synchronous contract is cheap enough to hit the throughput target.

The first tuning move for overload remains semantic enqueue sharding when the workload can accept partitioned FIFO. This ADR lowers the durable completion write budget underneath those defaults.

## Validation

- Runtime tests assert that completed and failed ready-backed terminal rows are narrow, hydrate correctly through `load_job`, and can retry without resurrecting the retained ready backing row.
- Runtime tests assert that the `{schema}.terminal_jobs` compatibility view hydrates ready-backed terminal rows while physical terminal rows remain narrow or compact.
- Runtime tests assert that successful compact receipt-backed completions write no completed `done_entries` row, appear in `terminal_jobs`, write no per-job completed receipt closures, write compact `lease_claim_closure_batches` evidence, can be hidden by SQL compatibility delete through `receipt_completion_tombstones`, remain closed from the receipt plane, and are counted by untrusted fallback and rebuild paths.
- Runtime tests assert that receipt claims materialized into `leases` after lease-ring rotation still complete through the general receipt fallback and delete the materialized lease by stable identity.
- Runtime tests assert that cancelling an unclaimed available job tombstones the ready lane and writes a wide terminal row because the job never had a claimed attempt snapshot.
- Existing DLQ move, bulk move, retry, and discard flows hydrate terminal rows before moving them, remove the terminal fact, and leave retained ready backing rows inert until queue prune.
- Runtime tests assert that terminal mutations append signed deltas, exact counts include folded counters plus pending deltas, maintenance rolls sealed deltas into `queue_terminal_live_counts`, prune folds terminal history into permanent rollups, and rebuild restores counters from `terminal_jobs`.
- `correctness/storage/AwaSegmentedStorage.tla` models retained terminal ready bodies, compact receipt closure evidence, terminal-to-DLQ terminal-fact deletion, and queue prune reclaiming retained ready bodies with any remaining terminal facts.

## Relationship to Other ADRs

- Refines ADR-019's queue-storage terminal family.
- Preserves ADR-023's receipt-plane safety: completion still closes the exact claim and writes a durable terminal fact.
- Complements ADR-025: enqueue sharding attacks producer head-row contention; this ADR reduces completion-side WAL/row pressure.
- Supersedes the useful part of the rejected ADR-024 deferred-materialisation experiment without adopting its asynchronous terminal-history contract.

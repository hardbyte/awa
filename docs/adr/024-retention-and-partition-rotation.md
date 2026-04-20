# ADR-024: Retention and Partition Rotation Policy

## Status

Accepted

## Context

ADR-019 replaces row-by-row cleanup of terminal and DLQ rows with
segmented partitions that rotate through open → sealed → pruned states.
ADR-020 makes the DLQ its own segmented family with independent retention.
These two ADRs each reference retention, but neither pins down the
operational policy: how often rotate runs, how prune interacts with
long-running readers, how `statement_timeout` / `lock_timeout` guard
against stalls, and what retention knobs exist.

## Decision

### Rotation

Each segmented family (`ready_entries`, `deferred_jobs`, `waiting_entries`,
`terminal_entries`, `dlq_entries`, `leases`) has its own rotation interval
driven by the maintenance leader. Defaults:

- Lease segments: fast rotation (seconds) — lease churn is the dominant
  hot-path source and the partition is narrow.
- Ready / deferred / waiting / terminal / dlq segments: slower rotation
  (minutes) — these carry immutable payloads and rotate on size or age
  rather than churn.

Rotation only seals the current segment and promotes the next pruned
segment to open. It does not touch partition contents — prune is a
separate step.

### Prune

Prune runs on the maintenance leader. It walks sealed segments oldest
first, acquires the segment-level `ACCESS EXCLUSIVE` lock inside a
transaction with `SET LOCAL lock_timeout = '50ms'`, re-counts live rows
inside the same transaction (to catch any racing rescue insert), and
`TRUNCATE`s on success. Under reader contention prune returns
`PruneOutcome::Blocked` gracefully and the next tick retries — there is
no waiting retry loop inside a single tick.

### Retention knobs

- `ClientBuilder::descriptor_retention` / `AsyncClient.start(...,
  descriptor_retention_days=...)` — catalog retention (default 30 days).
- `RetentionPolicy::completed` / `failed` / `cancelled` — terminal-family
  row retention inside segments, before segment-level prune.
- `RetentionPolicy::dlq` — per-queue DLQ retention override.
- Segment counts (`QueueStorageConfig::queue_slot_count`,
  `lease_slot_count`): control the rotation ring size. Larger rings
  tolerate longer-running readers; smaller rings reclaim space faster.

### Reader-horizon interaction

`TRUNCATE` clears a partition but is held back by any in-flight
transaction that has the segment in its MVCC snapshot. Long analytical
reads on `awa.jobs` — the compatibility UNION-ALL view — can pin prune
for the lifetime of the snapshot. Operators are expected to run long
reads on a replica; the main branch's `statement_timeout` default
(30 s) limits the exposure on the primary.

## Consequences

### Positive

- Retention is a rotation story, not a row-by-row `DELETE` storm —
  matches ADR-019's "retention via partition rotation" intent.
- Prune is bounded by segment size, not by workload age.
- Graceful backoff under reader contention keeps the maintenance loop
  from stalling.

### Negative

- Ring-size misconfiguration can cause prune to consistently hit busy
  segments and make no progress. Operators need to match ring size to
  reader-horizon tolerance.
- `statement_timeout` is a Postgres-level setting; Awa cannot enforce
  it against clients that override it. Long-running reader discipline
  remains an operator contract.
- Retention knobs are numerous and live in three different configs
  (catalog, row-level, segment-level). Worth consolidating in
  `docs/configuration.md`.

## Relationship to ADR-019 and ADR-020

ADR-019 established the segmented storage layout; ADR-020 made DLQ a
first-class family within it. This ADR fills the operational gap those
two left: the policy that governs when segments rotate, when partitions
prune, and how retention knobs compose. No storage-layout changes are
introduced here.

See [ADR-019](019-queue-storage-redesign.md) and
[ADR-020](020-dead-letter-queue.md).

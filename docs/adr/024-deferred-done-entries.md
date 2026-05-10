# ADR-024: Deferred `done_entries` Materialisation

## Status

Proposed (under feature flag `awa_deferred_done_entries`). Pre-0.6, so
the schema and contract surface are still in flux.

## Context

The 2026-05-09 cross-system benchmark (postgresql-job-queue-benchmarking
sweep #26) measured awa at 14.2 k jobs/s (1×256w) against pgque's 40 k.
Investigation against the upstream pgque source showed pgque pays ~1
row write per completed job (one `last_tick` UPDATE per consumed
batch); awa pays ~3 row writes per completed job — `lease_claim_closures`
INSERT (5 cols), `attempt_state` DELETE, and `done_entries` INSERT (19
cols including `args` and `payload` JSONB).

The dominant WAL hit on the awa hot path is `done_entries`: each row
duplicates the `args` and `payload` JSONB the database already has in
`ready_entries`. The receipt-ring model from ADR-023 keeps
`ready_entries` rows alive after claim — they are reclaimed by partition
TRUNCATE rotation, not row-level DELETE — so the source data is still
queryable when completion fires.

This means `done_entries` can be reconstructed from
`lease_claim_closures` ⨝ `lease_claims` ⨝ `ready_entries` *after* the
fact. Doing so on a periodic maint pass instead of synchronously on
the completion hot path moves the JSONB write off the latency path
and amortises it across larger batches.

## Decision

The completion hot path stops writing `done_entries` directly. A new
**materialiser** background task scans `lease_claim_closures` for rows
that don't yet have a corresponding `done_entries` row, joins
`lease_claims` and `ready_entries` to assemble the terminal record,
and bulk-INSERTs into `done_entries`.

A read-side **deferred view** (`awa.done_entries_view` or callsite
UNION) returns the union of physically-materialised rows and
not-yet-materialised closures so external callers (admin, CLI,
`jobs_view`) never observe a job as still-running once its closure
exists.

A **rotation guard** prevents a `ready_entries` partition from being
TRUNCATEd while any `lease_claim_closures` row still references it
without a corresponding `done_entries` row. Without the guard, a
partition rotation between closure-write and materialise-read would
orphan the source data permanently.

## Architecture

### Hot-path complete (changed)

`complete_runtime_batch` receipt path drops the `insert_done_rows_tx`
call. The CTE remains the same:

```sql
WITH completed (claim_slot, job_id, run_lease) AS (...),
locked_claims AS (... FOR UPDATE OF claims),
inserted_closures AS (
    INSERT INTO {schema}.lease_claim_closures (...)
    SELECT ... FROM locked_claims
    ON CONFLICT DO NOTHING
    RETURNING job_id, run_lease
),
deleted_attempts AS (
    DELETE FROM {schema}.attempt_state
    USING inserted_closures
    WHERE ...
)
SELECT job_id, run_lease FROM inserted_closures;
```

No `done_entries` write. WAL footprint per completion: closure row (5
cols, no JSONB) + attempt-row delete + minimal index touches.

### Materialiser (new)

A new periodic task in `awa-worker/src/materializer.rs` runs on a
configurable cadence (`AWA_MATERIALIZE_INTERVAL_MS`, default 100 ms)
and a single-leader lease (cooperates with maintenance leader
election; doesn't need its own).

Each pass:

1. Reads up to `AWA_MATERIALIZE_BATCH` (default 4096) closure rows
   that have no matching `done_entries` row, oldest first.
2. JOINs the source data:
   - `lease_claim_closures` for `(claim_slot, job_id, run_lease,
     outcome, closed_at)`
   - `lease_claims` for `(ready_slot, ready_generation, queue,
     priority, attempt, max_attempts, lane_seq)`
   - `ready_entries` for `(kind, args, run_at, attempted_at,
     created_at, unique_key, unique_states, payload)`
3. Bulk-INSERTs into `done_entries` with `ON CONFLICT
   (ready_slot, queue, priority, lane_seq) DO NOTHING` (idempotent).

The pass is a single SQL statement — one round-trip per batch.

### Rotation guard (already in place)

Implementation note: a fresh review of `prune_oldest` in
`awa-model/src/queue_storage.rs` showed the existing pre-TRUNCATE
check already serves as the rotation guard the deferred path needs.
Today's check counts ready_entries rows in the candidate partition
that have no corresponding done_entries row:

```sql
SELECT count(*)::bigint
FROM {ready_child} AS ready
LEFT JOIN {done_child} AS done
  ON done.ready_generation = ready.ready_generation
 AND done.queue = ready.queue
 AND done.priority = ready.priority
 AND done.lane_seq = ready.lane_seq
WHERE done.lane_seq IS NULL
```

Under the synchronous path this counted "still-running or never-started"
work. Under the deferred path the same count *also* catches
"completed-but-not-yet-materialised" — the closure has been written but
done_entries lags. The prune returns
`SkippedActive { reason: QueuePendingReady, count: N }` and the
maintenance loop retries on the next vacuum tick.

Once the materialiser drains the backlog (default cadence 100 ms), the
LEFT JOIN matches, the count drops to zero, and the next
`rotate_queue_storage_queue` tick prunes successfully. No new SQL
needed; the `materialise_done_entries` task is the only addition.

The dedicated helper `unmaterialised_closures_for_ready_partition`
mirrors this check from the closure side; it's exposed for ops
visibility but not on the prune hot path.

### Read-side projection

`awa.jobs_view` and admin queries that today read `done_entries`
directly switch to a UNION view that surfaces both:

- Physical `done_entries` rows (the common case after maint runs)
- Synthesised rows from `lease_claim_closures` + `lease_claims` +
  `ready_entries` for closures not yet materialised

The synthesised projection cannot be cached — it joins three live
tables on every read — but it's bounded by the materialiser-batch
backlog, which under default cadence stays under a few thousand rows.

### Crash safety

`lease_claim_closures` is the source of truth: a job is *completed*
the moment a closure row is written. Crash recovery requires no
extra mechanism — the next materialiser pass picks up unmaterialised
closures.

If the materialiser itself crashes mid-batch, its work is idempotent:
the next pass sees the same backlog and re-runs `INSERT ... ON
CONFLICT DO NOTHING`. No state needs to be carried across passes.

## Consequences

**Throughput.** Hot-path WAL drops by the entire `done_entries`
INSERT. JSONB writes (the heaviest component, since `args` and
`payload` are duplicated) move off the completion latency path.
Expected lift on the bench shape: 20–40 % at 1×128 w / 1×256 w. Will
be confirmed by the 4-cell A/B in
`benchmarks/portable/long_horizon` once implementation lands.

**Latency: completion p99 improves.** No JSONB write on the
completion path.

**Latency: read-after-complete sees a small lag.** A worker that
completes a job and immediately queries the admin view to read it
back may see "synthesised" projection rather than physical
`done_entries`. The synthesised projection is correct (returns the
same row); the only operational difference is plan time vs. an indexed
read. Materialiser cadence (100 ms) bounds this to sub-second.

**Bloat profile unchanged.** `done_entries` is partitioned and
TRUNCATE-rotated by ADR-019; that doesn't change. The maint pass
just feeds it on a slightly slower clock.

**New failure mode: materialiser falls behind.** If the materialiser
backlog grows unboundedly (e.g. a stuck rotation), reads through the
synthesised view get slower; eventually the rotation guard would
block partition rotation entirely. The `materialise_lag` metric
(closures - done_entries) is exposed in `awa.runtime_storage_health`
for ops monitoring.

**Schema impact.** Pre-0.6, no compatibility hops. Existing
clusters running an alpha will need a one-shot backfill (run the
materialiser inline once on first start of the new binary, against
all closures whose done_entries row is missing). Encoded as a
data-migration step in the install path.

## Validation

- TLA+ spec at `correctness/storage/AwaDeferredMaterialize.tla`.
  Safety + liveness exhaustive on (Jobs={j1,j2}, 3 segments, max
  run_lease 2): 1710 distinct states, all invariants hold.
- Unit tests in `awa/tests/queue_storage_runtime_test.rs`:
  - hot path skips done_entries write
  - materialiser reconstructs the same row the synchronous path
    would have written
  - rotation guard blocks Prune when closure backlog references the
    partition; unblocks after materialiser drains
  - read-side view returns the same job state during the transient
    closure-but-no-done window
- A/B on `postgresql-job-queue-benchmarking` long_horizon at W=64 /
  128 / 256 with `AWA_DEFER_DONE_ENTRIES=1`. Recorded to
  `docs/adr/bench/024-deferred-done-entries-2026-05-XX/`.

## Alternatives considered

- **Thin done_entries (Design B in the plan).** Drop only the
  JSONB columns from `done_entries`, keep the row write
  synchronous, JOIN `ready_entries` on read. Smaller change, no
  rotation guard. Rejected because the hot-path write is still on
  the latency path and the read side has to JOIN per row anyway.
  Deferred materialisation is a strict superset of the win.
- **No `done_entries` at all (Design C extreme).** Reconstruct the
  terminal projection on every read from `closures` ⨝
  `lease_claims` ⨝ `ready_entries`. Rejected because audit /
  long-tail reads break once `ready_entries` partitions rotate out.
  Deferred materialisation gives us a stable terminal index for
  reads past the rotation horizon.
- **Different per-attempt scratch table.** Stash `args/payload` in a
  dedicated `closure_payload` table, write per-completion. Same WAL
  cost as the status quo; no win.

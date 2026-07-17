# ADR-023: Receipt Plane Ring Partitioning

## Status

Accepted (implemented). `lease_claim_receipts` is the default in 0.6. The validation artifact under `docs/adr/bench/` is the empirical evidence the design holds; this ADR is the architectural record.

**Evolution (0.7, [#246](https://github.com/hardbyte/awa/issues/246)).** The original design routed deadline-backed claims through row-local `lease_claims` (so the deadline-rescue cursor could stay index-backed) and reserved `lease_claim_batches.deadline_at` for a later compact-deadline path. That path is now the default: every job claimed in one `claim_ready_runtime` call shares one `claimed_at`, hence one `deadline_at`, so a deadline-backed claim writes the same compact `lease_claim_batches` row as a zero-deadline claim, with the batch's `deadline_at` populated. At 256-worker saturation this removed the ~83k steady-state live `lease_claims` rows the row-local path materialized (and the e2e p99 regression they caused). Deadline rescue gains a second per-slot cursor over `(deadline_at, batch_id)` on `claim_ring_slots` (index-backed by a partial `(deadline_at, batch_id) WHERE deadline_at IS NOT NULL` index per `lease_claim_batches` child) that expands expired batches and force-closes open members into `lease_claim_closure_batches` with outcome `deadline_expired`. Row-local `lease_claims` and its deadline cursor are retained additively for claims that materialize into mutable leases and for legacy in-flight claims written before the upgrade. The sentences below that say deadline-backed claims live in `lease_claims` describe the pre-#246 shape; the compact representation is now used for both deadline modes.

## Context

ADR-019 committed the hot queue plane to a vacuum-aware discipline: ready and terminal queue entries reclaim space by partition rotation and `TRUNCATE`, not by row-level cleanup. The remaining planes have narrower, explicit churn: materialized leases live in a bounded mutable lease ring, `deferred_jobs` is a plain scheduled/retryable backlog table promoted by delete-and-insert, and `dlq_entries` is an operator hold table with bounded retention cleanup. The ADR-019 validation artifact recorded 276 exact dead tuples across the entire schema after a 1000/s soak — `queue_lanes=19, ready=0, done=255, leases=2, attempt_state=0` — consistent with the design intent.

The experimental receipt-backed short-job path introduced after that validation adds three tables:

- `lease_claims` — append-only claim receipts (durable claim history)
- `lease_claim_closures` — append-only closure tombstones
- `open_receipt_claims` — a bounded "currently live receipt-backed attempt" frontier, introduced so rescue and queue-count queries would not degrade into anti-joins against unbounded claim history

`lease_claims` and `lease_claim_closures` honour the ADR-019 contract: they are insert-only. `open_receipt_claims` does not. Its design is one `INSERT` per claim and one `DELETE` per completion, so every completion produces a dead tuple on that table.

Measurements at the time the receipt ring landed confirmed this is the remaining MVCC source. In an internal long-horizon run from `custom-20260424T065828Z-227187` (since-extracted to [postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking), 28-minute clean phase at ~800/s per replica, receipts on), the `open_receipt_claims` heap held a median of 28,390 dead tuples and a peak of 93,789 while every other hot table stayed under 100 dead tuples. The autovacuum floor is `autovacuum_naptime=60s`, which is global, so per-table thresholds do not move the median: a measured before/after run with per-table knobs on `open_receipt_claims` left that median unchanged. Knobs treat the symptom; they cannot remove the `DELETE` from the hot path.

The receipt-backed short-job path is the only way to retire per-claim mutable lease row churn on the common path, and making it the default is a 0.6 release goal. Delivering that goal without violating ADR-019 requires bringing `open_receipt_claims` onto the same rotation-and- prune discipline as the rest of the engine.

## Design goals and non-goals

Guided by the 0.6 priorities from ADR-019:

- Preserve at-least-once delivery. No claim may be lost across any crash, restart, or partition-truncation boundary.
- Preserve stale-writer protection by `(job_id, run_lease)`. Completion must still lose cleanly against a rescue or cancel on the same attempt.
- Keep the claim hot path bounded and predictable under partition maintenance. The fix must remove per-completion receipt-row churn; one compact, append-only emission row per claim batch is acceptable only if it removes broader claim-ring parent scans from the hot stale-cursor recovery path.
- Eliminate the remaining MVCC churn source. Steady-state dead tuples on the receipt plane should be zero once rotation catches up.
- Finish the vacuum-aware story before 0.6 ships; hold the quality bar rather than the timeline.

Non-goals:

- Do not change the heartbeat / deadline / callback-timeout rescue contract. Those continue to live on `attempt_state` and `active_leases`.
- Do not change the external API or the `(job_id, run_lease)` stale-writer guard.
- Do not introduce any new reservation or pre-start state. The archived [`lease-plane-redesign-spike`](../archive/0.6-storage-design/lease-plane-redesign-spike.md) record shows that direction has been tried and rejected repeatedly on cost grounds.

## Decision

Apply ADR-019's rotation-and-prune pattern to the receipt plane. Remove `open_receipt_claims` as a distinct table.

### Physical layout

1. `lease_claims` becomes `PARTITIONED BY LIST (claim_slot)` with a small fixed set of child partitions (`lease_claims_0..N-1`). Row-local receipt claims live here when Awa needs per-claim deadline indexing or later materialization metadata.
2. `lease_claim_batches` is partitioned by `claim_slot` with matching children. Receipt claims (zero-deadline and, since #246, deadline-backed — every job claimed in one call shares one `deadline_at`) write one compact row per claim batch, storing arrays of job ids, run leases, receipt ids, lane ids, attempts, and max attempts plus a derived receipt multirange for exact membership checks.
3. `lease_claim_closures` becomes `PARTITIONED BY LIST (claim_slot)` with matching children. A closure row lives in the same `claim_slot` as its originating claim.
4. `lease_claim_closure_batches` is partitioned by `claim_slot` with matching children. Compact successful completions write globally allocated receipt identities here, plus a derived `int8multirange`, so open-claim proofs can use an indexed `receipt_ranges @> receipt_id` membership check while prune still truncates the matching claim and closure partitions together.
5. A new control-plane pair `claim_ring_state` and `claim_ring_slots` coordinates rotation, mirroring the existing `lease_ring_state` and `lease_ring_slots`. `claim_ring_slots` also stores per-slot rescue cursors: stale rescue sweeps receipt rows and compact claim batches by `(claimed_at, job_id, run_lease)`, while deadline rescue sweeps deadline-backed claims in both shapes: row-local rows by `(deadline_at, job_id, run_lease)` and compact batches by `(deadline_at, batch_id)` (#246).
6. `open_receipt_claims` is deleted. Its indexes and the schema-install backfill are dropped.

### Hot path

- Claim: append queue-slot-local batch/range attempt evidence to `ready_claim_attempt_batches` and append receipt claims to the current claim-slot partition in the same transaction. Both deadline modes use compact `lease_claim_batches` (#246): all jobs claimed in one call share one deadline, and the batch-deadline rescue cursor stays index-backed via the partial `(deadline_at, batch_id)` sweep index. Row-local `lease_claims` remains for claims that materialise into mutable leases and for legacy in-flight rows. The claim result carries `claim_slot` and the immutable `receipt_id` through to the worker so compact closure evidence can be tied back to the exact claim partition and attempt.
- Complete: successful compact receipt completion records terminal history through `receipt_completion_batches` and records claim-local closure proof through `lease_claim_closure_batches`. Non-success exits, receipt rescue, and cancellation close by claim shape: a row-local `lease_claims` claim appends an explicit `lease_claim_closures` row, while a compact `lease_claim_batches` claim appends a `lease_claim_closure_batches` row carrying `ready_slot` / `ready_generation`. A compact claim has no `lease_claims` row, so an explicit closure for it would be invisible to the queue-prune count proof (which joins `lease_claims`); the closure batch keeps it countable via `compact_count`.
- The claim step and compact successful completion step do not `UPDATE` or `DELETE` receipt-plane history. Non-success, rescue, cancellation, and terminal-delete closure paths append durable closure rows — `lease_claim_closures` for row-local claims, `lease_claim_closure_batches` for compact claims; row-local claims can also set the `lease_claims.closed_at` fence so a waiter rechecks the claim as closed after the winning transaction commits.
- Short-job fast path moves the cost to append-only ledgers: claim writes receipt rows plus one ready-slot attempt batch row for the claimed lane range, and compact success writes batch-level terminal and claim-closure evidence without a per-job closure row, count-delta row, or receipt delete.
- Non-success closure batches are ordered by `claim_slot` before `(job_id, run_lease)` so a flusher presents partition-local closure rows to Postgres. That ordering does not change the stale-writer guard; it only improves locality for the append-only receipt plane.
- Default successful completions avoid writing a terminal payload copy when the runtime payload is empty or unchanged from `ready_entries`. When an entire done batch has empty terminal payloads, completion also skips the ready-payload lookup that would otherwise be needed to prove unchanged non-empty payloads can be elided.
- The receipt-backed compact completion SQL takes a per-`(job_id, run_lease)` advisory transaction lock, validates the matching receipt claim by `(claim_slot, job_id, run_lease, receipt_id)` from either `lease_claims` or `lease_claim_batches`, skips claims that have already materialized `leases` rows, removes matching `attempt_state`, appends compact claim-local closure evidence, and appends compact terminal history in one statement. Known-key materialization, explicit close paths, and receipt rescue use the same advisory key before row locks, so a same-attempt race observes the committed winner before deciding whether the receipt is still open. The compact terminal history carries the public terminal-history contract through `{schema}.terminal_jobs`; the compact closure batch carries the claim-prune contract for successful completions. Exact terminal counts read retained compact batches directly; the terminal-count delta ledger is reserved for `done_entries` terminal mutations.

### Open-claim queries

Every read that currently targets `open_receipt_claims` becomes a bounded read over the active `lease_claims` and `lease_claim_batches` child partitions, anti-joined with durable closure evidence. Evidence can be an explicit `lease_claim_closures` row, a matching compact `lease_claim_closure_batches` item, or durable disposition rows retained for older rows:

- "Is `(job_id, run_lease)` still open?" — exact lookup into active row-local claims or compact claim-batch items, anti-join durable closure evidence. Used by the completion guard and by `load_job` on receipt-backed attempts.
- "Scan stale receipt claims for rescue." — per-slot cursor scan ordered by `(claimed_at, job_id, run_lease)`, limited to row-local claims and compact claim-batch items old enough to be rescue candidates, anti-join durable closure evidence and materialized leases, take the same per-attempt advisory key used by completion, and close stale candidates by appending rescue closures.
- "Scan expired receipt deadlines for rescue." — two independent per-slot cursor scans, both limited to claims whose deadline has already passed and both anti-joining durable closure evidence and materialized leases under the same per-attempt advisory key used by completion (#246). The compact-batch scan is ordered by `(deadline_at, batch_id)`, expands each expired batch into its members, and closes open members by appending `deadline_expired` rows to `lease_claim_closure_batches`; its cursor advances past a batch only when every member is closed. The legacy row-local scan is ordered by `(deadline_at, job_id, run_lease)` over `lease_claims` and closes into `lease_claim_closures`, covering in-flight claims written before the compact-deadline upgrade.
- "Count in-flight receipt-backed attempts." — count row-local claims plus compact claim-batch items minus matching closure or durable disposition evidence.

Stale claim-cursor recovery is deliberately not an open-claim query. A claim transaction also writes `ready_claim_attempt_batches` in the ready row's queue-ring slot, keyed by `(ready_slot, ready_generation, queue, priority, enqueue_shard)` plus an `int8multirange` of covered `lane_seq` values. If the post-commit cursor advance is lost, the next claim can advance over already-emitted attempts by reading this queue-slot-local ledger. That keeps the hot claim path from taking parent `lease_claims` / closure evidence locks while old claim partitions are being truncated, while amortizing the proof row across the whole claim batch.

Both rescue cursors are bounded cyclic sweeps. Each pass scans forward from the cursor and wraps to the start of the partition when it reaches the end, but it does not visit rows that are too new to be candidates for that rescue class. Stale rescue can advance over closed claims, materialized lease-plane claims, old claims with fresh heartbeat evidence, and claims it closes in the current rescue transaction; it stops before a stale open claim that it did not close. Deadline rescue can advance over closed claims, materialized lease-plane claims, and claims it closes in the current rescue transaction. These cursors keep liveness scans from repeatedly proving old completed receipts are closed when a long reader prevents partition prune, without making every tick re-prove claims that are not yet old or expired enough to rescue.

### Rotation and prune

- The maintenance leader owns `claim_ring_state` rotation on a cadence chosen to keep the active scan surface bounded. Initial target: rotate at the same cadence as the queue ring so claim partitions age out roughly in step with the ready / done partitions they reference.
- A claim-slot partition may be truncated only when every claim in it is represented by durable closure evidence. If open claims remain, prune skips the partition until normal completion, normal non-success closure, or the separate receipt-rescue scans close those claims.
- Queue-ring prune must not remove terminal history for a segment before matching receipt claims have durable closure evidence. It also must not remove retained ready or tombstone rows that are still at or ahead of their lane's claim cursor, because those rows are the cursor's proof that the prefix has been spent. These sealed-slot skip proofs run before the ready/terminal child-table exclusive locks so maintenance skips active slots without blocking hot claim reads, then run again after the locks so rows that committed while the lock waited are visible before truncate. Current successful compact completions write terminal/closure evidence synchronously, and compatibility/cold paths preserve or create closure evidence before hiding terminal history.
- Prune order mirrors `prune_oldest` and `prune_oldest_leases`, except the open-claim proof intentionally runs both before and after the child-table exclusive locks:
  1. `FOR UPDATE` on `claim_ring_state`.
  2. `FOR UPDATE` on the target `claim_ring_slots` row.
  3. Prove every visible claim in the sealed slot has durable closure evidence.
  4. Bounded `ACCESS EXCLUSIVE` on all claim-ring partitions for that slot (row-local claims, compact claim batches, explicit closures, and compact closure batches).
  5. Recheck not-current and re-prove no open claims under the partition locks, then reset the slot's rescue cursor and `TRUNCATE`.
- Partition truncation never races with claim because claim writes to the ring's current slot and the prune path rechecks the sealed partition after taking the exclusive child locks.

### Invariants preserved

- At-least-once delivery: a partition cannot truncate while live claims remain in it. Rescue is the gating step, not the prune itself.
- `(job_id, run_lease)` stale-writer protection: the authoritative record is the row-local claim or compact claim-batch item plus durable closure evidence. The closure table follows the claim shape, not the exit reason: compact `lease_claim_batches` claims close into `lease_claim_closure_batches` on every path (successful completion, non-success exit, cancellation, rescue), while row-local `lease_claims` claims close into `lease_claim_closures`. Wide/cold paths that operate on row-local claims use `lease_claim_closures`.
- Heartbeat / callback-timeout rescue: unchanged for materialized leases. Deadline rescue is split by storage shape: materialized leases stay on the lease-side deadline path, while receipt-only claims use the deadline cursor and append a `deadline_expired` closure before entering the same retry / DLQ routing.

### Migration

This is a breaking schema change even though the external API does not change.

1. A new migration creates `claim_ring_state`, `claim_ring_slots`, and the partitioned `lease_claims`, `lease_claim_batches`, `lease_claim_closures`, and `lease_claim_closure_batches` shapes.
2. Existing `lease_claims` and `lease_claim_closures` rows are rewritten into the current slot of the new partitioned parents. Compact claim batches and compact closure batches are new storage and start empty on upgrade.
3. The legacy `open_receipt_claims` table is not part of the live read path. `prepare_schema()` drops it when empty, and refuses to proceed if it still contains rows so an operator can drain or reverse-migrate those rows deliberately.
4. Subsequent claim, complete, rescue, and count paths target the partitioned claim/closure-evidence tables. The live receipt set is derived as an anti-join over the active claim-ring partitions; stale-rescue and deadline-rescue scans use separate per-slot cursors stored on `claim_ring_slots`.

### Historical reverse-migration outline

The original 0.5-to-0.6 rollout runbook carried a manual escape hatch for reverting the receipt-plane partitioning before 0.6 workers had taken over. It was deliberately never shipped as a down migration, and it is not a supported rollback after queue storage has accepted work; retain it here so an incident responder can identify the historical boundary rather than infer a downgrade from the current schema.

1. Quiesce all 0.6 workers and receipt-plane writers, take a database snapshot, and verify `lease_claim_batches` and `lease_claim_closure_batches` are empty before renaming the partitioned `lease_claims` and `lease_claim_closures` parents so their pre-0.6 names are available.
2. Create unpartitioned `lease_claims` and `lease_claim_closures` tables from the exact pre-ADR-023 release schema, including its indexes and constraints.
3. Copy every row from the renamed partitioned parents with `INSERT ... SELECT`, then reconcile row counts and open-claim evidence before dropping the partitioned parents.
4. Inspect both compact batch tables separately. They are compact claim/closure evidence that older runtimes cannot read — 0.6 wrote `lease_claim_batches` for zero-deadline receipt claims from the start, so live claims can exist in the batch representation with no row-local counterpart. If either table contains rows, do not start the older fleet: restore the snapshot or continue forward on 0.6 unless release-matched reverse SQL has materialized equivalent row-local claims and closures.

TLA+ coverage (`AwaSegmentedStorage`, `AwaStorageLockOrder`) is extended to model the claim-ring rotation and the rescue-before-truncate precondition, parallel to the existing lease-ring model.

## Validation

Success criteria for this redesign, measured on the long-horizon portable harness used for the ADR-019 baseline:

- `open_receipt_claims` is absent from the schema. Steady-state dead tuples across the queue-storage schema return to the ADR-019-validation shape: low hundreds, concentrated in ring-state singletons.
- Throughput on the `1x32`, `4x8`, and soak profiles is no worse than the current branch and should improve slightly because claim and completion each drop a table touch.
- Crash-under-load recovers cleanly. The rescue-before-truncate path is exercised by the existing crash-recovery scenarios and by a new test that drives rescue concurrently with partition prune.
- `lease_claim_receipts` is on by default for 0.6 with no dead-tuple regression relative to the ADR-019 validation run.

## Consequences

### Positive

- The 0.6 vacuum-aware story becomes complete: no hot table relies on `DELETE` or `UPDATE` for reclamation.
- The short-job hot path drops one `INSERT` at claim and one `DELETE` at completion. Per-job database work is strictly reduced.
- Autovacuum tuning on `open_receipt_claims` becomes dead code and is removed. The per-table HOT tuning on the small ring-state and head tables stays because it addresses a separate per-row UPDATE class that this ADR does not change.
- Receipts become the default short-job path for 0.6, retiring the per-claim mutable lease row on the common path.

### Negative

- This is a breaking schema migration.
- "Currently open" queries move from a single bounded-frontier lookup to a bounded anti-join across a small number of active partitions. Query planning needs spot-checking once the partition count is chosen.
- Rescue gains a partition-aware variant and must run before prune takes bounded `ACCESS EXCLUSIVE`. The interaction point is small but adds a prune-path precondition not present for `ready` / `done`.
- The default-success path now writes one compact completion-batch row and one compact claim-closure batch row for the worker completion batch. ADR-026 explains how compact terminal history is exposed through `{schema}.terminal_jobs`, how claim-local closure evidence protects receipt pruning, and how exact counts and segment-level retention are preserved.

## Alternatives Considered

### Per-table autovacuum tuning on `open_receipt_claims`

Rejected. A measured 28-minute soak with aggressive per-table thresholds and cost knobs left the median dead-tuple count on this table unchanged relative to the unmodified baseline (`custom-20260424T041700Z-278649` vs. `custom-20260424T065828Z-227187`). The rate-limiter is `autovacuum_naptime=60s`, which is global, and the table is already eligible for vacuum on every wake-up. Per-table knobs improve vacuum efficiency when it runs but do not change the steady-state floor.

### Documented operational `autovacuum_naptime` reduction

Rejected as the primary fix. Lowering `autovacuum_naptime` globally improves reclamation cadence but pushes configuration requirements onto operators and applies to every table in their database. It contradicts the ADR-019 principle that vacuum-awareness is a property of the schema design, not of operator tuning.

### Awa-owned periodic VACUUM on `open_receipt_claims`

Rejected. A background `VACUUM` loop would mask the churn but keeps a hot `DELETE` on the common completion path and leaves the table as a permanent architectural outlier. The rest of the engine has no equivalent self-vacuum loop.

### UPDATE-based soft close on `open_receipt_claims`

Rejected. Marking a `closed_at` column and sweeping closed rows periodically keeps the table live-bounded, and a HOT-eligible `UPDATE` avoids a new heap tuple. But the sweep still performs `DELETE`s in batch, index bloat still tracks throughput, and the architectural outlier persists.

### Ship 0.6 with receipts off

Rejected. Shipping with receipts off lets 0.6 hit the dead-tuple budget today, but it leaves the short-job path on the mutable `leases` ring and defers the work tracked in the archived [`lease-plane-redesign-spike`](../archive/0.6-storage-design/lease-plane-redesign-spike.md). ADR-019's vacuum-aware intent is only satisfied when receipts are on by default and do not regress the dead-tuple budget. This ADR is the path to that posture.

## Relationship to Earlier ADRs

- ADR-019 established the vacuum-aware discipline. This ADR applies that discipline to the one remaining hot table that did not follow it.
- ADR-013 (run-lease-guarded finalization) is unchanged. The authoritative record for `(job_id, run_lease)` staleness moves from a bounded mutable frontier to partitioned append-only tables; the guarantee does not.
- The archived [`lease-plane-redesign-spike`](../archive/0.6-storage-design/lease-plane-redesign-spike.md) identifies `open_receipt_claims` as the compromise that unblocked the receipt-backed path. This ADR is the follow-through that the spike anticipated.

## Implementation and Validation Status

This ADR has been implemented for 0.6:

- `lease_claims`, `lease_claim_batches`, `lease_claim_closures`, and `lease_claim_closure_batches` are partitioned by `claim_slot`.
- `claim_ring_state` and `claim_ring_slots` control rotation and prune.
- `claim_ring_slots` stores per-slot stale-rescue and deadline-rescue sweep cursors so maintenance walks receipt history in bounded cyclic windows without reintroducing a per-claim mutable frontier. The stale sweep visits only claims old enough for the rescue cutoff; within that window it can pass claims whose heartbeat evidence is fresh and revisit them after wrap. Stale open claims stop advancement until rescue closes them. The deadline sweep visits only expired receipt deadlines and closes expired receipt-only claims.
- `open_receipt_claims` is removed from fresh installs and is no longer a hot path table.
- `lease_claim_receipts` defaults to `true`.
- Successful compact receipt completion writes compact terminal history plus compact claim-local closure evidence. SQL compatibility delete hides compact completed rows by writing `receipt_completion_tombstones`, without mutating the compact terminal batch or reopening the receipt.
- Receipts mode supports per-claim deadlines. Since #246 the claim path writes `deadline_at` onto the compact `lease_claim_batches` row when `QueueConfig.deadline_duration > 0` (all jobs in one claim call share one deadline); legacy row-local deadline rows remain readable. The sibling rescue scan (`rescue_expired_receipt_deadlines_tx`) advances by `claim_ring_slots.deadline_cursor_*` for row-local claims and `claim_ring_slots.batch_deadline_cursor_*` for compact batches, force-closing deadline-backed receipt claims whose `deadline_at` has passed without a closure or materialized lease, writing a `'deadline_expired'` closure. The maintenance entry point (`rescue_expired_deadlines`) merges the lease-side and receipt-side scans into one batch per tick, so receipts mode and the existing hard-deadline behaviour compose without operator intervention.

Validation evidence is split by purpose:

- Runtime and long-horizon evidence lives in [`bench/023-receipt-ring-validation-2026-04-26.md`](bench/023-receipt-ring-validation-2026-04-26.md). The recorded runs include the 115-minute 4x8 receipts-on long-horizon run and the 12-hour overnight run; receipt closure partitions stayed at 0 dead tuples across every phase, and receipt claims remained bounded.
- Spec and implementation mapping lives in [`../../correctness/storage/MAPPING.md`](../../correctness/storage/MAPPING.md). The storage TLA+ family models claim-ring rotation, partition prune safety, receipt rescue, running cancel, and DLQ retry trace witnesses.
- Operator-facing tuning and defaults live in [`../configuration.md`](../configuration.md#queue-storage-tuning).

The detailed phase-by-phase implementation notes were intentionally kept out of this ADR. ADRs record the decision and its consequences; dated build logs, benchmark output, and branch-era investigation notes belong in validation artifacts or the 0.6 storage-design archive.

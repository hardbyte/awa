# ADR-040: Append-only ring-rotation ledgers

## Status

Accepted — implemented for 0.7 ([#371](https://github.com/hardbyte/awa/issues/371),
from the 0.7 performance campaign). Ships with a **stop-the-world, lockstep
binary+migration upgrade** (migration v041); see the CHANGELOG 0.7 upgrade notes.

## Context

The queue-storage engine (ADR-023) advances three ring cursors — queue, lease,
and claim — by rotating one slot forward on a timer. Before this ADR each cursor
lived in a mutable singleton row (`{ring}_ring_state.current_slot` /
`.generation`, one row per ring) that every rotation UPDATEd, and the queue ring
additionally UPDATEd the incoming slot's `queue_ring_slots.generation`. Queue
prune similarly UPDATEd (upserted) `queue_terminal_rollups` inside the prune
transaction.

Every one of those writes is on the hot control path and, more importantly,
produces a dead tuple. Under a **pinned MVCC horizon** — any long-running
`REPEATABLE READ` transaction or idle-in-transaction backend elsewhere in the
database — those dead versions cannot be reclaimed. At a 1s rotation cadence the
three singletons churn ~3.6k dead rows/hour/ring (~10.8k/hour total), plus the
per-rotate `queue_ring_slots` UPDATE, and each hot-path claim/enqueue reads these
same rows. This is exactly the dead-tuple accumulation ADR-026 and #169 commit to
avoiding, but on the ring bookkeeping rather than on terminal history.

The #371 idle-skip change (merged first) removed the rotation write entirely when
a ring is idle. This ADR removes it on the *busy* path too, by changing the
cursor's representation.

## Decision

**Represent each ring cursor as an append-only rotation ledger.**

- New table `{ring}_ring_rotations (generation BIGINT PRIMARY KEY, slot INT,
  rotated_at TIMESTAMPTZ)`. The **current cursor is the max-generation row** — a
  backward primary-key scan, `ORDER BY generation DESC LIMIT 1`, O(1). Rotation
  **appends** one row instead of UPDATEing a singleton.
- The append is a **compare-and-swap**: the `generation` primary key means a
  rotator whose observed cursor was already consumed by a competitor inserts zero
  rows (`ON CONFLICT (generation) DO NOTHING`) and treats the tick as a lost race
  rather than double-advancing.
- Per-slot generations are **derived, not stored**: rotation advances slot and
  generation in lock-step (`slot = generation mod slot_count`, genesis `(0, 0)`),
  so a sealed slot's last-open generation is a function of the current cursor and
  `slot_count`. The mutable `{ring}_ring_slots.generation` column is dropped; the
  slot rows remain as row-lock targets (and, for the claim ring, rescue-cursor
  holders).
- The `{ring}_ring_state` singletons are **demoted to cold config**: they keep
  `slot_count` and the #290 terminal-counter trust marker on the queue ring, and
  are no longer written per rotation. Their `current_slot` / `generation` cursor
  columns are **dropped** (see Consequences — this is the compat break).
- **Rotate ↔ prune ↔ delta-rollup serialization** moves from `FOR UPDATE` on the
  singleton (the row is gone) to a **per-ring `pg_advisory_xact_lock`**,
  try-locked so a periodic rotate skips under contention instead of queueing
  behind a prune. The lock order (ring advisory lock → `{ring}_ring_slots FOR
  UPDATE` → child partitions `ACCESS EXCLUSIVE`) is modelled in
  `AwaStorageLockOrder.tla`.
- **Queue prune stops upserting rollups.** It appends per-(queue, priority) rows
  to a new `queue_terminal_rollup_deltas` landing table. Exact count readers add
  the unfolded delta sums to their `queue_terminal_rollups` reads, so results are
  exact regardless of fold timing.
- The maintenance leader, on the existing 30s terminal-rollup tick, runs two
  **horizon-gated folds**: `fold_terminal_rollup_deltas` (drains the deltas into
  the permanent rollups with the historical `GREATEST(0, …)` clamp) and
  `fold_ring_rotation_ledgers` (trims each ledger to one full wrap, `slot_count`
  rows, retaining every sealed slot's last-open generation). Both stand down while
  another backend pins the horizon, so the versions they delete are immediately
  reclaimable when created.

## Consequences

**Positive**

- The busy-path rotation write is an **append**, not an UPDATE: no dead tuple, so
  a pinned MVCC horizon can no longer strand ring bookkeeping. Combined with the
  idle-skip, the ring control plane is write-free when idle and dead-tuple-free
  when busy. The `receipt_plane_regression_gate` asserts zero UPDATE/DELETE and
  zero dead tuples across the `{ring}_ring_state` family under load.
- Prune's rollup accounting no longer leaves an unreclaimable dead rollup version
  per prune under a pinned horizon.
- Cursor reads stay O(1) (backward PK scan); sealed-slot enumeration is pure
  arithmetic on the cursor, removing a per-slot table scan.

**Negative / limits — deliberate compatibility break**

- Dropping `current_slot` / `generation` from the singletons is an **exception to
  the additive-only migration policy**. It is intentional: a pre-v041 binary that
  read a now-frozen cursor would silently misroute writes into sealed slots,
  whereas a *missing column* fails loudly and safely. **Binaries and migration
  v041 must move together — no mixed fleet of pre-/post-v041 binaries against one
  database.** This requires a brief stop-the-world upgrade; the CHANGELOG 0.7
  upgrade notes document the procedure. The v023 install helper seeds each ledger
  from the legacy cursor **before** dropping the columns, so the current cursor
  survives the representation change exactly.
- Counts are exact but split across two relations (rollups + unfolded deltas)
  between folds; every count reader must sum both. This is encapsulated in the
  three read sites and covered by tests.
- The ledger grows by one row per rotation until the next horizon-clear fold trims
  it; a permanently pinned horizon lets it grow unbounded (the same failure mode a
  pinned horizon already imposes on every deferred rollup). Operators already
  monitor long-lived transactions for this reason.

## Alternatives considered

- **Keep the singleton, VACUUM harder.** A pinned horizon defeats VACUUM by
  construction; no autovacuum tuning reclaims versions the horizon still needs.
  Rejected — it treats the symptom.
- **HOT updates on the singleton.** HOT still produces a dead tuple that a pinned
  horizon cannot reclaim; it only avoids index bloat. Rejected.
- **Additive migration (keep the columns, stop writing them).** Leaves a stale
  cursor that a rolled-back binary would trust and misroute on — a silent
  data-routing hazard. The loud-failure drop is safer. Rejected.
- **Fold the ledger inside rotation** (trim on every append). Puts a horizon probe
  and a DELETE back on the hot path; the whole point is to keep rotation a bare
  append. Deferring the trim to horizon-gated maintenance keeps the hot path
  clean. Rejected.

## Relationship to other ADRs

Extends ADR-023's ring-partitioned receipt plane; applies ADR-026's dead-tuple
reclaim discipline to the ring control plane (the same append-only-then-fold shape
ADR-026 uses for terminal history); the rollup-delta landing table mirrors the
#290 terminal-count delta pattern. The advisory-lock ordering and the horizon-gated
folds are modelled in `correctness/storage/AwaStorageLockOrder.tla` and
`AwaDeadTupleContract.tla`.

# ADR-040: Append-only ring-rotation ledgers

## Status

Accepted — implemented for 0.7 ([#371](https://github.com/hardbyte/awa/issues/371),
from the 0.7 performance campaign). Ships as a **staged rolling upgrade**
(expand → flip → contract) via migration v042 — **no stop-the-world window**,
safe for a mixed 0.6.2/0.7 fleet after the required 0.6.2 stepping-stone. See the "Staged rolling upgrade" section below,
`docs/upgrade-0.6-to-0.7.md`, and the CHANGELOG 0.7 upgrade notes.

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
  singleton (the row is gone) to a **per-ring `pg_try_advisory_xact_lock`**,
  so a periodic rotate skips under contention instead of queueing
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
  another backend **genuinely pins** the horizon, so the versions they delete are
  immediately reclaimable when created.
  - "Genuinely pins" is deliberately narrow: a backend counts as pinning only if
    it is **idle-in-transaction holding a write xid** (it keeps its snapshot with
    no in-flight statement to bound the hold), or **actively holding a snapshot
    whose transaction has been open longer than a threshold**
    (`MVCC_HORIZON_PIN_MIN_AGE`, 5s). It must NOT trip on the transient
    `backend_xmin` that every ordinary statement — including sub-millisecond
    hot-path claims and enqueues — sets for its own duration. The threshold sits
    well above normal query/claim latency yet far below the 30s fold cadence, so a
    genuinely long-lived reader still stands the folds down before their next
    round of deletions accrues, while steady traffic never does. The first
    implementation gated on any live `backend_xmin` and so skipped the folds on
    essentially every tick under continuous load, letting the ledgers and the
    delta landing table grow without bound for the whole run (see the Negative
    consequence below); the age gate is the fix.

## Staged rolling upgrade (expand → flip → contract)

The cutover from the mutable singleton columns to the ledger is delivered as a
staged upgrade so it needs **no stop-the-world window** and tolerates a mixed
0.6.2/0.7 fleet. The patched 0.6.2 stepping-stone is mandatory: older 0.6
migrators can destructively misclassify a newer schema, while 0.6.2 recognizes
v042 only in compat authority and otherwise fails closed. Each queue-storage schema carries a `ring_cursor_authority`
control row (`columns` | `ledger`) selecting which representation is
authoritative for all three rings; the per-schema `ring_cursor(ring)` SQL
function and the Rust rotate/prune seams branch on it.

- **Expand (migration v042, additive).** Create and seed the three ledgers and
  the rollup-delta table; **keep** the compat `current_slot` / `generation`
  columns (restore them if an earlier unreleased shipped-v042 dev schema dropped
  them, re-seeding from the ledger max — the inverse seed). Upgrades start in
  `columns`; fresh installs start in `ledger` (no old binary can exist).
- **Compat mode (`columns`).** The singleton columns are authoritative, exactly
  as 0.6 wrote them. A 0.7 rotator takes the same `{ring}_ring_state` row
  `FOR UPDATE` a 0.6.2 rotator takes (serializing the two), CASes the columns, AND
  shadows the ledger — reconciling first by backfilling any generations an
  interleaved 0.6.2 rotator advanced the columns past. Cursor reads come from the
  columns. So a mixed fleet is correct and the ledger is a faithful, ready-to-
  promote copy.
- **Flip (one-way `columns → ledger`).** `awa storage flip-ring-authority`, or
  the maintenance leader's auto-flip once the whole fresh fleet has reported a
  0.7+ `binary_version` (an additive `awa.runtime_instances` column; 0.6 rows
  leave it NULL) continuously for a stable period. The flip is transactional
  across all three rings: it takes the three singletons `FOR UPDATE` (serializing
  behind every in-flight compat rotator), reconciles and verifies all three
  ledgers against the final compat cursors, **poisons** both stale cursor fields
  and the legacy prune metadata, and activates a database trigger that rejects
  later old-style cursor advances. A returning pre-flip binary therefore fails
  loudly rather than rotating from -1 back to 0, misrouting, or pruning a live slot. A manual flip
  refuses (without `--force`) while any fresh-heartbeat runtime is not known to
  be flip-aware.
- **Lock discipline & the authority-read ordering.** The compat serializer is the
  singleton `FOR UPDATE`; the ledger serializer is the per-ring advisory lock.
  A rotator takes the singleton **before** reading the authority (and the flip
  takes all three singletons), which closes a TOCTOU where a rotator reads
  `columns`, the flip commits, and the rotator then advances under the stale
  discipline. `AwaStorageLockOrder.tla` models both disciplines, the flip action,
  and a `MixedFleet` invariant (no ring advanced under both disciplines at once);
  the compat-mode singleton churn is a documented, bounded transition-mode
  exemption in `AwaDeadTupleContract.tla`.
- **Contract (0.8).** Drop the compat columns for good, guarded by an exclusive
  window asserting ledger authority and no live pre-flip binary. Tracked as a
  0.8 issue.

## Consequences

**Positive**

- **No stop-the-world upgrade and no lockstep 0.7 binary/migration coupling.** After
  the mandatory rolling 0.6.2 patch step, the additive expand + staged flip let
  operators roll 0.7 binaries and run the migration in either order with a mixed fleet,
  then promote to the dead-tuple-free ledger
  once fully on 0.7.

- The busy-path rotation write is an **append**, not an UPDATE: no dead tuple, so
  a pinned MVCC horizon can no longer strand ring bookkeeping. Combined with the
  idle-skip, the ring control plane is write-free when idle and dead-tuple-free
  when busy. The `receipt_plane_regression_gate` asserts zero UPDATE/DELETE and
  zero dead tuples across the `{ring}_ring_state` family under load.
- Prune's rollup accounting no longer leaves an unreclaimable dead rollup version
  per prune under a pinned horizon.
- Cursor reads stay O(1) (backward PK scan); sealed-slot enumeration is pure
  arithmetic on the cursor, removing a per-slot table scan.

**Negative / limits**

- The upgrade is now **two operator-visible phases** (roll the fleet, then flip)
  rather than one. The flip can be automated (the maintenance auto-flip), but
  reaching the dead-tuple-free ledger regime is not instantaneous on upgrade — a
  cluster left in compat authority keeps the pre-#371 singleton churn (bounded by
  rotation cadence, reclaimable when the horizon is clear) until it flips. This
  is the deliberate cost of avoiding a stop-the-world window.
- Rolling a binary back **across the flip** is unsupported: the flip poisons the
  compat state and database-enforces the cursor fence, so a pre-flip binary fails
  loudly. Before the flip, rollback to the 0.6.2 stepping-stone is safe. (This
  replaces the original design's hard "no mixed fleet at all"
  constraint with a narrower "no rollback across the flip".)
- The compat cursor columns and the per-slot `generation` columns survive until
  the 0.8 contract migration drops them — a small, cold storage cost carried
  through 0.7 as the safety net that makes the rolling upgrade possible.
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
`#290` terminal-count delta pattern. The advisory-lock ordering and the horizon-gated
folds are modelled in `correctness/storage/AwaStorageLockOrder.tla` and
`AwaDeadTupleContract.tla`.

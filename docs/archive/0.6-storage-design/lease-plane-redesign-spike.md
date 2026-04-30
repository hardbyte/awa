# Lease Plane Redesign Spike

> **Status: superseded for the receipt plane by [ADR-023](../../adr/023-receipt-plane-ring-partitioning.md).**
> The narrow-implementation-spike portion of this doc that proposed
> `open_receipt_claims` as a bounded live-frontier table has been
> replaced by the partitioned `lease_claims` / `lease_claim_closures`
> ring; the table no longer exists in the schema (`prepare_schema`
> drops it on every install). Read the rest of this doc as a snapshot
> of the lease-plane investigation that triggered ADR-023; the
> sections on the long-running lease plane (heartbeat, callback,
> `attempt_state`) are unchanged. References to `open_receipt_claims`
> in the body below are kept verbatim as historical context — they
> describe the design that motivated ADR-023, not the shipping
> architecture.

## Why this exists

The split-head change removed `queue_lanes` as the dominant MVCC hotspot. The
remaining steady-state churn is now concentrated in the lease plane:

- `leases_*` partitions
- `lease_ring_state`
- `lease_ring_slots`

Small tuning experiments were not enough:

- slower lease rotation reduced ring-state churn but moved much more dead
  tuples into `leases_*`
- higher `lease_slot_count` spread churn around but increased total dead tuples
- the current implementation keeps `lease_ring_slots` only as transitional
  compatibility/inspection state; lease prune order is now derived from
  `lease_ring_state`, and the remaining work is about making the lease plane
  colder without trading away delivery latency

So the next change has to be architectural rather than another timing tweak.

## Current lease lifecycle

Today the queue-storage runtime uses a rotating mutable `leases` table family.

Claim:

1. read the current lease slot / generation from `{schema}.lease_ring_state`
2. insert a row into `{schema}.leases`

Running / waiting:

- heartbeat updates the lease row
- callback wait updates the lease row
- deadline / callback timeout metadata lives on the lease row

Completion / retry / snooze / cancel / rescue:

- delete the lease row
- hydrate the immutable ready payload from the ready partition
- append the next immutable row (`done_entries`, `deferred_jobs`, `dlq_entries`)

Rotation / prune:

- maintenance rotates lease slots
- maintenance truncates old lease partitions once empty

That means even a short successful job causes:

1. one mutable lease insert
2. one mutable lease delete

And a long-running job adds repeated heartbeat updates on top.

## What the pressure profile is telling us

The current implementation is already good at:

- append-only ready / terminal storage
- low subscriber and end-to-end latency
- bounded queue metadata churn

The remaining pressure is specifically:

- one mutable live execution row per claimed attempt
- repeated updates for heartbeat / callback wait
- delete churn when attempts leave `running`

In other words, the queue plane is now mostly event-log shaped, but the lease
plane is still a mutable state table.

## Redesign goal

Move the lease plane closer to the same shape as the queue plane:

- append-only for the common short-job path
- mutable state only for attempts that truly need it

The desired steady state is:

- short jobs do not create or update a mutable lease row
- long-running / waiting jobs create a mutable per-attempt row only when
  heartbeat, callback wait, or progress needs it
- rescue logic still has a bounded, checkable source of truth

## Reservation-plane spike

We also tested a deeper dispatcher-side variant of this idea:

- reserve work ahead in a bounded live frontier
- promote reservation -> active attempt only when a worker actually starts
- expire stale reservations back to ready work if the worker dies before start

The safety model was sound:

- late start loses after reservation expiry
- expired reservations can be reinserted as fresh ready rows
- the worker/runtime tests stayed green with this boundary

But the first implementation was not good enough to keep.

### What was tried

1. A bounded `open_dispatch_reservations` table.
2. Executor-side promotion from reservation -> receipt-backed active attempt.
3. Expiry/re-enqueue for reservations that were never started.
4. A second pass with local buffering of reservations in the dispatcher.

### What the measurements showed

The reservation plane improved backlog and latency at very low worker counts,
but it regressed throughput once the worker pool got larger.

The core reason was cost shape:

- reservation requires one DB round-trip
- start promotion required a second DB round-trip per actually-started job

So the design reduced claim-roundtrip pressure at small worker counts but paid
for it with an extra per-job start transaction.

The buffered-reservation variant was worse:

- it preserved the safety boundary better than the earlier naive prefetch spike
- but it still reduced throughput, including at `1` worker in the benchmark

We also tried a batched reservation/start frontier aimed specifically at the
realistic many-small-replica case:

- bounded `open_dispatch_reservations`
- batched dispatcher reservation
- batched promotion into receipt-backed active attempts
- maintenance expiry + re-enqueue for workers that died before start

That version kept the intended safety boundary:

- promotion works directly into `open_receipt_claims`
- stale reservations are re-enqueued
- late promotion after expiry loses cleanly

But the realistic replica benchmark still regressed badly:

- `1x32`: `clean_1 397/s`, `pressure_1 576/s`, `recovery_1 730/s`
- `4x8`: `clean_1 157/s`, `pressure_1 92/s`, `recovery_1 12/s`

So even the batched frontier did not clear the bar for `0.6`. It was reverted.

### Current conclusion

The reservation plane is still a plausible long-term direction, but only if
promotion is much cheaper than the spikes we tried:

- batched reservation promotion
- or another design that separates reservation from attempt start without
  introducing one extra transaction per job

For the `0.6` branch, every reservation-plane implementation was reverted.
The design note remains because it explains the right safety boundary if we
return to this later:

- `reserved but not started`
- `active attempt`

must remain distinct.

## Recommended redesign

### 1. Split the current lease plane into two structures

#### `lease_claims`

Append-only claim receipts.

Each claim appends one immutable row containing:

- `(job_id, run_lease)`
- ready reference `(ready_slot, ready_generation, lane_seq)`
- queue / priority
- `claimed_at`
- static attempt metadata (`attempt`, `max_attempts`)

This row replaces the current use of `leases` as the durable "claim happened"
record.

#### `open_receipt_claims`

Bounded live frontier for currently-open receipt-backed attempts.

This table duplicates only the metadata the runtime needs to answer hot-path
questions without scanning append-only history:

- queue / priority / lane ordering for counts and lookup
- ready reference for hydration and rescue
- `claimed_at` for the short-attempt grace window

`lease_claims` remains the durable claim history; `open_receipt_claims`
exists only while the attempt is still live on the receipt path.

#### `attempt_state`

Optional mutable execution state keyed by `(job_id, run_lease)`.

This row exists only when an attempt needs mutable runtime state:

- heartbeat / staleness tracking
- hard deadline tracking
- callback wait metadata
- progress checkpoint
- temporary callback result / wait bookkeeping

Short jobs should claim and complete without ever creating `attempt_state`.

### 2. Make the immutable terminal / deferred rows the closure record

The closure of an attempt is already visible in immutable storage:

- `done_entries`
- `deferred_jobs`
- `dlq_entries`

Those rows already carry `job_id` and `run_lease`.

So for the short-job path, we do not need a mutable lease delete to say
"running has ended". The closure is the next immutable append.

### 3. Rescue becomes two-tiered

#### Tier A: short-attempt grace window

For attempts with a `lease_claims` row but **no** `attempt_state` row:

- treat them as short, recently claimed attempts
- do not rescue them immediately
- once `claimed_at` exceeds a configured grace/staleness window and there is
  still no closure row, rescue them

This covers workers that crash before the first heartbeat or before any
callback wait registration.

#### Tier B: explicit long-running attempt state

For attempts with `attempt_state`:

- use current heartbeat / deadline / callback-timeout rescue semantics
- all mutable rescue scanning happens against `attempt_state`, not the claim
  receipts

That keeps rescue correctness while avoiding mutable rows for short jobs.

## Why this is better than the current mutable lease table

### Short jobs

Current:

- insert mutable lease row
- delete mutable lease row

Proposed:

- append immutable `lease_claims`
- append immutable terminal / deferred row
- no mutable lease row at all

That removes the steady insert/delete churn from the common path.

### Long-running jobs

Current:

- mutable lease row exists from claim time onward
- heartbeat updates the lease row

Proposed:

- immutable claim receipt at claim time
- mutable `attempt_state` row only after the job actually needs runtime state

That makes mutable churn track "active long-running attempts" rather than
"every claim".

### Control-plane simplification

If `lease_claims` is append-only, the lease ring / prune machinery becomes
closer to the queue ring:

- rotation is about sealing append-only claim segments
- prune no longer needs to wait for every short completion to delete its live
  row

The mutable control plane shrinks to `attempt_state` plus small rotation
metadata.

## Invariants to preserve

Any implementation of this redesign must preserve:

- stale writer protection by `(job_id, run_lease)`
- exactly-once terminalization per attempt
- no rescue before the short-attempt grace window expires
- heartbeat / deadline / callback rescue tied only to `attempt_state`
- completion path must still be able to return "already rescued/cancelled" for
  stale workers

## Likely implementation shape

### Schema

Add:

- `{schema}.lease_claims`
- rotating `lease_claims_<slot>` partitions
- maybe `{schema}.lease_claim_ring_state` if we keep the current lease ring
  separate from ready segments

Retain and narrow:

- `{schema}.attempt_state`

Retire or shrink:

- `{schema}.leases`

### Claim

Replace:

- insert into `{schema}.leases`

With:

- append to `{schema}.lease_claims`

No `attempt_state` row yet.

### Heartbeat / callback wait / progress

First touch lazily creates `attempt_state`.

That row then becomes the authoritative mutable state for:

- `heartbeat_at`
- `deadline_at`
- callback metadata
- progress

### Completion / retry / failure / cancel

For short jobs:

- append immutable closure row only

For long-running jobs:

- append immutable closure row
- delete `attempt_state`

### Rescue

Short-path rescue scans:

- `lease_claims`
- left join against closure rows and `attempt_state`
- rescue only claims older than the short-attempt grace window

Long-path rescue scans:

- `attempt_state`

## Risks

- Rescue logic becomes more subtle because "currently running" is no longer
  represented by one mutable row family.
- Short-attempt rescue must be carefully bounded so we do not rescue healthy
  jobs before they have a chance to heartbeat or complete.
- Admin/state inspection will need a clear view of "claimed but not yet
  materialized into attempt_state".
- TLA+ coverage will need to model the short-attempt grace window explicitly.

## Recommendation

The next real storage redesign should be:

1. keep the current split queue heads
2. stop trying to micro-tune the mutable `leases` ring
3. replace `leases` with:
   - append-only `lease_claims`
   - optional mutable `attempt_state`
4. make rescue explicitly two-tiered:
   - claim-age based for short attempts
   - heartbeat/deadline/callback based for long attempts

That is the design most likely to remove the remaining lease-plane dead-tuple
pressure without giving up Awa's dispatch, rescue, callback, and stale-writer
guarantees.

## Narrow implementation spike

There is now a narrow experimental path in the branch for **short successful
jobs only**:

- append-only `lease_claims`
- bounded `open_receipt_claims`
- append-only `lease_claim_closures`
- no mutable `leases` row on the common short path
- lazy materialization into `attempt_state` on first heartbeat / progress
- lazy materialization into `leases` on callback registration or rarer
  lease-specific mutation paths
- stale short claims that never materialize are rescued append-only after the
  grace window by writing a rescue closure and requeueing the attempt
- no `attempt_state` row unless the attempt actually needs mutable callback or
  progress state
- guarded so it only activates when queue `deadline_duration = 0`

That spike is deliberately not the full redesign above. It exists to answer one
question: does removing the insert/delete churn on short claims materially help
steady-state MVCC behavior?

### Measured result

Short portable profile, `awa` only, before vs after the short-job receipt path:

- `clean_1` median dead tuples: `1647.5 -> 178.5`
- `readers_1` median dead tuples: `2927.0 -> 727.0`
- `pressure_1` median dead tuples: `2882.5 -> 610.5`
- `recovery_1` median dead tuples: `1971.5 -> 218.0`

Throughput stayed effectively flat in the same profile:

- `clean_1`: `~800/s`
- `readers_1`: `~800/s`
- `pressure_1`: `~1200/s`
- `recovery_1`: `~800/s`

The trade-off in the current spike is latency:

- subscriber / end-to-end p99 got worse, especially in `pressure_1` and
  `recovery_1`

So the spike validates the direction:

- append-only short-claim receipts dramatically reduce steady-state dead tuples
- heartbeat/progress-only receipt-backed attempts can stay off the mutable
  lease heap while still getting stale-heartbeat rescue
- long-horizon runs still need a bounded live frontier; otherwise queue counts
  and receipt rescue degrade into append-only history scans
- the next work is making the materialized long-running path cheaper on the
  delivery path, and then extending the same model further across more of the
  long-running rescue surface

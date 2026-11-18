---- MODULE AwaStorageLockOrder ----
EXTENDS TLC, Naturals, FiniteSets, Sequences

\* Lock-ordering protocol spec for the queue-storage engine.
\*
\* The data-level specs (AwaSegmentedStorage, AwaSegmentedStorageRaces)
\* deliberately abstract away from Postgres locks. This spec models the
\* SQL locks directly so a future refactor that weakens or re-orders
\* them trips an invariant.
\*
\* The claim/rotate/prune race against the lease ring is mitigated by:
\*   - the rotators taking FOR UPDATE on lease_ring_state and the
\*     target slot row (so two rotators serialise),
\*   - LOCK TABLE ACCESS EXCLUSIVE on the partition child under a bounded
\*     lock_timeout (so prune can break starvation without queueing
\*     indefinitely ahead of worker traffic before truncating).
\* Note: the claim CTE reads lease_ring_state with a plain SELECT, NOT
\* a FOR SHARE. The conflict detection between claim and rotate is the
\* rotator's CAS UPDATE (`WHERE current_slot=$ AND generation=$`) plus
\* the rotator's empty-partition busy-check, not a row-level lock.
\* The plans below treat the ring-state read as a no-lock step (it is
\* simply elided from the plan).
\*
\* ADR-023/026 extends the locked resources with the claim ring
\* (claim_ring_state, claim_ring_slots, lease_claims / lease_claim_batches
\* child partitions, lease_claim_closures child partitions, compact
\* closure-batch child partitions) and the parallel rotate/prune
\* plans for the claim ring. The claim path takes a RowExclusive on
\* the lease_claims or lease_claim_batches child partition (receipts mode)
\* or the leases child partition (legacy mode). Closure writes on terminal transitions take
\* a RowExclusive on the lease_claim_closures or
\* lease_claim_closure_batches child partition matching the originating
\* claim.
\*
\* What it models:
\* - each transaction as an ordered sequence of (resource, mode) lock
\*   acquisitions
\* - simplified lock mode compatibility: shared (S) vs exclusive (X)
\*   (S/S is compatible; everything else conflicts)
\* - blocking: a tx that wants a lock incompatible with one held by
\*   another tx enters a "waiting" state and records what it is waiting
\*   for
\* - commit: an unblocked tx that has acquired all its locks releases
\*   them and terminates
\*
\* What it checks:
\* - NoDeadlock: the waits-for graph is acyclic
\* - LockCompatibility: no two incompatible locks are held on the same
\*   resource at the same time
\* - HeldOnlyByAliveTxs: committed/aborted txs hold no locks
\*
\* What it intentionally does not model:
\* - MVCC / snapshot isolation — we only care about lock-order safety
\* - the actual data under the locks — AwaSegmentedStorage covers that
\* - Postgres's lock-timeout or deadlock detector abort choice — we
\*   flag cycles as safety violations so the spec fails fast rather
\*   than modelling the race-to-abort
\* - implicit table-level locks beyond what is explicitly named in each
\*   plan; we treat each named resource as the lock unit

CONSTANTS TxIds,
          Queues,
          StripeA,
          StripeB,
          Priorities,
          LeaseSlots,
          ReadySlots,
          ClaimSlots

\* Lock modes. We collapse the Postgres matrix to S/X.
\*
\* `ModeShared` covers everything in Postgres that allows other writers
\* to proceed concurrently: AccessShare (plain SELECT), RowShare (SELECT
\* FOR UPDATE / FOR SHARE), and RowExclusive (INSERT / UPDATE / DELETE).
\* These all share with each other at the table level — two concurrent
\* INSERTs to the same partition do not deadlock at the table level.
\* Row-level FOR UPDATE conflicts on the same row are deliberately
\* abstracted away; the spec models partition-level locking, not
\* row-level. Conflicts that fall through that abstraction (e.g. two
\* admin cancels of the same job) serialise via row locks the spec
\* doesn't track, and they can't deadlock with anything else because
\* they're per-row.
\*
\* `ModeExclusive` covers AccessExclusive (LOCK TABLE / TRUNCATE) and
\* the FOR UPDATE on singleton rows like `lease_ring_state`. For a
\* singleton row, "FOR UPDATE" really IS exclusive at the abstraction
\* level — only one tx at a time gets to advance the cursor.
\*
\* The compatibility check therefore is: S/S allowed, S/X and X/X both
\* conflict.
ModeShared == "S"
ModeExclusive == "X"
Modes == {ModeShared, ModeExclusive}

Compatible(held, wanted) == held = ModeShared /\ wanted = ModeShared

\* Resource identities. We model each parameterised resource as a record
\* so its identity is uniquely carried by its parameters.
\*
\* `LaneResource` carries `(queue, priority)` only; the shard plane
\* introduced by ADR-025 splits each `(queue, priority)` into N
\* independent shard rows in `queue_enqueue_heads` / `queue_claim_heads`
\* / `ready_entries` / `leases` / `done_entries`. At the lock-order
\* abstraction layer, each shard is a disjoint copy of the same set of
\* row-level locks, so the existing deadlock and lock-compatibility
\* proofs hold per shard. The cross-shard invariant — one enqueue
\* transaction touches exactly one shard's head rows, never two — is
\* enforced in the Rust implementation by `shard_for_enqueue` picking
\* a single shard per `insert_*_tx` call. Two transactions on the same
\* lane but different shards lock disjoint resources and cannot
\* deadlock; two transactions on the same lane and shard reduce to
\* the existing `LaneResource(q, p)` model.
LaneResource(q, p) == [k |-> "queue_lane", q |-> q, p |-> p]
\* Creating a new lane takes two more locks before the enqueue lock. The
\* `queue_lanes` row: an uncommitted insert of a `(queue, priority)` key
\* makes a second inserter of that key wait for its transaction. The
\* provisioning advisory lock `{schema}:{queue}:{priority}:{shard}`, under
\* which the lane's sequences and head rows are created. Once a lane
\* exists neither blocks, so modelling both as held to commit
\* over-approximates every enqueue after the first.
LaneRowResource(q, p) == [k |-> "queue_lane_row", q |-> q, p |-> p]
LaneProvisionResource(q, p) == [k |-> "queue_lane_provision", q |-> q, p |-> p]
LeaseRingStateResource == [k |-> "lease_ring_state"]
LeaseRingSlotResource(s) == [k |-> "lease_ring_slot", s |-> s]
LeaseChildResource(s) == [k |-> "lease_child", s |-> s]
QueueRingStateResource == [k |-> "queue_ring_state"]
QueueRingSlotResource(s) == [k |-> "queue_ring_slot", s |-> s]
ReadyChildResource(s) == [k |-> "ready_child", s |-> s]
ReadyClaimAttemptBatchChildResource(s) == [k |-> "ready_claim_attempt_batch_child", s |-> s]
ReadyTombstoneChildResource(s) == [k |-> "ready_tombstone_child", s |-> s]
ReadySegmentChildResource(s) == [k |-> "ready_segment_child", s |-> s]
DoneChildResource(s) == [k |-> "done_child", s |-> s]
ReceiptBatchChildResource(s) == [k |-> "receipt_batch_child", s |-> s]
ReceiptTombstoneChildResource(s) == [k |-> "receipt_tombstone_child", s |-> s]
TerminalDeltaChildResource(s) == [k |-> "terminal_delta_child", s |-> s]
LeasesParentResource == [k |-> "leases_parent"]
ClaimProofResource == [k |-> "claim_proof_children"]
ClosureProofResource == [k |-> "closure_proof_children"]
ClosureBatchProofResource == [k |-> "closure_batch_proof_children"]
ClaimRingStateResource == [k |-> "claim_ring_state"]
ClaimRingSlotResource(s) == [k |-> "claim_ring_slot", s |-> s]
ClaimChildResource(s) == [k |-> "claim_child", s |-> s]
ClaimBatchChildResource(s) == [k |-> "claim_batch_child", s |-> s]
ClosureChildResource(s) == [k |-> "closure_child", s |-> s]
ClosureBatchChildResource(s) == [k |-> "closure_batch_child", s |-> s]
\* The `awa.job_unique_claims` row of one unique key. An uncommitted insert
\* of the key, the row lock `ON CONFLICT DO UPDATE` takes on an existing
\* claim, and an uncommitted release (`DELETE`) each make every other
\* claimant or releaser of the key wait for the transaction.
UniqueClaimResource(u) == [k |-> "unique_claim", u |-> u]

\* Two keys, ordered 1 < 2 as their bytes sort.
UniqueKeys == {1, 2}

LaneResources == { LaneResource(q, p) : q \in Queues, p \in Priorities }
LaneRowResources == { LaneRowResource(q, p) : q \in Queues, p \in Priorities }
LaneProvisionResources == { LaneProvisionResource(q, p) : q \in Queues, p \in Priorities }
LeaseRingSlotResources == { LeaseRingSlotResource(s) : s \in LeaseSlots }
LeaseChildResources == { LeaseChildResource(s) : s \in LeaseSlots }
QueueRingSlotResources == { QueueRingSlotResource(s) : s \in ReadySlots }
ReadyChildResources == { ReadyChildResource(s) : s \in ReadySlots }
ReadyClaimAttemptBatchChildResources == { ReadyClaimAttemptBatchChildResource(s) : s \in ReadySlots }
ReadyTombstoneChildResources == { ReadyTombstoneChildResource(s) : s \in ReadySlots }
ReadySegmentChildResources == { ReadySegmentChildResource(s) : s \in ReadySlots }
DoneChildResources == { DoneChildResource(s) : s \in ReadySlots }
ReceiptBatchChildResources == { ReceiptBatchChildResource(s) : s \in ReadySlots }
ReceiptTombstoneChildResources == { ReceiptTombstoneChildResource(s) : s \in ReadySlots }
TerminalDeltaChildResources == { TerminalDeltaChildResource(s) : s \in ReadySlots }
ClaimRingSlotResources == { ClaimRingSlotResource(s) : s \in ClaimSlots }
ClaimChildResources == { ClaimChildResource(s) : s \in ClaimSlots }
ClaimBatchChildResources == { ClaimBatchChildResource(s) : s \in ClaimSlots }
ClosureChildResources == { ClosureChildResource(s) : s \in ClaimSlots }
ClosureBatchChildResources == { ClosureBatchChildResource(s) : s \in ClaimSlots }
UniqueClaimResources == { UniqueClaimResource(u) : u \in UniqueKeys }

Resources ==
    LaneResources \cup
    LaneRowResources \cup
    LaneProvisionResources \cup
    {LeaseRingStateResource} \cup
    LeaseRingSlotResources \cup
    LeaseChildResources \cup
    {QueueRingStateResource} \cup
    QueueRingSlotResources \cup
    ReadyChildResources \cup
    ReadyClaimAttemptBatchChildResources \cup
    ReadyTombstoneChildResources \cup
    ReadySegmentChildResources \cup
    DoneChildResources \cup
    ReceiptBatchChildResources \cup
    ReceiptTombstoneChildResources \cup
    TerminalDeltaChildResources \cup
    {LeasesParentResource} \cup
    {ClaimProofResource} \cup
    {ClosureProofResource} \cup
    {ClosureBatchProofResource} \cup
    {ClaimRingStateResource} \cup
    ClaimRingSlotResources \cup
    ClaimChildResources \cup
    ClaimBatchChildResources \cup
    ClosureChildResources \cup
    ClosureBatchChildResources \cup
    UniqueClaimResources

\* A plan step is [res, mode].
Step(res, mode) == [res |-> res, mode |-> mode]

\* A small, explicit two-stripe shape for the hot logical queue case.
\* `StripeA` / `StripeB` are physical queue names such as `queue#0`
\* and `queue#1`. The enqueue path processes grouped physical queues
\* in a stable order inside one transaction. Runtime claim used to
\* visit multiple physical stripes inside one claim transaction; the
\* Rust implementation now claims each physical stripe in its own
\* transaction, so the main spec only starts single-stripe claim plans.
TwoStripeQueuesOK ==
    /\ StripeA \in Queues
    /\ StripeB \in Queues
    /\ StripeA # StripeB

\* Transaction kind plans. Each mirrors the actual SQL in
\* awa-model/src/queue_storage.rs as noted inline.

\* insert_ready_rows_tx / insert_ready_rows_copy_tx
\*   reserve_enqueue_seq takes a transaction-scoped lane lock for each
\*   grouped physical queue
\*   INSERT / COPY ready rows
\*   INSERT ready_segments for each grouped physical queue
\*
\* The model collapses the reserve_enqueue_seq advisory lock and lane rows
\* into LaneResource, because the deadlock class we care about is
\* multi-stripe transactions taking lane-family locks in different
\* physical-queue orders. Enqueue keeps a stable physical-stripe order.
EnqueueTwoStripePlan(p) ==
    << Step(LaneResource(StripeA, p), ModeExclusive),
       Step(LaneResource(StripeB, p), ModeExclusive) >>

\* First enqueue to a `(queue, priority)` lane, by any producer:
\*   SQL inserts (`awa.insert_job`, `insert_jobs_compat_batch`):
\*     INSERT INTO queue_lanes ... ON CONFLICT DO NOTHING
\*     INSERT INTO queue_enqueue_heads, whose BEFORE INSERT trigger takes
\*       the provisioning lock to create the lane's sequences
\*     reserve_enqueue_seq takes the lane's enqueue lock
\*   QueueStorage::ensure_lane_inserts (batch enqueue, promotion of
\*   scheduled or retried jobs, prepare_queue):
\*     INSERT INTO queue_lanes ... ON CONFLICT DO NOTHING
\*     pg_advisory_xact_lock on the provisioning key, then provision_lane
\*       or the head-row inserts
\*     reserve_enqueue_seq takes the lane's enqueue lock
ProvisionLanePlan(q, p) ==
    << Step(LaneRowResource(q, p), ModeExclusive),
       Step(LaneProvisionResource(q, p), ModeExclusive),
       Step(LaneResource(q, p), ModeExclusive) >>

\* QueueStorage::ensure_lane_inserts through 0.6.15 took the provisioning
\* lock before writing the `queue_lanes` row, the reverse of the SQL
\* inserts. Kept only as a negative regression harness.
OldQueueStorageProvisionLanePlan(q, p) ==
    << Step(LaneProvisionResource(q, p), ModeExclusive),
       Step(LaneRowResource(q, p), ModeExclusive),
       Step(LaneResource(q, p), ModeExclusive) >>

\* Enqueue of a job with a unique key, fresh or re-enqueued, takes the key's
\* claim before the lane's enqueue lock:
\*   fresh: `awa.insert_job_compat` (claim, then reserve_enqueue_seq);
\*     `insert_jobs_compat_batch`, `QueueStorage::insert_ready_rows_tx` and
\*     `insert_ready_rows_copy_tx` (every claim in key order, then lanes)
\*   re-enqueue: `QueueStorage::insert_existing_ready_rows_inner_tx`
\*     (promotion, callback resume, admin retry, DLQ retry single, legacy
\*     bulk and batched, priority aging) syncs every claim in key order
\*     before `ensure_lane` / `advance_enqueue_head`
\*   deferred cancel and migrated-reschedule cancellation release or claim
\*     the key before `ensure_lane` provisions shard 0
UniqueEnqueuePlan(u, q, p) ==
    << Step(UniqueClaimResource(u), ModeExclusive),
       Step(LaneResource(q, p), ModeExclusive) >>

\* A batch whose rows carry both keys and land on both stripes: claims in
\* key order, then lanes in canonical order.
UniqueBatchEnqueuePlan(p) ==
    << Step(UniqueClaimResource(1), ModeExclusive),
       Step(UniqueClaimResource(2), ModeExclusive),
       Step(LaneResource(StripeA, p), ModeExclusive),
       Step(LaneResource(StripeB, p), ModeExclusive) >>

\* Key-order rule: within one transaction every change to
\* `awa.job_unique_claims`, claim or release, happens in bytewise key order,
\* and all of them happen before any lane lock. A claim and a release of a
\* key both hold the key's row exclusively until commit, so the model does
\* not tell them apart.
\*
\* A transaction that releases or claims several jobs' keys in one pass
\* (`QueueStorage::apply_unique_claim_changes_tx`), then may ensure a lane:
\*   bulk cancel (`cancel_jobs_by_ids`, `cancel_job`, `cancel_job_in_tx`:
\*     one pass over every cancelled job's change, then `ensure_lane` for
\*     cancelled deferred jobs)
\*   drain chunks (`cancel_deferred_chunk_tx` then `ensure_lane`;
\*     `cancel_available_chunk_tx`)
\*   batch completion (`complete_claimed_batch`,
\*     `complete_runtime_batch_slow_in_tx` across its receipt and
\*     materialized groups, `complete_job_batch_by_id_in_tx`)
\*   claim (`claim_runtime_batch_with_aging_physical`), rescue
\*     (`settle_rescued_attempts_tx` across every batch, retry, failure and
\*     DLQ outcome), `discard_failed_by_kind` across done and DLQ rows,
\*     `bulk_move_failed_to_dlq`, and every single-row `insert_*_rows_tx`
UniqueClaimPassPlan(q, p) ==
    << Step(UniqueClaimResource(1), ModeExclusive),
       Step(UniqueClaimResource(2), ModeExclusive),
       Step(LaneResource(q, p), ModeExclusive) >>

\* A transition that also enqueues ADR-029 follow-ups in its transaction
\* (`complete_queue_storage_with_followups` and the other executor
\* `*_with_followups` paths) makes two claim passes: the parent's
\* transition changes its claim, then each follow-up insert
\* (`awa.insert_job_compat`) claims the follow-up's key and takes its lane.
\* The key-order rule holds within each pass but not across them, so a
\* parent releasing key `a` with a follow-up claiming key `b < a` breaks it.
\* Not part of `Next`: this is a known gap, checked only by the
\* counterexample harness below.
CompleteWithFollowUpPlan(a, b, q, p) ==
    << Step(UniqueClaimResource(a), ModeExclusive),
       Step(UniqueClaimResource(b), ModeExclusive),
       Step(LaneResource(q, p), ModeExclusive) >>

\* Through 0.6.18 those paths changed claims in row order, one pass per
\* group of rows. Kept only as a negative regression harness.
OldRowOrderClaimPassPlan ==
    << Step(UniqueClaimResource(2), ModeExclusive),
       Step(UniqueClaimResource(1), ModeExclusive) >>

\* Re-enqueue through 0.6.18 reserved the lane before syncing the row's
\* claim, the reverse of a fresh enqueue. Kept only as a negative
\* regression harness.
OldReEnqueueUniquePlan(u, q, p) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(UniqueClaimResource(u), ModeExclusive) >>

\* The claim CTE
\* (`claim_runtime_batch_with_aging_for_instance` in queue_storage.rs)
\*   FOR UPDATE OF queue_claim_heads SKIP LOCKED (per-(queue,priority) row)
\*   refresh queue_claim_heads.ready_segment_* on the already-locked row
\*     when the cached ready-slot hint no longer validates
\*   plain SELECT of lease_ring_state and claim_ring_state — no
\*     FOR SHARE / FOR UPDATE; serialisation against rotate is via
\*     the rotator's CAS UPDATE on (current_slot, generation), not a
\*     row lock here, so these reads are NOT modelled as plan steps.
\*   plain SELECT of ready_entries_<slot> (implicit AccessShare on child)
\*   INSERT INTO ready_claim_attempt_batches_<ready_slot> (receipts mode)
\*   INSERT INTO lease_claim_batches_<claim_slot> or
\*     lease_claims_<claim_slot> (receipts mode) OR
\*   INSERT INTO leases_<lease_slot> (legacy mode)
\*   UPDATE queue_claim_heads (already locked, no new acquire)
\*
\* Two distinct plans because the receipts and legacy modes write to
\* different children. `experimental_lease_claim_receipts` selects
\* between them at runtime.
\* RowExclusive on partition children (INSERT/UPDATE/DELETE/SELECT FOR
\* UPDATE) is `ModeShared` in this spec — see the lock-mode comment
\* above. The lane row uses `ModeExclusive` because `FOR UPDATE OF
\* queue_claim_heads` on the same lane row really does serialise.
ClaimReceiptsPlan(q, p, readySlot, claimSlot) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(ReadyClaimAttemptBatchChildResource(readySlot), ModeShared),
       Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared) >>

ClaimLegacyPlan(q, p, readySlot, leaseSlot) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeShared) >>

\* Historical unsafe logical-queue claim shape: one transaction walked
\* multiple physical stripes starting at a rotating probe point. This
\* can invert the stable enqueue order and form a waits-for cycle:
\* enqueue holds StripeA then wants StripeB while claim holds StripeB
\* then wants StripeA. The production path no longer uses this shape;
\* it remains in the spec only as a negative regression harness.
OldClaimTwoStripeReceiptsPlan(p, readySlot, claimSlot) ==
    << Step(LaneResource(StripeB, p), ModeExclusive),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(LaneResource(StripeA, p), ModeExclusive),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared) >>

\* complete_runtime_batch receipt branch (ADR-023/026)
\*   No queue_lanes lock (completion does not gate on a lane row)
\*   Successful compact receipt completion validates the exact claim by
\*   receipt_id, reads closure evidence to stay idempotent, writes compact
\*   claim-local closure evidence, and compact receipt completion history.
\*   Compact counts are read from
\*   retained receipt batches, so this path does not touch the terminal-count
\*   delta children.
\*   It also anti-joins the leases parent so materialized receipt claims
\*   fall back to the lease-deleting completion path. That fallback matches
\*   materialized receipt leases by stable ready-lane / attempt identity
\*   because the lease ring can rotate after the original receipt claim.
\*   Rust first takes per-(job_id, run_lease) advisory transaction locks in
\*   deterministic key order. Known-key materialization, explicit close,
\*   and receipt rescue paths use the same advisory key before row locks.
\*   Those locks
\*   serialize same-attempt state changes but do not participate in the
\*   table-lock waits-for cycles modelled here.
\*   Non-success close paths are modelled by CloseReceiptPlan /
\*   CancelReceiptOnlyPlan / CancelRunningPlan.
CompletePlan(claimSlot, readySlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared),
       Step(LeasesParentResource, ModeShared),
       Step(ReceiptBatchChildResource(readySlot), ModeShared) >>

\* close_receipt_tx (called from cancel_job_tx)
\*   transaction-scoped per-(job_id, run_lease) advisory lock first
\*   WITH locked_claim AS (SELECT ... FROM lease_claims /
\*     lease_claim_batches FOR UPDATE)
\*   INSERT INTO lease_claim_closures ... ON CONFLICT DO NOTHING
CloseReceiptPlan(claimSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared) >>

\* rescue_stale_receipt_claims_tx
\*   plain SELECT of claim_ring_state to prefer the oldest initialized slot
\*   SELECT ... FROM claim_ring_slots[slot] FOR UPDATE
\*   SELECT ... FROM lease_claims / lease_claim_batches claims LEFT JOIN
\*     attempt_state ...
\*     WHERE NOT EXISTS (closures) AND NOT EXISTS (leases)
\*     pg_try_advisory_xact_lock per (job_id, run_lease)
\*     FOR UPDATE OF claims SKIP LOCKED
\*   INSERT INTO lease_claim_closures ... ON CONFLICT DO NOTHING
\*   UPDATE claim_ring_slots[slot] rescue cursor
\* The leases anti-join takes AccessShare on the leases
\* parent so rescue can race against prune_oldest_leases.
\* PostgreSQL takes every relation lock of a statement when it plans it,
\* before the executor takes any row lock. The cursor row's FOR UPDATE is
\* written first in the statement but is therefore acquired last.
RescueReceiptsPlan(claimSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(LeasesParentResource, ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared),
       Step(ClaimRingSlotResource(claimSlot), ModeExclusive) >>

\* rescue_expired_receipt_deadlines_tx
\*   SELECT ... FROM claim_ring_slots[slot] FOR UPDATE
\*   bounded scan of lease_claims_<slot> ordered by deadline_at
\*   anti-join lease_claim_closures_<slot> and leases parent
\*   pg_try_advisory_xact_lock per (job_id, run_lease)
\*   FOR UPDATE OF expired claim rows SKIP LOCKED
\*   INSERT INTO lease_claim_closures ... deadline_expired
\*   UPDATE claim_ring_slots[slot] deadline cursor
\* Same resource order as stale receipt rescue; only the index order and
\* advancement predicate differ.
RescueReceiptDeadlinesPlan(claimSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(LeasesParentResource, ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared),
       Step(ClaimRingSlotResource(claimSlot), ModeExclusive) >>

\* ensure_running_leases_from_receipts_tx
\*   transaction-scoped per-(job_id, run_lease) advisory lock first
\*   CTE claim_refs: SELECT ... FROM lease_claims FOR UPDATE OF claims
\*   anti-join explicit and compact closure evidence
\*   INSERT INTO leases ...
\*   UPDATE lease_claims SET materialized_at = ...
EnsureRunningPlan(claimSlot, leaseSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeShared) >>

\* cancel_job_tx receipt-only branch
\*   SELECT ... FROM lease_claims / lease_claim_batches FOR UPDATE SKIP LOCKED
\*   insert_done_rows_tx → INSERT INTO done_entries
\*   INSERT INTO lease_claim_closures
\*   defensive DELETE FROM leases (sweeps any concurrent materialization)
\*   pg_notify('awa:cancel', ...)
CancelReceiptOnlyPlan(claimSlot, readySlot, leaseSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(DoneChildResource(readySlot), ModeShared),
       Step(TerminalDeltaChildResource(readySlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeShared) >>

\* cancel_job_tx running-lease branch
\*   DELETE FROM leases ... RETURNING
\*   insert_done_rows_tx → INSERT INTO done_entries
\*   close_receipt_tx (claim child + closure child)
\*   pg_notify
CancelRunningPlan(leaseSlot, readySlot, claimSlot) ==
    << Step(LeaseChildResource(leaseSlot), ModeShared),
       Step(DoneChildResource(readySlot), ModeShared),
       Step(TerminalDeltaChildResource(readySlot), ModeShared),
       Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClaimBatchChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared) >>

\* terminal-delete paths that remove done_entries evidence before claim prune
\* can consume the corresponding receipt. Rust materializes an explicit
\* closure in the same transaction. The representative plan covers
\* retry-from-terminal, DLQ move, discard, and SQL compatibility delete.
TerminalDeletePlan(readySlot, claimSlot) ==
    << Step(DoneChildResource(readySlot), ModeShared),
       Step(TerminalDeltaChildResource(readySlot), ModeShared),
       Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(ClosureBatchChildResource(claimSlot), ModeShared) >>

\* batch set_priority / move_queue ready-row lane move
\*   SELECT source ready row FOR UPDATE after reading the source claim-head
\*     cursor. The claim-head row itself is not locked by this path; the row
\*     lock on ready_entries serializes against concurrent claim/cancel of the
\*     same job and does not conflict with producers on lane heads.
\*   INSERT source ready_tombstone
\*   ensure/reserve destination enqueue lane
\*   INSERT destination ready row
\* The implementation serializes the overall batch runner with an advisory
\* lock today. This plan still records the row/table locks so future
\* multi-runner work has an explicit lock-order regression target.
BatchReadyLaneMovePlan(srcQ, srcP, dstQ, dstP, readySlot) ==
    << Step(ReadyChildResource(readySlot), ModeShared),
       Step(LaneResource(dstQ, dstP), ModeExclusive),
       Step(ReadyChildResource(readySlot), ModeShared) >>

\* Maintenance claim-cursor advance over a pinning lane's spent prefix
\* (#779; `AwaMaintenanceCursorAdvance.tla`)
\*   SELECT ... FROM queue_claim_heads FOR UPDATE SKIP LOCKED (lane row)
\*   LOCK TABLE leases, ready_segments, done_entries, ready_entries,
\*     ready_claim_attempt_batches, ready_tombstones IN ACCESS SHARE MODE
\*     NOWAIT, in that order, which is prune's child exclusive-lock order
\*     (with lease-claim receipts it leaves out leases and done_entries,
\*     which its scan does not read)
\*   one statement over those tables that finds the lane's first unspent
\*     row and raises the lane cursor with set_sequence_next
\* The plan names one slot's children. The real LOCK TABLE covers every
\* child of every slot; the statement also reads deferred_jobs,
\* dlq_entries, queue_enqueue_heads and queue_claim_heads under a short
\* transaction-local lock_timeout, and set_sequence_next takes a blocking
\* session advisory lock on the lane cursor that is held only around its
\* setval. Those omissions are safe because the advance never waits on a
\* lock prune or rotation holds: the head is SKIP LOCKED, the queue-slot
\* families are NOWAIT, and the remaining waits are bounded. Modelling
\* the steps as blocking acquires over-approximates it.
MaintenanceCursorAdvancePlan(q, p, readySlot) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(LeasesParentResource, ModeShared),
       Step(ReadySegmentChildResource(readySlot), ModeShared),
       Step(DoneChildResource(readySlot), ModeShared),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(ReadyClaimAttemptBatchChildResource(readySlot), ModeShared),
       Step(ReadyTombstoneChildResource(readySlot), ModeShared) >>

\* rotate_leases
\*   SELECT ... FROM lease_ring_state FOR UPDATE
\*   SELECT count(*) FROM lease_child[next_slot] (AccessShare on child)
\*   UPDATE lease_ring_state / lease_ring_slots
RotateLeasesPlan(nextSlot) ==
    << Step(LeaseRingStateResource, ModeExclusive),
       Step(LeaseChildResource(nextSlot), ModeShared) >>

\* prune_oldest_leases
\*   SELECT ... FROM lease_ring_state FOR UPDATE
\*   SELECT ... FROM lease_ring_slots[slot] FOR UPDATE
\*   LOCK TABLE lease_child[slot] ACCESS EXCLUSIVE with bounded lock_timeout
PruneLeasesPlan(slot) ==
    << Step(LeaseRingStateResource, ModeExclusive),
       Step(LeaseRingSlotResource(slot), ModeExclusive),
       Step(LeaseChildResource(slot), ModeExclusive) >>

\* rotate_ready similar to rotate_leases (approximated)
RotateReadyPlan(nextSlot) ==
    << Step(QueueRingStateResource, ModeExclusive),
       Step(ReadyChildResource(nextSlot), ModeShared),
       Step(ReadyClaimAttemptBatchChildResource(nextSlot), ModeShared),
       Step(ReadyTombstoneChildResource(nextSlot), ModeShared),
       Step(ReadySegmentChildResource(nextSlot), ModeShared),
       Step(DoneChildResource(nextSlot), ModeShared),
       Step(ReceiptBatchChildResource(nextSlot), ModeShared),
       Step(ReceiptTombstoneChildResource(nextSlot), ModeShared),
       Step(TerminalDeltaChildResource(nextSlot), ModeShared) >>

\* prune_oldest
\*   SELECT ... FROM queue_ring_state FOR UPDATE
\*   SELECT ... FROM queue_ring_slots FOR UPDATE
\*   prove sealed-slot skip gates before child exclusive locks:
\*     active leases (leases parent), unclosed receipt claims
\*     (claim/closure proof resources), and pending retained ready rows
\*     at or beyond the lane cursor
\*   LOCK TABLE ready/claim-attempt/done/tombstone/segment/compact/delta children ACCESS EXCLUSIVE with bounded lock_timeout
\*   repeat the active/pending/unclosed proofs after the exclusive child
\*     locks before TRUNCATE
PruneReadyPlan(slot) ==
    << Step(QueueRingStateResource, ModeExclusive),
       Step(QueueRingSlotResource(slot), ModeExclusive),
       Step(LeasesParentResource, ModeShared),
       Step(ClaimProofResource, ModeShared),
       Step(ClosureProofResource, ModeShared),
       Step(ClosureBatchProofResource, ModeShared),
       Step(ReadyChildResource(slot), ModeShared),
       Step(ReadySegmentChildResource(slot), ModeExclusive),
       Step(DoneChildResource(slot), ModeExclusive),
       Step(ReadyChildResource(slot), ModeExclusive),
       Step(ReadyClaimAttemptBatchChildResource(slot), ModeExclusive),
       Step(ReadyTombstoneChildResource(slot), ModeExclusive),
       Step(ReceiptBatchChildResource(slot), ModeExclusive),
       Step(ReceiptTombstoneChildResource(slot), ModeExclusive),
       Step(TerminalDeltaChildResource(slot), ModeExclusive),
       Step(LeasesParentResource, ModeShared),
       Step(ClaimProofResource, ModeShared),
       Step(ClosureProofResource, ModeShared),
       Step(ClosureBatchProofResource, ModeShared),
       Step(ReadyChildResource(slot), ModeShared) >>

\* rotate_claims (ADR-023)
\*   SELECT ... FROM claim_ring_state FOR UPDATE
\*   SELECT count(*) FROM claim_child[next_slot]
\*   SELECT count(*) FROM claim_batch_child[next_slot]
\*   SELECT count(*) FROM closure_child[next_slot]
\*   SELECT count(*) FROM closure_batch_child[next_slot]
\*   UPDATE claim_ring_state
RotateClaimsPlan(nextSlot) ==
    << Step(ClaimRingStateResource, ModeExclusive),
       Step(ClaimChildResource(nextSlot), ModeShared),
       Step(ClaimBatchChildResource(nextSlot), ModeShared),
       Step(ClosureChildResource(nextSlot), ModeShared),
       Step(ClosureBatchChildResource(nextSlot), ModeShared) >>

\* prune_oldest_claims (ADR-023)
\*   SELECT ... FROM claim_ring_state FOR UPDATE
\*   SELECT ... FROM claim_ring_slots[slot] FOR UPDATE
\*   prove every claim in the sealed slot has durable closure evidence
\*     with plain SELECT / AccessShare on the claim children. The Rust
\*     implementation uses an exact count proof; if counts do not prove
\*     closure, it conservatively skips the truncate instead of running
\*     an unbounded per-claim anti-join over retained compact batches.
\*   LOCK TABLE claim_child[slot] ACCESS EXCLUSIVE with bounded lock_timeout
\*   LOCK TABLE claim_batch_child[slot] ACCESS EXCLUSIVE with bounded lock_timeout
\*   LOCK TABLE closure_child[slot] ACCESS EXCLUSIVE with bounded lock_timeout
\*   LOCK TABLE closure_batch_child[slot] ACCESS EXCLUSIVE with bounded lock_timeout
\*   open-claim slots skip before the exclusive lock path; the plan below
\*     models the successful truncate path after the proof has passed
\*   TRUNCATE claim_child[slot] + claim_batch_child[slot] +
\*     closure_child[slot] + closure_batch_child[slot]
\* The slot row is locked only once the partitions are held. Rescue takes
\* the partitions and then the slot row, so the 0.6.16 order below, slot row
\* first, deadlocked with every rescue of the slot for as long as prune
\* waited for the partitions.
OldPruneClaimsPlan(slot) ==
    << Step(ClaimRingStateResource, ModeExclusive),
       Step(ClaimRingSlotResource(slot), ModeExclusive),
       Step(ClaimChildResource(slot), ModeShared),
       Step(ClaimBatchChildResource(slot), ModeShared),
       Step(ClosureChildResource(slot), ModeShared),
       Step(ClosureBatchChildResource(slot), ModeShared),
       Step(ClaimChildResource(slot), ModeExclusive),
       Step(ClaimBatchChildResource(slot), ModeExclusive),
       Step(ClosureChildResource(slot), ModeExclusive),
       Step(ClosureBatchChildResource(slot), ModeExclusive) >>

PruneClaimsPlan(slot) ==
    << Step(ClaimRingStateResource, ModeExclusive),
       Step(ClaimChildResource(slot), ModeShared),
       Step(ClaimBatchChildResource(slot), ModeShared),
       Step(ClosureChildResource(slot), ModeShared),
       Step(ClosureBatchChildResource(slot), ModeShared),
       Step(ClaimChildResource(slot), ModeExclusive),
       Step(ClaimBatchChildResource(slot), ModeExclusive),
       Step(ClosureChildResource(slot), ModeExclusive),
       Step(ClosureBatchChildResource(slot), ModeExclusive),
       Step(ClaimRingSlotResource(slot), ModeExclusive) >>

\* Ascending sequence of a finite set of slot numbers.
RECURSIVE AscendingSeq(_)
AscendingSeq(S) ==
    IF S = {} THEN << >>
    ELSE LET m == CHOOSE x \in S : \A y \in S : x <= y
         IN <<m>> \o AscendingSeq(S \ {m})

\* A shared lock on one family's partition for every slot, in slot order.
SharedOnEachSlot(Res(_), slots) ==
    LET order == AscendingSeq(slots)
    IN [i \in 1..Len(order) |-> Step(Res(order[i]), ModeShared)]

\* rotate_if_active / rotate_leases_if_active / rotate_claims_if_active
\* idle probe: a read-only transaction of its own, run before the rotate
\* transaction and holding no row lock, that reads every partition of the
\* ring with `SELECT EXISTS` on each parent (AccessShare on every child).
\* Parents are probed in the order the ring's prune locks its children, and
\* the probe stops at the first parent with a row; the plans model the
\* longest probe. The queue ring also reads queue_terminal_live_counts,
\* which no modelled transaction locks.
IdleProbeQueuePlan ==
    SharedOnEachSlot(ReadySegmentChildResource, ReadySlots)
    \o SharedOnEachSlot(DoneChildResource, ReadySlots)
    \o SharedOnEachSlot(ReadyChildResource, ReadySlots)
    \o SharedOnEachSlot(ReadyClaimAttemptBatchChildResource, ReadySlots)
    \o SharedOnEachSlot(ReadyTombstoneChildResource, ReadySlots)
    \o SharedOnEachSlot(ReceiptBatchChildResource, ReadySlots)
    \o SharedOnEachSlot(ReceiptTombstoneChildResource, ReadySlots)
    \o SharedOnEachSlot(TerminalDeltaChildResource, ReadySlots)

IdleProbeLeasePlan == SharedOnEachSlot(LeaseChildResource, LeaseSlots)

IdleProbeClaimPlan ==
    SharedOnEachSlot(ClaimChildResource, ClaimSlots)
    \o SharedOnEachSlot(ClaimBatchChildResource, ClaimSlots)
    \o SharedOnEachSlot(ClosureChildResource, ClaimSlots)
    \o SharedOnEachSlot(ClosureBatchChildResource, ClaimSlots)

VARIABLES
    heldLocks,     \* [resource -> set of <<tx, mode>>]
    txState,       \* [tx -> "idle" | "running" | "committed"]
    txPlan,        \* [tx -> Seq]
    txNextStep     \* [tx -> Nat]

vars == <<heldLocks, txState, txPlan, txNextStep>>

EmptyPlan == << >>

Init ==
    /\ heldLocks = [r \in Resources |-> {}]
    /\ txState = [t \in TxIds |-> "idle"]
    /\ txPlan = [t \in TxIds |-> EmptyPlan]
    /\ txNextStep = [t \in TxIds |-> 0]

ConfigOK ==
    TwoStripeQueuesOK

\* Does any other tx hold an incompatible lock on r wrt mode?
Blocked(t, r, mode) ==
    \E u \in TxIds \ {t} :
        \E m \in Modes :
            /\ <<u, m>> \in heldLocks[r]
            /\ ~ Compatible(m, mode)

\* Which txs currently block t on its pending step?
BlockingTxsForStep(t, step) ==
    { u \in TxIds \ {t} :
        \E m \in Modes :
            /\ <<u, m>> \in heldLocks[step.res]
            /\ ~ Compatible(m, step.mode) }

\* Start a Claim transaction in receipts mode. Different txs can pick
\* different slots, exercising the protocol across interleavings.
StartClaimReceipts(t, q, p, readySlot, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ readySlot \in ReadySlots
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = ClaimReceiptsPlan(q, p, readySlot, claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

\* Start a Claim transaction in legacy (non-receipts) mode. The two
\* modes don't run interleaved in production — the runtime config
\* picks one — but modelling both lets TLC explore each shape.
StartClaimLegacy(t, q, p, readySlot, leaseSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ readySlot \in ReadySlots
    /\ leaseSlot \in LeaseSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = ClaimLegacyPlan(q, p, readySlot, leaseSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartEnqueueTwoStripe(t, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = EnqueueTwoStripePlan(p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartProvisionLane(t, q, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = ProvisionLanePlan(q, p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartUniqueEnqueue(t, u, q, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ u \in UniqueKeys
    /\ q \in Queues
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = UniqueEnqueuePlan(u, q, p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartUniqueBatchEnqueue(t, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = UniqueBatchEnqueuePlan(p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartUniqueClaimPass(t, q, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = UniqueClaimPassPlan(q, p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartComplete(t, claimSlot, readySlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ readySlot \in ReadySlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CompletePlan(claimSlot, readySlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartMaintenanceCursorAdvance(t, q, p, readySlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ readySlot \in ReadySlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = MaintenanceCursorAdvancePlan(q, p, readySlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartRotateLeases(t, nextSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ nextSlot \in LeaseSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = RotateLeasesPlan(nextSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartPruneLeases(t, slot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ slot \in LeaseSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = PruneLeasesPlan(slot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartRotateReady(t, nextSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ nextSlot \in ReadySlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = RotateReadyPlan(nextSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartPruneReady(t, slot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ slot \in ReadySlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = PruneReadyPlan(slot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartRotateClaims(t, nextSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ nextSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = RotateClaimsPlan(nextSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartPruneClaims(t, slot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ slot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = PruneClaimsPlan(slot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartIdleProbe(t, plan) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = plan]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartCloseReceipt(t, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CloseReceiptPlan(claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartRescueReceipts(t, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = RescueReceiptsPlan(claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartRescueReceiptDeadlines(t, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = RescueReceiptDeadlinesPlan(claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartEnsureRunning(t, claimSlot, leaseSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ leaseSlot \in LeaseSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = EnsureRunningPlan(claimSlot, leaseSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartCancelReceiptOnly(t, claimSlot, readySlot, leaseSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ readySlot \in ReadySlots
    /\ leaseSlot \in LeaseSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CancelReceiptOnlyPlan(claimSlot, readySlot, leaseSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartCancelRunning(t, leaseSlot, readySlot, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ leaseSlot \in LeaseSlots
    /\ readySlot \in ReadySlots
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CancelRunningPlan(leaseSlot, readySlot, claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartTerminalDelete(t, readySlot, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ readySlot \in ReadySlots
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = TerminalDeletePlan(readySlot, claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartBatchReadyLaneMove(t, srcQ, srcP, dstQ, dstP, readySlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ srcQ \in Queues
    /\ dstQ \in Queues
    /\ srcP \in Priorities
    /\ dstP \in Priorities
    /\ readySlot \in ReadySlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = BatchReadyLaneMovePlan(srcQ, srcP, dstQ, dstP, readySlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

\* Try to acquire the next lock in t's plan. Enabled iff no conflict.
\* A blocked tx does not fire this — it simply sits until the blocker
\* commits. The state-space accounts for "some other tx commits first"
\* naturally.
AcquireNext(t) ==
    /\ t \in TxIds
    /\ txState[t] = "running"
    /\ txNextStep[t] > 0
    /\ txNextStep[t] <= Len(txPlan[t])
    /\ LET step == txPlan[t][txNextStep[t]]
       IN
       /\ ~ Blocked(t, step.res, step.mode)
       /\ heldLocks' = [heldLocks EXCEPT
                           ![step.res] = heldLocks[step.res] \cup {<<t, step.mode>>}]
       /\ txNextStep' = [txNextStep EXCEPT ![t] = txNextStep[t] + 1]
    /\ UNCHANGED <<txState, txPlan>>

\* Commit once all plan steps have been acquired. Release everything.
Commit(t) ==
    /\ t \in TxIds
    /\ txState[t] = "running"
    /\ txNextStep[t] > Len(txPlan[t])
    /\ txState' = [txState EXCEPT ![t] = "committed"]
    /\ heldLocks' = [r \in Resources |->
                        {<<u, m>> \in heldLocks[r] : u # t}]
    /\ txPlan' = [txPlan EXCEPT ![t] = EmptyPlan]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 0]

\* Committed txs can be recycled back to idle so TLC can explore
\* further interleavings within a bounded state space.
Recycle(t) ==
    /\ t \in TxIds
    /\ txState[t] = "committed"
    /\ txState' = [txState EXCEPT ![t] = "idle"]
    /\ UNCHANGED <<heldLocks, txPlan, txNextStep>>

Stutter == /\ UNCHANGED vars

Next ==
    \/ \E t \in TxIds, p \in Priorities : StartEnqueueTwoStripe(t, p)
    \/ \E t \in TxIds, q \in Queues, p \in Priorities : StartProvisionLane(t, q, p)
    \/ \E t \in TxIds, u \in UniqueKeys, q \in Queues, p \in Priorities :
          StartUniqueEnqueue(t, u, q, p)
    \/ \E t \in TxIds, p \in Priorities : StartUniqueBatchEnqueue(t, p)
    \/ \E t \in TxIds, q \in Queues, p \in Priorities :
          StartUniqueClaimPass(t, q, p)
    \/ \E t \in TxIds, q \in Queues, p \in Priorities,
         rs \in ReadySlots, cs \in ClaimSlots :
          StartClaimReceipts(t, q, p, rs, cs)
    \/ \E t \in TxIds, q \in Queues, p \in Priorities,
         rs \in ReadySlots, ls \in LeaseSlots :
          StartClaimLegacy(t, q, p, rs, ls)
    \/ \E t \in TxIds, cs \in ClaimSlots, rs \in ReadySlots :
          StartComplete(t, cs, rs)
    \/ \E t \in TxIds, ls \in LeaseSlots : StartRotateLeases(t, ls)
    \/ \E t \in TxIds, ls \in LeaseSlots : StartPruneLeases(t, ls)
    \/ \E t \in TxIds, rs \in ReadySlots : StartRotateReady(t, rs)
    \/ \E t \in TxIds, rs \in ReadySlots : StartPruneReady(t, rs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartRotateClaims(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartPruneClaims(t, cs)
    \/ \E t \in TxIds : StartIdleProbe(t, IdleProbeQueuePlan)
    \/ \E t \in TxIds : StartIdleProbe(t, IdleProbeLeasePlan)
    \/ \E t \in TxIds : StartIdleProbe(t, IdleProbeClaimPlan)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartCloseReceipt(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartRescueReceipts(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartRescueReceiptDeadlines(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots, ls \in LeaseSlots :
          StartEnsureRunning(t, cs, ls)
    \/ \E t \in TxIds, cs \in ClaimSlots, rs \in ReadySlots,
         ls \in LeaseSlots :
          StartCancelReceiptOnly(t, cs, rs, ls)
    \/ \E t \in TxIds, ls \in LeaseSlots, rs \in ReadySlots,
         cs \in ClaimSlots :
           StartCancelRunning(t, ls, rs, cs)
    \/ \E t \in TxIds, rs \in ReadySlots, cs \in ClaimSlots :
          StartTerminalDelete(t, rs, cs)
    \/ \E t \in TxIds, srcQ \in Queues, dstQ \in Queues,
         srcP \in Priorities, dstP \in Priorities, rs \in ReadySlots :
          StartBatchReadyLaneMove(t, srcQ, srcP, dstQ, dstP, rs)
    \/ \E t \in TxIds, q \in Queues, p \in Priorities, rs \in ReadySlots :
          StartMaintenanceCursorAdvance(t, q, p, rs)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

Spec == ConfigOK /\ Init /\ [][Next]_vars

\* ---- Sanity check for the deadlock detector ----
\*
\* A deliberately cycle-creating pair of plans. If TLC does NOT flag
\* NoDeadlock on SpecDeadlockDemo, the deadlock detector is broken.
\* This is a harness for the checker, not a model of any real path.

CycleAPlan ==
    << Step(LeaseRingStateResource, ModeExclusive),
       Step(QueueRingStateResource, ModeExclusive) >>

CycleBPlan ==
    << Step(QueueRingStateResource, ModeExclusive),
       Step(LeaseRingStateResource, ModeExclusive) >>

StartCycleA(t) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CycleAPlan]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

StartCycleB(t) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CycleBPlan]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextDeadlockDemo ==
    \/ \E t \in TxIds : StartCycleA(t)
    \/ \E t \in TxIds : StartCycleB(t)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecDeadlockDemo == Init /\ [][NextDeadlockDemo]_vars

\* ---- Historical striped-claim deadlock harness ----
\*
\* This models the pre-fix shape where a single logical queue claim
\* transaction could walk multiple physical stripes in an order opposite
\* to the producer's stable grouped enqueue order. TLC is expected to
\* flag NoDeadlock on this spec. If it passes, this harness no longer
\* describes the bug class we fixed.

StartOldClaimTwoStripeReceipts(t, p, readySlot, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ p \in Priorities
    /\ readySlot \in ReadySlots
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] =
        OldClaimTwoStripeReceiptsPlan(p, readySlot, claimSlot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextOldStripedClaimDeadlockDemo ==
    \/ \E t \in TxIds, p \in Priorities : StartEnqueueTwoStripe(t, p)
    \/ \E t \in TxIds, p \in Priorities, rs \in ReadySlots,
         cs \in ClaimSlots :
          StartOldClaimTwoStripeReceipts(t, p, rs, cs)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecOldStripedClaimDeadlockDemo ==
    ConfigOK /\ Init /\ [][NextOldStripedClaimDeadlockDemo]_vars

\* ---- Historical lane-provisioning deadlock harness ----
\*
\* A 0.6.15 QueueStorage process and a SQL producer that are both first to
\* use a lane: QueueStorage holds the provisioning lock and waits for the
\* SQL producer's uncommitted `queue_lanes` row, while the SQL producer
\* holds that row and waits for the provisioning lock. TLC is expected to
\* flag NoDeadlock on this spec.

StartOldQueueStorageProvisionLane(t, q, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = OldQueueStorageProvisionLanePlan(q, p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextOldLaneProvisioningDeadlockDemo ==
    \/ \E t \in TxIds, q \in Queues, p \in Priorities : StartProvisionLane(t, q, p)
    \/ \E t \in TxIds, q \in Queues, p \in Priorities :
          StartOldQueueStorageProvisionLane(t, q, p)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecOldLaneProvisioningDeadlockDemo ==
    ConfigOK /\ Init /\ [][NextOldLaneProvisioningDeadlockDemo]_vars

\* ---- Historical re-enqueue unique-claim deadlock harness ----
\*
\* A 0.6.18 re-enqueue holds the lane and waits for a fresh enqueue's
\* uncommitted claim of the same key, while the fresh enqueue holds the
\* claim and waits for the lane. TLC is expected to flag NoDeadlock on this
\* spec. It is also the mixed-version shape while 0.6.18 and later runtimes
\* share a database.

StartOldReEnqueueUnique(t, u, q, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ u \in UniqueKeys
    /\ q \in Queues
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = OldReEnqueueUniquePlan(u, q, p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextOldReEnqueueUniqueDeadlockDemo ==
    \/ \E t \in TxIds, u \in UniqueKeys, q \in Queues, p \in Priorities :
          StartUniqueEnqueue(t, u, q, p)
    \/ \E t \in TxIds, u \in UniqueKeys, q \in Queues, p \in Priorities :
          StartOldReEnqueueUnique(t, u, q, p)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecOldReEnqueueUniqueDeadlockDemo ==
    ConfigOK /\ Init /\ [][NextOldReEnqueueUniqueDeadlockDemo]_vars

\* ---- Historical row-order claim-pass deadlock harness ----
\*
\* A 0.6.18 bulk cancel releases key 2 and waits on key 1, whose row a
\* fresh enqueue of both keys holds; the enqueue then waits on the
\* uncommitted release of key 2. TLC is expected to flag NoDeadlock.

StartOldRowOrderClaimPass(t) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = OldRowOrderClaimPassPlan]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextOldRowOrderClaimPassDeadlockDemo ==
    \/ \E t \in TxIds, p \in Priorities : StartUniqueBatchEnqueue(t, p)
    \/ \E t \in TxIds : StartOldRowOrderClaimPass(t)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecOldRowOrderClaimPassDeadlockDemo ==
    ConfigOK /\ Init /\ [][NextOldRowOrderClaimPassDeadlockDemo]_vars

\* ---- Completion follow-up deadlock harness (known gap) ----
\*
\* Two parents, holding keys 1 and 2, each complete with a mask that releases
\* on completion and register a uniquely keyed follow-up for the other's key.
\* Each releases its own key, then waits to claim the key the other has just
\* released and not committed. TLC is expected to flag NoDeadlock: the
\* key-order rule covers one claim pass, and a transition with follow-ups
\* takes two.

StartCompleteWithFollowUp(t, a, b, q, p) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ a \in UniqueKeys
    /\ b \in UniqueKeys
    /\ a # b
    /\ q \in Queues
    /\ p \in Priorities
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CompleteWithFollowUpPlan(a, b, q, p)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextCompletionFollowUpDeadlockDemo ==
    \/ \E t \in TxIds, a, b \in UniqueKeys, q \in Queues, p \in Priorities :
          StartCompleteWithFollowUp(t, a, b, q, p)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecCompletionFollowUpDeadlockDemo ==
    ConfigOK /\ Init /\ [][NextCompletionFollowUpDeadlockDemo]_vars

\* ---- Historical claim-prune slot-row deadlock harness ----
\*
\* 0.6.16 claim prune locked the slot's `claim_ring_slots` row before the
\* slot's partitions. Rescue holds the partitions and waits for that row
\* while prune holds the row and waits for the partitions. TLC is expected
\* to flag NoDeadlock on this spec.

StartOldPruneClaims(t, slot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ slot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = OldPruneClaimsPlan(slot)]
    /\ txNextStep' = [txNextStep EXCEPT ![t] = 1]
    /\ UNCHANGED heldLocks

NextOldClaimPruneSlotRowDeadlockDemo ==
    \/ \E t \in TxIds, cs \in ClaimSlots : StartRescueReceipts(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartOldPruneClaims(t, cs)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

SpecOldClaimPruneSlotRowDeadlockDemo ==
    ConfigOK /\ Init /\ [][NextOldClaimPruneSlotRowDeadlockDemo]_vars

\* ---- Invariants ----

TypeOK ==
    /\ TxIds # {}
    /\ Queues # {}
    /\ StripeA \in Queues
    /\ StripeB \in Queues
    /\ StripeA # StripeB
    /\ Priorities # {}
    /\ LeaseSlots # {}
    /\ ReadySlots # {}
    /\ ClaimSlots # {}
    /\ heldLocks \in [Resources -> SUBSET (TxIds \X Modes)]
    /\ txState \in [TxIds -> {"idle", "running", "committed"}]
    /\ txNextStep \in [TxIds -> Nat]

\* No two incompatible locks on the same resource.
LockCompatibility ==
    \A r \in Resources :
        \A lh1, lh2 \in heldLocks[r] :
            lh1 # lh2 =>
                (lh1[1] = lh2[1] \/ Compatible(lh1[2], lh2[2]))

\* Only running txs hold locks.
HeldOnlyByRunningTxs ==
    \A r \in Resources :
        \A lh \in heldLocks[r] :
            txState[lh[1]] = "running"

\* Is tx t currently blocked on its next plan step?
IsBlocked(t) ==
    /\ txState[t] = "running"
    /\ txNextStep[t] > 0
    /\ txNextStep[t] <= Len(txPlan[t])
    /\ LET step == txPlan[t][txNextStep[t]]
       IN Blocked(t, step.res, step.mode)

\* Direct waits-for: tx t waits for tx u.
WaitsFor(t, u) ==
    /\ t # u
    /\ IsBlocked(t)
    /\ LET step == txPlan[t][txNextStep[t]]
       IN \E m \in Modes :
            /\ <<u, m>> \in heldLocks[step.res]
            /\ ~ Compatible(m, step.mode)

\* Waits-for reachability, bounded. For N = Cardinality(TxIds) workers
\* we only need paths up to length N.
RECURSIVE WaitsForPath(_, _, _)
WaitsForPath(t, u, k) ==
    IF k = 0 THEN FALSE
    ELSE
        \/ WaitsFor(t, u)
        \/ \E v \in TxIds : WaitsFor(t, v) /\ WaitsForPath(v, u, k - 1)

NoDeadlock ==
    \A t \in TxIds :
        ~ WaitsForPath(t, t, Cardinality(TxIds))

\* All txs can eventually unblock (liveness-adjacent safety check): no
\* state where every running tx is blocked.
NoGlobalStall ==
    \/ \A t \in TxIds : txState[t] # "running"
    \/ \E t \in TxIds : txState[t] = "running" /\ ~ IsBlocked(t)

=============================================================================

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
\*   - LOCK TABLE ACCESS EXCLUSIVE on the partition child (so prune
\*     waits for in-flight claim/complete writes to commit before
\*     truncating).
\* Note: the claim CTE reads lease_ring_state with a plain SELECT, NOT
\* a FOR SHARE. The conflict detection between claim and rotate is the
\* rotator's CAS UPDATE (`WHERE current_slot=$ AND generation=$`) plus
\* the rotator's empty-partition busy-check, not a row-level lock.
\* The plans below treat the ring-state read as a no-lock step (it is
\* simply elided from the plan).
\*
\* ADR-023 extends the locked resources with the claim ring
\* (claim_ring_state, claim_ring_slots, lease_claims child partitions,
\* lease_claim_closures child partitions) and the parallel rotate/prune
\* plans for the claim ring. The claim path takes a RowExclusive on
\* the lease_claims child partition (receipts mode) or the leases child
\* partition (legacy mode). Closure writes on terminal transitions take
\* a RowExclusive on the lease_claim_closures child partition matching
\* the originating claim.
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
\* - Postgres's lock_timeout or deadlock detector abort choice — we
\*   flag cycles as safety violations so the spec fails fast rather
\*   than modelling the race-to-abort
\* - implicit table-level locks beyond what is explicitly named in each
\*   plan; we treat each named resource as the lock unit

CONSTANTS TxIds,
          Queues,
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
LaneResource(q, p) == [k |-> "queue_lane", q |-> q, p |-> p]
LeaseRingStateResource == [k |-> "lease_ring_state"]
LeaseRingSlotResource(s) == [k |-> "lease_ring_slot", s |-> s]
LeaseChildResource(s) == [k |-> "lease_child", s |-> s]
QueueRingStateResource == [k |-> "queue_ring_state"]
QueueRingSlotResource(s) == [k |-> "queue_ring_slot", s |-> s]
ReadyChildResource(s) == [k |-> "ready_child", s |-> s]
DoneChildResource(s) == [k |-> "done_child", s |-> s]
LeasesParentResource == [k |-> "leases_parent"]
ClaimRingStateResource == [k |-> "claim_ring_state"]
ClaimRingSlotResource(s) == [k |-> "claim_ring_slot", s |-> s]
ClaimChildResource(s) == [k |-> "claim_child", s |-> s]
ClosureChildResource(s) == [k |-> "closure_child", s |-> s]

LaneResources == { LaneResource(q, p) : q \in Queues, p \in Priorities }
LeaseRingSlotResources == { LeaseRingSlotResource(s) : s \in LeaseSlots }
LeaseChildResources == { LeaseChildResource(s) : s \in LeaseSlots }
QueueRingSlotResources == { QueueRingSlotResource(s) : s \in ReadySlots }
ReadyChildResources == { ReadyChildResource(s) : s \in ReadySlots }
DoneChildResources == { DoneChildResource(s) : s \in ReadySlots }
ClaimRingSlotResources == { ClaimRingSlotResource(s) : s \in ClaimSlots }
ClaimChildResources == { ClaimChildResource(s) : s \in ClaimSlots }
ClosureChildResources == { ClosureChildResource(s) : s \in ClaimSlots }

Resources ==
    LaneResources \cup
    {LeaseRingStateResource} \cup
    LeaseRingSlotResources \cup
    LeaseChildResources \cup
    {QueueRingStateResource} \cup
    QueueRingSlotResources \cup
    ReadyChildResources \cup
    DoneChildResources \cup
    {LeasesParentResource} \cup
    {ClaimRingStateResource} \cup
    ClaimRingSlotResources \cup
    ClaimChildResources \cup
    ClosureChildResources

\* A plan step is [res, mode].
Step(res, mode) == [res |-> res, mode |-> mode]

\* Transaction kind plans. Each mirrors the actual SQL in
\* awa-model/src/queue_storage.rs as noted inline.

\* The claim CTE
\* (`claim_runtime_batch_with_aging_for_instance` in queue_storage.rs)
\*   FOR UPDATE OF queue_claim_heads SKIP LOCKED (per-(queue,priority) row)
\*   plain SELECT of lease_ring_state and claim_ring_state — no
\*     FOR SHARE / FOR UPDATE; serialisation against rotate is via
\*     the rotator's CAS UPDATE on (current_slot, generation), not a
\*     row lock here, so these reads are NOT modelled as plan steps.
\*   plain SELECT of ready_entries_<slot> (implicit AccessShare on child)
\*   INSERT INTO lease_claims_<claim_slot> (receipts mode) OR
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
       Step(ClaimChildResource(claimSlot), ModeShared) >>

ClaimLegacyPlan(q, p, readySlot, leaseSlot) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeShared) >>

\* complete_runtime_batch receipt branch (ADR-023)
\*   No queue_lanes lock (completion does not gate on a lane row)
\*   INSERT INTO lease_claim_closures (routed to the originating claim_slot)
\*   INSERT INTO done_entries / dlq_entries / deferred_jobs
CompletePlan(claimSlot, readySlot) ==
    << Step(ClosureChildResource(claimSlot), ModeShared),
       Step(DoneChildResource(readySlot), ModeShared) >>

\* close_receipt_tx (queue_storage.rs:5450, called from cancel_job_tx)
\*   WITH locked_claim AS (SELECT ... FROM lease_claims FOR UPDATE)
\*   INSERT INTO lease_claim_closures ... ON CONFLICT DO NOTHING
CloseReceiptPlan(claimSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared) >>

\* rescue_stale_receipt_claims_tx (queue_storage.rs:6672)
\*   SELECT ... FROM lease_claims claims LEFT JOIN attempt_state ...
\*     WHERE NOT EXISTS (closures) AND NOT EXISTS (leases)
\*     FOR UPDATE OF claims SKIP LOCKED
\*   INSERT INTO lease_claim_closures ... ON CONFLICT DO NOTHING
\* The leases anti-join takes AccessShare on the leases
\* parent so rescue can race against prune_oldest_leases.
RescueReceiptsPlan(claimSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(LeasesParentResource, ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared) >>

\* ensure_running_leases_from_receipts_tx (queue_storage.rs:6102)
\*   CTE claim_refs: SELECT ... FROM lease_claims FOR UPDATE OF claims
\*   INSERT INTO leases ...
\*   UPDATE lease_claims SET materialized_at = ...
EnsureRunningPlan(claimSlot, leaseSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeShared) >>

\* cancel_job_tx receipt-only branch (queue_storage.rs:5621)
\*   SELECT ... FROM lease_claims FOR UPDATE OF claims SKIP LOCKED
\*   insert_done_rows_tx → INSERT INTO done_entries
\*   INSERT INTO lease_claim_closures
\*   defensive DELETE FROM leases (sweeps any concurrent materialization)
\*   pg_notify('awa:cancel', ...)
CancelReceiptOnlyPlan(claimSlot, readySlot, leaseSlot) ==
    << Step(ClaimChildResource(claimSlot), ModeShared),
       Step(DoneChildResource(readySlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeShared) >>

\* cancel_job_tx running-lease branch (queue_storage.rs ~:5581)
\*   DELETE FROM leases ... RETURNING
\*   insert_done_rows_tx → INSERT INTO done_entries
\*   close_receipt_tx (claim child + closure child)
\*   pg_notify
CancelRunningPlan(leaseSlot, readySlot, claimSlot) ==
    << Step(LeaseChildResource(leaseSlot), ModeShared),
       Step(DoneChildResource(readySlot), ModeShared),
       Step(ClaimChildResource(claimSlot), ModeShared),
       Step(ClosureChildResource(claimSlot), ModeShared) >>

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
\*   LOCK TABLE lease_child[slot] ACCESS EXCLUSIVE
PruneLeasesPlan(slot) ==
    << Step(LeaseRingStateResource, ModeExclusive),
       Step(LeaseRingSlotResource(slot), ModeExclusive),
       Step(LeaseChildResource(slot), ModeExclusive) >>

\* rotate_ready similar to rotate_leases (approximated)
RotateReadyPlan(nextSlot) ==
    << Step(QueueRingStateResource, ModeExclusive),
       Step(ReadyChildResource(nextSlot), ModeShared) >>

\* prune_oldest
\*   SELECT ... FROM queue_ring_state FOR UPDATE
\*   SELECT ... FROM queue_ring_slots FOR UPDATE
\*   LOCK TABLE ready_child[slot], done_child[slot] ACCESS EXCLUSIVE
\*   SELECT count FROM leases WHERE ready_slot = $1 (AccessShare on leases parent)
PruneReadyPlan(slot) ==
    << Step(QueueRingStateResource, ModeExclusive),
       Step(QueueRingSlotResource(slot), ModeExclusive),
       Step(ReadyChildResource(slot), ModeExclusive),
       Step(DoneChildResource(slot), ModeExclusive),
       Step(LeasesParentResource, ModeShared) >>

\* rotate_claims (ADR-023)
\*   SELECT ... FROM claim_ring_state FOR UPDATE
\*   SELECT count(*) FROM claim_child[next_slot]
\*   SELECT count(*) FROM closure_child[next_slot]
\*   UPDATE claim_ring_state
RotateClaimsPlan(nextSlot) ==
    << Step(ClaimRingStateResource, ModeExclusive),
       Step(ClaimChildResource(nextSlot), ModeShared),
       Step(ClosureChildResource(nextSlot), ModeShared) >>

\* prune_oldest_claims (ADR-023)
\*   SELECT ... FROM claim_ring_state FOR UPDATE
\*   SELECT ... FROM claim_ring_slots[slot] FOR UPDATE
\*   LOCK TABLE claim_child[slot] ACCESS EXCLUSIVE
\*   LOCK TABLE closure_child[slot] ACCESS EXCLUSIVE
\*   rescue-before-truncate: close any still-open claims via the
\*     existing receipt-rescue path (modelled at the data-spec level, no
\*     extra locks here because the rescue uses the same child AccessExclusive)
\*   TRUNCATE claim_child[slot] + closure_child[slot]
PruneClaimsPlan(slot) ==
    << Step(ClaimRingStateResource, ModeExclusive),
       Step(ClaimRingSlotResource(slot), ModeExclusive),
       Step(ClaimChildResource(slot), ModeExclusive),
       Step(ClosureChildResource(slot), ModeExclusive) >>

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

StartComplete(t, claimSlot, readySlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ claimSlot \in ClaimSlots
    /\ readySlot \in ReadySlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = CompletePlan(claimSlot, readySlot)]
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
    \/ \E t \in TxIds, cs \in ClaimSlots : StartCloseReceipt(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartRescueReceipts(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots, ls \in LeaseSlots :
          StartEnsureRunning(t, cs, ls)
    \/ \E t \in TxIds, cs \in ClaimSlots, rs \in ReadySlots,
         ls \in LeaseSlots :
          StartCancelReceiptOnly(t, cs, rs, ls)
    \/ \E t \in TxIds, ls \in LeaseSlots, rs \in ReadySlots,
         cs \in ClaimSlots :
          StartCancelRunning(t, ls, rs, cs)
    \/ \E t \in TxIds : AcquireNext(t)
    \/ \E t \in TxIds : Commit(t)
    \/ \E t \in TxIds : Recycle(t)
    \/ Stutter

Spec == Init /\ [][Next]_vars

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

\* ---- Invariants ----

TypeOK ==
    /\ TxIds # {}
    /\ Queues # {}
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

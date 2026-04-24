---- MODULE AwaStorageLockOrder ----
EXTENDS TLC, Naturals, FiniteSets, Sequences

\* Lock-ordering protocol spec for the queue-storage engine.
\*
\* The data-level specs (AwaSegmentedStorage, AwaSegmentedStorageRaces)
\* deliberately abstract away from Postgres locks. After reviewing the
\* Rust SQL, the claim/rotate/prune race turned out to be mitigated by
\* FOR SHARE on lease_ring_state plus ACCESS EXCLUSIVE on lease
\* partitions. This spec models those locks directly so a future
\* refactor that weakens or re-orders them would trip an invariant.
\*
\* ADR-023 extends the locked resources with the claim ring
\* (claim_ring_state, claim_ring_slots, lease_claims child partitions,
\* lease_claim_closures child partitions) and the parallel rotate/prune
\* plans for the claim ring. The claim path now also takes a FOR SHARE
\* on claim_ring_state and a RowExclusive on the lease_claims child
\* partition. Closure writes on terminal transitions take a RowExclusive
\* on the lease_claim_closures child partition that was selected when
\* the claim was first made.
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

\* Lock modes. We collapse the Postgres matrix to S/X because the only
\* nontrivial compatibility in the claim/rotate/prune paths is FOR
\* SHARE vs FOR UPDATE; everything else is X.
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

\* claim_ready_runtime
\*   SELECT ... FROM queue_lanes FOR UPDATE (per-priority row)
\*   CTE lease_ring: SELECT ... FROM lease_ring_state FOR SHARE
\*   CTE claim_ring: SELECT ... FROM claim_ring_state FOR SHARE (ADR-023)
\*   SELECT ... FROM ready_entries_SLOT ... (implicit AccessShare on child)
\*   INSERT INTO leases (routed to current lease partition)
\*   INSERT INTO lease_claims (routed to current claim partition, ADR-023)
\* We parameterise the tx by the queue/priority, the ready slot it
\* reads from, and the lease/claim slots the rings point at when it reads.
ClaimPlan(q, p, readySlot, leaseSlot, claimSlot) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(LeaseRingStateResource, ModeShared),
       Step(ClaimRingStateResource, ModeShared),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeExclusive),
       Step(ClaimChildResource(claimSlot), ModeExclusive) >>

\* complete_runtime_batch receipt branch (ADR-023)
\*   No queue_lanes lock (completion does not gate on a lane row)
\*   INSERT INTO lease_claim_closures (routed to the originating claim_slot)
\*   INSERT INTO done_entries / dlq_entries / deferred_entries
\* We parameterise the tx by the claim slot (carried on the completed
\* claim) and the ready slot of the terminal row.
CompletePlan(claimSlot, readySlot) ==
    << Step(ClosureChildResource(claimSlot), ModeExclusive),
       Step(DoneChildResource(readySlot), ModeExclusive) >>

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

\* Start a Claim transaction. Different txs can pick different slots,
\* exercising the protocol across interleavings.
StartClaim(t, q, p, readySlot, leaseSlot, claimSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ readySlot \in ReadySlots
    /\ leaseSlot \in LeaseSlots
    /\ claimSlot \in ClaimSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = ClaimPlan(q, p, readySlot, leaseSlot, claimSlot)]
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
         rs \in ReadySlots, ls \in LeaseSlots, cs \in ClaimSlots :
          StartClaim(t, q, p, rs, ls, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots, rs \in ReadySlots :
          StartComplete(t, cs, rs)
    \/ \E t \in TxIds, ls \in LeaseSlots : StartRotateLeases(t, ls)
    \/ \E t \in TxIds, ls \in LeaseSlots : StartPruneLeases(t, ls)
    \/ \E t \in TxIds, rs \in ReadySlots : StartRotateReady(t, rs)
    \/ \E t \in TxIds, rs \in ReadySlots : StartPruneReady(t, rs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartRotateClaims(t, cs)
    \/ \E t \in TxIds, cs \in ClaimSlots : StartPruneClaims(t, cs)
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

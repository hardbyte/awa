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
          ReadySlots

\* Lock modes. We collapse the Postgres matrix to S/X because the only
\* nontrivial compatibility in the claim/rotate/prune paths is FOR
\* SHARE vs FOR UPDATE; everything else is X.
ModeShared == "S"
ModeExclusive == "X"
Modes == {ModeShared, ModeExclusive}

Compatible(held, wanted) == held = ModeShared /\ wanted = ModeShared

\* Resource identities. We use strings for readability in TLC traces.
QueueLaneRes(q, p) == "queue_lane"
LeaseRingState == "lease_ring_state"
LeaseRingSlot(s) == "lease_ring_slot"
LeaseChild(s) == "lease_child"
QueueRingState == "queue_ring_state"
QueueRingSlot(s) == "queue_ring_slot"
ReadyChild(s) == "ready_child"
DoneChild(s) == "done_child"
LeasesParent == "leases_parent"

\* Since identities collapse to strings when TLC renders them, we make
\* the actual identities concrete by tagging them with the parameter.
\* TLA+ doesn't have native nominal strings for this, so we model each
\* parameterised resource as a record. Use records for uniqueness.
LaneResource(q, p) == [k |-> "queue_lane", q |-> q, p |-> p]
LeaseRingStateResource == [k |-> "lease_ring_state"]
LeaseRingSlotResource(s) == [k |-> "lease_ring_slot", s |-> s]
LeaseChildResource(s) == [k |-> "lease_child", s |-> s]
QueueRingStateResource == [k |-> "queue_ring_state"]
QueueRingSlotResource(s) == [k |-> "queue_ring_slot", s |-> s]
ReadyChildResource(s) == [k |-> "ready_child", s |-> s]
DoneChildResource(s) == [k |-> "done_child", s |-> s]
LeasesParentResource == [k |-> "leases_parent"]

LaneResources == { LaneResource(q, p) : q \in Queues, p \in Priorities }
LeaseRingSlotResources == { LeaseRingSlotResource(s) : s \in LeaseSlots }
LeaseChildResources == { LeaseChildResource(s) : s \in LeaseSlots }
QueueRingSlotResources == { QueueRingSlotResource(s) : s \in ReadySlots }
ReadyChildResources == { ReadyChildResource(s) : s \in ReadySlots }
DoneChildResources == { DoneChildResource(s) : s \in ReadySlots }

Resources ==
    LaneResources \cup
    {LeaseRingStateResource} \cup
    LeaseRingSlotResources \cup
    LeaseChildResources \cup
    {QueueRingStateResource} \cup
    QueueRingSlotResources \cup
    ReadyChildResources \cup
    DoneChildResources \cup
    {LeasesParentResource}

\* A plan step is [res, mode].
Step(res, mode) == [res |-> res, mode |-> mode]

\* Transaction kind plans. Each mirrors the actual SQL in
\* awa-model/src/queue_storage.rs as noted inline.

\* claim_ready_runtime at queue_storage.rs:1647
\*   SELECT ... FROM queue_lanes FOR UPDATE (per-priority row)
\*   CTE lease_ring: SELECT ... FROM lease_ring_state FOR SHARE
\*   SELECT ... FROM ready_entries_SLOT ... (implicit AccessShare on child)
\*   INSERT INTO leases (routed to current lease partition)
\* We parameterise the tx by the queue/priority, the ready slot it
\* reads from, and the lease slot the ring points at when it reads.
ClaimPlan(q, p, readySlot, leaseSlot) ==
    << Step(LaneResource(q, p), ModeExclusive),
       Step(LeaseRingStateResource, ModeShared),
       Step(ReadyChildResource(readySlot), ModeShared),
       Step(LeaseChildResource(leaseSlot), ModeExclusive) >>

\* rotate_leases at queue_storage.rs:6184
\*   SELECT ... FROM lease_ring_state FOR UPDATE
\*   SELECT count(*) FROM lease_child[next_slot] (AccessShare on child)
\*   UPDATE lease_ring_state / lease_ring_slots
RotateLeasesPlan(nextSlot) ==
    << Step(LeaseRingStateResource, ModeExclusive),
       Step(LeaseChildResource(nextSlot), ModeShared) >>

\* prune_oldest_leases at queue_storage.rs:6384
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

\* prune_oldest at queue_storage.rs:6252
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
StartClaim(t, q, p, readySlot, leaseSlot) ==
    /\ t \in TxIds
    /\ txState[t] = "idle"
    /\ q \in Queues
    /\ p \in Priorities
    /\ readySlot \in ReadySlots
    /\ leaseSlot \in LeaseSlots
    /\ txState' = [txState EXCEPT ![t] = "running"]
    /\ txPlan' = [txPlan EXCEPT ![t] = ClaimPlan(q, p, readySlot, leaseSlot)]
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
         rs \in ReadySlots, ls \in LeaseSlots :
          StartClaim(t, q, p, rs, ls)
    \/ \E t \in TxIds, ls \in LeaseSlots : StartRotateLeases(t, ls)
    \/ \E t \in TxIds, ls \in LeaseSlots : StartPruneLeases(t, ls)
    \/ \E t \in TxIds, rs \in ReadySlots : StartRotateReady(t, rs)
    \/ \E t \in TxIds, rs \in ReadySlots : StartPruneReady(t, rs)
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

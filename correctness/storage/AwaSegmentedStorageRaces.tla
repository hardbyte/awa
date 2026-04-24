---- MODULE AwaSegmentedStorageRaces ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused race spec: does the claim-path read of lease_ring_state and
\* claim_ring_state race with concurrent RotateLeaseSegments /
\* PruneLeaseSegment and RotateClaimSegments / PruneClaimSegment?
\*
\* The base AwaSegmentedStorage spec models Claim as a single atomic
\* action: in one step it reads leaseSegmentCursor and claimSegmentCursor,
\* inserts the lease row and the claim-receipt row tagged with those
\* cursor values, and advances laneState. That hides the real code path:
\*
\*   1. claim_ready_runtime locks queue_lanes via FOR UPDATE
\*   2. it reads lease_ring_state.current_slot and claim_ring_state.current_slot
\*   3. it inserts into {schema}.leases tagged with lease_slot AND
\*      {schema}.lease_claims tagged with claim_slot
\*
\* If rotate_leases or rotate_claims runs between step 2 and step 3, the
\* insert lands in the now-sealed slot. If prune runs next (its
\* precondition requires the slot to have no live row, which is true at
\* that instant because the pending insert has not committed), the slot
\* transitions to "pruned" before the insert lands. The insert then
\* produces a row in a "pruned" segment.
\*
\* To expose this, BeginClaim and CommitClaim are separate actions with a
\* per-worker claimIntent snapshot in between. Rotate and Prune actions
\* are enabled between BeginClaim and CommitClaim.
\*
\* If TLC finds a trace violating PrunedLeaseSegmentsAreEmpty or
\* PrunedClaimSegmentsAreEmpty, the race is real at the spec's
\* abstraction level. If not, the preconditions on CommitClaim (which
\* must re-check that the target segments are still usable) are strong
\* enough to prevent it — and we've effectively specified what the Rust
\* claim path needs to enforce.
\*
\* This spec is deliberately minimal: no waiting, no DLQ, no deferred. It
\* is solely about claim vs rotate/prune on the lease and claim families.
\* Read it as a refinement of AwaSegmentedStorage's Claim /
\* Rotate{Lease,Claim} / Prune{Lease,Claim} fragment.

CONSTANTS Jobs,
          Workers,
          ReadySegmentCount,
          LeaseSegmentCount,
          ClaimSegmentCount,
          MaxRunLease,
          MaxAppendSeq

ReadySegments == 1..ReadySegmentCount
LeaseSegments == 1..LeaseSegmentCount
ClaimSegments == 1..ClaimSegmentCount
SegmentStates == {"open", "sealed", "pruned"}

NoWorker == "none"
NoJob == "nojob"
NoReadySegment == 0
NoLeaseSegment == 0
NoClaimSegment == 0
NoLaneSeq == 0

VARIABLES readyEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          laneSeq,
          readySegmentOf,
          leaseSegmentOf,
          claimSegmentOf,
          claimOpen,
          readySegments,
          leaseSegments,
          claimSegments,
          readySegmentCursor,
          leaseSegmentCursor,
          claimSegmentCursor,
          laneState,
          claimIntent

vars == <<readyEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          laneSeq,
          readySegmentOf,
          leaseSegmentOf,
          claimSegmentOf,
          claimOpen,
          readySegments,
          leaseSegments,
          claimSegments,
          readySegmentCursor,
          leaseSegmentCursor,
          claimSegmentCursor,
          laneState,
          claimIntent>>

NextReadySegment(s) == IF s = ReadySegmentCount THEN 1 ELSE s + 1
NextLeaseSegment(s) == IF s = LeaseSegmentCount THEN 1 ELSE s + 1
NextClaimSegment(s) == IF s = ClaimSegmentCount THEN 1 ELSE s + 1

CurrentReady == (readyEntries \ activeLeases)

JobsInReadySegment(seg) == {j \in Jobs : readySegmentOf[j] = seg}
JobsInLeaseSegment(seg) == {j \in Jobs : leaseSegmentOf[j] = seg}
JobsInClaimSegment(seg) == {j \in Jobs : claimSegmentOf[j] = seg}

InitSegments(Range) == [s \in Range |-> IF s = 1 THEN "open" ELSE "pruned"]

NoIntent == [job |-> NoJob, leaseSeg |-> NoLeaseSegment, claimSeg |-> NoClaimSegment]

Init ==
    /\ readyEntries = {}
    /\ activeLeases = {}
    /\ leaseOwner = [j \in Jobs |-> NoWorker]
    /\ runLease = [j \in Jobs |-> 0]
    /\ taskLease = [w \in Workers |-> [j \in Jobs |-> 0]]
    /\ laneSeq = [j \in Jobs |-> NoLaneSeq]
    /\ readySegmentOf = [j \in Jobs |-> NoReadySegment]
    /\ leaseSegmentOf = [j \in Jobs |-> NoLeaseSegment]
    /\ claimSegmentOf = [j \in Jobs |-> NoClaimSegment]
    /\ claimOpen = {}
    /\ readySegments = InitSegments(ReadySegments)
    /\ leaseSegments = InitSegments(LeaseSegments)
    /\ claimSegments = InitSegments(ClaimSegments)
    /\ readySegmentCursor = 1
    /\ leaseSegmentCursor = 1
    /\ claimSegmentCursor = 1
    /\ laneState = [appendSeq |-> 1, claimSeq |-> 1]
    /\ claimIntent = [w \in Workers |-> NoIntent]

EnqueueReady(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ readyEntries' = readyEntries \cup {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneState' = [laneState EXCEPT !.appendSeq = laneState.appendSeq + 1]
    /\ UNCHANGED <<activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   leaseSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   readySegments,
                   leaseSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimSegmentCursor,
                   claimIntent>>

\* Phase 1 of claim: the worker has acquired the queue_lanes row lock
\* (modelled by the "no other worker has a pending intent on the same
\* lane" precondition) and has snapshotted the lease_ring_state AND
\* claim_ring_state cursors. Between BeginClaim and CommitClaim,
\* RotateLeaseSegments / PruneLeaseSegment / RotateClaimSegments /
\* PruneClaimSegment may fire.
BeginClaim(w, j) ==
    /\ w \in Workers
    /\ claimIntent[w] = NoIntent
    /\ ~ (\E other \in Workers : other # w /\ claimIntent[other].job = j)
    /\ j \in CurrentReady
    /\ laneSeq[j] = laneState.claimSeq
    /\ runLease[j] < MaxRunLease
    /\ leaseSegments[leaseSegmentCursor] = "open"
    /\ claimSegments[claimSegmentCursor] = "open"
    /\ claimIntent' = [claimIntent EXCEPT ![w] =
                          [job |-> j,
                           leaseSeg |-> leaseSegmentCursor,
                           claimSeg |-> claimSegmentCursor]]
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   readySegments,
                   leaseSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimSegmentCursor,
                   laneState>>

\* Phase 2: commit the claim using the snapshotted lease and claim
\* segments. The critical modelling choice here is what the action
\* re-checks on those segments at commit time.
\*
\* If CommitClaim blindly uses claimIntent[w].leaseSeg and claimSeg, a
\* race is possible: either segment may have been rotated out and
\* pruned. The resulting lease or receipt row then sits in a "pruned"
\* segment, violating PrunedLeaseSegmentsAreEmpty /
\* PrunedClaimSegmentsAreEmpty.
\*
\* If we add a re-check at commit time (both segments still "open", or
\* at least not "pruned"), the race becomes unobservable and the
\* invariants hold. That extra precondition is what the Rust
\* claim_ready_runtime function must guarantee either by re-reading the
\* ring-state rows under the queue_lanes lock, by holding FOR SHARE on
\* both ring-state rows across the insert, or by some equivalent.
\*
\* We keep the "naive" commit here to force the race to be observable.
CommitClaim(w) ==
    /\ w \in Workers
    /\ claimIntent[w] # NoIntent
    /\ LET j == claimIntent[w].job
           leaseSeg == claimIntent[w].leaseSeg
           claimSeg == claimIntent[w].claimSeg
       IN
       /\ j \in CurrentReady
       /\ laneSeq[j] = laneState.claimSeq
       /\ activeLeases' = activeLeases \cup {j}
       /\ leaseOwner' = [leaseOwner EXCEPT ![j] = w]
       /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
       /\ taskLease' = [taskLease EXCEPT ![w][j] = runLease[j] + 1]
       /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = leaseSeg]
       /\ claimSegmentOf' = [claimSegmentOf EXCEPT ![j] = claimSeg]
       /\ claimOpen' = claimOpen \cup {j}
       /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
       /\ claimIntent' = [claimIntent EXCEPT ![w] = NoIntent]
       /\ UNCHANGED <<readyEntries,
                      laneSeq,
                      readySegmentOf,
                      readySegments,
                      leaseSegments,
                      claimSegments,
                      readySegmentCursor,
                      leaseSegmentCursor,
                      claimSegmentCursor>>

\* Safer variant: CommitClaimChecked aborts if either snapshotted
\* segment has rotated out of "open". Disabled by default — toggle in
\* the config by swapping CommitClaim for CommitClaimChecked in Next.
CommitClaimChecked(w) ==
    /\ w \in Workers
    /\ claimIntent[w] # NoIntent
    /\ LET j == claimIntent[w].job
           leaseSeg == claimIntent[w].leaseSeg
           claimSeg == claimIntent[w].claimSeg
       IN
       /\ j \in CurrentReady
       /\ laneSeq[j] = laneState.claimSeq
       /\ leaseSegments[leaseSeg] = "open"
       /\ claimSegments[claimSeg] = "open"
       /\ activeLeases' = activeLeases \cup {j}
       /\ leaseOwner' = [leaseOwner EXCEPT ![j] = w]
       /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
       /\ taskLease' = [taskLease EXCEPT ![w][j] = runLease[j] + 1]
       /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = leaseSeg]
       /\ claimSegmentOf' = [claimSegmentOf EXCEPT ![j] = claimSeg]
       /\ claimOpen' = claimOpen \cup {j}
       /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
       /\ claimIntent' = [claimIntent EXCEPT ![w] = NoIntent]
       /\ UNCHANGED <<readyEntries,
                      laneSeq,
                      readySegmentOf,
                      readySegments,
                      leaseSegments,
                      claimSegments,
                      readySegmentCursor,
                      leaseSegmentCursor,
                      claimSegmentCursor>>

\* CommitClaim can also abort — in reality the SQL tx could see a
\* serialization conflict and fail. We model that as a rollback that
\* discards the intent.
AbortClaim(w) ==
    /\ w \in Workers
    /\ claimIntent[w] # NoIntent
    /\ claimIntent' = [claimIntent EXCEPT ![w] = NoIntent]
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   readySegments,
                   leaseSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimSegmentCursor,
                   laneState>>

RotateLeaseSegments ==
    LET next == NextLeaseSegment(leaseSegmentCursor) IN
    /\ leaseSegments[leaseSegmentCursor] = "open"
    /\ leaseSegments[next] = "pruned"
    /\ leaseSegments' = [leaseSegments EXCEPT
                           ![leaseSegmentCursor] = "sealed",
                           ![next] = "open"]
    /\ leaseSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   readySegments,
                   claimSegments,
                   readySegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   claimIntent>>

PruneLeaseSegment(seg) ==
    /\ seg \in LeaseSegments
    /\ leaseSegments[seg] = "sealed"
    /\ \A j \in Jobs : leaseSegmentOf[j] = seg => j \notin activeLeases
    /\ leaseSegmentOf' = [j \in Jobs |-> IF leaseSegmentOf[j] = seg THEN NoLeaseSegment ELSE leaseSegmentOf[j]]
    /\ leaseSegments' = [leaseSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   readySegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   claimIntent>>

RotateClaimSegments ==
    LET next == NextClaimSegment(claimSegmentCursor) IN
    /\ claimSegments[claimSegmentCursor] = "open"
    /\ claimSegments[next] = "pruned"
    /\ claimSegments' = [claimSegments EXCEPT
                           ![claimSegmentCursor] = "sealed",
                           ![next] = "open"]
    /\ claimSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   laneState,
                   claimIntent>>

\* ADR-023 prune of the claim ring. Requires every claim in the target
\* partition to have already closed (in this minimal spec, no close
\* action is modelled, so prune only fires on partitions that never
\* received a claim — which is the race-exposing case: the claim is
\* about to land but hasn't committed yet, so claimOpen looks empty).
PruneClaimSegment(seg) ==
    /\ seg \in ClaimSegments
    /\ claimSegments[seg] = "sealed"
    /\ \A j \in Jobs : claimSegmentOf[j] = seg => j \notin claimOpen
    /\ claimSegmentOf' = [j \in Jobs |-> IF claimSegmentOf[j] = seg THEN NoClaimSegment ELSE claimSegmentOf[j]]
    /\ claimSegments' = [claimSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimOpen,
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   claimIntent>>

Stutter == /\ UNCHANGED vars

\* Race-exposing Next: uses the naive CommitClaim that doesn't re-check
\* the lease or claim segment state. If TLC finds
\* PrunedLeaseSegmentsAreEmpty or PrunedClaimSegmentsAreEmpty violated,
\* the race is real.
NextRace ==
    \/ \E j \in Jobs : EnqueueReady(j)
    \/ \E w \in Workers, j \in Jobs : BeginClaim(w, j)
    \/ \E w \in Workers : CommitClaim(w)
    \/ \E w \in Workers : AbortClaim(w)
    \/ RotateLeaseSegments
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ RotateClaimSegments
    \/ \E seg \in ClaimSegments : PruneClaimSegment(seg)
    \/ Stutter

\* Safe-commit Next: uses CommitClaimChecked which re-reads the lease
\* and claim segment states at commit time. Expect both pruned-empty
\* invariants to hold.
NextSafe ==
    \/ \E j \in Jobs : EnqueueReady(j)
    \/ \E w \in Workers, j \in Jobs : BeginClaim(w, j)
    \/ \E w \in Workers : CommitClaimChecked(w)
    \/ \E w \in Workers : AbortClaim(w)
    \/ RotateLeaseSegments
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ RotateClaimSegments
    \/ \E seg \in ClaimSegments : PruneClaimSegment(seg)
    \/ Stutter

\* Select one of NextRace / NextSafe via the config's SPECIFICATION line.
Spec == Init /\ [][NextRace]_vars
SpecSafe == Init /\ [][NextSafe]_vars

TypeOK ==
    /\ Jobs # {}
    /\ Workers # {}
    /\ ReadySegmentCount >= 1
    /\ LeaseSegmentCount >= 1
    /\ ClaimSegmentCount >= 1
    /\ MaxRunLease >= 1
    /\ MaxAppendSeq >= Cardinality(Jobs)
    /\ readyEntries \subseteq Jobs
    /\ activeLeases \subseteq Jobs
    /\ leaseOwner \in [Jobs -> Workers \cup {NoWorker}]
    /\ runLease \in [Jobs -> 0..MaxRunLease]
    /\ taskLease \in [Workers -> [Jobs -> 0..MaxRunLease]]
    /\ laneSeq \in [Jobs -> 0..MaxAppendSeq]
    /\ readySegmentOf \in [Jobs -> ReadySegments \cup {NoReadySegment}]
    /\ leaseSegmentOf \in [Jobs -> LeaseSegments \cup {NoLeaseSegment}]
    /\ claimSegmentOf \in [Jobs -> ClaimSegments \cup {NoClaimSegment}]
    /\ claimOpen \subseteq Jobs
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ leaseSegments \in [LeaseSegments -> SegmentStates]
    /\ claimSegments \in [ClaimSegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ leaseSegmentCursor \in LeaseSegments
    /\ claimSegmentCursor \in ClaimSegments
    /\ laneState.appendSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.claimSeq \in 1..(MaxAppendSeq + 1)
    /\ claimIntent \in [Workers -> [job: Jobs \cup {NoJob},
                                    leaseSeg: LeaseSegments \cup {NoLeaseSegment},
                                    claimSeg: ClaimSegments \cup {NoClaimSegment}]]

OneOpenLeaseSegment == Cardinality({s \in LeaseSegments : leaseSegments[s] = "open"}) = 1
OneOpenClaimSegment == Cardinality({s \in ClaimSegments : claimSegments[s] = "open"}) = 1
LeaseCursorIsOpen == leaseSegments[leaseSegmentCursor] = "open"
ClaimCursorIsOpen == claimSegments[claimSegmentCursor] = "open"

ActiveLeasesSubsetReadyEntries == activeLeases \subseteq readyEntries

\* Key race-sensitive invariant for the lease ring. If a lease lands in
\* a pruned segment, this trips and TLC emits a trace proving the race.
PrunedLeaseSegmentsAreEmpty ==
    \A j \in Jobs : leaseSegmentOf[j] # NoLeaseSegment =>
        leaseSegments[leaseSegmentOf[j]] # "pruned"

\* Same for the claim ring: a receipt landing in a pruned claim segment
\* is an ADR-023 correctness violation.
PrunedClaimSegmentsAreEmpty ==
    \A j \in Jobs : claimSegmentOf[j] # NoClaimSegment =>
        claimSegments[claimSegmentOf[j]] # "pruned"

\* Variant that additionally forbids sealed — a lease landing in a
\* sealed segment isn't a correctness bug per se (sealed segments can
\* still hold live leases until drain), but the race exposure is larger
\* if sealed is also a landing state we didn't intend.
ActiveLeasesInOpenSegmentsOnly ==
    \A j \in activeLeases : leaseSegments[leaseSegmentOf[j]] = "open"

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs : taskLease[w][j] <= runLease[j]

=============================================================================

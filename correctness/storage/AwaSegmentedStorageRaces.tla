---- MODULE AwaSegmentedStorageRaces ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused race spec: does the claim-path read of lease_ring_state race
\* with concurrent RotateLeaseSegments / PruneLeaseSegment?
\*
\* The base AwaSegmentedStorage spec models Claim as a single atomic
\* action: in one step it reads leaseSegmentCursor, inserts the lease row
\* tagged with that cursor value, and advances laneState. That hides the
\* real code path:
\*
\*   1. claim_ready_runtime (queue_storage.rs:1647) locks queue_lanes via
\*      FOR UPDATE for its queue+priority row
\*   2. it reads lease_ring_state.current_slot WITHOUT a lock
\*   3. it inserts into {schema}.leases tagged with that slot
\*
\* If rotate_leases (which does not touch queue_lanes) runs between
\* step 2 and step 3, the insert lands in the now-sealed slot. If prune
\* runs next (it requires the slot to have no live leases, which is true
\* at that instant because the pending insert has not committed), the
\* slot transitions to "pruned" before the insert lands. The insert then
\* produces a lease row in a "pruned" segment.
\*
\* To expose this, BeginClaim and CommitClaim are separate actions with a
\* per-worker claimIntent snapshot in between. Rotate and Prune actions
\* are enabled between BeginClaim and CommitClaim.
\*
\* If TLC finds a trace violating PrunedLeaseSegmentsAreEmpty, the race is
\* real at the spec's abstraction level. If not, the preconditions on
\* CommitClaim (which must re-check that the target segment is still
\* usable) are strong enough to prevent it — and we've effectively
\* specified what the Rust claim path needs to enforce.
\*
\* This spec is deliberately minimal: no waiting, no DLQ, no deferred. It
\* is solely about claim vs rotate/prune on the lease family. Read it as
\* a refinement of AwaSegmentedStorage's Claim/Rotate/Prune fragment.

CONSTANTS Jobs,
          Workers,
          ReadySegmentCount,
          LeaseSegmentCount,
          MaxRunLease,
          MaxAppendSeq

ReadySegments == 1..ReadySegmentCount
LeaseSegments == 1..LeaseSegmentCount
SegmentStates == {"open", "sealed", "pruned"}

NoWorker == "none"
NoJob == "nojob"
NoReadySegment == 0
NoLeaseSegment == 0
NoLaneSeq == 0

VARIABLES readyEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          laneSeq,
          readySegmentOf,
          leaseSegmentOf,
          readySegments,
          leaseSegments,
          readySegmentCursor,
          leaseSegmentCursor,
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
          readySegments,
          leaseSegments,
          readySegmentCursor,
          leaseSegmentCursor,
          laneState,
          claimIntent>>

NextReadySegment(s) == IF s = ReadySegmentCount THEN 1 ELSE s + 1
NextLeaseSegment(s) == IF s = LeaseSegmentCount THEN 1 ELSE s + 1

CurrentReady == (readyEntries \ activeLeases)

JobsInReadySegment(seg) == {j \in Jobs : readySegmentOf[j] = seg}
JobsInLeaseSegment(seg) == {j \in Jobs : leaseSegmentOf[j] = seg}

InitSegments(Range) == [s \in Range |-> IF s = 1 THEN "open" ELSE "pruned"]

NoIntent == [job |-> NoJob, leaseSeg |-> NoLeaseSegment]

Init ==
    /\ readyEntries = {}
    /\ activeLeases = {}
    /\ leaseOwner = [j \in Jobs |-> NoWorker]
    /\ runLease = [j \in Jobs |-> 0]
    /\ taskLease = [w \in Workers |-> [j \in Jobs |-> 0]]
    /\ laneSeq = [j \in Jobs |-> NoLaneSeq]
    /\ readySegmentOf = [j \in Jobs |-> NoReadySegment]
    /\ leaseSegmentOf = [j \in Jobs |-> NoLeaseSegment]
    /\ readySegments = InitSegments(ReadySegments)
    /\ leaseSegments = InitSegments(LeaseSegments)
    /\ readySegmentCursor = 1
    /\ leaseSegmentCursor = 1
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
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimIntent>>

\* Phase 1 of claim: the worker has acquired the queue_lanes row lock
\* (modelled by the "no other worker has a pending intent on the same
\* lane" precondition) and has snapshotted the lease_ring_state cursor.
\* Between BeginClaim and CommitClaim, RotateLeaseSegments and
\* PruneLeaseSegment may fire.
BeginClaim(w, j) ==
    /\ w \in Workers
    /\ claimIntent[w] = NoIntent
    /\ ~ (\E other \in Workers : other # w /\ claimIntent[other].job = j)
    /\ j \in CurrentReady
    /\ laneSeq[j] = laneState.claimSeq
    /\ runLease[j] < MaxRunLease
    /\ leaseSegments[leaseSegmentCursor] = "open"
    /\ claimIntent' = [claimIntent EXCEPT ![w] =
                          [job |-> j, leaseSeg |-> leaseSegmentCursor]]
    /\ UNCHANGED <<readyEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

\* Phase 2: commit the claim using the snapshotted lease segment. The
\* critical modelling choice here is what the action re-checks on the
\* lease segment at commit time.
\*
\* If CommitClaim blindly uses claimIntent[w].leaseSeg, a race is
\* possible: that segment may have been rotated out and pruned. The
\* resulting lease row sits in a "pruned" segment, violating
\* PrunedLeaseSegmentsAreEmpty.
\*
\* If we add a re-check at commit time (e.g., the segment is still
\* "open" or at least not "pruned"), the race becomes unobservable, and
\* the invariant holds. That extra precondition is what the Rust
\* claim_ready_runtime function must guarantee either by re-reading
\* lease_ring_state under the queue_lanes lock, by holding a share lock
\* on lease_ring_state across the insert, or by some equivalent.
\*
\* We keep the "naive" commit here to force the race to be observable.
CommitClaim(w) ==
    /\ w \in Workers
    /\ claimIntent[w] # NoIntent
    /\ LET j == claimIntent[w].job
           leaseSeg == claimIntent[w].leaseSeg
       IN
       /\ j \in CurrentReady
       /\ laneSeq[j] = laneState.claimSeq
       /\ activeLeases' = activeLeases \cup {j}
       /\ leaseOwner' = [leaseOwner EXCEPT ![j] = w]
       /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
       /\ taskLease' = [taskLease EXCEPT ![w][j] = runLease[j] + 1]
       /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = leaseSeg]
       /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
       /\ claimIntent' = [claimIntent EXCEPT ![w] = NoIntent]
       /\ UNCHANGED <<readyEntries,
                      laneSeq,
                      readySegmentOf,
                      readySegments,
                      leaseSegments,
                      readySegmentCursor,
                      leaseSegmentCursor>>

\* Safer variant: CommitClaim aborts if the snapshotted lease segment has
\* rotated out of "open". Disabled by default — toggle in the config by
\* swapping CommitClaim for CommitClaimChecked in Next.
CommitClaimChecked(w) ==
    /\ w \in Workers
    /\ claimIntent[w] # NoIntent
    /\ LET j == claimIntent[w].job
           leaseSeg == claimIntent[w].leaseSeg
       IN
       /\ j \in CurrentReady
       /\ laneSeq[j] = laneState.claimSeq
       /\ leaseSegments[leaseSeg] = "open"
       /\ activeLeases' = activeLeases \cup {j}
       /\ leaseOwner' = [leaseOwner EXCEPT ![j] = w]
       /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
       /\ taskLease' = [taskLease EXCEPT ![w][j] = runLease[j] + 1]
       /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = leaseSeg]
       /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
       /\ claimIntent' = [claimIntent EXCEPT ![w] = NoIntent]
       /\ UNCHANGED <<readyEntries,
                      laneSeq,
                      readySegmentOf,
                      readySegments,
                      leaseSegments,
                      readySegmentCursor,
                      leaseSegmentCursor>>

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
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
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
                   readySegments,
                   readySegmentCursor,
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
                   readySegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   laneState,
                   claimIntent>>

Stutter == /\ UNCHANGED vars

\* Race-exposing Next: uses the naive CommitClaim that doesn't re-check
\* the lease segment state. If TLC finds PrunedLeaseSegmentsAreEmpty
\* violated, the race is real.
NextRace ==
    \/ \E j \in Jobs : EnqueueReady(j)
    \/ \E w \in Workers, j \in Jobs : BeginClaim(w, j)
    \/ \E w \in Workers : CommitClaim(w)
    \/ \E w \in Workers : AbortClaim(w)
    \/ RotateLeaseSegments
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ Stutter

\* Safe-commit Next: uses CommitClaimChecked which re-reads the lease
\* segment state at commit time. Expect PrunedLeaseSegmentsAreEmpty to
\* hold.
NextSafe ==
    \/ \E j \in Jobs : EnqueueReady(j)
    \/ \E w \in Workers, j \in Jobs : BeginClaim(w, j)
    \/ \E w \in Workers : CommitClaimChecked(w)
    \/ \E w \in Workers : AbortClaim(w)
    \/ RotateLeaseSegments
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ Stutter

\* Select one of NextRace / NextSafe via the config's SPECIFICATION line.
Spec == Init /\ [][NextRace]_vars
SpecSafe == Init /\ [][NextSafe]_vars

TypeOK ==
    /\ Jobs # {}
    /\ Workers # {}
    /\ ReadySegmentCount >= 1
    /\ LeaseSegmentCount >= 1
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
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ leaseSegments \in [LeaseSegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ leaseSegmentCursor \in LeaseSegments
    /\ laneState.appendSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.claimSeq \in 1..(MaxAppendSeq + 1)
    /\ claimIntent \in [Workers -> [job: Jobs \cup {NoJob},
                                    leaseSeg: LeaseSegments \cup {NoLeaseSegment}]]

OneOpenLeaseSegment == Cardinality({s \in LeaseSegments : leaseSegments[s] = "open"}) = 1
LeaseCursorIsOpen == leaseSegments[leaseSegmentCursor] = "open"

ActiveLeasesSubsetReadyEntries == activeLeases \subseteq readyEntries

\* Key race-sensitive invariant. If a lease lands in a pruned segment,
\* this trips and TLC emits a trace proving the race.
PrunedLeaseSegmentsAreEmpty ==
    \A j \in Jobs : leaseSegmentOf[j] # NoLeaseSegment =>
        leaseSegments[leaseSegmentOf[j]] # "pruned"

\* Variant that additionally forbids sealed — a lease landing in a
\* sealed segment isn't a correctness bug per se (sealed segments can
\* still hold live leases until drain), but the race exposure is larger
\* if sealed is also a landing state we didn't intend.
ActiveLeasesInOpenSegmentsOnly ==
    \A j \in activeLeases : leaseSegments[leaseSegmentOf[j]] = "open"

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs : taskLease[w][j] <= runLease[j]

=============================================================================

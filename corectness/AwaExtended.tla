---- MODULE AwaExtended ----
EXTENDS FiniteSets, Sequences, Integers

Jobs == {"j1", "j2", "j3"}
Workers == {"w1", "w2"}
Queues == {"q1", "q2"}
QueueOf == [j \in Jobs |-> IF j = "j3" THEN "q2" ELSE "q1"]
WorkerQueue == [w \in Workers |-> IF w = "w2" THEN "q2" ELSE "q1"]
QueueSeq == <<"q1", "q2">>
MinWorkers == [q \in Queues |-> 1]
Weight == [q \in Queues |-> IF q = "q1" THEN 3 ELSE 1]
GlobalOverflow == 2
MaxDemand == 2

JobStates == {"available", "running", "retryable", "completed", "failed", "cancelled"}
FinalStates == {"retryable", "completed", "failed", "cancelled"}
PermitKinds == {"none", "local", "overflow"}
NoWorker == "no_worker"

VARIABLES
    jobState,
    owner,
    permitHolder,
    permitKind,
    cancelRequested,
    shutdownPhase,
    dispatchersAlive,
    heartbeatAlive,
    maintenanceAlive

vars ==
    <<jobState, owner, permitHolder, permitKind, cancelRequested,
      shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

RECURSIVE SumSeq(_)
SumSeq(seq) ==
    IF Len(seq) = 0
    THEN 0
    ELSE seq[1] + SumSeq(SubSeq(seq, 2, Len(seq)))

JobsInQueue(q) == {j \in Jobs : QueueOf[j] = q}

UnreservedAvailableInQueue(q) ==
    \E j \in JobsInQueue(q) : jobState[j] = "available" /\ permitHolder[j] = NoWorker

RunningJobs ==
    {j \in Jobs : jobState[j] = "running"}

HeldJobs(q, kind) ==
    {j \in JobsInQueue(q) : permitHolder[j] \in Workers /\ permitKind[j] = kind}

LocalHeld(q) ==
    Cardinality(HeldJobs(q, "local"))

OverflowHeld(q) ==
    Cardinality(HeldJobs(q, "overflow"))

TotalHeldPermits ==
    Cardinality({j \in Jobs : permitHolder[j] \in Workers})

TotalOverflowHeld ==
    Cardinality({j \in Jobs : permitHolder[j] \in Workers /\ permitKind[j] = "overflow"})

NeedsOverflow(q) ==
    /\ dispatchersAlive
    /\ UnreservedAvailableInQueue(q)
    /\ LocalHeld(q) >= MinWorkers[q]

Contending(q) ==
    NeedsOverflow(q) \/ OverflowHeld(q) > 0

ContendingWeight ==
    SumSeq([i \in 1..Len(QueueSeq) |->
        IF Contending(QueueSeq[i]) THEN Weight[QueueSeq[i]] ELSE 0
    ])

FairShare(q) ==
    IF ContendingWeight = 0
    THEN 0
    ELSE (GlobalOverflow * Weight[q] + ContendingWeight - 1) \div ContendingWeight

Init ==
    /\ jobState = [j \in Jobs |-> "available"]
    /\ owner = [j \in Jobs |-> NoWorker]
    /\ permitHolder = [j \in Jobs |-> NoWorker]
    /\ permitKind = [j \in Jobs |-> "none"]
    /\ cancelRequested = [j \in Jobs |-> FALSE]
    /\ shutdownPhase = "running"
    /\ dispatchersAlive = TRUE
    /\ heartbeatAlive = TRUE
    /\ maintenanceAlive = TRUE

ReserveLocal(w, j) ==
    LET q == WorkerQueue[w] IN
    /\ dispatchersAlive
    /\ jobState[j] = "available"
    /\ permitHolder[j] = NoWorker
    /\ QueueOf[j] = q
    /\ LocalHeld(q) < MinWorkers[q]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = w]
    /\ permitKind' = [permitKind EXCEPT ![j] = "local"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, owner, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ReserveOverflow(w, j) ==
    LET q == WorkerQueue[w] IN
    /\ dispatchersAlive
    /\ jobState[j] = "available"
    /\ permitHolder[j] = NoWorker
    /\ QueueOf[j] = q
    /\ NeedsOverflow(q)
    /\ TotalOverflowHeld < GlobalOverflow
    /\ OverflowHeld(q) < FairShare(q)
    /\ permitHolder' = [permitHolder EXCEPT ![j] = w]
    /\ permitKind' = [permitKind EXCEPT ![j] = "overflow"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, owner, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ReleaseReservation(w, j) ==
    /\ permitHolder[j] = w
    /\ jobState[j] = "available"
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ UNCHANGED <<jobState, owner, cancelRequested, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ClaimReserved(w, j) ==
    /\ dispatchersAlive
    /\ jobState[j] = "available"
    /\ permitHolder[j] = w
    /\ permitKind[j] \in {"local", "overflow"}
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ UNCHANGED <<owner, permitHolder, permitKind, cancelRequested,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

StartExecution(w, j) ==
    /\ jobState[j] = "running"
    /\ permitHolder[j] = w
    /\ owner[j] = NoWorker
    /\ owner' = [owner EXCEPT ![j] = w]
    /\ UNCHANGED <<jobState, permitHolder, permitKind, cancelRequested,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

Finalize(w, j, toState) ==
    /\ toState \in FinalStates
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ jobState' = [jobState EXCEPT ![j] = toState]
    /\ owner' = [owner EXCEPT ![j] = NoWorker]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

Rescue(j) ==
    LET w == permitHolder[j] IN
    /\ maintenanceAlive
    /\ jobState[j] = "running"
    /\ w \in Workers
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ owner' = [owner EXCEPT ![j] = NoWorker]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

AdminCancel(j) ==
    /\ jobState[j] \in {"available", "running", "retryable"}
    /\ jobState' = [jobState EXCEPT ![j] = "cancelled"]
    /\ owner' = [owner EXCEPT ![j] = NoWorker]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelRequested' =
        IF jobState[j] = "running"
        THEN [cancelRequested EXCEPT ![j] = TRUE]
        ELSE cancelRequested
    /\ UNCHANGED <<shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

PromoteRetryable(j) ==
    /\ jobState[j] = "retryable"
    /\ jobState' = [jobState EXCEPT ![j] = "available"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<owner, permitHolder, permitKind, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ShutdownBegin ==
    /\ shutdownPhase = "running"
    /\ shutdownPhase' = "stop_claim"
    /\ dispatchersAlive' = FALSE
    /\ maintenanceAlive' = FALSE
    /\ UNCHANGED <<jobState, owner, permitHolder, permitKind, cancelRequested, heartbeatAlive>>

EnterDraining ==
    /\ shutdownPhase = "stop_claim"
    /\ shutdownPhase' = "draining"
    /\ UNCHANGED <<jobState, owner, permitHolder, permitKind, cancelRequested,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

StopHeartbeat ==
    /\ heartbeatAlive
    /\ shutdownPhase = "draining"
    /\ RunningJobs = {}
    /\ heartbeatAlive' = FALSE
    /\ UNCHANGED <<jobState, owner, permitHolder, permitKind, cancelRequested,
                  shutdownPhase, dispatchersAlive, maintenanceAlive>>

FinishShutdown ==
    /\ shutdownPhase = "draining"
    /\ ~heartbeatAlive
    /\ RunningJobs = {}
    /\ TotalHeldPermits = 0
    /\ shutdownPhase' = "stopped"
    /\ UNCHANGED <<jobState, owner, permitHolder, permitKind, cancelRequested,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

Stutter ==
    UNCHANGED vars

Next ==
    \/ \E w \in Workers, j \in Jobs : ReserveLocal(w, j)
    \/ \E w \in Workers, j \in Jobs : ReserveOverflow(w, j)
    \/ \E w \in Workers, j \in Jobs : ReleaseReservation(w, j)
    \/ \E w \in Workers, j \in Jobs : ClaimReserved(w, j)
    \/ \E w \in Workers, j \in Jobs : StartExecution(w, j)
    \/ \E w \in Workers, j \in Jobs, s \in FinalStates : Finalize(w, j, s)
    \/ \E j \in Jobs : Rescue(j)
    \/ \E j \in Jobs : AdminCancel(j)
    \/ \E j \in Jobs : PromoteRetryable(j)
    \/ ShutdownBegin
    \/ EnterDraining
    \/ StopHeartbeat
    \/ FinishShutdown
    \/ Stutter

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ owner \in [Jobs -> Workers \cup {NoWorker}]
    /\ permitHolder \in [Jobs -> Workers \cup {NoWorker}]
    /\ permitKind \in [Jobs -> PermitKinds]
    /\ cancelRequested \in [Jobs -> BOOLEAN]
    /\ shutdownPhase \in {"running", "stop_claim", "draining", "stopped"}
    /\ dispatchersAlive \in BOOLEAN
    /\ heartbeatAlive \in BOOLEAN
    /\ maintenanceAlive \in BOOLEAN

RunningOwned ==
    \A j \in Jobs : owner[j] \in Workers => jobState[j] = "running"

NonRunningUnowned ==
    \A j \in Jobs : jobState[j] # "running" => owner[j] = NoWorker

RunningHasPermit ==
    \A j \in Jobs : jobState[j] = "running" => permitHolder[j] \in Workers /\ permitKind[j] \in {"local", "overflow"}

NonRunningHasNoPermit ==
    \A j \in Jobs : jobState[j] \in {"completed", "failed", "cancelled", "retryable"} =>
        permitHolder[j] = NoWorker /\ permitKind[j] = "none"

LocalCapacitySafe ==
    \A q \in Queues : LocalHeld(q) <= MinWorkers[q]

OverflowCapacitySafe ==
    TotalOverflowHeld <= GlobalOverflow

NoClaimAfterStopClaim ==
    shutdownPhase \in {"stop_claim", "draining", "stopped"} => ~dispatchersAlive

HeartbeatUntilDrained ==
    ((shutdownPhase \in {"stop_claim", "draining"}) /\ RunningJobs # {})
    => heartbeatAlive

ServicePhaseConsistency ==
    /\ dispatchersAlive => shutdownPhase = "running"
    /\ maintenanceAlive => shutdownPhase = "running"
    /\ ~heartbeatAlive => shutdownPhase \in {"draining", "stopped"}

ExecutingHasOwner ==
    \A j \in Jobs : owner[j] \in Workers => permitHolder[j] = owner[j]

Spec == Init /\ [][Next]_vars

====

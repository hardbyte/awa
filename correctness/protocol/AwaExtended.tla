---- MODULE AwaExtended ----
EXTENDS FiniteSets, Sequences, Integers

\* Whether the worker drops a handler still running well past its deadline
\* (#546). FALSE models the earlier runtime, where a rescued attempt whose
\* handler never returned kept its worker slot forever.
CONSTANT DropRescuedHandlers

Instances == {"i1", "i2"}
Jobs == {"j1", "j2", "j3"}
\* One job is enough to show a hung handler holding the queue's only slot.
HungHandlerJobs == {"j1"}
Queues == {"q1", "q2"}
QueueOf == [j \in Jobs |-> IF j = "j3" THEN "q2" ELSE "q1"]
QueueSeq == <<"q1", "q2">>
MinWorkers == [q \in Queues |-> 1]
Weight == [q \in Queues |-> IF q = "q1" THEN 3 ELSE 1]
GlobalOverflow == 2
BatchMax == 2
RateMax == 1
MaxAttempts == 2

JobStates == {"available", "running", "retryable", "completed", "failed"}
\* Outcomes a handler's attempt can finalize to. `retryable` needs attempts
\* left; on the last attempt a retryable error fails the job instead.
FinalStates == {"retryable", "completed", "failed"}
PermitKinds == {"none", "local", "overflow"}
ShutdownPhases == {"running", "stop_claim", "draining", "stopped"}
NoInstance == "no_instance"
DeferredStates == {"retryable"}
HotStates == JobStates \ DeferredStates

VARIABLES
    jobState,
    attempt,
    lease,
    dbOwner,
    permitHolder,
    permitKind,
    inFlight,
    taskLease,
    rescuedSlot,      \* [i -> [j -> permit kind]]: the slot a rescued
                      \* attempt's still-running handler keeps holding
    cancelRequested,
    heartbeatFresh,
    deadlineExpired,
    rateBudget,
    shutdownPhase,
    dispatchersAlive,
    heartbeatAlive,
    maintenanceAlive,
    leader

vars ==
    <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
      inFlight, taskLease, rescuedSlot, cancelRequested, heartbeatFresh,
      deadlineExpired,
      rateBudget, shutdownPhase, dispatchersAlive, heartbeatAlive,
      maintenanceAlive, leader>>

RECURSIVE SumSeq(_)
SumSeq(seq) ==
    IF Len(seq) = 0
    THEN 0
    ELSE seq[1] + SumSeq(SubSeq(seq, 2, Len(seq)))

JobsInQueue(q) == {j \in Jobs : QueueOf[j] = q}

UnreservedAvailableInQueue(q) ==
    \E j \in JobsInQueue(q) :
        /\ jobState[j] = "available"
        /\ permitHolder[j] = NoInstance

HeldJobs(i, q, kind) ==
    {j \in JobsInQueue(q) : permitHolder[j] = i /\ permitKind[j] = kind}

\* A rescued attempt whose handler has not returned still holds its slot
\* on that instance, beside any reservation for the job's retry.
RescuedHeld(i, q, kind) ==
    {j \in JobsInQueue(q) : rescuedSlot[i][j] = kind}

HeldByInstance(i) ==
    Cardinality({j \in Jobs : permitHolder[j] = i})
    + Cardinality({j \in Jobs : rescuedSlot[i][j] # "none"})

LocalHeld(i, q) ==
    Cardinality(HeldJobs(i, q, "local")) + Cardinality(RescuedHeld(i, q, "local"))

OverflowHeld(i, q) ==
    Cardinality(HeldJobs(i, q, "overflow")) + Cardinality(RescuedHeld(i, q, "overflow"))

TotalOverflowHeld(i) ==
    Cardinality({j \in Jobs : permitHolder[j] = i /\ permitKind[j] = "overflow"})
    + Cardinality({j \in Jobs : rescuedSlot[i][j] = "overflow"})

InFlightJobs(i) ==
    {j \in Jobs : inFlight[i][j]}

TrackedRunningJobs(i) ==
    {j \in Jobs :
        /\ jobState[j] = "running"
        /\ (permitHolder[j] = i \/ dbOwner[j] = i \/ inFlight[i][j])}

NeedsOverflow(i, q) ==
    /\ dispatchersAlive[i]
    /\ HeldByInstance(i) < BatchMax
    /\ rateBudget[i][q] > 0
    /\ UnreservedAvailableInQueue(q)
    /\ LocalHeld(i, q) >= MinWorkers[q]

Contending(i, q) ==
    NeedsOverflow(i, q) \/ OverflowHeld(i, q) > 0

ContendingWeight(i) ==
    SumSeq([k \in 1..Len(QueueSeq) |->
        IF Contending(i, QueueSeq[k]) THEN Weight[QueueSeq[k]] ELSE 0
    ])

FairShare(i, q) ==
    IF ContendingWeight(i) = 0
    THEN 0
    ELSE (GlobalOverflow * Weight[q] + ContendingWeight(i) - 1) \div ContendingWeight(i)

OwnerAbandoned(j) ==
    /\ dbOwner[j] \in Instances
    /\ shutdownPhase[dbOwner[j]] = "stopped"

UnownedAbandoned(j) ==
    /\ dbOwner[j] = NoInstance
    /\ permitHolder[j] = NoInstance
    /\ \E i \in Instances : shutdownPhase[i] = "stopped"

Abandoned(j) ==
    /\ jobState[j] = "running"
    /\ (OwnerAbandoned(j) \/ UnownedAbandoned(j))

RecoverableAbandoned(j) ==
    /\ Abandoned(j)
    /\ \E i \in Instances :
        /\ maintenanceAlive[i]
        /\ shutdownPhase[i] = "running"

Init ==
    /\ jobState = [j \in Jobs |-> "available"]
    /\ attempt = [j \in Jobs |-> 0]
    /\ lease = [j \in Jobs |-> 0]
    /\ dbOwner = [j \in Jobs |-> NoInstance]
    /\ permitHolder = [j \in Jobs |-> NoInstance]
    /\ permitKind = [j \in Jobs |-> "none"]
    /\ inFlight = [i \in Instances |-> [j \in Jobs |-> FALSE]]
    /\ taskLease = [i \in Instances |-> [j \in Jobs |-> 0]]
    /\ rescuedSlot = [i \in Instances |-> [j \in Jobs |-> "none"]]
    /\ cancelRequested = [i \in Instances |-> [j \in Jobs |-> FALSE]]
    /\ heartbeatFresh = [j \in Jobs |-> FALSE]
    /\ deadlineExpired = [j \in Jobs |-> FALSE]
    /\ rateBudget = [i \in Instances |-> [q \in Queues |-> RateMax]]
    /\ shutdownPhase = [i \in Instances |-> "running"]
    /\ dispatchersAlive = [i \in Instances |-> TRUE]
    /\ heartbeatAlive = [i \in Instances |-> TRUE]
    /\ maintenanceAlive = [i \in Instances |-> TRUE]
    /\ leader = "i1"

ReserveLocal(i, j) ==
    LET q == QueueOf[j] IN
    /\ dispatchersAlive[i]
    /\ jobState[j] = "available"
    /\ permitHolder[j] = NoInstance
    /\ HeldByInstance(i) < BatchMax
    /\ LocalHeld(i, q) < MinWorkers[q]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = i]
    /\ permitKind' = [permitKind EXCEPT ![j] = "local"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, inFlight, taskLease,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

ReserveOverflow(i, j) ==
    LET q == QueueOf[j] IN
    /\ dispatchersAlive[i]
    /\ jobState[j] = "available"
    /\ permitHolder[j] = NoInstance
    /\ HeldByInstance(i) < BatchMax
    /\ NeedsOverflow(i, q)
    /\ TotalOverflowHeld(i) < GlobalOverflow
    /\ OverflowHeld(i, q) < FairShare(i, q)
    /\ permitHolder' = [permitHolder EXCEPT ![j] = i]
    /\ permitKind' = [permitKind EXCEPT ![j] = "overflow"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, inFlight, taskLease,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

ReleaseReservation(i, j) ==
    /\ permitHolder[j] = i
    /\ jobState[j] = "available"
    /\ ~inFlight[i][j]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoInstance]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, inFlight, taskLease,
                  cancelRequested, heartbeatFresh, deadlineExpired,
                  rateBudget, shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

ClaimReserved(i, j) ==
    LET q == QueueOf[j] IN
    /\ dispatchersAlive[i]
    /\ jobState[j] = "available"
    /\ permitHolder[j] = i
    /\ permitKind[j] \in {"local", "overflow"}
    /\ rateBudget[i][q] > 0
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ attempt' = [attempt EXCEPT ![j] = @ + 1]
    /\ lease' = [lease EXCEPT ![j] = @ + 1]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ rateBudget' = [rateBudget EXCEPT ![i][q] = @ - 1]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ UNCHANGED <<dbOwner, permitHolder, permitKind, inFlight, taskLease,
                  shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

StartExecution(i, j) ==
    /\ jobState[j] = "running"
    /\ permitHolder[j] = i
    /\ dbOwner[j] = NoInstance
    /\ ~inFlight[i][j]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = i]
    /\ inFlight' = [inFlight EXCEPT ![i][j] = TRUE]
    /\ taskLease' = [taskLease EXCEPT ![i][j] = lease[j]]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = TRUE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, permitHolder, permitKind,
                  rateBudget, shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

HeartbeatPulse(i, j) ==
    /\ heartbeatAlive[i]
    /\ inFlight[i][j]
    /\ jobState[j] = "running"
    /\ dbOwner[j] = i
    /\ taskLease[i][j] = lease[j]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, deadlineExpired,
                  rateBudget, shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

HeartbeatBecomesStale(j) ==
    /\ jobState[j] = "running"
    /\ dbOwner[j] \in Instances
    /\ heartbeatFresh[j]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, deadlineExpired,
                  rateBudget, shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

DeadlineExpires(j) ==
    /\ jobState[j] = "running"
    /\ dbOwner[j] \in Instances
    /\ ~deadlineExpired[j]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  rateBudget, shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

Finalize(i, j, toState) ==
    /\ toState \in FinalStates
    /\ toState = "retryable" => attempt[j] < MaxAttempts
    /\ inFlight[i][j]
    /\ jobState[j] = "running"
    /\ dbOwner[j] = i
    /\ taskLease[i][j] = lease[j]
    /\ jobState' = [jobState EXCEPT ![j] = toState]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoInstance]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoInstance]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ inFlight' = [inFlight EXCEPT ![i][j] = FALSE]
    /\ taskLease' = [taskLease EXCEPT ![i][j] = 0]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<attempt, lease, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive, leader, rescuedSlot>>

FinalizeRejected(i, j, toState) ==
    /\ toState \in FinalStates
    /\ inFlight[i][j]
    /\ (jobState[j] # "running"
        \/ dbOwner[j] # i
        \/ taskLease[i][j] # lease[j])
    /\ inFlight' = [inFlight EXCEPT ![i][j] = FALSE]
    /\ taskLease' = [taskLease EXCEPT ![i][j] = 0]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ permitHolder' =
        IF permitHolder[j] = i /\ lease[j] = taskLease[i][j]
        THEN [permitHolder EXCEPT ![j] = NoInstance]
        ELSE permitHolder
    /\ permitKind' =
        IF permitHolder[j] = i /\ lease[j] = taskLease[i][j]
        THEN [permitKind EXCEPT ![j] = "none"]
        ELSE permitKind
    /\ rescuedSlot' = [rescuedSlot EXCEPT ![i][j] = "none"]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive, leader>>

\* Rescue spends the attempt like any other failure: retry while attempts
\* remain, fail once the rescued attempt was the last (#546).
RescueOutcome(j) ==
    IF attempt[j] < MaxAttempts THEN "retryable" ELSE "failed"

\* Rescue frees the job for its retry, but a handler still running for the
\* rescued attempt keeps its worker slot until it returns or is dropped.
RescuedSlotAfter(j) ==
    LET h == permitHolder[j] IN
    IF h \in Instances /\ inFlight[h][j]
    THEN [rescuedSlot EXCEPT ![h][j] = permitKind[j]]
    ELSE rescuedSlot

\* The worker gives up on a handler still running past its deadline plus
\* two deadline-rescue intervals: it drops the handler future and returns
\* the slot without finalizing, since rescue has settled the attempt.
DropRescuedHandler(i, j) ==
    /\ DropRescuedHandlers
    /\ rescuedSlot[i][j] # "none"
    /\ inFlight[i][j]
    /\ inFlight' = [inFlight EXCEPT ![i][j] = FALSE]
    /\ taskLease' = [taskLease EXCEPT ![i][j] = 0]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i][j] = FALSE]
    /\ rescuedSlot' = [rescuedSlot EXCEPT ![i][j] = "none"]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  heartbeatFresh, deadlineExpired, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive, leader>>

RescueByHeartbeat(i, j) ==
    /\ leader = i
    /\ maintenanceAlive[i]
    /\ jobState[j] = "running"
    /\ dbOwner[j] \in Instances
    /\ ~heartbeatFresh[j]
    /\ jobState' = [jobState EXCEPT ![j] = RescueOutcome(j)]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoInstance]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoInstance]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ rescuedSlot' = RescuedSlotAfter(j)
    /\ cancelRequested' =
        [cancelRequested EXCEPT ![i][j] = cancelRequested[i][j] \/ inFlight[i][j]]
    /\ UNCHANGED <<attempt, lease, inFlight, taskLease, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive, leader>>

RescueByDeadline(i, j) ==
    /\ leader = i
    /\ maintenanceAlive[i]
    /\ jobState[j] = "running"
    /\ dbOwner[j] \in Instances
    /\ deadlineExpired[j]
    /\ jobState' = [jobState EXCEPT ![j] = RescueOutcome(j)]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoInstance]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoInstance]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ rescuedSlot' = RescuedSlotAfter(j)
    /\ cancelRequested' =
        [cancelRequested EXCEPT ![i][j] = cancelRequested[i][j] \/ inFlight[i][j]]
    /\ UNCHANGED <<attempt, lease, inFlight, taskLease, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive, leader>>

PromoteRetryable(j) ==
    /\ jobState[j] = "retryable"
    /\ jobState' = [jobState EXCEPT ![j] = "available"]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive,
                  maintenanceAlive, leader, rescuedSlot>>

RefillBudget(i, q, n) ==
    /\ i \in Instances
    /\ q \in Queues
    /\ n \in 1..RateMax
    /\ rateBudget' =
        [rateBudget EXCEPT ![i][q] =
            IF @ + n < RateMax THEN @ + n ELSE RateMax]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive, leader, rescuedSlot>>

AcquireLeadership(i) ==
    /\ leader = NoInstance
    /\ maintenanceAlive[i]
    /\ shutdownPhase[i] # "stopped"
    /\ leader' = i
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive, rescuedSlot>>

RelinquishLeadership(i) ==
    /\ leader = i
    /\ leader' = NoInstance
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive, rescuedSlot>>

ShutdownBegin(i) ==
    /\ shutdownPhase[i] = "running"
    /\ shutdownPhase' = [shutdownPhase EXCEPT ![i] = "stop_claim"]
    /\ dispatchersAlive' = [dispatchersAlive EXCEPT ![i] = FALSE]
    /\ permitHolder' =
        [j \in Jobs |->
            IF permitHolder[j] = i /\ jobState[j] = "available"
            THEN NoInstance
            ELSE permitHolder[j]]
    /\ permitKind' =
        [j \in Jobs |->
            IF permitHolder[j] = i /\ jobState[j] = "available"
            THEN "none"
            ELSE permitKind[j]]
    /\ cancelRequested' =
        [cancelRequested EXCEPT ![i] =
            [j \in Jobs |-> cancelRequested[i][j] \/ inFlight[i][j]]]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, inFlight, taskLease,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  heartbeatAlive, maintenanceAlive, leader, rescuedSlot>>

EnterDraining(i) ==
    /\ shutdownPhase[i] = "stop_claim"
    /\ shutdownPhase' = [shutdownPhase EXCEPT ![i] = "draining"]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, rateBudget, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive, leader, rescuedSlot>>

StopHeartbeat(i) ==
    /\ heartbeatAlive[i]
    /\ shutdownPhase[i] = "draining"
    /\ TrackedRunningJobs(i) = {}
    /\ InFlightJobs(i) = {}
    /\ heartbeatAlive' = [heartbeatAlive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase, dispatchersAlive,
                  maintenanceAlive, leader, rescuedSlot>>

StopMaintenance(i) ==
    /\ maintenanceAlive[i]
    /\ shutdownPhase[i] = "draining"
    /\ ~heartbeatAlive[i]
    /\ maintenanceAlive' = [maintenanceAlive EXCEPT ![i] = FALSE]
    /\ leader' = IF leader = i THEN NoInstance ELSE leader
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, rateBudget, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, rescuedSlot>>

DrainTimeout(i) ==
    /\ shutdownPhase[i] = "draining"
    /\ InFlightJobs(i) # {}
    /\ shutdownPhase' = [shutdownPhase EXCEPT ![i] = "stopped"]
    /\ heartbeatAlive' = [heartbeatAlive EXCEPT ![i] = FALSE]
    /\ maintenanceAlive' = [maintenanceAlive EXCEPT ![i] = FALSE]
    /\ dispatchersAlive' = [dispatchersAlive EXCEPT ![i] = FALSE]
    /\ inFlight' = [inFlight EXCEPT ![i] = [j \in Jobs |-> FALSE]]
    /\ taskLease' = [taskLease EXCEPT ![i] = [j \in Jobs |-> 0]]
    /\ rescuedSlot' = [rescuedSlot EXCEPT ![i] = [j \in Jobs |-> "none"]]
    /\ cancelRequested' = [cancelRequested EXCEPT ![i] = [j \in Jobs |-> FALSE]]
    /\ permitHolder' =
        [j \in Jobs |->
            IF permitHolder[j] = i THEN NoInstance ELSE permitHolder[j]]
    /\ permitKind' =
        [j \in Jobs |->
            IF permitHolder[j] = i THEN "none" ELSE permitKind[j]]
    /\ leader' = IF leader = i THEN NoInstance ELSE leader
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, heartbeatFresh,
                  deadlineExpired, rateBudget>>

FinishShutdown(i) ==
    /\ shutdownPhase[i] = "draining"
    /\ ~heartbeatAlive[i]
    /\ ~maintenanceAlive[i]
    /\ InFlightJobs(i) = {}
    /\ HeldByInstance(i) = 0
    /\ shutdownPhase' = [shutdownPhase EXCEPT ![i] = "stopped"]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  inFlight, taskLease, cancelRequested, heartbeatFresh,
                  deadlineExpired, rateBudget, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive, leader, rescuedSlot>>

AnyFinalize ==
    \E i \in Instances, j \in Jobs, s \in FinalStates : Finalize(i, j, s)

AnyFinalizeRejected ==
    \E i \in Instances, j \in Jobs, s \in FinalStates : FinalizeRejected(i, j, s)

AnyReserveOverflow ==
    \E i \in Instances, j \in Jobs : ReserveOverflow(i, j)

AnyStartExecution ==
    \E i \in Instances, j \in Jobs : StartExecution(i, j)

AnyPromoteRetryable ==
    \E j \in Jobs : PromoteRetryable(j)

AnyHeartbeatPulse ==
    \E i \in Instances, j \in Jobs : HeartbeatPulse(i, j)

AnyHeartbeatBecomesStale ==
    \E j \in Jobs : HeartbeatBecomesStale(j)

AnyRefill ==
    \E i \in Instances, q \in Queues, n \in 1..RateMax : RefillBudget(i, q, n)

AnyAcquireLeadership ==
    \E i \in Instances : AcquireLeadership(i)

AnyEnterDraining ==
    \E i \in Instances : EnterDraining(i)

AnyStopHeartbeat ==
    \E i \in Instances : StopHeartbeat(i)

AnyStopMaintenance ==
    \E i \in Instances : StopMaintenance(i)

AnyFinishShutdown ==
    \E i \in Instances : FinishShutdown(i)

AnyDrainTimeout ==
    \E i \in Instances : DrainTimeout(i)

AnyRescueByHeartbeat ==
    \E i \in Instances, j \in Jobs : RescueByHeartbeat(i, j)

AnyRescueByDeadline ==
    \E i \in Instances, j \in Jobs : RescueByDeadline(i, j)

AnyDropRescuedHandler ==
    \E i \in Instances, j \in Jobs : DropRescuedHandler(i, j)

Next ==
    \/ \E i \in Instances, j \in Jobs : ReserveLocal(i, j)
    \/ \E i \in Instances, j \in Jobs : ReserveOverflow(i, j)
    \/ \E i \in Instances, j \in Jobs : ReleaseReservation(i, j)
    \/ \E i \in Instances, j \in Jobs : ClaimReserved(i, j)
    \/ AnyStartExecution
    \/ AnyFinalize
    \/ AnyFinalizeRejected
    \/ AnyHeartbeatPulse
    \/ AnyHeartbeatBecomesStale
    \/ \E j \in Jobs : DeadlineExpires(j)
    \/ AnyRescueByHeartbeat
    \/ AnyRescueByDeadline
    \/ AnyDropRescuedHandler
    \/ AnyPromoteRetryable
    \/ AnyRefill
    \/ AnyAcquireLeadership
    \/ \E i \in Instances : ShutdownBegin(i)
    \/ AnyEnterDraining
    \/ AnyStopHeartbeat
    \/ AnyStopMaintenance
    \/ AnyDrainTimeout
    \/ AnyFinishShutdown

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ attempt \in [Jobs -> 0..MaxAttempts]
    /\ lease \in [Jobs -> 0..MaxAttempts]
    /\ dbOwner \in [Jobs -> Instances \cup {NoInstance}]
    /\ permitHolder \in [Jobs -> Instances \cup {NoInstance}]
    /\ permitKind \in [Jobs -> PermitKinds]
    /\ inFlight \in [Instances -> [Jobs -> BOOLEAN]]
    /\ taskLease \in [Instances -> [Jobs -> 0..MaxAttempts]]
    /\ rescuedSlot \in [Instances -> [Jobs -> PermitKinds]]
    /\ cancelRequested \in [Instances -> [Jobs -> BOOLEAN]]
    /\ heartbeatFresh \in [Jobs -> BOOLEAN]
    /\ deadlineExpired \in [Jobs -> BOOLEAN]
    /\ rateBudget \in [Instances -> [Queues -> 0..RateMax]]
    /\ shutdownPhase \in [Instances -> ShutdownPhases]
    /\ dispatchersAlive \in [Instances -> BOOLEAN]
    /\ heartbeatAlive \in [Instances -> BOOLEAN]
    /\ maintenanceAlive \in [Instances -> BOOLEAN]
    /\ leader \in Instances \cup {NoInstance}

RunningHasPermit ==
    \A j \in Jobs :
        jobState[j] = "running" /\ ~Abandoned(j) =>
            permitHolder[j] \in Instances /\ permitKind[j] \in {"local", "overflow"}

DBOwnerRequiresRunning ==
    \A j \in Jobs :
        dbOwner[j] \in Instances => jobState[j] = "running"

CurrentOwnerConsistent ==
    \A j \in Jobs :
        dbOwner[j] \in Instances /\ shutdownPhase[dbOwner[j]] # "stopped" =>
            /\ permitHolder[j] = dbOwner[j]
            /\ inFlight[dbOwner[j]][j]
            /\ taskLease[dbOwner[j]][j] = lease[j]

TaskLeaseBounded ==
    \A i \in Instances, j \in Jobs :
        inFlight[i][j] =>
            /\ taskLease[i][j] > 0
            /\ taskLease[i][j] <= lease[j]

TerminalReleasesPermit ==
    \A j \in Jobs :
        jobState[j] \in FinalStates =>
            /\ dbOwner[j] = NoInstance
            /\ permitHolder[j] = NoInstance
            /\ permitKind[j] = "none"

DeferredRowsIdle ==
    \A j \in Jobs :
        jobState[j] \in DeferredStates =>
            /\ dbOwner[j] = NoInstance
            /\ permitHolder[j] = NoInstance
            /\ permitKind[j] = "none"

\* No job is ever retried past its budget: whatever ends an attempt (an
\* error, a crash, a hang), a job waiting to run has attempts left. Before
\* #546 rescue sent exhausted attempts back to `retryable`, and the claim
\* path has no attempt check to stop them.
RetryHasBudget ==
    \A j \in Jobs :
        jobState[j] \in {"available", "retryable"} => attempt[j] < MaxAttempts

\* A rescued slot belongs to a handler that is still running for an attempt
\* the job has moved past.
RescuedSlotHasStaleTask ==
    \A i \in Instances, j \in Jobs :
        rescuedSlot[i][j] # "none" =>
            /\ inFlight[i][j]
            /\ (jobState[j] # "running"
                \/ dbOwner[j] # i
                \/ taskLease[i][j] # lease[j])

LocalCapacitySafe ==
    \A i \in Instances, q \in Queues : LocalHeld(i, q) <= MinWorkers[q]

OverflowCapacitySafe ==
    \A i \in Instances : TotalOverflowHeld(i) <= GlobalOverflow

BatchBounded ==
    \A i \in Instances : HeldByInstance(i) <= BatchMax

RateBudgetBounded ==
    rateBudget \in [Instances -> [Queues -> 0..RateMax]]

NoClaimAfterStopClaim ==
    \A i \in Instances :
        shutdownPhase[i] \in {"stop_claim", "draining", "stopped"} =>
            ~dispatchersAlive[i]

HeartbeatUntilDrained ==
    \A i \in Instances :
        ((shutdownPhase[i] \in {"stop_claim", "draining"})
         /\ TrackedRunningJobs(i) # {})
        => heartbeatAlive[i]

ServicePhaseConsistency ==
    \A i \in Instances :
        /\ dispatchersAlive[i] => shutdownPhase[i] = "running"
        /\ ~heartbeatAlive[i] => shutdownPhase[i] \in {"draining", "stopped"}
        /\ ~maintenanceAlive[i] => shutdownPhase[i] \in {"draining", "stopped"}

LeaderConsistent ==
    leader = NoInstance
    \/ (leader \in Instances
        /\ maintenanceAlive[leader]
        /\ shutdownPhase[leader] # "stopped")

StoppedInstancesQuiescent ==
    \A i \in Instances :
        shutdownPhase[i] = "stopped" =>
            /\ ~dispatchersAlive[i]
            /\ ~heartbeatAlive[i]
            /\ ~maintenanceAlive[i]
            /\ InFlightJobs(i) = {}
            /\ HeldByInstance(i) = 0

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(AnyAcquireLeadership)
    /\ WF_vars(AnyEnterDraining)
    /\ WF_vars(AnyStopHeartbeat)
    /\ WF_vars(AnyStopMaintenance)
    /\ WF_vars(AnyFinishShutdown)
    /\ WF_vars(AnyDrainTimeout)
    /\ WF_vars(AnyReserveOverflow)
    /\ WF_vars(AnyStartExecution)
    /\ WF_vars(AnyFinalize)
    /\ WF_vars(AnyFinalizeRejected)
    /\ WF_vars(AnyPromoteRetryable)
    /\ WF_vars(AnyHeartbeatPulse)
    /\ WF_vars(AnyHeartbeatBecomesStale)
    /\ WF_vars(AnyRescueByHeartbeat)
    /\ WF_vars(AnyRescueByDeadline)
    /\ WF_vars(AnyDropRescuedHandler)
    /\ WF_vars(AnyRefill)

\* Hung handlers: a handler whose attempt was rescued may never return, so
\* a late finish gets no fairness. Only dropping it frees its slot.
HungHandlerSpec ==
    Spec
    /\ WF_vars(AnyAcquireLeadership)
    /\ WF_vars(AnyStartExecution)
    /\ WF_vars(AnyFinalize)
    /\ WF_vars(AnyPromoteRetryable)
    /\ WF_vars(AnyHeartbeatPulse)
    /\ WF_vars(AnyHeartbeatBecomesStale)
    /\ WF_vars(AnyRescueByHeartbeat)
    /\ WF_vars(AnyRescueByDeadline)
    /\ WF_vars(AnyDropRescuedHandler)
    /\ WF_vars(AnyRefill)

I1DrainEventuallyStops ==
    (shutdownPhase["i1"] = "stop_claim") ~> (shutdownPhase["i1"] = "stopped")

I1Q1OverflowProgress ==
    NeedsOverflow("i1", "q1")
    ~> (OverflowHeld("i1", "q1") > 0 \/ ~NeedsOverflow("i1", "q1"))

\* The Codex finding on #546: with one worker slot, a rescued handler that
\* never returns must still give its slot back, or the retry never runs.
RescuedSlotsReleased ==
    \A i \in Instances, j \in Jobs :
        (rescuedSlot[i][j] # "none") ~> (rescuedSlot[i][j] = "none")

AbandonedJobsEventuallyLeaveRunning ==
    \A j \in Jobs : RecoverableAbandoned(j) ~> (jobState[j] # "running")

====

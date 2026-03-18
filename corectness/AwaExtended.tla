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
BatchMax == 2
RateMax == 1
MaxAttempts == 2

JobStates == {"available", "running", "retryable", "completed", "failed", "cancelled"}
FinalStates == {"retryable", "completed"}
PermitKinds == {"none", "local", "overflow"}
NoWorker == "no_worker"

VARIABLES
    jobState,
    attempt,
    lease,
    dbOwner,
    taskWorker,
    taskLease,
    permitHolder,
    permitKind,
    inFlight,
    cancelRequested,
    heartbeatFresh,
    deadlineExpired,
    rateBudget,
    shutdownPhase,
    dispatchersAlive,
    heartbeatAlive,
    maintenanceAlive

vars ==
    <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
      permitHolder, permitKind, inFlight, cancelRequested,
      heartbeatFresh, deadlineExpired, rateBudget,
      shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

RECURSIVE SumSeq(_)
SumSeq(seq) ==
    IF Len(seq) = 0
    THEN 0
    ELSE seq[1] + SumSeq(SubSeq(seq, 2, Len(seq)))

JobsInQueue(q) == {j \in Jobs : QueueOf[j] = q}

UnreservedAvailableInQueue(q) ==
    \E j \in JobsInQueue(q) :
        /\ jobState[j] = "available"
        /\ permitHolder[j] = NoWorker

HeldJobs(q, kind) ==
    {j \in JobsInQueue(q) : permitHolder[j] \in Workers /\ permitKind[j] = kind}

HeldByWorker(w) ==
    Cardinality({j \in Jobs : permitHolder[j] = w})

LocalHeld(q) ==
    Cardinality(HeldJobs(q, "local"))

OverflowHeld(q) ==
    Cardinality(HeldJobs(q, "overflow"))

TotalHeldPermits ==
    Cardinality({j \in Jobs : permitHolder[j] \in Workers})

TotalOverflowHeld ==
    Cardinality({j \in Jobs : permitHolder[j] \in Workers /\ permitKind[j] = "overflow"})

InFlightJobs ==
    {j \in Jobs : inFlight[j]}

RunningJobs ==
    {j \in Jobs : jobState[j] = "running"}

NeedsOverflow(q) ==
    /\ dispatchersAlive
    /\ rateBudget[q] > 0
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
    /\ attempt = [j \in Jobs |-> 0]
    /\ lease = [j \in Jobs |-> 0]
    /\ dbOwner = [j \in Jobs |-> NoWorker]
    /\ taskWorker = [j \in Jobs |-> NoWorker]
    /\ taskLease = [j \in Jobs |-> 0]
    /\ permitHolder = [j \in Jobs |-> NoWorker]
    /\ permitKind = [j \in Jobs |-> "none"]
    /\ inFlight = [j \in Jobs |-> FALSE]
    /\ cancelRequested = [j \in Jobs |-> FALSE]
    /\ heartbeatFresh = [j \in Jobs |-> FALSE]
    /\ deadlineExpired = [j \in Jobs |-> FALSE]
    /\ rateBudget = [q \in Queues |-> RateMax]
    /\ shutdownPhase = "running"
    /\ dispatchersAlive = TRUE
    /\ heartbeatAlive = TRUE
    /\ maintenanceAlive = TRUE

ReserveLocal(w, j) ==
    LET q == WorkerQueue[w] IN
    /\ dispatchersAlive
    /\ jobState[j] = "available"
    /\ permitHolder[j] = NoWorker
    /\ HeldByWorker(w) < BatchMax
    /\ QueueOf[j] = q
    /\ LocalHeld(q) < MinWorkers[q]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = w]
    /\ permitKind' = [permitKind EXCEPT ![j] = "local"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  inFlight, heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ReserveOverflow(w, j) ==
    LET q == WorkerQueue[w] IN
    /\ dispatchersAlive
    /\ jobState[j] = "available"
    /\ permitHolder[j] = NoWorker
    /\ HeldByWorker(w) < BatchMax
    /\ QueueOf[j] = q
    /\ NeedsOverflow(q)
    /\ TotalOverflowHeld < GlobalOverflow
    /\ OverflowHeld(q) < FairShare(q)
    /\ permitHolder' = [permitHolder EXCEPT ![j] = w]
    /\ permitKind' = [permitKind EXCEPT ![j] = "overflow"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  inFlight, heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ReleaseReservation(w, j) ==
    /\ permitHolder[j] = w
    /\ jobState[j] = "available"
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  inFlight, cancelRequested, heartbeatFresh, deadlineExpired,
                  rateBudget, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive>>

ClaimReserved(w, j) ==
    LET q == QueueOf[j] IN
    /\ dispatchersAlive
    /\ jobState[j] = "available"
    /\ permitHolder[j] = w
    /\ permitKind[j] \in {"local", "overflow"}
    /\ attempt[j] < MaxAttempts
    /\ rateBudget[q] > 0
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ attempt' = [attempt EXCEPT ![j] = @ + 1]
    /\ lease' = [lease EXCEPT ![j] = @ + 1]
    /\ rateBudget' = [rateBudget EXCEPT ![q] = @ - 1]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<dbOwner, taskWorker, taskLease, permitHolder, permitKind,
                  inFlight, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive>>

StartExecution(w, j) ==
    /\ jobState[j] = "running"
    /\ permitHolder[j] = w
    /\ dbOwner[j] = NoWorker
    /\ taskWorker[j] = NoWorker
    /\ ~inFlight[j]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = w]
    /\ taskWorker' = [taskWorker EXCEPT ![j] = w]
    /\ taskLease' = [taskLease EXCEPT ![j] = lease[j]]
    /\ inFlight' = [inFlight EXCEPT ![j] = TRUE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = TRUE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, permitHolder, permitKind,
                  cancelRequested, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

HeartbeatPulse(j) ==
    /\ heartbeatAlive
    /\ inFlight[j]
    /\ jobState[j] = "running"
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  deadlineExpired, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

HeartbeatBecomesStale(j) ==
    /\ inFlight[j]
    /\ jobState[j] = "running"
    /\ heartbeatFresh[j]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  deadlineExpired, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

DeadlineExpires(j) ==
    /\ inFlight[j]
    /\ jobState[j] = "running"
    /\ ~deadlineExpired[j]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  heartbeatFresh, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

Finalize(w, j, toState) ==
    /\ toState \in FinalStates
    /\ inFlight[j]
    /\ jobState[j] = "running"
    /\ dbOwner[j] = w
    /\ taskWorker[j] = w
    /\ taskLease[j] = lease[j]
    /\ jobState' = [jobState EXCEPT ![j] = toState]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoWorker]
    /\ taskWorker' = [taskWorker EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![j] = 0]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ inFlight' = [inFlight EXCEPT ![j] = FALSE]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<attempt, lease, rateBudget, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

FinalizeRejected(w, j, toState) ==
    /\ toState \in FinalStates
    /\ inFlight[j]
    /\ taskWorker[j] = w
    /\ (jobState[j] # "running"
        \/ dbOwner[j] # w
        \/ taskLease[j] # lease[j])
    /\ taskWorker' = [taskWorker EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![j] = 0]
    /\ inFlight' = [inFlight EXCEPT ![j] = FALSE]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, permitHolder, permitKind,
                  rateBudget, shutdownPhase, dispatchersAlive,
                  heartbeatAlive, maintenanceAlive>>

RescueByHeartbeat(j) ==
    /\ maintenanceAlive
    /\ inFlight[j]
    /\ jobState[j] = "running"
    /\ ~heartbeatFresh[j]
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoWorker]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<attempt, lease, taskWorker, taskLease, inFlight,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

RescueByDeadline(j) ==
    /\ maintenanceAlive
    /\ inFlight[j]
    /\ jobState[j] = "running"
    /\ deadlineExpired[j]
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoWorker]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<attempt, lease, taskWorker, taskLease, inFlight,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

AdminCancel(j) ==
    /\ jobState[j] \in {"available", "running", "retryable"}
    /\ jobState' = [jobState EXCEPT ![j] = "cancelled"]
    /\ dbOwner' = [dbOwner EXCEPT ![j] = NoWorker]
    /\ permitHolder' = [permitHolder EXCEPT ![j] = NoWorker]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelRequested' =
        IF inFlight[j]
        THEN [cancelRequested EXCEPT ![j] = TRUE]
        ELSE [cancelRequested EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<attempt, lease, taskWorker, taskLease, inFlight,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

PromoteRetryable(j) ==
    /\ jobState[j] = "retryable"
    /\ ~inFlight[j]
    /\ jobState' = [jobState EXCEPT ![j] = "available"]
    /\ cancelRequested' = [cancelRequested EXCEPT ![j] = FALSE]
    /\ heartbeatFresh' = [heartbeatFresh EXCEPT ![j] = FALSE]
    /\ deadlineExpired' = [deadlineExpired EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, rateBudget,
                  shutdownPhase, dispatchersAlive, heartbeatAlive, maintenanceAlive>>

RefillBudget(q, n) ==
    /\ q \in Queues
    /\ n \in 1..RateMax
    /\ rateBudget' = [rateBudget EXCEPT ![q] =
          IF @ + n < RateMax THEN @ + n ELSE RateMax]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  heartbeatFresh, deadlineExpired, shutdownPhase,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

ShutdownBegin ==
    /\ shutdownPhase = "running"
    /\ shutdownPhase' = "stop_claim"
    /\ dispatchersAlive' = FALSE
    /\ maintenanceAlive' = FALSE
    /\ permitHolder' =
        [j \in Jobs |->
            IF jobState[j] = "available" THEN NoWorker ELSE permitHolder[j]]
    /\ permitKind' =
        [j \in Jobs |->
            IF jobState[j] = "available" THEN "none" ELSE permitKind[j]]
    /\ cancelRequested' =
        [j \in Jobs |-> cancelRequested[j] \/ inFlight[j]]
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  inFlight, heartbeatFresh,
                  deadlineExpired, rateBudget, heartbeatAlive>>

EnterDraining ==
    /\ shutdownPhase = "stop_claim"
    /\ shutdownPhase' = "draining"
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

StopHeartbeat ==
    /\ heartbeatAlive
    /\ shutdownPhase = "draining"
    /\ RunningJobs = {}
    /\ InFlightJobs = {}
    /\ heartbeatAlive' = FALSE
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  shutdownPhase, dispatchersAlive, maintenanceAlive>>

FinishShutdown ==
    /\ shutdownPhase = "draining"
    /\ ~heartbeatAlive
    /\ RunningJobs = {}
    /\ InFlightJobs = {}
    /\ TotalHeldPermits = 0
    /\ shutdownPhase' = "stopped"
    /\ UNCHANGED <<jobState, attempt, lease, dbOwner, taskWorker, taskLease,
                  permitHolder, permitKind, inFlight, cancelRequested,
                  heartbeatFresh, deadlineExpired, rateBudget,
                  dispatchersAlive, heartbeatAlive, maintenanceAlive>>

AnyFinalize ==
    \E w \in Workers, j \in Jobs, s \in FinalStates : Finalize(w, j, s)

AnyFinalizeRejected ==
    \E w \in Workers, j \in Jobs, s \in FinalStates : FinalizeRejected(w, j, s)

AnyStartExecution ==
    \E w \in Workers, j \in Jobs : StartExecution(w, j)

AnyPromoteRetryable ==
    \E j \in Jobs : PromoteRetryable(j)

AnyHeartbeatPulse ==
    \E j \in Jobs : HeartbeatPulse(j)

AnyRefill ==
    \E q \in Queues, n \in 1..RateMax : RefillBudget(q, n)

Next ==
    \/ \E w \in Workers, j \in Jobs : ReserveLocal(w, j)
    \/ \E w \in Workers, j \in Jobs : ReserveOverflow(w, j)
    \/ \E w \in Workers, j \in Jobs : ReleaseReservation(w, j)
    \/ \E w \in Workers, j \in Jobs : ClaimReserved(w, j)
    \/ \E w \in Workers, j \in Jobs : StartExecution(w, j)
    \/ AnyFinalize
    \/ AnyFinalizeRejected
    \/ \E j \in Jobs : HeartbeatBecomesStale(j)
    \/ \E j \in Jobs : DeadlineExpires(j)
    \/ AnyHeartbeatPulse
    \/ \E j \in Jobs : RescueByHeartbeat(j)
    \/ \E j \in Jobs : RescueByDeadline(j)
    \/ AnyPromoteRetryable
    \/ AnyRefill
    \/ ShutdownBegin
    \/ EnterDraining
    \/ StopHeartbeat
    \/ FinishShutdown

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ attempt \in [Jobs -> Nat]
    /\ lease \in [Jobs -> Nat]
    /\ dbOwner \in [Jobs -> Workers \cup {NoWorker}]
    /\ taskWorker \in [Jobs -> Workers \cup {NoWorker}]
    /\ taskLease \in [Jobs -> Nat]
    /\ permitHolder \in [Jobs -> Workers \cup {NoWorker}]
    /\ permitKind \in [Jobs -> PermitKinds]
    /\ inFlight \in [Jobs -> BOOLEAN]
    /\ cancelRequested \in [Jobs -> BOOLEAN]
    /\ heartbeatFresh \in [Jobs -> BOOLEAN]
    /\ deadlineExpired \in [Jobs -> BOOLEAN]
    /\ rateBudget \in [Queues -> 0..RateMax]
    /\ shutdownPhase \in {"running", "stop_claim", "draining", "stopped"}
    /\ dispatchersAlive \in BOOLEAN
    /\ heartbeatAlive \in BOOLEAN
    /\ maintenanceAlive \in BOOLEAN

RunningHasPermit ==
    \A j \in Jobs :
        jobState[j] = "running" =>
            permitHolder[j] \in Workers /\ permitKind[j] \in {"local", "overflow"}

InFlightMatchesTaskWorker ==
    \A j \in Jobs :
        inFlight[j] <=> taskWorker[j] \in Workers

InFlightStateConsistent ==
    \A j \in Jobs :
        inFlight[j] =>
            /\ jobState[j] \in {"running", "retryable", "cancelled"}
            /\ taskLease[j] > 0

CurrentOwnerConsistent ==
    \A j \in Jobs :
        dbOwner[j] \in Workers =>
            /\ jobState[j] = "running"
            /\ permitHolder[j] = dbOwner[j]

AttemptAndLeaseMonotone ==
    \A j \in Jobs :
        /\ lease[j] <= attempt[j]
        /\ taskLease[j] <= lease[j]

TerminalReleasesPermit ==
    \A j \in Jobs :
        jobState[j] \in {"completed", "failed", "cancelled", "retryable"} /\ ~inFlight[j]
        => permitHolder[j] = NoWorker /\ permitKind[j] = "none"

LocalCapacitySafe ==
    \A q \in Queues : LocalHeld(q) <= MinWorkers[q]

OverflowCapacitySafe ==
    TotalOverflowHeld <= GlobalOverflow

BatchBounded ==
    \A w \in Workers : HeldByWorker(w) <= BatchMax

RateBudgetBounded ==
    \A q \in Queues : rateBudget[q] \in 0..RateMax

NoClaimAfterStopClaim ==
    shutdownPhase \in {"stop_claim", "draining", "stopped"} => ~dispatchersAlive

HeartbeatUntilDrained ==
    ((shutdownPhase \in {"stop_claim", "draining"}) /\ RunningJobs # {})
    => heartbeatAlive

ServicePhaseConsistency ==
    /\ dispatchersAlive => shutdownPhase = "running"
    /\ maintenanceAlive => shutdownPhase = "running"
    /\ ~heartbeatAlive => shutdownPhase \in {"draining", "stopped"}

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(EnterDraining)
    /\ WF_vars(StopHeartbeat)
    /\ WF_vars(FinishShutdown)
    /\ WF_vars(AnyStartExecution)
    /\ WF_vars(AnyFinalize)
    /\ WF_vars(AnyFinalizeRejected)
    /\ WF_vars(AnyPromoteRetryable)
    /\ WF_vars(AnyHeartbeatPulse)
    /\ WF_vars(AnyRefill)

DrainEventuallyStops ==
    (shutdownPhase = "stop_claim") ~> (shutdownPhase = "stopped")

Q2OverflowProgress ==
    NeedsOverflow("q2") ~> (OverflowHeld("q2") > 0 \/ ~NeedsOverflow("q2"))

====

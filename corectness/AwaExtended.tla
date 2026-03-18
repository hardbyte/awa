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
NoOwner == "no_owner"

VARIABLES jobState, owner, permitKind, cancelFlag, demand, shutdownPhase, heartbeatMode

vars == <<jobState, owner, permitKind, cancelFlag, demand, shutdownPhase, heartbeatMode>>

RECURSIVE SumSeq(_)
SumSeq(seq) ==
    IF Len(seq) = 0
    THEN 0
    ELSE seq[1] + SumSeq(SubSeq(seq, 2, Len(seq)))

JobsInQueue(q) == {j \in Jobs : QueueOf[j] = q}

AvailableInQueue(q) ==
    \E j \in JobsInQueue(q) : jobState[j] = "available"

LocalRunning(q) ==
    Cardinality({j \in JobsInQueue(q) : jobState[j] = "running" /\ permitKind[j] = "local"})

OverflowRunning(q) ==
    Cardinality({j \in JobsInQueue(q) : jobState[j] = "running" /\ permitKind[j] = "overflow"})

TotalOverflowRunning ==
    Cardinality({j \in Jobs : jobState[j] = "running" /\ permitKind[j] = "overflow"})

Contending(q) ==
    demand[q] > 0 \/ OverflowRunning(q) > 0

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
    /\ owner = [j \in Jobs |-> NoOwner]
    /\ permitKind = [j \in Jobs |-> "none"]
    /\ cancelFlag = [w \in Workers |-> FALSE]
    /\ demand = [q \in Queues |-> 0]
    /\ shutdownPhase = "running"
    /\ heartbeatMode = "alive"

SetDemand(q, d) ==
    /\ q \in Queues
    /\ d \in 0..MaxDemand
    /\ ~AvailableInQueue(q) => d = 0
    /\ demand' = [demand EXCEPT ![q] = d]
    /\ UNCHANGED <<jobState, owner, permitKind, cancelFlag, shutdownPhase, heartbeatMode>>

ClaimLocal(w, j) ==
    LET q == WorkerQueue[w] IN
    /\ shutdownPhase = "running"
    /\ jobState[j] = "available"
    /\ QueueOf[j] = q
    /\ LocalRunning(q) < MinWorkers[q]
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ owner' = [owner EXCEPT ![j] = w]
    /\ permitKind' = [permitKind EXCEPT ![j] = "local"]
    /\ UNCHANGED <<cancelFlag, demand, shutdownPhase, heartbeatMode>>

ClaimOverflow(w, j) ==
    LET q == WorkerQueue[w] IN
    /\ shutdownPhase = "running"
    /\ heartbeatMode = "alive"
    /\ jobState[j] = "available"
    /\ QueueOf[j] = q
    /\ LocalRunning(q) >= MinWorkers[q]
    /\ demand[q] > 0
    /\ TotalOverflowRunning < GlobalOverflow
    /\ OverflowRunning(q) < FairShare(q)
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ owner' = [owner EXCEPT ![j] = w]
    /\ permitKind' = [permitKind EXCEPT ![j] = "overflow"]
    /\ UNCHANGED <<cancelFlag, demand, shutdownPhase, heartbeatMode>>

Finalize(w, j, toState) ==
    /\ toState \in FinalStates
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ jobState' = [jobState EXCEPT ![j] = toState]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ UNCHANGED <<cancelFlag, demand, shutdownPhase, heartbeatMode>>

Rescue(j) ==
    LET w == owner[j] IN
    /\ jobState[j] = "running"
    /\ w \in Workers
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ permitKind' = [permitKind EXCEPT ![j] = "none"]
    /\ cancelFlag' = [x \in Workers |-> cancelFlag[x] \/ x = w]
    /\ UNCHANGED <<demand, shutdownPhase, heartbeatMode>>

AdminCancel(j) ==
    LET w == owner[j] IN
    /\ jobState[j] \in {"available", "running", "retryable"}
    /\ jobState' = [jobState EXCEPT ![j] = "cancelled"]
    /\ owner' =
        IF jobState[j] = "running"
        THEN [owner EXCEPT ![j] = NoOwner]
        ELSE owner
    /\ permitKind' =
        IF jobState[j] = "running"
        THEN [permitKind EXCEPT ![j] = "none"]
        ELSE permitKind
    /\ cancelFlag' =
        IF jobState[j] = "running"
        THEN [x \in Workers |-> cancelFlag[x] \/ x = w]
        ELSE cancelFlag
    /\ UNCHANGED <<demand, shutdownPhase, heartbeatMode>>

ShutdownBegin ==
    /\ shutdownPhase = "running"
    /\ shutdownPhase' = "stop_claim"
    /\ UNCHANGED <<jobState, owner, permitKind, cancelFlag, demand, heartbeatMode>>

EnterDraining ==
    /\ shutdownPhase = "stop_claim"
    /\ shutdownPhase' = "draining"
    /\ UNCHANGED <<jobState, owner, permitKind, cancelFlag, demand, heartbeatMode>>

StopHeartbeat ==
    /\ heartbeatMode = "alive"
    /\ shutdownPhase \in {"draining", "stopped"}
    /\ \A j \in Jobs : jobState[j] # "running"
    /\ heartbeatMode' = "stopped"
    /\ UNCHANGED <<jobState, owner, permitKind, cancelFlag, demand, shutdownPhase>>

FinishShutdown ==
    /\ shutdownPhase = "draining"
    /\ heartbeatMode = "stopped"
    /\ \A j \in Jobs : jobState[j] # "running"
    /\ shutdownPhase' = "stopped"
    /\ UNCHANGED <<jobState, owner, permitKind, cancelFlag, demand, heartbeatMode>>

Stutter ==
    UNCHANGED vars

Next ==
    \/ \E q \in Queues, d \in 0..MaxDemand : SetDemand(q, d)
    \/ \E w \in Workers, j \in Jobs : ClaimLocal(w, j)
    \/ \E w \in Workers, j \in Jobs : ClaimOverflow(w, j)
    \/ \E w \in Workers, j \in Jobs, s \in FinalStates : Finalize(w, j, s)
    \/ \E j \in Jobs : Rescue(j)
    \/ \E j \in Jobs : AdminCancel(j)
    \/ ShutdownBegin
    \/ EnterDraining
    \/ StopHeartbeat
    \/ FinishShutdown
    \/ Stutter

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ owner \in [Jobs -> Workers \cup {NoOwner}]
    /\ permitKind \in [Jobs -> PermitKinds]
    /\ cancelFlag \in [Workers -> BOOLEAN]
    /\ demand \in [Queues -> 0..MaxDemand]
    /\ shutdownPhase \in {"running", "stop_claim", "draining", "stopped"}
    /\ heartbeatMode \in {"alive", "stopped"}

RunningOwned ==
    \A j \in Jobs : jobState[j] = "running" => owner[j] \in Workers

NonRunningUnowned ==
    \A j \in Jobs : jobState[j] # "running" => owner[j] = NoOwner

RunningHasPermit ==
    \A j \in Jobs : jobState[j] = "running" => permitKind[j] \in {"local", "overflow"}

NonRunningHasNoPermit ==
    \A j \in Jobs : jobState[j] # "running" => permitKind[j] = "none"

LocalCapacitySafe ==
    \A q \in Queues : LocalRunning(q) <= MinWorkers[q]

OverflowCapacitySafe ==
    TotalOverflowRunning <= GlobalOverflow

NoClaimAfterStopClaim ==
    shutdownPhase \in {"stop_claim", "draining", "stopped"}
    => \A j \in Jobs : jobState[j] = "available" \/ jobState[j] # "available"

HeartbeatUntilDrained ==
    ((shutdownPhase = "stop_claim")
      \/ (shutdownPhase = "draining" /\ \E j \in Jobs : jobState[j] = "running"))
    => heartbeatMode = "alive"

Spec == Init /\ [][Next]_vars

====

---- MODULE AwaCore ----
EXTENDS FiniteSets, Naturals

Jobs == {"j1", "j2"}
Workers == {"w1"}
Queues == {"q1"}
QueueOf == [j \in Jobs |-> "q1"]
WorkerQueue == [w \in Workers |-> "q1"]

JobStates == {"available", "running", "retryable", "completed", "failed", "cancelled"}
FinalStates == {"retryable", "completed", "failed", "cancelled"}
NoOwner == "no_owner"

VARIABLES jobState, owner, lease, taskLease, cancelFlag, shutdownPhase

vars == <<jobState, owner, lease, taskLease, cancelFlag, shutdownPhase>>

Init ==
    /\ jobState = [j \in Jobs |-> "available"]
    /\ owner = [j \in Jobs |-> NoOwner]
    /\ lease = [j \in Jobs |-> 0]
    /\ taskLease = [w \in Workers |-> [j \in Jobs |-> 0]]
    /\ cancelFlag = [w \in Workers |-> FALSE]
    /\ shutdownPhase = "running"

Claim(w, j) ==
    /\ shutdownPhase = "running"
    /\ jobState[j] = "available"
    /\ QueueOf[j] = WorkerQueue[w]
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ owner' = [owner EXCEPT ![j] = w]
    /\ lease' = [lease EXCEPT ![j] = @ + 1]
    /\ UNCHANGED <<taskLease, cancelFlag, shutdownPhase>>

StartTask(w, j) ==
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ taskLease[w][j] = 0
    /\ taskLease' = [taskLease EXCEPT ![w][j] = lease[j]]
    /\ UNCHANGED <<jobState, owner, lease, cancelFlag, shutdownPhase>>

FinalizeAccepted(w, j, toState) ==
    /\ toState \in FinalStates
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ taskLease[w][j] = lease[j]
    /\ jobState' = [jobState EXCEPT ![j] = toState]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<lease, cancelFlag, shutdownPhase>>

FinalizeRejected(w, j, toState) ==
    /\ toState \in FinalStates
    /\ taskLease[w][j] > 0
    /\ (jobState[j] # "running"
        \/ owner[j] # w
        \/ taskLease[w][j] # lease[j])
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<jobState, owner, lease, cancelFlag, shutdownPhase>>

Rescue(j) ==
    LET w == owner[j] IN
    /\ jobState[j] = "running"
    /\ w \in Workers
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ cancelFlag' = [x \in Workers |-> cancelFlag[x] \/ x = w]
    /\ UNCHANGED <<lease, taskLease, shutdownPhase>>

Promote(j) ==
    /\ jobState[j] = "retryable"
    /\ jobState' = [jobState EXCEPT ![j] = "available"]
    /\ UNCHANGED <<owner, lease, taskLease, cancelFlag, shutdownPhase>>

AdminCancel(j) ==
    LET w == owner[j] IN
    /\ jobState[j] \in {"available", "running", "retryable"}
    /\ jobState' = [jobState EXCEPT ![j] = "cancelled"]
    /\ owner' =
        IF jobState[j] = "running"
        THEN [owner EXCEPT ![j] = NoOwner]
        ELSE owner
    /\ cancelFlag' =
        IF jobState[j] = "running"
        THEN [x \in Workers |-> cancelFlag[x] \/ x = w]
        ELSE cancelFlag
    /\ UNCHANGED <<lease, taskLease, shutdownPhase>>

ShutdownBegin ==
    /\ shutdownPhase = "running"
    /\ shutdownPhase' = "stop_claim"
    /\ UNCHANGED <<jobState, owner, lease, taskLease, cancelFlag>>

EnterDraining ==
    /\ shutdownPhase = "stop_claim"
    /\ shutdownPhase' = "draining"
    /\ UNCHANGED <<jobState, owner, lease, taskLease, cancelFlag>>

FinishShutdown ==
    /\ shutdownPhase = "draining"
    /\ \A j \in Jobs : jobState[j] # "running"
    /\ shutdownPhase' = "stopped"
    /\ UNCHANGED <<jobState, owner, lease, taskLease, cancelFlag>>

Stutter ==
    UNCHANGED vars

Next ==
    \/ \E w \in Workers, j \in Jobs : Claim(w, j)
    \/ \E w \in Workers, j \in Jobs : StartTask(w, j)
    \/ \E w \in Workers, j \in Jobs, s \in FinalStates : FinalizeAccepted(w, j, s)
    \/ \E w \in Workers, j \in Jobs, s \in FinalStates : FinalizeRejected(w, j, s)
    \/ \E j \in Jobs : Rescue(j)
    \/ \E j \in Jobs : Promote(j)
    \/ \E j \in Jobs : AdminCancel(j)
    \/ ShutdownBegin
    \/ EnterDraining
    \/ FinishShutdown
    \/ Stutter

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ owner \in [Jobs -> Workers \cup {NoOwner}]
    /\ lease \in [Jobs -> Nat]
    /\ taskLease \in [Workers -> [Jobs -> Nat]]
    /\ cancelFlag \in [Workers -> BOOLEAN]
    /\ shutdownPhase \in {"running", "stop_claim", "draining", "stopped"}

RunningOwned ==
    \A j \in Jobs : jobState[j] = "running" => owner[j] \in Workers

NonRunningUnowned ==
    \A j \in Jobs : jobState[j] # "running" => owner[j] = NoOwner

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs :
        taskLease[w][j] <= lease[j]

Spec == Init /\ [][Next]_vars

====

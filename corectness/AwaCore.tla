---- MODULE AwaCore ----
EXTENDS FiniteSets

Jobs == {"j1", "j2"}
Workers == {"w1"}
Queues == {"q1"}
QueueOf == [j \in Jobs |-> "q1"]
WorkerQueue == [w \in Workers |-> "q1"]

JobStates == {"available", "running", "retryable", "completed", "failed", "cancelled"}
FinalStates == {"retryable", "completed", "failed", "cancelled"}
NoOwner == "no_owner"

VARIABLES jobState, owner, cancelFlag, shutdownPhase

vars == <<jobState, owner, cancelFlag, shutdownPhase>>

Init ==
    /\ jobState = [j \in Jobs |-> "available"]
    /\ owner = [j \in Jobs |-> NoOwner]
    /\ cancelFlag = [w \in Workers |-> FALSE]
    /\ shutdownPhase = "running"

Claim(w, j) ==
    /\ shutdownPhase = "running"
    /\ jobState[j] = "available"
    /\ QueueOf[j] = WorkerQueue[w]
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ owner' = [owner EXCEPT ![j] = w]
    /\ UNCHANGED <<cancelFlag, shutdownPhase>>

Finalize(w, j, toState) ==
    /\ toState \in FinalStates
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ jobState' = [jobState EXCEPT ![j] = toState]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ UNCHANGED <<cancelFlag, shutdownPhase>>

Rescue(j) ==
    LET w == owner[j] IN
    /\ jobState[j] = "running"
    /\ w \in Workers
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ cancelFlag' = [x \in Workers |-> cancelFlag[x] \/ x = w]
    /\ UNCHANGED shutdownPhase

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
    /\ UNCHANGED shutdownPhase

ShutdownBegin ==
    /\ shutdownPhase = "running"
    /\ shutdownPhase' = "stop_claim"
    /\ UNCHANGED <<jobState, owner, cancelFlag>>

EnterDraining ==
    /\ shutdownPhase = "stop_claim"
    /\ shutdownPhase' = "draining"
    /\ UNCHANGED <<jobState, owner, cancelFlag>>

FinishShutdown ==
    /\ shutdownPhase = "draining"
    /\ \A j \in Jobs : jobState[j] # "running"
    /\ shutdownPhase' = "stopped"
    /\ UNCHANGED <<jobState, owner, cancelFlag>>

Stutter ==
    UNCHANGED vars

Next ==
    \/ \E w \in Workers, j \in Jobs : Claim(w, j)
    \/ \E w \in Workers, j \in Jobs, s \in FinalStates : Finalize(w, j, s)
    \/ \E j \in Jobs : Rescue(j)
    \/ \E j \in Jobs : AdminCancel(j)
    \/ ShutdownBegin
    \/ EnterDraining
    \/ FinishShutdown
    \/ Stutter

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ owner \in [Jobs -> Workers \cup {NoOwner}]
    /\ cancelFlag \in [Workers -> BOOLEAN]
    /\ shutdownPhase \in {"running", "stop_claim", "draining", "stopped"}

RunningOwned ==
    \A j \in Jobs : jobState[j] = "running" => owner[j] \in Workers

NonRunningUnowned ==
    \A j \in Jobs : jobState[j] # "running" => owner[j] = NoOwner

NoClaimAfterStopClaim ==
    shutdownPhase \in {"stop_claim", "draining", "stopped"}
    => \A j \in Jobs : jobState[j] = "running" => TRUE

Spec == Init /\ [][Next]_vars

====

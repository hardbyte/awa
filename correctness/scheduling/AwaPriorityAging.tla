---- MODULE AwaPriorityAging ----
EXTENDS Naturals, TLC

\* Focused scheduling/liveness model for ADR-005.
\*
\* One queue, one worker, two logical jobs:
\* - hi: recurring highest-priority work
\* - lo: long-waiting low-priority work
\*
\* The model checks that strict priority ordering is preserved, but maintenance-
\* based aging still ensures the low-priority job eventually gets claimed.

Jobs == {"hi", "lo"}
JobStates == {"available", "running", "completed"}
Priorities == 1..4
NoJob == "none"
JobRank == [j \in Jobs |-> IF j = "lo" THEN 1 ELSE 2]

MaxHighRecycles == 3
MaxEnqueueOrder == 6

VARIABLES jobState,
          priority,
          enqueueOrder,
          nextEnqueueOrder,
          highRecycles,
          running,
          lastClaimed

vars == <<jobState, priority, enqueueOrder, nextEnqueueOrder, highRecycles, running, lastClaimed>>

LexBefore(j, k) ==
    \/ priority[j] < priority[k]
    \/ /\ priority[j] = priority[k]
       /\ enqueueOrder[j] < enqueueOrder[k]
    \/ /\ priority[j] = priority[k]
       /\ enqueueOrder[j] = enqueueOrder[k]
       /\ JobRank[j] < JobRank[k]

Preferred(j) ==
    /\ jobState[j] = "available"
    /\ \A k \in Jobs : jobState[k] = "available" => (j = k \/ LexBefore(j, k))

Init ==
    /\ jobState = [j \in Jobs |-> "available"]
    /\ priority = [j \in Jobs |-> IF j = "hi" THEN 1 ELSE 4]
    /\ enqueueOrder = [j \in Jobs |-> IF j = "hi" THEN 2 ELSE 1]
    /\ nextEnqueueOrder = 3
    /\ highRecycles = 0
    /\ running = NoJob
    /\ lastClaimed = NoJob

ClaimHi ==
    /\ running = NoJob
    /\ Preferred("hi")
    /\ jobState' = [jobState EXCEPT !["hi"] = "running"]
    /\ running' = "hi"
    /\ lastClaimed' = "hi"
    /\ UNCHANGED <<priority, enqueueOrder, nextEnqueueOrder, highRecycles>>

ClaimLo ==
    /\ running = NoJob
    /\ Preferred("lo")
    /\ jobState' = [jobState EXCEPT !["lo"] = "running"]
    /\ running' = "lo"
    /\ lastClaimed' = "lo"
    /\ UNCHANGED <<priority, enqueueOrder, nextEnqueueOrder, highRecycles>>

CompleteHi ==
    /\ running = "hi"
    /\ jobState' = [jobState EXCEPT !["hi"] = "completed"]
    /\ running' = NoJob
    /\ UNCHANGED <<priority, enqueueOrder, nextEnqueueOrder, highRecycles, lastClaimed>>

CompleteLo ==
    /\ running = "lo"
    /\ jobState' = [jobState EXCEPT !["lo"] = "completed"]
    /\ running' = NoJob
    /\ UNCHANGED <<priority, enqueueOrder, nextEnqueueOrder, highRecycles, lastClaimed>>

RecycleHi ==
    /\ jobState["hi"] = "completed"
    /\ highRecycles < MaxHighRecycles
    /\ nextEnqueueOrder <= MaxEnqueueOrder
    /\ jobState' = [jobState EXCEPT !["hi"] = "available"]
    /\ priority' = [priority EXCEPT !["hi"] = 1]
    /\ enqueueOrder' = [enqueueOrder EXCEPT !["hi"] = nextEnqueueOrder]
    /\ nextEnqueueOrder' = nextEnqueueOrder + 1
    /\ highRecycles' = highRecycles + 1
    /\ UNCHANGED <<running, lastClaimed>>

RecycleHiForever ==
    /\ jobState["hi"] = "completed"
    /\ jobState' = [jobState EXCEPT !["hi"] = "available"]
    /\ priority' = [priority EXCEPT !["hi"] = 1]
    /\ UNCHANGED <<enqueueOrder, nextEnqueueOrder, highRecycles, running, lastClaimed>>

AgeLow ==
    /\ jobState["lo"] = "available"
    /\ priority["lo"] > 1
    /\ priority' = [priority EXCEPT !["lo"] = @ - 1]
    /\ UNCHANGED <<jobState, enqueueOrder, nextEnqueueOrder, highRecycles, running, lastClaimed>>

Stutter ==
    /\ UNCHANGED vars

Next ==
    \/ ClaimHi
    \/ ClaimLo
    \/ CompleteHi
    \/ CompleteLo
    \/ RecycleHi
    \/ RecycleHiForever
    \/ AgeLow
    \/ Stutter

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(ClaimHi)
    /\ WF_vars(ClaimLo)
    /\ WF_vars(CompleteHi)
    /\ WF_vars(CompleteLo)
    /\ WF_vars(RecycleHi)
    /\ WF_vars(AgeLow)

NoAgingFairSpec ==
    Spec
    /\ WF_vars(ClaimHi)
    /\ WF_vars(ClaimLo)
    /\ WF_vars(CompleteHi)
    /\ WF_vars(CompleteLo)
    /\ WF_vars(RecycleHiForever)

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ priority \in [Jobs -> Priorities]
    /\ enqueueOrder \in [Jobs -> 1..MaxEnqueueOrder]
    /\ nextEnqueueOrder \in 1..(MaxEnqueueOrder + 1)
    /\ highRecycles \in 0..MaxHighRecycles
    /\ running \in Jobs \cup {NoJob}
    /\ lastClaimed \in Jobs \cup {NoJob}

SingleRunner ==
    /\ running = NoJob \/ jobState[running] = "running"
    /\ \A j \in Jobs : jobState[j] = "running" => j = running

PriorityRange == priority["hi"] = 1 /\ priority["lo"] \in Priorities

OrderConsistent ==
    /\ jobState["hi"] = "available" /\ jobState["lo"] = "available" /\ priority["hi"] < priority["lo"]
       => lastClaimed # "lo"

LowEventuallyLeavesAvailable == <>(jobState["lo"] \in {"running", "completed"})

LowEventuallyAgedOrClaimed == <>(priority["lo"] = 1 \/ jobState["lo"] \in {"running", "completed"})

=============================================================================

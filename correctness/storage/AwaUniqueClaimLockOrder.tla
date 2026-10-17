---- MODULE AwaUniqueClaimLockOrder ----
EXTENDS TLC, FiniteSets, Sequences, Naturals

CONSTANTS Scenario, Broken

TxIds == {"producer", "other"}
Resources == {"unique:a", "unique:b", "enqueue_lane", "queue_ring_state", "done_child"}

ProducerPlan ==
    IF Scenario = "multiple_keys"
    THEN <<"unique:a", "unique:b">>
    ELSE IF Scenario = "counter_rebuild"
    THEN <<"queue_ring_state", "done_child">>
    ELSE <<"unique:a", "enqueue_lane">>

OtherPlan ==
    IF Scenario = "multiple_keys"
    THEN IF Broken THEN <<"unique:b", "unique:a">>
         ELSE <<"unique:a", "unique:b">>
    ELSE IF Scenario = "counter_rebuild"
    THEN IF Broken THEN <<"done_child", "queue_ring_state">>
         ELSE <<"queue_ring_state", "done_child">>
    ELSE IF Broken THEN <<"enqueue_lane", "unique:a">>
         ELSE <<"unique:a", "enqueue_lane">>

Plan(t) == IF t = "producer" THEN ProducerPlan ELSE OtherPlan

VARIABLES position, held, finished
vars == <<position, held, finished>>

Init ==
    /\ position = [t \in TxIds |-> 1]
    /\ held = [t \in TxIds |-> {}]
    /\ finished = [t \in TxIds |-> FALSE]

Acquire(t) ==
    /\ ~finished[t]
    /\ position[t] <= Len(Plan(t))
    /\ \A other \in TxIds \ {t} : Plan(t)[position[t]] \notin held[other]
    /\ held' = [held EXCEPT ![t] = @ \cup {Plan(t)[position[t]]}]
    /\ position' = [position EXCEPT ![t] = @ + 1]
    /\ UNCHANGED finished

Commit(t) ==
    /\ ~finished[t]
    /\ position[t] > Len(Plan(t))
    /\ held' = [held EXCEPT ![t] = {}]
    /\ finished' = [finished EXCEPT ![t] = TRUE]
    /\ UNCHANGED position

Next == (\E t \in TxIds : Acquire(t) \/ Commit(t)) \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars

WaitingFor(t, other) ==
    /\ ~finished[t]
    /\ position[t] <= Len(Plan(t))
    /\ Plan(t)[position[t]] \in held[other]

NoDeadlock ==
    ~(WaitingFor("producer", "other") /\ WaitingFor("other", "producer"))

TypeOK ==
    /\ Scenario \in {"unique_lane", "multiple_keys", "counter_rebuild"}
    /\ Broken \in BOOLEAN
    /\ \A t \in TxIds :
         /\ position[t] \in 1..(Len(Plan(t)) + 1)
         /\ held[t] \subseteq Resources
         /\ finished[t] \in BOOLEAN

====

------------------------ MODULE AwaCronDefinitions ------------------------
EXTENDS Naturals, FiniteSets
\* Existing schedule definitions during rolling manifest changes. Each publish
\* and reconciliation step is atomic under cron_protocol_lock. Capability and
\* retirement gates are checked separately by AwaCronOwnership and SQL tests.
CONSTANT AgreementGate
Instances == {"a", "b"}
Revisions == {1, 2}
VARIABLES declared, definition
vars == <<declared, definition>>
Participants(d) == {i \in Instances : d[i] # 0}
Consensus(d) == /\ Participants(d) # {}
                /\ \A i,j \in Participants(d) : d[i] = d[j]
Wanted(d) == UNION {{d[i]} : i \in Participants(d)}
Init == /\ declared = [i \in Instances |-> IF i = "a" THEN 1 ELSE 0]
        /\ definition = 1
Publish(i,r) ==
 /\ declared' = [declared EXCEPT ![i] = r]
 /\ definition' = IF ~AgreementGate \/ Consensus(declared') THEN r ELSE definition
Expire(i) == /\ declared' = [declared EXCEPT ![i] = 0]
             /\ UNCHANGED definition
Reconcile == /\ UNCHANGED declared
             /\ definition' = IF Consensus(declared)
                              THEN CHOOSE r \in Wanted(declared) : TRUE
                              ELSE definition
Next == \/ \E i \in Instances,r \in Revisions : Publish(i,r)
        \/ \E i \in Instances : Expire(i)
        \/ Reconcile
Spec == Init /\ [][Next]_vars
TypeOK == /\ declared \in [Instances -> ({0} \cup Revisions)]
          /\ definition \in Revisions
DefinitionChangesRequireAgreement ==
 [][definition' # definition => (Consensus(declared') /\ definition' \in Wanted(declared'))]_vars
StableInit == /\ declared = [i \in Instances |-> 2] /\ definition = 1
StableSpec == StableInit /\ [][Reconcile]_vars /\ WF_vars(Reconcile)
ConvergedEventuallyActivates == <> (definition = 2)
=============================================================================

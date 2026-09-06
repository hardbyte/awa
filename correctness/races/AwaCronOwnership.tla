------------------------- MODULE AwaCronOwnership -------------------------
EXTENDS Naturals, FiniteSets, TLC
\* Companion to AwaCron: each Publish/Reconcile step is one transaction under
\* cron_protocol_lock, including old-runtime snapshot writes via the trigger.
\* Evaluation is intentionally outside this lock and races retirement by row CAS.
CONSTANT FenceEnabled
Owners == {"billing", "mail"}
Instances == {"old", "newA", "newB"}
Names == {"invoice", "reminder"}
OwnerOf(n) == IF n = "invoice" THEN "billing" ELSE "mail"
VARIABLES live, capable, declared, owner, desired, agreed, retired, fired, phase
vars == <<live, capable, declared, owner, desired, agreed, retired, fired, phase>>
Init ==
 /\ live = {}
 /\ capable = {}
 /\ declared = {}
 /\ owner = [i \in Instances |-> "billing"]
 /\ desired = [i \in Instances |-> {}]
 /\ agreed = [o \in Owners |-> 0]
 /\ retired = {}
 /\ fired = {}
 /\ phase = 0
Participants(o) == {i \in live \cap declared : owner[i] = o}
Consensus(o) == /\ Participants(o) # {}
                /\ live \subseteq capable
                /\ \A i,j \in Participants(o) : desired[i] = desired[j]
Wanted(o) == UNION {desired[i] : i \in Participants(o)}
Publish(i,o,s,auth,support) ==
 /\ live' = live \cup {i}
 /\ capable' = IF support THEN capable \cup {i} ELSE capable \ {i}
 /\ declared' = IF auth THEN declared \cup {i} ELSE declared \ {i}
 /\ owner' = [owner EXCEPT ![i] = o]
 /\ desired' = [desired EXCEPT ![i] = s]
 \* Conservatively restart grace on changed declarations and legacy evidence.
 /\ agreed' = IF /\ i \in live /\ support /\ i \in capable
                   /\ auth = (i \in declared) /\ owner[i] = o /\ desired[i] = s
               THEN agreed ELSE [o2 \in Owners |-> 0]
 /\ UNCHANGED <<retired,fired,phase>>
Expire(i) ==
 /\ i \in live
 /\ live' = live \ {i}
 \* A missing evidence window resets grace; outage is never desired empty.
 /\ agreed' = [o \in Owners |-> 0]
 /\ UNCHANGED <<capable,declared,owner,desired,retired,fired,phase>>
Observe(o) ==
 /\ agreed' = [agreed EXCEPT ![o] = IF Consensus(o) THEN 1 ELSE 0]
 /\ UNCHANGED <<live,capable,declared,owner,desired,retired,fired,phase>>
Reconcile(o) ==
 /\ Consensus(o) /\ agreed[o] = 1
 /\ retired' = retired \cup {n \in Names : OwnerOf(n) = o /\ n \notin Wanted(o)}
 /\ UNCHANGED <<live,capable,declared,owner,desired,agreed,fired,phase>>
\* A legacy UPSERT never clears the retirement tombstone.
LegacyUpsert == UNCHANGED vars
Enqueue(i,n) ==
 /\ i \in live /\ phase = 0
 /\ (FenceEnabled => n \notin retired)
 /\ fired' = fired \cup {n}
 /\ phase' = 1
 /\ UNCHANGED <<live,capable,declared,owner,desired,agreed,retired>>
ResetFire == /\ phase = 1 /\ phase' = 0 /\ fired' = {}
             /\ UNCHANGED <<live,capable,declared,owner,desired,agreed,retired>>
Next == \/ \E i \in Instances,o \in Owners,s \in SUBSET Names,a,b \in BOOLEAN: Publish(i,o,s,a,b)
        \/ \E i \in Instances: Expire(i)
        \/ \E o \in Owners: Observe(o) \/ Reconcile(o)
        \/ \E i \in Instances,n \in Names: Enqueue(i,n)
        \/ ResetFire \/ LegacyUpsert
Spec == Init /\ [][Next]_vars
TypeOK == /\ live \subseteq Instances /\ retired \subseteq Names
          /\ agreed \in [Owners -> {0,1}]
RetirementIsDurable == [][retired \subseteq retired']_vars
RetiredCannotFire == [][\A n \in retired : n \notin (fired' \ fired)]_vars
\* Liveness only in a stable capable fleet; outage/rollout fairness is not assumed.
ConvergedInit ==
 /\ live = {"newA"}
 /\ capable = {"newA"}
 /\ declared = {"newA"}
 /\ owner = [i \in Instances |-> "billing"]
 /\ desired = [i \in Instances |-> {}]
 /\ agreed = [o \in Owners |-> 0]
 /\ retired = {} /\ fired = {} /\ phase = 0
StableNext == Observe("billing") \/ Reconcile("billing")
StableSpec == ConvergedInit /\ [][StableNext]_vars
              /\ WF_vars(Observe("billing")) /\ WF_vars(Reconcile("billing"))
ConvergedEventuallyRetires == <> ("invoice" \in retired)
=============================================================================

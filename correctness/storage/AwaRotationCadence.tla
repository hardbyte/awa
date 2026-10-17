---- MODULE AwaRotationCadence ----
EXTENDS Naturals, Sequences, TLC

CONSTANTS BrokenEmptyGate, BrokenClock

Slots == 0..2
MinAge == 2
TargetBytes == 2
MaxBytes == 3
NextSlot(slot) == (slot + 1) % 3
Min(left, right) == IF left < right THEN left ELSE right

VARIABLES current, history, rows, bytes, seen, age, draining,
          waitedOnEmpty, sealedYoung
vars == <<current, history, rows, bytes, seen, age, draining,
          waitedOnEmpty, sealedYoung>>

Init ==
    /\ current = 0
    /\ history = <<0>>
    /\ rows = [slot \in Slots |-> FALSE]
    /\ bytes = [slot \in Slots |-> 0]
    /\ seen = FALSE
    /\ age = 0
    /\ draining = FALSE
    /\ waitedOnEmpty = FALSE
    /\ sealedYoung = FALSE

Enqueue ==
    /\ ~draining
    /\ bytes[current] < MaxBytes
    /\ \E batch \in 1..MaxBytes :
         bytes' = [bytes EXCEPT ![current] = Min(@ + batch, MaxBytes)]
    /\ rows' = [rows EXCEPT ![current] = TRUE]
    /\ UNCHANGED <<current, history, seen, age, draining, waitedOnEmpty, sealedYoung>>

Delete ==
    /\ \E slot \in Slots :
         /\ rows[slot]
         /\ rows' = [rows EXCEPT ![slot] = FALSE]
    /\ UNCHANGED <<current, history, bytes, seen, age, draining, waitedOnEmpty, sealedYoung>>

NewLeader ==
    /\ ~draining
    /\ seen' = FALSE
    /\ age' = 0
    /\ UNCHANGED <<current, history, rows, bytes, draining, waitedOnEmpty, sealedYoung>>

BeginDrain ==
    /\ ~draining
    /\ draining' = TRUE
    /\ UNCHANGED <<current, history, rows, bytes, seen, age, waitedOnEmpty, sealedYoung>>

Tick ==
    LET occupied == IF BrokenEmptyGate THEN bytes[current] > 0 ELSE rows[current]
        observedAge == IF ~occupied THEN 0 ELSE IF seen THEN Min(age + 1, MinAge) ELSE 0
        eligibleAge == IF BrokenClock THEN MinAge ELSE observedAge
        waiting == occupied /\ eligibleAge < MinAge /\ bytes[current] < TargetBytes
        idle == \A slot \in Slots : bytes[slot] = 0
        rotate == ~idle /\ ~waiting /\ ~rows[NextSlot(current)]
    IN
    /\ current' = IF rotate THEN NextSlot(current) ELSE current
    /\ history' = IF rotate
                   THEN Append(IF Len(history) = 3 THEN Tail(history) ELSE history,
                               NextSlot(current))
                   ELSE history
    /\ seen' = IF rotate THEN FALSE ELSE occupied
    /\ age' = IF rotate THEN 0 ELSE observedAge
    /\ waitedOnEmpty' = (waiting /\ ~rows[current])
    /\ sealedYoung' = (rotate /\ rows[current] /\ observedAge < MinAge
                       /\ bytes[current] < TargetBytes)
    /\ UNCHANGED <<rows, bytes, draining>>

Prune ==
    /\ Len(history) > 1
    /\ history[1] # current
    /\ bytes[history[1]] > 0
    /\ rows' = [rows EXCEPT ![history[1]] = FALSE]
    /\ bytes' = [bytes EXCEPT ![history[1]] = 0]
    /\ UNCHANGED <<current, history, seen, age, draining, waitedOnEmpty, sealedYoung>>

Next == Enqueue \/ Delete \/ NewLeader \/ BeginDrain \/ Tick \/ Prune \/ UNCHANGED vars
\* Drain assumes stable leadership, already-prunable jobs, continuing ticks,
\* and eventual successful prune opportunities. Rotation moves the oldest
\* target, so prune may be enabled intermittently and needs strong fairness.
Spec == Init /\ [][Next]_vars /\ WF_vars(Tick) /\ SF_vars(Prune)

TypeOK ==
    /\ current \in Slots
    /\ history \in Seq(Slots)
    /\ Len(history) \in 1..3
    /\ rows \in [Slots -> BOOLEAN]
    /\ bytes \in [Slots -> 0..MaxBytes]
    /\ seen \in BOOLEAN
    /\ age \in 0..MinAge
    /\ draining \in BOOLEAN

EmptyNeverWaits == ~waitedOnEmpty
YoungSmallSlotStaysOpen == ~sealedYoung
DrainCompletes == draining ~> (\A slot \in Slots : bytes[slot] = 0)

TargetIsHardCap == \A slot \in Slots : bytes[slot] <= TargetBytes

====

---- MODULE AwaQuiescedTransition ----
EXTENDS Naturals, TLC
CONSTANT LockSnapshots
VARIABLES state, fresh, snapshotLock, phase, entryWasQuiesced
vars == <<state, fresh, snapshotLock, phase, entryWasQuiesced>>

\* The operator keeps worker processes stopped throughout this command.
\* A snapshot writer already in flight may finish before we obtain the table
\* lock. The model checks that it cannot invalidate the liveness observation
\* between the check and flip. Stale evidence is not a process fence.
Init == /\ state = "prepared"
        /\ fresh = FALSE
        /\ snapshotLock = FALSE
        /\ phase = "idle"
        /\ entryWasQuiesced = TRUE
Acquire == /\ phase = "idle"
           /\ snapshotLock' = LockSnapshots
           /\ phase' = "locked"
           /\ UNCHANGED <<state, fresh, entryWasQuiesced>>
Snapshot == /\ ~snapshotLock
            /\ fresh' = TRUE
            /\ UNCHANGED <<state, snapshotLock, phase, entryWasQuiesced>>
Expire == /\ fresh' = FALSE
          /\ UNCHANGED <<state, snapshotLock, phase, entryWasQuiesced>>
Check == /\ phase = "locked"
         /\ ~fresh
         /\ phase' = "checked"
         /\ UNCHANGED <<state, fresh, snapshotLock, entryWasQuiesced>>
Refuse == /\ phase = "locked"
          /\ fresh
          /\ phase' = "done"
          /\ snapshotLock' = FALSE
          /\ UNCHANGED <<state, fresh, entryWasQuiesced>>
Flip == /\ phase = "checked"
        /\ state' = "mixed_transition"
        /\ entryWasQuiesced' = ~fresh
        /\ phase' = "done"
        /\ snapshotLock' = FALSE
        /\ UNCHANGED fresh
Next == Acquire \/ Snapshot \/ Expire \/ Check \/ Refuse \/ Flip
Spec == Init /\ [][Next]_vars
QuiescedAtFlip == entryWasQuiesced
====

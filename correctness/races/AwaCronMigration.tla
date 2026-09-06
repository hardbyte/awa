------------------------ MODULE AwaCronMigration ------------------------
EXTENDS Naturals
\* Released atomic enqueue: cron then storage. The migration's entire pending
\* range must use the same order, even when v045 is the last DDL step.
CONSTANT DrainCronFirst
VARIABLES migration, enqueue, cron, storage
vars == <<migration, enqueue, cron, storage>>
Init == /\ migration = 0 /\ enqueue = 0 /\ cron = "free" /\ storage = "free"
MigrationFirst == /\ migration = 0
                  /\ IF DrainCronFirst
                        THEN /\ cron = "free" /\ cron' = "migration" /\ UNCHANGED storage
                        ELSE /\ storage = "free" /\ storage' = "migration" /\ UNCHANGED cron
                  /\ migration' = 1 /\ UNCHANGED enqueue
MigrationSecond == /\ migration = 1
                   /\ IF DrainCronFirst
                         THEN /\ storage = "free" /\ storage' = "migration" /\ UNCHANGED cron
                         ELSE /\ cron = "free" /\ cron' = "migration" /\ UNCHANGED storage
                   /\ migration' = 2 /\ UNCHANGED enqueue
MigrationCommit == /\ migration = 2 /\ migration' = 3
                   /\ cron' = "free" /\ storage' = "free" /\ UNCHANGED enqueue
EnqueueFirst == /\ enqueue = 0 /\ cron = "free" /\ cron' = "enqueue"
               /\ enqueue' = 1 /\ UNCHANGED <<migration, storage>>
EnqueueSecond == /\ enqueue = 1 /\ storage = "free" /\ storage' = "enqueue"
                /\ enqueue' = 2 /\ UNCHANGED <<migration, cron>>
EnqueueCommit == /\ enqueue = 2 /\ enqueue' = 3
                 /\ cron' = "free" /\ storage' = "free" /\ UNCHANGED migration
Next == \/ MigrationFirst \/ MigrationSecond \/ MigrationCommit
        \/ EnqueueFirst \/ EnqueueSecond \/ EnqueueCommit
        \/ /\ migration = 3 /\ enqueue = 3 /\ UNCHANGED vars
Spec == Init /\ [][Next]_vars
NoLockCycle == ~(migration = 1 /\ enqueue = 1)
=============================================================================

---- MODULE AwaStorageTransition ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused model for the 0.5.x -> 0.6 storage transition control plane.
\*
\* This deliberately models the transition singleton, producer routing,
\* runtime capability/role gates, canonical backlog drain, queue-storage
\* rows, finalize, and abort interlocks. It does not model per-job execution
\* details; those belong in AwaSegmentedStorage and the runtime protocol
\* models.

CONSTANTS MaxCanonicalBacklog,
          MaxQueueRows,
          RequireQueueExecutorOnEnter,
          RescheduleStaysCanonical

States == {"canonical", "prepared", "mixed_transition", "active"}
Engines == {"canonical", "queue_storage", "none"}

VARIABLES state,
          currentEngine,
          preparedEngine,
          preparedSchemaReady,
          canonicalBacklog,
          queueRows,
          oldCanonicalLive,
          autoPreMixedLive,
          queueTargetLive,
          explicitDrainLive,
          mixedEntryHadQueueExecutor

vars == <<state,
          currentEngine,
          preparedEngine,
          preparedSchemaReady,
          canonicalBacklog,
          queueRows,
          oldCanonicalLive,
          autoPreMixedLive,
          queueTargetLive,
          explicitDrainLive,
          mixedEntryHadQueueExecutor>>

ActiveEngine ==
    IF state \in {"mixed_transition", "active"}
        THEN IF preparedEngine = "none" THEN currentEngine ELSE preparedEngine
        ELSE currentEngine

\* `runtime_instances.storage_capability` as currently reported by the
\* implementation. An auto 0.6 runtime started before mixed transition reports
\* `queue_storage` while canonical/prepared, then `canonical_drain_only` once
\* routing flips because its effective storage was resolved to canonical at
\* startup. Queue-storage targets report `queue_storage` throughout.
LiveCanonicalCapability ==
    oldCanonicalLive

LiveDrainCapability ==
    explicitDrainLive
    + IF state \in {"mixed_transition", "active"}
        THEN autoPreMixedLive
        ELSE 0

LiveQueueCapability ==
    queueTargetLive
    + IF state \in {"canonical", "prepared"}
        THEN autoPreMixedLive
        ELSE 0

\* Worst-case #456 workload: when any canonical backlog exists, reserve one
\* row as a handler that always re-schedules. `DrainCanonical` cannot complete
\* that row; only its re-schedule can remove it from the canonical plane.
SnoozeCount ==
    IF canonicalBacklog > 0 THEN 1 ELSE 0

\* Runtimes that will actually execute queue-storage work immediately after
\* the routing flip.
LiveQueueExecutor ==
    queueTargetLive

\* Ghost assertion bit: set exactly when EnterMixedTransition fires. Queue
\* targets are allowed to stop later, so this checks the transition gate rather
\* than treating executor liveness as a permanent invariant.

CanEnterMixedCurrentSql ==
    /\ state = "prepared"
    /\ preparedEngine = "queue_storage"
    /\ preparedSchemaReady
    /\ LiveCanonicalCapability = 0
    /\ LiveQueueCapability > 0

CanEnterMixedDesired ==
    /\ CanEnterMixedCurrentSql
    /\ LiveQueueExecutor > 0

CanEnterMixed ==
    IF RequireQueueExecutorOnEnter
        THEN CanEnterMixedDesired
        ELSE CanEnterMixedCurrentSql

CanFinalize ==
    /\ state = "mixed_transition"
    /\ preparedEngine = "queue_storage"
    /\ canonicalBacklog = 0
    /\ LiveCanonicalCapability = 0

CanAbortMixed ==
    /\ state = "mixed_transition"
    /\ LiveQueueCapability = 0
    /\ queueRows = 0

Init ==
    /\ state = "canonical"
    /\ currentEngine = "canonical"
    /\ preparedEngine = "none"
    /\ preparedSchemaReady = FALSE
    /\ canonicalBacklog = 0
    /\ queueRows = 0
    /\ oldCanonicalLive = 0
    /\ autoPreMixedLive = 0
    /\ queueTargetLive = 0
    /\ explicitDrainLive = 0
    /\ mixedEntryHadQueueExecutor = TRUE

PrepareQueueStorage ==
    /\ state \in {"canonical", "prepared"}
    /\ currentEngine = "canonical"
    /\ preparedEngine' = "queue_storage"
    /\ state' = "prepared"
    /\ UNCHANGED <<currentEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

PrepareSchema ==
    /\ state = "prepared"
    /\ preparedEngine = "queue_storage"
    /\ preparedSchemaReady' = TRUE
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

EnterMixedTransition ==
    /\ CanEnterMixed
    /\ state' = "mixed_transition"
    /\ mixedEntryHadQueueExecutor' = (LiveQueueExecutor > 0)
    /\ UNCHANGED <<currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive>>

Finalize ==
    /\ CanFinalize
    /\ state' = "active"
    /\ currentEngine' = "queue_storage"
    /\ preparedEngine' = "none"
    /\ UNCHANGED <<preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

AbortPrepared ==
    /\ state = "prepared"
    /\ state' = "canonical"
    /\ preparedEngine' = "none"
    /\ preparedSchemaReady' = FALSE
    /\ UNCHANGED <<currentEngine,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

AbortMixed ==
    /\ CanAbortMixed
    /\ state' = "canonical"
    /\ preparedEngine' = "none"
    /\ preparedSchemaReady' = FALSE
    /\ UNCHANGED <<currentEngine,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StartOldCanonical ==
    /\ state \in {"canonical", "prepared"}
    /\ oldCanonicalLive = 0
    /\ oldCanonicalLive' = 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StopOldCanonical ==
    /\ oldCanonicalLive > 0
    /\ oldCanonicalLive' = oldCanonicalLive - 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StartAutoPreMixed ==
    /\ state \in {"canonical", "prepared"}
    /\ autoPreMixedLive = 0
    /\ autoPreMixedLive' = 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StopAutoPreMixed ==
    /\ autoPreMixedLive > 0
    /\ autoPreMixedLive' = autoPreMixedLive - 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StartQueueTarget ==
    /\ state # "canonical"
    /\ preparedEngine = "queue_storage"
    /\ preparedSchemaReady
    /\ queueTargetLive = 0
    /\ queueTargetLive' = 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   canonicalBacklog,
                   preparedSchemaReady,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StopQueueTarget ==
    /\ queueTargetLive > 0
    /\ queueTargetLive' = queueTargetLive - 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

StartExplicitDrain ==
    /\ state \in {"prepared", "mixed_transition"}
    /\ explicitDrainLive = 0
    /\ explicitDrainLive' = 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   mixedEntryHadQueueExecutor>>

StopExplicitDrain ==
    /\ explicitDrainLive > 0
    /\ explicitDrainLive' = explicitDrainLive - 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   mixedEntryHadQueueExecutor>>

ProducerEnqueueCanonical ==
    /\ ActiveEngine = "canonical"
    /\ canonicalBacklog < MaxCanonicalBacklog
    /\ canonicalBacklog' = canonicalBacklog + 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

ProducerEnqueueQueueStorage ==
    /\ ActiveEngine = "queue_storage"
    /\ queueRows < MaxQueueRows
    /\ queueRows' = queueRows + 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

\* Completion of ordinary canonical work. The reserved perpetual snoozer is
\* excluded because it never completes.
DrainCanonical ==
    /\ canonicalBacklog > SnoozeCount
    /\ LiveDrainCapability > 0
    /\ canonicalBacklog' = canonicalBacklog - 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

CompleteQueueStorage ==
    /\ queueRows > 0
    /\ LiveQueueExecutor > 0
    /\ queueRows' = queueRows - 1
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor>>

\* #456: after routing flips, a canonical attempt that re-schedules records
\* exactly one fresh-id queue-storage disposition. Ordinarily it is deferred
\* executable work. If a newer duplicate owns a required unique claim, it is
\* cancelled terminal evidence instead. `queueRows` counts both families, so
\* either outcome decrements canonicalBacklog and increments queueRows. The
\* claim-level choice is checked in AwaMigratedRescheduleUnique.tla.
\*
\* With RescheduleStaysCanonical = TRUE the historical write back to
\* canonical scheduled_jobs is modeled as a stutter, preserving the drain
\* wedge as an expected counterexample.
MigrateCanonicalRescheduleToQueueStorage ==
    /\ ActiveEngine = "queue_storage"
    /\ LiveDrainCapability > 0
    /\ canonicalBacklog > 0
    /\ IF RescheduleStaysCanonical
         THEN UNCHANGED vars
         ELSE /\ queueRows < MaxQueueRows
              /\ canonicalBacklog' = canonicalBacklog - 1
              /\ queueRows' = queueRows + 1
              /\ UNCHANGED <<state,
                             currentEngine,
                             preparedEngine,
                             preparedSchemaReady,
                             oldCanonicalLive,
                             autoPreMixedLive,
                             queueTargetLive,
                             explicitDrainLive,
                             mixedEntryHadQueueExecutor>>

Stutter == UNCHANGED vars

Next ==
    \/ PrepareQueueStorage
    \/ PrepareSchema
    \/ EnterMixedTransition
    \/ Finalize
    \/ AbortPrepared
    \/ AbortMixed
    \/ StartOldCanonical
    \/ StopOldCanonical
    \/ StartAutoPreMixed
    \/ StopAutoPreMixed
    \/ StartQueueTarget
    \/ StopQueueTarget
    \/ StartExplicitDrain
    \/ StopExplicitDrain
    \/ ProducerEnqueueCanonical
    \/ ProducerEnqueueQueueStorage
    \/ DrainCanonical
    \/ CompleteQueueStorage
    \/ MigrateCanonicalRescheduleToQueueStorage
    \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ state \in States
    /\ currentEngine \in Engines
    /\ preparedEngine \in Engines
    /\ preparedSchemaReady \in BOOLEAN
    /\ canonicalBacklog \in 0..MaxCanonicalBacklog
    /\ queueRows \in 0..MaxQueueRows
    /\ oldCanonicalLive \in 0..1
    /\ autoPreMixedLive \in 0..1
    /\ queueTargetLive \in 0..1
    /\ explicitDrainLive \in 0..1
    /\ mixedEntryHadQueueExecutor \in BOOLEAN

PreparedRequiresEngine ==
    state \in {"prepared", "mixed_transition"} => preparedEngine = "queue_storage"

ActiveImpliesQueueStorageCurrent ==
    state = "active" => currentEngine = "queue_storage" /\ preparedEngine = "none"

CanonicalAndPreparedRouteCanonical ==
    state \in {"canonical", "prepared"} => ActiveEngine = "canonical"

MixedAndActiveRouteQueueStorage ==
    state \in {"mixed_transition", "active"} => ActiveEngine = "queue_storage"

NoMixedWithCanonicalOnlyRuntime ==
    state \in {"mixed_transition", "active"} => oldCanonicalLive = 0

FinalizeOnlyAfterDrain ==
    state = "active" => canonicalBacklog = 0

MixedHasQueueExecutor ==
    state \in {"mixed_transition", "active"} => mixedEntryHadQueueExecutor

AbortMixedKeepsCanonicalIfQueueStorageUnused ==
    state = "canonical" /\ currentEngine = "canonical" => queueRows = 0

\* The cross-plane disposition cannot appear while routing is canonical. The
\* implementation provides this atomicity by holding FOR SHARE on the
\* transition singleton against abort/finalize's FOR UPDATE lock.
NoQueueRowsUnderCanonicalRouting ==
    ActiveEngine = "canonical" => queueRows = 0

\* In mixed transition, a live drain runtime can reduce any backlog: ordinary
\* rows complete, while the reserved perpetual snoozer leaves the canonical
\* plane through the re-schedule disposition.
CanonicalBacklogReducible ==
    \/ canonicalBacklog > SnoozeCount
    \/ /\ canonicalBacklog > 0
       /\ ~RescheduleStaysCanonical
       /\ ActiveEngine = "queue_storage"

MixedTransitionCanReduceCanonicalBacklog ==
    (/\ state = "mixed_transition"
     /\ canonicalBacklog > 0
     /\ LiveDrainCapability > 0) => CanonicalBacklogReducible

\* One guarded canonical row becomes exactly one queue-storage disposition,
\* either deferred work or cancelled terminal evidence.
RescheduleMigrationConservesWork ==
    [][MigrateCanonicalRescheduleToQueueStorage =>
         canonicalBacklog' + queueRows' = canonicalBacklog + queueRows]_vars

=============================================================================

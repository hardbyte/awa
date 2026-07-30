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
          GateMigrate07,
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
          mixedEntryHadQueueExecutor,
          migrated07,
          migrate07EntryClean,
          snoozerCanonical

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
          mixedEntryHadQueueExecutor,
          migrated07,
          migrate07EntryClean,
          snoozerCanonical>>

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

\* Canonical backlog jobs whose handler re-schedules on every run (snooze /
\* retry-after / retry backoff). Such a job is never *completed* by a drain
\* worker, so it is excluded from `DrainCanonical`; the only action that can
\* remove it from the canonical plane is the re-schedule itself.
SnoozeCount ==
    IF snoozerCanonical THEN 1 ELSE 0

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
    /\ migrated07 = FALSE
    /\ migrate07EntryClean = TRUE
    /\ snoozerCanonical = FALSE

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   explicitDrainLive,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

\* A canonical enqueue of a job whose handler re-schedules on every run. At
\* most one is modeled; it is the #456 workload that kept
\* `canonical_live_backlog()` above zero forever.
ProducerEnqueueCanonicalSnoozing ==
    /\ ActiveEngine = "canonical"
    /\ ~snoozerCanonical
    /\ canonicalBacklog < MaxCanonicalBacklog
    /\ canonicalBacklog' = canonicalBacklog + 1
    /\ snoozerCanonical' = TRUE
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

\* Completion of a canonical backlog job by a drain-capable runtime. A
\* re-scheduling job is never completed, so it is not drainable — see
\* MigrateCanonicalRescheduleToQueueStorage.
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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

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
                   mixedEntryHadQueueExecutor,
                   migrated07,
                   migrate07EntryClean,
                   snoozerCanonical>>

\* #456: a canonical-claimed attempt whose handler asked to be re-scheduled
\* (snooze / retry-after / retryable error).
\*
\* Once routing has flipped, `reschedule_canonical_attempt_tx` takes the
\* canonical `jobs_hot` row and re-inserts the attempt into the active
\* queue-storage schema's `deferred_jobs` under a fresh id, so an existing
\* canonical row moves cross-plane: the canonical backlog shrinks by one and
\* the queue-storage plane gains one row. While the cluster is still
\* canonical or merely prepared, the historical `jobs_hot -> scheduled_jobs`
\* move applies and nothing crosses planes, so this action is disabled.
\*
\* With RescheduleStaysCanonical = TRUE the pre-fix behavior is modeled: the
\* row is written back into the canonical deferred backlog, which is net-zero
\* on `canonicalBacklog`. A job that re-schedules on every run then never
\* leaves the canonical plane, `canonical_live_backlog()` never reaches zero,
\* and `storage_finalize` can never pass — see
\* AwaStorageTransitionRescheduleStaysCanonical.cfg.
\*
\* The `queueRows < MaxQueueRows` conjunct is a model-finiteness bound, not a
\* real precondition; MixedTransitionCanReduceCanonicalBacklog is therefore
\* stated over the routing rule rather than over enabledness.
MigrateCanonicalRescheduleToQueueStorage ==
    /\ ActiveEngine = "queue_storage"
    /\ LiveDrainCapability > 0
    /\ canonicalBacklog > 0
    /\ IF RescheduleStaysCanonical
         THEN UNCHANGED vars
         ELSE /\ queueRows < MaxQueueRows
              /\ canonicalBacklog' = canonicalBacklog - 1
              /\ queueRows' = queueRows + 1
              /\ \/ /\ snoozerCanonical
                    /\ snoozerCanonical' = FALSE
                 \/ /\ canonicalBacklog > SnoozeCount
                    /\ UNCHANGED snoozerCanonical
              /\ UNCHANGED <<state,
                             currentEngine,
                             preparedEngine,
                             preparedSchemaReady,
                             oldCanonicalLive,
                             autoPreMixedLive,
                             queueTargetLive,
                             explicitDrainLive,
                             mixedEntryHadQueueExecutor,
                             migrated07,
                             migrate07EntryClean>>

\* The 0.7 migrate gate (#370 / ADR-037). `awa migrate` on a 0.7 binary
\* applies pending migrations only when the transition is finalized or the
\* cluster is effectively fresh — canonical, unprepared, no canonical work,
\* and no recently-live runtime of any kind (mirroring the SQL conditions in
\* `awa.storage_auto_finalize_if_fresh`).
Migrate07GateOpen ==
    \/ state = "active"
    \/ /\ state = "canonical"
       /\ preparedEngine = "none"
       /\ canonicalBacklog = 0
       /\ oldCanonicalLive + autoPreMixedLive + queueTargetLive + explicitDrainLive = 0

\* Ghost record of what the gate is meant to guarantee at the moment the
\* migration lands: no canonical work and no canonical-only runtime. A
\* drain-only runtime may remain after finalization because supported writes
\* route exclusively to queue storage and the canonical backlog is empty.
\* Checked by Migrate07OnlyOnQuiescedCanonical; the Ungated config
\* demonstrates the counterexample without the gate.
Migrate07 ==
    /\ ~migrated07
    /\ IF GateMigrate07 THEN Migrate07GateOpen ELSE TRUE
    /\ migrated07' = TRUE
    /\ migrate07EntryClean' =
           (canonicalBacklog = 0 /\ LiveCanonicalCapability = 0)
    /\ UNCHANGED <<state,
                   currentEngine,
                   preparedEngine,
                   preparedSchemaReady,
                   canonicalBacklog,
                   queueRows,
                   oldCanonicalLive,
                   autoPreMixedLive,
                   queueTargetLive,
                   explicitDrainLive,
                   mixedEntryHadQueueExecutor,
                   snoozerCanonical>>

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
    \/ ProducerEnqueueCanonicalSnoozing
    \/ ProducerEnqueueQueueStorage
    \/ DrainCanonical
    \/ CompleteQueueStorage
    \/ MigrateCanonicalRescheduleToQueueStorage
    \/ Migrate07
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
    /\ migrated07 \in BOOLEAN
    /\ migrate07EntryClean \in BOOLEAN
    /\ snoozerCanonical \in BOOLEAN

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

\* A 0.7 migration never lands while canonical work exists or a runtime
\* that can still execute canonical work is live.
Migrate07OnlyOnQuiescedCanonical ==
    migrated07 => migrate07EntryClean

\* #456 safety: the cross-plane re-schedule can never place a row in the
\* queue-storage plane while producer routing is still canonical. Together
\* with AbortMixedKeepsCanonicalIfQueueStorageUnused this is what makes
\* `storage_abort` sound: aborting back to canonical only ever happens with
\* the queue-storage plane empty, and the re-schedule's `FOR SHARE` lock on
\* `awa.storage_transition_state` is what gives the implementation the
\* action atomicity the model assumes here.
NoQueueRowsUnderCanonicalRouting ==
    ActiveEngine = "canonical" => queueRows = 0

\* A re-scheduling job is part of the canonical backlog for as long as it
\* has not migrated, so it can never outlive the backlog count. This is what
\* lets FinalizeOnlyAfterDrain imply that no snoozing canonical job survives
\* finalization.
SnoozerImpliesCanonicalWork ==
    snoozerCanonical => canonicalBacklog > 0

\* #456 drain convergence, as safety: while mixed transition is in progress
\* and a drain-capable runtime is live, some modeled action always reduces
\* the canonical backlog — either an ordinary job completes, or a
\* re-scheduling job migrates cross-plane. Violating this means the backlog
\* is wedged above zero, so `CanFinalize` can never become true.
\*
\* Stated as a safety invariant rather than a liveness property on purpose:
\* the model deliberately permits a cluster to never prepare, to abort, and
\* to stop every runtime, so no unconditional `<>[] state = "active"` holds
\* even with the fix, and adding enough fairness to exclude those behaviors
\* would encode the conclusion into the hypotheses.
CanonicalBacklogReducible ==
    \/ canonicalBacklog > SnoozeCount
    \/ /\ snoozerCanonical
       /\ ~RescheduleStaysCanonical
       /\ ActiveEngine = "queue_storage"

MixedTransitionCanReduceCanonicalBacklog ==
    (/\ state = "mixed_transition"
     /\ canonicalBacklog > 0
     /\ LiveDrainCapability > 0) => CanonicalBacklogReducible

\* Work conservation for the cross-plane move: it relocates an attempt, it
\* never drops or duplicates one. An action property, since conservation is
\* a statement about the step rather than about any single state.
RescheduleMigrationConservesWork ==
    [][MigrateCanonicalRescheduleToQueueStorage =>
         canonicalBacklog' + queueRows' = canonicalBacklog + queueRows]_vars

=============================================================================

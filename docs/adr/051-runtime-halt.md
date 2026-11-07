# ADR-051: Scoped runtime controls

## Status

Proposed. The defaults below are design choices requiring contention and failure testing before acceptance; no runtime behavior is implemented by this record.

## Context

Queue pause stops claims but leaves maintenance running. Containing reclamation lock pressure should leave dispatch, promotion and rescue available. Awa needs durable live controls shared by all runtimes in a database, including restarted clients and successor maintenance leaders, without making enqueue depend on those controls.

The [source audit](../design/runtime-control-source-audit.md) identifies claim paths, reclamation paths and observation limits on main and 0.6.

## Decision

Use five fixed database-wide scopes. A full halt sets all five atomically; enqueue remains available in application transactions.

| Scope | Controlled work |
| --- | --- |
| `dispatch` | New claims; admitted attempts drain normally. |
| `cron` | Cron declaration sync and due-job enqueue. |
| `maintenance.prune` | Rotation, reclamation, retention, terminal rollups and storage-authority changes. |
| `maintenance.promote` | Scheduled/retry promotion and priority/key readiness work. |
| `maintenance.rescue` | Heartbeat, deadline and callback rescue. |

Auxiliary metadata refresh, descriptor publication and administrative batch work stop whenever all three maintenance scopes are halted. Resuming dispatch or cron alone leaves them stopped; resuming a maintenance scope permits them, except reclamation still requires `maintenance.prune`. Control observation, liveness reporting and completion of admitted work continue. A local stop handle remains independent of database access.

### State and enforcement

Store one row per scope in ordinary table `awa.runtime_controls`, containing its revision, current halt ID, mode, reason, actor and update time. An explicit resume must name the current halt ID; stale resumes fail and cannot remove queue pauses or local restrictions.

Enforce dispatch in every claim-producing SQL path without locking the control row, covering canonical claims, slot functions, custom substrates and reinstalls. Require Read Committed claim transactions and reject other isolation levels at guarded entry points; evaluate permission and claim together. Overlapping evaluations may finish after halt commits, and admitted jobs may launch later. This avoids client polling delay without promising instantaneous cessation.

Reuse the existing leader session in an independently scheduled observer that serializes control reads, liveness checks and unlocks. Maintenance must not block that observer. Read controls before initial work and leadership handoff; add no control-observer connections. Other runtimes observe controls and signal cancellation through their existing snapshot cycle and pool; pool starvation can delay that observation.

| Proposed default | Value |
| --- | --- |
| Leader control observation | Every 1 s, with ±20% jitter |
| Control-read deadline | 1 s |
| Maintenance/cron permission freshness | 5 s, measured monotonically |
| Runtime control reporting | Every 10 s; unknown after 30 s without observation |
| Drain wait | 30 s, then return pending without escalating |
| Reclamation resume budget | One potentially exclusive-lock transaction admission/s per leader; concurrency 1 |

On read failure, retain the last observation. Maintenance and cron admit no new work once permission is 5 s old; a known halt remains until explicit resume. Dispatch has no observer-freshness lease: each claim reads authoritative state in SQL. A stale reporting cache becomes unknown, without stopping dispatch. Unreadable claim state remains an ordinary claim error; enqueue is unaffected.

### Drain, cancellation and recovery

`drain` is the default: stop new admissions and let admitted work finish. `cancel` additionally requests cooperative cancellation; handlers, external effects and server-side SQL may continue.

Results distinguish `committed` intent, a runtime's `observed` revision, and `drained` admitted work. Fleet drain succeeds only when every relevant durable participant confirms no outstanding work, including in-flight claim evaluations and SQL; after 30 s, missing evidence returns pending/unknown. Observation alone never certifies cessation.

Runtimes register durably before admitting work, serialized with halt changes. The cancel transaction records pending evidence for affected participants in `awa.runtime_control_participants`, outside expiring snapshots. Missing observation, timeout or heartbeat expiry cannot clear it. Resuming a scope requires positive reconciliation or audited independent fencing evidence for every unresolved owner, including earlier halts. The proposed command `awa control reconcile --halt <id> --runtime <id> --fence-evidence <reference>` records that assertion when an owner cannot report back; it does not itself fence a host or undo effects.

On reclamation resume, wait a random 0–1 s, preserve ordinary cadences and backoff, and apply the shared budget across rings and substrates without catch-up bursts. Preserve the existing 10 ms prune lock timeout; it does not bound an entire operation.

Halt, resume and reconciliation use hardened `SECURITY DEFINER` functions granted only to a control-operator role; revoke public execution and direct control writes from producer/runtime roles. Publish each runtime's observed revisions, scope states, observation times and outstanding work in `awa.runtime_instances`; report expired observations as unknown rather than stopped.

## Alternatives considered

**A single all-work halt** couples reclamation containment to loss of dispatch and recovery. **Queue pause or startup settings** leave maintenance uncontrolled live. **A mandatory external flag provider or extra observer connections** adds dependencies where existing database paths suffice.

## Consequences

Operators can stop reclamation while useful work continues. A `maintenance.prune` drain-only subset is independently useful; it need not wait for complete dispatch fencing or cancellation/reconciliation machinery. Unsupported scopes must be refused, and coverage must include rotation and terminal-rollup reclamation.

Halting maintenance accumulates storage or delayed jobs. Freshness expiry deliberately trades maintenance availability for containment; SQL gates add claim-path read cost that requires measurement. Unpersisted local stops protect only that runtime's lifetime.

Older binaries are covered only where patched database entry points constrain their actual paths. Canonical inline callers and maintenance leaders otherwise need an upgrade or enforced fence. A 0.6 repeatable patch is a delivery option requiring verification, not established coverage. Follow [ADR-041](041-rolling-upgrade-policy.md); align privilege separation with proposed [ADR-043](043-postgresql-capability-functions.md).

## Relationship to ADR-028

[ADR-028](028-maintenance-only-runtime-role.md) proposes running maintenance without executing jobs. Its standalone and embedded runtimes obey the same applicable maintenance and cron scopes. Local leadership handoff cannot remove a database restriction. The control observer and reporting remain active while maintenance is halted.

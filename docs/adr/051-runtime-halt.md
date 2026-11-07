# ADR-051: Scoped runtime controls

## Status

Proposed. Phase 1 can be accepted independently. Timing defaults require rehearsal and measurement.

## Context

Queue pause stops claims but leaves maintenance running. Containing reclamation lock pressure should leave dispatch, promotion and rescue available. Durable controls must survive runtime restart and leadership handoff without making application enqueue depend on them. The [source audit](../design/runtime-control-source-audit.md) records the claim and maintenance paths on main and 0.6.

## Decision

Use five fixed database-wide scopes: `dispatch`, `cron`, `maintenance.prune`, `maintenance.promote` and `maintenance.rescue`. A full halt sets all installed scopes atomically; it is available only when all five are supported. Application enqueue remains unchanged. Drain is the default, and a local stop remains independent of database access.

## Phase 1: reclamation gate

### Permanent reclamation cap

Always allow at most one strong-relation-lock transaction admission per second per leader, with concurrency one across all rings and substrates. Strong means potentially requesting `SHARE`, `SHARE ROW EXCLUSIVE`, `EXCLUSIVE` or `ACCESS EXCLUSIVE`; ordinary `ROW EXCLUSIVE` writes do not qualify. The map identifies capped operations. Each nested transaction needs its own admission before its first possible strong-lock request; one permit cannot cover an entire multi-transaction timer callback.

Wait at least one monotonic second after a capped transaction settles before admitting the next; store no catch-up credit. Apply the cap during normal operation, startup, leadership changes and resume, preserving ordinary cadences and backoff. A capped transaction settles only when its commit or rollback is acknowledged, or when its connection is known closed or `pg_cancel_backend` has succeeded; a locally cancelled task or client-side timeout alone cannot release the slot. Keep the existing 10 ms prune lock timeout; it does not bound an entire operation.

This cap is independent of the control schema and can ship in a budget-only build before any control table exists.

### Reclamation capacity

The cap is only safe when reclamation demand stays well below it. Each ring rotation eventually needs one capped prune transaction, so steady-state demand per leader is approximately

> Σ over every queue-storage schema the leader serves of (1/`queue_rotate_interval` + 1/`lease_rotate_interval` + 1/`claim_rotate_interval`), plus terminal-rollup slot transactions per second and any other capped housekeeping.

`Blocked` attempts consume admissions too. Today's defaults rotate all three rings every 1 s, a demand of about three admissions per second against a cap below one. That configuration would exhaust free slots within seconds, rotation would report `SkippedBusy`, and the rings would stop recycling. Phase 1 therefore:

- Raises the default rotation intervals so default demand is at most half the cap. The rehearsal compares candidate defaults (for example 5 s, 10 s and 30 s) and the operator guidance states their cost: rows per slot grow with the interval, and retained history spans about the slot count multiplied by the interval.
- Computes configured demand at leader start and on configuration change. Demand above half the cap logs a warning and is exported as a metric; demand at or above the cap fails validation, because the backlog would then grow without bound.
- Keeps the cap fixed rather than scaling it with configuration, so the strong-lock ceiling stays predictable.

Install one `maintenance.prune` row in ordinary table `awa.runtime_controls`, with revision, halt ID, mode, reason, actor and update time. Support drain-only halt and halt-ID-checked resume through operator-only functions. Gate rotation, pruning, retention, terminal rollups, ledger folds, automatic storage-authority changes and the housekeeping assigned to this scope below. Other duties continue normally; unsupported scope or cancel requests fail explicitly.

Use the existing leader session in an independently scheduled observer: read every 1 s with ±20% jitter and a 1 s query deadline. Require an initial read before gated work; stop new admissions when permission is 5 s old. Control-read failure is distinct from loss of the leader session. Keep observation and truthful requested/observed/draining status active while halted, with no extra observer connection.

The control capability is enabled only after its additive migration. Before enablement the budget-only build performs no control reads; after enablement a missing or malformed row denies gated work. On 0.6, deliver only Phase 1: an additive repeatable table/function patch and compatible runtime changes, without refreshing claim functions or reinstalling substrates. All possible leaders must support it or be fenced before fleet coverage is claimed.

The closed enum and complete branch classification below are Phase 1 requirements. Phase 1 enforces only `maintenance.prune`; the other scopes remain unrestricted until their controls are implemented.

<details markdown="1">
<summary>Shared branch map required for Phase 1</summary>

### Exhaustive branch map

Schedule leader work through a closed `MaintenanceBranch` enum with an exhaustive scope-and-budget match and no wildcard/default-allow arm. A new branch cannot be registered without explicit classifications. Split mixed callbacks and reporter side effects into classified operations; CI checks the branch inventory, startup paths and nested operations against this table. Control-only exemptions are explicit variants; cap flags follow the audited SQL definitions and must be rechecked when those definitions change.

| Leader branch, nested operation or shared entry | Scope | Capped transaction |
| --- | --- | --- |
| Initial and periodic `sync_periodic_jobs_to_db` | `cron` | No |
| Independent cron evaluator | `cron` | No |
| `rescue_stale_heartbeats` | `maintenance.rescue` | No |
| `rescue_expired_deadlines` | `maintenance.rescue` | No |
| `rescue_expired_callbacks` | `maintenance.rescue` | No |
| `promote_scheduled` | `maintenance.promote` | No |
| `age_waiting_priorities` | `maintenance.promote` | No |
| Cleanup: completed/failed jobs and DLQ retention | `maintenance.prune` | No |
| `cleanup_batch_operations` | `maintenance.prune` | No |
| `cleanup_stale_runtime_snapshots` | `maintenance.prune` | No |
| `cleanup_stale_descriptors` | `maintenance.prune` | No |
| Queue-health metrics, both storage engines | `maintenance.prune` | No |
| `recompute_dirty_admin_metadata` | `maintenance.prune` | No |
| Canonical `refresh_admin_metadata` | `maintenance.prune` | Yes on 0.6; no on main |
| `process_batch_operation` | `dispatch` | No |
| Queue rotation | `maintenance.prune` | No |
| Queue pruning | `maintenance.prune` | Yes |
| Lease rotation | `maintenance.prune` | No |
| Lease pruning | `maintenance.prune` | Yes |
| Claim rotation | `maintenance.prune` | No |
| Claim pruning | `maintenance.prune` | Yes |
| Terminal-delta rollup | `maintenance.prune` | Yes, per slot transaction |
| Prune-delta fold | `maintenance.prune` | No |
| Rotation-ledger fold | `maintenance.prune` | No |
| Storage-authority flip | `maintenance.prune` | No |
| Startup/reporter descriptor publication | `maintenance.prune` | No |
| Startup/reporter cron manifest/reconciliation evidence | `cron` | No |
| Election, control/liveness checks, unlock and shutdown | Control-only | No |
| Runtime/attempt liveness and admitted-work completion | Control-only | No |
| Registration validation and bounded-claimer lease bookkeeping | Control-only | No |

Dirty keys here maintain dashboard metadata, not execution readiness. Housekeeping can pause with prune while recovery continues; control and liveness reporting must remain separate from those side effects. Preserve liveness-triggered safety fencing of cron evidence while suppressing new declarations. There is no aggregate rule that silently reopens side tasks when another scope resumes.

</details>

Acceptance is a pass/fail PostgreSQL 17 and 18 rehearsal, with retained admission/lock traces and exact job accounting:

- The exhaustive branch map above is implemented, and every strong-relation-lock transaction passes through the shared cap. Normal operation, backlog, resume and leadership changes respect its spacing and concurrency without catch-up.
- At the default and each documented rotation configuration, a one-hour mixed workload keeps measured capped demand at or below half the cap, and ring occupancy and slots awaiting prune stay bounded. Configured demand at or above the cap is rejected at validation.
- With healthy established leader/operator sessions, denying new connections or separately exhausting the worker pool does not stop observation: apply a readable halt revision within 3 s. Failed reads stop new gated admissions after 5 s. Admitted SQL remains reported as draining until settled.
- Halt suppresses every listed reclamation path while enqueue, dispatch, promotion and rescue continue in a separate mixed-workload run. A stale resume cannot clear a newer halt; drain-mode resume succeeds without waiting for a missing runtime or reconciliation.
- Repeat the additive patch safely; verify operator access and producer/runtime mutation denial. The cap also works without the schema in budget-only mode. Claims and substrate installers are unchanged on 0.6.

Phase 1 requires no dispatch fencing, cancel-participant registry or cancellation-reconciliation machinery.

## Full design (Proposed)

Prune and cron stop stale admission; promotion and rescue retain their last observed state while the leader session remains held. The full design adds the other scopes and cancellation accounting independently of Phase 1.

<details markdown="1">
<summary>Other scope policies, dispatch enforcement and cancellation contract</summary>

### Scope policies

After an initial successful read, apply these policies to control-read failures. A known halt remains until its matching explicit resume is observed.

| Scope | Controlled work | Failed control reads |
| --- | --- | --- |
| `dispatch` | Claims and administrative batch execution | Read SQL state at each admission. |
| `cron` | Declaration reconciliation and due-job enqueue | Stop new work after 5 s stale. |
| `maintenance.prune` | Reclamation and non-recovery housekeeping | Stop new work after 5 s stale. |
| `maintenance.promote` | Scheduled/retry promotion and priority aging | Keep last observed state. |
| `maintenance.rescue` | Heartbeat, deadline and callback recovery | Keep last observed state. |

Promotion and rescue keep recovery available through a control-read outage while the leader session remains held. Cron conservatively pauses new scheduled production; skipping reclamation and housekeeping is also preferable to operating on stale permission. Losing the session stops all leader work until re-election and a successful initial read, regardless of scope policy. A read timeout alone does not establish session loss. Expired observations are reported as unknown even where work continues.

### State and enforcement

Extend the Phase 1 table to one row per installed scope. Resume compare-and-swaps the current halt ID and cannot remove queue pauses or local restrictions. Halt, resume and reconciliation use hardened `SECURITY DEFINER` functions granted only to a control-operator role; revoke public execution and direct control writes from producers and runtime roles.

On main, enforce dispatch in every claim-producing SQL path and administrative batch-chunk admission without locking the control row, covering canonical claims, slot functions, custom substrates and reinstalls. Awa-owned admission transactions explicitly begin at Read Committed before any query, irrespective of database/role defaults. External callers must provide that isolation level or be rejected; never silently change a caller-owned transaction. Evaluate permission and admission together. Overlapping evaluations may finish after halt commits, and admitted jobs may launch later.

The control read adds an `ACCESS SHARE` relation lock, eligible for PostgreSQL's fast path when available. This ordinary, unpartitioned table is never truncated and is outside prune's partition-lock set; claim-path overhead still needs measurement.

The leader observer exclusively owns its existing session and serializes control reads, liveness checks and unlocks independently of maintenance. Other runtimes observe through their existing snapshot cycle and pool, which may delay cancellation. Report observed revisions, scope states, observation times and outstanding work in `awa.runtime_instances` every 10 s by default; after 30 s without observation show unknown, not stopped. Dispatch has no observer-freshness lease: an unreadable SQL control row is an ordinary claim error, while application enqueue is unaffected.

### Drain, cancellation and recovery

Drain stops new admissions and lets admitted work finish. **Drain-mode resume never waits for runtime acknowledgments, drain completion or reconciliation.** A missing or dead runtime cannot block it; the halt ID and independent local/queue restrictions still apply.

Results distinguish committed intent, an observed revision and drained work. Reporting fleet drain requires every relevant durable participant to confirm no outstanding work, including in-flight claims and SQL. After a 30 s wait, absent evidence returns pending/unknown, not cessation; this reporting wait is not a prerequisite for drain-mode resume.

Only cancel mode creates reconciliation obligations. Runtimes register durably before admission, serialized with halt changes; cancel records pending owners in `awa.runtime_control_participants`, outside expiring snapshots. Missing observation, timeout and heartbeat expiry cannot clear that evidence. A cancel-mode resume requires positive reconciliation or audited independent fencing evidence. Superseding a cancel retains unresolved evidence; downgrading an unresolved cancel to drain is rejected so it cannot bypass that obligation.

The proposed `awa control reconcile --halt <id> --runtime <id> --fence-evidence <reference>` records a fencing assertion for an owner that cannot report back. It does not itself fence a host or undo effects. Cooperative cancellation can leave handlers, external effects or server-side SQL running.

</details>

## Alternatives considered

An all-work switch unnecessarily stops recovery during reclamation incidents. Queue pause/startup settings do not provide live maintenance control. Mandatory external flags or extra observer connections add dependencies. One stale-state rule for every scope sacrifices recovery availability; a resume-only limiter leaves normal-operation lock pressure uncontrolled. A cap that scales with configured rotation keeps reclamation current but gives no fixed strong-lock ceiling, so Phase 1 fixes the cap and moves the defaults instead.

## Consequences

The permanent cap bounds strong-lock admissions but requires longer default rotation intervals, which enlarge each slot and lengthen retained history. A prune halt also pauses housekeeping, including descriptor publication, so runtimes that start during the halt do not appear in admin views until it ends. Promotion and rescue can continue on stale observed permission; their explicit halt can therefore be delayed by a control-read outage. Unpersisted local stops protect only that runtime's lifetime.

Full dispatch control is main-only work. A wrapper patch cannot cover canonical inline callers or unpatched slot functions, and older maintenance leaders need an upgrade or enforced fence. Follow [ADR-041](041-rolling-upgrade-policy.md); align privilege separation with proposed [ADR-043](043-postgresql-capability-functions.md).

## Relationship to ADR-028

[ADR-028](028-maintenance-only-runtime-role.md) proposes running maintenance without executing jobs. Its standalone and embedded runtimes use the same branch mapping, scope policies and permanent cap. Leadership handoff cannot lift a database restriction; control observation and reporting continue while work is halted.

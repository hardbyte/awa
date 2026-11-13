# Runtime controls

The repeatable `prune_controls` patch adds a durable, database-wide `maintenance.prune` control. A halt stops compatible runtimes from admitting reclamation and housekeeping. Enqueue, dispatch, handlers, cron, promotion, rescue and admin batches continue. Only `maintenance.prune` with mode `drain` is supported in 0.6.

## Install

Run `awa migrate` as the schema owner. The schema stays at v40. External migration runners apply `R__prune_controls.sql` as a repeatable script after V40. After `claim_batch_job_index` and `insert_jobs_compat_batch`, installation creates two ordinary tables, `awa.runtime_controls` and `awa.runtime_control_observations`, and installs functions. The observation table has `instance_id uuid PRIMARY KEY` and `observation jsonb NOT NULL`, with no foreign key. It does not alter `awa.runtime_instances` or refresh queue-storage installers or claim functions. Replaying the patch preserves an existing halt and explicit operator grants.

Upgrade or fence every process that can become maintenance leader before relying on the control. Older binaries ignore it; they can become leader and resume reclamation. A compatible binary discovers installation without a restart or leadership change. Before installation it retains the existing reclamation policy while catalog observations remain fresh. Once installation is observed, a missing or unreadable row cannot restore permission.

Apply runtime grants before deploying compatible binaries to an installed database. The patch does not grant runtime access automatically. Between installation and the grants, compatible runtimes cannot read the control: newly started runtimes keep prune admission closed, and existing permission expires after five seconds. Other scopes continue. Finish the grants to restore control reads; do not interpret a stored resume as proof that pruning is running.

Grant control reads and observation writes to runtime roles, and mutation only to an operator role. Replace these example role names with your own:

```sql
GRANT USAGE ON SCHEMA awa TO awa_runtime, awa_operator;
REVOKE ALL ON awa.runtime_controls, awa.runtime_control_observations
    FROM awa_runtime, awa_operator;
REVOKE EXECUTE ON FUNCTION awa.halt_runtime_scope(text, text, text),
                          awa.resume_runtime_scope(text, uuid)
    FROM awa_runtime;
GRANT SELECT ON awa.runtime_controls TO awa_runtime, awa_operator;
GRANT SELECT, INSERT, UPDATE, DELETE ON awa.runtime_control_observations
    TO awa_runtime;
GRANT SELECT ON awa.runtime_control_observations, awa.runtime_instances
    TO awa_operator;
GRANT EXECUTE ON FUNCTION awa.halt_runtime_scope(text, text, text),
                          awa.resume_runtime_scope(text, uuid)
    TO awa_operator;
```

The mutators are `SECURITY DEFINER` functions with a pinned search path. PUBLIC, producer and runtime roles receive no mutation privilege from the patch, including through broad default grants. Apply the exceptions above after blanket table/function grants, and remove inherited mutation privileges from their granting role. Schema owners and superusers retain their normal administrative authority. Do not grant operators direct writes to the control table. The stored actor is the database login (`session_user`).

## Halt and resume

```bash
awa --database-url "$AWA_OPERATOR_DATABASE_URL" control halt \
  --scope maintenance.prune --reason 'Investigating reclamation lock pressure'
awa --database-url "$AWA_OPERATOR_DATABASE_URL" control status
awa --database-url "$AWA_OPERATOR_DATABASE_URL" control resume \
  --scope maintenance.prune --halt '<current halt UUID>'
```

An existing SQL session can change the control even when a new CLI connection is unavailable:

```sql
SELECT * FROM awa.halt_runtime_scope('maintenance.prune', 'Investigating lock pressure', 'drain');
SELECT * FROM awa.resume_runtime_scope('maintenance.prune', '<current halt UUID>'::uuid);
```

Each halt creates a new UUID and increments the revision, even if already halted. Resume must name that exact current UUID; a stale resume fails. Drain-mode resume never waits for callback settlement or reconciliation, including work from an earlier halt. Replaying migrations does not resume the fleet.

The maintenance leader reads the row on its existing advisory-lock session every 0.8–1.2 seconds, with a one-second read deadline. No observer connection is added. Permission older than five seconds stops admission to this scope. Promotion and rescue keep running through a failed control read. If control reads fail, separate `SELECT 1` probes check session liveness; three consecutive failed probes end leadership. A control-table timeout with healthy probes does not.

Leadership loss releases the observer's session, stops new branches and cron, and lets the branches in flight finish their follow-ups and lifecycle notifications. The leader runs two maintenance loops, so up to two branches can be in flight: one recovery branch (promotion or a rescue) and one hygiene branch. Shutdown gives them up to five seconds, shared, to finish. Standbys use their existing shared pool, with separate one-second budgets for control reads and report writes. Pool pressure can leave their reports stale and make fleet status `degraded` or `unconfirmed`; the leader's existing session does not depend on acquiring a pool connection.

A halt is observed asynchronously. Work admitted before observation can finish, including its SQL transactions. Outstanding callbacks count local work; dropping a future releases its count even if SQL might still be running. Reclamation and admin metadata refresh track potentially unsettled SQL. A dropped future or transport failure sets `settlement_unknown` for thirty seconds after the latest uncertainty event, with the remaining window in `settlement_uncertainty_remaining_ms`. `abandoned_callbacks` is a cumulative diagnostic counter and can remain nonzero after uncertainty expires. Read-only queue health queries and descriptor publication errors do not create settlement uncertainty. Status can return `halted` after expiry without backend reconciliation. Expiry is a local reporting policy, not proof that a PostgreSQL backend settled or released its locks. Inspect backend activity and cancel or fence lingering sessions when lock relief is required.

The `observation` JSON in `awa.runtime_control_observations` includes `protocol_version` (currently `1`), `report_sequence`, permission freshness, requested halt ID, outstanding callbacks and unknown settlement. The publisher upserts each instance's latest sequence; an older report cannot overwrite a newer one. PostgreSQL adds `received_at` when a report arrives, so report recency does not depend on runtime clock skew. The embedded `fresh` value describes permission at report creation, rather than proving it remains fresh now.

## Read status

`awa control status` returns `requested`, aggregate `state`, `runtimes`, `unobserving_runtimes`, `leader_eligible_runtimes` and `observed_at`. Runtime entries include identity, version, leader flag and `maintenance_leader`, which is `false` for a runtime that never contends for leadership ([maintenance leader election](configuration.md#leader-election)). `leader_eligible_runtimes` counts live runtimes, not shutting down, that may lead; `0` with live runtimes means none of them will. Reading `maintenance_leader` needs `SELECT` on `awa.runtime_maintenance_eligibility`; without it, or before the `runtime_maintenance_eligibility` patch, every runtime reads as `true`. Stale registrations remain visible in `runtimes`; liveness uses the database clock and the standard cutoff of `max(3 × snapshot_interval, 30 seconds)`. Report recency uses server `received_at` and a window of `max(2 × snapshot_interval, 5 seconds)`, exposed as `report_freshness_ms` on each runtime. With the default ten-second snapshots the window is twenty seconds; this tolerates the reporting cadence without extending the five-second control-permission lifetime or adding polls.

`unobserving_runtimes` lists every live runtime with a missing, unsupported, wrong-protocol or stale report, or permission already stale when the report was published, with its identity and reason. Without a halt request, any such runtime makes the aggregate `degraded`; otherwise it is `running`. A resumed control can therefore be `degraded` while runtimes keep prune admission closed because they cannot read it.

For an active halt, any unobserving runtime, leader or standby, makes the aggregate `unconfirmed`. No live runtimes also means `unconfirmed`. A supported, recent report for an earlier halt leaves the request `pending`; unknown settlement or malformed safety facts yield `unknown`. `draining` means acknowledged work is outstanding; `halted` requires every live runtime to report the current halt with no outstanding work or unknown settlement.

Status is best-effort evidence from trusted runtime registrations and reports. It is not tamper-evident against holders of runtime credentials: the documented role can forge another runtime's observation or delete its `awa.runtime_instances` registration, hiding an unobserving runtime from the aggregate. Operator-only halt/resume protects the requested control, not the authenticity or completeness of confirmation. Protect runtime credentials, and fence untrusted processes and old leaders before relying on status. See [runtime control privileges](security.md#runtime-control-privileges).

Status neither certifies PostgreSQL lock settlement nor proves that a stale host is fenced. Older 0.6.15 binaries do not report this protocol and can still become leader and prune. Rolling upgrades are supported, but halt is unreliable until every potential leader is upgraded or fenced. Inspect status before relying on it, and investigate any unobserving runtime.

## Branch coverage

| Leader work | Scope |
| --- | --- |
| Queue, lease and claim rotation and prune | maintenance.prune |
| Terminal count rollup | maintenance.prune |
| Completed/DLQ/batch/snapshot/descriptor cleanup | maintenance.prune |
| Admin metadata refresh | maintenance.prune |
| Descriptor publication | maintenance.prune |
| Fresh storage initialization and auto-finalization | Unchanged |
| Scheduled/retry promotion and priority aging | Unchanged |
| Heartbeat, deadline and callback rescue | Unchanged |
| Cron sync and evaluation | Unchanged |
| Admin batch execution | Unchanged |
| Queue health gauges | Unchanged |

The scheduler names branches through an exhaustive enum; an inventory test rejects unclassified registrations. This patch leaves ring defaults and startup capacity checks unchanged and adds no reclamation rate cap. Resuming uses the existing intervals, backoff and per-pass limits. Rehearse the resulting backlog on your workload before a long halt.

Fresh storage initialization and storage selection proceed even when halt is requested or the initial control read fails. Descriptor publication and reclamation still require permission.

The five-scope design remains Proposed in [ADR-051](https://github.com/hardbyte/awa/pull/662); the maintenance-only runtime role is separate architectural work in [ADR-028](adr/028-maintenance-only-runtime-role.md). This patch supplies the prune-only operator control without requiring either full design.

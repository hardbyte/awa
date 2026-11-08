# Runtime controls

The repeatable `prune_controls` patch adds a durable, database-wide `maintenance.prune` control. A halt stops compatible runtimes from admitting reclamation and housekeeping. Enqueue, dispatch, handlers, cron, promotion, rescue and admin batches continue. Only `maintenance.prune` with mode `drain` is supported in 0.6.

## Install

Run `awa migrate` as the schema owner. The schema stays at v40. External migration runners apply `R__prune_controls.sql` as a repeatable script after V40. After `claim_batch_job_index` and `insert_jobs_compat_batch`, installation creates two ordinary tables, `awa.runtime_controls` and `awa.runtime_control_observations`, and installs functions. The observation table has `instance_id uuid PRIMARY KEY` and `observation jsonb NOT NULL`, with no foreign key. It does not alter `awa.runtime_instances` or refresh queue-storage installers or claim functions. Replaying the patch preserves an existing halt and explicit operator grants.

Upgrade or fence every process that can become maintenance leader before relying on the control. Older binaries ignore it; they can become leader and resume reclamation. A compatible binary discovers installation without a restart or leadership change. Before installation it retains the existing reclamation policy while catalog observations remain fresh. Once installation is observed, a missing or unreadable row cannot restore permission.

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

The maintenance leader reads the row on its existing advisory-lock session every 0.8–1.2 seconds, with a one-second read deadline. No observer connection is added. Permission older than five seconds stops admission to this scope. Promotion and rescue keep running through a failed control read; losing the leader session ends leadership. Standbys report through their existing runtime-reporting path.

A halt is observed asynchronously. Work admitted before observation can finish, including its SQL transactions. A hung query, dropped future or transport failure may leave settlement unknown; a status response is not proof that PostgreSQL released every lock. Inspect backend activity when immediate lock relief is required. The `observation` JSON in `awa.runtime_control_observations` includes `protocol_version` (currently `1`), `report_sequence`, permission freshness, requested halt ID, outstanding callbacks and unknown settlement. The publisher upserts each instance's latest sequence; an older report cannot overwrite a newer one. PostgreSQL adds `received_at` when a report arrives, so report recency does not depend on runtime clock skew. The embedded `fresh` value describes permission at report creation, rather than proving it remains fresh now.

## Read status

`awa control status` returns `requested`, aggregate `state`, `runtimes`, `unobserving_runtimes` and `observed_at`. Runtime entries include identity, version and leader flag. Stale registrations remain visible in `runtimes`; liveness uses the database clock and the standard cutoff of `max(3 × snapshot_interval, 30 seconds)`. Report recency uses server `received_at` and a window of `max(2 × snapshot_interval, 5 seconds)`, exposed as `report_freshness_ms` on each runtime. With the default ten-second snapshots the window is twenty seconds; this tolerates the reporting cadence without extending the five-second control-permission lifetime or adding polls.

For an active halt, `unobserving_runtimes` lists every live runtime with a missing, unsupported, wrong-protocol or stale report, or permission already stale when the report was published, with its identity and reason. Any such runtime, leader or standby, makes the aggregate `unconfirmed`. No live runtimes also means `unconfirmed`. A supported, recent report for an earlier halt leaves the request `pending`; unknown settlement or malformed safety facts yield `unknown`. `draining` means acknowledged work is outstanding; `halted` requires every live runtime to report the current halt with no outstanding work or unknown settlement. Without a halt request, the state is `running`.

Status is best-effort evidence from runtime registrations and reports. It neither certifies PostgreSQL lock settlement nor proves that a stale host is fenced. Older 0.6.15 binaries do not report this protocol and can still become leader and prune. Rolling upgrades are supported, but halt is unreliable until every potential leader is upgraded or fenced. Inspect status before relying on it, and investigate any unobserving runtime.

## Branch coverage

| Leader work | Scope |
| --- | --- |
| Queue, lease and claim rotation and prune | maintenance.prune |
| Terminal count rollup | maintenance.prune |
| Completed/DLQ/batch/snapshot/descriptor cleanup | maintenance.prune |
| Queue health and admin metadata refresh | maintenance.prune |
| Storage auto-finalization and descriptor publication | maintenance.prune |
| Scheduled/retry promotion and priority aging | Unchanged |
| Heartbeat, deadline and callback rescue | Unchanged |
| Cron sync and evaluation | Unchanged |
| Admin batch execution | Unchanged |

The scheduler names branches through an exhaustive enum; an inventory test rejects unclassified registrations. This patch leaves ring defaults and startup capacity checks unchanged and adds no reclamation rate cap. Resuming uses the existing intervals, backoff and per-pass limits. Rehearse the resulting backlog on your workload before a long halt.

The five-scope design remains Proposed in [ADR-051](https://github.com/hardbyte/awa/pull/662); the maintenance-only runtime role is separate architectural work in [ADR-028](adr/028-maintenance-only-runtime-role.md). This patch supplies the prune-only operator control without requiring either full design.

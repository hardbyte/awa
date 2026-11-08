# Upgrading from 0.6.15 to 0.6.16

0.6.16 adds a durable, database-wide halt for `maintenance.prune`. It is a rolling patch upgrade, but older binaries ignore the control and can still become maintenance leader and prune. Upgrade or fence every potential leader before relying on halt.

## Upgrade

1. Install the 0.6.16 CLI.
2. Run `awa migrate` once with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   The schema stays at v40. The repeatable `prune_controls` patch runs after `claim_batch_job_index` and `insert_jobs_compat_batch`.

3. Apply the [runtime and operator grants](runtime-controls.md#install). Runtime roles need `SELECT` on `awa.runtime_controls` and `SELECT, INSERT, UPDATE, DELETE` on `awa.runtime_control_observations`. Operators need `SELECT` on both tables and `awa.runtime_instances`, plus explicit `EXECUTE` on `awa.halt_runtime_scope(text, text, text)` and `awa.resume_runtime_scope(text, uuid)`. Neither role should have direct control-table writes. Reapply these exceptions after blanket grants and remove inherited mutation privileges from the granting role.
4. Roll workers, producers and admin processes to 0.6.16 after applying the grants. Compatible runtimes discover the patch without a restart or leadership change. They can also run before installation, retaining the existing reclamation policy while catalog observations remain fresh. Once they detect installation, missing read grants close prune admission when any existing permission expires, within five seconds; newly started runtimes have no permission to prune. Other scopes continue. If compatible runtimes are already deployed, expect this pause between migration and grants, and complete the grants before expecting pruning to resume.
5. Upgrade or fence every remaining process that can become maintenance leader. Inspect `awa control status` using the operator connection before relying on halt. Every live runtime, including standbys, must have a supported, recent protocol report.

The 0.6.15 lifecycle-hook and finalizer bounds remain in place. Ring defaults, startup capacity checks and reclamation rate limits are unchanged.

## What the migration changes

The patch creates `awa.runtime_controls` and `awa.runtime_control_observations` as ordinary tables, plus functions. The observation table has `instance_id uuid PRIMARY KEY` and `observation jsonb NOT NULL`, with no foreign key. It does not alter `awa.runtime_instances` or any other existing table, refresh the `v023` substrate installer or claim functions, or change storage routing.

The observation JSON includes protocol version `1`, report sequence and server `received_at`; publishing keeps the latest sequence for each instance. Re-running the patch preserves the current halt and explicit operator grants. An interrupted migration can be run again.

For an external migration runner, apply `R__prune_controls.sql` after `V40` and the existing `R__claim_batch_job_index.sql` and `R__insert_jobs_compat_batch.sql` patches. Extract the full repeatable set with:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --extract-to ./sql/awa --from 40
```

Replace all `R__` files together and keep previously applied `V` files unchanged. `--pending` lists only patches the connected database lacks.

## Halt, inspect and resume

Only `maintenance.prune` with mode `drain` is supported. It stops compatible runtimes from admitting reclamation and housekeeping, including descriptor publication; work admitted before observation may finish. Enqueue, dispatch, handlers, cron, promotion, rescue, admin batches and queue health gauges continue. Fresh storage initialization, auto-finalization and storage selection proceed even during halt or an initial control-read failure.

```bash
awa --database-url "$AWA_OPERATOR_DATABASE_URL" control halt \
  --scope maintenance.prune --reason 'Investigating reclamation lock pressure'
awa --database-url "$AWA_OPERATOR_DATABASE_URL" control status
awa --database-url "$AWA_OPERATOR_DATABASE_URL" control resume \
  --scope maintenance.prune --halt '<current halt UUID>'
```

Every halt creates a new UUID. Resume must name the current UUID; it never waits for callback settlement or reconciliation. If a new CLI connection is unavailable, use an established SQL session:

```sql
SELECT * FROM awa.halt_runtime_scope('maintenance.prune', 'Investigating lock pressure', 'drain');
SELECT * FROM awa.resume_runtime_scope('maintenance.prune', '<current halt UUID>'::uuid);
```

Status is asynchronous, best-effort evidence from runtime registrations and reports. It uses the database clock, the standard runtime liveness cutoff of `max(3 × snapshot_interval, 30 seconds)`, and a report recency window of `max(2 × snapshot_interval, 5 seconds)` (twenty seconds with default ten-second snapshots). Control permission still expires after five seconds. `unobserving_runtimes` identifies live runtimes with missing, unsupported, wrong-protocol or stale reports, or permission already stale at report creation, including their identity, version and leader flag. Without a halt request, any such runtime makes status `degraded`, including runtimes that keep prune admission closed because control reads fail. An active halt remains `unconfirmed` while any such runtime is live, or if none are live. A report for an earlier halt remains `pending`, and unknown settlement cannot produce `halted`.

Potentially unsettled reclamation or admin metadata refresh sets `settlement_unknown` for thirty seconds after the latest uncertainty event. `settlement_uncertainty_remaining_ms` reports the remaining window; the cumulative `abandoned_callbacks` counter can stay nonzero after expiry. Status can return `halted` after expiry without backend reconciliation; expiry does not establish PostgreSQL backend settlement or lock release. Standby control reads and report writes use the shared pool with separate one-second budgets, so pool pressure can also make status `degraded` or `unconfirmed`.

A `halted` status is not proof that PostgreSQL released every lock or that stale hosts are fenced. Runtime credentials can forge observations or delete runtime registrations, so confirmation also depends on trusting those credentials. The operator boundary protects the requested halt, not the reported proof. Inspect backend activity and cancel or fence lingering sessions if lock relief is required. See [runtime controls](runtime-controls.md#read-status) for the status fields and states and [runtime control privileges](security.md#runtime-control-privileges) for the trust boundary.

## Roll back

Before rolling back to 0.6.15, fence processes that could prune or explicitly accept losing the halt guarantee: 0.6.15 ignores the control, even when a halt remains stored. Leave the patch installed; its new tables and functions are safe for older binaries, and replay does not resume the fleet.

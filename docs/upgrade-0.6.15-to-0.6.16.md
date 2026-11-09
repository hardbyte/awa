# Upgrading from 0.6.15 to 0.6.16

0.6.16 is a rolling patch upgrade: the schema stays at v40 and traffic keeps running throughout. Batch inserts give way to per-row producers instead of deadlocking with them, `QueueStorage` creates new queue lanes in the same lock order as every other producer, operators can halt and resume `maintenance.prune` across the fleet, and point reads of a job no longer miss it while prune moves it.

Two parts of the upgrade need ordering. The new prune control needs grants before 0.6.16 runtimes can read it, and a halt only holds once no 0.6.15 process can become maintenance leader.

## Upgrade

1. Install the 0.6.16 CLI.
2. Run `awa migrate` once with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   On a 0.6.15 database the output reports `Schema patch claim_batch_job_index: applied`, `Schema patch insert_jobs_compat_batch: applied` and `Schema patch prune_controls: applied`. Each patch commits in its own transaction and none takes a lock on an `awa` or queue-storage table; an interrupted run can be run again to apply the rest. The batch insert fix is in the function, so 0.6.15 producers get it as soon as its patch commits.

3. Apply the [runtime and operator grants](runtime-controls.md#install). The patch does not grant runtime or operator access. Runtime roles need `SELECT` on `awa.runtime_controls` and `SELECT, INSERT, UPDATE, DELETE` on `awa.runtime_control_observations`. Operators need `SELECT` on both tables and on `awa.runtime_instances`, plus explicit `EXECUTE` on `awa.halt_runtime_scope(text, text, text)` and `awa.resume_runtime_scope(text, uuid)`. Neither role should be able to write the control table directly: apply these exceptions after any blanket grants, and remove inherited mutation privileges from the granting role.
4. Roll workers, producers and admin processes to 0.6.16, in any order. Until every `QueueStorage` process runs 0.6.16, see [new queue lanes](#new-queue-lanes).
5. Upgrade or fence every remaining process that can become maintenance leader, standbys included. Then run `awa control status` on the operator connection: before relying on a halt, every live runtime must have a supported, recent report.

Binaries can also go before the migration. A 0.6.16 runtime on a database without `prune_controls` keeps the existing reclamation policy and discovers the patch without a restart. Once the patch is installed, a runtime that cannot read the control stops admitting prune within five seconds, and a newly started one does not admit it at all, until the grants are in place; other maintenance continues. If 0.6.16 runtimes are already running, expect prune to pause between steps 2 and 3, and apply the grants before expecting it to resume.

### External migration runners

`awa migrate --sql --pending` prints `R__claim_batch_job_index`, `R__insert_jobs_compat_batch` and `R__prune_controls` on a 0.6.15 database. Apply them as repeatable scripts after `V40`, in that order, as the schema owner. Write the full repeatable set with:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --extract-to ./sql/awa --from 40
```

Replace all `R__` files together with that set and keep previously applied `V` files unchanged. `--pending` lists only the patches the connected database lacks, so its output is not the full set.

## Batch inserts and per-row producers

`insert_many_copy` and `insert_many_skip_unique_conflicts` insert through `awa.insert_jobs_compat_batch`. `awa migrate` replaces that function's body, which a 0.6.15 database reports as the pending `insert_jobs_compat_batch` patch. The patch is one `CREATE OR REPLACE FUNCTION` with no table DDL, and the function's signature, privileges and results are unchanged.

The function takes a batch's unique claims before its lane locks, while per-row inserts (generic `insert_many`, single-job inserts, and 0.6.14 and earlier binaries) take each job's claim and its lane together. In 0.6.15, a per-row transaction that held a lane the batch needed and then inserted a key the batch had claimed but not committed waited on the batch while the batch waited on it, and PostgreSQL aborted one of them with `deadlock detected` (SQLSTATE `40P01`).

In 0.6.16 the function takes its claims and lanes in a subtransaction. For its first 2 s, each lock wait is limited to what remains of a 500 ms budget from the start of the attempt, or half of its own session's `deadlock_timeout` if that is lower. When a wait would run longer, the batch rolls back every claim and lane it took and tries again after a short randomised pause. The per-row transaction gets the key or the lane, proceeds and commits, and on its next attempt the batch skips the keys that transaction committed, or fails with a unique violation when it does not skip conflicts, as it would for any key committed before it. The same applies to a transaction that enqueues lanes in separate statements in the opposite order to a batch. Two batches through the function still take claims in key order and lanes in lane order, so they cannot deadlock with each other.

After 2 s of retries the batch makes one last attempt that waits as the 0.6.15 function did. Contention that outlasts the retries is then served in lock order, and a cycle that rolling back cannot break ends in `deadlock detected` rather than endless retries.

What callers see:

- A batch insert and a per-row producer that share keys or lanes no longer abort each other with `40P01`, provided the batch call is the first thing its transaction locks and the per-row session's `deadlock_timeout` is above 500 ms (PostgreSQL's default is 1 s). The batch cannot see another session's setting, so its cap is fixed; a per-row session configured at or below 500 ms can still be aborted by its own, earlier deadlock check.
- A batch can only give way while inserting its unique claims fits the 500 ms budget. Each claim costs roughly 20 µs on a fast database server and more on a slow or busy one, so the limit is in the range of 10,000 to 20,000 unique keys. A larger batch, or one whose claims are slowed by load, uses up each bounded attempt before it reaches its lanes and ends in the ordinary attempt, so it behaves as in 0.6.15. Split such batches if they share keys or lanes with per-row producers.
- A per-row transaction that holds a lane for longer than the bounded phase (about 2.5 s) before it inserts one of the batch's keys meets the ordinary attempt, and one of the two is aborted with `40P01` as in 0.6.15. Cycles of three or more transactions can also behave as in 0.6.15.
- A batch whose transaction already holds locks from earlier statements can still be part of a deadlock, because rolling back the batch cannot release them. That cycle now ends after about 3 to 4 s instead of 1 to 2 s, and the transaction usually aborted is the batch's caller rather than the per-row producer.
- A caller's `lock_timeout` applies to each of the batch's lock waits, as it does to any statement's, and a wait it ends fails the call with `lock_not_available` (SQLSTATE `55P03`). Time the batch spends on its own work or on retries does not count against it.
- The 500 ms budget applies to each wait, not to a statement: a claim statement that meets several keys still in flight, or a lane reservation that waits for the lane's provisioning lock and then its lock, can wait up to the remaining budget each time.
- Each lane's enqueue lock is still taken when the batch reaches that lane and held until the calling transaction ends.
- Each call leaves one committed subtransaction in its caller's transaction, so a transaction that makes more than 64 batch calls overflows PostgreSQL's per-backend subtransaction cache. Commit between batches.

## New queue lanes

When `QueueStorage` is the first to use a `(queue, priority)` lane, through a batch enqueue or a worker promoting scheduled or retried jobs, 0.6.16 writes the lane's `queue_lanes` row before taking its provisioning lock, the order SQL inserts already use. 0.6.15 took them the other way round, so a 0.6.15 `QueueStorage` process and any other producer that were both first to use the same lane could deadlock, and one transaction failed with `40P01`. Lanes that already exist are unaffected. This part needs no migration.

Until every `QueueStorage` process runs 0.6.16, the deadlock can still happen between a 0.6.15 one and another producer. To avoid it during the roll, create the lanes of a new queue or priority before traffic first reaches it with the 0.6.16 `awa storage prepare-queue`, or retry the failed transaction.

## Prune halt

The `prune_controls` patch creates `awa.runtime_controls` and `awa.runtime_control_observations` as ordinary tables, plus the halt and resume functions. The observation table has `instance_id uuid PRIMARY KEY` and `observation jsonb NOT NULL`, with no foreign key. The patch does not alter `awa.runtime_instances` or any other existing table, refresh the `v023` substrate installer or claim functions, or change storage routing. Re-running it preserves the current halt and explicit operator grants.

0.6.15 binaries ignore the control, even while a halt is stored, and can still become maintenance leader and prune. Upgrade or fence every process that can lead before relying on a halt.

### Halt, inspect and resume

Only `maintenance.prune` with mode `drain` is supported. It stops compatible runtimes from admitting rotation, prune, terminal count rollup, cleanup, admin metadata refresh and descriptor publication; work admitted before the halt is observed may finish. Enqueue, dispatch, handlers, cron, promotion, rescue, admin batches and queue health gauges continue. Fresh storage initialization, auto-finalization and storage selection proceed even during a halt or an initial control-read failure.

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

The maintenance leader reads the control on its existing advisory-lock session every 0.8 to 1.2 seconds, and its permission to prune expires five seconds after its last successful read.

### Status

Status is asynchronous, best-effort evidence from runtime registrations and reports. It uses the database clock, the standard runtime liveness cutoff of `max(3 × snapshot_interval, 30 seconds)`, and a report recency window of `max(2 × snapshot_interval, 5 seconds)` (twenty seconds with the default ten-second snapshots). `unobserving_runtimes` lists live runtimes with missing, unsupported, wrong-protocol or stale reports, or permission already stale when the report was created, with their identity, version and leader flag. Without a halt request, any such runtime makes status `degraded`, including runtimes that keep prune admission closed because control reads fail. An active halt remains `unconfirmed` while any such runtime is live, or if none are live. A report for an earlier halt leaves the request `pending`, and unknown settlement cannot produce `halted`.

Potentially unsettled reclamation or admin metadata refresh sets `settlement_unknown` for thirty seconds after the latest uncertainty event, and `settlement_uncertainty_remaining_ms` reports the remaining window; the cumulative `abandoned_callbacks` counter can stay nonzero afterwards. Status can return `halted` once that window expires without any backend reconciliation. Standby control reads and report writes use the shared pool with separate one-second budgets, so pool pressure can also make status `degraded` or `unconfirmed`.

A `halted` status is not proof that PostgreSQL released every lock or that stale hosts are fenced. Runtime credentials can forge observations or delete runtime registrations, so confirmation also depends on trusting those credentials; the operator boundary protects the requested halt, not the reported proof. Inspect backend activity and cancel or fence lingering sessions if lock relief is required. See [runtime controls](runtime-controls.md#read-status) for the status fields and states and [runtime control privileges](security.md#runtime-control-privileges) for the trust boundary.

## Claim batch index

0.6.16 replaces the 0.6.15 `<schema>.ensure_claim_batch_job_index(slot)` in each queue-storage schema, so `awa migrate` applies the `claim_batch_job_index` patch again.

- The 0.6.15 function counted a partial GIN index on `job_ids`, or any index already holding the index's name, as the claim batch index, so claim prune could skip building the real one. The new function accepts only a valid, non-partial GIN index on `job_ids` alone. When something else already has the name `lease_claim_batches_<slot>_job_ids_idx`, it raises a warning naming it and leaves it in place, and that slot keeps scanning until you drop or rename it; claim prune builds the index the next time it empties the slot.
- A 0.6.15 refresh run by a migration role that owns the function but may only `SET ROLE` to the table owner (PostgreSQL 16+ membership `WITH INHERIT FALSE`) left `EXECUTE` with `PUBLIC`. Re-applying the patch resets `EXECUTE` to exactly the roles holding `TRUNCATE` on `lease_claim_batches`; see [Claim batch index](security.md#claim-batch-index). Migrations run with the schema owner as the effective role, as above, were not affected.

As in 0.6.15 the patch replaces functions only: it builds no index and takes no lock on any `awa` or queue-storage table. 0.6.15 binaries call the new function unchanged.

## Job reads during prune

`admin::get_job` and the other queue-storage point reads of one job read again in a new snapshot when the first read finds nothing. In 0.6.14 and 0.6.15 a read whose snapshot predated a queue prune could miss a failed job while prune moved its row out of the slot being pruned, and return `JobNotFound`. This part needs no migration and applies to each process once it runs 0.6.16.

## Roll back

Roll binaries back to 0.6.15 at any time and leave the patches installed:

- 0.6.15 binaries call the new `insert_jobs_compat_batch` and `ensure_claim_batch_job_index` with the same arguments, and a 0.6.15 `awa migrate` treats both as applied, so the batch insert and claim index fixes stay in place. Restoring the 0.6.15 batch insert body means applying the 0.6.15 `R__insert_jobs_compat_batch.sql`, which brings the deadlock back.
- The `prune_controls` tables and functions are safe for older binaries, and replaying the patch does not resume the fleet. 0.6.15 ignores the control, even while a halt is stored, so fence every process that could prune or accept losing the halt guarantee before rolling back a potential leader.
- A 0.6.15 `QueueStorage` process can again deadlock with another producer when both are first to use a new lane; see [new queue lanes](#new-queue-lanes).

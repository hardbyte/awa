# Upgrading from 0.6.15 to 0.6.16

0.6.16 adds a durable, database-wide halt for `maintenance.prune`, and batch inserts no longer deadlock with per-row producers. It is a rolling patch upgrade, but older binaries ignore the control and can still become maintenance leader and prune. Upgrade or fence every potential leader before relying on halt.

## Upgrade

1. Install the 0.6.16 CLI.
2. Run `awa migrate` once with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   The schema stays at v40. The output reports `Schema patch claim_batch_job_index: applied`, `Schema patch insert_jobs_compat_batch: applied` and `Schema patch prune_controls: applied`. The `insert_jobs_compat_batch` patch replaces the batch insert function's body; 0.6.15 producers get that fix as soon as the migration commits.

3. Apply the [runtime and operator grants](runtime-controls.md#install). Runtime roles need `SELECT` on `awa.runtime_controls` and `SELECT, INSERT, UPDATE, DELETE` on `awa.runtime_control_observations`. Operators need `SELECT` on both tables and `awa.runtime_instances`, plus explicit `EXECUTE` on `awa.halt_runtime_scope(text, text, text)` and `awa.resume_runtime_scope(text, uuid)`. Neither role should have direct control-table writes. Reapply these exceptions after blanket grants and remove inherited mutation privileges from the granting role.
4. Roll workers, producers and admin processes to 0.6.16 after applying the grants. Compatible runtimes discover the patch without a restart or leadership change. They can also run before installation, retaining the existing reclamation policy while catalog observations remain fresh. Once they detect installation, missing read grants close prune admission when any existing permission expires, within five seconds; newly started runtimes have no permission to prune. Other scopes continue. If compatible runtimes are already deployed, expect this pause between migration and grants, and complete the grants before expecting pruning to resume.
5. Upgrade or fence every remaining process that can become maintenance leader. Inspect `awa control status` using the operator connection before relying on halt. Every live runtime, including standbys, must have a supported, recent protocol report.

The 0.6.15 lifecycle-hook and finalizer bounds remain in place. Ring defaults, startup capacity checks and reclamation rate limits are unchanged.

## New queue lanes during the roll

When `QueueStorage` is the first to use a `(queue, priority)` lane, through a batch enqueue or a worker promoting scheduled or retried jobs, 0.6.16 writes the lane's `queue_lanes` row before taking its provisioning lock, the order SQL inserts already use. 0.6.15 takes them the other way round. Until every `QueueStorage` process runs 0.6.16, a 0.6.15 one and any other producer that are both first to use the same lane can still deadlock, and one transaction fails with `40P01`. Lanes that already exist are unaffected. To avoid it during the roll, create the lanes of a queue or priority before traffic first reaches it, with the 0.6.16 `awa storage prepare-queue`, or retry the failed transaction.

## What the migration changes

### `claim_batch_job_index`

0.6.16 replaces the 0.6.15 `<schema>.ensure_claim_batch_job_index(slot)`, so `awa migrate` applies this patch again. The 0.6.15 function counted a partial GIN index on `job_ids` as the claim batch index, and also any index already holding the index's name, so claim prune could skip building the real one. The new function accepts only a valid, non-partial GIN index on `job_ids`. When something else already has the name `lease_claim_batches_<slot>_job_ids_idx`, it raises a warning naming it and leaves it in place; drop or rename it and claim prune builds the index the next time it empties that slot.

As in 0.6.15 the patch replaces functions only: it builds no index and takes no lock on any `awa` or queue-storage table. Binaries from 0.6.15 call the new function unchanged.

### `prune_controls`

The patch creates `awa.runtime_controls` and `awa.runtime_control_observations` as ordinary tables, plus functions. The observation table has `instance_id uuid PRIMARY KEY` and `observation jsonb NOT NULL`, with no foreign key. It does not alter `awa.runtime_instances` or any other existing table, refresh the `v023` substrate installer or claim functions, or change storage routing.

The observation JSON includes protocol version `1`, report sequence and server `received_at`; publishing keeps the latest sequence for each instance. Re-running the patch preserves the current halt and explicit operator grants. An interrupted migration can be run again.

For an external migration runner, apply the new `R__claim_batch_job_index.sql` again, then `R__prune_controls.sql`, after `V40` and the existing `R__insert_jobs_compat_batch.sql` patch. Extract the full repeatable set with:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --extract-to ./sql/awa --from 40
```

Replace all `R__` files together and keep previously applied `V` files unchanged. `--pending` lists only patches the connected database lacks.

## Unique keys shared with per-row producers

The schema stays at v40. `awa migrate` replaces the body of `awa.insert_jobs_compat_batch`, which `insert_many_copy` and `insert_many_skip_unique_conflicts` call. A 0.6.15 database reports the `insert_jobs_compat_batch` patch as pending until it does. The patch is still one `CREATE OR REPLACE FUNCTION` with no table DDL and no table locks, and the function's signature, privileges and results are unchanged.

The function takes a batch's unique claims before its lane locks, while per-row inserts (generic `insert_many`, single-job inserts, and 0.6.14 and earlier binaries) take each job's claim and its lane together. In 0.6.15, a per-row transaction that held a lane the batch needed and then inserted a key the batch had claimed but not committed waited on the batch while the batch waited on it, and PostgreSQL aborted one of them with `deadlock detected` (SQLSTATE `40P01`).

In 0.6.16 the function takes its claims and lanes in a subtransaction and, while it holds any of them, waits at most 500 ms per lock, or half of its own session's `deadlock_timeout` if that is lower. When a wait would run longer, the batch rolls back what it took and tries again after a short randomised pause. The per-row transaction gets the key or the lane, proceeds and commits, and on its next attempt the batch skips the keys it committed, or fails with a unique violation when it does not skip conflicts, as it would for any key committed before it. The same applies to a transaction that enqueues lanes in separate statements in the opposite order to a batch. Two batches through the function still take claims in key order and lanes in lane order, so they cannot deadlock with each other.

After 2 s of retries, the batch makes one last attempt that waits as the 0.6.15 function did. Contention that outlasts the retries is then served in lock order, and a cycle that rolling back cannot break ends in `deadlock detected` rather than endless retries. The protection therefore lasts only for the bounded phase: a per-row transaction that holds a lane for longer than about 2.5 s and only then inserts one of the batch's keys meets the ordinary attempt, and one of the two is aborted with `40P01` as in 0.6.15.

What callers see:

- A batch insert and a per-row producer that share keys or lanes no longer abort each other with `40P01` when the batch call is the first thing its transaction locks and the per-row producer's `deadlock_timeout` is above 500 ms (PostgreSQL's default is 1 s). The batch cannot see another session's setting, so the cap is fixed; a per-row session configured at or below 500 ms can still be aborted by its own, earlier deadlock check.
- A per-row transaction that holds a lane longer than the bounded phase (about 2.5 s) before it inserts one of the batch's keys can still deadlock with the batch, as in 0.6.15.
- A batch whose transaction already holds locks from earlier statements can still be part of a deadlock: rolling back the batch cannot release them. That cycle now ends later than in 0.6.15, after about 3 to 4 s instead of 1 to 2 s, and the transaction usually aborted is the batch's caller rather than the per-row producer.
- A batch can only give way while inserting its unique claims takes less than the 500 ms budget. Each claim costs roughly 20 µs on a fast database server and more on a slow or busy one, so the limit is in the range of 10,000 to 20,000 unique keys. A larger batch, or one whose claims are slowed by load, uses up each bounded attempt before it reaches its lanes and ends in the ordinary attempt, so it behaves as in 0.6.15. Split such batches if they share keys or lanes with per-row producers.
- Under contention that outlasts the retries, a batch waits for its lanes in lock order, as in 0.6.15.
- A caller's `lock_timeout` applies to each of the batch's lock waits, as it does to any statement's, and a wait it ends fails the call with `lock_not_available` (SQLSTATE `55P03`). Time the batch spends on its own work or on retries does not count against it.
- The 500 ms budget applies to each wait, not to a statement: a claim statement that meets several keys still in flight, or a lane reservation that waits for the lane's provisioning lock and then its lock, can wait up to the remaining budget each time.
- The lane lock is still held only for the lane's reservation, the ready-row writes and the commit.

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

Before rolling back to 0.6.15, fence processes that could prune or explicitly accept losing the halt guarantee: 0.6.15 ignores the control, even when a halt remains stored. Leave the patches installed; the `prune_controls` tables and functions are safe for older binaries, and replay does not resume the fleet. 0.6.15 binaries call the new `ensure_claim_batch_job_index` unchanged, and a 0.6.15 `awa migrate` leaves it in place.

For the batch insert fix, roll binaries back to 0.6.15 at any time and leave the patch in place: 0.6.15 binaries call the same function with the same arguments. Rolling the function back means applying the 0.6.15 `R__insert_jobs_compat_batch.sql`, which brings the deadlock back.

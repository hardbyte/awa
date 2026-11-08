# Upgrading from 0.6.15 to 0.6.16

0.6.16 is a rolling patch upgrade. Traffic keeps running throughout, binaries and `awa migrate` can go in either order, and you can roll binaries back to 0.6.15 with the schema patch left in place.

## Upgrade

1. Install the 0.6.16 CLI.
2. Run `awa migrate` once, with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   The output reports `Schema patch insert_jobs_compat_batch: applied`.

3. Roll workers, producers and admin processes to 0.6.16, in any order.

The fix below is in the database function, so 0.6.15 producers get it as soon as step 2 commits.

## Unique keys shared with per-row producers

The schema stays at v40. `awa migrate` replaces the body of `awa.insert_jobs_compat_batch`, which `insert_many_copy` and `insert_many_skip_unique_conflicts` call. A 0.6.15 database reports the `insert_jobs_compat_batch` patch as pending until it does. The patch is still one `CREATE OR REPLACE FUNCTION` with no table DDL and no table locks, and the function's signature, privileges and results are unchanged.

The function takes a batch's unique claims before its lane locks, while per-row inserts (generic `insert_many`, single-job inserts, and 0.6.14 and earlier binaries) take each job's claim and its lane together. In 0.6.15, a per-row transaction that held a lane the batch needed and then inserted a key the batch had claimed but not committed waited on the batch while the batch waited on it, and PostgreSQL aborted one of them with `deadlock detected` (SQLSTATE `40P01`).

In 0.6.16 the function takes its claims and lanes in a subtransaction and never waits longer than half of `deadlock_timeout` (500 ms with the default of 1 s) while it holds any of them. When a wait would run longer, the batch rolls back what it took and tries again after a short randomised pause. The per-row transaction gets the key or the lane, proceeds and commits, and on its next attempt the batch skips the keys it committed, or fails with a unique violation when it does not skip conflicts, as it would for any key committed before it. The same applies to a transaction that enqueues lanes in separate statements in the opposite order to a batch. Two batches through the function still take claims in key order and lanes in lane order, so they do not make each other retry.

What callers see:

- No `40P01` from a batch insert, or from a per-row producer, caused by the two sharing keys or lanes.
- A batch that keeps losing a busy lane waits for that lane first on its next attempt, before it takes any claim, so it keeps its place in the lane's queue however long other producers hold it.
- A caller's `lock_timeout` bounds the whole call, retries included: the batch fails with `lock_not_available` (SQLSTATE `55P03`) when it is spent, as a per-row insert does. Without one, the batch keeps trying until it succeeds, as a lock wait would.
- The lane lock is still held only for the lane's reservation, the ready-row writes and the commit.

### External migration runners

`awa migrate --sql --pending` prints `R__insert_jobs_compat_batch` on a 0.6.15 database. Apply it after `V40`; it is safe inside the runner's transaction and safe to re-run. Replace the runner's `R__` files together with the full 0.6.16 set from `awa migrate --extract-to <dir> --from 40`, and keep the `V` files exactly as originally applied.

## Roll back

Roll binaries back to 0.6.15 at any time and leave the patch in place: 0.6.15 binaries call the same function with the same arguments. Rolling the function back means applying the 0.6.15 `R__insert_jobs_compat_batch.sql`, which brings the deadlock back.

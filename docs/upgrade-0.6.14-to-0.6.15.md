# Upgrading from 0.6.14 to 0.6.15

0.6.15 is a rolling patch upgrade. Traffic keeps running throughout, binaries and `awa migrate` can go in either order, and you can roll binaries back to 0.6.14 with the schema patch left in place.

## Upgrade

1. Install the 0.6.15 CLI.
2. Run `awa migrate` once, with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   The output reports `Schema patch insert_jobs_compat_batch: applied`.

3. Roll workers, producers and admin processes to 0.6.15, in any order.

Steps 2 and 3 can also run the other way round. A 0.6.15 producer checks for the patch on every `insert_many_copy` call, so it switches to the batch function on its next call after the patch commits, without a restart.

## What `awa migrate` changes

The schema stays at v40. The `insert_jobs_compat_batch` patch adds one function, `awa.insert_jobs_compat_batch`, and changes nothing else: no table DDL, no table or sequence locks, no change to `awa.insert_job_compat` or to storage routing. It applies as one transaction, so an interrupted run leaves nothing applied; run it again.

The function inserts a whole batch the way `awa.insert_job_compat` inserts one job, with the same validation, unique claims, lane assignment and storage routing. It does everything that does not depend on a lane first (job ids, unique claims in key order, new lane rows, scheduled rows, the result rows), then reserves each lane's range of positions once, in `(queue, priority, shard)` order, and writes the ready rows. Each lane's enqueue lock is therefore held only for the reservation, the ready-row write and the commit, instead of from the batch's first ready row to the commit. Concurrent producers and promotion on the same lane wait for that much shorter window.

What callers see:

- `insert_many_copy` (Rust) and `Client.insert_many_copy` (Python) use the function when it is installed and the connected role may execute it, otherwise the per-row path they used before. Their results are unchanged: rows in input order, jobs whose unique key is already claimed skipped.
- `insert_many_skip_unique_conflicts` is new in Rust. It inserts a batch through the function and skips jobs whose unique key is already claimed, returning the inserted rows in input order. It needs the patch and returns a validation error without it.
- Generic `insert_many` is unchanged: one statement calling `awa.insert_job_compat` per row, on every schema, and a unique conflict still fails the whole batch.
- A batch's rows in one lane occupy consecutive lane positions in input order, as before, and are described by one ready segment per run instead of one per row. A key repeated inside one batch keeps its first job in input order.

0.6.14 and earlier binaries never call the new function. They keep calling `awa.insert_job_compat`, which takes the same lane lock, so old and new producers share a lane without reordering it.

### Privileges

New functions are executable by `PUBLIC` unless your deployment revokes that through default privileges. If it does and the runtime role's default privileges do not cover functions created by the migration role, grant it explicitly:

```sql
GRANT EXECUTE ON FUNCTION awa.insert_jobs_compat_batch(
    text[], text[], jsonb[], text[], smallint[], smallint[], timestamptz[],
    jsonb[], jsonb[], bytea[], text[], text[], boolean
) TO awa_runtime;
```

Without `EXECUTE`, `insert_many_copy` keeps the per-row path and `insert_many_skip_unique_conflicts` fails with `permission denied`.

### External migration runners

`awa migrate --sql --pending` prints the `R__insert_jobs_compat_batch` patch on a 0.6.14 database. To write it as a file instead, run `awa migrate --extract-to <dir> --pending` and apply `R__insert_jobs_compat_batch.sql` as a repeatable script after `V40`. It is a single `CREATE OR REPLACE FUNCTION`, safe inside the runner's transaction and safe to re-run. Run it as the role that owns the existing `awa` functions, normally the schema owner.

Replace the runner's `R__` files with the set the 0.6.15 CLI extracts, together, and keep the `V` files exactly as originally applied.

## Roll back

Roll binaries back to 0.6.14 at any time. Leave the patch in place: released binaries do not call the function, and it holds no state.

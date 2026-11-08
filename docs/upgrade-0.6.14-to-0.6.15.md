# Upgrading from 0.6.14 to 0.6.15

0.6.15 is a rolling patch upgrade. Traffic keeps running throughout, binaries and `awa migrate` can go in either order, and you can roll binaries back to 0.6.14 with the schema patches left in place.

## Upgrade

1. Install the 0.6.15 CLI.
2. Run `awa migrate` once, with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   The output reports `Schema patch claim_batch_job_index: applied` and `Schema patch insert_jobs_compat_batch: applied`.

3. Roll workers, producers and admin processes to 0.6.15, in any order.

Steps 2 and 3 can also run the other way round. A 0.6.15 producer checks for the batch function on every `insert_many_copy` call, so it switches to it on its next call after the patch commits, without a restart. Claim prune starts indexing claim partitions on its next pass after the patch commits.

## What `awa migrate` changes

The schema stays at v40. `awa migrate` applies two repeatable schema patches, each in its own transaction. An interrupted run can leave the first applied and the second not; run it again to apply the rest. Neither takes a lock on any `awa` or queue-storage table, so both are safe under live traffic.

### `claim_batch_job_index`

The patch adds `<schema>.ensure_claim_batch_job_index(slot)` to every queue-storage schema and builds no index itself. Claim prune calls it right after truncating a claim partition, which builds a GIN index on `lease_claim_batches.job_ids` while the partition is empty. Every partition gains the index within one claim-ring cycle, and live claims never wait behind a build. Don't build the index yourself on a populated partition: the build holds a `SHARE` lock that blocks claim inserts into that partition, for about half a minute per 100k batches.

With the index, by-job lookups of zero-deadline (compact) receipt claims stop reading every retained claim batch. These are heartbeat and progress flush, completion fallbacks, receipt close, cancellation, rescue and `load_job`. Under load those scans could pick parallel plans and exhaust PostgreSQL's dynamic shared memory. A partition without the index yet returns the same rows by scan.

The function is `SECURITY DEFINER` and owned by the owner of `lease_claim_batches`. `EXECUTE` is reset to exactly the roles holding `TRUNCATE` on that table each time the patch runs; see [Claim batch index](security.md#claim-batch-index). A runtime that cannot execute it skips the build and keeps claiming.

### `insert_jobs_compat_batch`

The patch adds one function, `awa.insert_jobs_compat_batch`, and changes nothing else: no table DDL, no table or sequence locks, no change to `awa.insert_job_compat` or to storage routing.

The function inserts a whole batch the way `awa.insert_job_compat` inserts one job, with the same validation, unique claims, lane assignment and storage routing. It does everything that does not depend on a lane first (job ids, unique claims in key order, scheduled rows, the result rows). Then, in `(queue, priority, shard)` order, it registers each lane it is the first to use and reserves each lane's range of positions once, as every other producer registers and reserves lane by lane, and writes the ready rows. Each lane's enqueue lock is therefore held only for the reservation, the ready-row write and the commit, instead of from the batch's first ready row to the commit. Concurrent producers and promotion on the same lane wait for that much shorter window.

What callers see:

- `insert_many_copy` (Rust) and `Client.insert_many_copy` (Python) use the function when it is installed and the connected role may execute it, otherwise the per-row path they used before. Their results are unchanged: rows in input order, jobs whose unique key is already claimed skipped.
- `insert_many_skip_unique_conflicts` is new in Rust. It inserts a batch through the function and skips jobs whose unique key is already claimed, returning the inserted rows in input order. It needs the patch and returns a validation error without it.
- Generic `insert_many` is unchanged: one statement calling `awa.insert_job_compat` per row, on every schema, and a unique conflict still fails the whole batch.
- A batch's rows in one lane occupy consecutive lane positions in input order, as before, and are described by one ready segment per run instead of one per row. A key repeated inside one batch keeps its first job in input order.

0.6.14 and earlier binaries never call the new function. They keep calling `awa.insert_job_compat`, which takes the same lane lock, so old and new producers share a lane without reordering it.

#### Unique keys shared with per-row producers

The batch function takes all of a batch's unique claims before any lane lock. Per-row inserts (generic `insert_many`, single-job inserts, and 0.6.14 and earlier binaries) take each job's claim and its lane together, lane by lane. When a per-row transaction already holds a lane the batch needs and then inserts a job whose unique key the batch has claimed but not yet committed, each waits on the other. PostgreSQL aborts one of them with `deadlock detected` (SQLSTATE `40P01`), where before the per-row insert would have waited and then skipped the job or failed with a unique violation (`23505`). Retry the aborted transaction: by then the key is either committed, giving the old outcome, or free. Two batches through the function take claims in key order and lanes in lane order, so they do not deadlock with each other this way.

#### Privileges

New functions are executable by `PUBLIC` unless your deployment revokes that through default privileges. If it does and the runtime role's default privileges do not cover functions created by the migration role, grant it explicitly:

```sql
GRANT EXECUTE ON FUNCTION awa.insert_jobs_compat_batch(
    text[], text[], jsonb[], text[], smallint[], smallint[], timestamptz[],
    jsonb[], jsonb[], bytea[], text[], text[], boolean
) TO awa_runtime;
```

Without `EXECUTE`, `insert_many_copy` keeps the per-row path and `insert_many_skip_unique_conflicts` fails with `permission denied`.

### External migration runners

`awa migrate --sql --pending` prints the `R__claim_batch_job_index` and `R__insert_jobs_compat_batch` patches on a 0.6.14 database. Apply them as repeatable scripts after `V40`. Both are safe inside the runner's transaction and safe to re-run. Run them as the schema owner, which owns the existing `awa` functions and the queue-storage tables.

`--pending` lists only the patches that database lacks, so its output is not the full repeatable set. Write the full set with `awa migrate --extract-to <dir> --from 40`, which extracts every 0.6.15 `R__` file and no `V` files. Replace the runner's `R__` files with that set, together, and keep the `V` files exactly as originally applied.

## Worker memory when completions fall behind

A 0.6.15 worker bounds how many finished attempts may wait for their completion to commit: the larger of its worker capacity and twice the completion batch size times the completion shard count. With the queue-storage defaults (`AWA_COMPLETION_BATCH_SIZE=512`, one shard) and fewer than 512 workers, that is 1,024 attempts. A handler that finishes while the bound is reached keeps its worker slot until a completion commits, so the worker stops claiming new jobs instead of holding the backlog, and each waiting attempt's payload, in memory.

0.6.14 and earlier admitted every finished attempt. When completions fell behind handlers, for example under lock contention on the completion tables, worker memory grew by roughly 10 KB per waiting attempt plus its payload until the process was killed, and did not shrink afterwards.

What changes in practice:

- A worker whose completions stall shows its workers busy and its claim rate dropping to its completion rate, instead of its memory climbing. Attempts keep heartbeating while they wait, so they are not rescued as stale.
- Jobs with large payloads hold more memory per waiting attempt. Lower `AWA_COMPLETION_BATCH_SIZE` to lower the bound in proportion.
- If you reduced worker counts to keep 0.6.14 workers below their completion rate, you can restore them.

## Roll back

Roll binaries back to 0.6.14 at any time. Leave the patches in place: 0.6.14 binaries neither call the functions nor use the claim batch index, and neither patch holds state.

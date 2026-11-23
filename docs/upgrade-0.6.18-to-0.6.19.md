# Upgrading from 0.6.18 to 0.6.19

0.6.19 is a rolling patch upgrade: the schema stays at v40 and traffic keeps running throughout. It fixes jobs that queue storage could lose or leave stuck, deadlocks between unique jobs, and session advisory locks that a cancel could leave held. It also runs operator bulk actions in bounded batches and sends wake-ups after commit.

Two things need care during the roll:

- **Retrying jobs cancelled as duplicates.** Retry them only from 0.6.19 processes until no 0.6.18 process that retries jobs is left. See [duplicate retries during the roll](#duplicate-retries-during-the-roll).
- **Producers that enqueue inside `REPEATABLE READ` or `SERIALIZABLE` transactions on queue storage.** Keep them on `READ COMMITTED` until every producer and the maintenance leader run 0.6.19. See [producers above `READ COMMITTED`](#producers-above-read-committed).

Otherwise, binaries and the migration can roll in either order.

## Upgrade

1. Install the 0.6.19 CLI.
2. Run `awa migrate` once with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   On a 0.6.18 database the output reports three schema patches applied: `ring_generation_floor`, `sequence_lock_cancel_release` and `spent_claim_prefix_gaps`. None of them takes a table lock; they replace functions, and `ring_generation_floor` also adds one sequence and one function to each queue-storage schema.

3. Roll workers, producers and admin processes to 0.6.19, in any order, subject to the two cautions above.

0.6.18 binaries run unchanged against the patched schema.

Every release-branch CI run rehearses the migrate-first order on PostgreSQL 17 and 18 from released 0.6.9, 0.6.15, 0.6.16 and 0.6.17 (`awa/tests/patch_upgrade_rehearsal_test.rs`): released workers and producers keep running while 0.6.19's `awa migrate` applies the pending patches, released and 0.6.19 workers then run together, and the fleet rolls back to the released binaries with every job accounted for.

### If a patch is reported as still missing

`awa migrate` names each queue-storage schema a patch could not finish, and exits with an error. Two causes are expected:

- **A schema being prepared.** The patches do not wait for a queue-storage schema's install lock, which `prepare_schema()` holds while it installs. When the lock is taken, the patch fails with SQLSTATE `55P03` and changes nothing. Run `awa migrate` again.
- **A restricted migration role.** A role that may only `SET ROLE` to a schema's owner (PostgreSQL 16+ membership `WITH INHERIT FALSE`) refreshes that schema's functions as the owner. A role that can do neither leaves the old functions in place with a warning naming the schema. Run `awa migrate` as a role that owns the schema's functions, or can `SET ROLE` to their owner.

### External migration runners

`awa migrate --sql --pending` prints the three new `R__` scripts on a 0.6.18 database: `R__ring_generation_floor.sql`, `R__sequence_lock_cancel_release.sql` and `R__spent_claim_prefix_gaps.sql`. Apply them after `V40` and after the other `R__` scripts, in name order, as the owner of the installer and the queue-storage functions. Each of them rewrites functions in place that other scripts install in their released form:

- The scripts that embed the released installer (`R__prepared_lane_sequences.sql`, `R__definer_lane_provisioning.sql`, `R__claim_batch_job_index.sql`, `R__correlated_claim_tombstones.sql` and `R__replica_identity.sql`) put the released installer back, which undoes all three.
- `R__insert_jobs_compat_batch.sql` puts the released batch producer back, which undoes `R__ring_generation_floor.sql`.

Name order applies the new scripts after all of those. A runner that re-runs only the repeatables whose checksum changed, such as Flyway, can re-run one of those scripts and skip the new ones, so re-run all three new scripts whenever any of them runs. With Flyway, also install the three as `afterMigrate` callbacks; each is idempotent and changes nothing when its functions are already current. `awa migrate` re-applies them itself when needed.

Extract the full set with:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --extract-to ./sql/awa --from 40
```

Replace all `R__` files together with that set and keep previously applied `V` files unchanged. `awa migrate --sql` runs the pending patches in one transaction that takes the same advisory lock as `awa migrate`; pipe it to `psql -v ON_ERROR_STOP=1`.

## Producers above `READ COMMITTED`

On queue storage, a job enqueued inside a `REPEATABLE READ` or `SERIALIZABLE` transaction could be written into an older ring generation than a job already committed ahead of it in the same lane (#847). Claims then moved the lane past it, so it was never run, and queue prune later deleted it. This affected every producer path: `awa.insert_job_compat`, `awa.insert_jobs_compat_batch` and the Rust and Python batch enqueues. Producers on `READ COMMITTED`, PostgreSQL's default, were not affected.

0.6.19 keeps a generation floor in each queue-storage schema, a sequence that snapshots don't hide. Every producer raises it while it holds its lane lock, and the maintenance leader raises it after each rotation. Producers write into the newer of the floor and the generation they can see. Prune also counts unclaimed jobs, leases and open claims of any generation in a slot, so a job written into an older generation holds its slot until it is claimed. The `ring_generation_floor` patch adds the sequence and the function `enqueue_ring_cursor()` to each queue-storage schema; the function runs as the owner of the queue ring and any role may execute it, so runtime roles need no new grant.

The floor protects a lane only once every process that enqueues into it, and the maintenance leader, run 0.6.19: a 0.6.18 producer or leader does not raise it. Until then, a `REPEATABLE READ` or `SERIALIZABLE` producer can still land behind a 0.6.18 producer's job in an older generation. So:

1. Keep producers that enqueue on queue storage on `READ COMMITTED` until the roll is complete. A producer that sets `default_transaction_isolation` on its role or database, or opens its transactions at a higher level, should enqueue in a `READ COMMITTED` transaction for now.
2. After every producer and worker runs 0.6.19, higher isolation levels are safe. A `SERIALIZABLE` producer can still see serialization failures (SQLSTATE `40001`) from its own transaction; retry them as usual.

A job that a stale snapshot already placed behind its lane's claim cursor before the upgrade is not recovered: claims have moved past it, and prune can still delete it. If you ran such producers on 0.6.18 or earlier and suspect lost jobs, compare your own enqueue records with awa's.

## Duplicate retries during the roll

0.6.19 cancels an attempt as a duplicate when a newer job holds its unique key, and a due scheduled or retryable job when another job holds its key at promotion. When the job's `unique_states` include `cancelled`, 0.6.19 clears that state from the stored mask, so the cancelled row does not contend for the key, and records the original mask in the cancellation's error entry. A 0.6.19 retry restores it.

A 0.6.18 process that retries such a job, singly or in bulk, through the Rust or Python API, the CLI or the web UI, still takes the job's unique claim and fails while the newer job holds it, but it keeps the cleared mask, so the job does not hold its key if it is cancelled again. No stored form avoids this for 0.6.18.

During the roll, don't retry jobs whose last error ends in `as duplicate: unique claim held by a newer job` or `as duplicate: unique claim held by another job` from a 0.6.18 process. Retry them from a 0.6.19 process, or wait until no 0.6.18 process that retries jobs is left. Only jobs whose `unique_states` include `cancelled` are affected. This is an exception to awa's rolling-upgrade policy, under which any mix of adjacent patch releases may share a database.

A duplicate that 0.6.18 or earlier cancelled during a storage transition and stored without its unique key can no longer be retried: 0.6.19 rejects the retry with a unique conflict, and a bulk retry skips and reports it. Enqueue such a job afresh to run it again.

## Bulk actions run in batches

`awa job retry-failed`, `awa job discard`, `awa queue drain`, `awa dlq retry-bulk`, `awa dlq move` and `awa dlq purge`, the matching web UI actions, HTTP routes and Python methods now change at most `--batch-size` rows (default 1,000) per transaction until none match. They are no longer atomic: a run that stops on an error keeps the batches it committed and reports them. A run covers the jobs that existed when it started, so it finishes against a busy producer. A job whose retry would violate a unique claim is skipped and reported, and the CLI exits 2. Scripts that relied on all-or-nothing behaviour should check the reported counts. The Rust library functions keep their single transaction in 0.6 and gain batched counterparts. See [bulk admin operations](bulk-admin.md).

During the roll a 0.6.18 web UI works against a 0.6.19 server and the other way round.

## Wake-ups after commit

On queue storage, enqueues and retries that commit their own transaction now send their `NOTIFY` after the commit, coalesced to at most one per queue per 50 ms from each pool. This removes contention on PostgreSQL's cluster-wide notify lock. No configuration or schema change is needed. A short-lived Rust producer that enqueues and then exits or closes its pool should call `awa::flush_wakeups(&pool, timeout)` first; `Client::shutdown`, the Python client's `close()` and the CLI already do. A wake-up lost on exit only delays the claim until the claimer's next poll.

## Cancelled migrations

`awa migrate` holds a session advisory lock for the whole run. On PostgreSQL 14 and later on Linux, a run whose process is cancelled or exits releases it within about a second, even while a statement is still waiting on the server. On servers that cannot detect a closed client connection, a stranded statement keeps the lock until it finishes; end it with `pg_terminate_backend`. [Schema migrations](migrations.md) shows how to find it.

## Rollback

Roll binaries back to 0.6.18 in any order. Leave the patches in place: 0.6.18 runs unchanged against the patched schema.

Once any producer or the maintenance leader is back on 0.6.18, the generation floor no longer protects `REPEATABLE READ` and `SERIALIZABLE` producers; move them to `READ COMMITTED` before rolling back. Jobs that 0.6.19 cancelled as duplicates keep the cleared `unique_states` mask; retry them from 0.6.19 if you can, as above.

# Upgrading from 0.6.17 to 0.6.18

0.6.18 is a rolling patch upgrade: the schema stays at v40 and traffic keeps running throughout. Every awa table gets a replica identity, so awa keeps working when its schema is in a logical-replication publication.

Binaries and the migration can roll in either order.

## Upgrade

1. Install the 0.6.18 CLI.
2. Run `awa migrate` once with the schema owner as the effective role, while workers and producers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

   On a 0.6.17 database the output reports `Schema patch replica_identity: applied`. Unlike the earlier 0.6 patches, it takes table locks; see [replica identity locks](#replica-identity-locks).

3. Roll workers, producers and admin processes to 0.6.18, in any order.

0.6.17 binaries run unchanged against the patched schema.

### External migration runners

`awa migrate --sql --pending` prints `R__replica_identity` on a 0.6.17 database. Apply it as a repeatable script after `V40` and after the other `R__` scripts, as the owner of the awa and queue-storage tables. Extract the full set with:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --extract-to ./sql/awa --from 40
```

Replace all `R__` files together with that set and keep previously applied `V` files unchanged.

## Replica identity locks

PostgreSQL rejects `UPDATE` and `DELETE` on a table that is in a logical-replication publication and has no primary key, replica-identity index or `FULL` identity. On 0.6.17, 19 awa tables have none: `awa.admin_dirty_queue_marks`, `awa.admin_dirty_kind_marks`, `awa.job_unique_claims` and the 16 partitions of `awa.queue_terminal_count_deltas`, plus the partitions of any custom queue-storage schema. With the awa schema in a `FOR ALL TABLES` or `FOR TABLES IN SCHEMA awa` publication, the admin dirty-mark drain fails on every maintenance tick and unique-claim release fails.

The `replica_identity` patch runs `ALTER TABLE ... REPLICA IDENTITY` on each of those tables that lacks an identity:

- `REPLICA IDENTITY FULL` on the two mark tables, and on `queue_terminal_count_deltas` and each of its partitions in every queue-storage schema. PostgreSQL 17 and 18 do not copy a partitioned table's identity to existing or new partitions, so the parent and each partition are set, and the queue-storage installer sets them for schemas prepared later.
- `REPLICA IDENTITY USING INDEX idx_awa_jobs_unique` on `awa.job_unique_claims`.

Each `ALTER` takes `ACCESS EXCLUSIVE` on its table, and the `USING INDEX` form also takes `SHARE` on `idx_awa_jobs_unique`. These locks block every read and write of the table, including the trigger inserts every canonical job transition makes into the mark tables and every unique insert, and they are held until the patch's transaction commits. What to expect:

- Once granted, the locks last a few milliseconds: the change is catalog-only and rewrites no data.
- The wait is bounded. Each lock wait is limited by the migrating session's `lock_timeout`, or 300 ms when it has none. While one lock waits, statements on that table queue behind it, so a writer can stall for up to that bound, plus the time the patch holds locks it has already taken.
- A lock that cannot be granted in time, for example behind a long transaction that wrote to a mark table, fails the patch with SQLSTATE `55P03` naming the table. Nothing is changed; run `awa migrate` again, or end the long transaction first.
- A table that already has its identity is skipped, so a re-run after success takes no lock.
- With `awa migrate --sql`, all pending patches run in one transaction and the locks are held until it commits; `R__replica_identity` comes last so that is immediately after it.

Run the patch at a quiet moment if writers cannot tolerate a stall of a few hundred milliseconds. Replica identity changes only what PostgreSQL writes to WAL for `UPDATE` and `DELETE`: the mark tables log an old-row image of about 40 bytes per drained mark, and the delta tables are only inserted into and truncated, so they log nothing extra.

Keeping the awa schema out of publications remains the recommendation; see [logical replication and CDC](deploying-on-managed-postgres.md#logical-replication-and-cdc).

## Rollback

Roll binaries back to 0.6.17 in any order. Leave the patch in place: 0.6.17 runs unchanged against tables with a replica identity.

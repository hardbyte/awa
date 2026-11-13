# Upgrading from 0.6 to 0.7

The 0.6 → 0.7 upgrade is short: **the storage step is "be finalized."** Everything else in
0.7 is additive.

## Restricted runtime roles and v049

v049 refreshes lane-sequence helpers in the default and existing custom
queue-storage schemas without changing function signatures or cursor semantics.
Existing provisioned lanes run without schema CREATE; single-role owners retain
lazy creation. The migration adds no runtime version gate or authority flip.

A [migrate-first rehearsal with released `awa-pg==0.6.7`](https://github.com/hardbyte/awa/pull/501#compatibility-evidence)
verified a restricted worker completing jobs on the upgraded schema. That
rehearsal does not cover concurrent mixed-version rollouts.

External migration runners must apply the complete generated
`awa migrate --sql --from 48 --to 49` bundle: it refreshes the installer
definition before replacing lane helpers in the installed schemas. From v053,
runtimes without DDL privileges create lanes themselves through the schema
owner's provisioning functions, so `awa storage prepare-queue` is optional.

## Lane ownership and v053

Before v053 a lane's sequences belonged to the role that first enqueued into,
claimed from or promoted into it. With several runtime roles, only roles whose
default privileges covered that role could use the lane; any other role failed
with `permission denied for sequence`. v053 installs `SECURITY DEFINER`
`provision_lane` and `provision_lane_sequences` functions, owned by the schema
owner, in each queue-storage schema. The lane helpers create sequences through
them, with grants that mirror the lane's head table. Once v053 commits,
`awa migrate` gives every existing lane sequence to the schema owner and adds
its missing grants, as far as it is allowed to, one sequence per short
transaction with a bounded lock wait.

0.6 and earlier 0.7 binaries call the same helpers, so lanes they create after
v053 are owned by the schema owner too. A 0.7 binary running ahead of v053
detects that `provision_lane` is missing and creates lanes as before.

To adopt it:

1. Run `awa migrate` with the schema owner as the effective role, as described
   in [Database roles](security/database-roles.md).
2. Check the remaining state: `awa storage lane-sequences` (read-only, exits 2
   while lanes need repair). This command requires v053 to be installed.
3. Read the migrate output. Any lane sequence it could not repair is listed in
   a warning. A busy one needs another run of
   `awa storage lane-sequences --repair` as the migration role (without
   `PGOPTIONS`); the others name the roles that can repair them. See
   [Check and repair lane ownership](security/database-roles.md#check-and-repair-lane-ownership).

External migration runners apply the generated `awa migrate --sql --from 52
--to 53` bundle, which does not repair existing lane sequences, then run
`awa storage lane-sequences --repair` as the migration role itself.

### New queue lanes during the roll

When `QueueStorage` is the first to use a `(queue, priority)` lane, through a batch enqueue or a worker promoting scheduled or retried jobs, it writes the lane's `queue_lanes` row before taking its provisioning lock, the order SQL inserts already use. 0.6.15 and earlier, and 0.7 builds before this fix, take them the other way round. Until every `QueueStorage` process is upgraded, an older one and any other producer that are both first to use the same lane can still deadlock, and one transaction fails with `40P01`. Lanes that already exist are unaffected. To avoid it during the roll, create the lanes of a queue or priority before traffic first reaches it with `awa storage prepare-queue`, or retry the failed transaction. No schema change is involved.

## Custom-schema discovery during migration

Migration bundles discover custom substrates using catalog metadata without
evaluating their views or functions. Schemas, relations, routines and types
must belong to the effective migration role or the owner of the `awa` schema.
Sequences may belong to another role, because they run no code.
The claim allocator signature must exist and ring-state relations must be
ordinary tables. `public`, `information_schema` and `pg_*` are excluded.

Only those same owners may hold schema `CREATE` or table `TRIGGER` privileges.
Every trigger in the schema must be one awa installs, with exactly the
definition awa gives it. A trigger needs only the `TRIGGER` privilege to
create or replace, and its `WHEN` clause or arguments can run code that no
catalog dependency records, so an added or altered trigger, including one of
your own on an awa table, makes the schema untrusted. This prevents an
outsider from planting another object in an otherwise owned substrate, or
racing discovery with new objects. Role membership alone does not establish
trust, and superusers do not bypass these checks.

Unregistered unsafe custom schemas are skipped. Unsafe registered or prepared
targets abort the pending bundle, so the global version cannot advance while
leaving a configured substrate behind. Registry and transition references are
checked separately; unnamed legacy prepared targets also check `awa_exp`.
Use an explicit schema in transition metadata to disambiguate that legacy case.
Unsafe ownership or privileges in the default `awa` schema abort before the
installer runs. Use a dedicated owner role consistently, including `SET ROLE`
when connecting through a different login.
Review separately owned objects, remove triggers awa did not install, and remove untrusted creation/trigger grants
before migrating; a blanket role grant is not a substitute for this review.

A refused migration names the schema and up to five offending objects with the
rule each breaks, for example `relation awa.jobs_hot TRIGGER granted to
awa_runtime`. Nothing is applied. An unregistered schema that fails the check is
reported as a warning (`skipping untrusted queue-storage schema`) and left at
its current version. The common causes and their fixes, run as the `awa`
schema owner:

- **`GRANT ALL ON ALL TABLES` to the runtime role.** `ALL` includes `TRIGGER`.
  Grant the documented privileges instead, and fix default privileges the same
  way:

  ```sql
  REVOKE TRIGGER, REFERENCES ON ALL TABLES IN SCHEMA awa FROM awa_runtime;
  ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
    REVOKE TRIGGER, REFERENCES ON TABLES FROM awa_runtime;
  ```

- **A 0.6 split-role install whose runtime holds `CREATE` on the schema.** 0.6
  runtimes created lane sequences themselves, so they needed `CREATE`.
  Sequences they own are accepted; the `CREATE` grant is not, because it lets
  the runtime add objects while a migration runs. Until the migration refreshes
  them, the 0.6 lane helpers and enqueue-head triggers run
  `CREATE SEQUENCE IF NOT EXISTS` on every reservation, and that checks
  `CREATE` even for a sequence that exists, so revoking the grant makes running
  0.6 producers and workers fail with `42501` until the migration commits.
  Provisioning lanes in advance does not avoid this. Take a short pause:

  1. Stop 0.6 producers and workers, or hold them at a maintenance gate, so no
     enqueue or claim runs.
  2. Revoke the grant and commit:

     ```sql
     REVOKE CREATE ON SCHEMA awa FROM awa_runtime;
     ```

  3. Run `awa migrate` as the migrator. The bundle refreshes the lane helpers
     to use provisioned sequences without `CREATE` (v049).
  4. Provision any lanes the 0.7 runtimes will need, as the migrator:

     ```bash
     PGOPTIONS='-c role=awa_owner' \
       awa --database-url "$AWA_MIGRATOR_DATABASE_URL" storage prepare-queue \
       --queue <queue> --enqueue-shards <n>
     ```

  5. Start the 0.7 runtimes.

  An uninterrupted path would need the revoke and the helper refresh to become
  visible to every session at once; that has not been verified, so do not
  combine them into one transaction as a substitute for the pause.

- **A custom schema prepared by a runtime or another role.** A worker that
  auto-prepares a schema, or a DBA who ran the preparation as another user,
  leaves it owned by that role. Transfer the schema and every object the error
  lists to the `awa` schema owner, for example
  `ALTER SCHEMA tenant_qs OWNER TO awa_owner` and `ALTER TABLE ... OWNER TO`,
  `ALTER FUNCTION ... OWNER TO` for each listed object. If the runtime and the
  migrator are deliberately the same role, connect the migration as that role.

### Existing migration history and external runners

The built-in runner records migration versions, descriptions and application times;
it does not store SQL checksums or automatically rerun applied steps. These security
checks harden pending historical discovery loops, because an appended migration
would execute after those loops had already inspected untrusted objects. Standalone
steps receive only the small discovery bootstrap, without redefining the installer.

An already-current database has no pending discovery loop and does not run this
bootstrap; it need not receive the internal helper until a later bundle uses it. No new
schema version or runtime capability is introduced. Older binaries and previously
exported SQL retain the old behavior: use an updated binary and regenerate the
**pending** SQL bundle for upgrades. For external runners that track checksums,
preserve files already applied and generate a new pending bundle; do not overwrite
their historical files or reset checksum records to accept changed contents.
For example, extract with `awa migrate --pending --extract-to ./pending-migrations`
into a new directory. Execute complete generated bundles; individual discovery
tail files in the repository do not include their bootstrap prerequisites.

## v050: one lane index per ready partition

v050 drops `idx_<schema>_ready_<slot>_lane_shard` from every `ready_entries`
partition in the default schema and in the custom schemas that
`awa.queue_storage_schemas()` selects (see
[Custom-schema discovery](#custom-schema-discovery-during-migration)); other
schemas keep their indexes. The partition primary
key `(ready_slot, queue, priority, enqueue_shard, lane_seq)` serves every
per-lane lookup because current binaries pin `ready_slot` in claims, in the
queue prune and terminal-count fold gates, and in the maintenance lag gauge
(through `ready_segments`).

What the migration locks: it redefines `awa.install_queue_storage_substrate`
(no schema is reinstalled) and then drops the indexes in slot order. Each
`DROP INDEX` takes ACCESS EXCLUSIVE on its `ready_entries` partition and holds
it until the migration transaction commits, so enqueues and claims touching
that partition wait for the commit. No rows are scanned or rewritten; on a
scratch PostgreSQL 17 database with 409,600 ready rows the whole 49 → 50 step
took 27 ms. When the session has no `lock_timeout`, the migration sets a 2 s
timeout for each lock acquisition. It bounds each wait, not the total
migration time: every lock already taken is kept until commit, so a
migration that waits on several partitions holds the earlier ones meanwhile.
"Only ready partitions" describes the application tables it locks; the drop
also locks the dropped indexes and the catalog rows it changes. If an
acquisition times out (SQLSTATE `55P03`, typically behind a long-running read
of `awa.jobs` or a backup) the error names the partition, the whole migration
rolls back, and it can simply be re-run. Locks taken are held until commit,
about 60 per 16-slot schema, so a database with hundreds of queue-storage
schemas may need a larger `max_locks_per_transaction` for this step.
`queue_counts_exact`, which filters ready rows by queue across every slot,
seq-scans the ready partitions on PostgreSQL 17 without the index; at
200,000 ready rows across 20 queues it measured 177 ms against 136 ms. External runners apply the complete
generated `awa migrate --sql --from 49 --to 50` bundle.

Rollout order: current binaries' SQL works with or without the index, so
producers and workers without a readiness gate can roll before the
migration. Health-gated workers cannot: `/readyz` reports a worker ready only
once the schema reaches the binary's version, so a v050 worker on a v049
schema stays unready and a readiness-gated rollout stalls before its
migration step. For those, migrate first. Pre-v050 binaries keep working
against a v050 schema, but on PostgreSQL 17 and earlier their maintenance lag
gauge, queue prune gate and terminal-count fold gate fall back to sequential
scans of every ready partition until they are replaced (PostgreSQL 18 can
skip-scan the primary key), so keep that window short. Rolling back a binary is
therefore safe for correctness; to restore the old binary's query plans,
recreate the index per partition with
`CREATE INDEX CONCURRENTLY idx_<schema>_ready_<slot>_lane_shard ON <schema>.ready_entries_<slot> (queue, priority, enqueue_shard, lane_seq)`.

## Terminal live counts: one row per lane

After the ring authority flip, runtimes fold sealed-slot terminal-count deltas
into one `queue_terminal_live_counts` row per `(ready_slot, queue, priority,
enqueue_shard)` with `counter_bucket = 0`, and `rebuild_terminal_counters`
writes the same shape. The first fold after the flip collapses every bucket of a
lane it touches and adds the signed delta, so rows left in per-job-id buckets by
earlier folds, the installer backfill or the v027 rebuild stay exact. No
migration is involved and readers sum every bucket either way.

Before the flip, 0.6 runtimes can still hold the maintenance election and fold
into job-id buckets, so this release keeps that shape in its own folds and
rebuilds until the flip fences them. Exact terminal counts stay exact through a
mixed-version rollout and through a rollback before the flip; no operator action
is needed.

## v051: per-slot claim functions

v051 moves the claim query out of `claim_ready_runtime` into one
`claim_ready_slot_<N>` function per queue-ring slot in the default and every
discovered custom queue-storage schema. Each function addresses that slot's
ready, tombstone and attempt-batch children directly, so PostgreSQL keeps a
cached plan for it across ring rotations. `claim_ready_runtime` keeps its
signature and result and dispatches to the slot function, and the claim
evidence it writes is unchanged, so binaries before v051 claim unchanged on the
migrated schema and the migration adds no runtime version gate.

This is an installer replay, not a function-only change. Reinstalling each
substrate drops and recreates ring-state and queue-head triggers and takes
ACCESS EXCLUSIVE table locks that are held until the whole pending range
commits, so enqueue and claim in those schemas wait for the migration. Use a
quiet window and a bounded `lock_timeout` (for example
`PGOPTIONS='-c lock_timeout=5s' awa migrate`); on timeout nothing is applied,
so retry the complete migration. Apply it with credentials that own every
installed substrate.

The runtime role needs `EXECUTE` on the new functions. The
[default privileges](security/database-roles.md#runtime-grants) granted to `awa_runtime`
cover functions the migrator creates; if you granted `EXECUTE ON ALL FUNCTIONS`
once instead, run that grant again after migrating. External migration runners
apply the complete generated `awa migrate --sql --from 50 --to 51` bundle, which
refreshes the installer definition before the tail reinstalls the custom
substrates.

## v052: job output columns

v052 adds the storage for [job output](job-output.md) (ADR-048) to the default
and every discovered custom queue-storage schema:
- a nullable `output json` column on `done_entries`;
- a nullable `outputs json[]` array on `receipt_completion_batches`, with a
  `NOT VALID` check on its length;
- a final `output` column on `terminal_jobs`.

It is not an installer replay. It redefines the installer, so new schemas
get the columns, then runs a targeted helper for each schema. Nothing is
rewritten or scanned. Adding the columns and replacing the view still take
`ACCESS EXCLUSIVE` locks on the terminal tables and view, held until the
whole pending range commits, so completions and terminal reads wait while it
runs. It locks `terminal_jobs` before its tables, the order terminal reads
use. When no `lock_timeout` is set, each lock acquisition waits at most 2 s.
On a timeout or a deadlock nothing is applied; retry the complete migration.

Binaries before v052 run unchanged on the migrated schema: they name terminal
columns explicitly. While both versions run, a 0.6 worker that claims the
retry of an attempt a 0.7 worker failed sees that attempt's output in
`job.errors`; 0.7 workers remove it from the row they dispatch. A 0.7 binary running before the migration checks its
schema's catalog for the columns. Until they exist, it neither writes nor
reads them, and it drops completion outputs (counted as
`awa.job.output_dropped{awa.output.reason="schema_missing"}`). Health-gated
workers report not ready until the migration runs, as for any version bump.

External migration runners apply the complete `awa migrate --sql --from 51
--to 52` bundle, which defines the helper before the tail calls it.

## v054: claim batch job index

Point lookups of compact receipt claims by job id (heartbeat and progress flush, completion fallbacks, receipt close, cancellation, rescue and `load_job`) use a GIN index on `lease_claim_batches.job_ids` instead of reading every retained claim batch. v054 installs `<schema>.ensure_claim_batch_job_index(slot)` in the default and every discovered custom queue-storage schema and builds no index itself, so it takes no lock on any awa or queue-storage table. Claim prune calls that function right after truncating a claim partition, which builds the index while the partition is empty, so every partition gains it within one claim-ring cycle. A new schema's installer indexes its claim partitions as it creates them.

Do not build the index yourself on a populated partition: the build holds a `SHARE` lock, which blocks claim inserts into that partition, for about half a minute per 100k batches. Run the migration as the owner of the queue-storage tables, or as a migrator that can `SET ROLE` to it, so the function can create the index; see [Claim batch index](security/database-roles.md#claim-batch-index).

Binaries before v054 neither use nor build the index, and binaries that use it read the same rows, by scan, from a partition without it. A database coming from 0.6.15 already has the function from the 0.6 `claim_batch_job_index` schema patch, and v054 replaces it in place. External migration runners apply the complete `awa migrate --sql --from 53 --to 54` bundle, which defines the helper before the tail installs the function.

## v055: batch compatibility insert

v055 adds one function, `awa.insert_jobs_compat_batch`, and changes nothing else: no table DDL, no table or sequence locks, no change to `awa.insert_job_compat` or to storage routing. It inserts a whole batch the way `awa.insert_job_compat` inserts one job, with the same validation, unique claims, lane assignment and storage routing. It does everything that does not depend on a lane first (job ids, unique claims in key order, scheduled rows, the result rows). Then, in `(queue, priority, shard)` order, it registers each lane it is the first to use, reserves each lane's range of positions once and writes the ready rows. Each lane's enqueue lock is therefore taken when the batch reaches that lane instead of at the batch's first ready row. The lock is transaction-scoped: it is held until the calling transaction commits or rolls back, including any work the caller does after the batch. Concurrent producers and promotion on the same lane no longer wait for the batch's lane-independent work or its earlier lanes.

What callers see:

- `insert_many_copy` (Rust) and `Client.insert_many_copy` (Python) use the function when it is installed and the connected role may execute it, otherwise the per-row path. They check on every call, so a running producer switches on its next call after the migration commits. Their results are unchanged: rows in input order, jobs whose unique key is already claimed skipped.
- `insert_many_skip_unique_conflicts` (Rust) inserts a batch through the function and skips jobs whose unique key is already claimed, returning the inserted rows in input order. Use it instead of retrying a failed `insert_many` one job at a time. It returns a validation error on a schema before v055.
- Generic `insert_many` is unchanged: one statement calling `awa.insert_job_compat` per row, on every schema, and a unique conflict still fails the whole batch.
- A batch's rows in one lane occupy consecutive lane positions in input order, as before, and are described by one ready segment per run instead of one per row. A key repeated inside one batch keeps its first job in input order.

Binaries before v055 never call the function. They keep calling `awa.insert_job_compat`, which takes the same lane lock, so old and new producers share a lane without reordering it. A database coming from 0.6.15 already has the function from the 0.6 `insert_jobs_compat_batch` schema patch, and v055 replaces it in place.

The batch function takes all of a batch's unique claims before any lane lock, while per-row inserts (generic `insert_many`, single-job inserts, and binaries before v055) take each job's claim and its lane together, lane by lane. When a per-row transaction already holds a lane the batch needs and then inserts a job whose unique key the batch has claimed but not yet committed, PostgreSQL aborts one of them with `deadlock detected` (SQLSTATE `40P01`), where before the per-row insert would have waited and then skipped the job or failed with a unique violation (`23505`). [v057](#v057-batch-inserts-give-way-to-per-row-producers) replaces the function's body so the batch gives way instead. Two batches through the function take claims in key order and lanes in lane order, so they do not deadlock with each other this way.

New functions are executable by `PUBLIC` unless your deployment revokes that through default privileges. If it does and the runtime role's default privileges do not cover functions created by the migration role, grant it explicitly:

```sql
GRANT EXECUTE ON FUNCTION awa.insert_jobs_compat_batch(
    text[], text[], jsonb[], text[], smallint[], smallint[], timestamptz[],
    jsonb[], jsonb[], bytea[], text[], text[], boolean
) TO awa_runtime;
```

Without `EXECUTE`, `insert_many_copy` keeps the per-row path and `insert_many_skip_unique_conflicts` fails with `permission denied`.

External migration runners apply the complete `awa migrate --sql --from 54 --to 55` bundle, which refreshes trusted queue-storage discovery before it replaces the function.

## v056: claim batch index recognition

v056 replaces `<schema>.ensure_claim_batch_job_index(slot)` in the default and every discovered custom queue-storage schema. The v054 function counted any valid GIN index whose definition named `job_ids` as the claim batch index, a partial one included, and treated another index or relation that already had the index's name as the index, so claim prune could skip building the real one. The function now accepts only a valid, non-partial GIN index on `job_ids` alone. When something else already has the name `lease_claim_batches_<slot>_job_ids_idx`, it raises a warning naming it, returns false and leaves it in place; drop or rename it and claim prune builds the index the next time it empties that slot.

The migration also fixes who may execute the function after a refresh by a migration role that owns it but may only `SET ROLE` to the table owner (PostgreSQL 16+ membership `WITH INHERIT FALSE`). The refresh handed the function to the table owner first, after which the migration role could no longer revoke or grant on it, PostgreSQL only warned, and `EXECUTE` stayed with `PUBLIC`. It now resets `EXECUTE` after the transfer, acting as the new owner when it cannot as itself, so exactly the roles holding `TRUNCATE` on `lease_claim_batches` can execute it. Migrations run with the owner as the effective role, as [database roles](security/database-roles.md) describes, were not affected.

Like v054 it replaces functions only: it builds and drops no index and takes no lock on any `awa` or queue-storage table. Binaries before v056 call the new function unchanged, and an index the v054 function built is still recognised. A database coming from 0.6.16 already has this function from its `claim_batch_job_index` schema patch. External migration runners apply the complete `awa migrate --sql --from 55 --to 56` bundle, which defines the helper before the tail installs the function.

## v057: batch inserts give way to per-row producers

v057 replaces the body of `awa.insert_jobs_compat_batch`, which `insert_many_copy` and `insert_many_skip_unique_conflicts` call. It is one `CREATE OR REPLACE FUNCTION` with no table DDL and no table locks, and the function's signature, privileges and results are unchanged. The fix is in the function, so producers that already call it get it as soon as the migration commits, before or after their binaries roll.

The function takes a batch's unique claims before its lane locks, while per-row inserts (generic `insert_many`, single-job inserts, and binaries before v055) take each job's claim and its lane together. With the v055 body, a per-row transaction that held a lane the batch needed and then inserted a key the batch had claimed but not committed waited on the batch while the batch waited on it, and PostgreSQL aborted one of them with `deadlock detected` (SQLSTATE `40P01`).

From v057 the function takes its claims and lanes in a subtransaction and, while it holds any of them, waits at most 500 ms per lock, or half of its own session's `deadlock_timeout` if that is lower. When a wait would run longer, the batch rolls back what it took and tries again after a short randomised pause. The per-row transaction gets the key or the lane, proceeds and commits, and on its next attempt the batch skips the keys it committed, or fails with a unique violation when it does not skip conflicts, as it would for any key committed before it. The same applies to a transaction that enqueues lanes in separate statements in the opposite order to a batch. Two batches through the function still take claims in key order and lanes in lane order, so they cannot deadlock with each other.

After 2 s of retries, the batch makes one last attempt that waits as the v055 function did. Contention that outlasts the retries is then served in lock order, and a cycle that rolling back cannot break ends in `deadlock detected` rather than endless retries. The protection therefore lasts only for the bounded phase: a per-row transaction that holds a lane for longer than about 2.5 s and only then inserts one of the batch's keys meets the ordinary attempt, and one of the two is aborted with `40P01` as before.

What callers see:

- A batch insert and a per-row producer that share keys or lanes no longer abort each other with `40P01` when the batch call is the first thing its transaction locks and the per-row producer's `deadlock_timeout` is above 500 ms (PostgreSQL's default is 1 s). The batch cannot see another session's setting, so the cap is fixed; a per-row session configured at or below 500 ms can still be aborted by its own, earlier deadlock check.
- A per-row transaction that holds a lane longer than the bounded phase (about 2.5 s) before it inserts one of the batch's keys can still deadlock with the batch.
- A batch whose transaction already holds locks from earlier statements can still be part of a deadlock: rolling back the batch cannot release them. That cycle now ends later, after about 3 to 4 s instead of 1 to 2 s, and the transaction usually aborted is the batch's caller rather than the per-row producer.
- A batch can only give way while inserting its unique claims takes less than the 500 ms budget. Each claim costs roughly 20 µs on a fast database server and more on a slow or busy one, so the limit is in the range of 10,000 to 20,000 unique keys. A larger batch, or one whose claims are slowed by load, uses up each bounded attempt before it reaches its lanes and ends in the ordinary attempt, so it behaves as with the v055 body. Split such batches if they share keys or lanes with per-row producers.
- Under contention that outlasts the retries, a batch waits for its lanes in lock order, as before.
- A caller's `lock_timeout` applies to each of the batch's lock waits, as it does to any statement's, and a wait it ends fails the call with `lock_not_available` (SQLSTATE `55P03`). Time the batch spends on its own work or on retries does not count against it.
- The 500 ms budget applies to each wait, not to a statement: a claim statement that meets several keys still in flight, or a lane reservation that waits for the lane's provisioning lock and then its lock, can wait up to the remaining budget each time.
- The lane lock is still taken when the batch reaches the lane and held until the transaction ends.

Binaries before v057 that call the function pass the same arguments, so they can run before or after the migration, and rolling binaries back needs no schema change. A database coming from 0.6.16 already has this body from its `insert_jobs_compat_batch` schema patch, and v057 replaces it in place. External migration runners apply the complete `awa migrate --sql --from 56 --to 57` bundle, which refreshes trusted queue-storage discovery before it replaces the function.

## v058: lane row before lane lock

v058 replaces `<schema>.provision_lane` in the default and every discovered custom queue-storage schema. The v053 function took the lane's advisory lock and then inserted its `queue_lanes` row, while producers and `QueueStorage` insert the row first and take the lock after it. A `provision_lane` call and a producer that were both first to use the same new `(queue, priority)` could each wait for the other until PostgreSQL aborted one with `deadlock detected` (`40P01`). The function now inserts the row first.

It replaces functions only and takes no lock on any `awa` or queue-storage table. Binaries before v058 call the function with the same arguments, so they can run before or after the migration.

## v059: wider retry jitter

v059 replaces the body of `awa.backoff_duration(attempt, max_attempts)`, which sets the delay before an automatic retry. The delay was `2^attempt` seconds plus up to 25% of that, so jobs that failed together, for example while a dependency was down, retried within a quarter of their base delay of each other and reached the recovering dependency together. It is now `2^attempt` seconds multiplied by a random factor in [0.5, 1.5), still capped at 24 hours. The mean delay is the base delay (the old mean was 12.5% above it), the shortest delay for an attempt halves, and the retries from one failure spread over a window four times as wide, which cuts their peak arrival rate by about 3.5×. These figures hold while the whole range is below the cap, which is through attempt 15. From attempt 16 the top of the range is cut to 24 hours, and from attempt 18 every retry waits exactly 24 hours, with no jitter. The function sets every retry delay awa chooses itself: after a retryable handler error and after rescue of a stale heartbeat, an expired deadline or a callback timeout. `RetryAfter` and `Snooze` keep the delay the handler gives.

It is one `CREATE OR REPLACE FUNCTION` that keeps the function's signature, volatility, owner and privileges, and takes no lock on any `awa` or queue-storage table. Binaries before v059 call the same function from the same statements, so they get the new delays as soon as the migration commits and can run before or after it. Jobs already waiting to retry keep their scheduled time. External migration runners apply `awa migrate --sql --from 58 --to 59`.

## Tag indexes (ADR-049)

Tags become the indexed way to find jobs on queue storage. There is no
migration: new schemas, and the empty partitions of existing ones, get their
indexes when `awa migrate` or schema preparation runs. For the populated
partitions of an existing schema, run as the migrator after upgrading:

```
awa storage build-tag-index --wait
```

Each ring partition is indexed once pruning empties it, so `--wait` finishes
within one ring cycle. `--now` builds populated partitions immediately with
`CREATE INDEX CONCURRENTLY`, at the cost of holding back vacuum and blocking
pruning of each partition while it builds. Tag lookups work throughout and
scan any partition that has no index yet. See [Finding jobs](finding-jobs.md).

A batch operation whose filter uses several tags, several states or
`dead_lettered` is refused until every live runtime is 0.7.0-alpha.2 or
later. Several tags or states travel as a list under `tag` or `state`,
which a 0.6 server or runner refuses rather than ignoring, and DLQ retry and
purge by the full filter go to new endpoints (`/api/dlq/retry-matching`,
`/api/dlq/purge-matching`) that a 0.6 server answers with 405. A browser
holding the new admin UI during a rolling deploy therefore gets an error
from an old replica, never a wider operation. Scripts that call the 0.6
`/api/dlq/bulk-retry` and `/bulk-purge` bodies keep working unchanged.

A batch operation now needs a kind, queue, tag, ids or created bound, as a
DLQ purge does, unless it sets `allow_all`. A script that submits one with
only a state, such as `{"state": "available"}`, changed every job in that
state on 0.6 and is refused on 0.7.

## The one gate

`awa migrate` on a 0.7 binary refuses to apply migrations unless one of these holds:

- the cluster's storage transition is **finalized** (`awa storage status` reports
  `state = active`, `current_engine = queue_storage`), or
- the install is **fresh** — a brand-new database, or a schema with no jobs and no
  recently-live runtimes (the same conditions that let a fresh install auto-finalize at
  worker startup).

Any other shape — canonical with work, `prepared`, `mixed_transition` — is refused with an
error naming the steps below. Nothing is applied on refusal; re-run `awa migrate` after
finalizing.

This is the [ADR-037](adr/037-canonical-engine-deprecation.md) deprecation gate: the
canonical (row-mutating) engine is deprecated in 0.7 and its claim/execution/trigger paths
are removed in 0.8. Requiring finalization at the 0.7 boundary means no 0.7 feature ever
needs a canonical implementation, and gives operators one unambiguous instruction.

## If you are on 0.6, finalized

You are done with the storage step. Deploy 0.7 binaries and run `awa migrate` as usual.

## If you are on 0.6, not yet finalized

Complete the staged transition **on your 0.6 binaries** first
(full procedure: [Upgrade 0.5 to 0.6](upgrade-0.5-to-0.6.md)):

```bash
awa storage prepare --engine queue_storage
awa storage enter-mixed-transition
awa storage finalize --wait
```

For a stopped fleet, a CLI containing #457 can replace the middle command
with `awa storage enter-mixed-transition --quiesced`; follow the
[quiesced procedure](upgrade-0.5-to-0.6.md#quiesced-alternative-to-the-witness-runtime).
If canonical callbacks are outstanding, deploy the 0.6 backport of #462 before
flipping routing. Neither fix requires a new schema migration.

Then upgrade binaries to 0.7 and run `awa migrate`.

If `finalize --wait` sits at a non-zero backlog that never falls while jobs are visibly executing, your workload probably contains **perpetually snoozing jobs** — handlers that end every run in `JobResult::Snooze`. On builds before [#456](https://github.com/hardbyte/awa/issues/456) those re-entered canonical `scheduled_jobs` after each post-flip run, replenishing the backlog forever. Roll to a build carrying that fix (or apply the [manual backlog migration](upgrade-0.5-to-0.6.md#known-issues)) before waiting on finalize.

Remember the transition is a one-way door once queue-storage work is accepted; the 0.5→0.6
guide covers the abort boundaries.

## If you are on 0.5.x

Step through 0.6: upgrade to the latest 0.6.x, run `awa migrate`, walk the staged
transition to `finalize`, then upgrade to 0.7. A 0.7 binary will not migrate a 0.5-era
schema directly — stepping-stone upgrades are the supported path (the same pattern Oban,
River, and Postgres itself use).

## The v043 ring-rotation ledger migration

Migration **v043** ([#371](https://github.com/hardbyte/awa/issues/371),
[ADR-040](adr/040-append-only-ring-rotation-ledger.md)) moves the queue/lease/claim ring
cursors out of the mutable `{ring}_ring_state` singleton columns (`current_slot`,
`generation`) into append-only `{ring}_ring_rotations` ledgers (dead-tuple-free rotation
under a pinned MVCC horizon). It is delivered as a staged **expand → flip → contract**
upgrade supporting a mixed 0.6.2/0.7 fleet.

> **Required stepping-stone:** first roll the whole fleet to **0.6.2 or later**.
> 0.6.2 is the first 0.6 build that recognizes v043 as forward-compatible only
> while ring authority is `columns`, refuses unknown newer schemas without
> modifying them, and refuses to restart after the authority flip. Do not run a
> 0.7 migration while a 0.6.0 or 0.6.1 binary can invoke `awa migrate`; those
> releases contain the destructive newer-schema misclassification fixed by
> [#392](https://github.com/hardbyte/awa/issues/392).

`awa migrate` enforces this stepping-stone: v043 is refused while any runtime with a
fresh heartbeat reports a version below 0.6.2 or an unparseable version. The
`--allow-live-runtimes` override is available for operators who have independently verified
the fleet; it should not be needed in the normal rollout.

**v043 is additive (the expand phase).** It creates and seeds the three ledgers and the
rollup-delta landing table, and it **keeps** the compat `current_slot` / `generation`
columns in place. Each queue-storage schema gets a `ring_cursor_authority` control row
that selects which representation is authoritative:

- `columns` (compat) — the pre-0.7 singleton columns are authoritative, exactly as 0.6
  wrote them, so a live 0.6.2 binary keeps working. 0.7 rotators additionally shadow every
  advance into the ledger, keeping it a ready-to-promote copy.
- `ledger` — the append-only ledgers are authoritative (the #371 dead-tuple win).

An **upgrade** starts in `columns`; a **fresh install** starts directly in `ledger` (no
old binary can exist). The flip is one-way.

### Procedure (either order works after the 0.6.2 stepping-stone)

First roll 0.6.2 across the fleet as a normal patch release. After that, both
orders are supported:

1. **Roll binaries, then migrate**, or **migrate, then roll binaries** — either way the
   fleet runs mixed 0.6.2/0.7 against one database once v043 is applied. In
   compat mode a 0.6.2 rotator and a 0.7 rotator serialize on the same
   `{ring}_ring_state` row lock, so the cursor stays correct. A 0.6.2 restart also
   recognizes v043 while authority remains `columns`; it refuses rather than mutating the
   schema once authority is `ledger`.

   If you roll binaries first, a 0.7 worker **refuses to start** on the
   pre-migration v040 schema — it fails closed at startup naming `awa migrate`
   as the fix, and completes no work. Under a rolling deployment the new pods
   crash-loop harmlessly while the remaining 0.6.2 workers keep draining
   traffic, and come up unaided the moment the migration commits. Plan
   capacity for that window, or migrate first to avoid it.
2. Once **every** worker is on 0.7, promote to ledger authority to unlock the full #371
   dead-tuple benefits — either:
   - manually: `awa storage flip-ring-authority` (add `--schema <name>` for a custom
     schema; `--check` prints the fleet flip-readiness and exits without changing
     anything), or
   - automatically: the maintenance leader auto-flips once every fresh-heartbeat runtime
     has reported a 0.7+ `binary_version` continuously for a stable period (default 10
     minutes; a builder knob). It logs the flip loudly.

If you migrate first, roll 0.7 workers promptly. An all-0.6.2 fleet on v043 remains safe:
crash and heartbeat rescue are batch-aware, but 0.6.2's deadline-rescue sweep does not read
the v042 compact claim batches used for newly claimed deadline jobs. Deadline-based rescue
for those jobs resumes when a 0.7 runtime takes maintenance leadership. Roll the
0.6 maintenance leader promptly after migrating. Awa's built-in
migrator applies v041-v043 in one transaction, so other sessions see v040 or v043, never an
intermediate version. An external migration runner that commits each version separately can
expose v041/v042; a restarting 0.6.2 worker refuses those transient schemas and connects
normally once v043 is committed.

The manual flip **refuses** (without `--force`) while any fresh-heartbeat runtime is not
known to be flip-aware — i.e. a 0.6 (or pre-flip 0.7) binary might still be reading the
compat columns. Roll the whole fleet first, or pass `--force` only once you have confirmed
no pre-flip binary is live.

### After the flip — do not roll back to a pre-flip binary

The flip **fences returning pre-flip binaries**: under the three cursor locks it first
reconciles and verifies every shadow ledger, then poisons both compat cursor fields and
the legacy prune metadata. A database trigger rejects any later old-style cursor advance,
including the `-1 -> 0` rotation a pre-ledger binary would otherwise compute. A 0.6.2 (or
pre-flip 0.7) binary that reconnects therefore fails loudly instead of misrouting writes
or pruning an authoritative ledger slot.
Rolling a binary back **across the flip** is therefore not supported; roll back only to
another 0.7 build (which reads the ledger). **Before** the flip, the normal one-minor
skew guarantee holds and rollback to the 0.6.2 stepping-stone is safe.

### Contract (0.8)

Dropping the compat columns for good is deferred to **0.8** (the contract phase), after
ledger authority is universal and the release's capability gate excludes pre-flip binaries.
Until then the columns remain as a cheap, cold safety net.

Fresh installs are unaffected — they get ledger authority directly and never touch the
compat path.

## Runtime deprecation warning

Any 0.7 worker whose effective storage resolves to the canonical engine — an unfinalized
cluster, an explicit `canonical_drain` role, or a builder configured for canonical storage —
logs a startup warning naming this guide. Treat it as a to-do, not an emergency: canonical
still works in 0.7, and is gone in 0.8.

## Rollback

0.7's migrate *gate* itself changes nothing in the database. Migration **v043** is
additive (it keeps the compat cursor columns), so applying it does **not** break the
one-minor-version skew window: a 0.6.2 binary keeps working against a v043 schema in compat
authority. The skew window only ends at the **ring-authority flip** (see above): after the
flip the stale compat columns are poisoned, so a pre-flip binary fails loudly and rolling
back across the flip is **not** supported — roll back only to another 0.7 build. Before
the flip, rollback to the 0.6.2 stepping-stone is safe. There is no schema downgrade path (unchanged from
previous releases).


## v047: replica identity for every table

v047 gives the tables that had no primary key a replica identity, so awa keeps
working when its schema is in a logical-replication publication (Debezium's
default `FOR ALL TABLES`, or `FOR TABLES IN SCHEMA awa`). Without one PostgreSQL
rejects every UPDATE and DELETE on a published table, which stopped the
maintenance leader on each tick. `awa.job_unique_claims` identifies rows by its
existing uniqueness index; `awa.admin_dirty_queue_marks`,
`awa.admin_dirty_kind_marks`, `queue_terminal_rollup_deltas` and
`queue_terminal_count_deltas` with its partitions use `REPLICA IDENTITY FULL`
(ADR-045 forbids an index on the mark tables; the delta tables are append-only,
so FULL costs nothing on their hot path).

0.7.x runtimes retain the same query behavior after migration; replica identity
changes WAL images for UPDATE and DELETE. Apply the migration with DDL credentials
that own, or inherit ownership of, the canonical and installed custom substrates.
Schema USAGE alone does not permit ALTER TABLE. Discovery uses the full
`claim_ready_runtime(text,bigint,double precision,double precision)` signature
and skips unrelated same-named functions.

Use `awa migrate` or the generated version bundle. For an external runner,
`awa migrate --sql --from 46 --to 47` emits the installer definition and
default-substrate replay followed by the v047 tail. Raw v047 is only
that tail. The default SQL export wraps a transaction and migration advisory lock,
but does not run Rust runtime/cron preflights. External runners must perform the
documented cron preflight below before installer replay:
when `awa.cron_jobs` exists, acquire its ACCESS EXCLUSIVE lock before the first
pending statement. Use `--no-transaction` when the external runner owns the
transaction and migration lock. Apply the full bundle transactionally; see
[migration policy](adr/041-rolling-upgrade-policy.md).

The identity ALTERs and installer trigger replacement acquire table locks,
including ACCESS EXCLUSIVE locks on ring state and queue heads. Locks can wait
behind live transactions and remain held until the entire pending range commits.
Neither first application nor replay has a millisecond or lock-free guarantee.
Use an operational quiet window and a bounded `lock_timeout`; on timeout roll
back and retry the complete bundle. Applying earlier pending versions separately
can shorten the final lock-holding transaction, but the v047 bundle still includes
installer DDL. See [tracking PR #509](https://github.com/hardbyte/awa/pull/509),
[released-artifact rehearsal tracking #427](https://github.com/hardbyte/awa/issues/427),
and [Logical replication and CDC](deploying-on-managed-postgres.md#logical-replication-and-cdc).

## v046: wait-free admin dirty-key marks

v046 replaces the keyed `admin_dirty_queues` / `admin_dirty_kinds` tables the
canonical triggers wrote with `INSERT ... ON CONFLICT DO NOTHING` by append-only
`admin_dirty_queue_marks` / `admin_dirty_kind_marks` tables with no index or
constraint, and rewrites `mark_dirty_keys_*`, `recompute_dirty_admin_metadata()`
and `refresh_admin_metadata()` around them. The trigger write inside every job
transition becomes a plain heap insert that cannot wait on another transaction,
and the full refresh no longer `TRUNCATE`s under `ACCESS EXCLUSIVE`.

No operator action is required. The migration creates two small tables, copies
pending marks, and replaces function bodies; it takes no lock that conflicts with
job traffic. 0.6.x runtimes call the maintenance functions by name and keep
working on the migrated schema in either order. External SQL runners apply the
file like any other additive migration. The old tables are left in place and are
dropped by a later contract migration.

## v045: opt-in periodic ownership

v045 adds ownership, retirement, and normalized desired-declaration control
state. Existing unowned additive schedules retain their behavior. Apply the
migration before starting an authoritative runtime; a binary-first authoritative
startup refuses until the schema is present. No existing schedule is adopted or
retired by migration.

Roll every runtime to a cron-protocol-capable build before expecting automatic
retirement. Fresh older instances conservatively block it even if their cron
configuration is unrelated. Then explicitly adopt the owner's existing schedule
names and enable complete-set registration. Inspect `awa cron plan OWNER` before
removing declarations. A stopped fleet is an outage, never a delete signal;
use explicit `cron retire-owner OWNER --apply` for decommissioning.

Use current operator clients for owned schedules. The released 0.6.7 automatic
enqueue and additive UPSERT paths are fenced on retired rows; old manual trigger
uses a separate read/insert path and is outside the owned-schedule operator
contract. Old physical deletion is rejected. Existing jobs and retries are not
cancelled. Restore uses current database time, without replaying retired time.

The Rust migrator drains cron enqueues before applying the pending range, holding
`cron_jobs` in `ACCESS EXCLUSIVE` mode through commit. This prevents a lock-order
cycle between released cron enqueue and earlier storage DDL. Cron evaluation
pauses for the whole range (several seconds when upgrading from v040), then
resumes under its missed-fire policy. Ordinary workers may still encounter the
existing storage-DDL contention handled by the migration retry policy.

External SQL runners crossing v045 in a transaction with earlier migrations must
likewise acquire `LOCK TABLE awa.cron_jobs IN ACCESS EXCLUSIVE MODE` before the
**first** pending migration, when the table already exists. Acquiring it only at
v045 is too late. A fresh install has no old cron enqueues to drain.

External SQL runners must apply the complete migration transactionally under
the migration lock and retain the runtime-snapshot statement trigger: it makes
old and new evidence writers share the same retirement serializer. Do not disable
it or write declaration state independently of the snapshot transaction. The
lock protects control-plane decisions only, not ordinary job/lease heartbeats.

Reproduce the released-artifact proof with
`scripts/rehearse-cron-ownership.sh` (disposable DATABASE_URL required); the cron
model suite is `scripts/check-cron-models.sh`. Existing ring-authority rollback
restrictions still apply independently of cron ownership.

Reintroducing a retired desired name in a later deploy does not fail startup.
It remains inert and visible as an ownership/retirement blocker until an operator
restores it. Use `awa cron restore NAME` or `awa cron restore-owner OWNER` to
preview, then add `--apply`. Restore-owner affects only that owner's retired
schedules, preserves pause state, and starts evaluation from current database
time. Foreign ownership still fails startup and requires explicit transfer.
During mixed-manifest rollouts, existing definitions retain their last agreed
value; new names can be inserted immediately. Definition updates wait for fresh
capable fleet agreement, while removals additionally wait through grace.

### v048 claim cursor compatibility

Current runtimes work against both the pre-v048 and the v048 claim result shape and advance the lane cursor after every committed claim on either schema. `claim_ready_runtime` still returns `lane_drained`; current runtimes ignore it. v048 adds same-lane committed-evidence catch-up inside the claim, which covers an advance that never ran (for example a claimer that stopped between commit and advance). Legacy materialized-lease claims write no attempt evidence, so only their post-commit advance moves the cursor. Cursor folding uses committed evidence for the selected ready generation and re-routes before claiming a newer segment; it does not make asynchronous acknowledgments durable or change at-least-once delivery. See the correctness mapping for the bounded range and crash/promotion proof boundaries.

For v048 use `awa migrate` or the complete generated
`awa migrate --sql --from 47 --to 48` bundle. It includes the installer
definition and default-substrate replay before the v048 tail reinstalls custom
substrates. Raw `v048_claim_cursor_catch_up.sql` alone calls
whatever installer is already stored in the database; it does not supply the
new function definition. The generated bundle path supplies that dependency.
The same table-lock, ownership, external preflight and timeout/retry requirements
[documented for v047](#v047-replica-identity-for-every-table) apply. Trigger replay
is DDL and can block claim/enqueue/rotation activity until commit; there is no
fixed completion-time guarantee. See [tracking PR #513](https://github.com/hardbyte/awa/pull/513)
and [released-artifact rehearsal tracking #427](https://github.com/hardbyte/awa/issues/427).

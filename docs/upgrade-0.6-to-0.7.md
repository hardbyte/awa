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

For a runtime without DDL privileges, apply v049 as the migrator, run
`awa storage prepare-queue --queue <name>` for every physical queue, and grant
sequence `USAGE, SELECT, UPDATE` before starting producers or workers. Match
shard and stripe counts and provision extra lanes before increasing them.
External migration runners must apply the complete generated
`awa migrate --sql --from 48 --to 49` bundle: it refreshes the installer
definition before replacing lane helpers in the installed schemas.
See [Database roles](security/database-roles.md#provision-queues-before-starting-restricted-runtimes).

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
- a nullable `output` column on `done_entries`;
- a nullable `outputs` array on `receipt_completion_batches`, with a
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

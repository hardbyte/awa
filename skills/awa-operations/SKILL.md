---
name: awa-operations
description: Deploy, upgrade, and operate the Awa Postgres-native job queue and its worker fleet. Use when running database migrations, planning a rolling upgrade, sizing connection pools on managed Postgres, operating the dead-letter queue or cron schedules, driving a storage-engine transition, configuring the awa CLI, or serving the web admin UI.
license: MIT
compatibility: Requires an awa CLI matching the deployed runtime version and PostgreSQL access. Written for awa 0.7; pin to the release tag matching your deployment. The CLI, `awa health`, and `awa storage status` work against the database alone; the runtime `/readyz` probe and the fleet-liveness fields require a live worker fleet.
---

# Awa Operations

Use the awa CLI version that matches the deployed runtime and schema. A binary
refuses a database schema newer than it understands, so operate every database
from a CLI at least as new as the fleet. Read the upgrade note for the exact
version pair before changing anything (`docs/upgrade-0.6-to-0.7.md`,
`docs/upgrade-0.5-to-0.6.md`).

## Connect Safely

`awa` resolves a target in this order: `--database-url` > `--context`
(`AWA_CONTEXT`) > `DATABASE_URL` env > `default_context` in
`~/.config/awa/config.toml`. Every command echoes the resolved target with the
password stripped before acting; confirm it.

Read-only commands may fall back to `DATABASE_URL` or the default context.
Mutating commands refuse to run against an implicit target whenever more than
one context is defined — pass an explicit `--context` or `--database-url`. A
context marked `production = true` prompts for confirmation on a mutating
command; `--yes` skips the prompt, and a non-interactive shell without `--yes`
refuses rather than hanging. An explicit `--database-url` is never treated as
production.

Prefer `url_env` over an inline `url` in config so credentials never sit in the
file. Never print database URLs or passwords in logs or issues.

## Migrations

Migrations are forward-only; there are no down migrations. Roll back with a
database snapshot or reverse SQL, not with the CLI.

Use an updated binary to generate pending upgrade SQL. The runner records
versions, not checksums; applied steps are not rerun. Preserve already-applied
files in checksum-based external runners rather than replacing their history.

Migration discovery skips unregistered custom schemas with foreign-owned objects,
untrusted `CREATE`/`TRIGGER` grants, or any trigger awa did not install. Unsafe
registered/prepared substrates block the bundle before version advancement.
The error names up to five offending objects; the upgrade guide gives fixes for
`GRANT ALL` table grants, a 0.6 runtime's schema `CREATE` grant, and schemas
prepared by another role. Sequences owned by another role are accepted. The same condition in `awa` aborts before
installation, including for superuser migrators. Use a consistent dedicated owner
role and review ownership and grants before retrying; see the upgrade guide.

One `awa migrate` applies the whole pending range in a single transaction under
an advisory lock, so a failed or interrupted run commits nothing and concurrent
runners converge on one application. Every migration is re-runnable, so a retry
after any failure is safe.

```bash
awa migrate --pending                 # apply from current DB version to latest
awa migrate --sql --pending           # print the SQL instead of applying
awa migrate --extract-to ./out        # write SQL files for an external runner
```

`--sql` output is wrapped in one transaction taking the runner's advisory lock,
so `awa migrate --sql | psql -v ON_ERROR_STOP=1 "$DATABASE_URL"` is atomic.
`ON_ERROR_STOP=1` is what makes a failure *detectable*: plain psql rolls the
transaction back but still exits 0. Pass `--no-transaction`
when the consuming runner opens its own transaction per migration (Flyway,
Liquibase, dbmate); `--extract-to` files are never wrapped, for that reason.
`--from` / `--to` / `--version` select a range to render and are rejected on the
apply path — applying always targets the current version.

The migrator runs pre-flight gates before touching the schema: it refuses a
schema newer than the binary understands, refuses to apply while the storage
transition is unfinalized where a gate requires it, enforces per-migration
minimum runtime versions, and enforces exclusive (no-live-runtime) windows for
non-rolling migrations. `--allow-live-runtimes` overrides the live-runtime
checks — use it only after independently verifying compatibility.

Extracted or printed SQL does **not** run the Rust pre-flights. An external
runner must enforce version floors and exclusive windows itself. Do not append
`storage prepare`/`enter-mixed-transition`/`finalize` to extracted migration
files; those are not migration DDL.

For a pending range crossing v045, acquire `LOCK TABLE awa.cron_jobs IN ACCESS
EXCLUSIVE MODE` before the first pending DDL in that transaction if the table
already exists. The Rust migrator does this automatically. Cron evaluation
pauses until commit; the lock prevents a released enqueue from deadlocking with
earlier storage DDL. See the 0.6→0.7 upgrade guide for the rollout boundary.

Do not skip a major version. 0.5 → 0.7 is unsupported; step through 0.6.

## Restricted Database Roles

From v053 any runtime role creates lanes through the schema owner's
`SECURITY DEFINER` provisioning functions, so runtimes need neither schema
CREATE nor sequence default privileges, and `awa storage prepare-queue` is
optional. Run migrations with the schema owner as the effective role. Check
lane ownership with `awa storage lane-sequences` (exits 2 while lanes need
repair) and fix it with `--repair`, connected as the migration role itself
rather than as the schema owner, or with the statements it prints; see
[Database roles](../../docs/security/database-roles.md#check-and-repair-lane-ownership).

## Rolling Upgrades

Awa runs mixed-version fleets during a rollout; the constraints are version
specific, so follow the matching upgrade doc. The 0.6 → 0.7 upgrade is the
current worked example and its shape generalizes:

1. **Stepping stone first.** Roll the entire fleet to 0.6.2+ before applying the
   0.7 (v043) ring-rotation migration. 0.6.0/0.6.1 misclassify a newer schema
   destructively (#392).
2. **Finalize storage.** A 0.7 binary refuses `awa migrate` unless the storage
   transition is finalized (`storage status` shows `state=active`,
   `current_engine=queue_storage`) or the install is fresh.
3. **Roll binaries and migrate in either order.** Mixed 0.6.2/0.7 is safe in
   compatibility mode. If binaries roll first, a 0.7 worker refuses to start on
   the old schema and crash-loops harmlessly until the migration commits.
4. **Promote ring authority only after the fleet is fully 0.7.** Use
   `awa storage flip-ring-authority`, or let maintenance auto-flip once every
   worker has been 0.7 for a stable period.

Crash-safety facts to respect:

- After the flip, roll back only to another 0.7 build. The flip poisons the
  compatibility cursor fields and a database trigger rejects old-style cursor
  advances, so a returning pre-flip binary fails loudly.
- While a v043 fleet is still all-0.6.2, deadline-based rescue resumes only once
  a 0.7 runtime takes maintenance leadership. Roll the maintenance leader
  promptly.

The `flip-ring-authority` step itself has a hard guard — see Storage
Transitions below.

## Health And The Fleet

Workers are stateless; all state is in Postgres, and scaling means running more
processes. Exactly one worker per database is the elected maintenance leader
(session-scoped advisory lock — if it dies, another is elected on the next
tick). The leader alone runs cluster-wide scans: deferred→ready promotion, the
three rescue paths (stale-heartbeat, hard-deadline, callback-timeout), ring
rotation and prune, DLQ and descriptor cleanup, and cron evaluation. During an
unfinalized transition it promotes both planes' deferred backlogs, so drain and
post-flip execution do not depend on which runtime holds leadership.
Promotion gives each queue lane its own short transaction and hands a lane
whose enqueue lock stays busy to a dedicated waiter, so a hot queue delays
neither other queues' promotion nor rescue
(`awa_maintenance_promote_lane_contention_total` counts the hand-offs). Every
worker dispatches and heartbeats its own attempts. There is no `pg_cron` or
external scheduler.

Late completions from a rescued attempt lose against the newer attempt via the
`run_lease` guard, so rescue never double-commits. Set `heartbeat_staleness`
(default 90s) to at least 3× the heartbeat interval; detection latency is
roughly `heartbeat_staleness + heartbeat_rescue_interval`.

On queue storage a killed runtime's open claims stop the claim ring rotating
until rescue closes them. Rescue drains such a backlog in passes of 500-attempt
batches, bounded by `rescue_pass_budget` (1s) and `rescue_pass_max_rows`
(5000), with the next pass a second later while a backlog remains.
Stale-heartbeat rescue runs extra batches and early passes only after runtime
snapshots support a full settled staleness window. Steady evidence may predate a
stall; it is a sampled heuristic, not proof that an owner has exited. An
unconfirmed phase requires
`3 × max(runtime_snapshot_interval, 1s) < heartbeat_staleness`; 5-second
snapshots with 12-second staleness skip that phase and can accelerate rescue of
stale live claims. See
[rescue backlog](../../docs/configuration.md#rescue-backlog) for cutoff and
sampling limits. The ordinary first batch keeps its configured interval. This
evidence needs `heartbeat_rescue_interval` below half of `heartbeat_staleness`;
at the defaults acceleration usually starts 3–3.5 minutes after a runtime's
last snapshot. Leadership changes reset the evidence window. Zero for either
rescue pass control keeps one batch per ordinary interval. Retried jobs retain
their backoff, and due times can cluster; the pass row cap does not cap
promotion throughput. Watch `awa.maintenance.rescue.pass_size` (by
`awa.rescue.pass_end`) and `awa.maintenance.rescue.oldest_attempt_age`,
filtering to the current leader.

Finished handlers retry durable finalization for up to 120s from the first
finalize attempt when database writes are unavailable, including connection,
pool-acquire and read-only-session errors. Recovery requires a writable endpoint
and usable pooled connections; a DNS change alone does not redirect existing
read-only sessions. Monitor
`awa.completion.reconnect_retry` for outages or pool pressure. Pending durable
finalizers are bounded by the larger of worker capacity and twice the configured
completion batch size times completion shards (one flushing and one queued batch
per shard). A full budget backpressures returning handlers, retaining at most one
additional saved result per worker. Shutdown drains finalizers with handlers until the drain
deadline. Rescue can still win first, and expired/cancelled finalization falls
back to rescue, so keep handlers idempotent. A stale reply after an availability
error may mean the first finalize committed or another actor closed the lease;
completion metrics and lifecycle hooks are best effort in that case.

Health signals:

- Runtime listener (opt in via `AWA_HEALTH_ADDR`): `GET /healthz` liveness (200
  even while draining) and `GET /readyz` (503 with failing checks:
  postgres_connected, schema_version, schema_compatible, poll/heartbeat/
  maintenance loops, shutting_down). `leader` is informational and never gates.
  The listener has no TLS or auth — keep it off public ingress.
- `awa health [--json]`: database-only readiness (reachable, schema migrated for
  this binary, fleet heartbeat counts). Exit 0 ready, 1 unreachable or
  unmigrated.
- Worker utilization (0.7+): `sum by (awa_job_queue) (awa_queue_worker_in_use)
  / sum by (awa_job_queue) (awa_queue_worker_capacity)`. Waits climb steeply
  once a queue stays above about 0.8; add `max_workers`, and put job kinds with
  very different service times on separate queues. Weighted-mode capacity counts
  the whole overflow pool, so a queue can be saturated at a low ratio while
  other queues hold overflow permits; check its backlog too. See
  [sizing workers from utilization](../../docs/configuration.md#sizing-workers-from-utilization).

On Kubernetes, set `terminationGracePeriodSeconds` above the shutdown drain
timeout (e.g. 40s for a 30s drain) so a leader finishes draining before SIGKILL.

## Dead-Letter Queue

The DLQ is a separate physical table, not a job state; a DLQ'd job cannot be
claimed until retried out. It is per-queue opt-in (`dlq_enabled`) with a
default 30-day retention pruned by the maintenance leader. `dlq_reason` is
`max_attempts_exhausted`, `terminal_error`, `heartbeat_stale`,
`deadline_exceeded`, `callback_timeout`, or the reason passed to `awa dlq move`.

A poison job (one that crashes, hangs or panics on every attempt) stops after
`max_attempts`: crash and deadline rescues spend attempts like handler errors.
To contain one, `awa job cancel <id>` it or `awa queue pause` its queue, ship
the fix, then `awa dlq retry-bulk --queue <q> --kind <kind>`.

```bash
awa dlq depth --queue emails
awa dlq list --queue emails --limit 20
awa dlq retry <id>                       # resets attempt/lease, re-runs
awa dlq retry-bulk --queue emails        # bulk; --all required with no filter
awa dlq purge --kind SendEmail           # destructive; --all required if unfiltered
```

- Retry resets the job to `attempt=0`, `run_lease=0` on the runnable path with
  its stored queue/priority and immediate `run_at`. Use the programmatic retry
  APIs to override those.
- `retry-bulk`, `purge`, and `move` require `--all` when no filter is given —
  the guard against reviving or wiping the whole DLQ. Purge is unrecoverable;
  prefer `retry-bulk` if you might want the data back.
- Enabling `dlq_enabled` does not retroactively move existing `failed` rows.
  Move them explicitly with `awa dlq move --reason ... [--all]`.

## Cron

The default CLI cron surface includes `list` and `remove <name>`. Pause, resume, and
trigger are admin operations (the web UI and admin API, and the Python client),
not the CLI. Only the maintenance leader evaluates due schedules; enqueue is
atomic and re-checks `paused_at` inside the same statement, so a pause races
cleanly against a fire — a pause takes effect even if it lands between the
evaluator's read and the enqueue. Schedules are declared in application code;
for timezone and missed-fire (`coalesce`/`catch_up`) semantics, see the
`awa-jobs` skill.

### Owned schedules (v045+)

Use current operator clients. `awa cron plan OWNER` explains fleet capability,
manifest disagreement, zero-live refusal, conflicts, grace, and retirement
candidates. `--manifest PATH` previews a JSON array of proposed definitions
without publishing it. `cron adopt NAME OWNER` (optionally
`--expected-owner OLD` for transfer), `cron retire NAME`,
`cron retire-owner OWNER`, `cron restore NAME`, and `cron restore-owner OWNER`
default to dry-run; `--apply`
commits and `--actor LABEL` identifies the operator. The web UI provides previews
and Apply too. Restore-owner restores only retired rows, preserving pause and
starting their evaluation from now. Owned/retired rows cannot be physically removed.
Reintroducing a retired desired name does not fail startup, but it remains inert
until restoration. Plan/preview reads do not hold the evidence writer lock;
apply rechecks the decision. Actions reset only affected owners, including both
sides of an ownership transfer.

A retired schedule differs from pause: no current automatic/manual firing,
no resurrection by registration, and explicit restore starts from now without
replaying retired time. Legacy automatic enqueue and UPSERT honor the database
fence; legacy manual-trigger clients are outside this operator contract. Zero
live declarations never auto-retire an owner. Use retire-owner explicitly to
decommission a fleet. Keep the v045 snapshot trigger installed; it serializes
legacy evidence publication with retirement without locking job heartbeats.

## Storage Transitions

`awa storage status` is the source of truth: it reports the transition state
(`canonical` → `prepared` → `mixed_transition` → `active`), current/active/
prepared engine, backlog, blockers, and `can_finalize`. Drive transitions with
the storage commands so routing, drain, readiness, and interlocks move
together — never by swapping which workers you run.

```bash
awa storage status
awa storage prepare --engine queue_storage
awa storage enter-mixed-transition        # needs a live queue_storage_target runtime
awa storage finalize --check              # dry run: exit 0 ready, exit 2 blocked
awa storage finalize --wait               # poll until gates stay clear
```

On builds carrying #457, `storage enter-mixed-transition --quiesced` permits a
stopped fleet to flip without a target runtime. Stop all workers and keep them
stopped until it returns; any fresh heartbeat refuses the command. A stale
heartbeat is not a fence against a paused worker returning. Canonical backlog
is preserved and still blocks finalize: drain it before stopping for a role-free
cutover, or retain explicit canonical drain workers afterward. Follow the
[quiesced upgrade procedure](../../docs/upgrade-0.5-to-0.6.md#quiesced-alternative-to-the-witness-runtime).
On builds carrying #462, callback resolution falls back to canonical jobs only
during mixed transition, preserving lease fencing and transaction atomicity.
Both fixes need a 0.6 backport for unfinalized clusters; installing 0.7 first is
not a supported workaround.

Dangerous operations:

- `prepare-queue-storage-schema --reset --schema awa` is refused — it would drop
  the migration-owned `awa` schema (schema_version, runtime_instances, …) and
  leave the database unrecoverable. `DROP SCHEMA awa CASCADE` is not a supported
  operator action.
- The transition is a one-way door once queue-storage work is accepted:
  `storage abort` is rejected once rows exist in the new tables. Recovery is
  `finalize` or database restore.
- `flip-ring-authority` is one-way; it refuses without `--force` while any
  fresh-heartbeat runtime is not known to be flip-aware (an unknown or
  unparseable runtime version counts as not flip-aware).
- `rebuild-terminal-counters` is for counter-drift incidents and is best run on
  a quiesced fleet.

While the transition is unfinalized, a canonical attempt that re-schedules itself (snooze, retry backoff, `RetryAfter`) leaves the canonical plane rather than returning to canonical `scheduled_jobs`, so the drain converges even for handlers that snooze on every run instead of completing (#456). The ordinary outcome is a fresh-id row in the prepared queue-storage schema's `deferred_jobs`. If a newer duplicate owns a unique claim required by the destination state, the old attempt instead becomes cancelled terminal evidence with `rescheduled as duplicate`; it is not executable and the newer holder wins. If `finalize --wait` sits at a flat non-zero backlog while jobs are executing, you are on a build without that fix.

Rings rotate only when active — a frozen generation on an idle queue is healthy.
The condition to alert on is a frozen generation *with* accumulating rows
(`skipped_busy`): a pinned ring, almost always caused by a long-held snapshot.

## Prune Lock Timeout

Ring prune installs its `lock_timeout` after the initial slot-row locks and
pre-lock checks. From then on, each lock acquisition has its own timeout of
10 ms by default; this does not bound the whole prune. A lock timeout rolls it
back and counts `blocked` in
`awa_maintenance_prune_attempts_total{awa_ring,awa_ring_outcome}`. Sustained
`increase()` of `blocked` (or `error`) warrants checking `pruned` outcomes,
ring generations and accumulating rows; successful prunes can occur between
blocked attempts. Raise
`ClientBuilder::prune_lock_timeout` (Python
`queue_storage_prune_lock_timeout_ms`) in small steps (`50ms`, `100ms`, `250ms`) when short readers of the ring
partitions are the cause, and keep it well below `deadlock_timeout` (default 1 s)
and any enabled `statement_timeout`. Startup warns when it is at or above either
enabled limit. Of its initial partition locks, queue-ring prune waits under the
timeout only on `ready_segments` and takes its other partitions with `NOWAIT`.
`backed_off` means prune was skipped for a few ticks
after repeated `skipped_active`.

## Ring Rotation Cadence

For queue storage, `ClientBuilder::ring_rotation_cadence` seals a slot after a
five-second observed fill window once it holds 256 rows, at a 2 MiB total
physical-size target, or after 60 seconds regardless of rows, by default. Python
exposes `queue_storage_rotate_min_slot_age_ms` and
`queue_storage_rotate_target_slot_bytes`, plus `queue_storage_rotate_min_slot_rows`
and `queue_storage_rotate_max_slot_age_ms` (row floor before the age window seals,
and the ceiling that seals regardless). A zero byte target disables the size
trigger, a zero row floor disables the floor, and a zero maximum age disables the
ceiling. A row floor needs a size target or a ceiling, and a queue-storage client
refuses to build without one: lease rows are deleted as attempts finish, so a lease slot may
never reach the floor. A zero minimum age, or Rust `RingRotationCadence::EAGER`,
permits sealing each tick whatever the other settings.
Rust cadence and claim-interval overrides survive `queue_storage` calls in either order.
Empty slots bypass the fill window so reclamation can continue after drain.
Leader changes reset the local monotonic fill timers; the 60-second ceiling counts from when the slot opened on the database clock, so it still applies. The size is sampled and
can be exceeded by bursts or blocked reuse; use actual retained storage and
blocked-prune metrics for capacity planning. It is not a retention policy or
storage limit. Queue prune lock contention retries on the next decision tick;
active-data gates retain exponential backoff. Persistent contention can still
stall reclamation. See `docs/configuration.md` for the scheduling contract.

## Managed Postgres

Prefer PostgreSQL 18 where available; PG17 on some managed platforms caps
throughput because readers queue behind the ring-rotation `ACCESS EXCLUSIVE`
lock. Size for steady-state completion rate, not burst.

- **MVCC discipline is the biggest footgun.** Any long-held snapshot — `idle in
  transaction`, a BI tool, `pg_dump`, a stuck migration — pins the database-wide
  horizon and blocks vacuum *and* Awa ring prune across every table. Keep
  analytical readers on a replica; bound roles with
  `idle_in_transaction_session_timeout` and `statement_timeout`; alert on
  `pg_stat_activity.xact_start` age.
- **Pool sizing.** `max_connections >= sum(queue claimers) + 5`, plus headroom
  for handler SQL, producers, and admin. Each claimer holds a LISTEN connection;
  the leader holds one advisory-lock connection.
- **DDL creds.** Migrations and custom-schema prep need DDL-capable credentials.
  Cloud SQL IAM service accounts need a one-shot manual `cloudsqlsuperuser`
  grant; AlloyDB grants `alloydbsuperuser` automatically.
- **Role model.** `awa_owner` owns the schema (NOLOGIN); `awa_migrator` runs
  `awa migrate`; `awa_runtime` (least privilege, cannot alter schema) runs
  workers, producers, `awa serve`, and CLI admin.
- **v047+ migration bundles.** Use `awa migrate` or its generated `--sql`
  bundle rather than an isolated migration tail. Installer replay replaces
  triggers on ring state and queue heads and takes table locks through commit;
  schedule a quiet window, bound `lock_timeout`, and retry the whole transaction
  after a timeout. DDL credentials need ownership of all installed substrates.
- **CDC / logical replication.** Keep the awa schema (and any custom
  queue-storage schema) out of publications: list application tables, or with
  Debezium use `publication.autocreate.mode=filtered` plus
  `schema.exclude.list=awa,custom_queue_schema`, adding every installed custom
  queue-storage schema (replace the example name). Debezium matches each schema
  regex against the complete name; escape regex metacharacters in literal names.
  Alternatively, use `schema.include.list` for application schemas instead of an
  exclusion list; do not set both. See the [Debezium PostgreSQL filter reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-schema-exclude-list).
  If Awa is published anyway, schema v047+ works (every table has a replica
  identity); older schemas fail maintenance with
  `cannot delete from table ... does not have a replica identity`. Ring prune
  is a `TRUNCATE`, which publications publish by default. Slot lag can retain
  required WAL; lag alone does not establish an ordinary-table MVCC horizon pin.
  Inspect [`pg_replication_slots`](https://www.postgresql.org/docs/17/view-pg-replication-slots.html):
  `xmin` prevents vacuum from removing affected ordinary-table tuples, while
  `catalog_xmin` protects catalog tuples. Diagnose those horizons separately
  from WAL retention (`restart_lsn`) and consumer progress (`confirmed_flush_lsn`).

## Web Admin UI

`awa serve` is a read/write admin console, not the worker runtime. It has **no
built-in authentication** — treat it like a database admin console and restrict
access with ingress policy, firewall, or private networking. It binds
`127.0.0.1:3000` by default; set `--host 0.0.0.0` in containers.

Run one `awa serve` per database (single-backend by design; `--peer` links are
navigation only, no shared data plane). Use `--read-only` / `AWA_READ_ONLY=1`
for incident read-outs or against a replica — mutation endpoints return 503 and
the UI hides its buttons; read-only is auto-detected against a read-only
connection. Set `--callback-hmac-secret` to verify callback signatures; for
externally reachable callbacks with a private admin surface, run
`awa callbacks serve` instead, whose router has no admin routes.

`awa serve` caps every admin query at 60 seconds (`--statement-timeout` /
`AWA_STATEMENT_TIMEOUT`, in seconds; `0` keeps the server's setting). Postgres
keeps running a query after its HTTP request is abandoned, so this cap is what
stops repeated dashboard polls from piling up slow queries. When embedding
`awa_ui::router` in another service, build its pool with
`awa_ui::admin_pool_options(PgPoolOptions::new())` and
`connect_with(awa_ui::admin_connect_options(url.parse()?, Some(timeout)))`.
The release hook recovers connections whose request was dropped mid-query;
without it, a cancelled query can leak the pool slot over TLS.

## Before You Finish A Change

1. Confirm the resolved target and that you are on the intended context.
2. For a schema change, run `awa migrate --sql --pending` and read the plan.
3. For a storage or upgrade step, run `awa storage status` (and `finalize
   --check`) before and after.
4. Verify `awa health` and `/readyz` across the fleet, not just one worker.
5. Read the matching-version upgrade doc for gates, rollback limits, and
   ordering specific to the pair you are moving between.

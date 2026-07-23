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

Migrations are forward-only and transactional; there are no down migrations.
Roll back with a database snapshot or reverse SQL, not with the CLI.

```bash
awa migrate --pending                 # apply from current DB version to latest
awa migrate --sql --pending           # print the SQL instead of applying
awa migrate --extract-to ./out        # write SQL files for an external runner
```

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

Do not skip a major version. 0.5 → 0.7 is unsupported; step through 0.6.

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
rotation and prune, DLQ and descriptor cleanup, and cron evaluation. Every
worker dispatches and heartbeats its own attempts. There is no `pg_cron` or
external scheduler.

Late completions from a rescued attempt lose against the newer attempt via the
`run_lease` guard, so rescue never double-commits. Set `heartbeat_staleness`
(default 90s) to at least 3× the heartbeat interval; detection latency is
roughly `heartbeat_staleness + heartbeat_rescue_interval`.

Health signals:

- Runtime listener (opt in via `AWA_HEALTH_ADDR`): `GET /healthz` liveness (200
  even while draining) and `GET /readyz` (503 with failing checks:
  postgres_connected, schema_version, schema_compatible, poll/heartbeat/
  maintenance loops, shutting_down). `leader` is informational and never gates.
  The listener has no TLS or auth — keep it off public ingress.
- `awa health [--json]`: database-only readiness (reachable, schema migrated for
  this binary, fleet heartbeat counts). Exit 0 ready, 1 unreachable or
  unmigrated.

On Kubernetes, set `terminationGracePeriodSeconds` above the shutdown drain
timeout (e.g. 40s for a 30s drain) so a leader finishes draining before SIGKILL.

## Dead-Letter Queue

The DLQ is a separate physical table, not a job state; a DLQ'd job cannot be
claimed until retried out. It is per-queue opt-in (`dlq_enabled`) with a
default 30-day retention pruned by the maintenance leader. `dlq_reason` is
`max_attempts`, `manual`, or `deadline_expired`.

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

The CLI cron surface is `list` and `remove <name>` only. Pause, resume, and
trigger are admin operations (the web UI and admin API, and the Python client),
not the CLI. Only the maintenance leader evaluates due schedules; enqueue is
atomic and re-checks `paused_at` inside the same statement, so a pause races
cleanly against a fire — a pause takes effect even if it lands between the
evaluator's read and the enqueue. Schedules are declared in application code;
for timezone and missed-fire (`coalesce`/`catch_up`) semantics, see the
`awa-jobs` skill.

## Storage Transitions

`awa storage status` is the source of truth: it reports the transition state
(`canonical` → `prepared` → `mixed_transition` → `active`), current/active/
prepared engine, backlog, blockers, and `can_finalize`. Drive transitions with
the storage commands so routing, drain, readiness, and interlocks move
together — never by swapping which workers you run.

```bash
awa storage status
awa storage prepare --engine queue_storage
awa storage enter-mixed-transition        # needs a live queue_storage runtime
awa storage finalize --check              # dry run: exit 0 ready, exit 2 blocked
awa storage finalize --wait               # poll until gates stay clear
```

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

Rings rotate only when active — a frozen generation on an idle queue is healthy.
The condition to alert on is a frozen generation *with* accumulating rows
(`skipped_busy`): a pinned ring, almost always caused by a long-held snapshot.

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

## Before You Finish A Change

1. Confirm the resolved target and that you are on the intended context.
2. For a schema change, run `awa migrate --sql --pending` and read the plan.
3. For a storage or upgrade step, run `awa storage status` (and `finalize
   --check`) before and after.
4. Verify `awa health` and `/readyz` across the fleet, not just one worker.
5. Read the matching-version upgrade doc for gates, rollback limits, and
   ordering specific to the pair you are moving between.

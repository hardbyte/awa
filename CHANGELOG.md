# Changelog

Notable changes between releases. Detailed migration notes for storage
transitions live in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

## Unreleased

## [0.6.0-beta.1] — 2026-05-19

First beta of the 0.6 line. Frames the user-facing diff between
`0.5.x` and `0.6.0-beta.1` as one release; the `0.6.0-alpha.N`
entries below remain as the granular development log.

### What's new since 0.5.x

**Storage and durability**

- **Queue-storage engine, default-on**
  ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only
  `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries`
  paired with a partitioned receipt ring keep dead-tuple footprint
  bounded under sustained load. Replaces the row-mutating canonical
  engine for fresh installs.
- **Receipt-plane ring partitioning**
  ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)).
  `lease_claims` and `lease_claim_closures` partitioned by claim slot,
  rotated by the maintenance leader.
- **Per-claim deadlines** in receipts mode. Set
  `QueueConfig.deadline_duration` to bound a single attempt; rescue
  force-closes expired claims with `'deadline_expired'`.
- **Staged 0.5 → 0.6 transition tooling**: `awa storage prepare`,
  `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize`,
  `abort`. Fresh installs auto-finalize on first `awa migrate`. Full
  procedure in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

**Operator surfaces**

- **Dead Letter Queue**
  ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue
  `dlq_enabled` policy plus a full admin surface:
  `awa dlq depth | list | retry | retry-bulk | move | purge` (CLI),
  matching admin UI tab, Rust/Python client APIs.
- **Descriptor catalog**
  ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue
  and job-kind metadata (`display_name`, `description`, `owner`,
  `tags`, `docs_url`) drives admin UI labels and drift detection.
- **Cron missed-fire policy**. Periodic schedules persist an explicit
  `missed_fire_policy`: `coalesce` (default) or `catch_up`. Exposed in
  Rust/Python APIs and CLI schedule output.
- **`awa-pg[ui]` Python extra** + `python -m awa serve`. The bare
  `awa-pg` install stays small (workers/producers don't pay for the
  ~10 MB axum + dashboard bundle); `pip install 'awa-pg[ui]'` pulls in
  the `awa-cli` wheel and lets `python -m awa serve` launch the
  embedded React dashboard.
- **Managed-Postgres deployment guide**
  ([`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md)).
  Per-vCPU sizing data, Cloud SQL IAM grants, AlloyDB notes,
  auth-proxy sidecar pattern.

**Producer APIs and tuning**

- **Direct queue-storage COPY producer path** as the documented
  high-volume entry point. Rust:
  `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python:
  `Client.enqueue_many_copy(jobs)`. The compat-friendly
  `insert_many_copy_from_pool` / `client.insert_many_copy` routes
  through `awa.insert_job_compat()` once per row — fine for ad-hoc
  inserts but ~100–150 ms per row through a real DB proxy, not what
  you want for bursts.
- **`InsertOpts::ordering_key: Option<Vec<u8>>`** pins related jobs to
  the same shard at `enqueue_shards > 1`. Kafka-partition-key
  analogue: jobs sharing a key inherit that shard's strict FIFO. `None`
  rotates batches across shards.
- **`awa.queue_meta.enqueue_shards` per-queue semantic switch**
  ([ADR-025](docs/adr/025-sharded-enqueue-heads.md)). Default `1`
  preserves strict `(queue, priority)` FIFO. Setting `≥ 2` switches
  that queue to partitioned FIFO (strict within `(queue, priority,
  enqueue_shard)`, no order across shards) — comparable to choosing
  SQS Standard over FIFO.
- **Queue-storage completion-batcher defaults tuned** to `(batch=512, flush=1ms)`.
  Tunable via `AWA_COMPLETION_BATCH_SIZE` and `AWA_COMPLETION_FLUSH_MS`.

**Telemetry**

- **`awa-metrics` crate**. `AwaMetrics` and its `record_*` methods
  live in their own crate so non-runtime callers (`awa-ui`, `awa-cli`)
  can emit the same OTel counters as the worker. `awa-worker::AwaMetrics`
  re-exports for source compatibility.
- **Sub-second buckets on `awa.job.wait_duration`**. Previous boundaries
  didn't resolve pickup latency below 5 s.
- **`awa.enqueue.batch_size` and `awa.enqueue.duration`** histograms on
  the direct COPY enqueue path.
- **`awa.enqueue.shard` attribute on `awa.job.claimed`** so dashboards
  can see per-shard claim throughput.

### Upgrading from 0.5.x

- Fresh installs: nothing to do. `awa migrate` auto-finalizes to the
  queue-storage engine on first run.
- Existing 0.5.x clusters: walk the staged transition documented in
  [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). Roll out
  0.6 workers first (they understand both engines and the transition
  states), then walk `prepare → enter-mixed-transition → finalize`.
  **Rollback boundary:** one-way after `enter-mixed-transition`
  followed by any queue-storage write. Plan to restore from backup if
  rollback is needed past that point.
- Update the dependency: `awa = "0.6.0-beta.1"` (Rust) /
  `awa-pg==0.6.0-beta.1` (Python).

### Operating notes

- **Set `enqueue_shards` explicitly per queue.** The default `1` is the
  conservative FIFO-preserving choice but serialises producers on a
  single head row. We recommend `4` for contended hot queues. The
  trade-off is semantic, not a free perf win — see
  [`docs/configuration.md`](docs/configuration.md#sharding-the-enqueue-head-per-queue).
- **MVCC discipline.** If your application runs long-lived
  `REPEATABLE READ` / `SERIALIZABLE` transactions on the same database
  awa lives in, give awa its own database. Long-pinned `xmin` blocks
  dead-tuple cleanup and degrades queue-storage throughput over long
  horizons. Tracked in
  [#169](https://github.com/hardbyte/awa/issues/169).
- **Cloud SQL with IAM auth:** the runtime SA needs `cloudsqlsuperuser`
  for `prepare_schema` to install. Run `GRANT cloudsqlsuperuser TO
  "your-sa@project.iam"` once per instance. AlloyDB grants this
  automatically. Full guide:
  [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md).

### Changes since 0.6.0-alpha.9

For users already running an alpha:

- v021 shard-aware lane indexes on `ready_entries` / `done_entries` /
  `leases` partitions (#263). The narrow `(queue, priority, lane_seq)`
  indexes from before v017 didn't include `enqueue_shard`; on heavy
  backlog the planner discarded `(shards-1)/shards` of rows per
  partition per claim. Drain throughput at depth restored to
  equilibrium rate.
- `prepare_schema` startup race fixed (#264, #266). Concurrent worker
  startup on a fresh DB previously hung on PG18; install now runs in
  a single transaction with `pg_advisory_xact_lock`.
- `queue_storage_schema_ready` tightened to require every object the
  runtime depends on, so a partial install can't report ready (#266).
- Managed-Postgres deploy guide added (#265).
- `awa.enqueue.batch_size` / `awa.enqueue.duration` histograms added
  (#265).
- Direct queue-storage COPY producer path is the documented
  high-volume entry point (#263); compat path semantics unchanged.

### Performance
- Drop the `queue_lanes.available_count` cache (migration `v016`).
  The dispatcher derives availability from
  `queue_enqueue_heads.next_seq - queue_claim_heads.claim_seq` and
  the admin API (`queue_counts`) scans `ready_entries` for an exact
  count. Removes the queue-storage schema's largest dead-tuple
  source under pinned-xmin workloads (long-running reader
  transactions) without changing public API behaviour. See ADR-019
  § `lane_state` and segment cursor tables.
- Cache `(queue, priority, enqueue_shard)` lane presence in-process
  on the queue-storage path so subsequent enqueue batches skip the
  three `INSERT ... ON CONFLICT DO NOTHING` round-trips on
  `queue_lanes` / `queue_enqueue_heads` / `queue_claim_heads`. The
  cache invalidates and re-runs `ensure_lane` if a subsequent
  `UPDATE queue_enqueue_heads` finds no row (the observable signal
  of an earlier ensure_lane that ran inside a rolled-back
  transaction), so correctness is preserved.
- Shard the queue-storage active plane by `enqueue_shard` (migration
  `v017`). `awa.queue_meta.enqueue_shards` (default 1, range 1..=64)
  is a per-queue semantic mode switch — comparable to choosing SQS
  Standard over SQS FIFO, raising Kafka partition count, or using
  Pub/Sub ordering keys. The ordering contract at `enqueue_shards = 1`
  is unchanged (strict FIFO per `(queue, priority)`); at
  `enqueue_shards > 1` it becomes **partitioned FIFO** — strict FIFO
  within `(queue, priority, enqueue_shard)`, no order promised
  across shards. The shard column runs end-to-end:
  `queue_enqueue_heads`, `queue_claim_heads`, `ready_entries`,
  `leases`, and `done_entries` carry it in their primary keys;
  `lease_claims` carries it as a regular column. Raising the value
  per noisy queue delivers near-linear throughput scaling on
  contended enqueue workloads: a 16-producer same-queue local sweep
  measured 1.0× → 1.60× → 2.75× → 3.69× at S=1/2/4/8. Scope: this
  addresses producer-side enqueue contention. The
  high-worker-count rescue-path regression (1 replica × 256
  workers, receipts on, `LEASE_DEADLINE_MS` A/B) is a separate
  effect on the claim / rescue side and remains open for follow-up
  measurement. See ADR-025 for the full design.
- **Shard-aware lane indexes on `ready_entries`, `done_entries`, and
  `leases` child partitions** (migration `v021`,
  [#263](https://github.com/hardbyte/awa/pull/263)). The narrow
  `(queue, priority, lane_seq)` lane indexes that predated v017 did
  not include `enqueue_shard`, while `claim_ready_runtime` (since
  v017) filters on it. Under `enqueue_shards > 1` and deep backlog
  the planner scanned the lane index forward and post-filtered
  shard, discarding roughly `(shards-1)/shards` of rows per
  partition per claim. v021 replaces those with
  `(queue, priority, enqueue_shard, lane_seq)` indexes and drops
  the originals; `prepare_schema` is updated to match for fresh
  installs. On a Cloud SQL PG18 16 vCPU staging instance with a 3.5M
  row backlog, end-to-end drain went from ~1,300 jobs/s to
  ~8,800–10,600 jobs/s (single-claim probe: 11.4 ms → 0.81 ms).

### Added
- `InsertOpts::ordering_key: Option<Vec<u8>>` pins related jobs to
  the same shard at `enqueue_shards > 1`. The key bytes are mapped
  by Awa's portable shard hash and reduced modulo the queue's shard
  count, so Rust, SQL, and Python enqueue paths agree on routing.
  Use it as a Kafka-partition-key analogue: jobs for the same
  customer / order / account share a shard and inherit that shard's
  strict FIFO. `None` falls back to one rotor pick per `(queue,
  priority)` sub-batch. Ignored at `enqueue_shards = 1`.
- `awa.job.claimed` counter now carries an `awa.enqueue.shard`
  attribute on the queue-storage claim path so dashboards can see
  per-shard claim throughput. Canonical claims continue to emit the
  un-decorated series; the two attribute sets do not overlap, so
  Prometheus `sum by (awa_enqueue_shard)(rate(awa_job_claimed[1m]))`
  is the per-shard fairness panel.
- Tune the queue-storage completion-batcher defaults to
  `(batch=512, flush=1ms)`. The larger batch cap keeps the durable
  completion path amortised under load, while the 1 ms flush keeps
  low-latency finalization for short jobs. Tunable via
  `AWA_COMPLETION_BATCH_SIZE` and `AWA_COMPLETION_FLUSH_MS`.
- **Direct queue-storage COPY producer path is the documented
  high-volume entry point** ([#263](https://github.com/hardbyte/awa/pull/263)).
  Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python:
  `Client.enqueue_many_copy(jobs)`. `insert_many_copy_from_pool` /
  `client.insert_many_copy` remain available as the compatibility
  COPY surface and route through `awa.insert_job_compat()`. See
  [`docs/configuration.md`](docs/configuration.md#producer-path-choice).
- **Explicit sub-second buckets on `awa.job.wait_duration`**
  ([#263](https://github.com/hardbyte/awa/pull/263)). Histogram now
  resolves pickup latency below 5 s, which the previous bucket
  boundaries did not.
- **`awa.enqueue.batch_size` and `awa.enqueue.duration` histograms**
  ([#265](https://github.com/hardbyte/awa/pull/265)). Per-batch
  observability on the direct COPY enqueue path. Lets producers tell
  "batches are tiny" from "batches are slow" when an enqueue rate
  target is missed — neither was distinguishable from existing
  metrics. Recorded by Python `Client.enqueue_many_copy` (sync and
  async). Rust callers using `QueueStorage::enqueue_params_copy`
  directly can record via
  `AwaMetrics::from_global().record_enqueue_batch(queue, count, duration)`.
- **Managed-Postgres deployment guide**
  ([#265](https://github.com/hardbyte/awa/pull/265)). New
  [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md)
  covering Cloud SQL and AlloyDB specifics: per-vCPU sizing
  measurements from staging, Postgres-version recommendations, IAM
  `cloudsqlsuperuser` grant for Cloud SQL IAM users, the auth-proxy
  native-sidecar pattern, and `enqueue_shards` operator guidance.

### Fixed
- **`prepare_schema` no longer deadlocks on fresh-DB / small-pool /
  concurrent worker startup** ([#264](https://github.com/hardbyte/awa/issues/264),
  [#266](https://github.com/hardbyte/awa/pull/266)). Previously the
  install acquired `pg_advisory_lock(…)` on one dedicated pool
  connection but issued every DDL through `.execute(pool)`, which
  acquired *different* connections per statement. With a one-
  connection pool that self-deadlocked; with a small pool and
  concurrent worker startup it exhausted the pool with every worker
  holding one connection for the lock and competing for the rest for
  DDL. The PG18-on-fresh-DB hang reported in #264 is the most
  visible symptom. Install now runs in a single transaction on a
  single connection with `pg_advisory_xact_lock(…)`; the lock
  auto-releases on COMMIT/ROLLBACK and the install is atomic.
- **`queue_storage_schema_ready` checks the full required surface**
  ([#266](https://github.com/hardbyte/awa/pull/266)). Previously it
  only checked for `queue_ring_state`, `ready_entries`, and `leases`;
  if `prepare_schema` had failed partway through (a real failure
  mode under the old non-atomic install) the schema could report
  ready while `awa.job_id_seq`, `done_entries`, `deferred_jobs`, or
  the `claim_ready_runtime` function were missing — at which point
  producers got `relation "awa.job_id_seq" does not exist`. The
  check now requires every queue-storage substrate object the
  runtime depends on.
- **`AwaMetrics::from_global()` no longer rebuilt per call on hot
  paths** ([#265](https://github.com/hardbyte/awa/pull/265)).
  `from_global()` registers the entire instrument set on every
  invocation. The Python `Client`, `awa-ui` `AppState`, and
  `awa-cli` DLQ command now cache a single `AwaMetrics` handle and
  reuse it instead of rebuilding ~30 instruments per operation.

## [0.6.0-alpha.9] — 2026-05-08

### Fixed

- **Queue-storage priority-aging completions now preserve the lane priority in
  terminal storage.** Workers still receive the aged effective priority, with
  `_awa_original_priority` in metadata, but `done_entries` now uses the original
  queue lane priority for its storage key. This prevents aged low-priority jobs
  from colliding with high-priority jobs that share the same per-lane sequence.

## [0.6.0-alpha.8] — 2026-05-08

### Added

- **Lifecycle hooks now include `Started` events**. `JobEvent<T>` and
  `UntypedJobEvent` fire `Started` after a claim commits and just before the
  worker handler is invoked, so applications can observe job execution
  beginning as well as final outcomes.

### Changed

- **Breaking alpha API change:** exhaustive matches on `JobEvent<T>` or
  `UntypedJobEvent` must handle the new `Started` variant. No stable Awa
  release has been published yet, so this is documented as an alpha-series
  source break.

- **Reduced queue-storage per-claim deadline overhead.** Fresh queue-storage
  schemas now use a low-write BRIN index for `lease_claims.deadline_at`, and
  the claim helper computes claim/deadline timestamps once per batch instead of
  per returned row. Deadline rescue semantics are unchanged; this targets the
  alpha.8 benchmark path where non-zero `QueueConfig::deadline_duration` is
  enabled for short receipt-plane jobs.

## [0.6.0-alpha.7] — 2026-05-07

### Added

- **Cron missed-fire policy** ([#239](https://github.com/hardbyte/awa/issues/239),
  [#240](https://github.com/hardbyte/awa/pull/240)). Periodic schedules now
  persist an explicit `missed_fire_policy`: `coalesce` remains the default and
  enqueues only the latest due fire after delayed evaluation, while `catch_up`
  enqueues missed fires in timestamp order for idempotent reconciliation jobs.
  The policy is exposed through the Rust and Python APIs, CLI schedule output,
  docs, and migration v015.

### Changed

- **Cron evaluation runs in its own leader-scoped maintenance lane**
  ([#240](https://github.com/hardbyte/awa/pull/240)). Slow cleanup, rotation,
  or reconciliation work no longer starves the scheduler branch. Coalesced
  schedules also use a bounded latest-fire lookup so a stale high-frequency
  schedule does not scan every missed tick.
- **Runtime grants now document required `TRUNCATE` privileges**
  ([#239](https://github.com/hardbyte/awa/issues/239),
  [#240](https://github.com/hardbyte/awa/pull/240)). The security guide now
  includes `TRUNCATE` in runtime table grants and default privileges, with a
  DB-backed regression covering the previously documented grants.

### Fixed

- **Queue-storage claim transactions commit before rescueable runtime payload
  conversion** ([#222](https://github.com/hardbyte/awa/pull/222)). Runtime
  claims now keep hot claim transactions shorter while preserving rollback
  behavior for legacy zero-deadline claims that cannot be rescued after a
  conversion error.

## [0.6.0-alpha.6] — 2026-05-07

### Added

- **`awa-metrics` crate** ([#176](https://github.com/hardbyte/awa/issues/176),
  [#232](https://github.com/hardbyte/awa/pull/232)). The `AwaMetrics` type
  and its `record_*` methods move from `awa-worker` into a dedicated
  `awa-metrics` crate so non-runtime callers (`awa-ui`, `awa-cli`) can emit
  the same OTel counters as the worker without depending on the
  dispatcher/runtime crate graph. `awa-worker::AwaMetrics` re-exports for
  source compatibility — no semver break for `awa-cli` or `awa-python`.
  `awa-metrics::names` exposes public string constants for every metric, and
  `AwaMetrics::new()` is built from those constants so registered
  instruments and the public names can't drift.

### Changed

- **Multi-queue NOTIFY collapses to one round-trip per enqueue tx**
  ([#235](https://github.com/hardbyte/awa/pull/235)). The three queue-storage
  enqueue paths (`enqueue_runtime_rows`, `enqueue_params_batch`,
  `enqueue_params_copy`) used to issue one `pg_notify($1, '')` per distinct
  destination queue inside the enqueue transaction. Now routed through
  `notify_queues_tx`, which uses
  `SELECT pg_notify(channel, '') FROM unnest($1::text[])` so any number of
  queues becomes one round-trip. Single-queue enqueue (the common case) is
  unchanged.
- **Completion path: lease delete and `attempt_state` cleanup merge into a
  single CTE-as-DML statement** ([#235](https://github.com/hardbyte/awa/pull/235)).
  `complete_runtime_batch` previously issued two consecutive DELETEs (leases,
  then `attempt_state`) on the receipt-disabled path and on the materialized
  fallback inside the receipt-enabled path. They now share one statement —
  one fewer round-trip per completion batch, identical atomicity and
  return shape.

### Fixed

- **`awa.job.dlq_retried` tagged with the source queue** ([#232](https://github.com/hardbyte/awa/pull/232)).
  Single-job retry from `awa-ui` previously read `job.queue` from the
  returned `JobRow`, which carries the *destination* queue if the request
  body supplied a `queue` override. The metric now looks the source queue
  up before retry runs (matching `record_dlq_purged`'s pattern).

## [0.6.0-alpha.5] — 2026-05-04

### Added

- **`awa-pg[ui]` optional extra** ([#186](https://github.com/hardbyte/awa/issues/186)).
  `pip install 'awa-pg[ui]'` pulls in the [`awa-cli`](https://pypi.org/project/awa-cli/)
  wheel so `python -m awa serve` (and `awa serve` directly) launches the
  embedded React dashboard. The default `awa-pg` install stays small —
  workers and producers don't pay for the ~10 MB axum + UI bundle they
  don't need.
- `python -m awa serve` is now a subcommand. It detects the `awa` binary
  in `sys.prefix/{bin,Scripts}` (where `awa-cli`'s wheel installs it) and
  forwards the full argument tail verbatim. If the extra isn't installed,
  it exits with a `pip install 'awa-pg[ui]'` hint.

### Fixed

- **Restored queue-storage dispatcher throughput under high concurrency**
  ([#223](https://github.com/hardbyte/awa/issues/223)). Capacity-release wakes
  still drain ready work immediately, but the dispatcher now uses the configured
  fixed fallback poll interval instead of geometrically backing off after empty
  or permit-saturated polls.

### Changed

- Queue-storage throughput benchmarks can run against a non-canonical storage
  schema and configurable worker count, making local A/B checks safer and
  easier to reproduce.
- Added TLA+ trace witnesses for receipt-only cancel, callback wait, and DLQ
  purge paths, plus documentation alignment for the queue-storage design.

## [0.6.0-alpha.4] — 2026-05-03

### Changed

- Added capacity-wake suppression to reduce empty claim churn in quiet queues.
  This improved some operational churn metrics but regressed high-concurrency
  queue-storage throughput; alpha.5 keeps the useful wake-drain repair while
  restoring fixed fallback polling.

## [0.6.0-alpha.3] — 2026-05-02

### Changed

- **Completion-batcher default size lowered from `512` to `128`.** Cross-system
  matrix runs (1–4 worker processes × 16–128 workers per process) showed `128`
  delivered the lowest p99 in every cell and `512` bought no throughput while
  hurting tail latency under multi-process deployments. Override via
  `AWA_COMPLETION_BATCH_SIZE`. See `docs/benchmarking.md` for tuning notes.
- **Reduced queue-storage claimer heartbeat churn.** Claimer leases now skip
  refresh writes while still fresh, cutting coordination writes in the dispatch
  path without changing claim ownership semantics.
- **Updated architecture documentation.** The architecture guide now reflects
  the queue-storage receipt path, lazy lease materialization, crash recovery,
  maintenance leadership, and callback orchestration.

### Fixed

- **Receipt completion now serializes with heartbeat materialization.** The
  queue-storage completion path locks the matching receipt claim before writing
  its closure, preventing a concurrent heartbeat from recreating
  `attempt_state` after completion.
- **Hardened mixed Rust/Python chaos smoke coverage.** The mixed-fleet smoke
  test now waits for worker-observed completions from both runtimes instead of
  relying on transient terminal-row presence.

## [0.6.0-alpha.2] — 2026-05-02

### Added

- **Vacuum-aware queue storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)).
  Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and
  `dlq_entries` tables, paired with a partitioned receipt ring, keep the
  dead-tuple footprint bounded under sustained load. Replaces the
  canonical row-mutating engine for new installs.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)).
  `lease_claims` and `lease_claim_closures` are partitioned by claim
  slot and rotated by the maintenance leader.
- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)).
  Per-queue `dlq_enabled` policy and a full operator surface:
  `awa dlq depth | list | retry | retry-bulk | move | purge`, plus the
  matching admin UI tab. See [`docs/dead-letter-queue.md`](docs/dead-letter-queue.md).
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)).
  Code-declared queue and job-kind metadata (`display_name`,
  `description`, `owner`, `tags`, `docs_url`) drives admin UI labels
  and stale/drift detection.
- **Per-claim deadlines** in receipts mode. `QueueConfig.deadline_duration`
  writes `lease_claims.deadline_at`; the rescue path force-closes
  expired claims with `'deadline_expired'`.
- **Storage transition tooling**. `awa storage prepare`,
  `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize`,
  and `abort` cover the staged upgrade path. Fresh installs auto-finalize
  on first migrate.
- **`transition_role` runtime capability**. The `enter_mixed_transition`
  SQL gate requires a live `queue_storage_target` runtime, so a stale
  fleet cannot accidentally skip the staged path.
- **Migrations** v012, v013, and v014. All idempotent.

### Changed

- New installs default to the queue-storage engine; canonical
  row-mutating storage is no longer the implicit backend.
- Receipts mode is on by default for fresh deployments.

### Removed

- `benchmarks/portable/` extracted to its own repo at
  [hardbyte/postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking).
- The pre-0.6 `EXPERIMENTAL_LEASE_CLAIM_RECEIPTS` env alias.

### Upgrade notes

- Update your dependency to `awa = "0.6"` (Rust) /
  `awa-cli`, `awa-pg` (Python) at the matching version.
- Existing 0.5.x clusters with canonical data must walk the staged
  storage transition documented in
  [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). Fresh
  installs auto-finalize.
- Rollback after `enter-mixed-transition` followed by queue-storage
  writes is one-way (database restore only).

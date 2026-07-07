# Changelog

Notable changes between releases. Detailed migration notes for storage transitions live in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

## [Unreleased]

### Breaking changes

- **0.7 migrate gate ([#370](https://github.com/hardbyte/awa/issues/370), [ADR-037](docs/adr/037-canonical-engine-deprecation.md)).** `awa migrate` (and `awa_model::migrations::run`) now refuses to apply pending migrations unless the storage transition is finalized (`state = active`) or the install is fresh (no jobs, no recently-live runtimes — the same conditions as worker-startup auto-finalize). Nothing is applied on refusal; the error names the finalize steps. Complete the staged transition on 0.6 binaries first: see [`docs/upgrade-0.6-to-0.7.md`](docs/upgrade-0.6-to-0.7.md).
- **Canonical engine deprecated.** Runtimes whose effective storage resolves to the canonical engine log a startup deprecation warning; canonical claim/execution/trigger paths will be removed in 0.8.

### Correctness

- `AwaStorageTransition.tla` models the 0.7 migrate gate (`Migrate07OnlyOnQuiescedCanonical`), with an expected-counterexample config (`AwaStorageTransitionMigrate07Ungated.cfg`) witnessing why the gate is required; the transition model family runs in nightly CI via the TLC suite runner (`correctness/run-tlc-suite.sh`).

### Features

- **Worker health & readiness endpoints ([#368](https://github.com/hardbyte/awa/issues/368)).** Opt-in HTTP listener in the worker runtime (`AWA_HEALTH_ADDR` or `ClientBuilder::health_addr`): `GET /healthz` (process liveness, stays `200` through graceful drain) and `GET /readyz` (`503` + JSON reasons when Postgres is unreachable, the schema is older than the binary, a claim loop stalls, heartbeat/maintenance die, or shutdown starts). No new dependencies in `awa-worker`. Kubernetes probe examples in `docs/deployment.md`.
- **`awa health` CLI probe.** Cluster-level readiness from the database alone (reachable, schema migrated for this binary, storage state, fleet heartbeat counts) with `--json` output and exit codes for probe-less environments.

### Internal

- **Nightly binary/schema compat matrix ([#367](https://github.com/hardbyte/awa/issues/367)).** Pinned release artifacts (`awa-pg` 0.6.0 and 0.5.7 wheels from PyPI) run against the newest schema every night: the 0.6.0 leg asserts the full job lifecycle on a finalized cluster; the 0.5.7 leg asserts the documented asymmetry (producers route through the compat layer to the active engine, workers are inert); the backward-guard leg asserts the newest `awa migrate` refuses an old unfinalized schema with the exact finalize-steps message and a non-zero exit. The support statement lives in `docs/stability.md`.

- **CI: the Rust tests job is sharded ([#335](https://github.com/hardbyte/awa/issues/335)).** One `cargo test --workspace` run took ~31 minutes, ~22 of them the migration replay binary alone. The job is now a six-way matrix: the migration suite partitioned per-test across four shards with `cargo-nextest` (safe because those tests already serialize through a Postgres advisory lock), the other slow binaries in a `heavy` shard, and everything else — including any new test file, automatically — in `rest`. Shard membership lives in `scripts/ci-test-shard.sh`; the lint job validates it so renames can't drop coverage, and the nextest profile flags any test slower than 60s as the standing worklist for shrinking real-time fixture windows.

## [0.6.1] — 2026-07-07

Patch release: one canonical-engine bug fix, no migrations, no API changes.

### Fixed

- **Rescue sweeps no longer wedge on a unique-key conflict ([#388](https://github.com/hardbyte/awa/issues/388)) — both engines.** A unique job whose `unique_states` mask excludes `running` holds no claim while executing, so a newer duplicate can legitimately be enqueued and take the claim; returning the older job to a claiming state then conflicts. On the **canonical** engine the batched rescue UPDATEs (`running -> retryable`, `waiting_external -> retryable`) tripped the `sync_job_unique_claims` trigger (`idx_awa_jobs_unique`) and the whole 500-row sweep aborted every maintenance tick. On the **queue-storage** engine the same shape hid one layer deeper: rescue re-inserts each rescued job as a `retryable` deferred row inside one batch transaction, and `sync_unique_claim` raised `UniqueConflict` — aborting the whole batch identically. Both engines now degrade per row: clean rows rescue as before; the conflicted row is **cancelled** with an error entry (`rescued as duplicate: unique claim held by a newer job`) — the claim holder wins — via a savepoint on queue storage (terminal counters stay exact) and a row-at-a-time retry on canonical. Fallback cancellations dispatch the normal `Cancelled` lifecycle event and are counted under `awa.rescue.kind = "<kind>_duplicate_cancelled"`. Modelled in `correctness/storage/AwaCanonicalUniqueRescue.tla` (per-row config passes the `Convergence` liveness property; the batch-only config is the retained production-wedge counterexample), with end-to-end regression tests on both engines.

- **Admin UI no longer renders storage *capability* as the engine in use.** The per-instance badge previously showed `storage_capability` — which reads `queue_storage` for every capable worker even on a fully canonical cluster — and the sidebar cluster chip aggregated the same field. The runtime page now derives and shows the **effective engine** (from capability, transition role, and the authoritative transition state), noting capability only when it adds information, and the sidebar chip labels the cluster from `/api/storage` instead. Publishing the resolved engine in runtime snapshots (so the UI need not derive it) is tracked in [#391](https://github.com/hardbyte/awa/issues/391).

### Operator note

If a cluster is currently wedged (repeating `Failed to rescue ... idx_awa_jobs_unique` logs), upgrading is sufficient — the next sweep resolves it. The mask itself is still worth revisiting: on the canonical engine, a `unique_states` mask that a runtime transition can *enter* from outside (e.g. including `retryable` but not `running`) always risks superseded-job cancellations; masks closed under retry/rescue/promotion (`{}` or the full non-terminal set) avoid the conflict entirely. See the new [troubleshooting entry](docs/troubleshooting.md#rescue-fails-with-idx_awa_jobs_unique).
## [0.6.0] — 2026-07-04

The 0.6 line makes the append-only **queue-storage engine** the default and the supported substrate for high-throughput, low-bloat operation on managed Postgres. The [#169](https://github.com/hardbyte/awa/issues/169) pinned-MVCC dead-tuple gate **passed** on `main`: after the rc line removed the last dominant hot-row update path (the `queue_claim_heads` ready-segment routing cache), a 60-minute idle-in-transaction soak holds bounded queue depth through the pinned hour and drains fully in recovery. See "Benchmark evidence" below for the caveats — awa is not immune to long readers, and this release does not claim otherwise.

This entry frames the consolidated `0.5.x → 0.6.0` diff as one release; the dated `0.6.0-{alpha,beta,rc}` sections below remain the granular development log. Migrations v001–v039 apply via `awa migrate`; existing installs walk the staged transition (see "Upgrading from 0.5.x"). The SQL-only path for external migration tooling is documented in [`docs/migrations.md`](docs/migrations.md).

### What's new since 0.5.x

**Storage and durability**

- **Queue-storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries` paired with a partitioned receipt ring keep dead-tuple footprint bounded under sustained load. Replaces the row-mutating canonical engine for fresh installs.
- **No remaining hot-row update paths on the claim/complete loop.**
  - Lane cursors moved from MVCC-updated rows to Postgres sequences ([#321](https://github.com/hardbyte/awa/pull/321), v027).
  - Completion, DLQ, retry, and discard no longer `DELETE FROM ready_entries`; they route through append-only ready segments plus a tombstone ledger ([#323](https://github.com/hardbyte/awa/pull/323), v028).
  - Terminal counts append signed rows to a delta ledger that the maintenance leader folds asynchronously ([#329](https://github.com/hardbyte/awa/pull/329), v030, [ADR-026](docs/adr/026-narrow-terminal-history.md)).
  - The per-claim `queue_claim_heads` routing-cache update — the last dominant dead-tuple accumulator under a pinned horizon — is gone; claim routes through the `ready_segments` control plane instead ([#355](https://github.com/hardbyte/awa/pull/355), v039).
  - Rollup mutation and segment truncation stand down while another backend pins the MVCC horizon, then catch up once it clears ([#333](https://github.com/hardbyte/awa/pull/333)).
- **Compact receipt claim and completion batches** ([#352](https://github.com/hardbyte/awa/pull/352), v036–v038, [ADR-026](docs/adr/026-narrow-terminal-history.md)). Receipt-backed successful completions write compact terminal batches instead of one `done_entries` row per job, with compact claim-local closure, ready-segment, claim-attempt, and lease-claim evidence so stale scans stay bounded. `{schema}.terminal_jobs` is the public terminal read surface — direct SQL against `done_entries` must not assume all completed jobs are physically stored there.
- **Failed-terminal retention floor** ([#337](https://github.com/hardbyte/awa/issues/337), v032, [ADR-032](docs/adr/032-failed-terminal-retention.md)). Non-DLQ `failed` terminal rows stay retryable for at least `failed_retention` (default `72h`); rows aged past the floor are folded into a cumulative, monotonic `QueueCounts.pruned_failed` so operators have standing visibility of loss that was previously silent. `retry_failed_by_kind` / `retry_failed_by_queue` report the matched/retried/pruned split rather than silently dropping ids.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)). `lease_claims` and `lease_claim_closures` partitioned by claim slot, rotated by the maintenance leader, with bounded per-slot cursors so rescue/close/deadline sweeps cost work proportional to live receipts ([#349](https://github.com/hardbyte/awa/pull/349), v033–v035).
- **Per-claim deadlines** in receipts mode. Set `QueueConfig.deadline_duration` to bound a single attempt; rescue force-closes expired claims with `'deadline_expired'`.
- **Staged 0.5 → 0.6 transition tooling**: `awa storage prepare`, `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize` (with `--wait` / `--check`, [#298](https://github.com/hardbyte/awa/pull/298)), `abort`, plus a storage-transition readiness UI ([#299](https://github.com/hardbyte/awa/pull/299)). `awa migrate` owns the `awa.*` substrate DDL ([#308](https://github.com/hardbyte/awa/issues/308), v023) — it is no longer installed opportunistically, and `awa --reset --schema awa` is rejected. Fresh installs auto-finalize on first `awa migrate`. Full procedure in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

**Operator surfaces**

- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue `dlq_enabled` policy plus a full admin surface: `awa dlq depth | list | retry | retry-bulk | move | purge` (CLI), matching admin UI tab, Rust/Python client APIs.
- **Durable batch operations** ([#328](https://github.com/hardbyte/awa/pull/328), v029, [ADR-030](docs/adr/030-batch-operations.md)). Crash-safe, maintenance-driven bulk `set_priority` and `move_queue` over job selections, with preview/submit/list/get/cancel/purge through the HTTP API, CLI, and admin-UI Batch Ops tab; Python parity via [#332](https://github.com/hardbyte/awa/pull/332). The first-class reprioritization surface ([#307](https://github.com/hardbyte/awa/issues/307)).
- **Transaction-scoped admin cancel** ([#357](https://github.com/hardbyte/awa/pull/357)/[#358](https://github.com/hardbyte/awa/pull/358)). `admin::cancel_tx` and `admin::cancel_by_unique_key_tx` take a caller-provided `&mut PgConnection` and run the cancellation (and the cooperative `awa:cancel` NOTIFY) atomically with the caller's other work; the pool-based `cancel` / `cancel_by_unique_key` are unchanged.
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue and job-kind metadata (`display_name`, `description`, `owner`, `tags`, `docs_url`) drives admin UI labels and drift detection.
- **Cron missed-fire policy** (`coalesce` default / `catch_up`) plus **pause / resume** ([#320](https://github.com/hardbyte/awa/pull/320), v026): `POST /api/cron/{name}/pause` / `/resume`, evaluator skips paused rows, manual `trigger_cron_job` bypasses pause, `/cron` UI controls.
- **Metrics off the exact-scan path** ([#330](https://github.com/hardbyte/awa/pull/330), v031) — worker health reads lane-head cursor signals instead of scanning ready rows — plus **maintenance branch observability** ([#302](https://github.com/hardbyte/awa/pull/302)).
- **`awa-pg[ui]` Python extra** + `python -m awa serve`. The bare `awa-pg` install stays small; `pip install 'awa-pg[ui]'` pulls in the `awa-cli` wheel and launches the embedded React dashboard.
- **Managed-Postgres deployment guide** ([`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md)). Per-vCPU sizing data, Cloud SQL IAM grants, AlloyDB notes, auth-proxy sidecar pattern, and MVCC operator guidance (separate database for long readers, `idle_in_transaction_session_timeout`, `xact_start` alerting).

**Producer APIs and tuning**

- **`PartitionedQueue`** ([#348](https://github.com/hardbyte/awa/pull/348), [ADR-031](docs/adr/031-partitioned-queues.md)) — deterministic routing from one hot logical queue to multiple physical partitions. Partition 0 is the logical queue name itself (`email`, `email__p1`, …) so direct enqueues to the logical name stay consumable, and key→partition selection uses a domain-separated hash so it composes with ADR-025 enqueue-shard routing. Replaces the beta-series `QueueFanout` (see Breaking changes).
- **Per-job routing in Python COPY batch APIs** ([#348](https://github.com/hardbyte/awa/pull/348)). `insert_many_copy` / `enqueue_many_copy` accept `opts=[...]` entries that override `queue` and `ordering_key` per job, so one COPY call can carry a mixed-partition batch.
- **Transactional follow-up jobs** ([ADR-029](docs/adr/029-transactional-followup-jobs.md), [#285](https://github.com/hardbyte/awa/pull/285)/[#288](https://github.com/hardbyte/awa/pull/288)). `ClientBuilder::on_completed_enqueue` (and friends) registers a follow-up job inserted in the same transaction as the lifecycle transition.
- **Callback-only router and user-owned callback layers** ([#291](https://github.com/hardbyte/awa/pull/291)/[#293](https://github.com/hardbyte/awa/pull/293)) with a configurable URL prefix — embed Awa's router or implement the contract in your own API layer (axum and FastAPI examples). The `WaitingForCallback` lifecycle event plus client-side `resolve_callback` / `complete_external` / `fail_external` / `retry_external` ([#276](https://github.com/hardbyte/awa/pull/276)) make parked jobs visible to hooks and resolvable in-process.
- **Direct queue-storage COPY producer path** as the documented high-volume entry point. Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python: `Client.enqueue_many_copy(jobs)`. The compat `insert_many_copy` route remains for ad-hoc inserts.
- **`InsertOpts::ordering_key`** pins related jobs to the same shard at `enqueue_shards > 1` (a Kafka-partition-key analogue), and **`awa.queue_meta.enqueue_shards`** ([ADR-025](docs/adr/025-sharded-enqueue-heads.md)) is the per-queue semantic switch: default `1` preserves strict `(queue, priority)` FIFO; `≥ 2` switches to partitioned FIFO.
- **Completion-batcher defaults tuned** to `(batch=512, flush=1ms)` with adaptive completion shards, tunable via `AWA_COMPLETION_BATCH_SIZE` / `AWA_COMPLETION_FLUSH_MS` / `AWA_COMPLETION_SHARDS`.
- **`awa-seaorm` crate** ([#275](https://github.com/hardbyte/awa/pull/275)) for transactional enqueue alongside SeaORM application writes.
- **Registered handler futures no longer require `Sync`** ([#331](https://github.com/hardbyte/awa/pull/331)) — only `Send + 'static`, so handlers awaiting `sqlx` / `reqwest` compile without workarounds.

**Telemetry**

- **`awa-metrics` crate** so non-runtime callers (`awa-ui`, `awa-cli`) emit the same OTel counters as the worker (`awa-worker::AwaMetrics` re-exports for source compatibility).
- **Sub-second buckets on `awa.job.wait_duration`**, **`awa.enqueue.batch_size` / `awa.enqueue.duration` histograms**, and an **`awa.enqueue.shard` attribute on `awa.job.claimed`** for per-shard claim throughput.
- **`queue_counts_fast`** ([#289](https://github.com/hardbyte/awa/pull/289)) — index-only depth probe for high-cadence pollers. It under-counts unrolled terminal rows; use `queue_counts` when exact terminal counts matter.
- **Exact terminal counts without `done_entries` scans** ([#290](https://github.com/hardbyte/awa/issues/290) via [#304](https://github.com/hardbyte/awa/pull/304)/[#306](https://github.com/hardbyte/awa/pull/306)): folded counters serve exact reads when the terminal-counter trust marker is set (`awa storage rebuild-terminal-counters`; fresh installs auto-mark).
- **Maintenance branch duration histograms** + `awa_maintenance_branch_overrun_total{branch=…}` ([#302](https://github.com/hardbyte/awa/pull/302)).

### Breaking changes

Coming from 0.5.x, only the storage transition (below) needs action — the 0.5 public client API is otherwise source-compatible. The renames below landed within the 0.6 beta/rc series, so early-0.6 adopters must handle them:

- **`QueueFanout` → `PartitionedQueue`** ([#348](https://github.com/hardbyte/awa/pull/348)), no compatibility alias: `width` → `partitions`, `queue_fanout` → `partitioned_queue`, Python `*_per_queue` kwargs → `*_per_partition`. Routing also differs from the beta.2 `QueueFanout` (partition 0 is the logical name; domain-separated hash), so a given key may land on a different partition. Drain fanout queues before upgrading, or pin explicit names via `from_physical_queues`.
- **`QueueCounts.completed` → `QueueCounts.terminal`** ([#306](https://github.com/hardbyte/awa/pull/306)) — it counts completed, cancelled, and discarded terminal rows; `QueueCounts` also gains `pruned_failed`.
- **`retry_failed_by_kind` / `retry_failed_by_queue` return `RetryFailedOutcome { retried, matched, pruned_failed_count }`** ([#337](https://github.com/hardbyte/awa/issues/337)) instead of a bare `Vec<JobRow>`; Python `retry_failed` / `retry_failed_sync` return the outcome rather than a plain list.
- **`JobEvent` / `UntypedJobEvent` gain a `WaitingForCallback` variant** ([#276](https://github.com/hardbyte/awa/pull/276)) — exhaustive matches need a new arm.
- **`PruneOutcome::Pruned` gains `carried_failed_rows`** and **`QueueStorage::prune_oldest` takes a `failed_retention: Duration`** ([#337](https://github.com/hardbyte/awa/issues/337)) — `Duration::ZERO` disables the floor.

### Upgrading from 0.5.x

- **Fresh installs:** nothing to do. `awa migrate` auto-finalizes to the queue-storage engine on first run.
- **Existing 0.5.x clusters:** roll out 0.6 workers first (they understand both engines and the transition states), then walk `prepare → enter-mixed-transition → finalize`, following [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). **Rollback boundary:** one-way after `enter-mixed-transition` followed by any queue-storage write — plan to restore from backup if rollback is needed past that point.
- Update the dependency: `awa = "0.6.0"` (Rust) / `awa-pg==0.6.0` (Python).

### Operating notes

- **Set `enqueue_shards` explicitly per queue.** The default `1` is the conservative FIFO-preserving choice but serialises producers on a single head row; `4`/`8` is operator tuning for contended hot queues. The trade-off is semantic, not a free perf win — see [`docs/configuration.md`](docs/configuration.md#sharding-the-enqueue-head-per-queue).
- **MVCC discipline.** If your application runs long-lived `REPEATABLE READ` / `SERIALIZABLE` transactions on the same database awa lives in, give awa its own database. A long-pinned `xmin` blocks dead-tuple cleanup and degrades queue-storage throughput over long horizons — bound non-awa sessions with `idle_in_transaction_session_timeout` and alert on `pg_stat_activity.xact_start` age. Tracked in [#169](https://github.com/hardbyte/awa/issues/169).
- **Cloud SQL with IAM auth:** the runtime SA needs `cloudsqlsuperuser` for `prepare_schema` to install. Run `GRANT cloudsqlsuperuser TO "your-sa@project.iam"` once per instance. AlloyDB grants this automatically. Full guide: [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md).

### Benchmark evidence (#169)

Long-horizon pinned-MVCC behaviour, measured with the [companion harness](https://github.com/hardbyte/postgresql-job-queue-benchmarking) (1 replica, 64 workers, 800 jobs/s offered, 60-minute idle-in-transaction reader). **These are point-in-time results from that harness and hardware, not universal claims.**

- **Awa on the 0.6 storage engine:** the clean phase holds the offered rate at shallow median depth. Under the 60-minute pinned reader, completion sags but queue depth stays bounded (no dead-tuple cliff) and drains fully once the pin releases — the append-only segments hold zero dead tuples, with residual pressure confined to terminal-counter and ring-state metadata. This bounded-depth-through-the-pin / full-drain-in-recovery shape is the [#169](https://github.com/hardbyte/awa/issues/169) gate the 0.6.0 tag was held on, and it passed.
- **Reference comparison:** a feature-minimal reference queue (no per-row retries, heartbeats, cancellation, or deadlines) holds flat through the same phase at a fraction of the WAL/job. Every per-row-state-machine Postgres queue — River, Oban, pg-boss, Graphile, pgmq — shares awa's degradation shape under a pinned horizon. Awa is not immune to long readers; the MVCC-discipline rules above are the supported mitigation.
- A separate Cloud SQL PG18 16-vCPU staging sweep (v021 shard-aware lane indexes) moved end-to-end drain on a 3.5M-row backlog from ~1,300 jobs/s to ~8,800–10,600 jobs/s. That is a producer/claim-throughput result on contended backlog, independent of the MVCC question above.

## [0.6.0-rc.4] — 2026-06-30

### Fixed

- **`admin::cancel_by_unique_key` / `cancel_by_unique_key_tx` on the queue-storage engine** ([#359](https://github.com/hardbyte/awa/pull/359)). The candidate lookup's running-job branch read `unique_key` directly from `{schema}.leases`, but the `leases` table does not carry that column, so any cancel-by-unique-key issued while queue storage was the active engine failed at plan time with `column "unique_key" does not exist`. The running-job branch now recovers the key by joining each lease back to its originating `ready_entries` row — the same lane-identity join the candidate and jobs-compat views already use. Canonical-engine behaviour was unaffected.

## [0.6.0-rc.3] — 2026-06-25

### Changed

- **`admin::cancel_tx` / `admin::cancel_by_unique_key_tx` now take `&mut PgConnection`** ([#358](https://github.com/hardbyte/awa/pull/358)) instead of the `&mut sqlx::Transaction` introduced in rc.2. A `&mut Transaction` deref-coerces to `&mut PgConnection`, so existing callers are unaffected, while callers that only hold a connection mid-transaction (e.g. behind a `transact!`-style helper that derefs the transaction) can now use them too. This matches awa's existing multi-statement consumer convention (`insert_many_copy`). Internally the cancel runs in a nested transaction — a `SAVEPOINT` when the connection is already in one — so a cancel error rolls back only the cancel rather than poisoning the caller's transaction.

## [0.6.0-rc.2] — 2026-06-25

### Added

- **Transaction-scoped admin cancel** ([#357](https://github.com/hardbyte/awa/pull/357)). `admin::cancel_tx` and `admin::cancel_by_unique_key_tx` accept a caller-provided `&mut sqlx::Transaction` and run the cancellation inside it, so the cancel commits or rolls back atomically with the caller's other work and the cooperative `awa:cancel` NOTIFY to a running worker fires only on the caller's commit. The pool-based `cancel` / `cancel_by_unique_key` are unchanged. On the queue-storage engine the `_tx` variants skip the best-effort post-commit claim-cursor advance the pool variants perform — the derived queue depth can briefly over-count by one until later committed rows on the lane are claimed; canonical counts are unaffected.

## [0.6.0-rc.1] — 2026-06-24

First release candidate for the 0.6 line. It promotes the beta.2 queue-storage engine after the [#169](https://github.com/hardbyte/awa/issues/169) pinned-MVCC dead-tuple gate passed on `main`: a 60-minute idle-in-transaction soak now holds throughput with no dead-tuple cliff, and the last dominant pinned-horizon hot row (`queue_claim_heads`) is removed in this delta. Migrations v032–v039 apply via `awa migrate` (or the SQL-only path in [`docs/migrations.md`](docs/migrations.md) for external migration tooling).

### Added

- **Compact receipt claim and completion batches** ([#352](https://github.com/hardbyte/awa/pull/352), migrations v036-v038, [ADR-026](docs/adr/026-narrow-terminal-history.md)). Receipt-backed successful completions can now write compact terminal batches instead of one `done_entries` row per job, plus compact claim-local closure batches so stale receipt scans stay bounded. Queue storage also records compact `ready_segments` lane ranges so claim can route through committed ready segment metadata before validating `ready_entries`, compact `ready_claim_attempt_batches` range evidence so stale claim cursors do not need to scan claim-ring partitions while old slots are being truncated, and compact `lease_claim_batches` receipt evidence for zero-deadline claims. Deadline-backed receipt claims remain row-local in `lease_claims` for indexed deadline rescue. `{schema}.terminal_jobs` remains the public terminal read surface; direct SQL against `done_entries` should not assume all completed jobs are physically stored there.
- **Per-job routing in Python COPY batch APIs** ([#348](https://github.com/hardbyte/awa/pull/348), [ADR-031](docs/adr/031-partitioned-queues.md)). `insert_many_copy` / `enqueue_many_copy` (async and sync) accept `opts=[...]` entries that override `queue` and `ordering_key` per job, so one COPY call can carry a mixed-partition batch. Batch-level kwargs remain defaults; an explicit `"ordering_key": None` clears the key for that job.
- **Failed terminal retention floor** ([#337](https://github.com/hardbyte/awa/issues/337) via [#350](https://github.com/hardbyte/awa/pull/350), migration v032, [ADR-032](docs/adr/032-failed-terminal-retention.md)). Non-DLQ `failed` terminal rows now stay retryable for at least `failed_retention` (default `72h`) in queue storage. Queue-ring prune carries in-floor `failed` rows forward into the live segment as wide self-contained terminal rows before truncating the old slot, instead of dropping them on the next rotation. Failed rows aged past the floor are folded into the new `queue_terminal_rollups.pruned_failed_count` column and surfaced as `QueueCounts.pruned_failed` — a cumulative, monotonic count of failed rows no longer retryable, giving operators standing visibility of loss that was previously silent. `retry_failed_by_kind` / `retry_failed_by_queue` now return a `RetryFailedOutcome { retried, matched, pruned_failed_count }` so raced or pruned ids are reported rather than silently dropped, and the CLI surfaces the matched/retried split and a pruned-past-retention warning. `cancelled` rows are not held by the floor.

### Changed

- **Breaking (beta series): `QueueFanout` is replaced by `PartitionedQueue`** ([#348](https://github.com/hardbyte/awa/pull/348), [ADR-031](docs/adr/031-partitioned-queues.md)); no compatibility alias. Besides the rename (`width` → `partitions`, `queue_fanout` → `partitioned_queue`, Python `*_per_queue` config kwargs → `*_per_partition`), routing behavior differs from beta.2: partition 0 is now the logical queue name itself (`email`, `email__p1`, ...) so direct enqueues to the logical name stay consumable, and key→partition selection uses a domain-separated hash so it composes with ADR-025 enqueue-shard routing (the same key may land on a different partition than under beta.2 `QueueFanout`). Drain fanout queues before upgrading producers/workers, or pin explicit names via `from_physical_queues`.
- **Breaking (beta series): `retry_failed_by_kind` and `retry_failed_by_queue` return `RetryFailedOutcome`** ([#337](https://github.com/hardbyte/awa/issues/337)) instead of a bare `Vec<JobRow>`; `RetryFailedOutcome` carries `retried`, `matched`, and `pruned_failed_count`. Python `retry_failed` / `retry_failed_sync` return the outcome rather than a plain list of jobs.
- **Breaking (beta series): `PruneOutcome::Pruned` gains a `carried_failed_rows` field** ([#337](https://github.com/hardbyte/awa/issues/337)) — `Pruned { slot, carried_failed_rows }`; exhaustive matches on `PruneOutcome` need the new binding.
- **Breaking (beta series): `QueueStorage::prune_oldest` takes a `failed_retention: Duration`** ([#337](https://github.com/hardbyte/awa/issues/337)). `Duration::ZERO` disables the floor; `MaintenanceService` passes its configured `failed_retention`. `QueueCounts` gains a `pruned_failed: i64` field.
- **Maintenance prune lock avoidance + bounded receipt-plane scans** ([#349](https://github.com/hardbyte/awa/pull/349), migrations v033–v035). Queue-ring prune no longer queues behind a held lock — it skips a contended slot and retries on the next maintenance cycle, and genuine (non-lock) prune errors now propagate instead of being swallowed. Receipt rescue, receipt terminal-delete closures, and receipt deadline-rescue sweeps are bounded by per-slot cursors so they cost work proportional to live receipts rather than scanning whole rings; queue-storage metrics and maintenance emptiness checks stay off the receipt rings; and the default lease-ring rotation cadence is lowered to cut idle maintenance churn.

### Fixed

- **Stop writing the `queue_claim_heads` ready-segment routing cache** ([#355](https://github.com/hardbyte/awa/pull/355); completes the [#169](https://github.com/hardbyte/awa/issues/169) / [#197](https://github.com/hardbyte/awa/issues/197) HOT-update audit, migration v039). The per-claim `UPDATE` of the `ready_segment_*` routing cache on the singleton `queue_claim_heads` row was the dominant dead-tuple accumulator under a pinned MVCC horizon — a 60-minute idle-in-transaction soak left ~130k dead row versions on a single live row (~80% of all dead tuples), unreclaimable until the horizon released. `claim_ready_runtime` now resolves the target ready slot directly from the `ready_segments` control plane, ordered by `next_lane_seq` (ready segments are non-overlapping, so the smallest `next_lane_seq > claim_seq` is the next covering segment; the subsequent `ready_entries` lookup validates the row, so reservation gaps fall through safely) so the existing `(queue, priority, enqueue_shard, next_lane_seq, …)` index short-circuits at `LIMIT 1` instead of materialising and sorting the tail. The now-unused `ready_segment_slot` / `ready_segment_generation` / `ready_segment_next_lane_seq` columns are **retained** so a rolling-upgrade worker on the previous `queue_storage_schema_ready` check still starts (the additive-only migration policy defers dropping them to a major version). In the pinned-MVCC gate this removes the table's dead-tuple stream entirely and lowers claim p99 under the pin versus the previous cache. Continues the HOT-update audit from the `queue_lanes.available_count` removal (v016, [#251](https://github.com/hardbyte/awa/pull/251)) and the `leases` / `lease_claims` fillfactor pass ([#315](https://github.com/hardbyte/awa/pull/315)).
- **Compact claims close into the compact closure ledger on every path** ([#352](https://github.com/hardbyte/awa/pull/352)). Receipt rescue, terminal close (completion / fail / retryable / cancel), and admin cancel of a compact (zero-deadline) claim now write a `lease_claim_closure_batches` row instead of an explicit `lease_claim_closures` row. A compact claim has no `lease_claims` row, so an explicit closure was invisible to the queue-ring prune count proof and wedged the slot on `SkippedActive { QueueUnclosedClaimRefs }` until the claim ring pruned. Deadline-backed row-local claims continue to write explicit closures.

## [0.6.0-beta.2] — 2026-06-10

Second beta of the 0.6 line. The headline is storage-engine work under pinned MVCC horizons ([#169](https://github.com/hardbyte/awa/issues/169)): sequence-backed cursors, append-only ready segments, and a terminal-count delta ledger replace the remaining hot-row update paths that beta.1 still carried. This is also the release the #169 stable gate will be validated against — see "Benchmark evidence" below.

Migrations v022–v031 apply via `awa migrate` (or the SQL-only path in [`docs/migrations.md`](docs/migrations.md) for external migration tooling).

### Storage engine under pinned MVCC horizons (#169)

- **Sequence-backed lane cursors** ([#321](https://github.com/hardbyte/awa/pull/321), migration v027). Enqueue/claim lane cursors move from hot MVCC-updated rows to PostgreSQL sequences; the head tables remain as lane registries and lock targets. Claim cursors advance post-commit so non-transactional sequence state cannot skip work on rollback. `queue_terminal_live_counts` is striped by `job_id % 256` so completion-heavy lanes don't hammer one counter row.
- **Append-only ready segments + tombstone ledger** ([#323](https://github.com/hardbyte/awa/pull/323), migration v028). Completion, DLQ, retry, and discard no longer `DELETE FROM ready_entries`; rare ready invalidations append to `ready_tombstones`, claim treats tombstones as spent cursor evidence, and maintenance truncates ready/done/tombstone partitions together. Under a pinned reader, ready/done/tombstone partitions now hold zero dead tuples.
- **Terminal-count delta ledger + async rollup** ([#329](https://github.com/hardbyte/awa/pull/329), migration v030, [ADR-026](docs/adr/026-narrow-terminal-history.md)). Terminal mutations append signed rows to `queue_terminal_count_deltas` in the same transaction instead of upserting a live counter; the maintenance leader folds sealed-slot deltas into `queue_terminal_live_counts`. Exact reads stay honest while rollup lags: folded counts plus pending deltas.
- **Rollup deferral under pinned horizons** ([#333](https://github.com/hardbyte/awa/pull/333)). Maintenance stands down from mutating folded counters or truncating delta segments while another backend pins the MVCC horizon, then folds after the horizon clears. Prevents the counter table itself from bloating while vacuum is blocked.
- **Exact terminal counts without `done_entries` scans** ([#290](https://github.com/hardbyte/awa/issues/290) via [#304](https://github.com/hardbyte/awa/pull/304)/[#306](https://github.com/hardbyte/awa/pull/306)). `queue_counts_exact` reads the folded counters (bounded by slots × priorities × shards) when the terminal-counter trust marker is set, falling back to scanning `done_entries` mid-rolling-upgrade. Operators flip the marker with `awa storage rebuild-terminal-counters`; fresh installs auto-mark.
- **`queue_counts_fast`** ([#289](https://github.com/hardbyte/awa/pull/289)) — index-only depth probe for high-cadence pollers (dashboards, depth-target producers). Under-counts terminal rows the live segment hasn't rolled up; use `queue_counts` when exact terminal counts matter.
- **Metrics off the exact-scan path** ([#330](https://github.com/hardbyte/awa/pull/330), migration v031). Worker health/metrics availability reads lane-head cursor signals instead of exact ready-row scans; lag probing is bounded to the next claimable row per lane; a partial index serves the failed-count probe.
- **Maintenance and receipt-plane mitigations**: `fillfactor=50` + autovacuum knobs on `leases` / `lease_claims` partitions ([#315](https://github.com/hardbyte/awa/pull/315), migration v024); exponential prune backoff + branch-tracker hysteresis ([#316](https://github.com/hardbyte/awa/pull/316)); receipts mode drops the `leases.heartbeat_at` write and `state_hb` index — heartbeats live in `attempt_state` ([#317](https://github.com/hardbyte/awa/pull/317), migration v025); duration-margin hysteresis, per-branch cooldown, and a 250 ms lease-rotate default.

### Added

- **Durable batch operations** ([#328](https://github.com/hardbyte/awa/pull/328), migration v029, [ADR-030](docs/adr/030-batch-operations.md)). Crash-safe, maintenance-driven bulk `set_priority` and `move_queue` over job selections, with preview/submit/list/get/cancel/purge through the HTTP API, CLI, and a new admin-UI Batch Ops tab. Python parity — raw sync/async batch methods plus typed `Client` / `AsyncClient` helpers — via [#332](https://github.com/hardbyte/awa/pull/332). This is the first-class reprioritization surface ([#307](https://github.com/hardbyte/awa/issues/307)).
- **Transactional follow-up jobs** ([ADR-029](docs/adr/029-transactional-followup-jobs.md), [#285](https://github.com/hardbyte/awa/pull/285)/[#288](https://github.com/hardbyte/awa/pull/288)). `ClientBuilder::on_completed_enqueue` (and friends) registers a follow-up job inserted in the same transaction as the lifecycle transition for worker-driven outcomes; callback resolution through the worker `Client` commits transition + follow-up atomically too. Maintenance rescue stays best-effort by design.
- **Callback-only router and user-owned callback layers** ([#291](https://github.com/hardbyte/awa/pull/291)/[#293](https://github.com/hardbyte/awa/pull/293), closes [#281](https://github.com/hardbyte/awa/issues/281)). The callback ingress contract is shared and the URL prefix configurable; embed Awa's router or implement the contract in your own API layer — axum and FastAPI examples documented.
- **`WaitingForCallback` lifecycle event + client-side callback resolution** ([#276](https://github.com/hardbyte/awa/pull/276)). Jobs parking on `WaitForCallback` are now visible to lifecycle hooks, and `Client` gains `resolve_callback` / `complete_external` / `fail_external` / `retry_external` that dispatch the matching terminal event in-process.
- **Queue fanout helper** ([#327](https://github.com/hardbyte/awa/pull/327)). Rust `QueueFanout` + `ClientBuilder::queue_fanout` and Python `awa.QueueFanout` for deterministic routing from one hot logical queue to multiple physical queues; duplicate physical declarations are rejected. Replaced by `PartitionedQueue` in [#348](https://github.com/hardbyte/awa/pull/348).
- **Cron schedule pause / resume** ([#320](https://github.com/hardbyte/awa/pull/320), migration v026). `POST /api/cron/{name}/pause` / `/resume`; the evaluator skips paused rows and the `atomic_enqueue` CTE re-checks `paused_at IS NULL` so a pause asserted mid-evaluation still takes effect. `last_enqueued_at` is untouched while paused, so `missed_fire_policy` decides catch-up on resume. Manual `trigger_cron_job` bypasses pause. The `/cron` UI gains Pause/Resume controls.
- **`awa storage finalize --wait` / `--check`** ([#298](https://github.com/hardbyte/awa/pull/298)). `--wait` polls and finalizes once readiness gates stay clear for two consecutive observations; `--check` is a dry-run that exits 2 when blocked.
- **Storage-transition readiness UI** ([#299](https://github.com/hardbyte/awa/pull/299)). Time-in-state, epoch-anchored backlog history, a prominent `prepared_schema_ready=false` warning with the remediation command, and a rollback-boundaries panel.
- **`awa-seaorm` crate** ([#275](https://github.com/hardbyte/awa/pull/275)). Focused SeaORM adapter for transactional enqueue alongside application writes.
- **Maintenance branch observability** ([#302](https://github.com/hardbyte/awa/pull/302)). `awa.maintenance.branch.duration` histograms, delayed-tick warnings on on-time→delayed transitions, and `awa_maintenance_branch_overrun_total{branch=...}`.

### Changed

- **Breaking (beta series): `QueueCounts.completed` is renamed `QueueCounts.terminal`** ([#306](https://github.com/hardbyte/awa/pull/306)) — it counts completed, cancelled, and discarded terminal rows, and the old name misread as completed-only.
- **Breaking (beta series): `JobEvent` / `UntypedJobEvent` gain a `WaitingForCallback` variant** ([#276](https://github.com/hardbyte/awa/pull/276)) — exhaustive matches need a new arm.
- **`awa migrate` owns the default `awa.*` queue-storage substrate** ([#308](https://github.com/hardbyte/awa/issues/308) via [#310](https://github.com/hardbyte/awa/pull/310)/[#312](https://github.com/hardbyte/awa/pull/312)/[#313](https://github.com/hardbyte/awa/pull/313)/[#314](https://github.com/hardbyte/awa/pull/314), migration v023). Substrate DDL is migration-owned rather than installed opportunistically by `prepare_schema()`; `awa --reset --schema awa` is rejected; the SQL-only install/upgrade path for external migration tooling is documented and tested.
- **Registered handler futures no longer need `Sync`** ([#331](https://github.com/hardbyte/awa/pull/331)). Returned futures only require `Send + 'static`, so handlers awaiting `sqlx` / `reqwest` work compile without workarounds.
- **Chaos tests observe DB state, not stdout** ([#301](https://github.com/hardbyte/awa/pull/301), closes [#167](https://github.com/hardbyte/awa/issues/167)).

### Fixed

- **Concurrent queue-storage sequence reservations** ([#326](https://github.com/hardbyte/awa/pull/326)). `reserve_enqueue_seq` assumed `N × nextval()` returned a contiguous block; concurrent COPY producers could reserve overlapping ranges and hit `ready_entries_*_pkey`. Blocks now allocate under a per-sequence advisory lock (`nextval` + `setval`), and compatibility sync uses the same lock so it cannot rewind a hot enqueue sequence.
- **Flaky admin metadata cache test under parallel runners** ([#274](https://github.com/hardbyte/awa/pull/274)).

### Documentation

- **MVCC operator guidance** in [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md): keep long-running analytical readers on a separate database, bound non-Awa sessions with `idle_in_transaction_session_timeout`, alert on `pg_stat_activity.xact_start` age (query included), and give autovacuum instance-level capacity on managed engines.
- **Security guide: deployable roles and callback ingress boundaries** ([#294](https://github.com/hardbyte/awa/pull/294)).
- **Deadline-bounded polling example** ([#273](https://github.com/hardbyte/awa/pull/273)) — `poll_until_deadline.rs` plus an integration test pinning the retry-until-deadline pattern.
- App-owned database usage in examples ([#286](https://github.com/hardbyte/awa/pull/286)); callback ingress + maintenance ADR proposals ([#277](https://github.com/hardbyte/awa/pull/277)); the #169 storage spike archived under `docs/` ([#300](https://github.com/hardbyte/awa/pull/300)).

### Benchmark evidence and operating notes (#169)

Long-horizon pinned-MVCC behaviour, measured with the [companion harness](https://github.com/hardbyte/postgresql-job-queue-benchmarking) (1 replica, 64 workers, 800 jobs/s offered, 60-minute idle-in-transaction reader; numbers are point-in-time results from that harness and hardware, not universal claims):

- **Awa on this release's storage engine (post-#330 main, 2026-06-09 run):** clean phase holds 800/s at median depth 21. Under the 60-minute pinned reader, median completion is ~741/s, sagging to ~606/s by phase end with queue depth peaking at ~312k; peak sampled dead tuples ~91k (down from ~2.54M two iterations earlier — the append-only segments hold zero dead tuples, with the residual pressure in the terminal-counter and ring-state metadata). Recovery after the pin releases drained at ~963/s median but still held ~198k backlog after 10 minutes.
- **pgque reference on the same harness (2026-05-17/18):** holds ~800/s with median depth 0 through the same phase shape, at ~450 B WAL/job versus Awa's ~2.2–2.9 KiB/job. pgque buys that immunity by trading away per-row feature surface (per-row retries, heartbeats, cancellation, deadlines); every per-row-state-machine Postgres queue — River, Oban, pg-boss, Graphile, pgmq — shares Awa's degradation shape under a pinned horizon. Awa is not immune to long readers, and this release does not claim otherwise.
- **Operating guidance:** the MVCC-discipline rules above are the supported mitigation — separate database for long analytical readers, session timeouts, `xact_start` alerting. `enqueue_shards=1` remains the conservative default; `4`/`8` is operator tuning for contended hot queues, independent of the MVCC question.
- The `0.6.0` **stable** tag is gated on this release passing the long-horizon shape (bounded depth through the pinned hour, full drain in recovery) — tracked in [#169](https://github.com/hardbyte/awa/issues/169).

## [0.6.0-beta.1] — 2026-05-19

First beta of the 0.6 line. Frames the user-facing diff between `0.5.x` and `0.6.0-beta.1` as one release; the `0.6.0-alpha.N` entries below remain as the granular development log.

### What's new since 0.5.x

**Storage and durability**

- **Queue-storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries` paired with a partitioned receipt ring keep dead-tuple footprint bounded under sustained load. Replaces the row-mutating canonical engine for fresh installs.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)). `lease_claims` and `lease_claim_closures` partitioned by claim slot, rotated by the maintenance leader.
- **Per-claim deadlines** in receipts mode. Set `QueueConfig.deadline_duration` to bound a single attempt; rescue force-closes expired claims with `'deadline_expired'`.
- **Staged 0.5 → 0.6 transition tooling**: `awa storage prepare`, `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize`, `abort`. Fresh installs auto-finalize on first `awa migrate`. Full procedure in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

**Operator surfaces**

- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue `dlq_enabled` policy plus a full admin surface: `awa dlq depth | list | retry | retry-bulk | move | purge` (CLI), matching admin UI tab, Rust/Python client APIs.
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue and job-kind metadata (`display_name`, `description`, `owner`, `tags`, `docs_url`) drives admin UI labels and drift detection.
- **Cron missed-fire policy**. Periodic schedules persist an explicit `missed_fire_policy`: `coalesce` (default) or `catch_up`. Exposed in Rust/Python APIs and CLI schedule output.
- **`awa-pg[ui]` Python extra** + `python -m awa serve`. The bare `awa-pg` install stays small (workers/producers don't pay for the ~10 MB axum + dashboard bundle); `pip install 'awa-pg[ui]'` pulls in the `awa-cli` wheel and lets `python -m awa serve` launch the embedded React dashboard.
- **Managed-Postgres deployment guide** ([`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md)). Per-vCPU sizing data, Cloud SQL IAM grants, AlloyDB notes, auth-proxy sidecar pattern.

**Producer APIs and tuning**

- **Direct queue-storage COPY producer path** as the documented high-volume entry point. Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python: `Client.enqueue_many_copy(jobs)`. The compat-friendly `insert_many_copy_from_pool` / `client.insert_many_copy` routes through `awa.insert_job_compat()` once per row — fine for ad-hoc inserts but ~100–150 ms per row through a real DB proxy, not what you want for bursts.
- **`InsertOpts::ordering_key: Option<Vec<u8>>`** pins related jobs to the same shard at `enqueue_shards > 1`. Kafka-partition-key analogue: jobs sharing a key inherit that shard's strict FIFO. `None` rotates batches across shards.
- **`awa.queue_meta.enqueue_shards` per-queue semantic switch** ([ADR-025](docs/adr/025-sharded-enqueue-heads.md)). Default `1` preserves strict `(queue, priority)` FIFO. Setting `≥ 2` switches that queue to partitioned FIFO (strict within `(queue, priority, enqueue_shard)`, no order across shards) — comparable to choosing SQS Standard over FIFO.
- **Queue-storage completion-batcher defaults tuned** to `(batch=512, flush=1ms)`, with adaptive queue-storage completion shards: ordinary runtimes use `1`; runtimes configured for at least `512` workers use `4`. Tunable via `AWA_COMPLETION_BATCH_SIZE`, `AWA_COMPLETION_FLUSH_MS`, and `AWA_COMPLETION_SHARDS`.

**Telemetry**

- **`awa-metrics` crate**. `AwaMetrics` and its `record_*` methods live in their own crate so non-runtime callers (`awa-ui`, `awa-cli`) can emit the same OTel counters as the worker. `awa-worker::AwaMetrics` re-exports for source compatibility.
- **Sub-second buckets on `awa.job.wait_duration`**. Previous boundaries didn't resolve pickup latency below 5 s.
- **`awa.enqueue.batch_size` and `awa.enqueue.duration`** histograms on the direct COPY enqueue path.
- **`awa.enqueue.shard` attribute on `awa.job.claimed`** so dashboards can see per-shard claim throughput.

### Upgrading from 0.5.x

- Fresh installs: nothing to do. `awa migrate` auto-finalizes to the queue-storage engine on first run.
- Existing 0.5.x clusters: walk the staged transition documented in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). Roll out 0.6 workers first (they understand both engines and the transition states), then walk `prepare → enter-mixed-transition → finalize`. **Rollback boundary:** one-way after `enter-mixed-transition` followed by any queue-storage write. Plan to restore from backup if rollback is needed past that point.
- Update the dependency: `awa = "0.6.0-beta.1"` (Rust) / `awa-pg==0.6.0-beta.1` (Python).

### Operating notes

- **Set `enqueue_shards` explicitly per queue.** The default `1` is the conservative FIFO-preserving choice but serialises producers on a single head row. We recommend `4` for contended hot queues. The trade-off is semantic, not a free perf win — see [`docs/configuration.md`](docs/configuration.md#sharding-the-enqueue-head-per-queue).
- **MVCC discipline.** If your application runs long-lived `REPEATABLE READ` / `SERIALIZABLE` transactions on the same database awa lives in, give awa its own database. Long-pinned `xmin` blocks dead-tuple cleanup and degrades queue-storage throughput over long horizons. Tracked in [#169](https://github.com/hardbyte/awa/issues/169).
- **Cloud SQL with IAM auth:** the runtime SA needs `cloudsqlsuperuser` for `prepare_schema` to install. Run `GRANT cloudsqlsuperuser TO "your-sa@project.iam"` once per instance. AlloyDB grants this automatically. Full guide: [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md).

### Changes since 0.6.0-alpha.9

For users already running an alpha:

- v021 shard-aware lane indexes on `ready_entries` / `done_entries` / `leases` partitions (#263). The narrow `(queue, priority, lane_seq)` indexes from before v017 didn't include `enqueue_shard`; on heavy backlog the planner discarded `(shards-1)/shards` of rows per partition per claim. Drain throughput at depth restored to equilibrium rate.
- `prepare_schema` startup race fixed (#264, #266). Concurrent worker startup on a fresh DB previously hung on PG18; install now runs in a single transaction with `pg_advisory_xact_lock`.
- `queue_storage_schema_ready` tightened to require every object the runtime depends on, so a partial install can't report ready (#266).
- Managed-Postgres deploy guide added (#265).
- `awa.enqueue.batch_size` / `awa.enqueue.duration` histograms added (#265).
- Direct queue-storage COPY producer path is the documented high-volume entry point (#263); compat path semantics unchanged.

### Performance

- Drop the `queue_lanes.available_count` cache (migration `v016`). The dispatcher derives availability from `queue_enqueue_heads.next_seq - queue_claim_heads.claim_seq` and the admin API (`queue_counts`) scans `ready_entries` for an exact count. Removes the queue-storage schema's largest dead-tuple source under pinned-xmin workloads (long-running reader transactions) without changing public API behaviour. See ADR-019 § `lane_state` and segment cursor tables.
- Cache `(queue, priority, enqueue_shard)` lane presence in-process on the queue-storage path so subsequent enqueue batches skip the three `INSERT ... ON CONFLICT DO NOTHING` round-trips on `queue_lanes` / `queue_enqueue_heads` / `queue_claim_heads`. The cache invalidates and re-runs `ensure_lane` if a subsequent `UPDATE queue_enqueue_heads` finds no row (the observable signal of an earlier ensure_lane that ran inside a rolled-back transaction), so correctness is preserved.
- Shard the queue-storage active plane by `enqueue_shard` (migration `v017`). `awa.queue_meta.enqueue_shards` (default 1, range 1..=64) is a per-queue semantic mode switch — comparable to choosing SQS Standard over SQS FIFO, raising Kafka partition count, or using Pub/Sub ordering keys. The ordering contract at `enqueue_shards = 1` is unchanged (strict FIFO per `(queue, priority)`); at `enqueue_shards > 1` it becomes **partitioned FIFO** — strict FIFO within `(queue, priority, enqueue_shard)`, no order promised across shards. The shard column runs end-to-end: `queue_enqueue_heads`, `queue_claim_heads`, `ready_entries`, `leases`, and `done_entries` carry it in their primary keys; `lease_claims` carries it as a regular column. Raising the value per noisy queue delivers near-linear throughput scaling on contended enqueue workloads: a 16-producer same-queue local sweep measured 1.0× → 1.60× → 2.75× → 3.69× at S=1/2/4/8. Scope: this addresses producer-side enqueue contention. The high-worker-count rescue-path regression (1 replica × 256 workers, receipts on, `LEASE_DEADLINE_MS` A/B) is a separate effect on the claim / rescue side and remains open for follow-up measurement. See ADR-025 for the full design.
- **Shard-aware lane indexes on `ready_entries`, `done_entries`, and `leases` child partitions** (migration `v021`, [#263](https://github.com/hardbyte/awa/pull/263)). The narrow `(queue, priority, lane_seq)` lane indexes that predated v017 did not include `enqueue_shard`, while `claim_ready_runtime` (since v017) filters on it. Under `enqueue_shards > 1` and deep backlog the planner scanned the lane index forward and post-filtered shard, discarding roughly `(shards-1)/shards` of rows per partition per claim. v021 replaces those with `(queue, priority, enqueue_shard, lane_seq)` indexes and drops the originals; `prepare_schema` is updated to match for fresh installs. On a Cloud SQL PG18 16 vCPU staging instance with a 3.5M row backlog, end-to-end drain went from ~1,300 jobs/s to ~8,800–10,600 jobs/s (single-claim probe: 11.4 ms → 0.81 ms).

### Added

- `InsertOpts::ordering_key: Option<Vec<u8>>` pins related jobs to the same shard at `enqueue_shards > 1`. The key bytes are mapped by Awa's portable shard hash and reduced modulo the queue's shard count, so Rust, SQL, and Python enqueue paths agree on routing. Use it as a Kafka-partition-key analogue: jobs for the same customer / order / account share a shard and inherit that shard's strict FIFO. `None` falls back to one rotor pick per `(queue, priority)` sub-batch. Ignored at `enqueue_shards = 1`.
- `awa.job.claimed` counter now carries an `awa.enqueue.shard` attribute on the queue-storage claim path so dashboards can see per-shard claim throughput. Canonical claims continue to emit the un-decorated series; the two attribute sets do not overlap, so Prometheus `sum by (awa_enqueue_shard)(rate(awa_job_claimed[1m]))` is the per-shard fairness panel.
- Tune the queue-storage completion-batcher defaults to `(batch=512, flush=1ms)`. The larger batch cap keeps the durable completion path amortised under load, while the 1 ms flush keeps low-latency finalization for short jobs. Tunable via `AWA_COMPLETION_BATCH_SIZE` and `AWA_COMPLETION_FLUSH_MS`.
- **Direct queue-storage COPY producer path is the documented high-volume entry point** ([#263](https://github.com/hardbyte/awa/pull/263)). Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`. Python: `Client.enqueue_many_copy(jobs)`. `insert_many_copy_from_pool` / `client.insert_many_copy` remain available as the compatibility COPY surface and route through `awa.insert_job_compat()`. See [`docs/configuration.md`](docs/configuration.md#producer-path-choice).
- **Explicit sub-second buckets on `awa.job.wait_duration`** ([#263](https://github.com/hardbyte/awa/pull/263)). Histogram now resolves pickup latency below 5 s, which the previous bucket boundaries did not.
- **`awa.enqueue.batch_size` and `awa.enqueue.duration` histograms** ([#265](https://github.com/hardbyte/awa/pull/265)). Per-batch observability on the direct COPY enqueue path. Lets producers tell "batches are tiny" from "batches are slow" when an enqueue rate target is missed — neither was distinguishable from existing metrics. Recorded by Python `Client.enqueue_many_copy` (sync and async). Rust callers using `QueueStorage::enqueue_params_copy` directly can record via `AwaMetrics::from_global().record_enqueue_batch(queue, count, duration)`.
- **Managed-Postgres deployment guide** ([#265](https://github.com/hardbyte/awa/pull/265)). New [`docs/deploying-on-managed-postgres.md`](docs/deploying-on-managed-postgres.md) covering Cloud SQL and AlloyDB specifics: per-vCPU sizing measurements from staging, Postgres-version recommendations, IAM `cloudsqlsuperuser` grant for Cloud SQL IAM users, the auth-proxy native-sidecar pattern, and `enqueue_shards` operator guidance.

### Fixed

- **`prepare_schema` no longer deadlocks on fresh-DB / small-pool / concurrent worker startup** ([#264](https://github.com/hardbyte/awa/issues/264), [#266](https://github.com/hardbyte/awa/pull/266)). Previously the install acquired `pg_advisory_lock(…)` on one dedicated pool connection but issued every DDL through `.execute(pool)`, which acquired _different_ connections per statement. With a one- connection pool that self-deadlocked; with a small pool and concurrent worker startup it exhausted the pool with every worker holding one connection for the lock and competing for the rest for DDL. The PG18-on-fresh-DB hang reported in #264 is the most visible symptom. Install now runs in a single transaction on a single connection with `pg_advisory_xact_lock(…)`; the lock auto-releases on COMMIT/ROLLBACK and the install is atomic.
- **`queue_storage_schema_ready` checks the full required surface** ([#266](https://github.com/hardbyte/awa/pull/266)). Previously it only checked for `queue_ring_state`, `ready_entries`, and `leases`; if `prepare_schema` had failed partway through (a real failure mode under the old non-atomic install) the schema could report ready while `awa.job_id_seq`, `done_entries`, `deferred_jobs`, or the `claim_ready_runtime` function were missing — at which point producers got `relation "awa.job_id_seq" does not exist`. The check now requires every queue-storage substrate object the runtime depends on.
- **`AwaMetrics::from_global()` no longer rebuilt per call on hot paths** ([#265](https://github.com/hardbyte/awa/pull/265)). `from_global()` registers the entire instrument set on every invocation. The Python `Client`, `awa-ui` `AppState`, and `awa-cli` DLQ command now cache a single `AwaMetrics` handle and reuse it instead of rebuilding ~30 instruments per operation.

## [0.6.0-alpha.9] — 2026-05-08

### Fixed

- **Queue-storage priority-aging completions now preserve the lane priority in terminal storage.** Workers still receive the aged effective priority, with `_awa_original_priority` in metadata, but `done_entries` now uses the original queue lane priority for its storage key. This prevents aged low-priority jobs from colliding with high-priority jobs that share the same per-lane sequence.

## [0.6.0-alpha.8] — 2026-05-08

### Added

- **Lifecycle hooks now include `Started` events**. `JobEvent<T>` and `UntypedJobEvent` fire `Started` after a claim commits and just before the worker handler is invoked, so applications can observe job execution beginning as well as final outcomes.

### Changed

- **Breaking alpha API change:** exhaustive matches on `JobEvent<T>` or `UntypedJobEvent` must handle the new `Started` variant. No stable Awa release has been published yet, so this is documented as an alpha-series source break.

- **Reduced queue-storage per-claim deadline overhead.** Fresh queue-storage schemas now use a low-write BRIN index for `lease_claims.deadline_at`, and the claim helper computes claim/deadline timestamps once per batch instead of per returned row. Deadline rescue semantics are unchanged; this targets the alpha.8 benchmark path where non-zero `QueueConfig::deadline_duration` is enabled for short receipt-plane jobs.

## [0.6.0-alpha.7] — 2026-05-07

### Added

- **Cron missed-fire policy** ([#239](https://github.com/hardbyte/awa/issues/239), [#240](https://github.com/hardbyte/awa/pull/240)). Periodic schedules now persist an explicit `missed_fire_policy`: `coalesce` remains the default and enqueues only the latest due fire after delayed evaluation, while `catch_up` enqueues missed fires in timestamp order for idempotent reconciliation jobs. The policy is exposed through the Rust and Python APIs, CLI schedule output, docs, and migration v015.

### Changed

- **Cron evaluation runs in its own leader-scoped maintenance lane** ([#240](https://github.com/hardbyte/awa/pull/240)). Slow cleanup, rotation, or reconciliation work no longer starves the scheduler branch. Coalesced schedules also use a bounded latest-fire lookup so a stale high-frequency schedule does not scan every missed tick.
- **Runtime grants now document required `TRUNCATE` privileges** ([#239](https://github.com/hardbyte/awa/issues/239), [#240](https://github.com/hardbyte/awa/pull/240)). The security guide now includes `TRUNCATE` in runtime table grants and default privileges, with a DB-backed regression covering the previously documented grants.

### Fixed

- **Queue-storage claim transactions commit before rescueable runtime payload conversion** ([#222](https://github.com/hardbyte/awa/pull/222)). Runtime claims now keep hot claim transactions shorter while preserving rollback behavior for legacy zero-deadline claims that cannot be rescued after a conversion error.

## [0.6.0-alpha.6] — 2026-05-07

### Added

- **`awa-metrics` crate** ([#176](https://github.com/hardbyte/awa/issues/176), [#232](https://github.com/hardbyte/awa/pull/232)). The `AwaMetrics` type and its `record_*` methods move from `awa-worker` into a dedicated `awa-metrics` crate so non-runtime callers (`awa-ui`, `awa-cli`) can emit the same OTel counters as the worker without depending on the dispatcher/runtime crate graph. `awa-worker::AwaMetrics` re-exports for source compatibility — no semver break for `awa-cli` or `awa-python`. `awa-metrics::names` exposes public string constants for every metric, and `AwaMetrics::new()` is built from those constants so registered instruments and the public names can't drift.

### Changed

- **Multi-queue NOTIFY collapses to one round-trip per enqueue tx** ([#235](https://github.com/hardbyte/awa/pull/235)). The three queue-storage enqueue paths (`enqueue_runtime_rows`, `enqueue_params_batch`, `enqueue_params_copy`) used to issue one `pg_notify($1, '')` per distinct destination queue inside the enqueue transaction. Now routed through `notify_queues_tx`, which uses `SELECT pg_notify(channel, '') FROM unnest($1::text[])` so any number of queues becomes one round-trip. Single-queue enqueue (the common case) is unchanged.
- **Completion path: lease delete and `attempt_state` cleanup merge into a single CTE-as-DML statement** ([#235](https://github.com/hardbyte/awa/pull/235)). `complete_runtime_batch` previously issued two consecutive DELETEs (leases, then `attempt_state`) on the receipt-disabled path and on the materialized fallback inside the receipt-enabled path. They now share one statement — one fewer round-trip per completion batch, identical atomicity and return shape.

### Fixed

- **`awa.job.dlq_retried` tagged with the source queue** ([#232](https://github.com/hardbyte/awa/pull/232)). Single-job retry from `awa-ui` previously read `job.queue` from the returned `JobRow`, which carries the _destination_ queue if the request body supplied a `queue` override. The metric now looks the source queue up before retry runs (matching `record_dlq_purged`'s pattern).

## [0.6.0-alpha.5] — 2026-05-04

### Added

- **`awa-pg[ui]` optional extra** ([#186](https://github.com/hardbyte/awa/issues/186)). `pip install 'awa-pg[ui]'` pulls in the [`awa-cli`](https://pypi.org/project/awa-cli/) wheel so `python -m awa serve` (and `awa serve` directly) launches the embedded React dashboard. The default `awa-pg` install stays small — workers and producers don't pay for the ~10 MB axum + UI bundle they don't need.
- `python -m awa serve` is now a subcommand. It detects the `awa` binary in `sys.prefix/{bin,Scripts}` (where `awa-cli`'s wheel installs it) and forwards the full argument tail verbatim. If the extra isn't installed, it exits with a `pip install 'awa-pg[ui]'` hint.

### Fixed

- **Restored queue-storage dispatcher throughput under high concurrency** ([#223](https://github.com/hardbyte/awa/issues/223)). Capacity-release wakes still drain ready work immediately, but the dispatcher now uses the configured fixed fallback poll interval instead of geometrically backing off after empty or permit-saturated polls.

### Changed

- Queue-storage throughput benchmarks can run against a non-canonical storage schema and configurable worker count, making local A/B checks safer and easier to reproduce.
- Added TLA+ trace witnesses for receipt-only cancel, callback wait, and DLQ purge paths, plus documentation alignment for the queue-storage design.

## [0.6.0-alpha.4] — 2026-05-03

### Changed

- Added capacity-wake suppression to reduce empty claim churn in quiet queues. This improved some operational churn metrics but regressed high-concurrency queue-storage throughput; alpha.5 keeps the useful wake-drain repair while restoring fixed fallback polling.

## [0.6.0-alpha.3] — 2026-05-02

### Changed

- **Completion-batcher default size lowered from `512` to `128`.** Cross-system matrix runs (1–4 worker processes × 16–128 workers per process) showed `128` delivered the lowest p99 in every cell and `512` bought no throughput while hurting tail latency under multi-process deployments. Override via `AWA_COMPLETION_BATCH_SIZE`. See `docs/benchmarking.md` for tuning notes.
- **Reduced queue-storage claimer heartbeat churn.** Claimer leases now skip refresh writes while still fresh, cutting coordination writes in the dispatch path without changing claim ownership semantics.
- **Updated architecture documentation.** The architecture guide now reflects the queue-storage receipt path, lazy lease materialization, crash recovery, maintenance leadership, and callback orchestration.

### Fixed

- **Receipt completion now serializes with heartbeat materialization.** The queue-storage completion path locks the matching receipt claim before writing its closure, preventing a concurrent heartbeat from recreating `attempt_state` after completion.
- **Hardened mixed Rust/Python chaos smoke coverage.** The mixed-fleet smoke test now waits for worker-observed completions from both runtimes instead of relying on transient terminal-row presence.

## [0.6.0-alpha.2] — 2026-05-02

### Added

- **Vacuum-aware queue storage engine, default-on** ([ADR-019](docs/adr/019-queue-storage-redesign.md)). Append-only `ready_entries`, `deferred_jobs`, `done_entries`, and `dlq_entries` tables, paired with a partitioned receipt ring, keep the dead-tuple footprint bounded under sustained load. Replaces the canonical row-mutating engine for new installs.
- **Receipt-plane ring partitioning** ([ADR-023](docs/adr/023-receipt-plane-ring-partitioning.md)). `lease_claims` and `lease_claim_closures` are partitioned by claim slot and rotated by the maintenance leader.
- **Dead Letter Queue** ([ADR-020](docs/adr/020-dead-letter-queue.md)). Per-queue `dlq_enabled` policy and a full operator surface: `awa dlq depth | list | retry | retry-bulk | move | purge`, plus the matching admin UI tab. See [`docs/dead-letter-queue.md`](docs/dead-letter-queue.md).
- **Descriptor catalog** ([ADR-022](docs/adr/022-descriptor-catalog.md)). Code-declared queue and job-kind metadata (`display_name`, `description`, `owner`, `tags`, `docs_url`) drives admin UI labels and stale/drift detection.
- **Per-claim deadlines** in receipts mode. `QueueConfig.deadline_duration` writes `lease_claims.deadline_at`; the rescue path force-closes expired claims with `'deadline_expired'`.
- **Storage transition tooling**. `awa storage prepare`, `prepare-queue-storage-schema`, `enter-mixed-transition`, `finalize`, and `abort` cover the staged upgrade path. Fresh installs auto-finalize on first migrate.
- **`transition_role` runtime capability**. The `enter_mixed_transition` SQL gate requires a live `queue_storage_target` runtime, so a stale fleet cannot accidentally skip the staged path.
- **Migrations** v012, v013, and v014. All idempotent.

### Changed

- New installs default to the queue-storage engine; canonical row-mutating storage is no longer the implicit backend.
- Receipts mode is on by default for fresh deployments.

### Removed

- `benchmarks/portable/` extracted to its own repo at [hardbyte/postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking).
- The pre-0.6 `EXPERIMENTAL_LEASE_CLAIM_RECEIPTS` env alias.

### Upgrade notes

- Update your dependency to `awa = "0.6"` (Rust) / `awa-cli`, `awa-pg` (Python) at the matching version.
- Existing 0.5.x clusters with canonical data must walk the staged storage transition documented in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md). Fresh installs auto-finalize.
- Rollback after `enter-mixed-transition` followed by queue-storage writes is one-way (database restore only).

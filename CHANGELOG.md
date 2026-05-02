# Changelog

Notable changes between releases. Detailed migration notes for storage
transitions live in [`docs/upgrade-0.5-to-0.6.md`](docs/upgrade-0.5-to-0.6.md).

## Unreleased

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

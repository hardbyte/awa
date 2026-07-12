# Architecture Decision Records

Each file in this directory captures a single architectural decision — its context, the decision itself, the alternatives considered, and the consequences. ADRs are written when a decision has a non-obvious rationale, trades off across concerns, or will be hard to change later.

Template: `Status / Context / Decision / Consequences (positive, negative) / Alternatives Considered / Relationship to other ADRs`. Superseded ADRs stay in-place as historical context.

## Index

| # | Title | Status | Summary | Relationship |
| --: | --- | --- | --- | --- |
| 001 | [Postgres-only](001-postgres-only.md) | Accepted | Single storage backend, no pluggable adapter layer | Current layout per ADR-019 |
| 002 | [BLAKE3 uniqueness](002-blake3-uniqueness.md) | Accepted | Uniqueness keys hashed with BLAKE3, claims in `awa.job_unique_claims` | — |
| 003 | [Heartbeat + deadline hybrid](003-heartbeat-deadline-hybrid.md) | Accepted | Two independent rescue paths cover crash and runaway failure modes | Fields moved to `active_leases` per ADR-019 |
| 004 | [PyO3 async bridge](004-pyo3-async-bridge.md) | Accepted | Python workers are callbacks invoked by the Rust runtime via PyO3 | — |
| 005 | [Priority aging](005-priority-aging.md) | Accepted | Effective priority aging prevents starvation; canonical uses maintenance aging, queue storage uses claim-time aging | Aging target changed under ADR-019 |
| 006 | [AwaTransaction as narrow SQL surface](006-awa-transaction.md) | Accepted | Python transaction bridge exposes only insert + commit/rollback | — |
| 007 | [Periodic cron jobs](007-periodic-cron-jobs.md) | Accepted | Leader-elected scheduler with atomic CTE enqueue | — |
| 008 | [COPY batch ingestion](008-copy-batch-ingestion.md) | Accepted | Session-local staging table + COPY for 10k+-row inserts | Routes through `insert_job_compat` under ADR-019 |
| 009 | [Python sync support](009-python-sync-support.md) | Accepted | Every async method has a `_sync` counterpart for Django/Flask | — |
| 010 | [Per-queue rate limiting](010-rate-limiting.md) | Accepted | Per-worker token bucket composes with both concurrency modes | Storage-plane-agnostic |
| 011 | [Weighted concurrency](011-weighted-concurrency.md) | Accepted | Global worker pool with per-queue min guarantees and weighted overflow | Storage-plane-agnostic |
| 012 | [Hot / deferred job storage](012-hot-deferred-job-storage.md) | **Superseded by 019** | Manual hot/cold split of the `awa.jobs` heap | Superseded by ADR-019 |
| 013 | [Run lease and guarded finalization](013-run-lease-and-guarded-finalization.md) | Accepted | `run_lease` is the per-attempt identity; every finalize matches on it | Composite key on `active_leases` per ADR-019 |
| 014 | [Structured progress and metadata](014-structured-progress.md) | Accepted | JSONB progress buffer with heartbeat piggyback + atomic state-transition flush | Progress storage moved to `attempt_state` per ADR-019 |
| 015 | [Builder-side lifecycle hooks](015-post-commit-lifecycle-hooks.md) | Accepted | Builder-side hooks fire after claim start and guarded finalization commits | Guard lives on `active_leases` per ADR-019 |
| 016 | [Public Rust Postgres enqueue adapter API](016-rust-postgres-enqueue-adapter-api.md) | Accepted | Public Postgres insert-preparation contract plus built-in tokio-postgres adapter | Enables external Rust enqueue adapters |
| 017 | [Python insert-only transaction bridging](017-python-transaction-bridging.md) | Accepted | Python `awa.Transaction` is a thin wrapper over the Rust insert path | — |
| 018 | [HTTP Worker for serverless job dispatch](018-http-worker.md) | Accepted | `Worker` impl that dispatches to Lambda / Cloud Run via HTTP + BLAKE3-signed callbacks | Uses callback surface from ADR-021 |
| 019 | [Queue Storage Engine](019-queue-storage-redesign.md) | Accepted | Append-only ready / terminal entries, narrow `active_leases`, optional `attempt_state`, rotating segments | Supersedes ADR-012 |
| 020 | [Dead Letter Queue](020-dead-letter-queue.md) | Accepted | First-class DLQ storage family with per-queue opt-in, retention, and operator retry/purge | Lives inside ADR-019 |
| 021 | [Sequential callbacks and callback heartbeats](021-enhanced-external-wait.md) | Accepted | `wait_for_callback()` + `resume_external()` for multi-step orchestration; `heartbeat_callback` for long-running externals | Callback state moved to `active_leases` per ADR-019 |
| 022 | [Descriptor catalog](022-descriptor-catalog.md) | Accepted | `queue_descriptors` / `job_kind_descriptors` tables, BLAKE3-hashed, code-declared, off the hot path | Off the queue-storage hot path |
| 023 | [Receipt plane ring partitioning](023-receipt-plane-ring-partitioning.md) | Accepted | Partitioned `lease_claims`, explicit closures, and compact closure batches replace `open_receipt_claims`; receipts default on in 0.6 | Refines ADR-019 receipt plane |
| 024 | Deferred `done_entries` materialisation | Rejected | Investigated as a rotation guard; reverted in `053fec1` once a simpler integration test gave equivalent coverage | Historical |
| 025 | [Sharded enqueue heads](025-sharded-enqueue-heads.md) | Accepted | Per-queue `enqueue_shards` (default 1) spreads `queue_enqueue_heads` row-lock contention across N rows; FIFO becomes per-shard at S>1 | Refines ADR-019 enqueue path |
| 026 | [Narrow terminal history](026-narrow-terminal-history.md) | Accepted | Ready-backed terminal rows store only terminal facts, compact receipt completions use batch terminal history, and exact counts combine retained compact batches with append-only `done_entries` terminal-count deltas plus async sealed-slot rollup | Refines ADR-019 terminal path |
| 027 | [Callback ingress as a deployable surface](027-callback-ingress-surface.md) | Proposed | Separate signed callback ingress from the admin UI/API and expose callback-only embedding/CLI paths | Refines ADR-018 and ADR-021; uses ADR-029 for durable callback-driven side effects |
| 028 | [Maintenance-only runtime role](028-maintenance-only-runtime-role.md) | Proposed | Run promotion, rescue, pruning, and metadata maintenance without claiming or executing user jobs | Complements ADR-027 and ADR-018; uses ADR-029 for durable rescue-driven side effects |
| 029 | [Transactional follow-up jobs](029-transactional-followup-jobs.md) | Accepted | Durable lifecycle side effects are delivered by enqueuing follow-up Awa jobs — atomically with the triggering state UPDATE for worker-driven outcomes and for callback resolution via the worker `Client`, best-effort in a separate transaction for maintenance rescue; hooks remain for observation | Codifies ADR-015's "enqueue another job" guidance; addresses the durable-event punt in ADRs 027/028 |
| 030 | [Durable batch operations for operator bulk mutation](030-batch-operations.md) | Accepted | Filter-driven async bulk mutation with preview, progress, cancellation, retention, and maintenance-led execution; v0.6 starts with `set_priority` and `move_queue` | Refines ADR-019/025 operator mutation paths; complements ADR-028 |
| 031 | [Partitioned queues](031-partitioned-queues.md) | Accepted | First-class logical queue partitioning over ordinary physical queues, with domain-separated key routing and Python per-job COPY opts | Composes ADR-019/023/026 storage guarantees; refines the ADR-025 sharding interaction |
| 032 | [Failed terminal retention floor](032-failed-terminal-retention.md) | Accepted | Queue-storage prune carries in-floor `failed` terminal rows forward into the live segment as wide synthetic rows so they stay retryable for at least `failed_retention`; rows aged past the floor are folded into `queue_terminal_rollups.pruned_failed_count` and surfaced via `QueueCounts.pruned_failed` | Amends ADR-026's one-retention-unit consequence; refines ADR-019 terminal prune path |
| 033 | [Per-key execution control](033-per-key-execution-control.md) | Proposed | Tiered per-key concurrency limits and fairness: worker-local exact / fleet-approximate first, storage-exact only if it needs zero new hot mutable rows (#340) | Composes ADR-005/010/011/025; bound by ADR-026's ledger discipline |
| 034 | [Job dependencies](034-job-dependencies.md) | Proposed | Single-parent A→B chaining: `waiting_on` parking state promoted transactionally by the parent's guarded finalization, with an `on_parent_failure` policy (#14) | Builds on ADR-029; workflow engine remains a non-goal |
| 035 | [Backpressure and flow control](035-backpressure-flow-control.md) | Proposed | Soft depth signals from lane-head cursors by default, opt-in hard rejection, paced-producer helpers (#341) | Makes the ADR-006 transactional-enqueue tension explicit; composes ADR-025/031 |
| 036 | [Public surface stability policy](036-public-surface-stability-policy.md) | Accepted | `docs/stability.md` is the normative surface-by-surface compatibility map, deprecation policy, and binary/schema skew statement (#369); enforced via #402 semver checks and the #367 compat matrix | Governs ADR-016 and the #342 SQL producer contract; constrains all future surface-touching ADRs |
| 037 | [Canonical engine deprecation](037-canonical-engine-deprecation.md) | Accepted | 0.7 `awa migrate` refuses unfinalized clusters (fresh installs exempt); canonical deprecated with a startup warning in 0.7, claim/execution/trigger paths removed in 0.8 (#370) | Completes ADR-019's supersession of the pre-0.6 model; bounds the #360 dual-engine matrix |
| 038 | [Queue runtime overrides](038-queue-runtime-overrides.md) | Accepted | Hot-reloadable per-queue dispatch knobs via nullable `queue_meta` override columns, refreshed by dispatchers on a slow cadence; rate-limit retune and non-zero deadline changes only (Tier 2: #397) | Extends the queue_meta pause/resume control-plane pattern; respects ADR-026; guards ADR-023 claim-mode selection |
| 039 | [End-to-end trace propagation](039-trace-propagation.md) | Accepted | W3C `traceparent` captured at enqueue into the reserved `awa:traceparent` metadata key; first attempts join the producer trace as remote children, retries start fresh root traces with span links; OTel messaging semantic conventions on both sides; default-on, `AWA_TRACE_CAPTURE=off` kill switch (#110) | Builds on ADR-004's single Rust execution path; composes ADR-032 SQL producers via the documented key; adds the reserved `awa:` metadata namespace to ADR-036's policy |
| 040 | [Append-only ring-rotation ledgers](040-append-only-ring-rotation-ledger.md) | Accepted | Ring cursors move from mutable `{ring}_ring_state` singletons to append-only `{ring}_ring_rotations` ledgers (cursor = max-generation row; CAS on the generation PK); staged `columns` -> `ledger` authority supports the 0.6.2/0.7 rollout; queue prune appends `queue_terminal_rollup_deltas` folded by horizon-gated maintenance (#371) | Extends ADR-023's ring plane; applies ADR-026's dead-tuple reclaim discipline to the ring control plane; modelled in AwaStorageLockOrder / AwaDeadTupleContract |

## Validation artifacts

Runtime validation for ADR-019 is recorded in [`bench/019-queue-storage-validation-2026-04-19.md`](bench/019-queue-storage-validation-2026-04-19.md) (exact commands, raw output, measured numbers).

Runtime validation for ADR-023 is recorded in [`bench/023-receipt-ring-validation-2026-04-26.md`](bench/023-receipt-ring-validation-2026-04-26.md) (receipt-ring long-horizon and overnight evidence).

TLA+ correctness models that pin spec-level invariants are under [`../../correctness/`](../../correctness/) — the segmented-storage family (`AwaSegmentedStorage`, `AwaSegmentedStorageRaces`, `AwaStorageLockOrder`, `AwaSegmentedStorageTrace`) maps to ADR-019 and ADR-020; the worker-runtime family (`AwaCore`, `AwaExtended`, `AwaBatcher`, `AwaCbk`, `AwaDispatchClaim`, `AwaViewTrigger`, `AwaCron`) covers rescue, batcher, callback race, dispatcher claim, view-trigger concurrency, and cron double-fire.

## Conventions

- Status is one of: **Accepted**, **Proposed**, **Superseded by ADR-XXX**, **Deprecated**, **Rejected**. Superseded ADRs stay in the directory as historical context.
- Relationships to later ADRs that change implementation but not decision are recorded in a bottom-of-doc `## Relationship to ADR-XXX` section rather than a top-of-doc `## Note`.
- ADRs should be narrative: context, rationale, what-was-considered. Deep implementation detail belongs in [`../architecture.md`](../architecture.md) or a companion design doc, with the ADR holding the decision and its alternatives.
- New ADRs claim the next number in a small placeholder PR before writing to avoid collisions.

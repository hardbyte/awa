# Architecture Decision Records

Each file in this directory captures a single architectural decision — its context, the decision itself, the alternatives considered, and the consequences. ADRs are written when a decision has a non-obvious rationale, trades off across concerns, or will be hard to change later.

Each record preserves its status, context, decision, consequences, and alternatives. Accepted decisions are the default in the index; only exceptional states are labelled. Superseded and rejected records remain available as historical context.

## Index

| # | Decision | Summary | Status |
| --: | --- | --- | --- |
| 001 | [Postgres-only](001-postgres-only.md) | Single storage backend, no pluggable adapter layer. |  |
| 002 | [BLAKE3 uniqueness](002-blake3-uniqueness.md) | Uniqueness keys hashed with BLAKE3, claims in `awa.job_unique_claims`. |  |
| 003 | [Heartbeat + deadline hybrid](003-heartbeat-deadline-hybrid.md) | Two independent rescue paths cover crash and runaway failure modes. |  |
| 004 | [PyO3 async bridge](004-pyo3-async-bridge.md) | Python workers are callbacks invoked by the Rust runtime via PyO3. |  |
| 005 | [Priority aging](005-priority-aging.md) | Effective priority aging prevents starvation; canonical uses maintenance aging, queue storage uses claim-time aging. |  |
| 006 | [AwaTransaction as narrow SQL surface](006-awa-transaction.md) | Python transaction bridge exposes only insert + commit/rollback. |  |
| 007 | [Periodic cron jobs](007-periodic-cron-jobs.md) | Leader-elected scheduler with atomic CTE enqueue. |  |
| 008 | [COPY batch ingestion](008-copy-batch-ingestion.md) | Session-local staging table + COPY for 10k+-row inserts. |  |
| 009 | [Python sync support](009-python-sync-support.md) | Every async method has a `_sync` counterpart for Django/Flask. |  |
| 010 | [Per-queue rate limiting](010-rate-limiting.md) | Per-worker token bucket composes with both concurrency modes. |  |
| 011 | [Weighted concurrency](011-weighted-concurrency.md) | Global worker pool with per-queue min guarantees and weighted overflow. |  |
| 012 | [Hot / deferred job storage](012-hot-deferred-job-storage.md) | Manual hot/cold split of the `awa.jobs` heap. | <span class="awa-status awa-status--superseded">Superseded by 019</span> |
| 013 | [Run lease and guarded finalization](013-run-lease-and-guarded-finalization.md) | `run_lease` is the per-attempt identity; every finalize matches on it. |  |
| 014 | [Structured progress and metadata](014-structured-progress.md) | JSONB progress buffer with heartbeat piggyback + atomic state-transition flush. |  |
| 015 | [Builder-side lifecycle hooks](015-post-commit-lifecycle-hooks.md) | Builder-side hooks fire after claim start and guarded finalization commits. |  |
| 016 | [Public Rust Postgres enqueue adapter API](016-rust-postgres-enqueue-adapter-api.md) | Public Postgres insert-preparation contract plus built-in tokio-postgres adapter. |  |
| 017 | [Python insert-only transaction bridging](017-python-transaction-bridging.md) | Python `awa.Transaction` is a thin wrapper over the Rust insert path. |  |
| 018 | [HTTP Worker for serverless job dispatch](018-http-worker.md) | `Worker` impl that dispatches to Lambda / Cloud Run via HTTP + BLAKE3-signed callbacks. |  |
| 019 | [Queue Storage Engine](019-queue-storage-redesign.md) | Append-only ready / terminal entries, narrow `active_leases`, optional `attempt_state`, rotating segments. |  |
| 020 | [Dead Letter Queue](020-dead-letter-queue.md) | First-class DLQ storage family with per-queue opt-in, retention, and operator retry/purge. |  |
| 021 | [Sequential callbacks and callback heartbeats](021-enhanced-external-wait.md) | `wait_for_callback()` + `resume_external()` for multi-step orchestration; `heartbeat_callback` for long-running externals. |  |
| 022 | [Descriptor catalog](022-descriptor-catalog.md) | `queue_descriptors` / `job_kind_descriptors` tables, BLAKE3-hashed, code-declared, off the hot path. |  |
| 023 | [Receipt plane ring partitioning](023-receipt-plane-ring-partitioning.md) | Partitioned `lease_claims`, explicit closures, and compact closure batches replace `open_receipt_claims`; receipts default on in 0.6. |  |
| 024 | Deferred `done_entries` materialisation | Investigated as a rotation guard; reverted in `053fec1` once a simpler integration test gave equivalent coverage. | <span class="awa-status awa-status--rejected">Rejected</span> |
| 025 | [Sharded enqueue heads](025-sharded-enqueue-heads.md) | Per-queue `enqueue_shards` (default 1) spreads `queue_enqueue_heads` row-lock contention across N rows; FIFO becomes per-shard at S>1. |  |
| 026 | [Narrow terminal history](026-narrow-terminal-history.md) | Ready-backed terminal rows store only terminal facts, compact receipt completions use batch terminal history, and exact counts combine retained compact batches with append-only `done_entries` terminal-count deltas plus async sealed-slot rollup. |  |
| 027 | [Callback ingress as a deployable surface](027-callback-ingress-surface.md) | Separate signed callback ingress from the admin UI/API and expose callback-only embedding/CLI paths. | <span class="awa-status awa-status--proposed">Proposed</span> |
| 028 | [Maintenance-only runtime role](028-maintenance-only-runtime-role.md) | Run promotion, rescue, pruning, and metadata maintenance without claiming or executing user jobs. | <span class="awa-status awa-status--proposed">Proposed</span> |
| 029 | [Transactional follow-up jobs](029-transactional-followup-jobs.md) | Durable lifecycle side effects are delivered by enqueuing follow-up Awa jobs — atomically with the triggering state UPDATE for worker-driven outcomes and for callback resolution via the worker `Client`, best-effort in a separate transaction for maintenance rescue; hooks remain for observation. |  |
| 030 | [Durable batch operations for operator bulk mutation](030-batch-operations.md) | Filter-driven async bulk mutation with preview, progress, cancellation, retention, and maintenance-led execution; v0.6 starts with `set_priority` and `move_queue`. |  |
| 031 | [Partitioned queues](031-partitioned-queues.md) | First-class logical queue partitioning over ordinary physical queues, with domain-separated key routing and Python per-job COPY opts. |  |
| 032 | [Failed terminal retention floor](032-failed-terminal-retention.md) | Queue-storage prune carries in-floor `failed` terminal rows forward into the live segment as wide synthetic rows so they stay retryable for at least `failed_retention`; rows aged past the floor are folded into `queue_terminal_rollups.pruned_failed_count` and surfaced via `QueueCounts.pruned_failed`. |  |
| 033 | [Per-key execution control](033-per-key-execution-control.md) | Fleet-exact keyed grants with shard locality, bounded lane probing, and transactional closure wakeups; fairness remains separate (#340). |  |
| 034 | [Job dependencies](034-job-dependencies.md) | Single-parent A→B chaining: `waiting_on` parking state promoted transactionally by the parent's guarded finalization, with an `on_parent_failure` policy (#14). | <span class="awa-status awa-status--proposed">Proposed</span> |
| 035 | [Backpressure and flow control](035-backpressure-flow-control.md) | Soft depth signals from lane-head cursors by default, opt-in hard rejection, paced-producer helpers (#341). | <span class="awa-status awa-status--proposed">Proposed</span> |
| 036 | [Public surface stability policy](036-public-surface-stability-policy.md) | `docs/stability.md` is the normative surface-by-surface compatibility map, deprecation policy, and binary/schema skew statement (#369); enforced via #402 semver checks and the #367 compat matrix. |  |
| 037 | [Canonical engine deprecation](037-canonical-engine-deprecation.md) | 0.7 `awa migrate` refuses unfinalized clusters (fresh installs exempt); canonical deprecated with a startup warning in 0.7, claim/execution/trigger paths removed in 0.8 (#370). |  |
| 038 | [Queue runtime overrides](038-queue-runtime-overrides.md) | Hot-reloadable per-queue dispatch knobs via nullable `queue_meta` override columns, refreshed by dispatchers on a slow cadence; rate-limit retune and non-zero deadline changes only (Tier 2: #397). |  |
| 039 | [End-to-end trace propagation](039-trace-propagation.md) | W3C `traceparent` captured at enqueue into the reserved `awa:traceparent` metadata key; first attempts join the producer trace as remote children, retries start fresh root traces with span links; OTel messaging semantic conventions on both sides; default-on, `AWA_TRACE_CAPTURE=off` kill switch (#110). |  |
| 040 | [Append-only ring-rotation ledgers](040-append-only-ring-rotation-ledger.md) | Ring cursors move from mutable `{ring}_ring_state` singletons to append-only `{ring}_ring_rotations` ledgers (cursor = max-generation row; CAS on the generation PK); staged `columns` -> `ledger` authority supports the 0.6.2/0.7 rollout; queue prune appends `queue_terminal_rollup_deltas` folded by horizon-gated maintenance (#371). |  |
| 041 | [Rolling upgrade policy](041-rolling-upgrade-policy.md) | Rolling upgrades use expand → capability-gated flip → later contract; version floors guard expand migrations, while database fences and real N-1 rehearsals guard irreversible flips. |  |
| 042 | [Caller-owned finalization transactions](042-caller-owned-finalization-transactions.md) | A distinct handler type commits application rows and exact-lease completion in one transaction through a least-privilege finalization function (#401). |  |
| 043 | [PostgreSQL capability functions and least-privilege runtime roles](043-postgresql-capability-functions.md) | Replace blanket runtime table/function grants with allowlisted, role-specific capability entry points owned by a bounded execution role (#452); blanket definer conversion is rejected. | <span class="awa-status awa-status--proposed">Proposed</span> |
| 044 | [Gate A — storage evolution for 0.7](044-storage-evolution-gate-a.md) | The #295 segment-engine RFC graduates to 0.8: the allocator ideas landed inside the engine as staged migrations and measured better; the remaining WAL headroom has no in-place delivery path (#295, #383). |  |

## Correctness evidence

Executable TLA+ models live under [`correctness/`](https://github.com/hardbyte/awa/tree/main/correctness). The storage models cover segmented storage, storage races, lock ordering, and trace refinement; the runtime models cover claim, rescue, callbacks, batching, cron, and view-trigger concurrency. Benchmark evidence belongs with the benchmark artifacts, not in this decision index.

## Conventions

- Status is one of: **Accepted**, **Proposed**, **Superseded by ADR-XXX**, **Deprecated**, **Rejected**. Superseded ADRs stay in the directory as historical context.
- Relationships to later ADRs that change implementation but not decision are recorded in a bottom-of-doc `## Relationship to ADR-XXX` section rather than a top-of-doc `## Note`.
- ADRs should be narrative: context, rationale, what-was-considered. Deep implementation detail belongs in [`../architecture.md`](../architecture.md) or a companion design doc, with the ADR holding the decision and its alternatives.
- New ADRs claim the next number in a small placeholder PR before writing to avoid collisions.

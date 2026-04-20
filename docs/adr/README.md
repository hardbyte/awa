# Architecture Decision Records

Each file in this directory captures a single architectural decision —
its context, the decision itself, the alternatives considered, and the
consequences. ADRs are written when a decision has a non-obvious
rationale, trades off across concerns, or will be hard to change later.

Template: `Status / Context / Decision / Consequences (positive, negative) /
Alternatives Considered / Relationship to other ADRs`. Superseded ADRs stay
in-place as historical context.

## Index

| # | Title | Status | Summary | Relationship |
|---:|---|---|---|---|
| 001 | [Postgres-only](001-postgres-only.md) | Accepted | Single storage backend, no pluggable adapter layer | Current layout per ADR-019 |
| 002 | [BLAKE3 uniqueness](002-blake3-uniqueness.md) | Accepted | Uniqueness keys hashed with BLAKE3, claims in `awa.job_unique_claims` | — |
| 003 | [Heartbeat + deadline hybrid](003-heartbeat-deadline-hybrid.md) | Accepted | Two independent rescue paths cover crash and runaway failure modes | Fields moved to `active_leases` per ADR-019 |
| 004 | [PyO3 async bridge](004-pyo3-async-bridge.md) | Accepted | Python workers are callbacks invoked by the Rust runtime via PyO3 | — |
| 005 | [Priority aging](005-priority-aging.md) | Accepted | Maintenance-based aging prevents starvation while keeping dispatch a clean index scan | Aging target changed under ADR-019 |
| 006 | [AwaTransaction as narrow SQL surface](006-awa-transaction.md) | Accepted | Python transaction bridge exposes only insert + commit/rollback | — |
| 007 | [Periodic cron jobs](007-periodic-cron-jobs.md) | Accepted | Leader-elected scheduler with atomic CTE enqueue | — |
| 008 | [COPY batch ingestion](008-copy-batch-ingestion.md) | Accepted | Session-local staging table + COPY for 10k+-row inserts | Routes through `insert_job_compat` under ADR-019 |
| 009 | [Python sync support](009-python-sync-support.md) | Accepted | Every async method has a `_sync` counterpart for Django/Flask | — |
| 010 | [Per-queue rate limiting](010-rate-limiting.md) | Accepted | Per-worker token bucket composes with both concurrency modes | Storage-plane-agnostic |
| 011 | [Weighted concurrency](011-weighted-concurrency.md) | Accepted | Global worker pool with per-queue min guarantees and weighted overflow | Storage-plane-agnostic |
| 012 | [Hot / deferred job storage](012-hot-deferred-job-storage.md) | **Superseded by 019** | Manual hot/cold split of the `awa.jobs` heap | Superseded by ADR-019 |
| 013 | [Run lease and guarded finalization](013-run-lease-and-guarded-finalization.md) | Accepted | `run_lease` is the per-attempt identity; every finalize matches on it | Composite key on `active_leases` per ADR-019 |
| 014 | [Structured progress and metadata](014-structured-progress.md) | Accepted | JSONB progress buffer with heartbeat piggyback + atomic state-transition flush | Progress storage moved to `attempt_state` per ADR-019 |
| 015 | [Post-commit lifecycle hooks](015-post-commit-lifecycle-hooks.md) | Accepted | Builder-side hooks fire after guarded finalization commits | Guard lives on `active_leases` per ADR-019 |
| 016 | [Shared insert preparation and tokio-postgres adapter](016-bridge-adapters.md) | Accepted | Factor insert preparation into `PreparedRow`; tokio-postgres enqueue adapter | — |
| 017 | [Python insert-only transaction bridging](017-python-transaction-bridging.md) | Accepted | Python `awa.Transaction` is a thin wrapper over the Rust insert path | — |
| 018 | [HTTP Worker for serverless job dispatch](018-http-worker.md) | Accepted | `Worker` impl that dispatches to Lambda / Cloud Run via HTTP + HMAC-BLAKE3 | Uses callback surface from ADR-021 |
| 019 | [Queue Storage Engine](019-queue-storage-redesign.md) | Accepted | Append-only ready / terminal entries, narrow `active_leases`, optional `attempt_state`, rotating segments | Supersedes ADR-012 |
| 020 | [Dead Letter Queue](020-dead-letter-queue.md) | Accepted | First-class DLQ storage family with per-queue opt-in, retention, and operator retry/purge | Lives inside ADR-019 |
| 021 | [Sequential callbacks and callback heartbeats](021-enhanced-external-wait.md) | Accepted | `wait_for_callback()` + `resume_external()` for multi-step orchestration; `heartbeat_callback` for long-running externals | Callback state moved to `active_leases` per ADR-019 |
| 022 | [Descriptor catalog](022-descriptor-catalog.md) | Accepted | `queue_descriptors` / `job_kind_descriptors` tables, BLAKE3-hashed, code-declared, off the hot path | Off the queue-storage hot path |
| 023 | [Storage lock-ordering protocol](023-storage-lock-ordering.md) | Accepted | Explicit `FOR SHARE` / `FOR UPDATE` / `ACCESS EXCLUSIVE` order for claim / rotate / prune | Implementation contract of ADR-019 |
| 024 | [Retention and partition rotation policy](024-retention-and-partition-rotation.md) | Accepted | Per-family rotation intervals, lock-timed prune with re-count inside tx, retention knobs | Operational policy for ADR-019 + ADR-020 |
| 025 | [Completion batcher with snapshot pass-through](025-completion-batcher-snapshot-passthrough.md) | Accepted | Carry the claim-time snapshot through the completion batcher so terminal append avoids re-reading `ready_entries` | Performance contract of ADR-019 |

## Validation artifacts

Runtime validation for ADR-019 is recorded in
[`bench/019-queue-storage-validation-2026-04-19.md`](bench/019-queue-storage-validation-2026-04-19.md)
(exact commands, raw output, measured numbers).

TLA+ correctness models that pin spec-level invariants are under
[`../../correctness/`](../../correctness/) — the segmented-storage family
(`AwaSegmentedStorage`, `AwaSegmentedStorageRaces`, `AwaStorageLockOrder`,
`AwaSegmentedStorageTrace`) maps to ADR-019, ADR-020, and ADR-023; the
worker-runtime family (`AwaCore`, `AwaExtended`, `AwaBatcher`, `AwaCbk`,
`AwaDispatchClaim`, `AwaViewTrigger`, `AwaCron`) covers rescue, batcher,
callback race, dispatcher claim, view-trigger concurrency, and cron
double-fire.

## Conventions

- Status is one of: **Accepted**, **Proposed**, **Superseded by ADR-XXX**,
  **Deprecated**, **Rejected**. Superseded ADRs stay in the directory as
  historical context.
- Relationships to later ADRs that change implementation but not
  decision are recorded in a bottom-of-doc `## Relationship to ADR-XXX`
  section rather than a top-of-doc `## Note`.
- ADRs should be narrative: context, rationale, what-was-considered.
  Deep implementation detail belongs in
  [`../architecture.md`](../architecture.md) or a companion design
  doc, with the ADR holding the decision and its alternatives.
- New ADRs claim the next number in a small placeholder PR before
  writing to avoid collisions.

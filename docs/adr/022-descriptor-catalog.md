# ADR-022: Descriptor Catalog for Queues and Job Kinds

## Status

Accepted

## Context

Awa is a polyglot runtime: Rust and Python workers run against the same
Postgres schema and the same queues. Operators need consistent names,
descriptions, ownership, and documentation links for each queue and each job
kind, regardless of which runtime is hosting a particular worker. Previously
the admin surfaces inferred queue existence from `queue_state_counts`, which
meant an idle-but-declared queue disappeared from listings and a queue's
human-readable name lived only in handler code.

## Decision

Add two catalog tables, `awa.queue_descriptors` and `awa.job_kind_descriptors`,
that hold operator-facing metadata distinct from per-job payload metadata.

- **Code-declared**: whichever runtime hosts the workers declares the
  descriptors via the Rust `ClientBuilder` or the Python `AsyncClient`. Both
  SDKs use the same catalog tables and the same canonicalised-JSON BLAKE3
  hashing scheme, so a mixed Rust + Python fleet produces consistent rows.
- **Batched upsert on startup + every snapshot tick** (default 10 s): one
  multi-row `INSERT ... ON CONFLICT` per catalog per tick, chunked at 5000
  rows.
- **Derived liveness signals**: at read time the admin path joins
  `awa.runtime_instances.descriptor_hashes` against the catalog to derive
  `stale` (no live runtime refreshed the descriptor) and `drift` (two
  runtimes report different hashes for the same queue/kind).
- **Retention**: the maintenance leader garbage-collects descriptors whose
  `last_seen_at` is older than the configured `descriptor_retention`
  (default 30 days).

Descriptors are explicitly off the hot path — dispatcher, claim, completion
batcher, heartbeat, and maintenance rescue never touch the catalog or the
per-runtime hash columns.

## Consequences

### Positive

- Queue and kind names, descriptions, tags, docs URLs, and ownership live in
  one place, visible from the UI, CLI, and REST API regardless of runtime.
- Declared-but-idle queues stay visible.
- Rolling-deploy drift (two runtimes disagreeing on ownership or docs URL)
  is detectable from admin reads, not from application logs.
- Python and Rust share the same catalog semantics without either SDK
  reimplementing the hashing or retention logic.

### Negative

- Additional write volume at startup and every snapshot tick. Measured at
  ~24 ms for 2000 descriptors; negligible at realistic fleet sizes.
- Read-side derivation of liveness signals is `O(live_runtimes × declared_descriptors)`.
  Sits behind the `/api/queues` cache; very large fleets (≥1000 runtimes ×
  ≥500 descriptors) would want a materialised view.

## Relationship to ADR-019

Descriptors are deliberately off the queue-storage hot path. Dispatch,
claim, and completion never touch `awa.queue_descriptors` or
`awa.job_kind_descriptors`; only `awa.queue_meta` (pause/resume) remains on
the dispatcher's queue-state read. This preserves the ADR-019 property that
operator-facing metadata changes cannot affect dispatch throughput.

See [architecture.md → Control-plane descriptors](../architecture.md#control-plane-descriptors)
for the implementation details, hashing algorithm, and measured
performance profile.

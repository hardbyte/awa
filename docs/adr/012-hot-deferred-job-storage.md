# ADR-012: Split Hot and Deferred Job Storage

## Status

Accepted

## Context

The original schema stored every job lifecycle state in one `awa.jobs` table.
That worked well for modest queue sizes, but the performance investigations on
this branch exposed a bad scaling shape once large deferred frontiers were
introduced.

The critical issue was not just due-row lookup. With millions of future-dated
rows present, every job still churned through the same heap and indexes:

- `scheduled -> available`
- `available -> running`
- `running -> completed`

Even after adding due-time indexes, that kept cold deferred rows and hot
execution rows in the same physical structure. It made dispatch plans more
fragile, increased write amplification, and let background promotion work
interfere with hot-path benchmarks and runtime behavior.

## Decision

Split the physical storage into:

- `awa.jobs_hot`: runnable, running, waiting, and terminal rows
- `awa.scheduled_jobs`: deferred `scheduled` and `retryable` rows
- `awa.jobs`: compatibility `UNION ALL` view across both tables

This is a manual hot/cold split, not native table partitioning.

### Routing Rules

- Immediate inserts (`available`) go to `awa.jobs_hot`
- Future-dated inserts (`scheduled`) go to `awa.scheduled_jobs`
- Retry backoff rows (`retryable`) live in `awa.scheduled_jobs`
- Due promotion moves rows from `awa.scheduled_jobs` into `awa.jobs_hot`
- Dispatch, heartbeat, completion, and rescue operate on `awa.jobs_hot`

### Compatibility Surface

`awa.jobs` remains available as a compatibility view so raw SQL, tests, and
external producers do not need an immediate breaking change. `INSTEAD OF`
triggers route writes to the correct physical table.

### Uniqueness

Cross-table uniqueness is enforced through `awa.job_unique_claims` rather than
through a partial unique index on the jobs heap. This keeps the uniqueness
boundary intact across both physical tables.

## Consequences

### Positive

- Keeps the hot execution table small and planner-friendly
- Lets promotion use dedicated due-time indexes on the deferred table
- Separates cold deferred storage from high-churn execution updates
- Preserves backward compatibility for most SQL surfaces via `awa.jobs`

### Negative

- Adds trigger/view complexity to the schema
- Introduces some risk that compatibility-view queries hide physical-table
  costs if benchmarks or runtime code accidentally use the view on the hot path
- Requires explicit care in tests and admin paths to query the right physical
  table when measuring behavior

## Notes

This decision deliberately favors explicit physical tables over native Postgres
partitioning. State transitions would cause frequent row movement under
state-based partitioning, while range partitioning would still leave hot and
cold workloads intertwined. The manual split gives clearer operational control
and simpler hot-path query tuning.

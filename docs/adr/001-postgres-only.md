# ADR-001: Postgres-Only, No Multi-Backend Support

## Status

Accepted

## Note

ADR-019 changes Awa's primary physical storage layout from the canonical
`jobs_hot` / `scheduled_jobs` split to queue segments plus a lease/runtime
sidecar. The decision in this ADR still stands: Awa remains Postgres-only, and
the redesign continues to lean on Postgres-native primitives rather than a
pluggable storage abstraction.

## Context

When designing a background job queue, one of the first architectural decisions is whether to support multiple storage backends (e.g., Postgres + Redis + MySQL) or commit to a single backend. Existing Rust job queues like `fang` take the multi-backend approach, abstracting over storage via traits. Systems like River (Go), Oban (Elixir), and GoodJob (Ruby) take the opposite approach -- they are Postgres-only and leverage that commitment for deeper feature integration.

Awa targets teams that already run Postgres (the most common RDBMS in the Rust and Python ecosystems). Adding Redis or another broker introduces operational overhead: another system to monitor, back up, secure, and reason about during outages.

## Decision

Awa uses Postgres as its sole infrastructure dependency. There is no storage backend trait, no pluggable adapter layer, and no plan to support non-Postgres databases.

This commitment enables direct use of Postgres-specific features throughout the codebase:

### FOR UPDATE SKIP LOCKED (PRD section 6.2)

The claim query uses `SKIP LOCKED` to achieve contention-free concurrent dispatch without application-level locking:

```sql
SELECT id FROM awa.jobs
WHERE state = 'available' AND queue = $1
ORDER BY ... LIMIT $2
FOR UPDATE SKIP LOCKED
```

Multiple workers polling the same queue will never claim the same job. This is the foundation of Awa's correctness guarantee.

### Transactional Enqueue (PRD section 1)

Because `insert()` accepts a `PgExecutor`, jobs can be enqueued inside the same transaction as the business operation that creates them:

```rust
let mut tx = pool.begin().await?;
create_order(&mut *tx, &order).await?;
awa::insert(&mut *tx, &SendConfirmation { order_id }).await?;
tx.commit().await?;
```

If the transaction rolls back, the job never exists. This eliminates an entire class of consistency bugs that plague systems with separate storage for jobs and application data.

### Advisory Locks (PRD section 6.4)

Leader election for maintenance tasks uses `pg_try_advisory_lock`, avoiding the need for external coordination (ZooKeeper, etcd, Consul). The lock is session-scoped -- if the leader's connection drops, another worker acquires it automatically.

Migration serialization also uses advisory locks (`pg_advisory_lock`) to prevent concurrent migration attempts.

### LISTEN/NOTIFY (PRD section 6.2)

Dispatchers subscribe to `awa:<queue>` channels via `PgListener`. An `AFTER INSERT` trigger on `awa.jobs_hot` fires `pg_notify` for immediately-available jobs, reducing poll latency from the poll interval (200ms) to near-instant wakeup. `awa.jobs` remains a compatibility view, but dispatch and promotion operate on the physical hot/deferred tables directly. Notifications use an empty payload so PostgreSQL can coalesce wakeups within a transaction.

### Partial Indexes

The schema uses partial indexes extensively to keep the hot path fast:

- `idx_awa_jobs_hot_dequeue`: only hot `state = 'available'` rows
- `idx_awa_scheduled_jobs_run_at_scheduled`: only deferred `state = 'scheduled'` rows
- `idx_awa_scheduled_jobs_run_at_retryable`: only deferred `state = 'retryable'` rows
- `idx_awa_jobs_hot_heartbeat`: only hot `state = 'running'` rows
- `idx_awa_jobs_hot_deadline`: only hot `state = 'running' AND deadline_at IS NOT NULL`
- `idx_awa_jobs_unique`: unique claims on `awa.job_unique_claims`

### Database-Side Functions

Backoff calculation (`awa.backoff_duration`) and uniqueness bitmask checks (`awa.job_state_in_bitmask`) run as database functions, keeping logic close to the data. Cross-table uniqueness is maintained by trigger-managed claims in `awa.job_unique_claims`, which lets `awa.jobs_hot` and `awa.scheduled_jobs` share one uniqueness boundary without one giant jobs heap.

## Consequences

### Positive

- **Simpler operations:** No additional infrastructure to deploy, monitor, or fail over.
- **Transactional enqueue:** The single most impactful feature -- impossible with a separate message broker.
- **Fewer abstraction layers:** No backend trait means less indirection, simpler code, and no lowest-common-denominator API constraints.
- **Deeper Postgres integration:** Advisory locks, LISTEN/NOTIFY, partial indexes, SKIP LOCKED, and custom functions are all first-class citizens rather than optional optimizations.
- **Consistent state:** Jobs and application data live in the same database, simplifying backup and disaster recovery.

### Negative

- **Postgres-only:** Teams using MySQL, SQLite, or CockroachDB cannot use Awa.
- **Scale ceiling:** Postgres is not a message broker. For extremely high throughput (millions of jobs/second), a dedicated system like Kafka may be more appropriate. Awa targets the common case: thousands to tens of thousands of jobs per second, which Postgres handles comfortably with `SKIP LOCKED`.
- **Insert-only polyglot contract:** Non-Rust, non-Python producers can enqueue jobs via raw `INSERT` statements (the schema is the contract), but they cannot run workers. This is an acceptable trade-off -- River, Oban, and GoodJob all have the same constraint.

# ADR 008: Batch COPY Ingestion

**Status:** Accepted
**Date:** 2026-03-18

## Context

The PRD (section 18) calls for a high-throughput insert path using PostgreSQL's
COPY protocol. The existing `insert_many` uses multi-row `INSERT` statements
with parameterized queries, which is limited by PostgreSQL's 65,535 parameter
limit (requiring chunking at roughly 5,950 rows with 11 params per row) and
the overhead of query planning per statement.

The original COPY design predated later architectural changes:

- hot and deferred jobs now live in separate physical tables
- `awa.jobs` is a compatibility view rather than the main hot-path heap
- uniqueness is enforced through `awa.job_unique_claims`
- callers may invoke COPY multiple times inside one outer transaction

ADR-019 supersedes the hot/deferred physical layout as Awa's primary storage
engine. The staging-table decision in this ADR still stands, but backend-aware
routing now needs to target the active storage engine rather than assuming the
canonical tables are the hot path forever.

## Decision

Implement batch ingestion via a staging-table approach:

1. Create or reuse a session-local temp table in `pg_temp` with
   `ON COMMIT DELETE ROWS`
2. `COPY` CSV-encoded rows into that staging table
3. Route homogeneous batches directly to `awa.jobs_hot` or
   `awa.scheduled_jobs`; mixed batches continue to target the compatibility
   `awa.jobs` surface
4. For non-unique batches, use one `INSERT ... SELECT ... RETURNING *` from
   staging into the chosen target table
5. For batches containing unique jobs, read staged rows back and insert them
   one at a time under savepoints, skipping `23505` uniqueness conflicts
6. Explicitly clear staged rows after use so multiple COPY calls can happen
   safely inside the same outer transaction

### Why staging table instead of direct COPY into `awa.jobs`

- The staging table has no constraints, no indexes, and no Awa triggers, so
  the COPY phase stays simple and fast
- The final insert still goes through Awa's real insert semantics, including
  hot/deferred routing and enqueue side effects
- The compatibility `awa.jobs` surface is a view, so direct COPY into it is
  not a practical general solution
- Reusing a session-local temp table avoids repeated catalog churn under
  concurrent producers
- `ON COMMIT DELETE ROWS` still gives transactional cleanup on
  commit/rollback

### API signatures

```rust
pub async fn insert_many_copy(conn: &mut PgConnection, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
pub async fn insert_many_copy_from_pool(pool: &PgPool, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
```

Accepting `&mut PgConnection` allows callers to use COPY within a broader
transaction (Transaction derefs to PgConnection in sqlx 0.8).

### CSV serialization

Custom CSV serialization handles escaping and null encoding for:

- JSONB fields (JSON text, CSV-quoted)
- `TEXT[]` arrays (Postgres `{...}` literal, CSV-quoted)
- `BYTEA` (`\\x...` hex format)
- `TIMESTAMPTZ` (RFC 3339, or the COPY null sentinel)
- `BIT(8)` (text bit string)

### NOTIFY trigger impact

The enqueue notify trigger fires when the final insert reaches the hot table.
This is acceptable: PostgreSQL coalesces notifications within a transaction,
and dispatchers handle duplicates gracefully.

## Consequences

### Positive

- **No parameter limit:** COPY path bypasses the 65,535 parameter limit entirely.
- **Reusable staging path:** Session-local staging avoids repeated temp-table
  create/drop churn under contention.
- **Shared internals:** `PreparedRow` / `precompute_rows` are reused between
  `insert_many` and `insert_many_copy`.
- **Python support:** Python bindings expose `insert_many_copy` / `insert_many_copy_sync`.

### Negative

- **CSV serialization complexity:** Custom CSV encoding for JSONB, `TEXT[]`,
  `BYTEA`, and `TIMESTAMPTZ` requires careful escaping and adds a non-trivial
  code path to maintain.
- **Split unique path:** Unique jobs do not use one bulk `ON CONFLICT` path
  anymore; they fall back to savepoint-guarded row inserts after staging.
- **Staging overhead remains:** COPY still pays for staging and a final insert
  into the real Awa tables, so it is not automatically faster than chunked
  multi-row `INSERT` in every workload.

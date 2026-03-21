# ADR 008: Batch COPY Ingestion

**Status:** Accepted
**Date:** 2026-03-18

## Context

The PRD (section 18) calls for a high-throughput insert path using PostgreSQL's COPY protocol. The existing `insert_many` uses multi-row INSERT statements with parameterized queries, which is limited by PostgreSQL's 65,535 parameter limit (requiring chunking at ~5,950 rows with 11 params per row) and the overhead of query planning per statement.

## Decision

Implement batch ingestion via a **staging table** approach:

1. `CREATE TEMP TABLE awa_copy_staging (...) ON COMMIT DROP` — no constraints, no indexes
2. `COPY awa_copy_staging FROM STDIN (FORMAT csv)` — maximally fast bulk load
3. `INSERT INTO awa.jobs (...) SELECT ... FROM awa_copy_staging ON CONFLICT DO NOTHING RETURNING *`
4. Transaction commit drops the staging table automatically

### Why staging table instead of direct COPY into `awa.jobs`

- Staging table has no constraints, no indexes, no triggers — the COPY phase is maximally fast
- The `INSERT...SELECT` handles `ON CONFLICT` naturally for unique jobs
- `ON COMMIT DROP` is crash-safe — no cleanup needed on failure
- Uniform code path for unique and non-unique jobs

### API signatures

```rust
pub async fn insert_many_copy(conn: &mut PgConnection, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
pub async fn insert_many_copy_from_pool(pool: &PgPool, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError>
```

Accepting `&mut PgConnection` allows callers to use COPY within a broader transaction (Transaction derefs to PgConnection in sqlx 0.8).

### CSV serialization

Custom CSV serialization handles escaping for:
- JSONB fields (JSON text, CSV-quoted)
- TEXT[] arrays (Postgres `{elem1,"elem2"}` literal, CSV-quoted)
- BYTEA (hex-encoded, decoded via `decode(hex)` in SQL)
- TIMESTAMPTZ (RFC 3339, or `\N` for NULL)
- BIT(8) (text bit string, cast in INSERT...SELECT)

### NOTIFY trigger impact

The `trg_awa_notify` trigger fires AFTER INSERT on `awa.jobs`. The final `INSERT INTO awa.jobs ... SELECT ...` fires notifications per inserted row. This is acceptable — PostgreSQL coalesces notifications within a transaction, and dispatchers handle duplicates gracefully.

## Consequences

### Positive

- **No parameter limit:** COPY path bypasses the 65,535 parameter limit entirely.
- **No chunking needed:** Single-statement inserts of 10K+ rows without client-side batching.
- **Shared internals:** `RowValues` / `precompute_row_values` are reused between `insert_many` and `insert_many_copy`.
- **Python support:** Python bindings expose `insert_many_copy` / `insert_many_copy_sync`.

### Negative

- **CSV serialization complexity:** Custom CSV encoding for JSONB, TEXT[], BYTEA, and TIMESTAMPTZ requires careful escaping and adds a non-trivial code path to maintain.
- **Temp table per call:** Each invocation creates and drops a temporary staging table, adding per-call overhead that is amortized only for larger batches.

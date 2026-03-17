# ADR-006: AwaTransaction as Narrow SQL Surface

## Status

Accepted

## Context

Python users need transactional enqueue — the ability to insert a job atomically with business logic in the same database transaction. This is Awa's killer feature vs Redis-backed queues.

The question is: what API surface should the Python transaction provide?

Options considered:

1. **Full ORM integration** — accept psycopg3/asyncpg/SQLAlchemy connections and bridge them to Awa's sqlx pool. Maximum ergonomics, minimum adoption friction.

2. **Narrow raw SQL surface** — provide `execute()`, `fetch_one()`, `fetch_all()` with `$1/$2` placeholders and `dict[str, Any]` results. Plus `insert()` for job enqueue.

3. **Insert-only transaction** — only expose `insert()` within the transaction, no general SQL. Users must use a separate DB connection for business logic and accept non-atomic enqueue.

## Decision

Option 2: Narrow raw SQL surface (`AwaTransaction`).

```python
async with await client.transaction() as tx:
    await tx.execute(
        "INSERT INTO orders (id, total) VALUES ($1, $2)",
        [order_id, total]
    )
    await tx.insert(SendConfirmationEmail(order_id=order_id))
    # Commits on clean exit, rolls back on exception
```

### Why not full ORM integration?

- **Driver bridging is fragile.** psycopg3, asyncpg, and SQLAlchemy each manage connections differently. Bridging their connection pools with sqlx's pool would require version-pinned, driver-specific code that breaks on upgrades.
- **Connection sharing is unsafe.** Two drivers (sqlx and psycopg3) cannot safely share the same PG connection without coordinating protocol state. This is a known hard problem.
- **Scope creep.** ORM integration pulls in Django/SQLAlchemy as soft dependencies and multiplies the test surface.

We may revisit this in v0.2 as "experimental driver bridging" with explicit caveats.

### Why not insert-only?

- Users frequently need `INSERT INTO orders ... RETURNING id` before they can enqueue the follow-up job. Without raw SQL in the transaction, they'd need two connections and lose atomicity — defeating the purpose.
- The narrow surface (`execute`, `fetch_one`, `fetch_all`) is sufficient for the 80% use case.

## Consequences

### Positive

- **Guaranteed atomicity.** The transaction runs on a single sqlx connection. Commit = both business row and job exist. Rollback = neither exists.
- **Zero external dependencies.** No psycopg3, no asyncpg, no SQLAlchemy.
- **Simple mental model.** One transaction, raw SQL, `$1/$2` placeholders.

### Negative

- **Raw SQL ergonomics.** Users write `$1` placeholders instead of parameterized ORM queries. Schema-qualified table names required.
- **`dict[str, Any]` results.** No ORM model hydration. Users get plain dictionaries.
- **No savepoints.** Nested transactions are not supported in v0.1.
- **`async with await`** — the double-await pattern (`async with await client.transaction() as tx`) is unfamiliar to Python developers. This is inherent to the PyO3 async bridge: `transaction()` is an async method (needs `await`) that returns an async context manager (needs `async with`).

### v0.1 Limitations (explicit)

- No savepoints
- No nested transactions
- No ORM integration
- `$1, $2` placeholders only (no named parameters)
- `dict[str, Any]` results (no model hydration)
- Schema-qualified table names required (`awa.jobs`, not `jobs`)
- Shared connection pool with workers

These are intentional constraints, not bugs. The transaction surface is narrow by design.

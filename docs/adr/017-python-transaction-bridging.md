# ADR-017: Python Insert-Only Transaction Bridging

## Status

Accepted

## Context

ADR-006 chose `AwaTransaction` as a narrow sqlx-backed transaction surface for Python instead of trying to integrate directly with psycopg3, asyncpg, SQLAlchemy, or Django.

That decision rejected two things:

- sharing a live database connection between different drivers
- growing Awa into a general ORM abstraction

Those constraints still hold. What changed is the shape of the problem.

Python users already have application transactions managed by raw drivers or ORM/session layers. Forcing them onto `AwaTransaction` creates adoption friction for the common case where they already have:

- an `asyncpg` transaction in a FastAPI service
- a `psycopg` connection in a sync app
- a SQLAlchemy `Session` / `AsyncSession`
- a Django `transaction.atomic()` block

The question is whether Awa should support transactional enqueue within those existing transactions, and if so, how far that support extends.

## Decision

Expose a public Python bridge module, `awa.bridge`, with insert-only helpers that execute on the caller's existing transactional connection or session:

- `insert_job(conn_or_session, args, ...)`
- `insert_job_sync(conn_or_session, args, ...)`

### Supported surfaces

- `asyncpg.Connection`
- `psycopg.AsyncConnection`
- `psycopg.Connection`
- SQLAlchemy `AsyncConnection`
- SQLAlchemy `AsyncSession`
- SQLAlchemy `Connection`
- SQLAlchemy `Session`
- Django database connection inside `transaction.atomic()`

### Design constraints

- **Insert-only.** The bridge inserts jobs; it does not expose a general query API.
- **No connection sharing.** Awa does not share protocol state with another driver. The bridge executes SQL through the caller's own driver/session object.
- **Participates in the caller's transaction.** Commit/rollback semantics are owned by the application framework or driver already in use.
- **Runtime detection is acceptable in Python.** The bridge may inspect the passed object and dispatch to the appropriate adapter path.
- **Parity with core insert semantics is required.** Validation, state determination, and insert defaults must match the main Awa insert path, with dedicated tests guarding that parity.

## Scope boundaries

- **No worker/runtime bridging.** Polling, dispatch, heartbeating, retries, and admin operations remain on Awa's own runtime/client surfaces.
- **No transaction lifecycle management.** The bridge does not begin, commit, roll back, or manage savepoints for external transactions.
- **No ORM abstraction layer.** We support specific widely used connection/session types, not arbitrary ORM adapters.
- **No batch/COPY bridge.** Bridge support is single-job insert only.
- **No promise of every wrapper working.** Wrapper compatibility is explicit and test-backed, not implicit.

## Consequences

- Python teams can adopt Awa incrementally without rewriting application data access around `AwaTransaction`.
- The supported bridge surfaces become a compatibility commitment and must stay exercised in CI.
- ADR-006 remains valid for the core principle that Awa should not share database connections across drivers or become an ORM integration layer.
- The bridge has a broader support matrix than `AwaTransaction`, so semantic drift is the main maintenance risk; parity tests are the primary mitigation.

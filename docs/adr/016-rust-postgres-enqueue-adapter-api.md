# ADR-016: Public Rust Postgres Enqueue Adapter API

## Status

Accepted

## Context

Awa's insert functions (`awa::insert`, `awa::insert_with`) are bounded on sqlx's `PgExecutor` trait. Users whose Rust services already use tokio-postgres cannot insert jobs within their existing transactions without also depending on sqlx and managing a second connection pool.

ADR-006 rejected full ORM integration based on sharing connections or bridging driver protocol state. ADR-017 later accepted insert-only Python bridge helpers that execute through the caller's own driver/session.

This decision applies the same principle to Rust: factor out insert preparation so Awa can support transactional enqueue from non-sqlx Postgres stacks without sharing connections or becoming an ORM abstraction.

## Decision

### Shared insert preparation

Extract the computation that `insert_with` performs before touching the database — kind resolution, args serialization, null-byte validation, state determination, unique key (BLAKE3), unique states bitmask, and ordering-key propagation — into shared insert preparation in `awa-model/src/insert.rs`.

Both the existing sqlx path and bridge adapters call the same preparation functions. This is the primary defense against semantic drift: if insert semantics change, they change in one place.

### Public Rust adapter API

Expose the stable public preparation surface under `awa::adapter::postgres` and `awa_model::adapter::postgres`:

- `prepare_job_insert(args, opts)` for typed Rust `JobArgs`
- `prepare_raw_job_insert(kind, args_json, opts)` for dynamic producers
- `PreparedJobInsert`, with read-only getters in bind order
- `INSERT_JOB_SQL`, the canonical single-row Postgres insert statement
- `UNIQUE_VIOLATION_SQLSTATE`, for adapter error mapping

External Rust crates can execute `INSERT_JOB_SQL` through their own connection or transaction type while binding values from `PreparedJobInsert`. Awa remains responsible for insert semantics; the adapter remains responsible for driver-specific execution and row decoding.

The native sqlx path is not required to use this driver-friendly SQL. It may keep a typed `PgExecutor` query that binds `JobState` directly and returns `SELECT *`, while external adapters use `INSERT_JOB_SQL` with text casts and `state_str` for portable enum decoding.

### tokio-postgres adapter

The built-in feature-gated module `awa::bridge::tokio_pg` (behind the `tokio-postgres` Cargo feature) provides:

- `insert_job(client, args)` — default options
- `insert_job_with(client, args, opts)` — custom options
- `insert_job_raw(client, kind, args_json, opts)` — untyped

All functions are bounded on `tokio_postgres::GenericClient`, which is implemented for `Client` and `Transaction`. Pool wrappers (deadpool, bb8) do not implement this trait directly; users call `.transaction()` on the wrapper first.

The tokio-postgres bridge uses the public adapter API internally, so it acts as a checked in-repository example of how external Rust adapters should preserve Awa's insert semantics.

### Scope boundaries

- **Insert-only adapter contract.** Worker polling, heartbeating, claiming, and completion remain Awa runtime responsibilities. Applications may still use SeaORM, Diesel, or another stack inside handlers by passing those dependencies through worker state.
- **No connection sharing.** Each driver owns its own connection lifecycle.
- **No batch/COPY bridging.** The COPY path requires `PgConnection::copy_in_raw()` and is sqlx-specific. Bridge adapters support single-row parameterized INSERT only.
- **Native bulk paths remain native.** `insert_many_copy_from_pool` and `QueueStorage::enqueue_params_copy` stay on the SQLx/Awa runtime side. External adapters can add their own driver-specific bulk support later, but the public adapter contract does not promise a portable COPY abstraction.
- **External adapters are allowed.** Diesel, SeaORM, and similar crates can live outside the Awa workspace if they use the public preparation and SQL contract. They must prove transaction participation with driver-specific commit/rollback tests.
- **No generic adapter trait yet.** Driver and ORM transaction APIs differ enough that a trait would mostly abstract over "bind these values and decode a row" without a stable common executor model. The stable contract is the prepared insert plus SQL, not a universal Rust trait.

## Consequences

- Users can adopt Awa incrementally: enqueue from their existing tokio-postgres stack, run workers on the Awa runtime.
- External Rust adapter crates can preserve core insert semantics without copying private Awa logic.
- The public adapter API becomes a compatibility commitment. Changes to `PreparedJobInsert` bind order or `INSERT_JOB_SQL` require migration-level care and tests.
- The CI lint job runs clippy with `--all-features` and a dedicated CI lane runs the bridge integration tests.

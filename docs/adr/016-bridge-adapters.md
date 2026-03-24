# ADR-016: Shared Insert Preparation and tokio-postgres Enqueue Adapter

## Status

Accepted

## Context

Awa's insert functions (`awa::insert`, `awa::insert_with`) are bounded on sqlx's `PgExecutor` trait. Users whose Rust services already use tokio-postgres cannot insert jobs within their existing transactions without also depending on sqlx and managing a second connection pool.

ADR-006 rejected full ORM integration (sharing connections, bridging driver protocol state). This decision is narrower: factor out insert preparation so a tokio-postgres adapter can reuse it.

## Decision

### Shared insert preparation

Extract the computation that `insert_with` performs before touching the database — kind resolution, args serialization, null-byte validation, state determination, unique key (BLAKE3), unique states bitmask — into a `pub(crate) PreparedRow` struct and `prepare_row` / `prepare_row_raw` functions in `awa-model/src/insert.rs`.

Both the existing sqlx path and new bridge adapters call the same preparation functions. This is the primary defense against semantic drift: if insert semantics change, they change in one place.

### tokio-postgres adapter

A feature-gated module `awa::bridge::tokio_pg` (behind the `tokio-postgres` Cargo feature) provides:

- `insert_job(client, args)` — default options
- `insert_job_with(client, args, opts)` — custom options
- `insert_job_raw(client, kind, args_json, opts)` — untyped

All functions are bounded on `tokio_postgres::GenericClient`, which is implemented for `Client` and `Transaction`. Pool wrappers (deadpool, bb8) do not implement this trait directly; users call `.transaction()` on the wrapper first.

### Scope boundaries

- **Insert-only.** Workers, heartbeating, polling, and dispatch remain sqlx-only.
- **No connection sharing.** Each driver owns its own connection lifecycle.
- **No batch/COPY bridging.** The COPY path requires `PgConnection::copy_in_raw()` and is sqlx-specific. Bridge adapters support single-row parameterized INSERT only.
- **Future adapters** (diesel, sea-orm, etc.) are follow-on work, not covered by this decision.

## Consequences

- Users can adopt Awa incrementally: enqueue from their existing tokio-postgres stack, run workers on the Awa runtime.
- `PreparedRow` is `pub(crate)`, not public API. Adding a new adapter means adding a feature flag and submodule inside this crate.
- The CI lint job runs clippy with `--all-features` and a dedicated CI lane runs the bridge integration tests.

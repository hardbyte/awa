# Rust crates

The workspace separates storage and runtime concerns so producers do not need to carry a worker runtime.

| Crate | Use it for | API docs |
| --- | --- | --- |
| `awa` | The usual Rust client: typed jobs, workers, migrations, enqueue, and admin APIs | [docs.rs/awa](https://docs.rs/awa) |
| `awa-model` | Schema, migrations, records, enqueue operations, and admin queries without workers | [docs.rs/awa-model](https://docs.rs/awa-model) |
| `awa-worker` | Worker runtime and execution internals | [docs.rs/awa-worker](https://docs.rs/awa-worker) |
| `awa-testing` | PostgreSQL-backed fixtures and helpers for application tests | [docs.rs/awa-testing](https://docs.rs/awa-testing) |
| `awa-seaorm` | SeaORM integration | [docs.rs/awa-seaorm](https://docs.rs/awa-seaorm) |

## Recommended entry point

Most services should depend on `awa`. It re-exports the commonly used model and worker types, including `Client`, `JobArgs`, `JobResult`, `InsertOpts`, `QueueConfig`, migration helpers, and admin operations.

```toml
[dependencies]
awa = "0.6"
```

These site docs track the 0.7 development branch. Select the version matching your dependency in the docs.rs version menu, and use the [stability policy](../stability.md) when evaluating alpha APIs.

## Producers without workers

Choose `awa-model` when a service only inserts or inspects jobs. It keeps the execution runtime out of that process while preserving the same PostgreSQL contract.

For a complete compile-checked program, follow the [Rust getting started guide](../getting-started-rust.md).

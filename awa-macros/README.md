# awa-macros

Procedural macros for the [Awa](https://crates.io/crates/awa) Postgres-native job queue.

You don't normally depend on this crate directly — `#[derive(JobArgs)]` is re-exported by both [`awa`](https://crates.io/crates/awa) and [`awa-model`](https://crates.io/crates/awa-model). Add `awa` to your dependencies and the derive comes with it.

## What's in here

- `#[derive(JobArgs)]` — implements `awa::JobArgs` for a struct, generating the `kind()` method that identifies the job type across Rust and Python workers.

## Usage

```rust
use awa::JobArgs;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    to: String,
    subject: String,
}

assert_eq!(SendEmail::kind(), "send_email");
```

The default kind string is the struct's name converted from `CamelCase` to `snake_case`. Override it explicitly with `#[awa(kind = "...")]`:

```rust
#[derive(Debug, Serialize, Deserialize, JobArgs)]
#[awa(kind = "outbound_smtp_send")]
struct SendEmail {
    to: String,
    subject: String,
}

assert_eq!(SendEmail::kind(), "outbound_smtp_send");
```

The macro resolves `JobArgs` through whichever of `awa` or `awa-model` is in your `Cargo.toml`, so the same derive works for applications using the facade crate and for libraries depending on `awa-model` directly.

`Serialize` and `Deserialize` must be derived alongside `JobArgs` — the runtime serialises job arguments as JSON.

## License

MIT OR Apache-2.0

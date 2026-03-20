# awa-macros

Derive macros for the [Awa](https://crates.io/crates/awa) job queue.

Provides `#[derive(JobArgs)]` which generates:
- A `kind()` method returning the CamelCase→snake_case job kind string
- Serde trait bounds for JSON serialization

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

You get this automatically when you depend on [`awa`](https://crates.io/crates/awa).

## License

MIT OR Apache-2.0

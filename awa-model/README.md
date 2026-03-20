# awa-model

Core types, SQL queries, and migrations for the [Awa](https://crates.io/crates/awa) job queue.

This crate provides:
- `JobRow`, `JobState`, `InsertOpts`, `UniqueOpts` — job data types
- `insert`, `insert_with`, `insert_many`, `insert_many_copy` — job insertion
- `admin::*` — retry, cancel, pause, drain, queue stats, callback resolution
- `migrations::run` — schema creation and migration
- `cron::*` — periodic job schedule management

You don't usually need to depend on this crate directly — [`awa`](https://crates.io/crates/awa) re-exports everything.

## License

MIT OR Apache-2.0

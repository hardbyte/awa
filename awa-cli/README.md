# awa-cli

Command-line interface for the [Awa](https://crates.io/crates/awa) Postgres-native job queue. Run migrations, inspect and manage jobs, walk the storage transition, drive the Dead Letter Queue, list cron schedules, and serve the web admin UI.

## Install

The CLI is shipped as both a Rust binary and a Python wheel. Either distribution gives you the same `awa` executable, including the embedded React admin dashboard for `awa serve`.

```bash
# Python (no Rust toolchain needed)
pip install awa-cli

# Rust
cargo install awa-cli
```

If you're already using the [`awa-pg`](https://pypi.org/project/awa-pg/) Python SDK, install both with one command:

```bash
pip install 'awa-pg[ui]'
```

That pulls in `awa-cli` as a dependency so `python -m awa serve` works end-to-end alongside the worker SDK.

## Quick start

```bash
# Run migrations
awa --database-url $DATABASE_URL migrate

# Inspect
awa --database-url $DATABASE_URL queue stats
awa --database-url $DATABASE_URL job list --state failed
awa --database-url $DATABASE_URL job dump 12345
awa --database-url $DATABASE_URL job dump-run 12345

# Admin
awa --database-url $DATABASE_URL job retry 12345
awa --database-url $DATABASE_URL queue pause email
awa --database-url $DATABASE_URL queue drain email

# Web UI
awa --database-url $DATABASE_URL serve
# → http://127.0.0.1:3000
```

`DATABASE_URL` may be passed as the `--database-url` flag or read from the environment.

## Commands

| Command | Description |
| --- | --- |
| `migrate` | Apply migrations, or extract / print SQL with `--sql` / `--extract-to` |
| `job list` | List jobs with `--state` / `--kind` / `--queue` filters |
| `job dump <id>` | Pretty-print one job and its full lifecycle metadata |
| `job dump-run <id> [--attempt N]` | Pretty-print one attempt run |
| `job retry <id>` | Retry a failed or cancelled job |
| `job cancel <id>` | Cancel a job |
| `job retry-failed --kind K` | Retry every failed job of a given kind |
| `job discard --kind K` | Delete every failed job of a given kind |
| `queue stats` | Per-queue depth, lag, and throughput |
| `queue pause / resume / drain <queue>` | Queue admin |
| `cron list / remove` | List or remove cron schedules |
| `dlq depth [--queue Q]` | Total DLQ rows, optionally split by queue |
| `dlq list` | List DLQ entries with `--kind` / `--queue` / `--tag` / `--before-*` filters |
| `dlq retry <id>` | Retry a single DLQ row |
| `dlq retry-bulk` | Retry every DLQ row matching the filter (`--all` required if no filter is given) |
| `dlq move` | Move existing failed terminal rows into the DLQ |
| `dlq purge` | Delete DLQ rows matching the filter (`--all` required if no filter is given) |
| `storage status` | Current storage-transition state |
| `storage prepare --engine E` | Prepare a future storage engine without changing routing |
| `storage prepare-queue-storage-schema` | Materialize the queue-storage schema (tables, indexes, functions) |
| `storage enter-mixed-transition` | Begin routing new writes to the prepared engine |
| `storage finalize` | Finalize the transition once drain and capability gates pass |
| `storage abort` | Abort a prepared or mixed-transition rollout |
| `serve` | Start the embedded web admin UI |

Run `awa <command> --help` for the flags on any subcommand.

## Storage transition

For an existing 0.5.x cluster moving to the queue-storage engine, the typical sequence is:

```bash
awa --database-url $DATABASE_URL storage prepare-queue-storage-schema
awa --database-url $DATABASE_URL storage prepare --engine queue_storage
# ... roll out a binary that supports queue storage to all workers ...
awa --database-url $DATABASE_URL storage enter-mixed-transition
# ... drain the canonical engine ...
awa --database-url $DATABASE_URL storage finalize
```

See [`docs/upgrade-0.5-to-0.6.md`](../docs/upgrade-0.5-to-0.6.md) for the full pre-flight checklist, gate semantics, and rollback notes. Fresh installs auto-finalize on first migrate and do not need this sequence.

## Dead Letter Queue

`dlq retry-bulk` and `dlq purge` require an explicit filter (`--kind`, `--queue`, or `--tag`) or `--all`. This is intentional — a bare bulk retry / purge with no filter would touch every DLQ row, which is almost never what you want. See [`docs/dead-letter-queue.md`](../docs/dead-letter-queue.md).

## Web UI

`awa serve` starts an embedded admin UI ([`awa-ui`](../awa-ui)) bound to `127.0.0.1:3000` by default. The UI is read-only when the database reports `transaction_read_only = on` (e.g. on a replica) or when `--read-only` is passed explicitly. Mutation endpoints return 503 in that mode.

## License

MIT OR Apache-2.0

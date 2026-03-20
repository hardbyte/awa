# awa-cli

CLI for the [Awa](https://crates.io/crates/awa) Postgres-native job queue.

```bash
# Install (no Rust toolchain needed)
pip install awa-cli
# or: cargo install awa-cli

# Run migrations
awa --database-url $DATABASE_URL migrate

# Admin
awa --database-url $DATABASE_URL queue stats
awa --database-url $DATABASE_URL job list --state failed
awa --database-url $DATABASE_URL job retry 12345

# Web UI
awa --database-url $DATABASE_URL serve
# → http://127.0.0.1:3000
```

## Commands

| Command | Description |
|---------|-------------|
| `migrate` | Run database migrations (or extract SQL) |
| `job list` | List jobs with state/kind/queue filters |
| `job retry` | Retry a failed/cancelled job |
| `job cancel` | Cancel a job |
| `job retry-failed` | Retry all failed jobs by kind |
| `job discard` | Delete failed jobs by kind |
| `queue stats` | Show per-queue depth, lag, throughput |
| `queue pause` | Pause a queue |
| `queue resume` | Resume a paused queue |
| `queue drain` | Cancel all pending jobs in a queue |
| `cron list` | List registered cron schedules |
| `cron remove` | Remove a cron schedule |
| `serve` | Start the web UI dashboard |

## License

MIT OR Apache-2.0

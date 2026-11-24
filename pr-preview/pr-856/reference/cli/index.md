> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# CLI command map

The CLI is the migration and operations surface for AWA. The command itself is the exact reference for the installed version:

```bash
awa --help
awa <command> --help
```

| Command | Purpose |
| --- | --- |
| `migrate` | Apply AWA schema migrations |
| `health` | Check database and runtime health |
| `job` | List, inspect, dump, retry, cancel, or discard jobs |
| `queue` | Inspect queue statistics and runtime overrides |
| `dlq` | Inspect, redrive, or purge retained terminal failures |
| `batch-ops` | Operate on a selected set of jobs |
| `cron` | Inspect and manage periodic jobs |
| `storage` | Inspect or administer queue-storage transitions |
| `callbacks` | Run the callback receiver and administer callback state |
| `serve` | Host the web dashboard and admin API |
| `context` | Print shell or agent-oriented operational context |

## Connection options

Use `--database-url` or `DATABASE_URL`. Commands that mutate state can require stronger privileges than read-only inspection; use the [security guide](../../security/index.md) to split migration, runtime, maintenance, and observer roles.

```bash
awa --database-url "$DATABASE_URL" job list --queue payments
awa --database-url "$DATABASE_URL" job dump 42
awa --database-url "$DATABASE_URL" queue stats
```

The web UI started by `awa serve` is read-only when PostgreSQL reports a read-only transaction or when the command receives `--read-only`.

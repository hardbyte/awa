# awa-pg

Python bindings for [awa](https://github.com/hardbyte/awa), a
Postgres-native background job queue. Same engine, same SQL, same
defaults as the Rust core; native-speed dispatch via PyO3.

```bash
pip install awa-pg
```

## Quick start

```python
import asyncio
import os
from awa import AsyncClient

async def send_email(args, ctx):
    print(f"sending to {args['to']}: {args['subject']}")

async def main():
    client = AsyncClient(os.environ["DATABASE_URL"])
    await client.register_kind("send_email", send_email)
    await client.queue("email")

    await client.enqueue(
        "send_email",
        {"to": "ada@example.com", "subject": "hello"},
        queue="email",
    )

    await client.start([("email", 4)])  # 4 workers on the email queue

asyncio.run(main())
```

A synchronous worker model is also available via `awa.Client` for
codebases that aren't async-first.

## What you get

- **Transactional enqueue** — enqueue inside the same Postgres transaction
  as your application's writes.
- **Vacuum-aware storage** — append-only ready entries plus a partitioned
  receipt ring keep dead-tuple pressure bounded under sustained load.
  See [ADR-019](https://github.com/hardbyte/awa/blob/main/docs/adr/019-queue-storage-redesign.md)
  and [ADR-023](https://github.com/hardbyte/awa/blob/main/docs/adr/023-receipt-plane-ring-partitioning.md).
- **COPY ingestion** — `insert_many_copy` keeps the compatibility insert
  surface fast, and `enqueue_many_copy` streams directly into queue storage
  for high-volume Python producers.
- **Crash-safe execution** — heartbeat-based lease tracking; jobs whose
  workers vanish are rescued automatically.
- **Per-queue policy** — priorities, priority aging, weighted concurrency,
  rate limits, deadlines, retry/backoff, cron, dead-letter queue.
- **Progress tracking** — handlers can write structured progress that
  survives across retries.
- **Web UI (optional)** — `pip install 'awa-pg[ui]'` pulls in the
  [`awa-cli`](https://pypi.org/project/awa-cli/) wheel, which ships the
  dashboard binary. Then `python -m awa serve` (or `awa serve` directly)
  runs a live queue inspector, DLQ triage console, and retry controls
  on `http://127.0.0.1:3000`. The default `awa-pg` install stays small
  for workers and producers that don't need the dashboard.

## Migrations

```bash
python -m awa --database-url "$DATABASE_URL" migrate
```

Fresh installs go straight to the queue-storage engine on first migrate.
Existing 0.5.x installations should follow
[`docs/upgrade-0.5-to-0.6.md`](https://github.com/hardbyte/awa/blob/main/docs/upgrade-0.5-to-0.6.md)
for the staged transition.

## Documentation

- [Getting started (Python)](https://github.com/hardbyte/awa/blob/main/docs/getting-started-python.md)
- [Configuration](https://github.com/hardbyte/awa/blob/main/docs/configuration.md)
- [Dead Letter Queue](https://github.com/hardbyte/awa/blob/main/docs/dead-letter-queue.md)
- [Architecture](https://github.com/hardbyte/awa/blob/main/docs/architecture.md)
- [Cross-system benchmark comparison](https://github.com/hardbyte/postgresql-job-queue-benchmarking)

## License

Dual-licensed under MIT or Apache-2.0, at your option.

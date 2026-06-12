# awa-pg

Python bindings for [awa](https://github.com/hardbyte/awa), a Postgres-native background job queue. Same engine, same SQL, same defaults as the Rust core; native-speed dispatch via PyO3.

```bash
pip install awa-pg
```

## Quick start

```python
import asyncio
import os
from dataclasses import dataclass

from awa import AsyncClient


@dataclass
class SendEmail:
    to: str
    subject: str


async def main():
    client = AsyncClient(os.environ["DATABASE_URL"])

    @client.task(SendEmail, queue="email")
    async def send_email(job):
        print(f"sending to {job.args.to}: {job.args.subject}")

    await client.start([("email", 4)])  # 4 workers on the email queue

    await client.insert(
        SendEmail(to="ada@example.com", subject="hello"),
        queue="email",
    )

    await asyncio.sleep(1)
    await client.shutdown()

asyncio.run(main())
```

A synchronous worker model is also available via `awa.Client` for codebases that aren't async-first.

For application tables, keep using your existing database library. The `awa.bridge` helpers insert jobs through asyncpg, psycopg3, SQLAlchemy, or Django connections so app rows and jobs can commit in the same transaction.

## What you get

- **Transactional enqueue** — enqueue inside the same Postgres transaction as your application's writes, using your existing connection/session.
- **Vacuum-aware storage** — append-only ready entries plus a partitioned receipt ring keep dead-tuple pressure bounded under sustained load. See [ADR-019](https://github.com/hardbyte/awa/blob/main/docs/adr/019-queue-storage-redesign.md) and [ADR-023](https://github.com/hardbyte/awa/blob/main/docs/adr/023-receipt-plane-ring-partitioning.md).
- **COPY ingestion** — `enqueue_many_copy` streams directly into queue storage for high-volume Python producers. `insert_many_copy` remains the compatibility insert surface for canonical-storage and adapter-style callers. If workers use `queue_storage_queue_stripe_count > 1`, pass the same value to `enqueue_many_copy`.
- **Partitioned queues** — `PartitionedQueue` maps one hot logical queue to several physical queues so workers can drain independent streams without changing Awa's durability model.
- **Crash-safe execution** — heartbeat-based lease tracking; jobs whose workers vanish are rescued automatically.
- **Per-queue policy** — priorities, priority aging, weighted concurrency, rate limits, deadlines, retry/backoff, cron, dead-letter queue.
- **Durable batch operations** — preview, submit, monitor, and cancel async operator mutations such as reprioritizing queued jobs or moving a backlog to another queue.
- **Progress tracking** — handlers can write structured progress that survives across retries.
- **Web UI (optional)** — `pip install 'awa-pg[ui]'` pulls in the [`awa-cli`](https://pypi.org/project/awa-cli/) wheel, which ships the dashboard binary. Then `python -m awa serve` (or `awa serve` directly) runs a live queue inspector, DLQ triage console, and retry controls on `http://127.0.0.1:3000`. The default `awa-pg` install stays small for workers and producers that don't need the dashboard.

## Migrations

```bash
python -m awa --database-url "$DATABASE_URL" migrate
```

Fresh installs go straight to the queue-storage engine on first migrate. Existing 0.5.x installations should follow [`docs/upgrade-0.5-to-0.6.md`](https://github.com/hardbyte/awa/blob/main/docs/upgrade-0.5-to-0.6.md) for the staged transition.

## Durable batch operations

Batch operations are for operator-scale mutations. They preview a filtered set, persist a control-plane record, and let the maintenance leader apply the mutation in small chunks. Python exposes the generic envelope and helpers for the first two operation kinds:

```python
preview = await client.preview_set_priority(
    1,
    filter={"queue": "default", "state": "available"},
)

operation = await client.set_priority(
    1,
    filter={"queue": "default"},
    submitted_by="ops@example.com",
)

operation = await client.move_queue(
    "escalations",
    priority=1,
    filter={"tag": "incident-123"},
)

active = await client.list_batch_operations(state="running")
await client.cancel_batch_operation(operation["id"])
```

`awa.Client` has the same methods for synchronous scripts. Batch operations affect queued `available` and `scheduled` jobs; running, waiting, terminal, and DLQ rows keep their current attempt state.

## Partitioned FIFO and ordering keys

Queues default to strict FIFO per `(queue, priority)`. Operators can raise `awa.queue_meta.enqueue_shards` on a contended queue to trade strict FIFO for throughput; the contract then becomes **partitioned FIFO** — strict order within each shard, no ordering promised across shards. This is the same kind of decision as choosing SQS Standard over SQS FIFO, raising Kafka partition count, or using Pub/Sub ordering keys.

If your producer enqueues _related_ jobs that must execute in order — events for one customer, steps in one workflow, writes for one account — pass `ordering_key` so all jobs sharing that key land on the same shard:

```python
await client.insert(
    UpdateCustomer(customer_id=42, payload=...),
    queue="customer-updates",
    ordering_key=b"customer-42",
)
```

The key can be `bytes` or `str` (encoded UTF-8). Two enqueues with the same key always pick the same shard regardless of which producer process or batch they came from. At `enqueue_shards = 1` (the default) the key is ignored. See [`docs/adr/025-sharded-enqueue-heads.md`](https://github.com/hardbyte/awa/blob/main/docs/adr/025-sharded-enqueue-heads.md) for the full contract.

## Partitioned queues

A logical queue is the workload name your application thinks in, such as `customer-updates`. A physical queue is the queue name stored in Postgres and claimed by workers. Most workloads use one physical queue. For a very hot workload where partitioned ordering is acceptable, use `PartitionedQueue` to spread one logical queue over several physical queues:

```python
queue = awa.PartitionedQueue("customer-updates", 4)

@client.task(UpdateCustomer, queue=queue.physical_queues[0])
async def update_customer(job):
    ...

await client.start(queue.queue_configs(max_workers_per_partition=16))

await client.insert(
    UpdateCustomer(customer_id=42, payload=...),
    **queue.route_by_key("customer-42"),
)
```

Register the handler once and pass explicit partition configs to `start()`. Python handlers are dispatched by job kind; the queue name on `@client.task` gives `start()` a declared queue to validate.

`route_by_key()` returns `queue` and `ordering_key`, so jobs for the same key pick the same physical queue and keep per-key FIFO. `route_by_index()` returns a round-robin queue for workloads that do not need per-key ordering. The worker `queue_configs()` helper is explicit about `max_workers_per_partition` because each physical queue is configured independently; pass `global_max_workers` to `start()` if you need a logical fleet-wide cap.

`insert_many_copy()` and `enqueue_many_copy()` accept per-job `opts`, so a mixed-partition batch can still use one COPY call:

```python
await client.enqueue_many_copy(
    jobs,
    opts=[queue.route_by_key(job.customer_id) for job in jobs],
)
```

## Documentation

- [Getting started (Python)](https://github.com/hardbyte/awa/blob/main/docs/getting-started-python.md)
- [Configuration](https://github.com/hardbyte/awa/blob/main/docs/configuration.md)
- [Dead Letter Queue](https://github.com/hardbyte/awa/blob/main/docs/dead-letter-queue.md)
- [Architecture](https://github.com/hardbyte/awa/blob/main/docs/architecture.md)
- [Cross-system benchmark comparison](https://github.com/hardbyte/postgresql-job-queue-benchmarking)

## License

Dual-licensed under MIT or Apache-2.0, at your option.

# Python Getting Started

This guide takes you from `pip install` to a job reaching `completed`.

## Mental Model

Before the code, here is the operational model Awa is built around:

- inserting a job writes durable job state to Postgres, so enqueuing can live inside the same transaction as your application write
- workers claim runnable jobs, heartbeat while they execute, and rescue them if the worker dies
- retries, callback waits, and progress checkpoints are persisted in Postgres and exposed as one hydrated job snapshot instead of being held only in memory
- when you debug or operate the system, inspect the job first; the CLI and UI are designed around that read-only inspection path

That means “what happened?” is usually a database inspection question, not a worker-log archaeology exercise.

## Prerequisites

- PostgreSQL running locally or remotely
- Python 3.10+
- A database URL exported as `DATABASE_URL`

Example local URL:

```bash
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
```

## 1. Install Packages

```bash
python -m venv .venv
source .venv/bin/activate

pip install awa-pg
```

## 2. Run Migrations

```bash
python -m awa --database-url "$DATABASE_URL" migrate
```

## 3. Create a Worker

Create `quickstart.py`:

```python
import asyncio
import os
from dataclasses import dataclass

import awa

DATABASE_URL = os.environ["DATABASE_URL"]


@dataclass
class SendEmail:
    to: str
    subject: str


async def main() -> None:
    client = awa.AsyncClient(DATABASE_URL)

    @client.task(SendEmail, queue="email")
    async def handle_email(job):
        print(f"sending email to {job.args.to}: {job.args.subject}")

    await client.start([("email", 2)])

    job = await client.insert(
        SendEmail(to="alice@example.com", subject="Welcome"),
        queue="email",
    )

    await asyncio.sleep(1)

    result = await client.get_job(job.id)
    print(f"job {result.id} state = {result.state}")

    await client.shutdown()


asyncio.run(main())
```

## 4. Run It

```bash
python quickstart.py
```

Expected output is similar to:

```text
sending email to alice@example.com: Welcome
job 1 state = completed
```

## 5. Inspect the Queue

```bash
python -m awa --database-url "$DATABASE_URL" job list --queue email
python -m awa --database-url "$DATABASE_URL" job dump 1
python -m awa --database-url "$DATABASE_URL" job dump-run 1
python -m awa --database-url "$DATABASE_URL" queue stats
```

`job dump` gives you the whole job snapshot as JSON. `job dump-run` focuses on one attempt: the current attempt uses live row data, while historical attempts are reconstructed from the stored `errors[]` history.

## 6. Web UI (optional)

The dashboard ships in a separate wheel so the default `awa-pg` install stays small for workers and producers. Install the `[ui]` extra to bring in the `awa-cli` binary that hosts it:

```bash
pip install 'awa-pg[ui]'
python -m awa --database-url "$DATABASE_URL" serve
# → http://127.0.0.1:3000
```

`python -m awa serve` delegates to the `awa serve` binary (you can also call `awa serve` directly once the extra is installed). The UI is read-only when the database reports `transaction_read_only = on` (e.g. on a replica) or when `--read-only` is passed.

## Useful Variants

- `await client.migrate()` runs migrations from Python instead of the CLI.
- `awa.Client` provides a synchronous API for worker/admin/direct-producer code — all methods are plain (e.g., `client.insert(...)`, `client.migrate()`).
- `client.start()` accepts tuple queue configs for hard-reserved mode and dict configs for weighted mode. See [Configuration reference](configuration.md).
- `awa.PartitionedQueue` helps one hot logical queue use several physical queues while keeping routing deterministic. See [Partitioned queues](configuration.md#partitioned-queues).

## ORM Transaction Bridging

Most applications should keep using their normal database stack for business tables. Use `AsyncClient`/`Client` for workers, admin calls, migrations, and queue-only producers; when a web request already has a transaction, enqueue through `awa.bridge` on that same connection/session.

Install the app database libraries you already use, for example:

```bash
pip install 'sqlalchemy[asyncio]' asyncpg
```

Then enqueue in the same SQLAlchemy transaction as your application write:

```python
from dataclasses import dataclass

from awa.bridge import insert_job
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class SendEmail:
    to: str
    subject: str


async def create_order(session: AsyncSession, order_id: str, email: str) -> int:
    async with session.begin():
        await session.execute(
            text("INSERT INTO orders (id, email) VALUES (:id, :email)"),
            {"id": order_id, "email": email},
        )
        job = await insert_job(
            session,
            SendEmail(to=email, subject="Order confirmed"),
            queue="email",
        )
    return job["id"]
```

The same bridge supports asyncpg, psycopg3, SQLAlchemy, and Django; see [Bridge Adapters](bridge-adapters.md) for driver-specific examples.

### Routing related jobs to the same shard

By default, a queue is strict FIFO per `(queue, priority)`. Operators can opt a contended queue into **partitioned FIFO** by raising `awa.queue_meta.enqueue_shards` — order is then preserved within each shard, but not across shards. If your producer enqueues jobs that must be processed in order (per-customer events, sequential workflow steps), pass `ordering_key` so they all land on one shard:

```python
await client.insert(
    UpdateCustomer(customer_id=42, payload=...),
    queue="customer-updates",
    ordering_key=b"customer-42",  # str also accepted; UTF-8 encoded
)
```

At the default `enqueue_shards = 1` the key is ignored (everything is on shard 0 anyway). See [ADR-025](adr/025-sharded-enqueue-heads.md) for the partitioned-FIFO contract and [`docs/upgrade-0.5-to-0.6.md`](upgrade-0.5-to-0.6.md#raising-enqueue_shards) for the operator-side knob.

### Exporting OpenTelemetry metrics

awa records 20+ metrics (throughput, pickup latency, in-flight jobs, rescues, …) on the Rust side. Python workers enable OTLP export by calling `awa.init_telemetry(...)` once before the worker starts:

```python
import os
import awa

awa.init_telemetry(
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"],   # e.g. http://localhost:4317
    os.environ.get("OTEL_SERVICE_NAME", "my-service"),
)
# ... then build the client and start workers as normal.
```

`init_telemetry` is idempotent; only the first call installs a provider. Call `awa.shutdown_telemetry()` at the end of short-lived scripts to flush pending metrics. See [`awa-python/examples/telemetry.py`](../awa-python/examples/telemetry.py) for a runnable example.

### Distributed tracing

The worker-side execution span automatically continues a trace captured at
enqueue ([ADR-039](adr/039-trace-propagation.md)). Rust producers are captured
ambiently; a Python producer sets the reserved metadata key itself, because the
Rust core cannot see opentelemetry-python's ambient context:

```python
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

carrier: dict[str, str] = {}
TraceContextTextMapPropagator().inject(carrier)  # reads the current span
await client.insert(
    SendEmail(to="user@example.com"),
    metadata={"awa:traceparent": carrier["traceparent"]},
)
```

Inside a handler, `job.traceparent` returns the enqueue-site context so
Python-side instrumentation can continue the same trace:

```python
ctx = TraceContextTextMapPropagator().extract({"traceparent": job.traceparent})
with tracer.start_as_current_span("process-email", context=ctx):
    ...
```

## More Examples

- [Bundled quickstart example](../awa-python/examples/quickstart.py)
- [ETL pipeline example](../examples/python/etl_pipeline.py)
- [Webhook callback example](../examples/python/webhook_payments.py)
- [Deployment guide](deployment.md)
- [Troubleshooting](troubleshooting.md)

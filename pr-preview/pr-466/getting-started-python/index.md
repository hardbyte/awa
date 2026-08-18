> For the complete AWA documentation index, see [`llms.txt`](../llms.txt).

# Python Getting Started

This guide takes you from `uv init` to a job reaching `completed`.

!!! note "Version used in this guide"

    The install commands pin **v0.6.6**, the latest stable release. The canonical example is tested against both that release and the code on `main`; development-only 0.7 surfaces elsewhere on this site are identified by the site banner and stability labels.

## Prerequisites

- PostgreSQL running locally or remotely
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) (it will use or install a compatible Python 3.10+ interpreter)
- A database URL exported as `DATABASE_URL`

Example local URL:

```bash
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
```

## 1. Create a Project

```bash
uv init awa-python-quickstart --bare
cd awa-python-quickstart
uv add awa-pg==0.6.6
```

`uv init` creates a minimal Python project. `uv add` creates and manages the project's virtual environment, records `awa-pg` in `pyproject.toml`, and writes a lockfile—there is no environment activation step.

## 2. Run Migrations

```bash
uv run python -m awa --database-url "$DATABASE_URL" migrate
```

## 3. Create a Worker

Create `quickstart.py`:

```python
"""Awa Python quickstart — a complete runnable example.

Requires: uv add awa-pg==0.6.6
Requires: a running Postgres instance with DATABASE_URL set.

Usage from the repository's awa-python directory:
    DATABASE_URL=postgres://localhost/mydb uv run python examples/quickstart.py
"""

import asyncio
import os
from dataclasses import dataclass

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class SendEmail:
    to: str
    subject: str


async def main():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    # Define a worker
    @client.task(SendEmail, queue="email")
    async def handle_email(job):
        print(f"Sending email to {job.args.to}: {job.args.subject}")

    # Start processing before the first enqueue so a fresh 0.6 database can
    # auto-finalize to the queue-storage engine.
    await client.start([("email", 2)])

    # Insert a job
    job = await client.insert(
        SendEmail(to="alice@example.com", subject="Welcome"),
        queue="email",
    )
    print(f"Inserted job {job.id} (kind={job.kind}, state={job.state})")

    # Verify it reaches a terminal state without relying on a fixed delay.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10
    last_state = job.state
    try:
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out waiting for job {job.id} "
                    f"(last state: {last_state})"
                )

            # get_job is a single read-only query, so cancelling this await
            # cannot leave an application transaction partially committed.
            try:
                result = await asyncio.wait_for(
                    client.get_job(job.id), timeout=remaining
                )
            except asyncio.TimeoutError as error:
                raise TimeoutError(
                    f"timed out waiting for job {job.id} "
                    f"(last state: {last_state})"
                ) from error

            last_state = result.state
            if result.state == awa.JobState.Completed:
                break
            if result.state in (awa.JobState.Failed, awa.JobState.Cancelled):
                raise RuntimeError(
                    f"job {result.id} ended in terminal state {result.state}"
                )
            await asyncio.sleep(min(0.1, max(0, deadline - loop.time())))
    finally:
        await client.shutdown()

    print(f"Job {result.id} state: {result.state}")


if __name__ == "__main__":
    asyncio.run(main())
```

This page includes the repository's canonical example verbatim. CI runs it against PostgreSQL and the docs check compiles its Python syntax.

## 4. Run It

```bash
uv run python quickstart.py
```

You should see the inserted job, the handler output, and the terminal state. The first two lines can swap order because the worker starts before the insert:

```text
Inserted job 1 (kind=send_email, state=available)
Sending email to alice@example.com: Welcome
Job 1 state: completed
```

## What happened?

1. `client.migrate()` made the example standalone; the explicit migration command in step 2 is the deployment-friendly path.
2. Inserting the job wrote durable state to PostgreSQL.
3. The worker claimed it, incremented the attempt, and kept the claim alive while the handler ran.
4. The handler result became durable `completed` state, which the final query read back.

Retries, callback waits, and progress checkpoints follow the same rule: PostgreSQL is the system of record, not worker memory. When you debug a job, inspect its durable snapshot first instead of relying only on worker logs.

## 5. Inspect the Queue

```bash
uv run python -m awa --database-url "$DATABASE_URL" job list --queue email
uv run python -m awa --database-url "$DATABASE_URL" job dump 1
uv run python -m awa --database-url "$DATABASE_URL" job dump-run 1
uv run python -m awa --database-url "$DATABASE_URL" queue stats
```

`job dump` gives you the whole job snapshot as JSON. `job dump-run` focuses on one attempt: the current attempt uses live row data, while historical attempts are reconstructed from the stored `errors[]` history.

## 6. Web UI (optional)

The dashboard ships in a separate wheel so the default `awa-pg` install stays small for workers and producers. Install the `[ui]` extra to bring in the `awa-cli` binary that hosts it:

```bash
uv add 'awa-pg[ui]==0.6.6'
uv run python -m awa --database-url "$DATABASE_URL" serve
# → http://127.0.0.1:3000
```

`uv run python -m awa serve` delegates to the `awa serve` binary (you can also call `awa serve` directly once the extra is installed). The UI is read-only when the database reports `transaction_read_only = on` (e.g. on a replica) or when `--read-only` is passed.

## Useful Variants

- `await client.migrate()` runs migrations from Python instead of the CLI.
- `awa.Client` provides a synchronous API for worker/admin/direct-producer code — all methods are plain (e.g., `client.insert(...)`, `client.migrate()`).
- `client.start()` accepts tuple queue configs for hard-reserved mode and dict configs for weighted mode. See [Configuration reference](../configuration/index.md).
- `awa.PartitionedQueue` helps one hot logical queue use several physical queues while keeping routing deterministic. See [Partitioned queues](../configuration/index.md#partitioned-queues).

## ORM Transaction Bridging

Most applications should keep using their normal database stack for business tables. Use `AsyncClient`/`Client` for workers, admin calls, migrations, and queue-only producers; when a web request already has a transaction, enqueue through `awa.bridge` on that same connection/session.

Install the app database libraries you already use, for example:

```bash
uv add 'sqlalchemy[asyncio]' asyncpg
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

The same bridge supports asyncpg, psycopg3, SQLAlchemy, and Django; see [Bridge Adapters](../bridge-adapters/index.md) for driver-specific examples.

### Routing related jobs to the same shard

By default, a queue is strict FIFO per `(queue, priority)`. Operators can opt a contended queue into **partitioned FIFO** by raising `awa.queue_meta.enqueue_shards` — order is then preserved within each shard, but not across shards. If your producer enqueues jobs that must be processed in order (per-customer events, sequential workflow steps), pass `ordering_key` so they all land on one shard:

```python
await client.insert(
    UpdateCustomer(customer_id=42, payload=...),
    queue="customer-updates",
    ordering_key=b"customer-42",  # str also accepted; UTF-8 encoded
)
```

At the default `enqueue_shards = 1` the key is ignored (everything is on shard 0 anyway). See [ADR-025](../adr/025-sharded-enqueue-heads/index.md) for the partitioned-FIFO contract and [queue configuration](../configuration/index.md#sharding-the-enqueue-head-per-queue) for the operator-side knob.

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

`init_telemetry` is idempotent; only the first call installs a provider. Call `awa.shutdown_telemetry()` at the end of short-lived scripts to flush pending metrics. See [`awa-python/examples/telemetry.py`](https://github.com/hardbyte/awa/blob/main/awa-python/examples/telemetry.py) for a runnable example.

### Distributed tracing

Distributed tracing ([ADR-039](../adr/039-trace-propagation/index.md)) is automatic
when `opentelemetry-api` is importable (install it directly, via your tracing
stack, or with the `awa-pg[otel]` extra) — no configuration:

- **Producers**: `insert` / `insert_many_copy` / `enqueue_many_copy` and the
  `awa.bridge` helpers capture the current OpenTelemetry span's context into
  the reserved `awa:traceparent` metadata key. An explicit
  `metadata={"awa:traceparent": ...}` always wins, and
  `AWA_TRACE_CAPTURE=off` disables ambient capture.
- **Handlers**: the worker attaches the job's trace context as the ambient
  OpenTelemetry context before invoking your handler, so instrumented
  libraries (httpx, requests, SQLAlchemy, ...) and spans you create nest
  into the job's trace with no extra code. `job.traceparent` still exposes
  the stored enqueue-site value for inspection.

Without `opentelemetry` installed, all of this is a cached no-op.

To export awa's own Rust-side spans (`send {queue}`, `job.execute {kind}`)
to your collector, `init_telemetry` now installs a trace pipeline alongside
metrics (pass `traces=False` to keep it metrics-only). With it enabled,
handler-side spans nest under the execution span; without it, they attach to
the enqueue-site context so the trace still connects.

## More Examples

- [Bundled quickstart example](https://github.com/hardbyte/awa/blob/main/awa-python/examples/quickstart.py)
- [ETL pipeline example](https://github.com/hardbyte/awa/blob/main/examples/python/etl_pipeline.py)
- [Webhook callback example](https://github.com/hardbyte/awa/blob/main/examples/python/webhook_payments.py)
- [Deployment guide](../deployment/index.md)
- [Troubleshooting](../troubleshooting/index.md)

# Python Getting Started

This guide takes you from `pip install` to a job reaching `completed`.

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

pip install awa-pg awa-cli
```

## 2. Run Migrations

```bash
awa --database-url "$DATABASE_URL" migrate
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

    @client.worker(SendEmail, queue="email")
    async def handle_email(job):
        print(f"sending email to {job.args.to}: {job.args.subject}")

    client.start([("email", 2)])

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
job 1 state = JobState.Completed
```

## 5. Inspect the Queue

```bash
awa --database-url "$DATABASE_URL" job list --queue email
awa --database-url "$DATABASE_URL" queue stats
awa --database-url "$DATABASE_URL" serve
```

The UI starts on `http://127.0.0.1:3000` by default.

## Useful Variants

- `await client.migrate()` runs migrations from Python instead of the CLI.
- `awa.Client` provides a synchronous API for Django or Flask — all methods are plain (e.g., `client.insert(...)`, `client.migrate()`, `client.transaction()`).
- `client.start()` accepts tuple queue configs for hard-reserved mode and dict configs for weighted mode. See [Configuration reference](configuration.md).

## ORM Transaction Bridging

If you already have a database connection from psycopg3, asyncpg, SQLAlchemy, or Django, you can insert Awa jobs directly within your existing transaction — no separate Awa client connection needed:

```python
from awa.bridge import insert_job, insert_job_sync
```

**asyncpg:**

```python
async with conn.transaction():
    await conn.execute("INSERT INTO orders ...")
    job = await insert_job(conn, SendEmail(to="alice@example.com", subject="Order confirmed"))
```

**Django:**

```python
from django.db import connection, transaction

with transaction.atomic():
    Order.objects.create(...)
    job = insert_job_sync(connection, SendEmail(to="alice@example.com", subject="Order confirmed"))
```

**SQLAlchemy:**

```python
with Session(engine) as session, session.begin():
    session.execute(...)
    job = insert_job_sync(session, SendEmail(to="alice@example.com", subject="Confirmed"))
```

**psycopg3 (sync):**

```python
with psycopg.Connection.connect(dsn) as conn:
    with conn.transaction():
        conn.execute("INSERT INTO orders ...")
        job = insert_job_sync(conn, SendEmail(to="alice@example.com", subject="Confirmed"))
```

Both functions accept the same keyword arguments as `client.insert()`: `kind`, `queue`, `priority`, `max_attempts`, `tags`, `metadata`, `run_at`. The job is committed or rolled back with your transaction — Postgres triggers handle NOTIFY and all other bookkeeping automatically.

## More Examples

- [Bundled quickstart example](../awa-python/examples/quickstart.py)
- [ETL pipeline example](../examples/python/etl_pipeline.py)
- [Webhook callback example](../examples/python/webhook_payments.py)
- [Deployment guide](deployment.md)
- [Troubleshooting](troubleshooting.md)

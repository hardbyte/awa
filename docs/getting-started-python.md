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

    @client.task(SendEmail, queue="email")
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

## More Examples

- [Bundled quickstart example](../awa-python/examples/quickstart.py)
- [ETL pipeline example](../examples/python/etl_pipeline.py)
- [Webhook callback example](../examples/python/webhook_payments.py)
- [Deployment guide](deployment.md)
- [Troubleshooting](troubleshooting.md)

"""Awa Python quickstart — a complete runnable example.

Requires: pip install awa-pg
Requires: a running Postgres instance with DATABASE_URL set.

Usage:
    DATABASE_URL=postgres://localhost/mydb python examples/quickstart.py
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
    client = awa.Client(DATABASE_URL)
    await client.migrate()

    # Define a worker
    @client.worker(SendEmail, queue="email")
    async def handle_email(job):
        print(f"Sending email to {job.args.to}: {job.args.subject}")

    # Insert a job
    job = await client.insert(
        SendEmail(to="alice@example.com", subject="Welcome"),
        queue="email",
    )
    print(f"Inserted job {job.id} (kind={job.kind}, state={job.state})")

    # Start processing
    client.start([("email", 2)])
    await asyncio.sleep(1)
    await client.shutdown()

    # Verify it completed
    result = await client.get_job(job.id)
    print(f"Job {result.id} state: {result.state}")


if __name__ == "__main__":
    asyncio.run(main())

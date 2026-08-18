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
        await client.close()

    print(f"Job {result.id} state: {result.state}")


if __name__ == "__main__":
    asyncio.run(main())

"""Awa sync API example — no async/await required.

For Django views, Flask routes, or any synchronous Python code.

Usage:
    DATABASE_URL=postgres://localhost/mydb python examples/sync_api.py
"""

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


def main():
    client = awa.Client(DATABASE_URL)
    client.migrate()

    # Insert a job synchronously
    job = client.insert(SendEmail(to="bob@example.com", subject="Hello"))
    print(f"Inserted job {job.id} (kind={job.kind})")

    # Admin operations
    stats = client.queue_stats()
    for s in stats:
        print(f"  Queue '{s.queue}': {s.available} available, {s.running} running")

    # Read it back
    fetched = client.get_job(job.id)
    print(f"Job {fetched.id}: state={fetched.state}, args={fetched.args}")

    # Clean up
    client.cancel_sync(job.id)
    print(f"Cancelled job {job.id}")


if __name__ == "__main__":
    main()

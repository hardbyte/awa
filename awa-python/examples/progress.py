"""Awa progress tracking and checkpointing example.

Demonstrates set_progress, update_metadata, flush_progress, and
reading checkpoints on retry.

Usage:
    DATABASE_URL=postgres://localhost/mydb python examples/progress.py
"""

import asyncio
import os
from dataclasses import dataclass

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class BatchImport:
    total_items: int


async def main():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    @client.task(BatchImport, queue="etl")
    async def handle_import(job):
        # On retry, resume from the last checkpoint
        last_id = (job.progress or {}).get("metadata", {}).get("last_id", 0)
        print(f"  Attempt {job.attempt}: starting from last_id={last_id}")

        for i in range(last_id, job.args.total_items):
            # Simulate processing
            job.set_progress(
                int(100 * (i + 1) / job.args.total_items),
                f"Processing item {i + 1}/{job.args.total_items}",
            )
            job.update_metadata({"last_id": i + 1})

            # Simulate a failure partway through on first attempt
            if job.attempt == 1 and i == 2:
                print(f"  Failing at item {i} to demonstrate retry checkpoint")
                raise ValueError("transient failure")

        await job.flush_progress()
        print(f"  Completed all {job.args.total_items} items")

    job = await client.insert(BatchImport(total_items=5), queue="etl")
    print(f"Inserted job {job.id}")

    await client.start([("etl", 1)])
    await asyncio.sleep(3)
    await client.shutdown()

    result = await client.get_job(job.id)
    print(f"Final state: {result.state}")


if __name__ == "__main__":
    asyncio.run(main())

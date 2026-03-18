import asyncio
import os
from dataclasses import dataclass

import awa


@dataclass
class ChaosProbe:
    marker: str


async def main() -> None:
    database_url = os.environ["DATABASE_URL"]
    queue = os.environ["CHAOS_QUEUE"]
    role = os.environ["CHAOS_ROLE"]

    client = awa.Client(database_url)
    await client.migrate()

    @client.worker(ChaosProbe, queue=queue)
    async def handle(job):
        print(
            f"START role={role} pid={os.getpid()} job_id={job.id} attempt={job.attempt}",
            flush=True,
        )
        if role == "hang":
            while True:
                await asyncio.sleep(0.1)

        print(
            f"COMPLETE role={role} pid={os.getpid()} job_id={job.id} attempt={job.attempt}",
            flush=True,
        )
        return None

    client.start([(queue, 1)])
    print(f"READY role={role} pid={os.getpid()}", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

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
        if role == "hang_until_cancel" and job.attempt == 1:
            while not job.is_cancelled():
                await asyncio.sleep(0.05)
            print(
                f"CANCELLED role={role} pid={os.getpid()} job_id={job.id} attempt={job.attempt}",
                flush=True,
            )
            return awa.RetryAfter(seconds=0.05)
        if role == "callback_wait":
            if job.attempt == 1:
                token = await job.register_callback(timeout_seconds=0.3)
                print(
                    f"WAITING role={role} pid={os.getpid()} job_id={job.id} attempt={job.attempt} callback_id={token.id}",
                    flush=True,
                )
                return awa.WaitForCallback()

        print(
            f"COMPLETE role={role} pid={os.getpid()} job_id={job.id} attempt={job.attempt}",
            flush=True,
        )
        return None

    client.start(
        [(queue, 1)],
        leader_election_interval_ms=100,
        heartbeat_interval_ms=50,
        promote_interval_ms=50,
        deadline_rescue_interval_ms=100,
        callback_rescue_interval_ms=100,
    )
    print(f"READY role={role} pid={os.getpid()}", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

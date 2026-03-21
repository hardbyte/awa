import asyncio
import os
from dataclasses import dataclass

import awa


@dataclass
class ChaosProbe:
    marker: str


@dataclass
class SimpleChaosJob:
    seq: int


async def main() -> None:
    database_url = os.environ["DATABASE_URL"]
    queue = os.environ["MIXED_QUEUE"]
    mode = os.environ["MIXED_MODE"]

    client = awa.Client(database_url)
    await client.migrate()

    if mode == "worker_chaos_probe":
        @client.worker(ChaosProbe, queue=queue)
        async def handle(job):
            print(
                f"START mode={mode} pid={os.getpid()} job_id={job.id} marker={job.args.marker}",
                flush=True,
            )
            await asyncio.sleep(0.02)
            print(
                f"COMPLETE mode={mode} pid={os.getpid()} job_id={job.id} marker={job.args.marker}",
                flush=True,
            )
            return None

        client.start(
            [(queue, 1)],
            leader_election_interval_ms=100,
            heartbeat_interval_ms=50,
            promote_interval_ms=50,
        )
        print(f"READY mode={mode} pid={os.getpid()}", flush=True)
        await asyncio.Event().wait()
        return

    if mode == "insert_chaos_probe_batch":
        prefix = os.environ["MIXED_PREFIX"]
        count = int(os.environ["MIXED_COUNT"])
        for idx in range(count):
            await client.insert(ChaosProbe(marker=f"{prefix}-{idx}"), queue=queue)
        print(f"INSERTED mode={mode} prefix={prefix} count={count}", flush=True)
        return

    raise SystemExit(f"unknown MIXED_MODE={mode}")


if __name__ == "__main__":
    asyncio.run(main())

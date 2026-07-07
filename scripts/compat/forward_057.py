"""Forward-compat leg for the 0.5.x line against the newest (finalized)
schema. The supported statement is asymmetric: 0.5.7 *producers* keep
working (the compat routing on `awa.jobs` sends their inserts to the
active engine), while 0.5.7 *workers* are inert — they claim from the
canonical hot table, which is empty on a finalized cluster. This leg
asserts both halves. Runs under the awa-pg 0.5.7 wheel from PyPI."""

import asyncio
import os
import sys
from dataclasses import dataclass

import awa


@dataclass
class LegacyProducerJob:
    marker: str


async def main() -> int:
    client = awa.AsyncClient(os.environ["DATABASE_URL"])

    inserted = await client.insert(
        LegacyProducerJob(marker="from-0.5.7"), queue="compat_legacy"
    )
    print(f"0.5.7 producer inserted job {inserted.id}")

    @client.task(LegacyProducerJob, queue="compat_legacy")
    async def handle(job: LegacyProducerJob):
        # Reaching here would falsify the documented worker inertness.
        print("FAIL: 0.5.7 worker claimed a job on a finalized cluster", file=sys.stderr)
        os._exit(1)

    await client.start([("compat_legacy", 2)])
    await asyncio.sleep(10)
    await client.shutdown()
    print("FORWARD-0.5.7 PASS (producer routed, worker inert)")
    return 0


sys.exit(asyncio.run(main()))

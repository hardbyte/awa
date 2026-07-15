"""A pinned awa-pg 0.6.x wheel runs the full lifecycle on the newest schema."""

import asyncio
import os
import sys
from dataclasses import dataclass

import awa


@dataclass
class CompatJob:
    marker: str


async def main() -> int:
    client = awa.AsyncClient(os.environ["DATABASE_URL"])
    version = os.environ["COMPAT_VERSION"]
    queue = os.environ["COMPAT_QUEUE"]
    done = asyncio.Event()

    @client.task(CompatJob, queue=queue)
    async def handle(job: CompatJob):
        if job.args.marker == "lifecycle":
            done.set()

    inserted = await client.insert(CompatJob(marker="lifecycle"), queue=queue)
    print(f"inserted job {inserted.id}")

    # A parked job to exercise admin cancel against the new schema.
    import datetime

    parked = await client.insert(
        CompatJob(marker="parked"),
        queue=f"{queue}_parked",
        run_at=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
    )

    await client.start([(queue, 2)])
    try:
        await asyncio.wait_for(done.wait(), timeout=60)
    except asyncio.TimeoutError:
        print(f"FAIL: {version} worker never completed the job", file=sys.stderr)
        return 1
    print(f"lifecycle job completed by {version} worker")

    cancelled = await client.cancel(parked.id)
    if cancelled is None or str(cancelled.state) not in ("JobState.CANCELLED", "cancelled"):
        print(f"FAIL: cancel returned {cancelled}", file=sys.stderr)
        return 1
    print("parked job cancelled")

    # Keep the pinned worker alive across several maintenance ticks so the
    # mixed-version compat path exercises old-style ring rotation too.
    await asyncio.sleep(3)

    await client.shutdown()
    print(f"FORWARD-{version} PASS")
    return 0


sys.exit(asyncio.run(main()))

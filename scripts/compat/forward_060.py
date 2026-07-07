"""Forward-compat leg: a pinned awa-pg 0.6.x release runs the full job
lifecycle against the newest schema (enqueue -> claim -> complete, plus
cancel). Runs under the 0.6.0 wheel from PyPI — a real release artifact,
not a source build. Exits non-zero on any contract violation."""

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
    done = asyncio.Event()

    @client.task(CompatJob, queue="compat_forward")
    async def handle(job: CompatJob):
        if job.args.marker == "lifecycle":
            done.set()

    inserted = await client.insert(CompatJob(marker="lifecycle"), queue="compat_forward")
    print(f"inserted job {inserted.id}")

    # A parked job to exercise admin cancel against the new schema.
    import datetime

    parked = await client.insert(
        CompatJob(marker="parked"),
        queue="compat_forward_parked",
        run_at=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
    )

    await client.start([("compat_forward", 2)])
    try:
        await asyncio.wait_for(done.wait(), timeout=60)
    except asyncio.TimeoutError:
        print("FAIL: 0.6.0 worker never completed the job", file=sys.stderr)
        return 1
    print("lifecycle job completed by 0.6.0 worker")

    cancelled = await client.cancel(parked.id)
    if cancelled is None or str(cancelled.state) not in ("JobState.CANCELLED", "cancelled"):
        print(f"FAIL: cancel returned {cancelled}", file=sys.stderr)
        return 1
    print("parked job cancelled")

    await client.shutdown()
    print("FORWARD-0.6.0 PASS")
    return 0


sys.exit(asyncio.run(main()))

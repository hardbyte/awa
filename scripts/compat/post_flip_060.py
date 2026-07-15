"""Assert that a pinned pre-ledger 0.6.x artifact fails after authority flips."""

import asyncio
import datetime
import os
import sys
from dataclasses import dataclass

import awa


@dataclass
class PostFlipJob:
    marker: str


async def main() -> int:
    client = awa.AsyncClient(os.environ["DATABASE_URL"])
    version = os.environ["COMPAT_VERSION"]
    queue = os.environ["COMPAT_QUEUE"]
    parked = await client.insert(
        PostFlipJob(marker="must-not-cancel"),
        queue=queue,
        run_at=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
    )

    try:
        await client.cancel(parked.id)
    except awa.DatabaseError as error:
        if "no partition" not in str(error):
            print(f"FAIL: unexpected post-flip refusal: {error}", file=sys.stderr)
            return 1
        print(f"{version} post-flip operation refused loudly: {error}")
        return 0

    print(f"FAIL: {version} cancel crossed the ledger-authority fence", file=sys.stderr)
    return 1


sys.exit(asyncio.run(main()))

"""Assert that a pinned 0.6.0 artifact fails closed after the v042 authority flip."""

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
    parked = await client.insert(
        PostFlipJob(marker="must-not-cancel"),
        queue="compat_post_flip",
        run_at=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
    )

    try:
        await client.cancel(parked.id)
    except awa.DatabaseError as error:
        if "no partition" not in str(error):
            print(f"FAIL: unexpected post-flip refusal: {error}", file=sys.stderr)
            return 1
        print(f"0.6.0 post-flip operation refused loudly: {error}")
        return 0

    print("FAIL: 0.6.0 cancel crossed the ledger-authority fence", file=sys.stderr)
    return 1


sys.exit(asyncio.run(main()))

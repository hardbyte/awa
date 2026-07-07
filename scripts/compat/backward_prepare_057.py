"""Create a genuinely old (0.5.7-era) schema with live canonical work, as
the backward-guard fixture: the newest binary's `awa migrate` must refuse
it loudly (ADR-037 gate) rather than upgrade over live canonical jobs."""

import asyncio
import os
import sys
from dataclasses import dataclass

import awa


@dataclass
class LegacyJob:
    marker: str


async def main() -> int:
    client = awa.AsyncClient(os.environ["DATABASE_URL"])
    await client.migrate()
    inserted = await client.insert(LegacyJob(marker="stranded"), queue="compat_backward")
    print(f"0.5.7 schema prepared with canonical job {inserted.id}")
    return 0


sys.exit(asyncio.run(main()))

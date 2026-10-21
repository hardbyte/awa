"""A released 0.6 producer and worker that record every failure while the
database is prepared, has CREATE revoked, and is migrated underneath them."""

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import awa


@dataclass
class RehearsalJob:
    seq: int


async def main() -> int:
    state_dir = Path(os.environ["REHEARSAL_DIR"])
    client = awa.AsyncClient(os.environ["RUNTIME_DATABASE_URL"])
    completed: set[int] = set()
    inserted: list[int] = []
    errors: list[dict] = []
    started = time.monotonic()
    last_insert = started
    max_insert_gap = 0.0
    slow_inserts: list[dict] = []

    @client.task(RehearsalJob, queue="rehearsal")
    async def handle(job: RehearsalJob):
        completed.add(job.args.seq)

    await client.start([("rehearsal", 4)])
    seq = 0
    try:
        while not (state_dir / "stop").exists():
            priorities = [2] if not (state_dir / "all-priorities").exists() else [1, 2, 3, 4]
            priority = priorities[seq % len(priorities)]
            try:
                attempt_started = time.monotonic()
                await client.insert(RehearsalJob(seq=seq), queue="rehearsal", priority=priority)
                now = time.monotonic()
                if now - attempt_started > 0.25:
                    slow_inserts.append(
                        {"at": round(attempt_started - started, 2), "took": round(now - attempt_started, 2)}
                    )
                max_insert_gap = max(max_insert_gap, now - last_insert)
                last_insert = now
                inserted.append(seq)
            except Exception as error:  # noqa: BLE001 - every failure is the finding
                errors.append(
                    {
                        "at": round(time.monotonic() - started, 2),
                        "priority": priority,
                        "error": f"{type(error).__name__}: {error}"[:300],
                    }
                )
            seq += 1
            await asyncio.sleep(0.02)

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not set(inserted) <= completed:
            await asyncio.sleep(0.25)
    finally:
        await client.shutdown()
        await client.close()

    summary = {
        "attempted": seq,
        "inserted": len(inserted),
        "completed": len(set(inserted) & completed),
        "errors": len(errors),
        "first_errors": errors[:3],
        "error_window_s": [errors[0]["at"], errors[-1]["at"]] if errors else None,
        "max_insert_gap_s": round(max_insert_gap, 3),
        "slow_inserts": slow_inserts[:10],
    }
    print(json.dumps(summary))
    return 0


sys.exit(asyncio.run(main()))

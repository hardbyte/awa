"""Run the demo app's Awa workers."""

import asyncio

from .shared import (
    EMAIL_QUEUE,
    OPS_QUEUE,
    PAYMENTS_QUEUE,
    create_client,
    ensure_app_schema,
    register_workers,
)


async def main() -> None:
    client = create_client()
    await client.migrate()
    await ensure_app_schema(client)
    register_workers(client)
    client.start(
        [(EMAIL_QUEUE, 2), (OPS_QUEUE, 1), (PAYMENTS_QUEUE, 1)],
        leader_election_interval_ms=1000,
        heartbeat_interval_ms=1000,
        promote_interval_ms=1000,
    )
    try:
        await asyncio.Event().wait()
    finally:
        await client.shutdown(timeout_ms=5000)


if __name__ == "__main__":
    asyncio.run(main())

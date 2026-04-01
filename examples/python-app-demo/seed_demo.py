"""Seed the demo app and Awa UI with realistic domain data."""

import argparse
import asyncio

import awa

from demo_app.shared import (
    CACHE_QUEUE,
    CRON_NAME,
    EMAIL_QUEUE,
    GenerateRevenueReport,
    OPS_QUEUE,
    PAYMENTS_QUEUE,
    REPORTS_QUEUE,
    SEED_SCALE_PRESETS,
    WarmProductCache,
    clear_demo_data,
    create_checkout,
    create_client,
    ensure_app_schema,
    register_workers,
    seed_available_cache_jobs,
    seed_failed_syncs,
    seed_pending_payments,
    seed_scheduled_reports,
)


async def wait_for_state(
    client: awa.AsyncClient,
    job_id: int,
    expected_state: awa.JobState,
    *,
    timeout_seconds: float = 12.0,
) -> awa.Job:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        job = await client.get_job(job_id)
        if job.state == expected_state:
            return job
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(
                f"job {job_id} never reached {expected_state}; last state was {job.state}"
            )
        await asyncio.sleep(0.1)


async def wait_for_many(
    client: awa.AsyncClient,
    job_ids: list[int],
    expected_state: awa.JobState,
    *,
    timeout_seconds: float = 12.0,
) -> None:
    for job_id in job_ids:
        await wait_for_state(
            client,
            job_id,
            expected_state,
            timeout_seconds=timeout_seconds,
        )


async def wait_for_cron_sync(client: awa.AsyncClient, name: str, *, timeout_seconds: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        tx = await client.transaction()
        row = await tx.fetch_optional(
            "SELECT 1 FROM awa.cron_jobs WHERE name = $1",
            name,
        )
        await tx.commit()
        if row is not None:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"cron job {name} was not synced")
        await asyncio.sleep(0.1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed the demo FastAPI app and Awa UI.")
    parser.add_argument(
        "--scale",
        choices=sorted(SEED_SCALE_PRESETS.keys()),
        default="medium",
        help="Dataset size preset. Use 'medium' for video and 'large' for a denser dashboard.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    preset = SEED_SCALE_PRESETS[args.scale]

    client = create_client()
    await client.migrate()
    await ensure_app_schema(client)
    await clear_demo_data(client)

    callback_ids: list[str] = []
    register_workers(client, callback_ids)

    receipt_job_ids: list[int] = []
    for i in range(preset["completed_orders"]):
        checkout = await create_checkout(
            client,
            customer_email=f"customer{i + 1}@example.com",
            total_cents=1499 + (i * 200),
            order_id=f"ord_demo_{i + 1:03d}",
        )
        receipt_job_ids.append(int(checkout["confirmation_job_id"]))

    failed_ids = await seed_failed_syncs(client, preset["failed_syncs"])
    waiting_ids = await seed_pending_payments(client, preset["waiting_payments"])
    await seed_available_cache_jobs(client, preset["available_cache_jobs"])
    await seed_scheduled_reports(client, preset["scheduled_reports"])

    hero_available = await client.insert(
        WarmProductCache(slug="/"),
        queue=CACHE_QUEUE,
        tags=["demo", "hero", "cache"],
        metadata={"app": "demo-shop", "hero": True},
    )
    hero_scheduled = await client.insert(
        GenerateRevenueReport(report_name="weekly-gross-margin"),
        queue=REPORTS_QUEUE,
        run_at="2099-01-01T00:00:00+00:00",
        tags=["demo", "hero", "reports"],
        metadata={"app": "demo-shop", "hero": True},
    )

    await client.start(
        [(EMAIL_QUEUE, 2), (OPS_QUEUE, 1), (PAYMENTS_QUEUE, 1)],
        leader_election_interval_ms=100,
        heartbeat_interval_ms=100,
        promote_interval_ms=100,
    )

    await wait_for_many(client, receipt_job_ids, awa.JobState.Completed)
    await wait_for_many(client, failed_ids, awa.JobState.Failed)
    await wait_for_many(client, waiting_ids, awa.JobState.WaitingExternal)
    await wait_for_cron_sync(client, CRON_NAME)
    await client.shutdown()

    print("\nDemo app data ready")
    print("===================")
    print(f"scale preset:       {args.scale}")
    print(
        "queues:             "
        f"{EMAIL_QUEUE}, {PAYMENTS_QUEUE}, {OPS_QUEUE}, {CACHE_QUEUE}, {REPORTS_QUEUE}"
    )
    print(
        "counts:             "
        f"completed={len(receipt_job_ids)} "
        f"failed={len(failed_ids)} "
        f"waiting={len(waiting_ids)} "
        f"available={preset['available_cache_jobs'] + 1} "
        f"scheduled={preset['scheduled_reports'] + 1}"
    )
    print(f"hero available:     id={hero_available.id} state={hero_available.state}")
    print(f"hero scheduled:     id={hero_scheduled.id} state={hero_scheduled.state}")
    print(f"callback id:        {callback_ids[0] if callback_ids else 'missing'}")
    print(f"cron job:           {CRON_NAME}")
    print("\nSuggested next steps:")
    print("  (cd ../../awa-ui/frontend && npm install && npm run build)  # once per checkout")
    print("  uv run awa --database-url $DATABASE_URL serve")
    print("  uv run uvicorn demo_app.app:app --reload")


if __name__ == "__main__":
    asyncio.run(main())

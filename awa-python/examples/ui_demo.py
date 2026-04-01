"""Seed a realistic Awa admin/UI demo dataset.

This is a generic UI seed script.
For a more application-shaped example, use `examples/python-app-demo/`.

This example leaves behind a mix of:
- completed jobs
- failed jobs with preserved progress
- waiting_external jobs with callback IDs
- scheduled jobs
- available backlog jobs
- a cron schedule

Usage:
    DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
        python examples/ui_demo.py

For recording or screenshots:
    DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
        python examples/ui_demo.py --scale medium
"""

import asyncio
import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

SCALE_PRESETS = {
    "small": {
        "completed_bulk": 2,
        "failed_bulk": 1,
        "waiting_bulk": 1,
        "available_bulk": 6,
        "scheduled_bulk": 4,
    },
    "medium": {
        "completed_bulk": 12,
        "failed_bulk": 4,
        "waiting_bulk": 3,
        "available_bulk": 240,
        "scheduled_bulk": 120,
    },
    "large": {
        "completed_bulk": 24,
        "failed_bulk": 8,
        "waiting_bulk": 6,
        "available_bulk": 2000,
        "scheduled_bulk": 1000,
    },
}


@dataclass
class SendEmail:
    to: str
    subject: str


@dataclass
class ImportCustomers:
    source: str
    total_rows: int


@dataclass
class AwaitApproval:
    invoice_id: str


@dataclass
class GenerateReport:
    name: str


@dataclass
class RefreshCache:
    scope: str


async def wait_for_state(
    client: awa.AsyncClient,
    job_id: int,
    expected_state: awa.JobState,
    *,
    timeout_seconds: float = 8.0,
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


async def wait_for_cron_sync(client: awa.AsyncClient, name: str, *, timeout_seconds: float = 8.0) -> None:
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
            raise TimeoutError(f"cron job {name} was not synced to awa.cron_jobs")
        await asyncio.sleep(0.1)


async def wait_for_many(
    client: awa.AsyncClient,
    job_ids: list[int],
    expected_state: awa.JobState,
    *,
    timeout_seconds: float = 12.0,
) -> list[awa.Job]:
    jobs = []
    for job_id in job_ids:
        jobs.append(
            await wait_for_state(
                client,
                job_id,
                expected_state,
                timeout_seconds=timeout_seconds,
            )
        )
    return jobs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed a realistic Awa UI demo dataset.")
    parser.add_argument(
        "--scale",
        choices=sorted(SCALE_PRESETS.keys()),
        default="medium",
        help="Dataset size preset. Use 'medium' for hundreds or 'large' for thousands.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    preset = SCALE_PRESETS[args.scale]

    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    tx = await client.transaction()
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'ui_demo_%'")
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'intro_%'")
    await tx.execute("DELETE FROM awa.cron_jobs WHERE name LIKE 'ui_demo_%'")
    await tx.execute("DELETE FROM awa.cron_jobs WHERE name LIKE 'intro_%'")
    await tx.commit()

    callback_ids: list[str] = []

    @client.task(SendEmail, queue="ui_demo_email")
    async def handle_email(job):
        print(f"Sent welcome email to {job.args.to}")

    @client.task(ImportCustomers, queue="ui_demo_etl")
    async def handle_import(job):
        job.set_progress(35, "Validated 3 of 8 rows")
        job.update_metadata(
            {
                "source": job.args.source,
                "last_row": 3,
                "failure_stage": "validation",
            }
        )
        await job.flush_progress()
        raise awa.TerminalError("missing required column: email")

    @client.task(AwaitApproval, queue="ui_demo_callbacks")
    async def handle_approval(job):
        job.set_progress(90, "Waiting for external approval")
        job.update_metadata({"invoice_id": job.args.invoice_id, "provider": "stripe"})
        await job.flush_progress()
        token = await job.register_callback(timeout_seconds=3600)
        callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    client.periodic(
        name="ui_demo_daily_digest",
        cron_expr="0 9 * * *",
        args_type=SendEmail,
        args=SendEmail(to="ops@example.com", subject="Daily digest"),
        timezone="Pacific/Auckland",
        queue="ui_demo_email",
        tags=["demo", "cron"],
        metadata={"demo": "ui-demo"},
    )

    completed_ids: list[int] = []
    failed_ids: list[int] = []
    waiting_ids: list[int] = []

    for i in range(preset["completed_bulk"]):
        job = await client.insert(
            SendEmail(
                to=f"user{i + 1}@example.com",
                subject=f"Product update #{i + 1}",
            ),
            queue="ui_demo_email",
            tags=["demo", "completed"],
            metadata={"demo": "ui-demo", "campaign": "launch"},
        )
        completed_ids.append(job.id)

    for i in range(preset["failed_bulk"]):
        job = await client.insert(
            ImportCustomers(
                source=f"customers-batch-{i + 1}.csv",
                total_rows=8 + i,
            ),
            queue="ui_demo_etl",
            tags=["demo", "failed"],
            metadata={"demo": "ui-demo", "team": "ops"},
        )
        failed_ids.append(job.id)

    for i in range(preset["waiting_bulk"]):
        job = await client.insert(
            AwaitApproval(invoice_id=f"INV-2026-{100 + i}"),
            queue="ui_demo_callbacks",
            tags=["demo", "callback"],
            metadata={"demo": "ui-demo"},
        )
        waiting_ids.append(job.id)

    for i in range(preset["available_bulk"]):
        await client.insert(
            RefreshCache(scope=f"page-{i + 1}"),
            queue="ui_demo_backlog",
            tags=["demo", "available"],
            metadata={"demo": "ui-demo"},
        )

    for i in range(preset["scheduled_bulk"]):
        await client.insert(
            GenerateReport(name=f"scheduled-report-{i + 1}"),
            queue="ui_demo_reports",
            run_at=(datetime.now(timezone.utc) + timedelta(minutes=30 + (i * 10))).isoformat(),
            tags=["demo", "scheduled"],
            metadata={"demo": "ui-demo"},
        )

    completed_job = await client.insert(
        SendEmail(to="alice@example.com", subject="Welcome to Awa"),
        queue="ui_demo_email",
        tags=["demo", "completed"],
        metadata={"demo": "ui-demo", "hero": True},
    )
    failed_job = await client.insert(
        ImportCustomers(source="customers.csv", total_rows=8),
        queue="ui_demo_etl",
        tags=["demo", "failed"],
        metadata={"demo": "ui-demo", "hero": True, "team": "ops"},
    )
    waiting_job = await client.insert(
        AwaitApproval(invoice_id="INV-2026-001"),
        queue="ui_demo_callbacks",
        tags=["demo", "callback"],
        metadata={"demo": "ui-demo", "hero": True},
    )
    available_job = await client.insert(
        RefreshCache(scope="homepage"),
        queue="ui_demo_backlog",
        tags=["demo", "available"],
        metadata={"demo": "ui-demo", "hero": True},
    )
    scheduled_job = await client.insert(
        GenerateReport(name="weekly-revenue"),
        queue="ui_demo_reports",
        run_at=(datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(),
        tags=["demo", "scheduled"],
        metadata={"demo": "ui-demo", "hero": True},
    )
    completed_ids.append(completed_job.id)
    failed_ids.append(failed_job.id)
    waiting_ids.append(waiting_job.id)

    await client.start(
        [("ui_demo_email", 2), ("ui_demo_etl", 1), ("ui_demo_callbacks", 1)],
        leader_election_interval_ms=100,
        heartbeat_interval_ms=100,
        promote_interval_ms=100,
    )

    await wait_for_many(client, completed_ids, awa.JobState.Completed)
    await wait_for_many(client, failed_ids, awa.JobState.Failed)
    await wait_for_many(client, waiting_ids, awa.JobState.WaitingExternal)
    await wait_for_cron_sync(client, "ui_demo_daily_digest")
    await client.shutdown()

    completed = await client.get_job(completed_job.id)
    failed = await client.get_job(failed_job.id)
    waiting = await client.get_job(waiting_job.id)
    refreshed_available = await client.get_job(available_job.id)
    refreshed_scheduled = await client.get_job(scheduled_job.id)

    print("\nDemo data ready")
    print("================")
    print(f"scale preset:         {args.scale}")
    print(
        "counts:               "
        f"completed={len(completed_ids)} "
        f"failed={len(failed_ids)} "
        f"waiting={len(waiting_ids)} "
        f"available={preset['available_bulk'] + 1} "
        f"scheduled={preset['scheduled_bulk'] + 1}"
    )
    print(f"completed job:        id={completed.id} state={completed.state}")
    print(f"failed job:           id={failed.id} state={failed.state}")
    print(f"waiting callback job: id={waiting.id} state={waiting.state}")
    print(f"available job:        id={refreshed_available.id} state={refreshed_available.state}")
    print(f"scheduled job:        id={refreshed_scheduled.id} state={refreshed_scheduled.state}")
    print(f"callback id:          {callback_ids[0] if callback_ids else 'missing'}")
    print("cron job:             ui_demo_daily_digest")
    print("\nSuggested next step:")
    print("  (cd ../../awa-ui/frontend && npm install && npm run build)  # once per checkout")
    print("  awa --database-url $DATABASE_URL serve")
    print("  open http://127.0.0.1:3000")


if __name__ == "__main__":
    asyncio.run(main())

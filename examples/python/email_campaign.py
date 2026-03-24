"""
Email Campaign with Bulk Insert, Priority, and Admin Operations

Demonstrates:
- Fast bulk job insertion with COPY protocol
- Priority levels (transactional emails before marketing)
- Simulated failures with retry
- Admin operations: retry_failed, queue_stats
- Periodic cron schedule

In production you'd split this into:
- A marketing service that bulk-enqueues campaign emails (the insert_many_copy
  section) — triggered by an admin action or scheduled campaign
- A worker process: client.start([("email", 10)]) as a separate deployment
- An admin/ops script or the web UI for retry_failed, queue_stats, drain
- The periodic schedule runs on the worker (leader evaluates it)

Run (single-process demo):
    cd awa-python
    DATABASE_URL=postgres://... .venv/bin/python ../examples/python/email_campaign.py
"""

import asyncio
import os
import random
from dataclasses import dataclass

import awa


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


@dataclass
class SendEmail:
    to: str
    subject: str
    template: str
    campaign_id: str = ""


async def main():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    print("AWA Email Campaign Example\n")

    sent_count = 0

    @client.worker(SendEmail, queue="email")
    async def handle_email(job):
        nonlocal sent_count
        await asyncio.sleep(0.01)
        # Simulate 5% failure rate
        if random.random() < 0.05:
            raise Exception(f"SMTP refused for {job.args['to']}")
        sent_count += 1

    # ── Periodic schedule ───────────────────────────────────────

    client.periodic(
        name="email_health",
        cron_expr="0 * * * *",
        args_type=SendEmail,
        args=SendEmail(to="", subject="noop", template="noop"),
        queue="email",
        tags=["internal"],
    )

    # ── Bulk enqueue marketing campaign (fast COPY insert) ──────

    campaign_id = "spring_2026"
    recipients = [f"user{i}@example.com" for i in range(200)]

    print(f"Bulk inserting {len(recipients)} campaign emails (COPY protocol)...")
    campaign_jobs = [
        SendEmail(to=email, subject="Spring Sale", template="promo", campaign_id=campaign_id)
        for email in recipients
    ]
    await client.insert_many_copy(
        campaign_jobs,
        queue="email",
        priority=3,  # low priority — marketing
        tags=["marketing", campaign_id],
        metadata={"campaign": campaign_id},
    )
    print(f"  ✓ {len(recipients)} marketing emails enqueued\n")

    # ── High-priority transactional emails ──────────────────────

    print("Enqueuing 5 transactional emails (priority 1)...")
    for i in range(5):
        await client.insert(
            SendEmail(to=f"vip{i}@example.com", subject="Order Confirmed", template="order"),
            queue="email",
            priority=1,  # processed before marketing
            tags=["transactional"],
        )
    print("  ✓ Transactional emails will be processed first\n")

    # ── Process ─────────────────────────────────────────────────

    client.start([("email", 5)], leader_election_interval_ms=500)

    print("Processing...")
    for tick in range(60):
        await asyncio.sleep(1)
        stats = await client.queue_stats()
        eq = next((s for s in stats if s.queue == "email"), None)
        if eq:
            avail, running, failed = eq.available, eq.running, eq.failed
            if tick % 5 == 0:
                print(f"  avail={avail} running={running} failed={failed}")
            if avail == 0 and running == 0:
                break

    # ── Retry failures ──────────────────────────────────────────

    stats = await client.queue_stats()
    eq = next((s for s in stats if s.queue == "email"), None)
    fail_count = eq.failed if eq else 0

    print(f"\nFirst pass: {sent_count} sent, {fail_count} failed")

    if fail_count > 0:
        retried = await client.retry_failed(queue="email")
        print(f"Retrying {len(retried)} failed emails...")
        await asyncio.sleep(5)

    print(f"Final: {sent_count} total sent")
    health = await client.health_check()
    print(f"Health: leader={health.leader} heartbeat={health.heartbeat_alive}")

    await client.shutdown()
    print("✓ Done")


if __name__ == "__main__":
    asyncio.run(main())

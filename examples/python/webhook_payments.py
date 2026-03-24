"""
Webhook Payment Processing

Demonstrates:
- Multiple job types across different queues
- Priority-based dispatch (high-value payments first)
- Follow-up job enqueue from within a handler
- Queue stats and health monitoring
- Metadata for correlation and audit

In production you'd split this into:
- A checkout API (FastAPI/Django) that calls client.insert(ChargeCustomer(...))
  when a customer clicks "Pay" — this is the "Enqueue payment jobs" section
- A worker process: client.start([("payments", 4), ("notifications", 2)])
  as a separate deployment
- The charge handler enqueues receipt jobs — picked up by the notifications
  worker (same or different process on that queue)

Run (single-process demo):
    cd awa-python
    DATABASE_URL=postgres://... .venv/bin/python ../examples/python/webhook_payments.py
"""

import asyncio
import os
import uuid
from dataclasses import dataclass, field

import awa


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


# ── Job types ───────────────────────────────────────────────────────


@dataclass
class ChargeCustomer:
    customer_id: str
    amount_cents: int
    currency: str = "nzd"
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4())[:8])


@dataclass
class SendReceipt:
    customer_id: str
    charge_id: str
    amount_cents: int


# ── Main ────────────────────────────────────────────────────────────


async def main():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    print("AWA Webhook Payments Example\n")

    # ── Register workers ────────────────────────────────────────

    @client.worker(ChargeCustomer, queue="payments")
    async def handle_charge(job):
        customer_id = job.args.customer_id
        amount = job.args.amount_cents
        idem_key = job.args.idempotency_key

        job.set_progress(25, f"Initiating charge for {customer_id}")
        await job.flush_progress()

        # Simulate calling a payment gateway
        charge_id = f"ch_{idem_key}"
        await asyncio.sleep(0.05)

        job.set_progress(75, f"Charge {charge_id} succeeded")
        job.update_metadata({"charge_id": charge_id, "gateway": "simulated"})

        # Enqueue a follow-up receipt job from within the handler
        await client.insert(
            SendReceipt(
                customer_id=customer_id,
                charge_id=charge_id,
                amount_cents=amount,
            ),
            queue="notifications",
            tags=["receipt", customer_id],
        )

        print(f"  ✓ Charged {customer_id}: ${amount/100:.2f} → {charge_id}")

    @client.worker(SendReceipt, queue="notifications")
    async def handle_receipt(job):
        customer_id = job.args.customer_id
        charge_id = job.args.charge_id
        amount = job.args.amount_cents
        await asyncio.sleep(0.02)
        print(f"  ✉ Receipt sent to {customer_id}: ${amount/100:.2f} ({charge_id})")

    # ── Enqueue payment jobs ────────────────────────────────────

    customers = [
        ("cust_alice", 4999),
        ("cust_bob", 12500),
        ("cust_carol", 750),
        ("cust_dave", 99900),
    ]

    for customer_id, amount in customers:
        job = await client.insert(
            ChargeCustomer(customer_id=customer_id, amount_cents=amount),
            queue="payments",
            priority=1 if amount > 10000 else 2,
            tags=["payment", customer_id],
            metadata={"source": "checkout"},
        )
        print(f"  Enqueued charge for {customer_id}: ${amount/100:.2f} (job #{job.id})")

    print()

    # ── Start workers and monitor ───────────────────────────────

    client.start(
        [("payments", 2), ("notifications", 1)],
        leader_election_interval_ms=500,
    )

    for _ in range(30):
        await asyncio.sleep(1)
        stats = await client.queue_stats()
        payments = next((s for s in stats if s.queue == "payments"), None)
        notifs = next((s for s in stats if s.queue == "notifications"), None)

        p_busy = (payments.available + payments.running) if payments else 0
        n_busy = (notifs.available + notifs.running) if notifs else 0

        if p_busy == 0 and n_busy == 0:
            print(f"\n✓ All payments processed and receipts sent")
            break

    print("\nQueue stats:")
    for stat in await client.queue_stats():
        if stat.queue in ("payments", "notifications"):
            print(f"  {stat.queue}: failed={stat.failed} completed/hr={stat.completed_last_hour}")

    await client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

"""Shared domain and Awa setup for the demo app."""

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

ORDERS_TABLE = "demo_app_orders"

EMAIL_QUEUE = "store_email"
PAYMENTS_QUEUE = "store_payments"
OPS_QUEUE = "store_ops"
CACHE_QUEUE = "store_cache"
REPORTS_QUEUE = "store_reports"
CRON_NAME = "store_daily_revenue_digest"

SEED_SCALE_PRESETS = {
    "small": {
        "completed_orders": 4,
        "failed_syncs": 2,
        "waiting_payments": 2,
        "available_cache_jobs": 24,
        "scheduled_reports": 12,
    },
    "medium": {
        "completed_orders": 14,
        "failed_syncs": 4,
        "waiting_payments": 3,
        "available_cache_jobs": 180,
        "scheduled_reports": 90,
    },
    "large": {
        "completed_orders": 28,
        "failed_syncs": 8,
        "waiting_payments": 6,
        "available_cache_jobs": 1200,
        "scheduled_reports": 600,
    },
}


@dataclass
class SendOrderConfirmationEmail:
    order_id: str
    customer_email: str


@dataclass
class CapturePayment:
    order_id: str
    payment_ref: str
    total_cents: int


@dataclass
class SyncInventoryBatch:
    supplier: str
    total_items: int


@dataclass
class WarmProductCache:
    slug: str


@dataclass
class GenerateRevenueReport:
    report_name: str


def create_client() -> awa.Client:
    return awa.Client(DATABASE_URL)


async def ensure_app_schema(client: awa.Client) -> None:
    tx = await client.transaction()
    await tx.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
            order_id TEXT PRIMARY KEY,
            customer_email TEXT NOT NULL,
            total_cents INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await tx.commit()


async def clear_demo_data(client: awa.Client) -> None:
    tx = await client.transaction()
    await tx.execute(f"DELETE FROM {ORDERS_TABLE}")
    await tx.execute("DELETE FROM awa.jobs WHERE queue LIKE 'store_%'")
    await tx.execute("DELETE FROM awa.cron_jobs WHERE name = $1", CRON_NAME)
    await tx.commit()


def register_workers(client: awa.Client, callback_ids: list[str] | None = None) -> None:
    @client.worker(SendOrderConfirmationEmail, queue=EMAIL_QUEUE)
    async def handle_confirmation(job):
        print(
            f"Sent order confirmation for {job.args.order_id} to {job.args.customer_email}"
        )

    @client.worker(SyncInventoryBatch, queue=OPS_QUEUE)
    async def handle_inventory_sync(job):
        job.set_progress(42, "Validated 42% of supplier feed")
        job.update_metadata(
            {
                "supplier": job.args.supplier,
                "last_sku": "SKU-0042",
                "failure_stage": "price-validation",
            }
        )
        await job.flush_progress()
        raise awa.TerminalError("supplier feed missing wholesale_price")

    @client.worker(CapturePayment, queue=PAYMENTS_QUEUE)
    async def handle_payment(job):
        job.set_progress(95, "Waiting for payment provider callback")
        job.update_metadata(
            {
                "order_id": job.args.order_id,
                "payment_ref": job.args.payment_ref,
                "provider": "stripe",
            }
        )
        await job.flush_progress()
        token = await job.register_callback(timeout_seconds=3600)
        if callback_ids is not None:
            callback_ids.append(token.id)
        return awa.WaitForCallback(token)

    client.periodic(
        name=CRON_NAME,
        cron_expr="0 9 * * *",
        args_type=GenerateRevenueReport,
        args=GenerateRevenueReport(report_name="daily-revenue-digest"),
        timezone="Pacific/Auckland",
        queue=REPORTS_QUEUE,
        tags=["demo", "cron", "reports"],
        metadata={"app": "demo-shop"},
    )


async def create_checkout(
    client: awa.Client,
    *,
    customer_email: str,
    total_cents: int,
    order_id: str | None = None,
) -> dict[str, object]:
    resolved_order_id = order_id or f"ord_{uuid4().hex[:10]}"
    async with await client.transaction() as tx:
        inserted = await tx.fetch_optional(
            f"""
            INSERT INTO {ORDERS_TABLE} (order_id, customer_email, total_cents, status)
            VALUES ($1, $2, $3, 'submitted')
            ON CONFLICT (order_id) DO NOTHING
            RETURNING order_id
            """,
            resolved_order_id,
            customer_email,
            total_cents,
        )
        if inserted is None:
            return {
                "order_id": resolved_order_id,
                "confirmation_job_id": None,
                "duplicate": True,
            }
        confirmation_job = await tx.insert(
            SendOrderConfirmationEmail(
                order_id=resolved_order_id,
                customer_email=customer_email,
            ),
            queue=EMAIL_QUEUE,
            tags=["demo", "order-confirmation"],
            metadata={"app": "demo-shop", "order_id": resolved_order_id},
        )
    return {
        "order_id": resolved_order_id,
        "confirmation_job_id": confirmation_job.id,
        "duplicate": False,
    }


async def list_recent_orders(client: awa.Client, limit: int = 20) -> list[dict[str, object]]:
    tx = await client.transaction()
    rows = await tx.fetch_all(
        f"""
        SELECT order_id, customer_email, total_cents, status, created_at
        FROM {ORDERS_TABLE}
        ORDER BY created_at DESC
        LIMIT $1
        """,
        limit,
    )
    await tx.commit()
    return rows


async def seed_pending_payments(client: awa.Client, count: int) -> list[int]:
    ids: list[int] = []
    for i in range(count):
        job = await client.insert(
            CapturePayment(
                order_id=f"pay_{i + 1:03d}",
                payment_ref=f"pi_demo_{i + 1:03d}",
                total_cents=1999 + (i * 250),
            ),
            queue=PAYMENTS_QUEUE,
            tags=["demo", "payments"],
            metadata={"app": "demo-shop"},
        )
        ids.append(job.id)
    return ids


async def seed_failed_syncs(client: awa.Client, count: int) -> list[int]:
    ids: list[int] = []
    for i in range(count):
        job = await client.insert(
            SyncInventoryBatch(
                supplier=f"supplier-{i + 1}",
                total_items=100 + (i * 20),
            ),
            queue=OPS_QUEUE,
            tags=["demo", "inventory"],
            metadata={"app": "demo-shop", "team": "ops"},
        )
        ids.append(job.id)
    return ids


async def seed_available_cache_jobs(client: awa.Client, count: int) -> None:
    jobs = [
        WarmProductCache(slug=f"/products/demo-{i + 1}")
        for i in range(count)
    ]
    await client.insert_many_copy(
        jobs,
        queue=CACHE_QUEUE,
        tags=["demo", "cache"],
        metadata={"app": "demo-shop"},
    )


async def seed_scheduled_reports(client: awa.Client, count: int) -> None:
    for i in range(count):
        await client.insert(
            GenerateRevenueReport(report_name=f"scheduled-revenue-report-{i + 1}"),
            queue=REPORTS_QUEUE,
            run_at=(datetime.now(timezone.utc) + timedelta(minutes=30 + (i * 5))).isoformat(),
            tags=["demo", "reports"],
            metadata={"app": "demo-shop"},
        )

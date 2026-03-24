"""
ETL Pipeline with Progress Tracking and Checkpointing

Demonstrates:
- Transactional enqueue (all-or-nothing batch of jobs)
- Structured progress with percent, message, and metadata
- Checkpoint/resume: a crashed job resumes from the last processed row
- Periodic scheduling for daily runs
- Queue stats monitoring

In production you'd split this into:
- A scheduler/API that enqueues ImportTable jobs (the "Enqueue jobs" section)
- A worker process running `client.start([("etl", 3)])` as a separate
  deployment (e.g. k8s Deployment with replicas=2)
- The periodic schedule is registered on the worker and evaluated by
  whichever instance wins leader election
- Monitor via the web UI: `awa serve --database-url $DATABASE_URL`

Run (single-process demo):
    cd awa-python
    DATABASE_URL=postgres://... .venv/bin/python ../examples/python/etl_pipeline.py
"""

import asyncio
import os
from dataclasses import dataclass

import awa


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)


# ── Job types ───────────────────────────────────────────────────────


@dataclass
class ImportTable:
    """Import rows from a source table into the warehouse."""
    source_table: str
    batch_size: int = 1000


@dataclass
class AggregateMetrics:
    """Roll up imported data into summary metrics."""
    date: str
    tables: list  # list of table names to aggregate


# ── Simulated data source ───────────────────────────────────────────


TOTAL_ROWS = {"users": 5000, "orders": 12000, "events": 50000}


def fetch_rows(table: str, offset: int, limit: int) -> list[dict]:
    total = TOTAL_ROWS.get(table, 1000)
    if offset >= total:
        return []
    end = min(offset + limit, total)
    return [{"id": i} for i in range(offset, end)]


# ── Main ────────────────────────────────────────────────────────────


async def main():
    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    print("AWA ETL Pipeline Example\n")

    # ── Worker handlers (WORKER PROCESS in production) ────────────

    @client.worker(ImportTable, queue="etl")
    async def handle_import(job):
        table = job.args["source_table"]
        batch_size = job.args.get("batch_size", 1000)
        total = TOTAL_ROWS.get(table, 1000)

        # Resume from checkpoint if this is a retry
        checkpoint = (job.progress or {}).get("metadata", {})
        offset = checkpoint.get("last_offset", 0)
        rows_imported = checkpoint.get("rows_imported", 0)

        if offset > 0:
            print(f"  Resuming {table} from offset {offset}")

        while True:
            rows = fetch_rows(table, offset, batch_size)
            if not rows:
                break

            rows_imported += len(rows)
            offset += len(rows)

            pct = min(100, int(100 * rows_imported / total))
            job.set_progress(pct, f"Importing {table}: {rows_imported}/{total}")
            job.update_metadata({
                "last_offset": offset,
                "rows_imported": rows_imported,
                "source_table": table,
            })

            if (offset // batch_size) % 5 == 0:
                await job.flush_progress()

            await asyncio.sleep(0.005)

        print(f"  ✓ {table}: imported {rows_imported} rows")

    @client.worker(AggregateMetrics, queue="etl")
    async def handle_aggregate(job):
        tables = job.args["tables"]
        for i, table in enumerate(tables):
            pct = int(100 * (i + 1) / len(tables))
            job.set_progress(pct, f"Aggregating {table}")
            await asyncio.sleep(0.02)
        print(f"  ✓ Aggregated {len(tables)} tables for {job.args['date']}")

    # ── Schedule a daily run ────────────────────────────────────

    client.periodic(
        name="daily_users_import",
        cron_expr="0 2 * * *",
        args_type=ImportTable,
        args=ImportTable(source_table="users"),
        queue="etl",
        tags=["etl", "daily"],
    )

    # ── Enqueue jobs transactionally ────────────────────────────

    async with await client.transaction() as tx:
        for table in ["users", "orders", "events"]:
            await tx.insert(
                ImportTable(source_table=table, batch_size=500),
                queue="etl",
                tags=["etl", "import", table],
                metadata={"pipeline_run": "2026-03-22"},
            )
        await tx.insert(
            AggregateMetrics(date="2026-03-22", tables=["users", "orders", "events"]),
            queue="etl",
            priority=3,  # runs after imports
            tags=["etl", "aggregate"],
        )
    print("Enqueued 4 ETL jobs (3 imports + 1 aggregation)\n")

    # ── Start workers and monitor ───────────────────────────────

    client.start([("etl", 3)], leader_election_interval_ms=500)

    for _ in range(60):
        await asyncio.sleep(1)
        stats = await client.queue_stats()
        etl = next((s for s in stats if s["queue"] == "etl"), None)
        if etl:
            avail, running = etl["available"], etl["running"]
            if avail == 0 and running == 0:
                print(f"\n✓ All jobs completed")
                break
            print(f"  Queue: available={avail} running={running}")

    health = await client.health_check()
    print(f"Health: leader={health.leader} heartbeat={health.heartbeat_alive}")
    await client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

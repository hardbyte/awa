"""Awa OTLP metrics from Python.

Call ``awa.init_telemetry(...)`` once before starting the worker to install
an OTel MeterProvider on the Rust side. awa's 20+ built-in metrics
(throughput, pickup latency, in-flight jobs, rescues, …) then flow to any
OTLP collector that speaks gRPC.

Usage:
    DATABASE_URL=postgres://... \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    python examples/telemetry.py

With Grafana + Loki + Tempo + Mimir (``grafana/otel-lgtm``) running at that
endpoint, you should see ``awa_job_claimed_total``, ``awa_job_duration_*``,
``awa_job_wait_duration_*``, and similar series appear within a few seconds.
"""

import asyncio
import os
from dataclasses import dataclass

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "awa-python-example")


@dataclass
class SendEmail:
    to: str


async def main() -> None:
    # One-time setup. Safe to call again; subsequent calls are no-ops.
    installed = awa.init_telemetry(OTLP_ENDPOINT, SERVICE_NAME, export_interval_ms=1000)
    print(f"awa.init_telemetry installed={installed}")

    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()

    @client.task(SendEmail, queue="email")
    async def handle(job):
        await asyncio.sleep(0.05)
        print(f"sent to {job.args.to}")

    for i in range(20):
        await client.insert(SendEmail(to=f"user{i}@example.com"), queue="email")

    await client.start([("email", 4)])
    # Run long enough for the PeriodicReader to tick at least once.
    await asyncio.sleep(2.5)
    await client.shutdown()

    # Force a final flush so short-lived scripts do not drop the last window.
    awa.shutdown_telemetry()


if __name__ == "__main__":
    asyncio.run(main())

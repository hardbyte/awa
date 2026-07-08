#!/usr/bin/env python3
"""Cross-language trace e2e (ADR-039): a Python producer span, the Rust
runtime's job.execute span, and a Python handler span must land in Tempo as
ONE trace with correct parent-child edges across the FFI boundary:

    e2e.python.enqueue  (python producer)
      └── job.execute … (rust runtime, remote child via awa:traceparent)
            └── e2e.python.handle  (python handler, ambient context attach)

Requires a running Postgres, an OTLP collector, and Tempo (grafana/otel-lgtm
provides all three alongside Grafana). Environment:

    DATABASE_URL                 postgres://... (default local test instance)
    OTEL_EXPORTER_OTLP_ENDPOINT  default http://localhost:4317
    TEMPO_URL                    default http://localhost:3200

Run from awa-python with its dev environment (wheel built via maturin):

    uv run --with opentelemetry-exporter-otlp-proto-grpc \
        python ../scripts/trace-e2e-python.py
"""

import asyncio
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import awa

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)
OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
TEMPO_URL = os.environ.get("TEMPO_URL", "http://localhost:3200")
QUEUE = "trace_py_e2e"


@dataclass
class CrossLangJob:
    n: int


def hex_span_id(context) -> str:
    return f"{context.span_id:016x}"


def b64_of_hex(hex_id: str) -> str:
    return base64.b64encode(bytes.fromhex(hex_id)).decode()


def tempo_trace_spans(trace_id_hex: str) -> list[dict]:
    url = f"{TEMPO_URL}/api/traces/{trace_id_hex}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = json.load(resp)
    except Exception:
        return []
    spans = []
    for batch in body.get("batches", []):
        for scope in batch.get("scopeSpans", []):
            spans.extend(scope.get("spans", []))
    return spans


def wait_for_trace(trace_id_hex: str, minimum_spans: int, timeout_s: int = 90) -> list[dict]:
    deadline = time.monotonic() + timeout_s
    spans: list[dict] = []
    while time.monotonic() < deadline:
        spans = tempo_trace_spans(trace_id_hex)
        if len(spans) >= minimum_spans:
            return spans
        time.sleep(1)
    raise AssertionError(
        f"trace {trace_id_hex} has {len(spans)} spans in Tempo after {timeout_s}s, "
        f"expected >= {minimum_spans}: {[s.get('name') for s in spans]}"
    )


async def main() -> None:
    # Python-side span pipeline (producer + handler spans).
    provider = TracerProvider(resource=Resource.create({"service.name": "awa-python-e2e"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("trace-e2e")

    # Rust-side pipeline: exports the runtime's job.execute spans.
    awa.init_telemetry(OTLP_ENDPOINT, "awa-python-e2e-rust", traces=True)

    client = awa.AsyncClient(DATABASE_URL)
    await client.migrate()
    tx = await client.transaction()
    await tx.execute(f"DELETE FROM awa.jobs WHERE queue = '{QUEUE}'")
    await tx.commit()

    handler_span_ids: list[str] = []
    done = asyncio.Event()

    @client.task(CrossLangJob, queue=QUEUE)
    async def handle(job):
        # The worker attached the execution-span context before invoking us,
        # so this span nests under the Rust job.execute span.
        with tracer.start_as_current_span("e2e.python.handle") as span:
            handler_span_ids.append(hex_span_id(span.get_span_context()))
        done.set()

    with tracer.start_as_current_span("e2e.python.enqueue") as producer:
        producer_ctx = producer.get_span_context()
        producer_trace_hex = f"{producer_ctx.trace_id:032x}"
        producer_span_hex = hex_span_id(producer_ctx)
        job = await client.insert(CrossLangJob(n=1), queue=QUEUE)

    stored = await client.get_job(job.id)
    assert stored.traceparent and producer_trace_hex in stored.traceparent, (
        f"python producer capture failed: metadata={stored.metadata}"
    )

    await client.start([(QUEUE, 1)])
    try:
        await asyncio.wait_for(done.wait(), timeout=60)
    finally:
        await client.shutdown()
        await client.close()

    provider.force_flush()
    awa.shutdown_telemetry()

    # One trace, three spans, two cross-language edges.
    spans = wait_for_trace(producer_trace_hex, 3)
    by_b64_id = {s["spanId"]: s for s in spans}
    names = [s.get("name") for s in spans]

    producer_b64 = b64_of_hex(producer_span_hex)
    assert producer_b64 in by_b64_id, f"python producer span missing from trace: {names}"

    execute = next(
        (s for s in spans if str(s.get("name", "")).startswith("job.execute")), None
    )
    assert execute is not None, f"rust job.execute span missing from trace: {names}"
    assert execute.get("parentSpanId") == producer_b64, (
        "job.execute should be a remote child of the python producer span; "
        f"got parent {execute.get('parentSpanId')!r}, expected {producer_b64!r}"
    )
    assert execute.get("kind") == "SPAN_KIND_CONSUMER", execute.get("kind")

    handler_b64 = b64_of_hex(handler_span_ids[0])
    handler = by_b64_id.get(handler_b64)
    assert handler is not None, f"python handler span missing from trace: {names}"
    assert handler.get("parentSpanId") == execute["spanId"], (
        "e2e.python.handle should nest under the rust execution span; "
        f"got parent {handler.get('parentSpanId')!r}, expected {execute['spanId']!r}"
    )

    print(
        f"cross-language trace e2e OK: trace {producer_trace_hex} carries "
        f"python-enqueue -> rust-execute -> python-handle ({len(spans)} spans)"
    )


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

"""Distributed tracing (ADR-039): Python producer capture and handler-side
context attach.

Requires Postgres running at localhost:15432 (see docker command in test plan).
"""

import asyncio
import os
from dataclasses import dataclass

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

import awa
from awa import _trace

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:test@localhost:15432/awa_test"
)

# A local (non-global) tracer provider: global state would leak between
# test files, and the producer side only needs a recording span to exist.
_provider = TracerProvider()
_tracer = _provider.get_tracer("awa-trace-test")


@pytest.fixture
async def client():
    c = awa.AsyncClient(DATABASE_URL)
    await c.migrate()
    tx = await c.transaction()
    await tx.execute("DELETE FROM awa.jobs WHERE queue IN ('trace_py', 'trace_py_handler')")
    await tx.commit()
    try:
        yield c
    finally:
        await c.close()


@dataclass
class TracedJob:
    n: int


# -- unit: formatting / injection ------------------------------------------


def test_no_ambient_span_no_capture():
    assert _trace.current_traceparent() is None
    assert _trace.inject_traceparent(None) is None
    assert _trace.inject_traceparent({"k": "v"}) == {"k": "v"}


def test_capture_inside_span_roundtrips():
    with _tracer.start_as_current_span("producer") as span:
        traceparent = _trace.current_traceparent()
        assert traceparent is not None
        sc = span.get_span_context()
        assert traceparent == f"00-{sc.trace_id:032x}-{sc.span_id:016x}-{sc.trace_flags:02x}"

        captured = _trace.inject_traceparent(None)
        assert captured == {_trace.TRACEPARENT_KEY: traceparent}

        # caller-supplied value wins; input dict is not mutated
        explicit = {_trace.TRACEPARENT_KEY: "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
        assert _trace.inject_traceparent(explicit) is explicit
        original = {"user": "data"}
        merged = _trace.inject_traceparent(original)
        assert merged is not original
        assert original == {"user": "data"}
        assert merged["user"] == "data"
        assert merged[_trace.TRACEPARENT_KEY] == traceparent


def test_capture_kill_switch(monkeypatch):
    monkeypatch.setenv("AWA_TRACE_CAPTURE", "off")
    with _tracer.start_as_current_span("producer"):
        assert _trace.inject_traceparent(None) is None
        # explicit values still pass through untouched
        explicit = {_trace.TRACEPARENT_KEY: "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
        assert _trace.inject_traceparent(explicit) is explicit


# -- integration: producer capture flows into stored metadata ---------------


async def test_insert_inside_span_captures_traceparent(client):
    with _tracer.start_as_current_span("producer") as span:
        job = await client.insert(TracedJob(n=1), queue="trace_py")
        expected_trace_id = f"{span.get_span_context().trace_id:032x}"

    stored = await client.get_job(job.id)
    traceparent = stored.metadata.get(_trace.TRACEPARENT_KEY)
    assert traceparent is not None, f"metadata: {stored.metadata}"
    assert expected_trace_id in traceparent
    assert stored.traceparent == traceparent  # convenience getter


async def test_insert_outside_span_stores_nothing(client):
    job = await client.insert(TracedJob(n=2), queue="trace_py")
    stored = await client.get_job(job.id)
    assert _trace.TRACEPARENT_KEY not in stored.metadata
    assert stored.traceparent is None


# -- integration: handler runs under the propagated context -----------------


async def test_handler_sees_propagated_context(client):
    """The worker attaches the job's trace context before invoking the
    handler, so ambient OpenTelemetry state inside the handler carries the
    producer's trace id (via the enqueue-site fallback: no Rust-side trace
    pipeline is installed in tests, so the execution-span context is
    unavailable and the stored enqueue context is attached instead)."""
    seen: list[str | None] = []
    done = asyncio.Event()

    @client.task(TracedJob, queue="trace_py_handler")
    async def handler(job):
        sc = otel_trace.get_current_span(otel_context.get_current()).get_span_context()
        seen.append(f"{sc.trace_id:032x}" if sc.is_valid else None)
        done.set()

    with _tracer.start_as_current_span("producer") as span:
        producer_trace_id = f"{span.get_span_context().trace_id:032x}"
        await client.insert(TracedJob(n=3), queue="trace_py_handler")

    await client.start([("trace_py_handler", 1)])
    try:
        await asyncio.wait_for(done.wait(), timeout=30)
    finally:
        await client.shutdown()

    assert seen == [producer_trace_id]


async def test_handler_can_reextract_via_propagator(client):
    """The documented manual pattern keeps working alongside the automatic
    attach: extracting job.traceparent yields the same trace."""
    seen: list[str] = []
    done = asyncio.Event()

    @client.task(TracedJob, queue="trace_py_handler")
    async def handler(job):
        ctx = TraceContextTextMapPropagator().extract({"traceparent": job.traceparent})
        sc = otel_trace.get_current_span(ctx).get_span_context()
        seen.append(f"{sc.trace_id:032x}")
        done.set()

    with _tracer.start_as_current_span("producer") as span:
        producer_trace_id = f"{span.get_span_context().trace_id:032x}"
        await client.insert(TracedJob(n=4), queue="trace_py_handler")

    await client.start([("trace_py_handler", 1)])
    try:
        await asyncio.wait_for(done.wait(), timeout=30)
    finally:
        await client.shutdown()

    assert seen == [producer_trace_id]

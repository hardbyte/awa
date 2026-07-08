"""Distributed trace propagation for Python producers and handlers.

Mirrors the Rust side (ADR-039): when a producer enqueues inside an active
OpenTelemetry span, the W3C ``traceparent`` is stored under the reserved
``awa:traceparent`` metadata key; when a handler runs, the execution-span
context is attached as the ambient OpenTelemetry context so downstream
instrumentation (httpx, requests, SQLAlchemy, ...) nests under the job's
execution span automatically.

``opentelemetry-api`` is an optional dependency: everything here degrades to
a no-op when it is not importable, and the import decision is cached so the
per-insert cost of the uninstrumented path is one attribute read.
"""

from __future__ import annotations

import os
from typing import Any, Awaitable, Callable

TRACEPARENT_KEY = "awa:traceparent"
_CAPTURE_ENV = "AWA_TRACE_CAPTURE"

# None = not yet probed; True/False = cached import decision.
_otel_available: bool | None = None


def _otel() -> bool:
    global _otel_available
    if _otel_available is None:
        try:
            import opentelemetry.context  # noqa: F401
            import opentelemetry.trace  # noqa: F401

            _otel_available = True
        except ImportError:
            _otel_available = False
    return _otel_available


def _capture_disabled() -> bool:
    return os.environ.get(_CAPTURE_ENV, "").strip().lower() in ("off", "false", "0")


def current_traceparent() -> str | None:
    """The current OpenTelemetry span context as a W3C ``traceparent``.

    Returns ``None`` when ``opentelemetry-api`` is not installed or there is
    no valid span context (no SDK configured, or not inside a span).
    """
    if not _otel():
        return None
    from opentelemetry import trace

    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return None
    return (
        f"00-{span_context.trace_id:032x}"
        f"-{span_context.span_id:016x}"
        f"-{span_context.trace_flags:02x}"
    )


def inject_traceparent(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return metadata with the ambient traceparent captured under the
    reserved key. A caller-supplied value always wins; the input dict is
    never mutated. No-op without opentelemetry, without a valid ambient
    context, or when ``AWA_TRACE_CAPTURE`` is off.
    """
    if metadata is not None and TRACEPARENT_KEY in metadata:
        return metadata
    if _capture_disabled():
        return metadata
    traceparent = current_traceparent()
    if traceparent is None:
        return metadata
    captured = dict(metadata) if metadata else {}
    captured[TRACEPARENT_KEY] = traceparent
    return captured


async def call_with_context(
    traceparent: str | None,
    handler: Callable[[Any], Awaitable[Any]],
    job: Any,
) -> Any:
    """Invoke a handler with ``traceparent`` attached as the ambient
    OpenTelemetry context (the worker passes the execution-span context, so
    spans created inside the handler nest under ``job.execute``).

    Falls back to a plain call when opentelemetry is not installed or no
    context was provided.
    """
    if traceparent and _otel():
        from opentelemetry import context as otel_context
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )

        remote = TraceContextTextMapPropagator().extract({"traceparent": traceparent})
        token = otel_context.attach(remote)
        try:
            return await handler(job)
        finally:
            otel_context.detach(token)
    return await handler(job)

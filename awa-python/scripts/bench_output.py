"""Shared benchmark output schema for machine-readable JSONL results.

Both Rust and Python benchmarks emit one JSON line per scenario using
this schema (schema_version=1). Human-readable summaries go to stdout
as before; JSONL records are printed on a separate line prefixed with
@@BENCH_JSON@@ for easy extraction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

SCHEMA_VERSION = 1
JSONL_PREFIX = "@@BENCH_JSON@@"


@dataclass
class BenchThroughput:
    handler_per_s: float = 0.0
    db_finalized_per_s: float = 0.0


@dataclass
class BenchLatency:
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


@dataclass
class BenchRescue:
    deadline_rescued: int | None = None
    callback_timeouts: int | None = None
    heartbeat_rescued: int | None = None


@dataclass
class BenchMetrics:
    throughput: BenchThroughput = field(default_factory=BenchThroughput)
    drain_time_s: float | None = None
    latency_ms: BenchLatency | None = None
    rescue: BenchRescue | None = None


@dataclass
class BenchmarkResult:
    scenario: str
    language: str
    seeded: int
    metrics: BenchMetrics = field(default_factory=BenchMetrics)
    outcomes: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the shared schema dict, omitting None values."""
        result: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "scenario": self.scenario,
            "language": self.language,
            "seeded": self.seeded,
            "metrics": _strip_none(
                {
                    "throughput": {
                        "handler_per_s": self.metrics.throughput.handler_per_s,
                        "db_finalized_per_s": self.metrics.throughput.db_finalized_per_s,
                    },
                    "drain_time_s": self.metrics.drain_time_s,
                    "latency_ms": _strip_none(
                        {
                            "p50": self.metrics.latency_ms.p50,
                            "p95": self.metrics.latency_ms.p95,
                            "p99": self.metrics.latency_ms.p99,
                        }
                    )
                    if self.metrics.latency_ms
                    else None,
                    "rescue": _strip_none(
                        {
                            "deadline_rescued": self.metrics.rescue.deadline_rescued,
                            "callback_timeouts": self.metrics.rescue.callback_timeouts,
                            "heartbeat_rescued": self.metrics.rescue.heartbeat_rescued,
                        }
                    )
                    if self.metrics.rescue
                    else None,
                }
            ),
            "outcomes": self.outcomes,
        }
        if self.metadata:
            result["metadata"] = self.metadata
        return result

    def emit(self) -> None:
        """Print the JSONL record to stdout."""
        print(f"{JSONL_PREFIX}{json.dumps(self.to_dict(), separators=(',', ':'))}")


def _strip_none(d: dict[str, Any]) -> dict[str, Any] | None:
    """Remove keys with None values. Return None if dict would be empty."""
    result = {k: v for k, v in d.items() if v is not None}
    return result or None

"""Sample record — one row in raw.csv, one JSON line from an adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Sample:
    run_id: str
    system: str
    # 0-indexed replica id. `0` for single-replica runs (today's default)
    # and for cluster-scoped samples (e.g. PG-side pgstattuple metrics
    # that aren't per-replica by definition). Multi-replica worker-scoped
    # samples take 0..N-1.
    instance_id: int
    elapsed_s: float
    sampled_at: str  # ISO8601 UTC
    phase_label: str
    phase_type: str
    subject_kind: str  # table | index | session | adapter | cluster
    subject: str
    metric: str
    value: float
    window_s: float


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


RAW_CSV_HEADER = [
    "run_id",
    "system",
    "instance_id",
    "elapsed_s",
    "sampled_at",
    "phase_label",
    "phase_type",
    "subject_kind",
    "subject",
    "metric",
    "value",
    "window_s",
]

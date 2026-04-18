"""Sample record — one row in raw.csv, one JSON line from an adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Sample:
    run_id: str
    system: str
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

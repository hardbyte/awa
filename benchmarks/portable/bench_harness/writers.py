"""Writers for raw.csv, summary.json, manifest.json.

Tidy long-form CSV: one row per (system, subject, metric, sample).
"""

from __future__ import annotations

import csv
import json
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .phases import (
    PHASE_INCLUDED_IN_SUMMARY,
    Phase,
    PhaseType,
)
from .sample import RAW_CSV_HEADER, Sample

RATE_METRICS = {
    "enqueue_rate",
    "completion_rate",
    "retryable_failure_rate",
    "completed_priority_1_rate",
    "completed_priority_4_rate",
    "completed_original_priority_1_rate",
    "completed_original_priority_4_rate",
    "aged_completion_rate",
}

LATENCY_METRICS = {
    "producer_p50_ms",
    "producer_p95_ms",
    "producer_p99_ms",
    "producer_call_p50_ms",
    "producer_call_p95_ms",
    "producer_call_p99_ms",
    "subscriber_p50_ms",
    "subscriber_p95_ms",
    "subscriber_p99_ms",
    "end_to_end_p50_ms",
    "end_to_end_p95_ms",
    "end_to_end_p99_ms",
    "claim_p50_ms",
    "claim_p95_ms",
    "claim_p99_ms",
}

DEPTH_METRICS = {
    "queue_depth",
    "running_depth",
    "retryable_depth",
    "scheduled_depth",
    "total_backlog",
    "producer_target_rate",
}


# ────────────────────────────────────────────────────────────────────────
# raw.csv
# ────────────────────────────────────────────────────────────────────────


class RawCsvWriter:
    """Append-only writer for raw.csv. Safe to call from the orchestrator
    consumer thread — not thread-safe on its own."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.path.exists()
        self._fh = self.path.open("a", newline="")
        self._writer = csv.writer(self._fh)
        if is_new:
            self._writer.writerow(RAW_CSV_HEADER)
            self._fh.flush()

    def write(self, sample: Sample) -> None:
        d = asdict(sample)
        self._writer.writerow([d[col] for col in RAW_CSV_HEADER])

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.flush()
        finally:
            self._fh.close()


# ────────────────────────────────────────────────────────────────────────
# summary.json
# ────────────────────────────────────────────────────────────────────────


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2:
        return float(sorted_vals[mid])
    return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2)


def _peak(values: list[float]) -> float | None:
    return float(max(values)) if values else None


def _load_rows(raw_csv: Path) -> list[dict]:
    with raw_csv.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def aggregate_replica_metric_series(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    metric: str,
    subject_kind: str | None = None,
) -> list[float]:
    """Aggregate per-replica adapter metrics into a system-level series.

    Rules:
    - rate metrics: sum across replicas per timestamp
    - latency metrics: max across replicas per timestamp (avoid zero-idle dilution)
    - depth/observer metrics: max across replicas per timestamp
    - other metrics: raw values
    """
    if metric not in RATE_METRICS | LATENCY_METRICS | DEPTH_METRICS:
        return _phase_metric_values(
            rows,
            system=system,
            phase_label=phase_label,
            metric=metric,
            subject_kind=subject_kind,
        )

    per_elapsed: dict[float, list[float]] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
        ):
            continue
        if subject_kind and row["subject_kind"] != subject_kind:
            continue
        try:
            elapsed_s = float(row["elapsed_s"])
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        per_elapsed.setdefault(elapsed_s, []).append(value)

    out: list[float] = []
    for elapsed_s in sorted(per_elapsed):
        values = per_elapsed[elapsed_s]
        if metric in RATE_METRICS:
            out.append(float(sum(values)))
        else:
            out.append(float(max(values)))
    return out


def per_instance_phase_metrics(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
) -> dict[str, dict[str, float | int | None]]:
    """Replica-scoped summary for adapter metrics only."""
    replica_ids = sorted(
        {
            row["instance_id"]
            for row in rows
            if row["system"] == system
            and row["phase_label"] == phase_label
            and row["subject_kind"] == "adapter"
        },
        key=lambda value: int(value),
    )
    out: dict[str, dict[str, float | int | None]] = {}
    for replica_id in replica_ids:
        metrics = {}
        for metric in [
            "completion_rate",
            "enqueue_rate",
            "producer_p99_ms",
            "producer_call_p99_ms",
            "subscriber_p99_ms",
            "end_to_end_p99_ms",
            "claim_p99_ms",
            "queue_depth",
            "running_depth",
            "retryable_depth",
            "scheduled_depth",
            "total_backlog",
        ]:
            values = []
            for row in rows:
                if (
                    row["system"] != system
                    or row["phase_label"] != phase_label
                    or row["subject_kind"] != "adapter"
                    or row["instance_id"] != replica_id
                    or row["metric"] != metric
                ):
                    continue
                try:
                    values.append(float(row["value"]))
                except (TypeError, ValueError):
                    continue
            metrics[f"median_{metric}"] = _median(values)
            metrics[f"peak_{metric}"] = _peak(values)
            metrics[f"count_{metric}"] = len(values)
        out[replica_id] = metrics
    return out


def _replica_metric_sum(
    replicas: dict[str, dict[str, float | int | None]], metric: str
) -> float | None:
    values = [
        float(value)
        for replica in replicas.values()
        if (value := replica.get(metric)) is not None
    ]
    return float(sum(values)) if values else None


def _replica_metric_max(
    replicas: dict[str, dict[str, float | int | None]], metric: str
) -> float | None:
    values = [
        float(value)
        for replica in replicas.values()
        if (value := replica.get(metric)) is not None
    ]
    return float(max(values)) if values else None


def compute_summary(
    raw_csv: Path,
    *,
    run_id: str,
    scenario: str | None,
    phases: list[Phase],
) -> dict:
    """Compute summary.json from raw.csv. Pure post-processing step so it can
    also run standalone from a checked-in fixture (used by CI smoke)."""
    rows = _load_rows(raw_csv)

    # Group by (system, phase_label, metric, subject, instance_id).
    # Replicas of the same system emit independent sample streams; mixing
    # them here gives a wrong median (and a wrong sample count) for
    # adapter-scoped rate / latency / depth metrics. The previous bucket
    # key omitted `instance_id`, so a 4x8 run reported the median of two
    # interleaved streams instead of two per-replica medians or a
    # correctly-aggregated system-level series. For adapter metrics in
    # the rate / latency / depth category, we collapse the per-replica
    # streams via `aggregate_replica_metric_series`, which encodes the
    # right per-timestamp reduction (sum for rates, max for depth /
    # latency). For everything else (table-scoped n_dead_tup / size /
    # autovacuum count etc.) the per-replica observation is the same
    # value, so we keep the old single-bucket shape.
    adapter_metric_set = RATE_METRICS | LATENCY_METRICS | DEPTH_METRICS
    bucket: dict[tuple[str, str, str, str, str], list[float]] = {}
    for row in rows:
        try:
            v = float(row["value"])
        except (TypeError, ValueError):
            continue
        if (
            row.get("subject_kind") == "adapter"
            and row["metric"] in adapter_metric_set
        ):
            # Defer to the per-timestamp aggregator below; skip raw bucket.
            continue
        instance_id = row.get("instance_id") or ""
        key = (
            row["system"],
            row["phase_label"],
            row["metric"],
            row["subject"],
            instance_id,
        )
        bucket.setdefault(key, []).append(v)

    phase_by_label = {p.label: p for p in phases}
    out_systems: dict[str, dict] = {}

    for (system, label, metric, subject, _instance), values in bucket.items():
        phase = phase_by_label.get(label)
        if phase is None or not PHASE_INCLUDED_IN_SUMMARY.get(phase.type, True):
            continue
        sys_block = out_systems.setdefault(system, {"phases": {}})
        phase_block = sys_block["phases"].setdefault(
            label,
            {
                "phase_type": phase.type.value,
                "metrics": {},
            },
        )
        key = metric if not subject else f"{metric}@{subject}"
        existing = phase_block["metrics"].get(key)
        if existing is None:
            phase_block["metrics"][key] = {
                "median": _median(values),
                "peak": _peak(values),
                "count": len(values),
            }
        else:
            # Same (system, phase, metric, subject) under different
            # instance_ids on a non-adapter row: combine. Tables emit
            # the same value across replicas, so combining is a no-op
            # for `peak`; we keep the larger `count` and re-derive the
            # median over the union.
            combined = values + ([existing["median"]] * existing["count"])
            phase_block["metrics"][key] = {
                "median": _median(combined),
                "peak": max(existing["peak"], _peak(values)),
                "count": existing["count"] + len(values),
            }

    # Adapter-metric pass: aggregate per-replica streams into a single
    # system-level series before bucketing the median/peak/count.
    systems_seen = {row["system"] for row in rows}
    for system in systems_seen:
        for phase in phases:
            if not PHASE_INCLUDED_IN_SUMMARY.get(phase.type, True):
                continue
            for metric in adapter_metric_set:
                series = aggregate_replica_metric_series(
                    rows,
                    system=system,
                    phase_label=phase.label,
                    metric=metric,
                    subject_kind="adapter",
                )
                if not series:
                    continue
                sys_block = out_systems.setdefault(system, {"phases": {}})
                phase_block = sys_block["phases"].setdefault(
                    phase.label,
                    {
                        "phase_type": phase.type.value,
                        "metrics": {},
                    },
                )
                phase_block["metrics"][metric] = {
                    "median": _median(series),
                    "peak": _peak(series),
                    "count": len(series),
                }

    # Recovery metrics: for each system that has a recovery phase immediately
    # following an idle-in-tx phase, compute recovery_halflife_s and
    # recovery_to_baseline_s on n_dead_tup (summed across event tables).
    ordered_phases = [p for p in phases if p.type is not PhaseType.WARMUP]
    for i, phase in enumerate(ordered_phases):
        if phase.type is not PhaseType.RECOVERY:
            continue
        prev_idle = None
        prev_clean = None
        for prev in reversed(ordered_phases[:i]):
            if prev_idle is None and prev.type is PhaseType.IDLE_IN_TX:
                prev_idle = prev
            if prev_clean is None and prev.type is PhaseType.CLEAN:
                prev_clean = prev
            if prev_idle and prev_clean:
                break
        if not prev_idle:
            continue
        for system in out_systems:
            rec = _recovery_stats(
                rows,
                system=system,
                idle_label=prev_idle.label,
                recovery_label=phase.label,
                clean_label=prev_clean.label if prev_clean else None,
            )
            if rec:
                target = out_systems[system]["phases"].setdefault(
                    phase.label,
                    {
                        "phase_type": phase.type.value,
                        "metrics": {},
                    },
                )
                target.update(rec)

    for system, system_block in out_systems.items():
        for phase_label, phase_block in system_block["phases"].items():
            dead_tup = _phase_summed_values(
                rows,
                system=system,
                phase_label=phase_label,
                metric="n_dead_tup",
                subject_kind="table",
            )
            replicas = per_instance_phase_metrics(
                rows, system=system, phase_label=phase_label
            )
            phase_block["peak_dead_tup"] = _peak(dead_tup)
            phase_block["median_dead_tup"] = _median(dead_tup)
            phase_block["median_claim_p99_ms"] = _replica_metric_max(
                replicas, "median_claim_p99_ms"
            )
            phase_block["median_producer_p99_ms"] = _replica_metric_max(
                replicas, "median_producer_p99_ms"
            )
            phase_block["median_producer_call_p99_ms"] = _replica_metric_max(
                replicas, "median_producer_call_p99_ms"
            )
            phase_block["median_subscriber_p99_ms"] = _replica_metric_max(
                replicas, "median_subscriber_p99_ms"
            )
            phase_block["median_end_to_end_p99_ms"] = _replica_metric_max(
                replicas, "median_end_to_end_p99_ms"
            )
            phase_block["median_throughput_per_s"] = _replica_metric_sum(
                replicas, "median_completion_rate"
            )
            phase_block["median_enqueue_rate_per_s"] = _replica_metric_sum(
                replicas, "median_enqueue_rate"
            )
            phase_block["median_queue_depth"] = _replica_metric_max(
                replicas, "median_queue_depth"
            )
            phase_block["median_running_depth"] = _replica_metric_max(
                replicas, "median_running_depth"
            )
            phase_block["autovacuum_count_delta"] = _autovacuum_count_delta(
                rows,
                system=system,
                phase_label=phase_label,
            )
            phase_block["replicas"] = replicas

    return {
        "run_id": run_id,
        "scenario": scenario,
        "phases": [
            {"label": p.label, "type": p.type.value, "duration_s": p.duration_s}
            for p in phases
        ],
        "systems": out_systems,
    }


def _recovery_stats(
    rows: list[dict],
    *,
    system: str,
    idle_label: str,
    recovery_label: str,
    clean_label: str | None,
) -> dict | None:
    """recovery_halflife_s = time until n_dead_tup <= 0.1 * peak_idle.
    recovery_to_baseline_s = time until within 10% of median_clean."""

    def sum_by_elapsed(phase: str) -> list[tuple[float, float]]:
        per_elapsed: dict[float, float] = {}
        for r in rows:
            if (
                r["system"] == system
                and r["phase_label"] == phase
                and r["metric"] == "n_dead_tup"
                and r["subject_kind"] == "table"
            ):
                try:
                    t = float(r["elapsed_s"])
                    v = float(r["value"])
                except (TypeError, ValueError):
                    continue
                per_elapsed[t] = per_elapsed.get(t, 0.0) + v
        return sorted(per_elapsed.items())

    idle = sum_by_elapsed(idle_label)
    recovery = sum_by_elapsed(recovery_label)
    if not idle or not recovery:
        return None

    peak_idle = max(v for _, v in idle)
    recovery_start_t = recovery[0][0]
    halflife_target = peak_idle * 0.1

    halflife_s: float | None = None
    for t, v in recovery:
        if v <= halflife_target:
            halflife_s = t - recovery_start_t
            break

    to_baseline_s: float | None = None
    if clean_label:
        clean = sum_by_elapsed(clean_label)
        if clean:
            clean_vals = [v for _, v in clean]
            baseline = _median(clean_vals) or 0.0
            threshold = baseline * 1.10
            for t, v in recovery:
                if v <= threshold:
                    to_baseline_s = t - recovery_start_t
                    break

    return {
        "recovery_halflife_s": halflife_s,
        "recovery_to_baseline_s": to_baseline_s,
        "peak_idle_dead_tup": peak_idle,
    }


def _phase_metric_values(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    metric: str,
    subject_kind: str | None = None,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
        ):
            continue
        if subject_kind and row["subject_kind"] != subject_kind:
            continue
        try:
            values.append(float(row["value"]))
        except (TypeError, ValueError):
            continue
    return values


def _phase_summed_values(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    metric: str,
    subject_kind: str,
) -> list[float]:
    per_elapsed: dict[float, float] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
        ):
            continue
        if row["subject_kind"] != subject_kind:
            continue
        try:
            elapsed_s = float(row["elapsed_s"])
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        per_elapsed[elapsed_s] = per_elapsed.get(elapsed_s, 0.0) + value
    return list(per_elapsed.values())


def _autovacuum_count_delta(
    rows: list[dict], *, system: str, phase_label: str
) -> float | None:
    per_subject: dict[str, list[float]] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != "autovacuum_count"
        ):
            continue
        if row["subject_kind"] != "table":
            continue
        try:
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        per_subject.setdefault(row["subject"], []).append(value)
    # `pg_stat_user_tables.autovacuum_count` is monotonically
    # non-decreasing, so the delta over a phase is exactly
    # `max - min` regardless of CSV row order. The previous
    # `values[-1] - values[0]` reduction depended on the rows
    # being sorted by `elapsed_s`, which the queue→file drain
    # does not guarantee — under reorder it could under-count or
    # go negative.
    deltas = [
        max(values) - min(values)
        for values in per_subject.values()
        if len(values) >= 2
    ]
    return float(sum(deltas)) if deltas else None


def write_summary(summary: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(summary, fh, indent=2)


# ────────────────────────────────────────────────────────────────────────
# manifest.json
# ────────────────────────────────────────────────────────────────────────


def _safe_cmd(cmd: list[str]) -> str | None:
    try:
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return out.stdout.strip() or None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def capture_pg_env(database_url: str) -> dict:
    """Connect to PG and pull version + relevant config settings."""
    import psycopg

    settings_of_interest = [
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "autovacuum",
        "autovacuum_naptime",
        "autovacuum_vacuum_cost_delay",
        "autovacuum_vacuum_cost_limit",
        "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_scale_factor",
        "max_connections",
        "synchronous_commit",
        "wal_level",
    ]
    try:
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                settings: dict[str, str] = {}
                for name in settings_of_interest:
                    cur.execute("SELECT current_setting(%s)", (name,))
                    row = cur.fetchone()
                    if row:
                        settings[name] = row[0]
                return {"version": version, "settings": settings}
    except Exception as exc:  # defensive: manifest is best-effort
        return {"error": str(exc)}


def capture_host_env() -> dict:
    try:
        import multiprocessing

        cpu_count = multiprocessing.cpu_count()
    except Exception:
        cpu_count = None
    mem_bytes = None
    try:
        if hasattr(os, "sysconf"):
            mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError):
        pass
    return {
        "os": platform.platform(),
        "kernel": platform.release(),
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": cpu_count,
        "memory_bytes": mem_bytes,
        "python": sys.version.split()[0],
        "docker": _safe_cmd(["docker", "--version"]),
        "docker_compose": _safe_cmd(["docker", "compose", "version"]),
    }


def build_manifest(
    *,
    run_id: str,
    scenario: str | None,
    phases: list[Phase],
    systems: list[str],
    database_url: str,
    cli_args: list[str],
    adapter_versions: dict[str, dict],
    pg_image: str,
) -> dict:
    return {
        "run_id": run_id,
        "scenario": scenario,
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cli": cli_args,
        "systems": systems,
        "phases": [
            {"label": p.label, "type": p.type.value, "duration_s": p.duration_s}
            for p in phases
        ],
        "pg_image": pg_image,
        "postgres": capture_pg_env(database_url) if database_url else None,
        "host": capture_host_env(),
        "adapters": adapter_versions,
    }


def write_manifest(manifest: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(manifest, fh, indent=2)


# ────────────────────────────────────────────────────────────────────────
# README for the results directory
# ────────────────────────────────────────────────────────────────────────


def _versions_section(adapters: dict[str, dict] | None) -> str:
    if not adapters:
        return ""
    from .versions import format_revision_oneline

    lines = ["## Adapter versions", ""]
    lines.append("| System | Revision |")
    lines.append("| :--- | :--- |")
    for system in sorted(adapters):
        lines.append(
            f"| `{system}` | {format_revision_oneline(system, adapters[system])} |"
        )
    lines.append("")
    lines.append(
        "_Full detail (git SHA, branch, dirty flag, submodule describe, pinned "
        "dep version, adapter-reported runtime metadata) is in `manifest.json` "
        "under `adapters.<system>.revision`._"
    )
    lines.append("")
    return "\n".join(lines)


def write_run_readme(
    path: Path,
    *,
    scenario: str | None,
    phases: list[Phase],
    adapters: dict[str, dict] | None = None,
) -> None:
    phase_desc = " → ".join(
        f"{p.label} ({p.type.value}, {p.duration_s}s)" for p in phases
    )
    body = (
        "# Long-horizon bench run\n\n"
        f"- Scenario: `{scenario or 'custom'}`\n"
        f"- Phases: {phase_desc}\n\n"
        f"{_versions_section(adapters)}"
        "## Files\n\n"
        "- `raw.csv` — tidy long-form per-sample metrics (system × subject × metric).\n"
        "- `summary.json` — per-system per-phase aggregates + recovery metrics.\n"
        "- `manifest.json` — PG version, config, host, adapter versions, CLI args.\n"
        "- `plots/` — publication-quality plots (PNG 300dpi + SVG).\n"
        "- `index.html` — interactive offline report with timeline explorer and plot modal.\n\n"
        "## Rerun\n\n"
        "Reproduce with the exact CLI in `manifest.json -> cli`, using the same\n"
        "pinned PG image, and the same git SHA / submodule pointers recorded\n"
        "under `adapters.<system>.revision`. Results will differ slightly\n"
        "across hardware; the shape of the curves is what matters cross-system.\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)

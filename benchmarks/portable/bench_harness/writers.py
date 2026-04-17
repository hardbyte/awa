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

    # Group by (system, phase_label, metric, subject).
    bucket: dict[tuple[str, str, str, str], list[float]] = {}
    for row in rows:
        key = (row["system"], row["phase_label"], row["metric"], row["subject"])
        try:
            v = float(row["value"])
        except (TypeError, ValueError):
            continue
        bucket.setdefault(key, []).append(v)

    phase_by_label = {p.label: p for p in phases}
    out_systems: dict[str, dict] = {}

    for (system, label, metric, subject), values in bucket.items():
        phase = phase_by_label.get(label)
        if phase is None or not PHASE_INCLUDED_IN_SUMMARY.get(phase.type, True):
            continue
        sys_block = out_systems.setdefault(system, {"phases": {}})
        phase_block = sys_block["phases"].setdefault(label, {
            "phase_type": phase.type.value,
            "metrics": {},
        })
        key = metric if not subject else f"{metric}@{subject}"
        phase_block["metrics"][key] = {
            "median": _median(values),
            "peak": _peak(values),
            "count": len(values),
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
                target = out_systems[system]["phases"].setdefault(phase.label, {
                    "phase_type": phase.type.value,
                    "metrics": {},
                })
                target.update(rec)

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
            if (r["system"] == system and r["phase_label"] == phase
                    and r["metric"] == "n_dead_tup"
                    and r["subject_kind"] == "table"):
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
            cmd, capture_output=True, text=True, timeout=10, check=False,
        )
        return out.stdout.strip() or None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def capture_pg_env(database_url: str) -> dict:
    """Connect to PG and pull version + relevant config settings."""
    import psycopg
    settings_of_interest = [
        "shared_buffers", "work_mem", "maintenance_work_mem",
        "autovacuum", "autovacuum_naptime", "autovacuum_vacuum_cost_delay",
        "autovacuum_vacuum_cost_limit", "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_scale_factor", "max_connections",
        "synchronous_commit", "wal_level",
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

def write_run_readme(path: Path, *, scenario: str | None, phases: list[Phase]) -> None:
    phase_desc = " → ".join(
        f"{p.label} ({p.type.value}, {p.duration_s}s)" for p in phases
    )
    body = (
        "# Long-horizon bench run\n\n"
        f"- Scenario: `{scenario or 'custom'}`\n"
        f"- Phases: {phase_desc}\n\n"
        "## Files\n\n"
        "- `raw.csv` — tidy long-form per-sample metrics (system × subject × metric).\n"
        "- `summary.json` — per-system per-phase aggregates + recovery metrics.\n"
        "- `manifest.json` — PG version, config, host, adapter versions, CLI args.\n"
        "- `plots/` — publication-quality plots (PNG 300dpi + SVG).\n\n"
        "## Rerun\n\n"
        "Reproduce with the exact CLI in `manifest.json -> cli`, using the same\n"
        "pinned PG image. Results will differ slightly across hardware; the\n"
        "shape of the curves is what matters cross-system.\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)

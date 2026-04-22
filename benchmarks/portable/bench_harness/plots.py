"""Plot renderer for the long-horizon runner.

Outputs PNG (300 DPI) + SVG per plot with:
- Phase bands tinted by phase type, labelled at the top with duration
- End-of-line system labels (no boxed legend)
- Consistent Tableau-10 palette, reused across every plot
- LTTB decimation at render time, targeting ~1000 points per line
- Raw underlay (alpha=0.2) for spiky metrics (claim_p99, n_dead_tup)
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from .phases import PHASE_TINTS, Phase, PhaseType


# Tableau 10 (muted), reused across plots so each system has one colour
# everywhere.
_PALETTE = [
    "#4E79A7",
    "#F28E2B",
    "#59A14F",
    "#E15759",
    "#76B7B2",
    "#EDC948",
    "#B07AA1",
    "#FF9DA7",
    "#9C755F",
    "#BAB0AC",
]

# Linestyles cycled across variants within a family (e.g. awa-native solid,
# awa-docker dashed, awa-python dotted). Single-variant systems always get "-".
_VARIANT_LINESTYLES = ["-", "--", ":", "-."]


# Mapping system → (family, display_name). When a system is missing, we treat
# it as its own family with the bare system name as label — so existing
# callers that pass nothing keep working exactly as before.
SystemMeta = dict[str, tuple[str, str]]


def _assign_styles(
    systems: list[str], meta: SystemMeta | None
) -> dict[str, tuple[str, str, str]]:
    """For each system, return (color, linestyle, label).

    Systems sharing a `family` get the same base palette colour and are
    distinguished by linestyle in their listed order. Systems with no
    metadata default to (system, system) — own family, bare label.
    """
    meta = meta or {}
    family_order: list[str] = []
    by_family: dict[str, list[tuple[str, str]]] = {}
    for system in systems:
        family, label = meta.get(system, (system, system))
        if family not in by_family:
            family_order.append(family)
            by_family[family] = []
        by_family[family].append((system, label))

    out: dict[str, tuple[str, str, str]] = {}
    for family_index, family in enumerate(family_order):
        base_color = _PALETTE[family_index % len(_PALETTE)]
        for variant_index, (system, label) in enumerate(by_family[family]):
            linestyle = _VARIANT_LINESTYLES[variant_index % len(_VARIANT_LINESTYLES)]
            out[system] = (base_color, linestyle, label)
    return out


@dataclass
class PlotSpec:
    title: str
    filename_stem: str
    y_label: str
    log_scale: bool = False
    use_raw_underlay: bool = False
    sum_by_subject: bool = True  # aggregate across subject per system
    subject_kind: str | None = None  # filter to a kind (e.g. "table")


PLOT_SPECS: dict[str, PlotSpec] = {
    "n_dead_tup": PlotSpec(
        title="Dead tuples under load",
        filename_stem="dead_tuples",
        y_label="n_dead_tup (sum across event tables)",
        use_raw_underlay=True,
        subject_kind="table",
    ),
    "claim_p99_ms": PlotSpec(
        title="Claim p99 latency",
        filename_stem="claim_p99",
        y_label="claim p99 latency (ms)",
        log_scale=True,
        use_raw_underlay=True,
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "producer_p99_ms": PlotSpec(
        title="Producer p99 latency",
        filename_stem="producer_p99",
        y_label="producer p99 latency (ms)",
        log_scale=True,
        use_raw_underlay=True,
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "producer_call_p99_ms": PlotSpec(
        title="Producer call p99 latency",
        filename_stem="producer_call_p99",
        y_label="producer call p99 latency (ms)",
        log_scale=True,
        use_raw_underlay=True,
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "subscriber_p99_ms": PlotSpec(
        title="Subscriber p99 latency",
        filename_stem="subscriber_p99",
        y_label="subscriber p99 latency (ms)",
        log_scale=True,
        use_raw_underlay=True,
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "end_to_end_p99_ms": PlotSpec(
        title="End-to-end p99 latency",
        filename_stem="end_to_end_p99",
        y_label="end-to-end p99 latency (ms)",
        log_scale=True,
        use_raw_underlay=True,
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "completion_rate": PlotSpec(
        title="Completion throughput",
        filename_stem="throughput",
        y_label="completions per second (30s window)",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "completed_original_priority_1_rate": PlotSpec(
        title="Completed original priority 1 rate",
        filename_stem="completed_original_priority_1_rate",
        y_label="jobs/s",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "completed_original_priority_4_rate": PlotSpec(
        title="Completed original priority 4 rate",
        filename_stem="completed_original_priority_4_rate",
        y_label="jobs/s",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "aged_completion_rate": PlotSpec(
        title="Aged completion rate",
        filename_stem="aged_completion_rate",
        y_label="jobs/s",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "total_relation_size_mb": PlotSpec(
        title="Total table + TOAST + index size",
        filename_stem="table_size",
        y_label="size (MB, summed)",
        subject_kind="table",
    ),
    "queue_depth": PlotSpec(
        title="Queue depth",
        filename_stem="queue_depth",
        y_label="available jobs",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "running_depth": PlotSpec(
        title="Running depth",
        filename_stem="running_depth",
        y_label="running jobs",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "retryable_depth": PlotSpec(
        title="Retryable backlog",
        filename_stem="retryable_depth",
        y_label="retryable deferred jobs",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "scheduled_depth": PlotSpec(
        title="Scheduled backlog",
        filename_stem="scheduled_depth",
        y_label="scheduled deferred jobs",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
    "total_backlog": PlotSpec(
        title="Total backlog",
        filename_stem="total_backlog",
        y_label="available + running + deferred jobs",
        subject_kind="adapter",
        sum_by_subject=False,
    ),
}


# ────────────────────────────────────────────────────────────────────────
# LTTB (Largest-Triangle-Three-Buckets) decimation.
# Preserves peaks — important for dead-tuple spikes and latency tails.
# Reference: Steinarsson 2013.
# ────────────────────────────────────────────────────────────────────────


def lttb(
    xs: np.ndarray, ys: np.ndarray, threshold: int
) -> tuple[np.ndarray, np.ndarray]:
    """Downsample series (xs, ys) to `threshold` points using LTTB. If the
    input is already <= threshold, returns it unchanged."""
    n = len(xs)
    if threshold >= n or threshold <= 2:
        return xs, ys
    bucket_size = (n - 2) / (threshold - 2)
    out_x = np.empty(threshold, dtype=xs.dtype)
    out_y = np.empty(threshold, dtype=ys.dtype)
    out_x[0], out_y[0] = xs[0], ys[0]
    a = 0  # previously-selected index
    for i in range(threshold - 2):
        # Next bucket average (for triangle apex candidate).
        next_bucket_start = int(np.floor((i + 1) * bucket_size)) + 1
        next_bucket_end = int(np.floor((i + 2) * bucket_size)) + 1
        next_bucket_end = min(next_bucket_end, n)
        avg_x = (
            np.mean(xs[next_bucket_start:next_bucket_end])
            if next_bucket_end > next_bucket_start
            else xs[-1]
        )
        avg_y = (
            np.mean(ys[next_bucket_start:next_bucket_end])
            if next_bucket_end > next_bucket_start
            else ys[-1]
        )

        # Current bucket range.
        bucket_start = int(np.floor(i * bucket_size)) + 1
        bucket_end = int(np.floor((i + 1) * bucket_size)) + 1
        bucket_end = min(bucket_end, n)
        if bucket_end <= bucket_start:
            # Empty bucket — fall back to a mid-point sample.
            idx = min(bucket_start, n - 1)
            out_x[i + 1] = xs[idx]
            out_y[i + 1] = ys[idx]
            a = idx
            continue

        ax, ay = xs[a], ys[a]
        # Triangle area (2x) for each candidate in the bucket.
        dx = xs[bucket_start:bucket_end] - avg_x
        dy = ay - avg_y
        px = xs[bucket_start:bucket_end] - ax
        py = ys[bucket_start:bucket_end] - ay
        area = np.abs(dx * py - px * dy)
        best = int(np.argmax(area)) + bucket_start
        out_x[i + 1] = xs[best]
        out_y[i + 1] = ys[best]
        a = best
    out_x[-1] = xs[-1]
    out_y[-1] = ys[-1]
    return out_x, out_y


# ────────────────────────────────────────────────────────────────────────
# Data loading
# ────────────────────────────────────────────────────────────────────────


def load_raw_csv(path: Path) -> list[dict]:
    with path.open("r", newline="") as fh:
        return list(csv.DictReader(fh))


def _series_for(
    rows: list[dict],
    *,
    system: str,
    metric: str,
    subject_kind: str | None,
    sum_by_subject: bool,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (elapsed_s, value) series for one (system, metric)."""
    if sum_by_subject:
        per_elapsed: dict[float, float] = {}
        for r in rows:
            if r["system"] != system or r["metric"] != metric:
                continue
            if subject_kind and r["subject_kind"] != subject_kind:
                continue
            try:
                t = float(r["elapsed_s"])
                v = float(r["value"])
            except (TypeError, ValueError):
                continue
            per_elapsed[t] = per_elapsed.get(t, 0.0) + v
        items = sorted(per_elapsed.items())
    else:
        items = []
        for r in rows:
            if r["system"] != system or r["metric"] != metric:
                continue
            if subject_kind and r["subject_kind"] != subject_kind:
                continue
            try:
                items.append((float(r["elapsed_s"]), float(r["value"])))
            except (TypeError, ValueError):
                continue
        items.sort()
    if not items:
        return np.array([]), np.array([])
    xs = np.array([t for t, _ in items], dtype=float)
    ys = np.array([v for _, v in items], dtype=float)
    return xs, ys


def _phase_boundaries(phases: Iterable[Phase]) -> list[tuple[float, float, Phase]]:
    out: list[tuple[float, float, Phase]] = []
    t = 0.0
    for phase in phases:
        out.append((t, t + phase.duration_s, phase))
        t += phase.duration_s
    return out


# ────────────────────────────────────────────────────────────────────────
# Rendering
# ────────────────────────────────────────────────────────────────────────


def _setup_axes(
    ax: plt.Axes,
    *,
    phases: Iterable[Phase],
    title: str,
    y_label: str,
    log_scale: bool,
) -> None:
    phase_list = list(phases)
    total_s = sum(p.duration_s for p in phase_list) or 1
    for start, end, phase in _phase_boundaries(phase_list):
        tint, alpha = PHASE_TINTS[phase.type]
        ax.axvspan(start, end, color=tint, alpha=alpha, zorder=0)
        # Label at top — position inside the band.
        mid = (start + end) / 2
        ax.text(
            mid,
            1.01,
            phase.describe(),
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="bottom",
            fontsize=8,
            color="#333",
        )
    ax.set_title(title, pad=18)
    ax.set_ylabel(y_label)

    # X tick formatter: hours:minutes
    def _fmt(x, _pos):
        if total_s >= 3600:
            h, rem = divmod(int(x), 3600)
            m, _ = divmod(rem, 60)
            return f"{h}h{m:02d}m" if h else f"{m}m"
        m, s = divmod(int(x), 60)
        return f"{m}m{s:02d}s" if m else f"{s}s"

    ax.xaxis.set_major_formatter(plt.FuncFormatter(_fmt))
    ax.set_xlabel("elapsed time")
    ax.set_xlim(0, total_s)
    if log_scale:
        ax.set_yscale("log")
    ax.grid(True, axis="y", linestyle=":", color="#999", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _label_end(ax: plt.Axes, x: float, y: float, system: str, color: str) -> None:
    ax.annotate(
        f" {system}",
        xy=(x, y),
        xycoords="data",
        va="center",
        ha="left",
        color=color,
        fontsize=9,
        annotation_clip=False,
    )


def _subtitle_for(phases: Iterable[Phase]) -> str:
    parts = []
    for p in phases:
        minutes = p.duration_s / 60
        pretty = f"{minutes:g}m" if minutes >= 1 else f"{p.duration_s}s"
        parts.append(f"{p.type.value} {pretty}")
    return " → ".join(parts)


def render_plot(
    rows: list[dict],
    *,
    systems: list[str],
    metric: str,
    spec: PlotSpec,
    phases: list[Phase],
    out_dir: Path,
    lttb_target: int = 1000,
    system_meta: SystemMeta | None = None,
) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.2))
    _setup_axes(
        ax,
        phases=phases,
        title=spec.title,
        y_label=spec.y_label,
        log_scale=spec.log_scale,
    )
    fig.text(
        0.5,
        0.925,
        _subtitle_for(phases),
        ha="center",
        va="bottom",
        fontsize=9,
        color="#555",
    )

    styles = _assign_styles(systems, system_meta)
    for system in systems:
        color, linestyle, label = styles[system]
        xs, ys = _series_for(
            rows,
            system=system,
            metric=metric,
            subject_kind=spec.subject_kind,
            sum_by_subject=spec.sum_by_subject,
        )
        if xs.size == 0:
            continue
        if spec.use_raw_underlay and xs.size > lttb_target:
            ax.plot(
                xs,
                ys,
                color=color,
                alpha=0.18,
                linewidth=0.8,
                linestyle=linestyle,
                zorder=2,
            )
        px, py = lttb(xs, ys, lttb_target)
        ax.plot(px, py, color=color, linestyle=linestyle, linewidth=1.6, zorder=3)
        _label_end(ax, px[-1], py[-1], label, color)

    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"{spec.filename_stem}.png"
    svg_path = out_dir / f"{spec.filename_stem}.svg"
    fig.tight_layout(rect=[0, 0, 0.96, 0.88])
    fig.savefig(png_path, dpi=300)
    fig.savefig(svg_path)
    plt.close(fig)


def render_faceted_dead_tuples(
    rows: list[dict],
    *,
    systems: list[str],
    phases: list[Phase],
    out_dir: Path,
    lttb_target: int = 1000,
    system_meta: SystemMeta | None = None,
) -> None:
    """Small multiples, one panel per system, one line per event table."""
    if not systems:
        return
    cols = min(3, len(systems))
    rowsn = (len(systems) + cols - 1) // cols
    fig, axes = plt.subplots(
        rowsn, cols, figsize=(6.5 * cols, 4.5 * rowsn), sharex=True, squeeze=False
    )
    meta = system_meta or {}
    for idx, system in enumerate(systems):
        ax = axes[idx // cols][idx % cols]
        # One call per panel: the earlier two-pass version double-drew
        # phase bands and overwrote display titles with raw system names.
        _, panel_title = meta.get(system, (system, system))
        _setup_axes(
            ax,
            phases=phases,
            title=panel_title,
            y_label="n_dead_tup",
            log_scale=False,
        )
        subject_series: list[tuple[str, list[tuple[float, float]], float]] = []
        subjects = {
            r["subject"]
            for r in rows
            if r["system"] == system
            and r["metric"] == "n_dead_tup"
            and r["subject_kind"] == "table"
        }
        for subj in sorted(subjects):
            items = []
            for r in rows:
                if (
                    r["system"] == system
                    and r["metric"] == "n_dead_tup"
                    and r["subject"] == subj
                ):
                    try:
                        items.append((float(r["elapsed_s"]), float(r["value"])))
                    except (TypeError, ValueError):
                        continue
            items.sort()
            if items:
                subject_series.append((subj, items, max(v for _, v in items)))

        if any(peak > 0 for _, _, peak in subject_series):
            ax.set_yscale("symlog", linthresh=1.0)

        subject_series.sort(key=lambda item: item[2], reverse=True)
        label_limit = 5
        for j, (subj, items, _peak) in enumerate(subject_series):
            color = _PALETTE[j % len(_PALETTE)]
            xs = np.array([t for t, _ in items])
            ys = np.array([v for _, v in items])
            px, py = lttb(xs, ys, lttb_target)
            emphasize = j < label_limit
            ax.plot(
                px,
                py,
                color=color,
                linewidth=1.6 if emphasize else 1.0,
                alpha=0.95 if emphasize else 0.35,
                label=subj,
            )
            if emphasize:
                _label_end(ax, px[-1], py[-1], subj.split(".")[-1], color)

        if len(subject_series) > label_limit:
            ax.text(
                0.99,
                0.02,
                f"labels show top {label_limit} tables by peak",
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=8,
                color="#666",
            )

    # Blank out any extra subplot cells.
    for idx in range(len(systems), rowsn * cols):
        axes[idx // cols][idx % cols].axis("off")

    out_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    fig.savefig(out_dir / "dead_tuples_faceted.png", dpi=300)
    fig.savefig(out_dir / "dead_tuples_faceted.svg")
    plt.close(fig)


def render_all(
    raw_csv: Path,
    *,
    systems: list[str],
    phases: list[Phase],
    out_dir: Path,
    system_meta: SystemMeta | None = None,
) -> list[Path]:
    rows = load_raw_csv(raw_csv)
    out_dir.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    for metric, spec in PLOT_SPECS.items():
        render_plot(
            rows,
            systems=systems,
            metric=metric,
            spec=spec,
            phases=phases,
            out_dir=out_dir,
            system_meta=system_meta,
        )
        out.append(out_dir / f"{spec.filename_stem}.png")
    render_faceted_dead_tuples(
        rows,
        systems=systems,
        phases=phases,
        out_dir=out_dir,
        system_meta=system_meta,
    )
    out.append(out_dir / "dead_tuples_faceted.png")
    return out

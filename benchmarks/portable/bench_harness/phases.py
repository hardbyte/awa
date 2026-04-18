"""Phase DSL: label=type:duration.

Phase types register (a) a tint for the plotter and (b) runtime enter/exit
hooks — but the plot tint is always available even when the runtime hooks
haven't been installed (useful for rendering from a fixture CSV without a
live Postgres).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Callable


class PhaseType(str, Enum):
    WARMUP = "warmup"
    CLEAN = "clean"
    IDLE_IN_TX = "idle-in-tx"
    RECOVERY = "recovery"
    ACTIVE_READERS = "active-readers"
    HIGH_LOAD = "high-load"


# Matplotlib-compatible colour tints. Tuned for the dark "neutral gray" base
# used by the rest of the plots; idle-in-tx is the scream colour.
PHASE_TINTS: dict[PhaseType, tuple[str, float]] = {
    PhaseType.WARMUP:          ("#D0D0D0", 0.40),
    PhaseType.CLEAN:           ("#B8B8B8", 0.35),
    PhaseType.IDLE_IN_TX:      ("#D46A6A", 0.30),
    PhaseType.RECOVERY:        ("#DCDCDC", 0.30),
    PhaseType.ACTIVE_READERS:  ("#E0B66C", 0.30),
    PhaseType.HIGH_LOAD:       ("#A378C8", 0.30),
}

# Whether samples in this phase type feed into summary.json (warmup excluded).
PHASE_INCLUDED_IN_SUMMARY: dict[PhaseType, bool] = {
    PhaseType.WARMUP:          False,
    PhaseType.CLEAN:           True,
    PhaseType.IDLE_IN_TX:      True,
    PhaseType.RECOVERY:        True,
    PhaseType.ACTIVE_READERS:  True,
    PhaseType.HIGH_LOAD:       True,
}


@dataclass(frozen=True)
class Phase:
    label: str
    type: PhaseType
    duration_s: int

    def describe(self) -> str:
        minutes = self.duration_s / 60
        if minutes >= 1:
            pretty = f"{minutes:g}m"
        else:
            pretty = f"{self.duration_s}s"
        return f"{self.label} · {pretty}"


_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def parse_duration(text: str) -> int:
    """Parse a duration like '30s', '10m', '2h', '90' (seconds)."""
    text = text.strip().lower()
    if not text:
        raise ValueError("empty duration")
    unit_map = {"s": 1, "m": 60, "h": 3600}
    if text[-1] in unit_map:
        n = float(text[:-1])
        return int(n * unit_map[text[-1]])
    return int(text)


def parse_phase_spec(spec: str) -> Phase:
    """Parse a single --phase argument: label=type:duration."""
    if "=" not in spec or ":" not in spec:
        raise ValueError(
            f"Bad --phase spec {spec!r}; expected label=type:duration, "
            f"e.g. idle_1=idle-in-tx:60m"
        )
    label, rest = spec.split("=", 1)
    type_str, duration_str = rest.rsplit(":", 1)
    label = label.strip()
    if not _LABEL_RE.match(label):
        raise ValueError(
            f"Bad phase label {label!r}: must match [A-Za-z][A-Za-z0-9_]*"
        )
    try:
        phase_type = PhaseType(type_str.strip())
    except ValueError as exc:
        raise ValueError(
            f"Unknown phase type {type_str!r}. "
            f"Known: {', '.join(t.value for t in PhaseType)}"
        ) from exc
    duration_s = parse_duration(duration_str)
    if duration_s <= 0:
        raise ValueError(f"Phase duration must be positive, got {duration_s}s")
    return Phase(label=label, type=phase_type, duration_s=duration_s)


# Named scenarios desugar into explicit phase lists.
SCENARIOS: dict[str, list[str]] = {
    "idle_in_tx_saturation": [
        "warmup=warmup:10m",
        "clean_1=clean:60m",
        "idle_1=idle-in-tx:60m",
        "recovery_1=recovery:30m",
    ],
    "long_horizon": [
        "warmup=warmup:10m",
        "clean_1=clean:60m",
        "idle_1=idle-in-tx:60m",
        "recovery_1=recovery:120m",
        "idle_2=idle-in-tx:120m",
    ],
    # Composition examples; shorter so manual runs stay tractable.
    "sustained_high_load": [
        "warmup=warmup:10m",
        "clean_1=clean:30m",
        "pressure_1=high-load:120m",
        "recovery_1=clean:30m",
    ],
    "active_readers": [
        "warmup=warmup:10m",
        "clean_1=clean:30m",
        "readers_1=active-readers:60m",
        "recovery_1=clean:30m",
    ],
    "soak": [
        "warmup=warmup:10m",
        "clean_1=clean:6h",
    ],
}


def resolve_scenario(
    scenario: str | None,
    extra_phases: list[str] | None,
) -> list[Phase]:
    specs: list[str] = []
    if scenario is not None:
        if scenario not in SCENARIOS:
            raise ValueError(
                f"Unknown scenario {scenario!r}. "
                f"Known: {', '.join(SCENARIOS)}"
            )
        specs.extend(SCENARIOS[scenario])
    if extra_phases:
        specs.extend(extra_phases)
    if not specs:
        raise ValueError("no phases supplied (use --scenario or --phase)")
    phases = [parse_phase_spec(s) for s in specs]
    labels_seen: set[str] = set()
    for phase in phases:
        if phase.label in labels_seen:
            raise ValueError(f"Duplicate phase label: {phase.label!r}")
        labels_seen.add(phase.label)
    if phases[0].type is not PhaseType.WARMUP:
        raise ValueError(
            "First phase must be type warmup so samples can be excluded "
            "from summaries; got "
            f"{phases[0].type.value}"
        )
    return phases


# ────────────────────────────────────────────────────────────────────────
# Runtime hooks
# ────────────────────────────────────────────────────────────────────────
#
# Phase-type hooks are registered as a pair of (enter, exit) callables that
# take a live context object (the harness passes in a PhaseRuntime with a
# DB URL + adapter handle + logger). They can open side connections, raise
# producer rates on the adapter, etc.
#
# The functions here don't need a live database to import — real work happens
# inside the callables, which are imported by the orchestrator only.

PhaseHook = Callable[["PhaseRuntime"], None]


@dataclass
class PhaseRuntime:
    """Passed to phase enter/exit hooks."""
    database_url: str
    phase: Phase
    # Opaque state the hook may stash for its exit pair.
    state: dict[str, object]


class HookRegistry:
    def __init__(self) -> None:
        self._enter: dict[PhaseType, PhaseHook] = {}
        self._exit: dict[PhaseType, PhaseHook] = {}

    def register(
        self,
        phase_type: PhaseType,
        enter: PhaseHook | None = None,
        exit: PhaseHook | None = None,
    ) -> None:
        if enter:
            self._enter[phase_type] = enter
        if exit:
            self._exit[phase_type] = exit

    def enter(self, runtime: PhaseRuntime) -> None:
        hook = self._enter.get(runtime.phase.type)
        if hook:
            hook(runtime)

    def exit(self, runtime: PhaseRuntime) -> None:
        hook = self._exit.get(runtime.phase.type)
        if hook:
            hook(runtime)


def default_registry() -> HookRegistry:
    """Build the default registry, wiring in the phase-type hooks.

    Importing here avoids forcing psycopg at import-time for harness users
    who only want the DSL (e.g. the CI smoke test).
    """
    from . import hooks  # local to avoid eager psycopg import
    registry = HookRegistry()
    registry.register(PhaseType.IDLE_IN_TX,
                      enter=hooks.enter_idle_in_tx,
                      exit=hooks.exit_idle_in_tx)
    registry.register(PhaseType.ACTIVE_READERS,
                      enter=hooks.enter_active_readers,
                      exit=hooks.exit_active_readers)
    registry.register(PhaseType.HIGH_LOAD,
                      enter=hooks.enter_high_load,
                      exit=hooks.exit_high_load)
    # warmup, clean, recovery — no extra runtime action; the adapter's
    # steady workload carries the load.
    return registry

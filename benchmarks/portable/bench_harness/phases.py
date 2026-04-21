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
    # Destructive / lifecycle phases. See UNIFIED_DRIVER_DESIGN.md §2.
    KILL_WORKER = "kill-worker"
    START_WORKER = "start-worker"


# Matplotlib-compatible colour tints. Tuned for the dark "neutral gray" base
# used by the rest of the plots; idle-in-tx is the scream colour.
PHASE_TINTS: dict[PhaseType, tuple[str, float]] = {
    PhaseType.WARMUP:          ("#D0D0D0", 0.40),
    PhaseType.CLEAN:           ("#B8B8B8", 0.35),
    PhaseType.IDLE_IN_TX:      ("#D46A6A", 0.30),
    PhaseType.RECOVERY:        ("#DCDCDC", 0.30),
    PhaseType.ACTIVE_READERS:  ("#E0B66C", 0.30),
    PhaseType.HIGH_LOAD:       ("#A378C8", 0.30),
    # Destructive: kill is scream-red, start is calm-green.
    PhaseType.KILL_WORKER:     ("#C04A4A", 0.35),
    PhaseType.START_WORKER:    ("#6CAF6C", 0.25),
}

# Whether samples in this phase type feed into summary.json (warmup excluded).
# Destructive phases are *included* — the whole point is to capture the system
# behaviour during and after the destructive action.
PHASE_INCLUDED_IN_SUMMARY: dict[PhaseType, bool] = {
    PhaseType.WARMUP:          False,
    PhaseType.CLEAN:           True,
    PhaseType.IDLE_IN_TX:      True,
    PhaseType.RECOVERY:        True,
    PhaseType.ACTIVE_READERS:  True,
    PhaseType.HIGH_LOAD:       True,
    PhaseType.KILL_WORKER:     True,
    PhaseType.START_WORKER:    True,
}


@dataclass(frozen=True)
class Phase:
    label: str
    type: PhaseType
    duration_s: int
    # Type-specific parameters parsed from the spec's optional parenthesised
    # clause. For `kill-worker(instance=0)`, params == {"instance": "0"}.
    # Frozen dict-of-str-to-str: enough for every destructive phase type in
    # §2 of the design. A richer schema (ints, lists) can be layered on in
    # a future iteration; today instance indexing is the only consumer.
    params: tuple[tuple[str, str], ...] = ()

    def describe(self) -> str:
        minutes = self.duration_s / 60
        if minutes >= 1:
            pretty = f"{minutes:g}m"
        else:
            pretty = f"{self.duration_s}s"
        return f"{self.label} · {pretty}"

    def param(self, name: str, default: str | None = None) -> str | None:
        for k, v in self.params:
            if k == name:
                return v
        return default

    def int_param(self, name: str, default: int) -> int:
        raw = self.param(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(
                f"phase {self.label!r}: param {name!r} must be an integer, "
                f"got {raw!r}"
            ) from exc


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


_TYPE_WITH_PARAMS_RE = re.compile(
    r"^(?P<type>[a-z][a-z0-9-]*)"  # type name, kebab-case
    r"(?:\((?P<params>[^)]*)\))?$"  # optional `(k=v,k=v)` params
)


def _parse_type_and_params(type_str: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Split `kill-worker(instance=0,signal=term)` into ("kill-worker",
    (("instance","0"),("signal","term"))). Legacy `idle-in-tx` parses to
    ("idle-in-tx", ()).
    """
    m = _TYPE_WITH_PARAMS_RE.match(type_str.strip())
    if not m:
        raise ValueError(
            f"Bad phase type spec {type_str!r}; expected "
            "<type> or <type>(key=value[,key=value])"
        )
    raw_params = m.group("params") or ""
    params: list[tuple[str, str]] = []
    if raw_params:
        for kv in raw_params.split(","):
            if "=" not in kv:
                raise ValueError(
                    f"Bad param in phase type {type_str!r}: expected k=v, got {kv!r}"
                )
            k, v = kv.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                raise ValueError(f"Bad param in phase type {type_str!r}: empty key")
            params.append((k, v))
    return m.group("type"), tuple(params)


def parse_phase_spec(spec: str) -> Phase:
    """Parse a single --phase argument: label=type[(k=v,...)]:duration.

    Examples:
        warmup_1=warmup:10m            — no params
        kill=kill-worker(instance=0):60s — instance selector for the
                                           destructive action
    """
    if "=" not in spec or ":" not in spec:
        raise ValueError(
            f"Bad --phase spec {spec!r}; expected label=type:duration, "
            f"e.g. idle_1=idle-in-tx:60m"
        )
    label, rest = spec.split("=", 1)
    # Duration is always the last `:`-separated segment. We rsplit on `:`
    # so `kill-worker(instance=0):60s` — whose params section never
    # contains `:` — splits cleanly at the duration boundary.
    type_str, duration_str = rest.rsplit(":", 1)
    label = label.strip()
    if not _LABEL_RE.match(label):
        raise ValueError(
            f"Bad phase label {label!r}: must match [A-Za-z][A-Za-z0-9_]*"
        )
    type_name, params = _parse_type_and_params(type_str)
    try:
        phase_type = PhaseType(type_name)
    except ValueError as exc:
        raise ValueError(
            f"Unknown phase type {type_name!r}. "
            f"Known: {', '.join(t.value for t in PhaseType)}"
        ) from exc
    duration_s = parse_duration(duration_str)
    if duration_s <= 0:
        raise ValueError(f"Phase duration must be positive, got {duration_s}s")
    return Phase(label=label, type=phase_type, duration_s=duration_s, params=params)


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
    # Broad, balanced event/message-delivery comparison profile.
    # Starts at steady-state, adds subscriber/read pressure, then pushes
    # backlog pressure with high-load before finishing in a clean tail.
    "event_delivery_matrix": [
        "warmup=warmup:10m",
        "clean_1=clean:20m",
        "readers_1=active-readers:20m",
        "pressure_1=high-load:20m",
        "recovery_1=clean:20m",
    ],
    # Message-bus burst / catch-up profile. Use to compare how systems
    # absorb a sustained oversupply of work and how quickly they drain
    # once offered load returns to baseline.
    "event_delivery_burst": [
        "warmup=warmup:10m",
        "clean_1=clean:15m",
        "pressure_1=high-load:45m",
        "recovery_1=clean:30m",
    ],
    # Multi-replica steady-state comparison. Intended to be run with
    # `--replicas >= 2`; the phase sequence itself is nondestructive.
    "fleet_steady_state": [
        "warmup=warmup:10m",
        "clean_1=clean:30m",
        "readers_1=active-readers:30m",
    ],
    "soak": [
        "warmup=warmup:10m",
        "clean_1=clean:6h",
    ],
    # Replaces the legacy chaos.py `scenario_crash_recovery`. Harsh kill
    # of replica 0, then restart and measure recovery. Pass/fail answers
    # (`jobs_lost`, `recovery_time`) are derived from the shared raw.csv
    # time series — see UNIFIED_DRIVER_DESIGN.md §5.
    #
    # Meaningful only with `--replicas >=2`; the kill of replica 0 lets
    # replica 1 carry the load through the kill phase. Single-replica
    # runs technically work but the "recovery" phase simply measures
    # time-to-empty after restart.
    "crash_recovery": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "kill=kill-worker(instance=0):60s",
        "restart=start-worker(instance=0):60s",
    ],
    # Crash a replica while the fleet is already under backlog pressure, then
    # restart it and watch the tail recover. Intended for --replicas >= 2.
    "crash_recovery_under_load": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "pressure_1=high-load:120s",
        "kill=kill-worker(instance=0):60s",
        "restart=start-worker(instance=0):60s",
        "recovery_1=clean:120s",
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
    # Destructive phase types act on the replica pool via
    # state["replica_pool"] — see hooks.enter_kill_worker / enter_start_worker.
    registry.register(PhaseType.KILL_WORKER,
                      enter=hooks.enter_kill_worker)
    registry.register(PhaseType.START_WORKER,
                      enter=hooks.enter_start_worker)
    # warmup, clean, recovery — no extra runtime action; the adapter's
    # steady workload carries the load.
    return registry

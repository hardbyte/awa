"""Long-horizon benchmark harness — orchestrator modules."""

from .phases import (
    Phase,
    PhaseType,
    PHASE_TINTS,
    parse_phase_spec,
    parse_duration,
    SCENARIOS,
    resolve_scenario,
)

__all__ = [
    "PHASE_TINTS",
    "Phase",
    "PhaseType",
    "SCENARIOS",
    "parse_duration",
    "parse_phase_spec",
    "resolve_scenario",
]

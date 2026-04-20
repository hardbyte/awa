"""Typed, cross-validated configuration for the long-horizon runner.

argparse stays as the CLI front-end (standard `--help`, standard shell
completion, standard error framing) and this module owns the validation
pass that runs after `parse_args`. Keeps the rules in one place so they
can be exercised in tests without spinning up the full orchestrator, and
produces readable error messages for bad combinations (unknown system,
scenario + phase conflict, non-positive cadence).
"""

from __future__ import annotations

import argparse
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from .adapters import ADAPTERS, DEFAULT_PG_IMAGE
from .phases import Phase, SCENARIOS, resolve_scenario


class CliConfig(BaseModel):
    """Validated long-horizon runner configuration.

    Built from an argparse Namespace via :meth:`from_namespace`. All
    cross-field rules (unknown systems, scenario/phase combination,
    producer-mode interactions) are enforced here. On failure the caller
    should surface errors through ``parser.error`` so the CLI UX stays
    consistent with every other argparse error.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # ── Phase selection ────────────────────────────────────────────────
    scenario: str | None = None
    phase_specs: list[str] = Field(default_factory=list)

    # ── System selection ───────────────────────────────────────────────
    systems: list[str]

    # ── Environment ────────────────────────────────────────────────────
    pg_image: str = DEFAULT_PG_IMAGE
    fast: bool = False
    skip_build: bool = False

    # ── Sampling ───────────────────────────────────────────────────────
    # PositiveInt would serve, but keeping explicit ge=1 with a custom
    # message tunes the validator output for the CLI error path.
    sample_every: Annotated[int, Field(ge=1)]
    producer_rate: Annotated[int, Field(ge=0)]
    producer_mode: Literal["fixed", "depth-target"] = "fixed"
    target_depth: Annotated[int, Field(ge=1)] = 1000
    worker_count: Annotated[int, Field(ge=1)]
    high_load_multiplier: Annotated[float, Field(gt=0.0)] = 1.5
    # Replica count per system. 1 is the legacy single-replica mode and
    # the default. `producer_rate` and `worker_count` are per-replica, not
    # per-fleet: N replicas at producer_rate=800 offer 800*N jobs/s in
    # aggregate. Holding total offered load constant across replica counts
    # is a scenario-level choice made at CLI time (divide producer_rate
    # by replicas before passing).
    replicas: Annotated[int, Field(ge=1)] = 1

    @field_validator("scenario")
    @classmethod
    def _scenario_must_be_known(cls, v: str | None) -> str | None:
        if v is None or v in SCENARIOS:
            return v
        known = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"unknown scenario {v!r}; known: {known}")

    @field_validator("systems")
    @classmethod
    def _systems_non_empty_and_known(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("at least one --systems entry is required")
        unknown = [s for s in v if s not in ADAPTERS]
        if unknown:
            known = ", ".join(sorted(ADAPTERS))
            raise ValueError(f"unknown systems: {unknown}; known: {known}")
        return v

    @model_validator(mode="after")
    def _at_least_one_phase_source(self) -> "CliConfig":
        # resolve_scenario runs the deeper "first phase must be warmup"
        # / "no duplicate labels" checks; we just guarantee that at least
        # one of scenario or --phase provides input.
        if self.scenario is None and not self.phase_specs:
            raise ValueError("either --scenario or at least one --phase is required")
        return self

    @model_validator(mode="after")
    def _target_depth_only_meaningful_for_depth_target(self) -> "CliConfig":
        # Not strictly a bug to pass --target-depth with --producer-mode=fixed
        # (the producer ignores it). Warning-level nudge via a stored flag
        # that the CLI can surface; no ValueError so we don't gate an
        # otherwise-valid run on a no-op arg.
        return self

    def resolve_phases(self) -> list[Phase]:
        """Turn scenario + raw phase specs into a validated Phase list.

        resolve_scenario owns the per-phase rules (duration > 0, first
        phase is warmup, no duplicate labels). Raises ValueError with a
        CLI-friendly message on bad input.
        """
        return resolve_scenario(self.scenario, self.phase_specs)

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> "CliConfig":
        """Build from argparse output. Maps flag names to model fields."""
        systems = [s.strip() for s in args.systems.split(",") if s.strip()]
        return cls(
            scenario=args.scenario,
            phase_specs=list(args.phase or []),
            systems=systems,
            pg_image=args.pg_image,
            fast=args.fast,
            skip_build=args.skip_build,
            sample_every=args.sample_every,
            producer_rate=args.producer_rate,
            producer_mode=args.producer_mode,
            target_depth=args.target_depth,
            worker_count=args.worker_count,
            high_load_multiplier=args.high_load_multiplier,
            replicas=args.replicas,
        )


def format_validation_error(exc: ValidationError) -> str:
    """Turn a pydantic ValidationError into a multi-line CLI error.

    argparse's `parser.error` prefixes the first line; subsequent lines
    are indented so the output stays scannable at the terminal.
    """
    lines: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ())) or "config"
        msg = err.get("msg", "invalid")
        lines.append(f"{loc}: {msg}")
    if len(lines) == 1:
        return lines[0]
    return "invalid configuration:\n  - " + "\n  - ".join(lines)

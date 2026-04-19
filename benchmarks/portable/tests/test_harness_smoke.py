"""Smoke tests for the long-horizon harness. No DB, no Docker.

These run in the PR-blocking `bench-harness-smoke` CI job. They must finish
in well under a minute.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from bench_harness.phases import (
    PHASE_TINTS,
    Phase,
    PhaseType,
    SCENARIOS,
    parse_duration,
    parse_phase_spec,
    resolve_scenario,
)
from bench_harness.plots import PLOT_SPECS, lttb, render_all
from bench_harness.sample import RAW_CSV_HEADER
from bench_harness.writers import compute_summary, write_run_readme

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_CSV = FIXTURES / "raw_idle_in_tx_saturation.csv"


# ─── Phase DSL ──────────────────────────────────────────────────────

def test_duration_parsing():
    assert parse_duration("30s") == 30
    assert parse_duration("10m") == 600
    assert parse_duration("2h") == 7200
    assert parse_duration("45") == 45


def test_phase_spec_parsing():
    p = parse_phase_spec("idle_1=idle-in-tx:60m")
    assert p == Phase(label="idle_1", type=PhaseType.IDLE_IN_TX, duration_s=3600)


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_scenarios_resolve_and_start_with_warmup(scenario: str):
    phases = resolve_scenario(scenario, None)
    assert phases
    assert phases[0].type is PhaseType.WARMUP, (
        "first phase must be warmup so samples can be excluded from summary"
    )


def test_duplicate_label_rejected():
    with pytest.raises(ValueError):
        resolve_scenario(None, [
            "warmup=warmup:10s",
            "clean_1=clean:10s",
            "clean_1=clean:10s",
        ])


def test_first_phase_must_be_warmup():
    with pytest.raises(ValueError):
        resolve_scenario(None, ["clean_1=clean:10s"])


def test_phase_tints_cover_all_types():
    for pt in PhaseType:
        assert pt in PHASE_TINTS


# ─── Raw CSV schema ─────────────────────────────────────────────────

def test_raw_csv_header_matches_spec():
    expected = [
        "run_id", "system", "elapsed_s", "sampled_at",
        "phase_label", "phase_type", "subject_kind", "subject",
        "metric", "value", "window_s",
    ]
    assert RAW_CSV_HEADER == expected


def test_fixture_rows_match_header():
    with FIXTURE_CSV.open() as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == RAW_CSV_HEADER
        for row in reader:
            # Value should be a float.
            float(row["value"])


# ─── Summary ────────────────────────────────────────────────────────

def test_summary_on_fixture_excludes_warmup_and_finds_recovery():
    phases = [
        parse_phase_spec("warmup=warmup:60s"),
        parse_phase_spec("clean_1=clean:90s"),
        parse_phase_spec("idle_1=idle-in-tx:90s"),
        parse_phase_spec("recovery_1=recovery:90s"),
    ]
    summary = compute_summary(
        FIXTURE_CSV, run_id="fixture-run", scenario="idle_in_tx_saturation",
        phases=phases,
    )
    assert "awa" in summary["systems"]
    phases_out = summary["systems"]["awa"]["phases"]
    assert "clean_1" in phases_out
    assert "idle_1" in phases_out
    assert "recovery_1" in phases_out
    # Warmup must be excluded from summary.
    assert "warmup" not in phases_out
    # Recovery metrics populated — both halflife and to_baseline present.
    assert phases_out["recovery_1"]["recovery_halflife_s"] is not None
    assert phases_out["recovery_1"]["recovery_to_baseline_s"] is not None


# ─── Plots ──────────────────────────────────────────────────────────

def test_lttb_preserves_endpoints():
    import numpy as np
    xs = np.arange(1000.0)
    ys = np.sin(xs / 50) * 100 + 500
    px, py = lttb(xs, ys, 100)
    assert len(px) == 100
    assert px[0] == xs[0] and px[-1] == xs[-1]


def test_plot_renderer_produces_all_files(tmp_path: Path):
    phases = [
        parse_phase_spec("warmup=warmup:60s"),
        parse_phase_spec("clean_1=clean:90s"),
        parse_phase_spec("idle_1=idle-in-tx:90s"),
        parse_phase_spec("recovery_1=recovery:90s"),
    ]
    out = tmp_path / "plots"
    render_all(FIXTURE_CSV, systems=["awa", "river"], phases=phases, out_dir=out)
    for spec in PLOT_SPECS.values():
        assert (out / f"{spec.filename_stem}.png").exists()
        assert (out / f"{spec.filename_stem}.svg").exists()
    assert (out / "dead_tuples_faceted.png").exists()


def test_run_readme_written(tmp_path: Path):
    phases = [parse_phase_spec("warmup=warmup:10s")]
    write_run_readme(tmp_path / "README.md", scenario="custom", phases=phases)
    body = (tmp_path / "README.md").read_text()
    assert "warmup" in body
    assert "raw.csv" in body


# ─── CliConfig validation ───────────────────────────────────────────

from bench_harness.config import CliConfig, format_validation_error
from pydantic import ValidationError


def _config_kwargs(**overrides):
    base = dict(
        scenario="long_horizon",
        phase_specs=[],
        systems=["awa"],
        pg_image="postgres:17.2-alpine",
        fast=False,
        skip_build=False,
        sample_every=10,
        producer_rate=800,
        producer_mode="fixed",
        target_depth=1000,
        worker_count=32,
        high_load_multiplier=1.5,
    )
    base.update(overrides)
    return base


def test_config_happy_path():
    config = CliConfig(**_config_kwargs())
    phases = config.resolve_phases()
    assert phases[0].type.value == "warmup"
    assert config.systems == ["awa"]


def test_config_rejects_non_positive_sample_every():
    with pytest.raises(ValidationError) as exc:
        CliConfig(**_config_kwargs(sample_every=0))
    assert "sample_every" in format_validation_error(exc.value)


def test_config_rejects_unknown_system():
    with pytest.raises(ValidationError) as exc:
        CliConfig(**_config_kwargs(systems=["awa", "bogus"]))
    assert "bogus" in format_validation_error(exc.value)


def test_config_rejects_empty_systems():
    with pytest.raises(ValidationError):
        CliConfig(**_config_kwargs(systems=[]))


def test_config_rejects_unknown_scenario():
    with pytest.raises(ValidationError) as exc:
        CliConfig(**_config_kwargs(scenario="nope"))
    assert "nope" in format_validation_error(exc.value)


def test_config_requires_scenario_or_phase():
    with pytest.raises(ValidationError) as exc:
        CliConfig(**_config_kwargs(scenario=None, phase_specs=[]))
    assert "--scenario" in format_validation_error(exc.value)


def test_config_accepts_phase_specs_without_scenario():
    config = CliConfig(**_config_kwargs(
        scenario=None,
        phase_specs=["warmup=warmup:10s", "clean_1=clean:30s"],
    ))
    phases = config.resolve_phases()
    assert [p.label for p in phases] == ["warmup", "clean_1"]


def test_config_rejects_negative_producer_rate():
    with pytest.raises(ValidationError):
        CliConfig(**_config_kwargs(producer_rate=-1))


def test_config_rejects_zero_worker_count():
    with pytest.raises(ValidationError):
        CliConfig(**_config_kwargs(worker_count=0))


def test_config_producer_mode_literal():
    with pytest.raises(ValidationError):
        CliConfig(**_config_kwargs(producer_mode="nope"))


def test_format_validation_error_is_readable():
    try:
        CliConfig(**_config_kwargs(sample_every=0, systems=["bogus"]))
    except ValidationError as exc:
        formatted = format_validation_error(exc)
        # Multi-error: starts with "invalid configuration" prefix
        # and one bullet per error.
        assert "invalid configuration" in formatted
        assert formatted.count("\n  - ") >= 2


# ── versions / README revision rendering ────────────────────────────────


from bench_harness.versions import capture_adapter_revision, capture_all
from bench_harness.writers import write_run_readme


def test_versions_known_systems_return_dicts():
    # Every registered adapter must get *some* dict back — the harness is
    # the authoritative source on what was compared, so silently returning
    # nothing would be a reporting regression.
    for system in ("awa", "awa-docker", "awa-python", "procrastinate", "river", "oban", "pgque"):
        rev = capture_adapter_revision(system)
        assert isinstance(rev, dict)
        assert "source" in rev


def test_versions_unknown_system_is_still_a_dict():
    rev = capture_adapter_revision("totally-made-up")
    assert rev.get("source") == "unknown"
    assert "note" in rev


def test_versions_capture_all_covers_each():
    out = capture_all(["awa", "procrastinate"])
    assert set(out) == {"awa", "procrastinate"}


def test_versions_upstream_pins_resolve():
    # These are read from pyproject.toml / go.mod / mix.exs — regressions
    # here (e.g. regex drift after a file reformat) silently drop versions
    # from the report, so pin them explicitly.
    assert capture_adapter_revision("procrastinate").get("pinned_version")
    assert capture_adapter_revision("river").get("pinned_version")
    assert capture_adapter_revision("oban").get("pinned_version_constraint")


def test_readme_includes_versions_table(tmp_path: Path):
    phases = [
        parse_phase_spec("warmup_1=warmup:10s"),
        parse_phase_spec("smoke=clean:20s"),
    ]
    adapters = {
        "awa": {
            "version": "0.5.4-alpha.1",
            "schema_version": "current",
            "revision": {
                "source": "awa repo",
                "git_short": "abc1234",
                "git_branch": "main",
                "dirty": False,
            },
        },
        "procrastinate": {
            "version": "3.7.3",
            "revision": {
                "source": "procrastinate-bench/pyproject.toml",
                "library": "procrastinate",
                "pinned_version": "3.7.3",
            },
        },
        "pgque": {
            "revision": {
                "source": "awa repo + pgque submodule",
                "git_short": "abc1234",
                "git_branch": "main",
                "dirty": False,
                "pgque_submodule_sha": "3b75f585c3d3fe3985a1688266d0f232c79213ec",
                "pgque_submodule_describe": "alpha3-5-g3b75f58",
            },
        },
    }
    out = tmp_path / "README.md"
    write_run_readme(out, scenario="custom", phases=phases, adapters=adapters)
    body = out.read_text()
    assert "Adapter versions" in body
    assert "abc1234" in body  # awa SHA
    assert "procrastinate" in body and "3.7.3" in body  # pinned upstream
    assert "3b75f58" in body  # pgque submodule short SHA


def test_readme_omits_section_when_no_adapters(tmp_path: Path):
    phases = [parse_phase_spec("warmup_1=warmup:10s")]
    out = tmp_path / "README.md"
    write_run_readme(out, scenario=None, phases=phases, adapters=None)
    assert "Adapter versions" not in out.read_text()

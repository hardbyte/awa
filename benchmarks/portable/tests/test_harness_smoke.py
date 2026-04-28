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
from bench_harness.report import write_interactive_report
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
        "run_id", "system", "instance_id", "elapsed_s", "sampled_at",
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


def test_interactive_report_written(tmp_path: Path):
    phases = [
        parse_phase_spec("warmup=warmup:60s"),
        parse_phase_spec("clean_1=clean:90s"),
        parse_phase_spec("idle_1=idle-in-tx:90s"),
        parse_phase_spec("recovery_1=recovery:90s"),
    ]
    out = tmp_path / "run"
    plots = out / "plots"
    plots.mkdir(parents=True)
    render_all(FIXTURE_CSV, systems=["awa", "river"], phases=phases, out_dir=plots)
    summary = compute_summary(
        FIXTURE_CSV, run_id="fixture-run", scenario="idle_in_tx_saturation", phases=phases
    )
    manifest = {
        "run_id": "fixture-run",
        "scenario": "idle_in_tx_saturation",
        "pg_image": "postgres:17.2-alpine",
        "cli": ["long_horizon.py", "run", "--scenario", "idle_in_tx_saturation"],
    }
    report = write_interactive_report(
        run_dir=out,
        raw_csv=FIXTURE_CSV,
        summary=summary,
        manifest=manifest,
        phases=phases,
        systems=["awa", "river"],
    )
    body = report.read_text()
    assert "Timeline Explorer" in body
    assert "phase-summary-table" in body
    assert "plots/throughput.svg" in body


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
    for system in ("awa", "awa-canonical", "awa-docker", "awa-python", "procrastinate", "river", "oban", "pgque", "pgmq", "pgboss"):
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
    assert capture_adapter_revision("pgmq").get("pinned_version")
    assert capture_adapter_revision("pgboss").get("pinned_version")


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
        "pgboss": {
            "revision": {
                "source": "pgboss-bench/package.json",
                "library": "pg-boss",
                "pinned_version": "12.15.0",
            },
        },
        "pgmq": {
            "revision": {
                "source": "pgmq-bench/main.py",
                "library": "pgmq extension",
                "pg_image": "ghcr.io/pgmq/pg18-pgmq:v1.10.0",
                "pinned_version": "v1.10.0",
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
    assert "pgmq extension" in body and "v1.10.0" in body
    assert "pg-boss" in body and "12.15.0" in body


def test_readme_omits_section_when_no_adapters(tmp_path: Path):
    phases = [parse_phase_spec("warmup_1=warmup:10s")]
    out = tmp_path / "README.md"
    write_run_readme(out, scenario=None, phases=phases, adapters=None)
    assert "Adapter versions" not in out.read_text()


# ── instance_id plumbing ────────────────────────────────────────────────


from bench_harness.metrics import parse_adapter_record


def _dummy_phase():
    return ("smoke", "clean")


def test_parse_adapter_record_defaults_instance_id_to_zero():
    line = '{"kind":"adapter","metric":"depth","value":42,"window_s":10}'
    sample = parse_adapter_record(
        line,
        run_id="r1",
        expected_system="awa",
        bench_start=0.0,
        get_phase=_dummy_phase,
    )
    assert sample is not None
    assert sample.instance_id == 0


def test_parse_adapter_record_honours_instance_id():
    line = '{"kind":"adapter","metric":"depth","value":42,"window_s":10,"instance_id":2}'
    sample = parse_adapter_record(
        line,
        run_id="r1",
        expected_system="awa",
        bench_start=0.0,
        get_phase=_dummy_phase,
    )
    assert sample is not None
    assert sample.instance_id == 2


def test_parse_adapter_record_rejects_garbage_instance_id_silently():
    # Malformed adapter output shouldn't poison the sample — fall back to 0.
    line = '{"kind":"adapter","metric":"depth","value":42,"window_s":10,"instance_id":"not-a-number"}'
    sample = parse_adapter_record(
        line,
        run_id="r1",
        expected_system="awa",
        bench_start=0.0,
        get_phase=_dummy_phase,
    )
    assert sample is not None
    assert sample.instance_id == 0


# ── --replicas CLI knob ─────────────────────────────────────────────────


def test_config_replicas_defaults_to_one():
    config = CliConfig(**_config_kwargs())
    assert config.replicas == 1


def test_config_replicas_accepts_positive_ints():
    config = CliConfig(**_config_kwargs(replicas=5))
    assert config.replicas == 5


def test_config_rejects_zero_replicas():
    # Zero is not "no adapter" — it's a confused CLI invocation. Fail
    # clearly so the operator doesn't watch a no-op run complete.
    with pytest.raises(ValidationError):
        CliConfig(**_config_kwargs(replicas=0))


def test_config_rejects_negative_replicas():
    with pytest.raises(ValidationError):
        CliConfig(**_config_kwargs(replicas=-1))


def test_argparse_replicas_flag():
    from bench_harness.orchestrator import build_parser

    p = build_parser()
    ns = p.parse_args(["run", "--scenario", "idle_in_tx_saturation", "--replicas", "3"])
    assert ns.replicas == 3
    # Default survives when flag is absent.
    ns = p.parse_args(["run", "--scenario", "idle_in_tx_saturation"])
    assert ns.replicas == 1


# ── ReplicaPool state machine ───────────────────────────────────────────


import threading as _threading
from unittest.mock import MagicMock

from bench_harness.replica_pool import ReplicaPool, ReplicaState


def _fake_launch_fn(
    *,
    descriptor_override: dict | None = None,
) -> tuple:
    """Build a (launch_fn, launch_calls, processes) triple for tests.

    The fake never actually spawns a subprocess. It returns a `MagicMock`
    standing in for `subprocess.Popen` with a controllable `poll()` and
    `send_signal()`. Tests drive state transitions by flipping the
    process's `poll_value` manually.
    """
    calls: list[int] = []
    processes: list[MagicMock] = []

    def launch(instance_id: int):
        calls.append(instance_id)
        proc = MagicMock()
        proc.poll = MagicMock(return_value=None)  # alive by default
        proc.returncode = None
        proc.send_signal = MagicMock(
            side_effect=lambda _: (
                setattr(proc, "returncode", 0),
                proc.poll.configure_mock(return_value=0),
            )
        )
        proc.wait = MagicMock(return_value=0)
        proc.kill = MagicMock(
            side_effect=lambda: (
                setattr(proc, "returncode", -9),
                proc.poll.configure_mock(return_value=-9),
            )
        )
        processes.append(proc)
        tailer = _threading.Thread(target=lambda: None)
        tailer.start()
        stop_event = _threading.Event()
        desc = descriptor_override or {
            "system": "awa",
            "instance_id": instance_id,
            "event_tables": ["awa.jobs_hot"],
            "extensions": [],
        }
        return proc, tailer, stop_event, desc

    return launch, calls, processes


def test_pool_construction_slots_are_unstarted():
    launch, _calls, _procs = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=3, launch_fn=launch)
    assert [s.state for s in pool.slots] == [ReplicaState.UNSTARTED] * 3
    assert pool.descriptor is None


def test_pool_rejects_non_positive_capacity():
    launch, _, _ = _fake_launch_fn()
    with pytest.raises(ValueError):
        ReplicaPool(system="awa", capacity=0, launch_fn=launch)


def test_pool_start_all_transitions_to_running():
    launch, calls, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=3, launch_fn=launch)
    pool.start_all()
    assert calls == [0, 1, 2]
    assert [s.state for s in pool.slots] == [ReplicaState.RUNNING] * 3
    assert pool.descriptor is not None


def test_pool_stop_worker_marks_stopped():
    launch, _, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=2, launch_fn=launch)
    pool.start_all()
    pool.stop_worker(0)
    assert pool.slot(0).state is ReplicaState.STOPPED
    assert pool.slot(1).state is ReplicaState.RUNNING


def test_pool_kill_worker_marks_killed():
    launch, _, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=1, launch_fn=launch)
    pool.start_all()
    pool.kill_worker(0)
    assert pool.slot(0).state is ReplicaState.KILLED


def test_pool_restart_worker_goes_back_to_running():
    launch, calls, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=1, launch_fn=launch)
    pool.start_all()
    pool.restart_worker(0)
    assert calls == [0, 0]  # launched twice for the same slot
    assert pool.slot(0).state is ReplicaState.RUNNING


def test_pool_start_worker_on_running_slot_raises():
    launch, _, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=1, launch_fn=launch)
    pool.start_all()
    with pytest.raises(RuntimeError, match="already RUNNING"):
        pool.start_worker(0)


def test_pool_detect_crashes_flips_state_and_returns_slot():
    launch, _, procs = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=2, launch_fn=launch)
    pool.start_all()
    # Simulate replica 1 crashing: poll() starts returning an exit code.
    procs[1].returncode = 1
    procs[1].poll.configure_mock(return_value=1)
    crashed = pool.detect_crashes()
    assert [s.instance_id for s in crashed] == [1]
    assert pool.slot(1).state is ReplicaState.CRASHED
    # Second call must not re-report — state is no longer RUNNING.
    assert pool.detect_crashes() == []


def test_pool_detect_crashes_ignores_deliberate_stops():
    launch, _, procs = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=2, launch_fn=launch)
    pool.start_all()
    pool.stop_worker(0)  # deliberate — slot 0 is now STOPPED
    # Even though the (simulated) process is no longer alive, detect_crashes
    # won't treat this as a fault because the state transitioned away from
    # RUNNING before we looked.
    assert pool.detect_crashes() == []


def test_pool_stop_worker_on_non_running_is_noop():
    launch, _, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=1, launch_fn=launch)
    # UNSTARTED → stop is a no-op.
    pool.stop_worker(0)
    assert pool.slot(0).state is ReplicaState.UNSTARTED
    pool.start_all()
    pool.stop_worker(0)
    # STOPPED → stop again is a no-op.
    pool.stop_worker(0)
    assert pool.slot(0).state is ReplicaState.STOPPED


def test_pool_start_all_tears_down_partial_on_failure():
    # Two replicas succeed, the third fails mid-launch. The pool must
    # tear down 0 and 1 before propagating.
    counter = {"n": 0}

    def flaky_launch(instance_id: int):
        counter["n"] += 1
        if instance_id == 2:
            raise RuntimeError("simulated descriptor handshake failure")
        proc = MagicMock()
        proc.poll = MagicMock(return_value=None)
        proc.returncode = None
        proc.send_signal = MagicMock(
            side_effect=lambda _: (
                setattr(proc, "returncode", 0),
                proc.poll.configure_mock(return_value=0),
            )
        )
        proc.wait = MagicMock(return_value=0)
        proc.kill = MagicMock()
        tailer = _threading.Thread(target=lambda: None)
        tailer.start()
        return proc, tailer, _threading.Event(), {"instance_id": instance_id}

    pool = ReplicaPool(system="awa", capacity=3, launch_fn=flaky_launch)
    with pytest.raises(RuntimeError, match="simulated descriptor handshake"):
        pool.start_all()
    # Slots 0 and 1 were launched then torn down — handle cleared.
    assert pool.slot(0).process is None
    assert pool.slot(1).process is None


def test_pool_out_of_range_instance_id_raises():
    launch, _, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=2, launch_fn=launch)
    with pytest.raises(IndexError):
        pool.slot(5)
    with pytest.raises(IndexError):
        pool.slot(-1)


def test_pool_stop_all_is_idempotent():
    launch, _, _ = _fake_launch_fn()
    pool = ReplicaPool(system="awa", capacity=2, launch_fn=launch)
    pool.start_all()
    pool.kill_worker(0)  # now slot 0 is KILLED (not RUNNING)
    pool.stop_all()      # only slot 1 needs stopping
    assert pool.slot(0).state is ReplicaState.KILLED
    assert pool.slot(1).state is ReplicaState.STOPPED
    # Second stop_all is a no-op — nothing to transition.
    pool.stop_all()
    assert pool.slot(0).state is ReplicaState.KILLED
    assert pool.slot(1).state is ReplicaState.STOPPED


# ── kill-worker / start-worker parsing + hooks ──────────────────────────


from bench_harness.hooks import enter_kill_worker, enter_start_worker
from bench_harness.phases import Phase, PhaseRuntime, PhaseType


def test_parse_phase_spec_accepts_params():
    p = parse_phase_spec("kill=kill-worker(instance=2):60s")
    assert p.label == "kill"
    assert p.type is PhaseType.KILL_WORKER
    assert p.duration_s == 60
    assert p.int_param("instance", 0) == 2


def test_parse_phase_spec_no_params_defaults_empty():
    p = parse_phase_spec("warmup_1=warmup:10m")
    assert p.params == ()
    # Default is honoured when param missing.
    assert p.int_param("instance", 5) == 5


def test_parse_phase_spec_multiple_params():
    p = parse_phase_spec("r=kill-worker(instance=1,reason=crash):30s")
    assert p.int_param("instance", 0) == 1
    assert p.param("reason") == "crash"


def test_parse_phase_spec_rejects_bad_param():
    with pytest.raises(ValueError, match="expected k=v"):
        parse_phase_spec("bad=kill-worker(instance):30s")


def test_parse_phase_spec_int_param_rejects_non_integer():
    p = parse_phase_spec("k=kill-worker(instance=not-int):30s")
    with pytest.raises(ValueError, match="must be an integer"):
        p.int_param("instance", 0)


def test_kill_worker_default_instance_is_zero():
    # Covers the "no params" default via the int_param helper.
    p = parse_phase_spec("k=kill-worker:30s")
    assert p.int_param("instance", 0) == 0


def test_start_worker_phase_type_present():
    p = parse_phase_spec("r=start-worker(instance=0):30s")
    assert p.type is PhaseType.START_WORKER


def test_crash_recovery_scenario_resolves():
    phases = resolve_scenario("crash_recovery", None)
    types = [p.type for p in phases]
    # Must start with warmup (the first-phase-must-be-warmup check), and
    # pair kill with start in order — otherwise replica 0 is dead going
    # into the following scenario.
    assert types[0] is PhaseType.WARMUP
    assert PhaseType.KILL_WORKER in types
    assert PhaseType.START_WORKER in types
    kill_idx = types.index(PhaseType.KILL_WORKER)
    start_idx = types.index(PhaseType.START_WORKER)
    assert start_idx > kill_idx, "start-worker must come after kill-worker"
    # Kill phase targets the same replica the start phase brings back.
    kill_phase = phases[kill_idx]
    start_phase = phases[start_idx]
    assert kill_phase.int_param("instance", 0) == start_phase.int_param(
        "instance", 0
    )


def test_event_delivery_matrix_scenario_resolves_expected_shape():
    phases = resolve_scenario("event_delivery_matrix", None)
    assert [p.type for p in phases] == [
        PhaseType.WARMUP,
        PhaseType.CLEAN,
        PhaseType.ACTIVE_READERS,
        PhaseType.HIGH_LOAD,
        PhaseType.CLEAN,
    ]


def test_fleet_steady_state_scenario_resolves_expected_shape():
    phases = resolve_scenario("fleet_steady_state", None)
    assert [p.type for p in phases] == [
        PhaseType.WARMUP,
        PhaseType.CLEAN,
        PhaseType.ACTIVE_READERS,
    ]


def test_kill_worker_hook_calls_pool_kill():
    pool = MagicMock()
    phase = Phase(
        label="kill",
        type=PhaseType.KILL_WORKER,
        duration_s=60,
        params=(("instance", "2"),),
    )
    runtime = PhaseRuntime(
        database_url="", phase=phase, state={"replica_pool": pool}
    )
    enter_kill_worker(runtime)
    pool.kill_worker.assert_called_once_with(2)


def test_start_worker_hook_calls_pool_start():
    pool = MagicMock()
    phase = Phase(
        label="restart",
        type=PhaseType.START_WORKER,
        duration_s=60,
        params=(("instance", "1"),),
    )
    runtime = PhaseRuntime(
        database_url="", phase=phase, state={"replica_pool": pool}
    )
    enter_start_worker(runtime)
    pool.start_worker.assert_called_once_with(1)


def test_kill_worker_hook_without_pool_errors():
    phase = Phase(
        label="kill",
        type=PhaseType.KILL_WORKER,
        duration_s=60,
        params=(),
    )
    runtime = PhaseRuntime(database_url="", phase=phase, state={})
    with pytest.raises(RuntimeError, match="kill-worker requires state"):
        enter_kill_worker(runtime)


def test_start_worker_hook_without_pool_errors():
    phase = Phase(
        label="restart",
        type=PhaseType.START_WORKER,
        duration_s=60,
        params=(),
    )
    runtime = PhaseRuntime(database_url="", phase=phase, state={})
    with pytest.raises(RuntimeError, match="start-worker requires state"):
        enter_start_worker(runtime)

"""Combine N single-system long-horizon runs into one consolidated report.

Use this when several systems were run as separate `long_horizon.py`
invocations (e.g. a wrapper script that loops one system per call) and
the cross-system overlays in `index.html` are needed after the fact.
The preferred path is still to pass `--systems a,b,c,...` to a single
invocation; this module exists for the case where that ship has sailed.

Inputs must share the same phase shape (label, type, duration). Each
system may appear in only one input run.

CLI is wired in `long_horizon.py` as the `combine` subcommand, see
`benchmarks/portable/README.md`.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from .phases import Phase, PhaseType
from .report import write_interactive_report


def _phase_signature(phases: list[dict]) -> tuple[tuple[str, str, int], ...]:
    return tuple((p["label"], p["type"], int(p["duration_s"])) for p in phases)


def _load_run(run_dir: Path) -> dict:
    return {
        "dir": run_dir,
        "manifest": json.loads((run_dir / "manifest.json").read_text()),
        "summary": json.loads((run_dir / "summary.json").read_text()),
    }


def _validate_compatible(runs: list[dict]) -> None:
    if not runs:
        raise SystemExit("combine: at least one run dir is required")
    base_sig = _phase_signature(runs[0]["manifest"]["phases"])
    for entry in runs[1:]:
        sig = _phase_signature(entry["manifest"]["phases"])
        if sig != base_sig:
            raise SystemExit(
                f"combine: phase shape mismatch between {runs[0]['dir'].name} "
                f"and {entry['dir'].name}.\n"
                f"  expected: {base_sig}\n"
                f"  found:    {sig}\n"
                f"All inputs must share the same phase labels, types, and durations."
            )


def _merge_systems_list(runs: list[dict]) -> list[str]:
    seen: list[str] = []
    for entry in runs:
        for system in entry["manifest"]["systems"]:
            if system not in seen:
                seen.append(system)
    return seen


def _merge_summary(runs: list[dict], systems: list[str]) -> dict:
    base = runs[0]["summary"]
    merged_systems: dict[str, dict] = {}
    for entry in runs:
        for system, payload in entry["summary"].get("systems", {}).items():
            if system in merged_systems:
                raise SystemExit(
                    f"combine: system '{system}' appears in multiple input runs. "
                    "combine is for joining one-system-per-run inputs; if you have "
                    "legitimate duplicates, drop one before combining."
                )
            merged_systems[system] = payload
    return {**base, "systems": {sys: merged_systems[sys] for sys in systems}}


def _concat_raw_csv(runs: list[dict], out_path: Path) -> None:
    header_written = False
    with out_path.open("w", newline="") as out_fh:
        writer = csv.writer(out_fh)
        for entry in runs:
            with (entry["dir"] / "raw.csv").open(newline="") as in_fh:
                reader = csv.reader(in_fh)
                header = next(reader)
                if not header_written:
                    writer.writerow(header)
                    header_written = True
                writer.writerows(reader)


def _build_combined_manifest(
    runs: list[dict], systems: list[str], run_id: str
) -> dict:
    base = runs[0]["manifest"]
    return {
        **base,
        "run_id": run_id,
        "systems": systems,
        "combined_from": [entry["manifest"]["run_id"] for entry in runs],
    }


def _phase_objects(manifest: dict) -> list[Phase]:
    return [
        Phase(
            label=p["label"],
            type=PhaseType(p["type"]),
            duration_s=int(p["duration_s"]),
            params=p.get("params", {}),
        )
        for p in manifest["phases"]
    ]


def add_subparser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "combine",
        help="Combine N single-system runs into one consolidated report.",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--out",
        required=True,
        type=Path,
        help="Output directory for the combined run (created if missing).",
    )
    parser.add_argument(
        "run_dirs",
        nargs="+",
        type=Path,
        help="Existing per-system run directories to combine.",
    )
    parser.set_defaults(func=run)
    return parser


def run(args: argparse.Namespace) -> int:
    runs = [_load_run(d) for d in args.run_dirs]
    _validate_compatible(runs)
    systems = _merge_systems_list(runs)

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = out_dir.name or f"combined-{runs[0]['manifest']['run_id']}"

    manifest = _build_combined_manifest(runs, systems, run_id)
    summary = _merge_summary(runs, systems)

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    raw_csv_path = out_dir / "raw.csv"
    _concat_raw_csv(runs, raw_csv_path)

    write_interactive_report(
        run_dir=out_dir,
        raw_csv=raw_csv_path,
        summary=summary,
        manifest=manifest,
        phases=_phase_objects(manifest),
        systems=systems,
    )

    import sys
    print(f"combined report: {out_dir / 'index.html'}", file=sys.stderr)
    print(f"systems:         {', '.join(systems)}", file=sys.stderr)
    print(f"merged from:     {len(runs)} run(s)", file=sys.stderr)
    return 0

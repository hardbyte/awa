"""Harness-side version capture for each adapter.

The runtime descriptor each adapter emits on startup (`kind: descriptor`) is
one half of the story: it records whatever the process chose to claim about
itself at start-of-run. This module records the other half — what the
harness can prove by inspecting the source tree and pinned manifests:

- `awa*`: the git SHA / branch / dirty state of the awa checkout (since
  the native `awa-bench`, the Docker image, and the Python wheel all compile
  from it). This is the canonical answer to "which commit of awa was
  benchmarked?".
- `pgque`: the submodule SHA of `pgque-bench/vendor/pgque` (that SQL is
  embedded into the image at build time).
- `procrastinate` / `river` / `oban`: the pinned upstream library version
  declared in the adapter's dependency file (pyproject.toml, go.mod, mix.exs).

The resulting block is attached to each adapter's entry in manifest.json as
`adapters.<system>.revision`, so `manifest.json` answers "what exact code was
compared?" without needing to cross-reference anything outside the run's
results directory.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SCRIPT_DIR.parent.parent


def _git(args: list[str], cwd: Path = REPO_ROOT) -> str | None:
    try:
        res = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    return res.stdout.strip() if res.returncode == 0 else None


def _awa_repo_revision() -> dict[str, Any]:
    """Git state of the awa repo — shared by awa, awa-docker, awa-python."""
    sha = _git(["rev-parse", "HEAD"])
    short = _git(["rev-parse", "--short", "HEAD"])
    branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    dirty = bool((_git(["status", "--porcelain"]) or "").strip())
    return {
        "source": "awa repo",
        "git_sha": sha,
        "git_short": short,
        "git_branch": branch,
        "dirty": dirty,
    }


def _pgque_submodule_revision() -> dict[str, Any]:
    """pgque upstream SHA via the submodule pointer."""
    base = _awa_repo_revision()
    sub_path = "benchmarks/portable/pgque-bench/vendor/pgque"
    status = _git(["submodule", "status", sub_path])
    # `git submodule status` prints " <sha> <path> (<describe>)"; leading
    # char is '-' for uninitialised, '+' for mismatch, space for in-sync.
    submodule_sha: str | None = None
    submodule_describe: str | None = None
    if status:
        m = re.match(r"^[\s\-+]?([0-9a-f]{7,40})\s+\S+(?:\s+\((.+?)\))?", status)
        if m:
            submodule_sha = m.group(1)
            submodule_describe = m.group(2)
    return {
        **base,
        "source": "awa repo + pgque submodule",
        "pgque_submodule_sha": submodule_sha,
        "pgque_submodule_describe": submodule_describe,
    }


# Regexes cheap enough to inline; these files are small and well-pinned.
_PROCRASTINATE_RE = re.compile(r'"procrastinate==([^"]+)"')
_RIVER_RE = re.compile(r"github\.com/riverqueue/river\s+v(\S+)")
_OBAN_RE = re.compile(r":oban,\s*\"~>\s*(\S+?)\"")
_PGMQ_IMAGE_RE = re.compile(r'^PGMQ_PG_IMAGE = "([^"]+)"$', re.MULTILINE)
_PGMQ_VERSION_RE = re.compile(r'^PGMQ_UPSTREAM_VERSION = "([^"]+)"$', re.MULTILINE)


def _read(path: Path) -> str:
    try:
        return path.read_text()
    except OSError:
        return ""


def _procrastinate_revision() -> dict[str, Any]:
    text = _read(SCRIPT_DIR / "procrastinate-bench" / "pyproject.toml")
    m = _PROCRASTINATE_RE.search(text)
    return {
        "source": "procrastinate-bench/pyproject.toml",
        "library": "procrastinate",
        "pinned_version": m.group(1) if m else None,
    }


def _river_revision() -> dict[str, Any]:
    text = _read(SCRIPT_DIR / "river-bench" / "go.mod")
    m = _RIVER_RE.search(text)
    return {
        "source": "river-bench/go.mod",
        "library": "github.com/riverqueue/river",
        "pinned_version": f"v{m.group(1)}" if m else None,
    }


def _oban_revision() -> dict[str, Any]:
    text = _read(SCRIPT_DIR / "oban-bench" / "mix.exs")
    m = _OBAN_RE.search(text)
    return {
        "source": "oban-bench/mix.exs",
        "library": "oban",
        "pinned_version_constraint": f"~> {m.group(1)}" if m else None,
    }


def _pgboss_revision() -> dict[str, Any]:
    path = SCRIPT_DIR / "pgboss-bench" / "package.json"
    try:
        data = json.loads(path.read_text())
    except OSError:
        data = {}
    except Exception:
        data = {}
    return {
        "source": "pgboss-bench/package.json",
        "library": "pg-boss",
        "pinned_version": ((data.get("dependencies") or {}).get("pg-boss")),
    }


def _pgmq_revision() -> dict[str, Any]:
    text = _read(SCRIPT_DIR / "pgmq-bench" / "main.py")
    image = _PGMQ_IMAGE_RE.search(text)
    version = _PGMQ_VERSION_RE.search(text)
    return {
        "source": "pgmq-bench/main.py",
        "library": "pgmq extension",
        "pg_image": image.group(1) if image else None,
        "pinned_version": version.group(1) if version else None,
    }


_CAPTURE: dict[str, Any] = {
    "awa": _awa_repo_revision,
    "awa-canonical": _awa_repo_revision,
    "awa-docker": _awa_repo_revision,
    "awa-python": _awa_repo_revision,
    "pgque": _pgque_submodule_revision,
    "pgmq": _pgmq_revision,
    "pgboss": _pgboss_revision,
    "procrastinate": _procrastinate_revision,
    "river": _river_revision,
    "oban": _oban_revision,
}


def capture_adapter_revision(system: str) -> dict[str, Any]:
    """Return a best-effort revision block for `system`.

    Always returns a dict. Unknown systems get a single-field marker so the
    report still shows *something* rather than silently omitting the adapter
    from the versions surface.
    """
    getter = _CAPTURE.get(system)
    if getter is None:
        return {"source": "unknown", "note": f"no version capture registered for {system!r}"}
    return getter()


def capture_all(systems: list[str]) -> dict[str, dict]:
    return {system: capture_adapter_revision(system) for system in systems}


def format_revision_oneline(system: str, entry: dict | None) -> str:
    """Render a one-line revision string for Markdown reports.

    `entry` is the manifest's `adapters.<system>` dict. It combines the
    runtime descriptor fields the adapter emitted (`version`,
    `schema_version`) with the harness-captured `revision` block (git
    SHA / submodule SHA / pinned upstream version). Returns a pipe-
    separator-safe one-liner suitable for a Markdown table cell.
    """
    entry = entry or {}
    rev = entry.get("revision") or {}
    parts: list[str] = []
    short = rev.get("git_short")
    branch = rev.get("git_branch")
    dirty = rev.get("dirty")
    if short:
        tag = f"`{short}`"
        if branch and branch != "HEAD":
            tag += f" on `{branch}`"
        if dirty:
            tag += " (dirty)"
        parts.append(tag)
    sub_sha = rev.get("pgque_submodule_sha")
    if sub_sha:
        describe = rev.get("pgque_submodule_describe")
        tag = f"pgque submodule `{sub_sha[:7]}`"
        if describe:
            tag += f" (`{describe}`)"
        parts.append(tag)
    pinned = rev.get("pinned_version") or rev.get("pinned_version_constraint")
    library = rev.get("library")
    if pinned:
        parts.append(f"{library or 'upstream'} `{pinned}`" if library else f"`{pinned}`")
    adapter_version = entry.get("version")
    schema_version = entry.get("schema_version")
    runtime_bits: list[str] = []
    if adapter_version:
        runtime_bits.append(f"adapter `{adapter_version}`")
    if schema_version and schema_version != adapter_version:
        runtime_bits.append(f"schema `{schema_version}`")
    if runtime_bits:
        parts.append(f"runtime: {', '.join(runtime_bits)}")
    return " · ".join(parts) if parts else "_no revision metadata_"

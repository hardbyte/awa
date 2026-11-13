#!/usr/bin/env python3
"""Reject generated-session trailers and session links in a pull request.

Scans every commit message in BASE..HEAD and the pull request body (read from
the PR_BODY environment variable). Exits 1 and lists each offending line.

Usage: check-commit-metadata.py BASE HEAD
"""

import os
import re
import subprocess
import sys

PATTERNS = [
    ("session trailer", re.compile(r"^\s*Claude-Session\s*:", re.IGNORECASE)),
    ("session link", re.compile(r"\bclaude\.ai/", re.IGNORECASE)),
    (
        "co-author trailer",
        re.compile(
            r"^\s*Co-Authored-By\s*:.*(\bClaude\b|noreply@anthropic\.com)",
            re.IGNORECASE,
        ),
    ),
    (
        "generated-by footer",
        re.compile(r"Generated\s+(with|by)\s+\[?Claude\b", re.IGNORECASE),
    ),
]


def findings(text: str) -> list[tuple[str, str]]:
    """Return (rule, line) for every line of text that matches a rule."""
    found = []
    for line in text.splitlines():
        for rule, pattern in PATTERNS:
            if pattern.search(line):
                found.append((rule, line.strip()))
                break
    return found


def commit_messages(base: str, head: str) -> list[tuple[str, str]]:
    output = subprocess.run(
        ["git", "log", "--format=%H%x00%B%x1e", f"{base}..{head}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    commits = []
    for record in output.split("\x1e"):
        record = record.strip("\n")
        if record:
            sha, _, message = record.partition("\x00")
            commits.append((sha, message))
    return commits


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    base, head = argv[1], argv[2]
    failures = []
    for sha, message in commit_messages(base, head):
        failures += [(f"commit {sha[:12]}", rule, line) for rule, line in findings(message)]
    body = os.environ.get("PR_BODY", "")
    failures += [("pull request body", rule, line) for rule, line in findings(body)]
    for where, rule, line in failures:
        print(f"::error::{where}: {rule}: {line}")
    if failures:
        print(
            "Remove these lines: reword the commits and edit the "
            "pull request description, then push.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

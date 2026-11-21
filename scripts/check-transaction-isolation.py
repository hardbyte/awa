#!/usr/bin/env python3
"""Reject transactions awa opens on a pool without naming READ COMMITTED.

Queue storage takes a lock and then reads what earlier holders committed. A
role or database default_transaction_isolation above READ COMMITTED keeps the
transaction's first snapshot and hides those rows (#847), so library code opens
its own transactions with awa_model::isolation::BeginReadCommitted rather than
PgPool::begin. A transaction handed to the application, which keeps the
session's level, is marked with an `isolation:` comment on the line or in the
three lines above. Test modules (`#[cfg(test)] mod ...`) are not checked.

Usage: check-transaction-isolation.py [repository root]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

POOL_BEGIN = re.compile(r"\bpool(?:\(\))?\s*\.\s*(?:try_)?begin\(\)")
# A BEGIN statement in a string literal that does not name its isolation level.
BARE_BEGIN = re.compile(r'"BEGIN\b(?![^"]*ISOLATION LEVEL)[^"]*"', re.IGNORECASE)
TEST_MODULE = re.compile(r"#\[cfg\(test\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s")
RUST_GLOBS = ["*/src/**/*.rs", "awa-python/src/**/*.rs"]
MARKER = "isolation:"


def strip_comments(text: str) -> str:
    """Blanks // comments, keeping line numbers."""
    return "\n".join(line.split("//", 1)[0] for line in text.split("\n"))


def blank_test_modules(code: str) -> str:
    """Blanks the bodies of `#[cfg(test)]` modules, keeping line numbers."""
    for module in reversed(list(TEST_MODULE.finditer(code))):
        open_brace = code.find("{", module.end())
        if open_brace < 0:
            continue
        depth, end = 0, len(code)
        for index in range(open_brace, len(code)):
            if code[index] == "{":
                depth += 1
            elif code[index] == "}":
                depth -= 1
                if depth == 0:
                    end = index + 1
                    break
        body = re.sub(r"[^\n]", " ", code[module.start() : end])
        code = code[: module.start()] + body + code[end:]
    return code


def findings(path: str, text: str) -> list[tuple[str, int, str]]:
    lines = text.split("\n")
    code = blank_test_modules(strip_comments(text))
    found = []
    for match in BARE_BEGIN.finditer(code):
        found.append(
            (
                path,
                code.count("\n", 0, match.start()) + 1,
                "a BEGIN statement without ISOLATION LEVEL takes the session's default; "
                "start it with BEGIN ISOLATION LEVEL READ COMMITTED",
            )
        )
    for match in POOL_BEGIN.finditer(code):
        line = code.count("\n", 0, match.start()) + 1
        if any(MARKER in lines[i] for i in range(max(0, line - 4), line)):
            continue
        found.append(
            (
                path,
                line,
                "pool.begin() inherits the session's default isolation; use "
                "BeginReadCommitted::begin_read_committed, or mark a transaction "
                f"the application owns with `// {MARKER} <why>`",
            )
        )
    return found


def check(root: Path) -> list[tuple[str, int, str]]:
    found = []
    seen = set()
    for pattern in RUST_GLOBS:
        for file in sorted(root.glob(pattern)):
            relative = file.relative_to(root).as_posix()
            if relative in seen or "/target/" in relative or "/tests/" in relative:
                continue
            seen.add(relative)
            found += findings(relative, file.read_text())
    return found


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
    found = check(root)
    for path, line, message in found:
        print(f"{path}:{line}: {message}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())

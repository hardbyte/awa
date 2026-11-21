#!/usr/bin/env python3
"""Reject transactions awa opens on a pool without naming READ COMMITTED.

Queue storage takes a lock and then reads what earlier holders committed. A
role or database default_transaction_isolation above READ COMMITTED keeps the
transaction's first snapshot and hides those rows (#847), so library code opens
its own transactions with awa_model::isolation::BeginReadCommitted rather than
PgPool::begin. Test modules (`#[cfg(test)] mod ...`) are not checked.

Usage: check-transaction-isolation.py [repository root]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

POOL_BEGIN = re.compile(r"\bpool(?:\(\))?\s*\.\s*(?:try_)?begin\(\)")
TEST_MODULE = re.compile(r"#\[cfg\(test\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s")
RUST_GLOBS = ["*/src/**/*.rs", "awa-python/src/**/*.rs"]


def strip_comments(text: str) -> str:
    """Blanks // comments, keeping line numbers."""
    return "\n".join(line.split("//", 1)[0] for line in text.split("\n"))


def findings(path: str, text: str) -> list[tuple[str, int, str]]:
    code = strip_comments(text)
    test_module = TEST_MODULE.search(code)
    if test_module:
        code = code[: test_module.start()]
    return [
        (
            path,
            code.count("\n", 0, match.start()) + 1,
            "pool.begin() inherits the session's default isolation; "
            "use BeginReadCommitted::begin_read_committed",
        )
        for match in POOL_BEGIN.finditer(code)
    ]


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

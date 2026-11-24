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
# A BEGIN statement in a string literal. It must name READ COMMITTED, or be
# READ ONLY: a read-only transaction may take a snapshot level, since it writes
# nothing a later lock holder has to see.
BEGIN_STATEMENT = re.compile(r'"BEGIN\b[^"]*"', re.IGNORECASE)
READ_COMMITTED = re.compile(r"\bISOLATION\s+LEVEL\s+READ\s+COMMITTED\b", re.IGNORECASE)
READ_ONLY = re.compile(r"\bREAD\s+ONLY\b", re.IGNORECASE)
BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
TEST_MODULE = re.compile(r"#\[cfg\(test\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s")
RUST_GLOBS = ["*/src/**/*.rs", "awa-python/src/**/*.rs"]
MARKER = "isolation:"


def strip_comments(text: str) -> str:
    """Blanks // comments, keeping line numbers."""
    return "\n".join(line.split("//", 1)[0] for line in text.split("\n"))


RAW_STRING = re.compile(r'b?r(#*)"')
CHAR_LITERAL = re.compile(r"'(?:\\.|[^'\\])'")


def matching_brace(code: str, open_brace: int) -> int:
    """The index just past the brace that closes `open_brace`, skipping string
    literals, raw strings, char literals and block comments."""
    depth, index = 0, open_brace
    while index < len(code):
        char = code[index]
        if code.startswith("/*", index):
            end = code.find("*/", index + 2)
            index = len(code) if end < 0 else end + 2
            continue
        raw = RAW_STRING.match(code, index)
        if raw and (index == 0 or not (code[index - 1].isalnum() or code[index - 1] == "_")):
            terminator = '"' + raw.group(1)
            end = code.find(terminator, raw.end())
            index = len(code) if end < 0 else end + len(terminator)
            continue
        if char == '"':
            index += 1
            while index < len(code) and code[index] != '"':
                index += 2 if code[index] == "\\" else 1
            index += 1
            continue
        char_literal = CHAR_LITERAL.match(code, index)
        if char_literal:
            index = char_literal.end()
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return len(code)


def blank_test_modules(code: str) -> str:
    """Blanks the bodies of inline `#[cfg(test)]` modules, keeping line
    numbers. An out-of-line `mod name;` has no body here."""
    for module in reversed(list(TEST_MODULE.finditer(code))):
        declaration = re.match(r"\s*\w+\s*([{;])", code[module.end() :])
        if not declaration or declaration.group(1) == ";":
            continue
        open_brace = module.end() + declaration.start(1)
        end = matching_brace(code, open_brace)
        body = re.sub(r"[^\n]", " ", code[module.start() : end])
        code = code[: module.start()] + body + code[end:]
    return code


def findings(path: str, text: str) -> list[tuple[str, int, str]]:
    lines = text.split("\n")
    code = blank_test_modules(strip_comments(text))
    found = []
    for match in BEGIN_STATEMENT.finditer(code):
        # Only the BEGIN statement's own modes count: the text up to its first
        # `;`, without comments, so a later statement or a comment that says
        # READ ONLY does not make the transaction read-only.
        statement = BLOCK_COMMENT.sub(" ", match.group(0)[1:-1]).split(";", 1)[0]
        statement = statement.split("--", 1)[0]
        if READ_COMMITTED.search(statement) or READ_ONLY.search(statement):
            continue
        found.append(
            (
                path,
                code.count("\n", 0, match.start()) + 1,
                "a BEGIN statement that writes must run at READ COMMITTED; "
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

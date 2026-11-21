#!/usr/bin/env python3
"""Reject session advisory locks that a cancel can leave held.

A session advisory lock (pg_advisory_lock, pg_try_advisory_lock and their
_shared forms) outlives the statement and the transaction that took it. Two
ways it leaks onto a pooled connection, where it stays held until that
connection closes:

* PL/pgSQL. An unlock in an EXCEPTION handler runs only for the conditions the
  handler names, and WHEN OTHERS does not match query_canceled, so a cancel or
  statement timeout skips it. Every session lock in the migration SQL must be
  taken inside a block whose handler names query_canceled and unlocks.

* Rust. A PoolConnection that is dropped, or whose unlock fails, goes back to
  the pool still holding the lock. Session locks taken from Rust must go
  through awa_model::advisory::SessionAdvisoryLock, which closes the
  connection unless the unlock succeeded. A lock on a dedicated, unpooled
  connection is fine; mark it with an `advisory-lock:` comment saying why.

Transaction-scoped locks (pg_advisory_xact_lock and friends) end with their
transaction and are not checked.

Usage: check-advisory-locks.py [repository root]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

SESSION_LOCK = re.compile(r"\bpg_(?:try_)?advisory_lock(?:_shared)?\s*\(", re.IGNORECASE)
UNLOCK_EXCLUSIVE = re.compile(r"\bpg_advisory_unlock\s*\(", re.IGNORECASE)
UNLOCK_SHARED = re.compile(r"\bpg_advisory_unlock_shared\s*\(", re.IGNORECASE)
UNLOCK_ALL = re.compile(r"\bpg_advisory_unlock_all\s*\(", re.IGNORECASE)
MARKER = "advisory-lock:"

SQL_GLOBS = ["awa-model/migrations/*.sql"]
RUST_GLOBS = ["*/src/**/*.rs", "awa-python/src/**/*.rs"]
# The guard every Rust session lock goes through.
RUST_GUARD = "awa-model/src/advisory.rs"

BLOCK_BEGIN = re.compile(r"^\s*(?:<<\w+>>\s*)?BEGIN\s*$", re.IGNORECASE)
BLOCK_EXCEPTION = re.compile(r"^\s*EXCEPTION\b", re.IGNORECASE)
BLOCK_END = re.compile(r"^\s*END\s*(?!IF\b|LOOP\b|CASE\b)(\w+\s*)?;", re.IGNORECASE)


def strip_sql_comment(line: str) -> str:
    return line.split("--", 1)[0]


def marked(lines: list[str], index: int, lookback: int) -> bool:
    return any(MARKER in lines[i] for i in range(max(0, index - lookback), index + 1))


class Block:
    def __init__(self, start: int) -> None:
        self.start = start
        self.exception: int | None = None
        self.handler_lines: list[str] = []
        # (line index, whether the lock is the shared form)
        self.locks: list[tuple[int, bool]] = []
        # Open CASE statements or expressions in this block, so a bare END
        # that closes a CASE expression is not taken for the block's END.
        self.case_depth = 0


BRANCH_START = re.compile(r"^\s*(?:EXCEPTION\s+)?WHEN\b", re.IGNORECASE)
CASE = re.compile(r"\bCASE\b", re.IGNORECASE)
END_CASE = re.compile(r"\bEND\s+CASE\b", re.IGNORECASE)
# Inside a handler a bare END closes a CASE expression; nested blocks are
# tracked separately and never reach the handler's lines.
BARE_END = re.compile(r"\bEND\b(?!\s+(?:CASE|IF|LOOP)\b)", re.IGNORECASE)


def handler_branches(handler_lines: list[str]) -> list[tuple[str, str]]:
    """Splits an EXCEPTION section into (conditions, body) branches."""
    branches: list[list[str]] = []
    # A WHEN inside a CASE in a branch body is a CASE arm, not a new branch.
    case_depth = 0
    for line in handler_lines:
        if case_depth == 0 and BRANCH_START.match(line):
            branches.append([line, ""])
        elif branches:
            branches[-1][1] += " " + line
        end_case = len(END_CASE.findall(line))
        opened = len(CASE.findall(line)) - end_case
        closed = end_case + len(BARE_END.findall(line))
        case_depth = max(0, case_depth + opened - closed)
    result = []
    for head, body in branches:
        match = re.search(r"\bWHEN\b(.*?)\bTHEN\b(.*)", head + body, re.IGNORECASE | re.DOTALL)
        if match:
            result.append((match.group(1), match.group(2)))
    return result


def unlock_problem(body: str, modes: set[bool]) -> str | None:
    """Why `body` does not release every lock mode in `modes` (True: shared)."""
    if UNLOCK_ALL.search(body):
        return None
    missing = [
        shared
        for shared in sorted(modes)
        if not (UNLOCK_SHARED if shared else UNLOCK_EXCLUSIVE).search(body)
    ]
    if not missing:
        return None
    if UNLOCK_EXCLUSIVE.search(body) or UNLOCK_SHARED.search(body):
        # pg_advisory_unlock releases only exclusive holds, and
        # pg_advisory_unlock_shared only shared ones.
        form = "pg_advisory_unlock_shared" if missing[0] else "pg_advisory_unlock"
        return f"does not unlock with {form}, which the lock's form needs"
    return "does not unlock"


def handler_problem(block: Block) -> str | None:
    if block.exception is None:
        return "its block has no EXCEPTION handler that unlocks"
    branches = handler_branches(block.handler_lines)
    modes = {shared for _, shared in block.locks}

    def branch(condition: str) -> tuple[str, str] | None:
        pattern = re.compile(rf"\b{condition}\b", re.IGNORECASE)
        return next((b for b in branches if pattern.search(b[0])), None)

    cancel, others = branch("query_canceled"), branch("OTHERS")
    if cancel is None:
        return "its block's EXCEPTION handler does not name query_canceled"
    if others is None:
        return "its block's EXCEPTION handler does not handle OTHERS"
    # Postgres runs the first branch whose conditions match, so a branch for a
    # specific error that does not unlock leaks the lock even when a later
    # OTHERS branch would have: every branch must unlock.
    for name, chosen in (("query_canceled", cancel), ("OTHERS", others)):
        problem = unlock_problem(chosen[1], modes)
        if problem:
            return f"its block's {name} branch {problem}"
    for conditions, body in branches:
        problem = unlock_problem(body, modes)
        if problem:
            return f"its block's {' '.join(conditions.split())} branch {problem}"
    return None


def sql_findings(path: str, text: str) -> list[tuple[str, int, str]]:
    """Session locks in PL/pgSQL whose block does not release them on cancel."""
    lines = text.splitlines()
    findings: list[tuple[str, int, str]] = []
    stack: list[Block] = []

    def close(block: Block) -> None:
        problem = handler_problem(block)
        if problem is None:
            return
        for lock, _ in block.locks:
            findings.append((path, lock + 1, f"session advisory lock: {problem}"))

    for index, raw in enumerate(lines):
        line = strip_sql_comment(raw)
        if BLOCK_BEGIN.match(line):
            stack.append(Block(index))
            continue
        if stack and BLOCK_EXCEPTION.match(line) and stack[-1].exception is None:
            stack[-1].exception = index
            stack[-1].handler_lines.append(line)
            continue
        if stack:
            block = stack[-1]
            end_case = len(END_CASE.findall(line))
            opened = len(CASE.findall(line)) - end_case
            closed = end_case + len(BARE_END.findall(line))
            closes_case = block.case_depth > 0 and closed > 0
            block.case_depth = max(0, block.case_depth + opened - closed)
            if BLOCK_END.match(line) and not closes_case:
                close(stack.pop())
                continue
        if stack and stack[-1].exception is not None:
            stack[-1].handler_lines.append(line)
        if SESSION_LOCK.search(line) and not marked(lines, index, 1):
            if not stack:
                findings.append(
                    (path, index + 1, "session advisory lock outside a PL/pgSQL block")
                )
            elif stack[-1].exception is not None:
                findings.append(
                    (path, index + 1, "session advisory lock taken inside an EXCEPTION handler")
                )
            else:
                for match in SESSION_LOCK.finditer(line):
                    shared = bool(re.search(r"_shared\s*\(", match.group(0), re.IGNORECASE))
                    stack[-1].locks.append((index, shared))
    for block in stack:
        close(block)
    return findings


def rust_findings(path: str, text: str) -> list[tuple[str, int, str]]:
    """Session locks taken from Rust outside the guard."""
    if path == RUST_GUARD:
        return []
    lines = text.splitlines()
    findings = []
    for index, line in enumerate(lines):
        code = line.split("//", 1)[0]
        if SESSION_LOCK.search(code) and not marked(lines, index, 3):
            findings.append(
                (
                    path,
                    index + 1,
                    "session advisory lock outside awa_model::advisory::SessionAdvisoryLock; "
                    f"use the guard, or mark a dedicated connection with `// {MARKER} <why>`",
                )
            )
    return findings


def check(root: Path) -> list[tuple[str, int, str]]:
    findings = []
    for pattern in SQL_GLOBS:
        for file in sorted(root.glob(pattern)):
            findings += sql_findings(file.relative_to(root).as_posix(), file.read_text())
    seen = set()
    for pattern in RUST_GLOBS:
        for file in sorted(root.glob(pattern)):
            relative = file.relative_to(root).as_posix()
            if relative in seen or "/target/" in relative or "/tests/" in relative:
                continue
            seen.add(relative)
            findings += rust_findings(relative, file.read_text())
    return findings


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
    findings = check(root)
    for path, line, message in findings:
        print(f"{path}:{line}: {message}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())

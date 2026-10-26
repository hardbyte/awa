#!/usr/bin/env python3
"""Choose the greatest strictly earlier SemVer tag in the same minor release."""

import re
import sys


def version_key(tag: str) -> tuple | None:
    identifier = r"[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*"
    match = re.fullmatch(
        rf"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
        rf"(?:-({identifier}))?(?:\+{identifier})?",
        tag,
    )
    if match is None:
        return None
    major, minor, patch, prerelease = match.groups()
    parts = () if prerelease is None else tuple(prerelease.split("."))
    if any(part.isdigit() and len(part) > 1 and part.startswith("0") for part in parts):
        return None
    ordered_parts = tuple(
        (0, int(part)) if part.isdigit() else (1, part) for part in parts
    )
    return int(major), int(minor), int(patch), prerelease is None, ordered_parts


def previous_release_tag(current_tag: str, tags: list[str]) -> str:
    current = version_key(current_tag)
    if current is None:
        raise ValueError(f"Invalid release tag: {current_tag}")
    candidates = []
    for tag in tags:
        key = version_key(tag)
        if key is not None and key[:2] == current[:2] and key < current:
            candidates.append((key, "+" not in tag, tag))
    return max(candidates)[2] if candidates else ""


if __name__ == "__main__":
    print(previous_release_tag(sys.argv[1], sys.stdin.read().splitlines()))

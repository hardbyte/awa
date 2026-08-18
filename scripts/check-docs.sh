#!/usr/bin/env bash
set -euo pipefail

mkdocs build --strict
python3 scripts/build-agent-docs.py
env -u RUSTC_WRAPPER cargo check -p awa --example quickstart
python3 -m py_compile awa-python/examples/quickstart.py

# The repository CI workflow owns workspace lint/build/database tests and runs
# both canonical quickstarts. This script keeps the docs-only check fast while
# verifying that included source still parses and compiles.

if rg --glob '*.md' '\]\((?:\.\./)+(?:awa|awa-python|correctness|docker|examples|CHANGELOG)' docs; then
  echo "docs contain repository-relative links that will break on the published site" >&2
  exit 1
fi

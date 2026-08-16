#!/usr/bin/env bash
set -euo pipefail

mkdocs build --strict
RUSTC_WRAPPER= cargo check -p awa --example quickstart
python -m py_compile awa-python/examples/quickstart.py

if rg --glob '*.md' '\]\((?:\.\./)+(?:awa|awa-python|correctness|docker|examples|CHANGELOG)' docs; then
  echo "docs contain repository-relative links that will break on the published site" >&2
  exit 1
fi

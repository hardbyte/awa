#!/usr/bin/env bash
# Runs the persona soak against another revision of awa.
#
#   scripts/persona-soak-against.sh <git-ref> [engine ...]
#
# The harness lives entirely in awa/tests, so any revision that builds with the same public
# API can be soaked: a detached worktree of <git-ref> is created, this checkout's harness
# files (including uncommitted changes) are copied over it, scripts/persona-soak.sh runs
# there with the arguments after the ref, and the worktree is removed afterwards. Results go
# to this checkout's soak-results/<timestamp>-<ref> unless SOAK_RESULTS_DIR is set.
#
# Environment is passed through to scripts/persona-soak.sh. Build outputs go to
# CARGO_TARGET_DIR when it is set, so builds of several revisions share one cache; the
# harness files are copied with a fresh modification time so cargo rebuilds the test binary
# rather than reusing one built from another revision.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <git-ref> [engine ...]" >&2
  exit 2
fi
ref="$1"
shift

here="$(cd "$(dirname "$0")/.." && pwd)"
label="$(printf '%s' "$ref" | tr -c 'A-Za-z0-9._-' '_')"
export SOAK_RESULTS_DIR="${SOAK_RESULTS_DIR:-$here/soak-results/$(date +%Y%m%d-%H%M%S)-$label}"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/awa-soak-against.XXXXXX")"
cleanup() { git -C "$here" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"; }
trap cleanup EXIT

git -C "$here" worktree add --detach "$worktree" "$ref" >/dev/null
tar -C "$here" -c awa/tests/persona_soak awa/tests/persona_soak_test.rs awa/tests/ci_timing.rs \
  scripts/persona-soak.sh \
  | tar -C "$worktree" -x -m
echo "== persona soak against $ref ($(git -C "$worktree" rev-parse --short=10 HEAD))"
(cd "$worktree" && scripts/persona-soak.sh "$@")

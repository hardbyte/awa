#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

SPEC_NAME=${1:?usage: ./correctness/run-tlc.sh <Spec.tla> [Config.cfg]}
CFG_NAME=${2:-${SPEC_NAME%.tla}.cfg}

docker build -t awa-tlaplus -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR" >/dev/null
docker run --rm -v "$SCRIPT_DIR:/work" awa-tlaplus \
  -metadir /tmp/tlc-states \
  -config "/work/$CFG_NAME" "/work/$SPEC_NAME"

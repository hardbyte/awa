#!/usr/bin/env bash
# Binary/schema compatibility matrix (#367), run nightly.
#
# Rolling deploys guarantee old binaries run against the newest schema and
# the newest binary meets not-yet-migrated schemas. This harness exercises
# those skews with PINNED RELEASE ARTIFACTS (awa-pg wheels from PyPI — the
# compiled runtime, not a source build of an old tag):
#
#   forward-0.6.0   full lifecycle (enqueue/claim/complete/cancel) against
#                   the newest schema, finalized to queue storage.
#   forward-0.5.7   producers still route through the compat layer to the
#                   active engine; workers are inert (canonical hot table is
#                   empty on a finalized cluster). Asserted, not assumed.
#   backward-guard  the newest `awa migrate` against a 0.5.7-era schema with
#                   live canonical work refuses loudly (ADR-037 gate) —
#                   message content and exit code asserted.
#
#   backward-0.6     the newest `awa migrate` against an UNFINALIZED 0.6-final
#                   schema with canonical work also refuses (v040 exists, so
#                   there are pending migrations to gate), while the same
#                   schema finalized upgrades cleanly.
#
# The support statement lives in docs/stability.md.
set -euo pipefail

PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGPASSWORD=${PGPASSWORD:-postgres}
export PGPASSWORD
BASE_URL="postgres://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}"
FWD_DB=awa_compat_forward
BACK_DB=awa_compat_backward
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
AWA_BIN=${AWA_BIN:-target/debug/awa}

psql_admin() { psql "${BASE_URL}/postgres" -v ON_ERROR_STOP=1 -qAt -c "$1"; }

echo "── setup: fresh databases"
psql_admin "DROP DATABASE IF EXISTS ${FWD_DB} WITH (FORCE)"
psql_admin "DROP DATABASE IF EXISTS ${BACK_DB} WITH (FORCE)"
psql_admin "CREATE DATABASE ${FWD_DB}"
psql_admin "CREATE DATABASE ${BACK_DB}"

echo "── setup: newest schema on ${FWD_DB}, finalized to queue storage"
"$AWA_BIN" --database-url "${BASE_URL}/${FWD_DB}" migrate
# Fresh-install finalize, exactly what a first worker's
# awa.storage_auto_finalize_if_fresh would do (valid because the database
# has no jobs and no runtimes).
psql "${BASE_URL}/${FWD_DB}" -v ON_ERROR_STOP=1 -q <<'SQL'
UPDATE awa.storage_transition_state
SET state = 'active',
    current_engine = 'queue_storage',
    prepared_engine = NULL,
    details = jsonb_build_object('schema', 'awa', 'auto_finalized', true),
    transition_epoch = transition_epoch + 1,
    updated_at = now(),
    finalized_at = now()
WHERE singleton;
INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
VALUES ('queue_storage', 'awa', now())
ON CONFLICT (backend) DO UPDATE
SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at;

-- Model an upgraded v042 schema: upgrades start in columns authority. A
-- fresh install starts in ledger authority, so restore the compat cursors
-- from the seeded ledgers before exercising the mixed 0.6.0/0.7 window.
UPDATE awa.ring_cursor_authority
SET authority = 'columns', flipped_at = NULL
WHERE singleton;
UPDATE awa.queue_ring_state AS state
SET current_slot = cursor.slot, generation = cursor.generation
FROM (SELECT slot, generation FROM awa.queue_ring_rotations ORDER BY generation DESC LIMIT 1) AS cursor
WHERE state.singleton;
UPDATE awa.lease_ring_state AS state
SET current_slot = cursor.slot, generation = cursor.generation
FROM (SELECT slot, generation FROM awa.lease_ring_rotations ORDER BY generation DESC LIMIT 1) AS cursor
WHERE state.singleton;
UPDATE awa.claim_ring_state AS state
SET current_slot = cursor.slot, generation = cursor.generation
FROM (SELECT slot, generation FROM awa.claim_ring_rotations ORDER BY generation DESC LIMIT 1) AS cursor
WHERE state.singleton;
SQL

echo "── setup: pinned release artifacts (PyPI wheels)"
uv venv --quiet --clear .compat-venv-060
uv pip install --quiet --python .compat-venv-060 "awa-pg==0.6.0"
uv venv --quiet --clear .compat-venv-057
uv pip install --quiet --python .compat-venv-057 "awa-pg==0.5.7"

echo "── leg: forward-0.6.0 (full lifecycle on newest schema)"
DATABASE_URL="${BASE_URL}/${FWD_DB}" .compat-venv-060/bin/python "${SCRIPT_DIR}/compat/forward_060.py"

echo "── leg: post-flip 0.6.0 is fenced"
"$AWA_BIN" --database-url "${BASE_URL}/${FWD_DB}" storage flip-ring-authority --force
DATABASE_URL="${BASE_URL}/${FWD_DB}" .compat-venv-060/bin/python "${SCRIPT_DIR}/compat/post_flip_060.py"
fence_state=$(psql "${BASE_URL}/${FWD_DB}" -qAt -F '|' -c \
  "SELECT a.authority, q.current_slot, q.generation
   FROM awa.ring_cursor_authority AS a
   CROSS JOIN awa.queue_ring_state AS q
   WHERE a.singleton AND q.singleton")
if [ "$fence_state" != "ledger|-1|-1" ]; then
  echo "FAIL: pinned 0.6.0 maintenance crossed the ledger-authority fence ($fence_state)" >&2
  exit 1
fi
echo "0.6.0 was fenced; compat cursor remains poisoned ($fence_state)"

echo "── leg: forward-0.5.7 (producer routes, worker inert)"
DATABASE_URL="${BASE_URL}/${FWD_DB}" .compat-venv-057/bin/python "${SCRIPT_DIR}/compat/forward_057.py"
routed=$(psql "${BASE_URL}/${FWD_DB}" -qAt -c \
  "SELECT count(*) FROM awa.jobs WHERE queue = 'compat_legacy'")
hot=$(psql "${BASE_URL}/${FWD_DB}" -qAt -c \
  "SELECT count(*) FROM awa.jobs_hot WHERE queue = 'compat_legacy'")
if [ "$routed" != "1" ] || [ "$hot" != "0" ]; then
  echo "FAIL: 0.5.7 insert should route to the active engine (view=$routed, jobs_hot=$hot)" >&2
  exit 1
fi
echo "0.5.7 insert routed to queue storage (visible in view, absent from jobs_hot)"

echo "── leg: backward guard (newest migrate refuses old unfinalized schema)"
DATABASE_URL="${BASE_URL}/${BACK_DB}" .compat-venv-057/bin/python "${SCRIPT_DIR}/compat/backward_prepare_057.py"
set +e
gate_output=$("$AWA_BIN" --database-url "${BASE_URL}/${BACK_DB}" migrate 2>&1)
gate_rc=$?
set -e
if [ "$gate_rc" -eq 0 ]; then
  echo "FAIL: newest migrate must refuse a 0.5.7 schema with canonical work" >&2
  echo "$gate_output"
  exit 1
fi
case "$gate_output" in
  *"storage transition not finalized"*"awa storage prepare"*) ;;
  *)
    echo "FAIL: refusal must name the finalize steps; got:" >&2
    echo "$gate_output"
    exit 1
    ;;
esac
echo "newest migrate refused legibly (exit ${gate_rc}, names the finalize steps)"

BACK06_DB=awa_compat_backward_06
echo "── leg: backward guard (0.6-final unfinalized schema with work refuses; finalized upgrades)"
psql_admin "DROP DATABASE IF EXISTS ${BACK06_DB} WITH (FORCE)"
psql_admin "CREATE DATABASE ${BACK06_DB}"
DATABASE_URL="${BASE_URL}/${BACK06_DB}" .compat-venv-060/bin/python - <<'PY'
import asyncio, os
from dataclasses import dataclass
import awa

@dataclass
class LegacyJob:
    marker: str

async def main():
    client = awa.AsyncClient(os.environ["DATABASE_URL"])
    await client.migrate()
    await client.insert(LegacyJob(marker="stranded"), queue="compat_backward")
    print("0.6.0 schema prepared with canonical job")

asyncio.run(main())
PY
set +e
gate06_output=$("$AWA_BIN" --database-url "${BASE_URL}/${BACK06_DB}" migrate 2>&1)
gate06_rc=$?
set -e
if [ "$gate06_rc" -eq 0 ]; then
  echo "FAIL: newest migrate must refuse an unfinalized 0.6 schema with canonical work" >&2
  echo "$gate06_output"
  exit 1
fi
case "$gate06_output" in
  *"storage transition not finalized"*"awa storage prepare"*) ;;
  *)
    echo "FAIL: 0.6 refusal must name the finalize steps; got:" >&2
    echo "$gate06_output"
    exit 1
    ;;
esac
echo "newest migrate refused the unfinalized 0.6 schema legibly (exit ${gate06_rc})"

UP06_DB=awa_compat_upgrade_06
echo "── leg: the supported upgrade — finalized 0.6 schema with work migrates cleanly"
psql_admin "DROP DATABASE IF EXISTS ${UP06_DB} WITH (FORCE)"
psql_admin "CREATE DATABASE ${UP06_DB}"
DATABASE_URL="${BASE_URL}/${UP06_DB}" .compat-venv-060/bin/python -c "
import asyncio, os, awa
async def main():
    client = awa.AsyncClient(os.environ['DATABASE_URL'])
    await client.migrate()
asyncio.run(main())
"
psql "${BASE_URL}/${UP06_DB}" -v ON_ERROR_STOP=1 -q <<'SQL'
UPDATE awa.storage_transition_state
SET state = 'active', current_engine = 'queue_storage', prepared_engine = NULL,
    details = jsonb_build_object('schema', 'awa', 'auto_finalized', true),
    transition_epoch = transition_epoch + 1, updated_at = now(), finalized_at = now()
WHERE singleton;
INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
VALUES ('queue_storage', 'awa', now())
ON CONFLICT (backend) DO UPDATE
SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at;
SQL
DATABASE_URL="${BASE_URL}/${UP06_DB}" .compat-venv-060/bin/python -c "
import asyncio, os, awa
from dataclasses import dataclass
@dataclass
class LiveJob:
    marker: str
async def main():
    client = awa.AsyncClient(os.environ['DATABASE_URL'])
    await client.insert(LiveJob(marker='live'), queue='compat_upgrade')
asyncio.run(main())
"
"$AWA_BIN" --database-url "${BASE_URL}/${UP06_DB}" migrate
upgraded=$(psql "${BASE_URL}/${UP06_DB}" -qAt -c "SELECT MAX(version) FROM awa.schema_version")
if [ "$upgraded" -lt 40 ]; then
  echo "FAIL: finalized 0.6 schema should upgrade to the newest version (got ${upgraded})" >&2
  exit 1
fi
echo "finalized 0.6 schema with live work upgraded cleanly to v${upgraded}"

echo
echo "compat matrix: all legs behaved as documented"

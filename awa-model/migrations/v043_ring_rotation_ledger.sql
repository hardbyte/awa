-- v043 (#371): append-only ring-rotation ledgers and terminal-rollup
-- deltas, delivered as a staged expand -> flip -> contract upgrade.
--
-- The mutable ring-state cursor singletons (`queue_ring_state`,
-- `lease_ring_state`, `claim_ring_state`) were UPDATEd on every rotation;
-- under a pinned MVCC horizon those dead row versions accumulate
-- (~3.6k/hour/ring at a 1s cadence) and every hot-path claim/enqueue reads
-- these rows. #371 moves the cursor into an append-only per-ring rotation
-- ledger (`{ring}_ring_rotations`; current cursor = max-generation row,
-- backward PK scan, O(1)), and moves prune's rollup accounting into an
-- append-only `queue_terminal_rollup_deltas` landing table folded by
-- horizon-gated maintenance.
--
-- This migration is ADDITIVE: it creates + seeds the ledgers and the delta table AND keeps
-- the compat `current_slot` / `generation` singleton columns (and the
-- per-slot `generation` columns) in place, restoring them if an earlier
-- (unreleased) shipped-v043 dev schema dropped them. Each queue-storage
-- schema carries a `ring_cursor_authority` control row selecting which
-- representation is authoritative:
--   * 'columns' — compat: the singleton columns win, exactly as 0.6 wrote
--     them, so a mixed 0.6.2/0.7 fleet is safe. 0.7 rotators shadow every
--     advance into the ledger, keeping it a ready-to-promote copy.
--   * 'ledger' — the append-only ledgers win (dead-tuple-free rotation).
-- Fresh installs start in 'ledger' (no old binary can exist); upgrades
-- start in 'columns' and flip one-way to 'ledger' once the whole fleet has
-- rolled to 0.7 — via `awa storage flip-ring-authority` or the maintenance
-- auto-flip. The flip fences any returning pre-flip binary by poisoning the
-- stale cursor/prune metadata and rejecting old-style cursor updates in a
-- database trigger. The 0.8
-- contract migration drops the columns for good. See the 0.7 upgrade notes
-- in CHANGELOG.md, docs/upgrade-0.6-to-0.7.md, and ADR-040.
--
-- v023 owns the substrate helper; the migration runner reapplies the
-- amended v018 (insert_job_compat) and v023 (install helper + default
-- `awa` install) files before this one, so the default `awa` substrate is
-- already converted. This migration:
--   1. adds the additive `binary_version` column to `awa.runtime_instances`
--      (0.7 runtimes populate it; 0.6 rows stay NULL — the flip-readiness
--      discriminator);
--   2. installs the core `awa.flip_ring_authority()` /
--      `awa.ring_authority_status()` functions;
--   3. refreshes every OTHER already-installed queue-storage schema
--      (v039-style discovery loop) so custom and in-transition schemas get
--      the ledgers, the restored compat columns, the authority control row,
--      and the `ring_cursor()` resolver too.

-- (1) Additive flip-readiness marker on the shared runtime catalog. 0.7+
-- runtimes set this at registration; a 0.6 binary does not know the column
-- and its heartbeat leaves it NULL, so a fresh-heartbeat NULL-version row
-- unambiguously means "a pre-flip-aware binary is live". `version` (already
-- NOT NULL) carries a semver string for both, but NULL here is the robust
-- capability signal the flip gate keys on.
ALTER TABLE awa.runtime_instances ADD COLUMN IF NOT EXISTS binary_version TEXT;

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_lease_slots INT;
    v_claim_slots INT;
    v_claim_runtime REGPROCEDURE;
    v_claim_runtime_def TEXT;
    v_lease_claim_receipts BOOLEAN;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
    LOOP
        IF to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.lease_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.claim_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT slot_count FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_queue_slots;

        EXECUTE format(
            'SELECT slot_count FROM %I.lease_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_lease_slots;

        EXECUTE format(
            'SELECT slot_count FROM %I.claim_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_claim_slots;

        IF v_queue_slots IS NULL OR v_lease_slots IS NULL OR v_claim_slots IS NULL THEN
            CONTINUE;
        END IF;

        v_claim_runtime := to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            v_schema
        ));
        v_claim_runtime_def := pg_get_functiondef(v_claim_runtime::oid);
        v_lease_claim_receipts := v_schema = 'awa'
            OR position(format('INSERT INTO %I.lease_claims', v_schema) IN v_claim_runtime_def) > 0
            OR position(format('INSERT INTO %I.lease_claim_batches', v_schema) IN v_claim_runtime_def) > 0;

        PERFORM awa.install_queue_storage_substrate(
            v_schema,
            v_queue_slots,
            v_lease_slots,
            v_claim_slots,
            v_lease_claim_receipts
        );
    END LOOP;
END;
$$;

-- (2) Core ring-authority control functions (#371 staged upgrade).
--
-- These operate on a named queue-storage schema's per-schema
-- `ring_cursor_authority` control row and the three ring-state singletons
-- (installed by v023). They live in `awa` (not per-schema) because the
-- flip gate reads the shared `awa.runtime_instances` fleet catalog.

-- Minimum semver a live runtime must report (in `binary_version`) to be
-- considered flip-aware. Kept as a SQL constant so the gate and any future
-- tooling agree. 0.7.0 is the first release that understands ledger
-- authority.
CREATE OR REPLACE FUNCTION awa.ring_authority_min_flip_version()
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$ SELECT '0.7.0'::text $$;

-- Parse a leading `MAJOR.MINOR.PATCH` out of a semver-ish string into a
-- sortable integer (major*1_000_000 + minor*1_000 + patch), ignoring any
-- `-prerelease` / `+build` suffix. Returns NULL for an unparseable input,
-- which the gate treats as "not known to be flip-aware".
CREATE OR REPLACE FUNCTION awa.semver_rank(p_version TEXT)
RETURNS BIGINT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN m[1] IS NULL THEN NULL
        ELSE m[1]::bigint * 1000000
           + COALESCE(m[2], '0')::bigint * 1000
           + COALESCE(m[3], '0')::bigint
    END
    FROM regexp_match(COALESCE(p_version, ''), '^(\d+)(?:\.(\d+))?(?:\.(\d+))?') AS m
$$;

-- Report the authority + flip-readiness for one queue-storage schema.
-- `fresh_seconds` is the heartbeat freshness window (default 90s) used to
-- decide which runtimes count as "live" for the flip gate.
CREATE OR REPLACE FUNCTION awa.ring_authority_status(
    p_schema        TEXT,
    p_fresh_seconds DOUBLE PRECISION DEFAULT 90
)
RETURNS TABLE(
    authority               TEXT,
    flipped_at              TIMESTAMPTZ,
    live_instances          BIGINT,
    flip_aware_instances    BIGINT,
    blocking_instances      BIGINT
)
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, awa, public
AS $$
DECLARE
    v_min_rank BIGINT := awa.semver_rank(awa.ring_authority_min_flip_version());
BEGIN
    IF to_regclass(format('%I.ring_cursor_authority', p_schema)) IS NULL THEN
        RAISE EXCEPTION 'ring_authority_status: schema % has no ring_cursor_authority (not a queue-storage schema?)',
            p_schema
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY EXECUTE format(
        $q$
        SELECT
            a.authority,
            a.flipped_at,
            fleet.live_instances,
            fleet.flip_aware_instances,
            fleet.blocking_instances
        FROM %I.ring_cursor_authority AS a
        CROSS JOIN (
            SELECT
                count(*) FILTER (WHERE fresh)                       AS live_instances,
                count(*) FILTER (WHERE fresh AND flip_aware)        AS flip_aware_instances,
                count(*) FILTER (WHERE fresh AND NOT flip_aware)    AS blocking_instances
            FROM (
                SELECT
                    last_seen_at >= now() - make_interval(secs => $1) AS fresh,
                    (binary_version IS NOT NULL
                     AND awa.semver_rank(binary_version) >= $2)       AS flip_aware
                FROM awa.runtime_instances
            ) AS classified
        ) AS fleet
        WHERE a.singleton
        $q$,
        p_schema
    )
    USING p_fresh_seconds, v_min_rank;
END;
$$;

-- Flip one schema's ring-cursor authority `columns -> ledger`, atomically
-- across all three rings. Idempotent (a no-op if already 'ledger').
--
-- Safety:
--   * Refuses (unless p_force) while any FRESH-heartbeat runtime is not
--     known to be flip-aware (`binary_version` NULL or < the min version).
--     Such a runtime may be a 0.6 (or pre-flip 0.7) binary that would read
--     the compat columns; flipping out from under it would silently
--     misroute its writes.
--   * Reconciles and verifies all three shadow ledgers while the compat
--     cursor rows are locked, so an old rotator that advanced immediately
--     before the flip cannot leave the promoted ledger stale.
--   * Fences any returning pre-flip binary AT flip time: the stale compat
--     cursor and prune metadata are poisoned, and a per-schema trigger
--     rejects any old-style cursor advance after ledger authority is live.
--   * Takes `FOR UPDATE` on the three ring-state singletons so it
--     serializes against any in-flight compat rotator on this schema
--     before it changes the authority they read.
CREATE OR REPLACE FUNCTION awa.flip_ring_authority(
    p_schema        TEXT,
    p_force         BOOLEAN DEFAULT FALSE,
    p_fresh_seconds DOUBLE PRECISION DEFAULT 90
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, awa, public
AS $$
DECLARE
    v_authority         TEXT;
    v_blocking          BIGINT;
    v_ring              TEXT;
    v_column_slot       INT;
    v_column_generation BIGINT;
    v_slot_count        INT;
    v_ledger_slot       INT;
    v_ledger_generation BIGINT;
BEGIN
    IF to_regclass(format('%I.ring_cursor_authority', p_schema)) IS NULL THEN
        RAISE EXCEPTION 'flip_ring_authority: schema % has no ring_cursor_authority (not a queue-storage schema?)',
            p_schema
            USING ERRCODE = '22023';
    END IF;

    -- Serialize against in-flight compat rotators (they take these same
    -- singletons FOR UPDATE) before reading/altering the authority.
    EXECUTE format('SELECT 1 FROM %I.queue_ring_state WHERE singleton FOR UPDATE', p_schema);
    EXECUTE format('SELECT 1 FROM %I.lease_ring_state WHERE singleton FOR UPDATE', p_schema);
    EXECUTE format('SELECT 1 FROM %I.claim_ring_state WHERE singleton FOR UPDATE', p_schema);

    EXECUTE format('SELECT authority FROM %I.ring_cursor_authority WHERE singleton', p_schema)
        INTO v_authority;

    IF v_authority = 'ledger' THEN
        RETURN 'ledger';  -- already flipped; one-way, idempotent
    END IF;

    IF NOT p_force THEN
        SELECT blocking_instances INTO v_blocking
        FROM awa.ring_authority_status(p_schema, p_fresh_seconds);
        IF v_blocking > 0 THEN
            RAISE EXCEPTION 'flip_ring_authority: % fresh runtime(s) are not known to be flip-aware (>= %); refusing to flip. Roll the whole fleet to 0.7 first, or pass force := true to override.',
                v_blocking, awa.ring_authority_min_flip_version()
                USING ERRCODE = '55000';  -- object_not_in_prerequisite_state
        END IF;
    END IF;

    -- The singleton locks above establish the final compat cursor for every
    -- ring. Reconcile each ledger through that generation, then verify the
    -- row being promoted is exactly the authoritative column cursor. This
    -- closes the window where a 0.6 rotator advanced columns after the last
    -- 0.7 compat rotation but immediately before the flip.
    FOR v_ring IN SELECT unnest(ARRAY['queue', 'lease', 'claim'])
    LOOP
        EXECUTE format(
            'SELECT current_slot, generation, slot_count FROM %I.%I_ring_state WHERE singleton',
            p_schema, v_ring
        ) INTO v_column_slot, v_column_generation, v_slot_count;

        IF v_column_generation < 0
           OR v_column_slot < 0
           OR v_column_slot >= v_slot_count
           OR v_column_slot <> ((v_column_generation % v_slot_count) + v_slot_count) % v_slot_count THEN
            RAISE EXCEPTION 'flip_ring_authority: invalid % compat cursor (slot %, generation %, slot_count %)',
                v_ring, v_column_slot, v_column_generation, v_slot_count
                USING ERRCODE = '55000';
        END IF;

        EXECUTE format(
            'SELECT slot, generation FROM %I.%I_ring_rotations ORDER BY generation DESC LIMIT 1',
            p_schema, v_ring
        ) INTO v_ledger_slot, v_ledger_generation;

        IF v_ledger_generation > v_column_generation THEN
            RAISE EXCEPTION 'flip_ring_authority: % ledger generation % is ahead of compat generation %',
                v_ring, v_ledger_generation, v_column_generation
                USING ERRCODE = '55000';
        END IF;

        EXECUTE format(
            'INSERT INTO %1$I.%2$I_ring_rotations (generation, slot) '
            'SELECT g, ((g %% $2) + $2) %% $2 '
            'FROM generate_series('
            '  COALESCE((SELECT max(generation) FROM %1$I.%2$I_ring_rotations), -1) + 1, '
            '  $1'
            ') AS g '
            'ON CONFLICT (generation) DO NOTHING',
            p_schema, v_ring
        ) USING v_column_generation, v_slot_count;

        EXECUTE format(
            'SELECT slot, generation FROM %I.%I_ring_rotations ORDER BY generation DESC LIMIT 1',
            p_schema, v_ring
        ) INTO v_ledger_slot, v_ledger_generation;

        IF (v_ledger_slot, v_ledger_generation)
               IS DISTINCT FROM (v_column_slot, v_column_generation) THEN
            RAISE EXCEPTION 'flip_ring_authority: % ledger cursor (slot %, generation %) does not match compat cursor (slot %, generation %)',
                v_ring, v_ledger_slot, v_ledger_generation,
                v_column_slot, v_column_generation
                USING ERRCODE = '55000';
        END IF;
    END LOOP;

    -- Fence returning pre-ledger binaries. Poisoning BOTH cursor fields makes
    -- the old lease/claim prune arithmetic return no initialized slot;
    -- poisoning per-slot generations makes the old queue prune scan empty.
    -- The trigger installed by v023 rejects an old rotator's attempt to turn
    -- (-1, -1) back into (0, 0) after authority changes below.
    EXECUTE format('UPDATE %I.queue_ring_state SET current_slot = -1, generation = -1 WHERE singleton', p_schema);
    EXECUTE format('UPDATE %I.lease_ring_state SET current_slot = -1, generation = -1 WHERE singleton', p_schema);
    EXECUTE format('UPDATE %I.claim_ring_state SET current_slot = -1, generation = -1 WHERE singleton', p_schema);
    EXECUTE format('UPDATE %I.queue_ring_slots SET generation = -1', p_schema);
    EXECUTE format('UPDATE %I.lease_ring_slots SET generation = -1', p_schema);
    EXECUTE format('UPDATE %I.claim_ring_slots SET generation = -1', p_schema);

    EXECUTE format(
        'UPDATE %I.ring_cursor_authority SET authority = ''ledger'', flipped_at = now() WHERE singleton',
        p_schema
    );

    RETURN 'ledger';
END;
$$;

REVOKE EXECUTE ON FUNCTION awa.flip_ring_authority(TEXT, BOOLEAN, DOUBLE PRECISION) FROM PUBLIC;

INSERT INTO awa.schema_version (version, description)
VALUES (43, 'Append-only ring-rotation ledgers + terminal-rollup deltas; staged ring-cursor authority for rolling upgrades (#371)')
ON CONFLICT (version) DO NOTHING;

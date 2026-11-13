-- v060: keep claim tombstone checks on indexed probes.
-- Tracking: https://github.com/hardbyte/awa/pull/713
--
-- A claim marks each candidate row with EXISTS over ready_tombstones. For a
-- small batch the planner can turn that EXISTS into a hashed subplan, which
-- scans a whole tombstone partition on every claim although the lookup index
-- exists. OFFSET 0 in the subquery keeps it correlated, one index probe per
-- candidate row.
--
-- V23_UP writes the fenced check into new substrates. This file rewrites the
-- claim functions already installed in the default and every discovered custom
-- queue-storage schema: <schema>.claim_ready_runtime and each
-- <schema>.claim_ready_slot_<n> from v051. It replaces each function from its
-- own definition, so signature, owner, privileges and settings are unchanged,
-- and a function that already carries the fence (a fresh install, a replay, or
-- a database coming from 0.6.17, whose correlated_claim_tombstones schema
-- patch made the same change) is left alone. No table DDL, no trigger change
-- and no substrate reinstallation.
--
-- N-1 binaries call the same functions with the same arguments and results
-- and the claim protocol is unchanged, so they run the same before and after
-- the migration and rolling back binaries needs no schema change.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table, and its work scales with the number of claim
-- functions, not with job count. A claim in flight finishes on the old body;
-- the next call uses the new one.
--
-- External runners apply the complete `awa migrate --sql --from 59 --to 60`
-- bundle, which refreshes trusted queue-storage discovery before this block.
-- Run it as the owner of the claim functions, or as a migrator that can
-- SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_function RECORD;
    v_definition TEXT;
    v_updated TEXT;
BEGIN
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        FOR v_function IN
            SELECT p.oid
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = v_schema
              AND p.prokind = 'f'
              AND (p.proname = 'claim_ready_runtime'
                   OR p.proname ~ '^claim_ready_slot_[0-9]+$')
            ORDER BY p.oid
        LOOP
            v_definition := pg_get_functiondef(v_function.oid);
            -- Only a check that closes straight after the lane_seq comparison
            -- is unfenced; one already followed by OFFSET does not match.
            v_updated := regexp_replace(
                v_definition,
                '(AND tomb[.]lane_seq = ready[.]lane_seq)([[:space:]]*[)])',
                '\1 OFFSET 0\2',
                'g'
            );
            IF v_updated <> v_definition THEN
                EXECUTE v_updated;
            END IF;
        END LOOP;
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (60, 'Keep claim tombstone checks correlated')
ON CONFLICT (version) DO NOTHING;

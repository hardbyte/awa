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
-- queue-storage schema: <schema>.claim_ready_runtime and the
-- <schema>.claim_ready_slot_<n> from v051 for each installed queue slot, each
-- found by its exact signature. It replaces each function from its
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
    v_queue_slots INT;
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
        IF to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format(
            'SELECT slot_count FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_queue_slots;

        -- Only the installer's functions: claim_ready_runtime and the
        -- claim_ready_slot_<n> for each installed queue slot, each by its
        -- exact signature. Another function that shares one of these names
        -- is not touched.
        FOR v_function IN
            SELECT candidate.proc
            FROM (
                SELECT to_regprocedure(format(
                    '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
                    v_schema
                )) AS proc
                UNION ALL
                SELECT to_regprocedure(format(
                    '%I.claim_ready_slot_%s(text,int,bigint,smallint,smallint,bigint,bigint,double precision,timestamptz,timestamptz,double precision,bigint)',
                    v_schema,
                    slot
                ))
                FROM generate_series(0, COALESCE(v_queue_slots, 0) - 1) AS slot
            ) AS candidate
            WHERE candidate.proc IS NOT NULL
        LOOP
            v_definition := pg_get_functiondef(v_function.proc::oid);
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

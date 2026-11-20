-- v061: let a claim cross rolled-back reservations ahead of spent rows.
-- Tracking: https://github.com/hardbyte/awa/issues/813
--
-- When the first ready row at or after a lane's claim cursor is spent
-- (cancelled, deleted, moved, reprioritised, or already claimed),
-- claim_ready_runtime moves the cursor over the spent rows and returns. It
-- moved the cursor only over spent rows with consecutive lane positions, so a
-- rolled-back enqueue reservation between two spent rows stopped it short: the
-- next claim moved the cursor to the row after the gap, and so on, one gap per
-- claim, each returning no jobs while the lane still held the oldest head.
-- The claim now crosses the rolled-back reservations between the leading spent
-- rows. A committed row follows only ended reservations, because every enqueue
-- reserves its lane position through reserve_enqueue_seq, which holds the
-- lane's reservation lock until the enqueue ends.
--
-- V23_UP writes the new spent prefix into new substrates. This file rewrites
-- <schema>.claim_ready_runtime, found by its exact signature, in the default
-- and every discovered custom queue-storage schema. The per-slot claim
-- functions carry no spent prefix. It replaces the function from its own
-- definition, so signature, owner, privileges and settings are unchanged, and
-- a function that already carries the new spent prefix (a fresh install, a
-- replay, or a database coming from 0.6.19, whose claim_spent_prefix_gaps
-- schema patch made the same change) is left alone. No table DDL, no trigger
-- change and no substrate reinstallation.
--
-- N-1 binaries call the same function with the same arguments and results and
-- the claim protocol is unchanged, so they run the same before and after the
-- migration and rolling back binaries needs no schema change.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table, and its work scales with the number of schemas, not
-- with job count. A claim in flight finishes on the old body; the next call
-- uses the new one. A claim whose head row is live runs the same statements
-- as before.
--
-- External runners apply the complete `awa migrate --sql --from 60 --to 61`
-- bundle, which refreshes trusted queue-storage discovery before this block.
-- Run it as the owner of the claim functions, or as a migrator that can
-- SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_function REGPROCEDURE;
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
        v_function := to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            v_schema
        ));
        IF v_function IS NULL THEN
            CONTINUE;
        END IF;
        v_definition := pg_get_functiondef(v_function::oid);
        v_updated := regexp_replace(
            v_definition,
            'selected_with_spent[.]lane_seq[[:space:]]+-[[:space:]]+row_number[(][)][[:space:]]+OVER[[:space:]]+[(]ORDER BY selected_with_spent[.]lane_seq[)]::bigint[[:space:]]+AS[[:space:]]+prefix_group([[:space:]]+)FROM[[:space:]]+selected_with_spent[[:space:]]+WHERE[[:space:]]+attempt_spent([[:space:]]+[)][[:space:]]+SELECT[[:space:]]+max[(]spent_prefix[.]lane_seq[)][[:space:]]+[+][[:space:]]+1[[:space:]]+INTO[[:space:]]+v_spent_next_seq[[:space:]]+FROM[[:space:]]+spent_prefix[[:space:]]+)WHERE[[:space:]]+prefix_group[[:space:]]+=[[:space:]]+v_lane_claim_seq[[:space:]]+-[[:space:]]+1;',
            'count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through\1FROM selected_with_spent\2WHERE unspent_through = 0;',
            'g'
        );
        IF v_updated <> v_definition THEN
            EXECUTE v_updated;
        END IF;
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (61, 'Let a claim cross rolled-back reservations ahead of spent rows')
ON CONFLICT (version) DO NOTHING;

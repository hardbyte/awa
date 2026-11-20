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
-- lane's reservation lock until the enqueue ends. A position in the window can
-- still hold a committed row of another ring generation, written by a producer
-- whose snapshot predates a rotation, so the prefix stops at the first segment
-- of another generation that ends inside it, and the next claim targets that
-- segment.
--
-- V23_UP writes the new spent prefix into new substrates. This file rewrites
-- <schema>.claim_ready_runtime, found by its exact signature, in the default
-- and every discovered custom queue-storage schema. The per-slot claim
-- functions carry no spent prefix. It replaces the function from its own
-- definition, so signature, owner, privileges and settings are unchanged. It
-- rewrites the released prefix, and the prefix of 0.6.19's
-- claim_spent_prefix_gaps schema patch, which crosses gaps without the
-- generation bound. A function that already carries the bounded prefix (a
-- fresh install or a replay) is left alone. No table DDL, no trigger change and
-- no substrate reinstallation.
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
        -- The released prefix stops at the first gap; rewrite it to the
        -- unbounded leading run, the shape 0.6.19's schema patch installs.
        v_updated := regexp_replace(
            v_definition,
            'selected_with_spent[.]lane_seq[[:space:]]+-[[:space:]]+row_number[(][)][[:space:]]+OVER[[:space:]]+[(]ORDER BY selected_with_spent[.]lane_seq[)]::bigint[[:space:]]+AS[[:space:]]+prefix_group([[:space:]]+)FROM[[:space:]]+selected_with_spent[[:space:]]+WHERE[[:space:]]+attempt_spent([[:space:]]+[)][[:space:]]+SELECT[[:space:]]+max[(]spent_prefix[.]lane_seq[)][[:space:]]+[+][[:space:]]+1[[:space:]]+INTO[[:space:]]+v_spent_next_seq[[:space:]]+FROM[[:space:]]+spent_prefix[[:space:]]+)WHERE[[:space:]]+prefix_group[[:space:]]+=[[:space:]]+v_lane_claim_seq[[:space:]]+-[[:space:]]+1;',
            'count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through\1FROM selected_with_spent\2WHERE unspent_through = 0;',
            'g'
        );
        -- Then bound the leading run by the first segment of another
        -- generation that ends inside it.
        v_updated := regexp_replace(
            v_updated,
            'SELECT[[:space:]]+max[(]spent_prefix[.]lane_seq[)][[:space:]]+[+][[:space:]]+1([[:space:]]+)INTO[[:space:]]+v_spent_next_seq([[:space:]]+)FROM[[:space:]]+spent_prefix[[:space:]]+WHERE[[:space:]]+unspent_through[[:space:]]+=[[:space:]]+0;',
            format(
                'SELECT LEAST(
                max(spent_prefix.lane_seq) + 1,
                (
                    SELECT min(other.first_lane_seq)
                    FROM %I.ready_segments AS other
                    WHERE other.queue = p_queue
                      AND other.priority = v_lane_priority
                      AND other.enqueue_shard = v_lane_shard
                      AND other.next_lane_seq > v_lane_claim_seq
                      AND other.next_lane_seq <= max(spent_prefix.lane_seq)
                      AND (other.ready_slot, other.ready_generation)
                          IS DISTINCT FROM (v_target_slot, v_target_generation)
                )
            )
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE unspent_through = 0;',
                v_schema
            ),
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

-- Schema patch spent_claim_prefix_gaps: let a claim cross rolled-back
-- reservations ahead of spent ready rows.
--
-- When the first ready row at or after a lane's claim cursor is spent
-- (cancelled, deleted, moved, reprioritised, or already claimed), the claim
-- moves the cursor over the spent rows and returns. It moved the cursor only
-- over spent rows contiguous with the cursor, so a rolled-back reservation at
-- the cursor, or between two spent rows, stopped it: every claim on the queue
-- picked that lane, moved nothing and returned no jobs until the maintenance
-- leader advanced the cursor, if it did. The claim now crosses the
-- rolled-back reservations before and between the leading spent rows; a
-- committed row follows only ended reservations, because a lane's enqueues
-- hold its reservation lock until they end. Tracking:
-- https://github.com/hardbyte/awa/issues/813
--
-- The versioned v023 installer is unchanged, so checksum-validating runners
-- see the same versioned files as 0.6.18. This patch rewrites the installed
-- definitions instead: awa.install_queue_storage_substrate, so substrates
-- installed later carry the new claim, and, in each queue-storage schema,
-- <schema>.claim_ready_runtime and each <schema>.claim_ready_slot_<n>. Each
-- function is replaced from its own definition, so signature, owner,
-- privileges, settings and comment are unchanged, and a function that already
-- carries the new spent prefix is left alone.
--
-- Order: the other schema patches that embed the v023 installer replace
-- awa.install_queue_storage_substrate with the released body, so this patch
-- runs after all of them. `awa migrate` applies it last, and its R__ name
-- sorts after theirs for runners that order repeatables by name.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table and its work scales with the number of claim functions,
-- not with job count. A claim in flight finishes on the old body; the next
-- call uses the new one. A claim whose head row is live runs the same
-- statements as before. It does not wait for a schema's install lock, which
-- prepare_schema() holds while it runs the installer: when the lock is taken
-- the patch fails with 55P03, changes nothing, and can be run again. In an
-- `awa migrate --sql` bundle the earlier replica_identity patch still holds
-- table locks that installer takes, so waiting there could deadlock. A
-- schema whose prepare_schema() commits while this patch runs can keep the
-- released claim; the patch probe names it until the patch runs again.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Function names, arguments, results and the claim protocol are
-- unchanged, so released 0.6 binaries run the same against a patched or an
-- unpatched schema, and maintenance cursor advances of either release read
-- the same evidence. Run it as the owner of the installer and the claim
-- functions, or as a migrator that can SET ROLE to them.

DO $$
DECLARE
    v_schema TEXT;
    v_function RECORD;
    v_definition TEXT;
    v_updated TEXT;
    v_old CONSTANT TEXT := 'selected_with_spent[.]lane_seq[[:space:]]+-[[:space:]]+row_number[(][)][[:space:]]+OVER[[:space:]]+[(]ORDER BY selected_with_spent[.]lane_seq[)]::bigint[[:space:]]+AS[[:space:]]+prefix_group([[:space:]]+)FROM[[:space:]]+selected_with_spent[[:space:]]+WHERE[[:space:]]+attempt_spent([[:space:]]+[)][[:space:]]+SELECT[[:space:]]+max[(]spent_prefix[.]lane_seq[)][[:space:]]+[+][[:space:]]+1[[:space:]]+INTO[[:space:]]+v_spent_next_seq[[:space:]]+FROM[[:space:]]+spent_prefix[[:space:]]+)WHERE[[:space:]]+prefix_group[[:space:]]+=[[:space:]]+v_lane_claim_seq[[:space:]]+-[[:space:]]+1;';
    v_new CONSTANT TEXT := 'count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through\1FROM selected_with_spent\2WHERE unspent_through = 0;';
BEGIN
    IF to_regprocedure('awa.install_queue_storage_substrate(text,integer,integer,integer,boolean)') IS NOT NULL THEN
        v_definition := pg_get_functiondef(
            'awa.install_queue_storage_substrate(text,integer,integer,integer,boolean)'::regprocedure
        );
        v_updated := regexp_replace(v_definition, v_old, v_new, 'g');
        IF v_updated <> v_definition THEN
            EXECUTE v_updated;
        END IF;
    END IF;

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
          AND EXISTS (
              SELECT 1 FROM pg_class AS awa_c
              WHERE awa_c.relnamespace = n.oid
                AND awa_c.relname = 'ready_tombstones'
                AND awa_c.relkind = 'p'
          )
        ORDER BY n.nspname
    LOOP
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        IF NOT pg_try_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        ) THEN
            RAISE EXCEPTION 'spent_claim_prefix_gaps: schema % is being prepared', v_schema
                USING ERRCODE = 'lock_not_available',
                      HINT = 'A prepare_schema() of that schema holds its install lock. This patch changed nothing and is safe to run again.';
        END IF;
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
            v_updated := regexp_replace(v_definition, v_old, v_new, 'g');
            IF v_updated <> v_definition THEN
                EXECUTE v_updated;
            END IF;
        END LOOP;
    END LOOP;
END;
$$;

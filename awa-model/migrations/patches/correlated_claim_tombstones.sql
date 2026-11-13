-- Schema patch correlated_claim_tombstones: keep claim tombstone checks on
-- indexed probes.
--
-- A claim marks each candidate row with EXISTS over ready_tombstones. For a
-- small batch the planner can turn that EXISTS into a hashed subplan, which
-- scans a whole tombstone partition on every claim although the lookup index
-- exists. OFFSET 0 in the subquery keeps it correlated, one index probe per
-- candidate row. Tracking: https://github.com/hardbyte/awa/pull/713
--
-- Applied after the v023 definitions, whose installer writes the fenced check
-- into new substrates. The block below rewrites the claim functions already
-- installed in each queue-storage schema: <schema>.claim_ready_runtime and
-- each <schema>.claim_ready_slot_<n>. It replaces each function from its own
-- definition, so signature, owner, privileges and settings are unchanged, and
-- a function that already carries the fence is left alone.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table and its work scales with the number of claim functions,
-- not with job count. A claim in flight finishes on the old body; the next
-- call uses the new one.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Function names, arguments, results and the claim protocol are
-- unchanged, so released 0.6 binaries run the same against a patched or an
-- unpatched schema. Run it as the owner of the claim functions, or as a
-- migrator that can SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_function RECORD;
    v_definition TEXT;
    v_updated TEXT;
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

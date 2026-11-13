-- v060: keep bounded claim tombstone checks correlated.
-- N-1 binaries use the same function signatures, results and claim protocol.
-- Function replacement only: no table DDL or substrate reinstallation. Work
-- scales with installed claim functions, not job count. Apply with an owner-
-- capable role; external runners must include V23_UP's discovery bootstrap.
-- Tracking: https://github.com/hardbyte/awa/pull/713

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
        FOR v_function IN
            SELECT p.oid
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = v_schema
              AND p.prokind = 'f'
              AND (p.proname = 'claim_ready_runtime'
                   OR p.proname ~ '^claim_ready_slot_[0-9]+$')
        LOOP
            v_definition := pg_get_functiondef(v_function.oid);
            -- Match only an unfenced correlated check; replay leaves an
            -- existing OFFSET intact. Preserve signatures, ownership and ACLs.
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

-- Backfill the partial failed-row index used by queue-storage metrics.
--
-- v023 installs this index for freshly prepared schemas. Existing
-- pre-v031 queue-storage schemas need this additive migration so the
-- bounded worker metrics path does not regress to a full done_entries
-- scan when reporting failed counts.

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_slot INT;
    v_child_name TEXT;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_class AS awa_c
              WHERE awa_c.relnamespace = n.oid AND awa_c.relname = 'queue_ring_state'
          )
          AND EXISTS (
              SELECT 1 FROM pg_class AS awa_c
              WHERE awa_c.relnamespace = n.oid AND awa_c.relname = 'done_entries'
          )
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
    LOOP
        EXECUTE format(
            'SELECT slot_count FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_queue_slots;

        IF v_queue_slots IS NULL OR v_queue_slots <= 0 THEN
            CONTINUE;
        END IF;

        FOR v_slot IN 0..(v_queue_slots - 1) LOOP
            v_child_name := format('done_entries_%s', v_slot);

            IF to_regclass(format('%I.%I', v_schema, v_child_name)) IS NULL THEN
                CONTINUE;
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = v_schema
                  AND table_name = v_child_name
                  AND column_name = 'state'
            ) THEN
                CONTINUE;
            END IF;

            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I.%I (queue) WHERE state = ''failed''::awa.job_state',
                format('idx_%s_done_%s_failed_queue', v_schema, v_slot),
                v_schema,
                v_child_name
            );
        END LOOP;
    END LOOP;
END $$;

INSERT INTO awa.schema_version (version, description)
VALUES (31, 'Backfill failed done-entry metric indexes for queue storage')
ON CONFLICT (version) DO NOTHING;

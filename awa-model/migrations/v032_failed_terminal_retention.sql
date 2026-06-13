-- Backfill the pruned_failed_count rollup column used by the failed
-- terminal retention floor (#337).
--
-- v023 installs this column for freshly prepared schemas. Existing
-- pre-v032 queue-storage schemas need this additive migration so prune
-- can fold failed terminal rows dropped past the retention floor into
-- their own rollup column instead of mixing them into
-- pruned_completed_count.

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE to_regclass(format('%I.queue_ring_state', n.nspname)) IS NOT NULL
          AND to_regclass(format('%I.queue_terminal_rollups', n.nspname)) IS NOT NULL
          AND to_regprocedure(format(
              '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
              n.nspname
          )) IS NOT NULL
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = v_schema
              AND table_name = 'queue_terminal_rollups'
              AND column_name = 'pruned_failed_count'
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I.queue_terminal_rollups ADD COLUMN IF NOT EXISTS pruned_failed_count BIGINT NOT NULL DEFAULT 0',
                v_schema
            );
        END IF;
    END LOOP;
END $$;

INSERT INTO awa.schema_version (version, description)
VALUES (32, 'Add pruned_failed_count to queue_terminal_rollups for the failed terminal retention floor')
ON CONFLICT (version) DO NOTHING;
